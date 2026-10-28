//! The settings the admin UI edits (admin-ui design §8.7): every editable
//! key, its group, its title and plain explanation, its kind, whether it
//! applies at once or after a restart, and whether a change needs a typed
//! confirmation. This list is the whole set of editable keys; anything not
//! here is read-only (§8.6). The default shown is read from
//! `FileConfig::default()` at the key's path.
//!
//! Beyond §8.7's eight groups: the four scope-recency keys in Group 2
//! (scope-recency design §5), `private_notify.backfill_max_age_hours`,
//! `private_search.max_age_days` and `video.playback` (private-video-pds
//! design), and a ninth group for the lodestar keys (lodestar-reads design
//! §2.14).

use serde_json::{Map, Number, Value};

use super::live::is_live;
use super::overlay::network_name_ok;
use crate::config::{COMMUNITY_NETWORK, UtcTimeOfDay, pds_pattern};

/// A page of settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Group {
    Access,
    Storage,
    Private,
    Blocks,
    Apps,
    Services,
    Limits,
    Profiles,
    Lookups,
    /// Refract's two live keys (refract design §9.1). Edited on the
    /// Communities page, not a generic settings page, so it is not in
    /// [`Group::ALL`].
    Communities,
}

impl Group {
    pub const ALL: [Self; 9] = [
        Self::Access,
        Self::Storage,
        Self::Private,
        Self::Blocks,
        Self::Apps,
        Self::Services,
        Self::Limits,
        Self::Profiles,
        Self::Lookups,
    ];

    #[must_use]
    pub fn slug(self) -> &'static str {
        match self {
            Self::Access => "access",
            Self::Storage => "storage",
            Self::Private => "private",
            Self::Blocks => "blocks",
            Self::Apps => "apps",
            Self::Services => "services",
            Self::Limits => "limits",
            Self::Profiles => "profiles",
            Self::Lookups => "lookups",
            Self::Communities => "communities",
        }
    }

    #[must_use]
    pub fn title(self) -> &'static str {
        match self {
            Self::Access => "Who can use Prism",
            Self::Storage => "What Prism stores",
            Self::Private => "Private posts",
            Self::Blocks => "Blocks (Farsight)",
            Self::Apps => "What apps see",
            Self::Services => "Extra services",
            Self::Limits => "Limits",
            Self::Profiles => "Profiles and caching",
            Self::Lookups => "Account lookups",
            Self::Communities => "Communities",
        }
    }

    /// One line for the settings index.
    #[must_use]
    pub fn summary(self) -> &'static str {
        match self {
            Self::Access => "Members, auto-join, and who may use Thread Lens.",
            Self::Storage => "Public data, clean-up, and which accounts Prism keeps.",
            Self::Private => "How private threads and private reads behave.",
            Self::Blocks => "What Prism does when Farsight is down.",
            Self::Apps => "Answers and texts that apps show.",
            Self::Services => "Requests Prism passes on to other services.",
            Self::Limits => "How many requests each visitor may make.",
            Self::Profiles => "How long profiles from other networks are reused.",
            Self::Lookups => "Where Prism looks up accounts first.",
            Self::Communities => "Accounts other communities still host after Bluesky took them down.",
        }
    }

    #[must_use]
    pub fn from_slug(slug: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|g| g.slug() == slug)
    }
}

/// What a setting holds, and how it is entered.
#[derive(Debug, Clone, PartialEq)]
pub enum Kind {
    /// On or off.
    Switch,
    /// One of `(value, label)`.
    Choice(&'static [(&'static str, &'static str)]),
    /// A network of the routing table (not the community's own).
    Network,
    /// An extra service's state: off, or on when an address was set at
    /// install.
    Service(&'static str),
    /// A whole number in a range, with its unit in words.
    Whole { min: i64, max: i64, unit: &'static str },
    /// A size in bytes, entered and shown in GB.
    Gigabytes,
    /// A number above 0.
    Decimal { max: f64, unit: &'static str },
    Text,
    /// Text; empty means Prism's own.
    OptionalText,
    /// `HH:MM`, UTC.
    TimeOfDay,
    /// Servers, one per line.
    Hosts,
    /// `name: text`, one per line.
    Lines { left: &'static str },
    /// `did domain domain …`, one per line.
    LiveNow,
    /// `topic | link | name | description`, one per line.
    Topics,
    /// The Refract communities list, one JSON value (refract design §3.1):
    /// edited on the Communities page, which saves the whole list.
    Communities,
}

impl Kind {
    /// Whether the form shows a box of several lines.
    #[must_use]
    pub fn multiline(&self) -> bool {
        matches!(self, Self::Hosts | Self::Lines { .. } | Self::LiveNow | Self::Topics | Self::Communities)
    }
}

/// A typed confirmation (§5.5) for a change in one direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Typed {
    No,
    /// Turning the switch on needs this word.
    TurningOn(&'static str),
    /// Turning the switch off needs this word.
    TurningOff(&'static str),
}

/// One editable key.
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub key: String,
    /// Its name on the form.
    pub label: String,
    pub kind: Kind,
    pub typed: Typed,
}

/// Settings explained together on a page.
#[derive(Debug, Clone, PartialEq)]
pub struct Section {
    pub group: Group,
    pub title: String,
    /// The explanation (§8.7's text, verbatim where it has one).
    pub help: String,
    /// Applies at once; otherwise after a restart.
    pub live: bool,
    pub fields: Vec<Field>,
}

fn field(key: &str, label: &str, kind: Kind) -> Field {
    Field { key: key.to_string(), label: label.to_string(), kind, typed: Typed::No }
}

fn one(group: Group, key: &str, title: &str, help: &str, kind: Kind) -> Section {
    Section { group, title: title.to_string(), help: help.to_string(), live: is_live(key), fields: vec![field(key, title, kind)] }
}

fn many(group: Group, title: &str, help: &str, fields: Vec<Field>) -> Section {
    let live = fields.first().is_some_and(|f| is_live(&f.key));
    Section { group, title: title.to_string(), help: help.to_string(), live, fields }
}

const fn whole(min: i64, max: i64, unit: &'static str) -> Kind {
    Kind::Whole { min, max, unit }
}

/// Every whole number a `u32` key may take.
const U32: i64 = 4_294_967_295;

const ACCESS: &[(&str, &str)] = &[("members", "Members"), ("signed_in", "Anyone signed in")];
const BOOKMARK_COUNTS: &[(&str, &str)] = &[("never", "Never"), ("above_floor", "Only from a number up"), ("always", "Always")];
const PLAYBACK: &[(&str, &str)] = &[("bsky_cdn", "Bluesky's video service"), ("pds_blob", "The author's own server")];

/// The twelve private reads with a timing floor, and their names on the form.
const FLOORS: [(&str, &str); 12] = [
    ("get_posts", "Posts"),
    ("get_post_thread", "Threads"),
    ("get_author_feed", "An account's posts"),
    ("get_timeline", "Timelines"),
    ("get_bookmarks", "Bookmarks"),
    ("get_likes", "Likes"),
    ("get_reposted_by", "Reposts"),
    ("get_quotes", "Quotes"),
    ("list_notifications", "Notifications"),
    ("get_unread_count", "Unread count"),
    ("get_replies", "Replies"),
    ("search_posts", "Search"),
];

/// The rate classes (§8.7 Group 7), with what each limits.
const RATE_CLASSES: [(&str, &str, &str); 13] = [
    ("anon_read", "Visitors not signed in", "Requests per second one visitor who is not signed in may make, and how many at once."),
    ("auth_read", "Signed-in accounts", "Requests per second one signed-in account may make to read, and how many at once."),
    ("search_anon", "Searches by visitors not signed in", "Searches per second one visitor who is not signed in may make, and how many at once."),
    ("search_auth", "Searches by signed-in accounts", "Searches per second one signed-in account may make, and how many at once."),
    ("write", "Changes", "Changes per second one account may make (likes, bookmarks, mutes and the like), and how many at once."),
    ("private", "Private posts", "Requests for private posts per second one account may make, and how many at once."),
    ("thread_lens", "Thread Lens", "Thread Lens requests per second one account may make, and how many at once."),
    ("index_request", "Fetching a missing post", "Requests per second to fetch a post Prism does not have yet, and how many at once."),
    ("resolve", "Account lookups", "Account lookups per second one visitor may ask for, and how many at once."),
    ("global", "Everything from one address", "Requests per second from one address in all, whatever they are, and how many at once."),
    ("service_proxy_member", "Extra services, members", "Requests per second one member may pass on to an extra service, and how many at once."),
    ("service_proxy_ip", "Extra services, others", "Requests per second one address may pass on to an extra service, and how many at once."),
    ("refract", "Accounts kept from other communities", "Requests per second one signed-in account may make for accounts other communities still host, and how many at once."),
];

/// Every editable setting, in page order. `networks` are the routing
/// table's names; each but the community's has its own background rate.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn sections(networks: &[String]) -> Vec<Section> {
    use Group::{Access, Apps, Blocks, Limits, Lookups, Private, Profiles, Services, Storage};
    let mut out = vec![
        // Group 1.
        one(Access, "community.auto_enroll", "Auto-join", "Auto-join. When on, an account on one of the servers below becomes a member the first time it opens a Horizon app. When off, only the people you add are members.", Kind::Switch),
        one(Access, "community.enroll_pds_patterns", "Servers whose accounts may auto-join", "Servers whose accounts may auto-join. Empty means the servers of your own network. One per line: a server's name, or *. and a name for every server under it.", Kind::Hosts),
        one(Access, "community.max_members", "Most members", "The most members Prism accepts through auto-join. Members you add yourself are always accepted and count toward it.", whole(0, U32, "members")),
        one(Access, "community.max_auto_enroll_per_hour", "Auto-joins per hour", "The most accounts that may auto-join in one hour.", whole(0, U32, "accounts")),
        one(Access, "community.reindex_retry_secs", "When re-indexing members should try again", "When a member's indexing is interrupted after it was first finished (for example after Prism missed part of the network's stream), their app is told to try again after this many seconds. They stay members meanwhile.", whole(1, 3600, "seconds")),
        one(Access, "community.never_ready_expiry_days", "Remove unfinished auto-joins after", "Auto-joined accounts that never finish their first sync are removed after this many days. 0 never removes them.", whole(0, 3650, "days")),
        one(Access, "threadlens.access", "Who may use Thread Lens", "Who may use Thread Lens. 'Members' is only your members. 'Anyone signed in' also lets any account signed in to a Horizon app use it; their lookups use the part of Farsight's rate set aside for non-members.", Kind::Choice(ACCESS)),
        // Group 2 (`indexing.public_scope` has its own form and command).
        one(Storage, "indexing.fill_max_wait_hours", "Longest wait for a refill", "When public data is turned back on, how long Prism waits for the last accounts to refill before answering public requests anyway.", whole(0, 8760, "hours")),
        one(Storage, "retention.notification_days", "Keep notifications for", "Notifications older than this are deleted.", whole(1, 36_500, "days")),
        one(Storage, "retention.tombstone_days", "Remember deleted records for", "How long Prism remembers deleted records, so a late copy of one is not stored again.", whole(1, 3650, "days")),
        one(Storage, "retention.max_age_days", "Delete public records older than", "Public records older than this are deleted, except those kept for members. 0 keeps them.", whole(0, 36_500, "days")),
        one(Storage, "retention.run_at_utc", "Daily clean-up time", "When the daily clean-up runs (UTC).", Kind::TimeOfDay),
        one(Storage, "storage.budget_bytes", "Database size before a warning", "How big the database may grow before Prism warns you. Shown in GB.", Kind::Gigabytes),
        one(Storage, "scope.reply_quote_days", "Keep accounts members replied to or quoted", "How long an account a member replied to or quoted stays indexed after the member's last reply or quote. 0: replies and quotes never keep an account indexed.", whole(0, 3650, "days")),
        one(Storage, "scope.like_repost_days", "Keep accounts members liked or reposted", "How long an account a member liked or reposted stays indexed after the member's last like or repost. 0: likes and reposts never keep an account indexed.", whole(0, 3650, "days")),
        one(Storage, "scope.rebuild_grace_days", "Extra days before an account is removed", "Extra days an account nobody follows, subscribes to or has interacted with recently stays indexed before it is removed. 0 removes it at the next nightly check.", whole(0, 30, "days")),
        one(Storage, "scope.sweep_max_purges", "Most accounts removed per night", "The most accounts the nightly check removes at once; the rest go on later nights.", whole(1, 1_000_000, "accounts")),
        // Group 3.
        one(Private, "private_read.max_chain", "How deep private threads go", "How many replies deep Prism follows a private thread (1–100). Deeper serves deeper replies but reads more under heavy use.", whole(1, 100, "replies")),
        many(
            Private,
            "Timing floors",
            "Timing floors. When on, private reads wait at least this long before answering, so the time taken does not hint at what exists. Off by default: private posts are friction, not secrecy. Setting some but not all leaves the unset ones untimed; the page warns.",
            FLOORS
                .iter()
                .map(|(k, label)| field(&format!("private_read.floor_ms.{k}"), label, whole(0, 10_000, "milliseconds (0 is off)")))
                .collect(),
        ),
        one(Private, "private_read.membership_valid_for_secs", "How long apps trust a membership answer", "How long a Horizon app may act on a member's membership answer before it asks again (1 minute to 1 hour). After you remove a member, their app may still offer private posting for up to this long; private reads stop at once either way.", whole(60, 3600, "seconds")),
        one(Private, "sync.head_max_age_secs", "How fresh private reads must be", "How old Prism's knowledge of an account's latest change may be before a private read checks it again (at most 10 minutes).", whole(0, 600, "seconds")),
        one(Private, "private_read.head_fallback", "Keep private posts readable while a relay is behind", "When a relay Prism reads private posts from falls behind, still serve a private post if Prism has every change of every account it depends on, checked per read. When off, private posts are unavailable until the relay catches up.", Kind::Switch),
        many(
            Private,
            "When apps should try again",
            "When a relay is behind, Prism tells apps to try again after a quarter of how far behind it is, at least the first value and at most the second.",
            vec![
                field("private_read.degraded_retry_min_secs", "At least", whole(1, 3600, "seconds")),
                field("private_read.degraded_retry_max_secs", "At most", whole(1, 3600, "seconds")),
            ],
        ),
        one(Private, "private_notify.backfill_max_age_hours", "Notify about late private posts for", "When Prism finds a private post while catching up rather than as it happens, it notifies people only if the post was made within this many hours, so an old post found late notifies nobody.", whole(0, 87_600, "hours")),
        one(Private, "private_search.max_age_days", "How far back private search reaches", "How far back a search of private posts reaches.", whole(1, 36_500, "days")),
        // Group 4.
        Section {
            group: Blocks,
            title: "Fallback while Farsight is down".to_string(),
            help: "When Farsight is down, ask another AppView which block lists apply. That AppView then learns which of your members' pages were loaded, while Farsight is down.".to_string(),
            live: false,
            fields: vec![
                Field { typed: Typed::TurningOn("ON"), ..field("farsight.fallback.enabled", "Use the fallback", Kind::Switch) },
                field("farsight.fallback.network", "Which AppView to ask", Kind::Network),
            ],
        },
        Section {
            group: Blocks,
            title: "Remember blocks".to_string(),
            help: "Remember block-list blocks Farsight has reported, so they still apply while Farsight is down. Turning it off deletes what was remembered when Prism restarts.".to_string(),
            live: false,
            fields: vec![Field { typed: Typed::TurningOff("OFF"), ..field("farsight.memory.enabled", "Remember blocks", Kind::Switch) }],
        },
        many(
            Blocks,
            "How much is remembered",
            "How long and how many remembered blocks are kept.",
            vec![
                field("farsight.memory.retention_hours", "Keep each for", whole(1, 720, "hours")),
                field("farsight.memory.max_per_viewer", "Most per member", whole(1, 10_000, "blocks")),
                field("farsight.memory.max_total", "Most in all", whole(1, 1_000_000, "blocks")),
            ],
        ),
        many(
            Blocks,
            "Farsight lookups",
            "How many Farsight lookups Prism makes per second. Raise it if many non-members use Thread Lens.",
            vec![
                field("farsight.rate_per_sec", "Per second", Kind::Decimal { max: 100_000.0, unit: "lookups a second" }),
                field("farsight.burst", "At once", whole(1, U32, "lookups")),
            ],
        ),
        // Group 5.
        one(Apps, "unspecced.check_email_confirmed", "Ask users to confirm their email", "Tell apps to ask users to confirm their email.", Kind::Switch),
        one(Apps, "unspecced.live_now", "Who may show 'live now'", "Accounts allowed to show a 'live now' badge, and the sites their link may point to. One account per line: its DID, then its sites, separated by spaces.", Kind::LiveNow),
        one(Apps, "trends.descriptions", "Descriptions of trending topics", "Your own description for a trending topic. One per line: the topic without #, a colon, then the description.", Kind::Lines { left: "topic" }),
        one(Apps, "trends.suggested_topics", "Suggested topics", "Topics always suggested, with their link and description. One per line: topic | link | name shown | description (the last two may be left out).", Kind::Topics),
        one(Apps, "honest_501.status_url", "Link for features Prism does not offer", "The link apps show when Prism does not offer something.", Kind::Text),
        one(Apps, "honest_501.messages", "Texts for features Prism does not offer", "Your own text for a feature Prism does not offer, per feature. One per line: the feature's technical name, a colon, then the text.", Kind::Lines { left: "feature" }),
        many(
            Apps,
            "Texts while public data is off",
            "The text apps show while public data is off, or refilling. Empty uses Prism's own text.",
            vec![
                field("honest_501.public_off_message", "While public data is off", Kind::OptionalText),
                field("honest_501.public_filling_message", "While public data refills", Kind::OptionalText),
            ],
        ),
        many(
            Apps,
            "Bookmark counts",
            "Whether apps see how many people bookmarked a post. In a small community a count of 1 can identify someone; 'above a floor' shows counts only from that number up.",
            vec![
                field("bookmarks.emit_count", "Show counts", Kind::Choice(BOOKMARK_COUNTS)),
                field("bookmarks.count_floor", "From this number up", whole(1, U32, "bookmarks")),
            ],
        ),
        one(Apps, "video.playback", "Where public videos play from", "Where apps play the videos of public posts from. 'Bluesky's video service' streams them from Bluesky, with a preview picture; 'the author's own server' plays the original file from the account's server, with no preview picture. Videos in private posts always play from the author's server, whatever this says.", Kind::Choice(PLAYBACK)),
        // Group 6.
        many(
            Services,
            "Extra services",
            "Whether Prism passes these requests on to the service set up for them at install ('on'), or answers that it does not offer them ('off'). 'On' is offered only when a service address was set at install.",
            vec![
                field("services.push.state", "Push notifications", Kind::Service("push")),
                field("services.age_assurance.state", "Age checks", Kind::Service("age_assurance")),
                field("services.contact.state", "Contact import", Kind::Service("contact")),
            ],
        ),
    ];
    // Group 7.
    for (class, title, help) in RATE_CLASSES {
        out.push(many(
            Limits,
            title,
            help,
            vec![
                field(&format!("rate_limits.{class}.per_sec"), "Per second", Kind::Decimal { max: 1_000_000.0, unit: "requests a second" }),
                field(&format!("rate_limits.{class}.burst"), "At once", whole(1, U32, "requests")),
            ],
        ));
    }
    // Group 8.
    out.push(many(
        Profiles,
        "Reusing profiles",
        "How long a profile from another network is reused before it is fetched again; and how long 'not found' is remembered.",
        vec![
            field("cache.profile_ttl_secs", "Reuse a profile for", whole(1, 2_592_000, "seconds")),
            field("cache.profile_negative_ttl_secs", "Remember 'not found' for", whole(1, 2_592_000, "seconds")),
        ],
    ));
    out.push(one(Profiles, "cache.profile_refresh_max", "Profiles waiting to be refreshed", "How many profiles may wait to be refreshed in the background.", whole(1, i64::MAX, "profiles")));
    for name in networks.iter().filter(|n| n.as_str() != COMMUNITY_NETWORK) {
        out.push(many(
            Profiles,
            &format!("Background refresh from {name}"),
            "How fast Prism refreshes profiles from this network in the background.",
            vec![
                field(&format!("constellation.networks[{name}].background_rate_per_sec"), "Per second", Kind::Decimal { max: 100_000.0, unit: "profiles a second" }),
                field(&format!("constellation.networks[{name}].background_burst"), "At once", whole(1, U32, "profiles")),
            ],
        ));
    }
    // Group 9 (lodestar-reads design §2.14).
    out.push(one(
        Lookups,
        "identity.lodestar_url",
        "Lodestar",
        "Lodestar is a read-only copy of the PLC directory, where most accounts' details live, that also checks every account's handle. With its address set, Prism asks it first when apps look up accounts. Prism takes its word only for showing accounts: who an account is, its handle, and which server it is on. Sign-in, operator sign-in, membership, deletions and everything Prism stores still use the PLC directory, which also stays the fallback whenever lodestar is slow or behind. Empty turns it off.",
        Kind::Text,
    ));
    out.push(many(
        Lookups,
        "Lodestar lookups",
        "How many lodestar lookups Prism makes per second, and how many at once.",
        vec![
            field("identity.lodestar_request_rate_per_sec", "Per second", Kind::Decimal { max: 1_000_000.0, unit: "lookups a second" }),
            field("identity.lodestar_request_burst", "At once", whole(1, U32, "lookups")),
        ],
    ));
    // Refract (refract design §3.1, §9.1).
    out.push(one(
        Group::Communities,
        "refract.enabled",
        "Keep accounts other communities still host",
        "When on, an account Bluesky took down stays readable through Prism while a community listed below still hosts it on its own server and has not banned it. When off, those accounts are hidden at once and removed after a delay.",
        Kind::Switch,
    ));
    out.push(one(
        Group::Communities,
        "refract.communities",
        "Communities",
        "The communities whose accounts are kept: their servers, how their decisions are read, and which labels end it. Saved as one list.",
        Kind::Communities,
    ));
    out.push(one(
        Group::Apps,
        "refract.visitor_viewer_state",
        "Show visitors their own likes on kept accounts",
        "When on, a signed-in reader who is not a member sees their own likes, reposts and follows on accounts other communities still host. To find them, Prism sends the reader's account, with each such post and account on screen, to the backlink service (Constellation) set up at install. When off, no reader's account is sent there and those apps hide like, repost and follow on such accounts.",
        Kind::Switch,
    ));
    out.push(one(
        Group::Apps,
        "public_page.enabled",
        "Public page",
        "When on, this instance's address shows a page with the live feed of events from the relays. When off, the page and its feed answer 'not found'.",
        Kind::Switch,
    ));
    out.push(one(
        Group::Apps,
        "public_page.name",
        "Public page name",
        "The name at the top of the public page and in its browser tab. Empty uses this instance's address.",
        Kind::Text,
    ));
    out.push(one(Lookups, "identity.lodestar_timeout_ms", "Longest wait for lodestar", "How long Prism waits for one lodestar answer before it asks the PLC directory instead (100 to 10,000).", whole(100, 10_000, "milliseconds")));
    out
}

/// The section and field of `key`, if it is an editable setting.
#[must_use]
pub fn lookup(key: &str, networks: &[String]) -> Option<(Section, Field)> {
    sections(networks).into_iter().find_map(|s| {
        let f = s.fields.iter().find(|f| f.key == key)?.clone();
        Some((s, f))
    })
}

/// Whether `key` names an editable setting, without knowing the routing
/// table: a network's key is accepted by its shape (the executor checks
/// the network exists).
#[must_use]
pub fn known(key: &str) -> bool {
    if let Some(rest) = key.strip_prefix("constellation.networks[")
        && let Some((name, field)) = rest.split_once("].")
    {
        return network_name_ok(name) && name != COMMUNITY_NETWORK && matches!(field, "background_rate_per_sec" | "background_burst");
    }
    lookup(key, &[]).is_some()
}

/// The unit a key is applied and set aside with (§8.2): keys only valid
/// together.
#[must_use]
pub fn unit_of(key: &str) -> String {
    if key.starts_with("constellation.networks[")
        && let Some(end) = key.find(']')
    {
        return key[..=end].to_string();
    }
    for prefix in ["farsight.fallback.", "farsight.memory.", "private_read.floor_ms."] {
        if key.starts_with(prefix) {
            return prefix.trim_end_matches('.').to_string();
        }
    }
    if let Some(rest) = key.strip_prefix("rate_limits.")
        && let Some((class, _)) = rest.split_once('.')
    {
        return format!("rate_limits.{class}");
    }
    match key {
        "bookmarks.emit_count" | "bookmarks.count_floor" => "bookmarks".to_string(),
        "farsight.rate_per_sec" | "farsight.burst" => "farsight.rate".to_string(),
        "identity.lodestar_request_rate_per_sec" | "identity.lodestar_request_burst" => "identity.lodestar_rate".to_string(),
        other => other.to_string(),
    }
}

/// Every key of `unit`.
#[must_use]
pub fn unit_keys(unit: &str, networks: &[String]) -> Vec<String> {
    sections(networks).into_iter().flat_map(|s| s.fields).map(|f| f.key).filter(|k| unit_of(k) == unit).collect()
}

/// The value at `key` in a serialized configuration. A key the
/// configuration does not write out has its section's default: an extra
/// service with no block is off, a network's background rate its default.
#[must_use]
pub fn value_at(cfg: &Value, key: &str) -> Value {
    if let Some(v) = super::overlay::get(cfg, key) {
        return v.clone();
    }
    if key.starts_with("services.") && key.ends_with(".state") {
        return Value::String("disabled".to_string());
    }
    if key.starts_with("constellation.networks[") {
        if key.ends_with(".background_burst") {
            return Value::from(crate::config::background_burst());
        }
        if key.ends_with(".background_rate_per_sec") {
            return Number::from_f64(crate::config::background_rate_per_sec().0).map_or(Value::Null, Value::Number);
        }
    }
    Value::Null
}

/// Every editable key, in page order.
#[must_use]
pub fn keys(networks: &[String]) -> Vec<String> {
    sections(networks).into_iter().flat_map(|s| s.fields).map(|f| f.key).collect()
}

/// What a form needs beyond the field: the networks and the services set
/// up at install.
#[derive(Debug, Clone, Default)]
pub struct Context {
    pub networks: Vec<String>,
    /// Services with an address set at install.
    pub proxied: Vec<String>,
}

/// The choices a choice-like field offers, as `(value, label)`.
#[must_use]
pub fn options(kind: &Kind, ctx: &Context) -> Vec<(String, String)> {
    match kind {
        Kind::Switch => vec![("on".into(), "On".into()), ("off".into(), "Off".into())],
        Kind::Choice(choices) => choices.iter().map(|(v, l)| ((*v).to_string(), (*l).to_string())).collect(),
        Kind::Network => ctx.networks.iter().filter(|n| n.as_str() != COMMUNITY_NETWORK).map(|n| (n.clone(), n.clone())).collect(),
        Kind::Service(name) => {
            let mut out = vec![("disabled".to_string(), "Off".to_string())];
            if ctx.proxied.iter().any(|p| p == name) {
                out.push(("proxy".to_string(), "On".to_string()));
            }
            out
        }
        _ => Vec::new(),
    }
}

/// Whether the field is a choice on the form.
#[must_use]
pub fn is_choice(kind: &Kind) -> bool {
    matches!(kind, Kind::Switch | Kind::Choice(_) | Kind::Network | Kind::Service(_))
}

const GB: f64 = 1_073_741_824.0;

#[allow(clippy::cast_precision_loss)]
fn gigabytes(bytes: u64) -> String {
    let gb = bytes as f64 / GB;
    if (gb - gb.round()).abs() < 1e-9 { format!("{gb:.0}") } else { format!("{gb:.2}") }
}

fn decimal_text(v: &Value) -> String {
    match v.as_f64() {
        Some(f) if (f - f.round()).abs() < 1e-9 && f.abs() < 1e15 => format!("{f:.0}"),
        Some(f) => f.to_string(),
        None => String::new(),
    }
}

/// The value as the form's input holds it.
#[must_use]
pub fn form_text(kind: &Kind, v: &Value) -> String {
    let s = |v: &Value| v.as_str().unwrap_or_default().to_string();
    match kind {
        Kind::Switch => if v.as_bool() == Some(true) { "on" } else { "off" }.to_string(),
        Kind::Choice(_) | Kind::Network | Kind::Service(_) | Kind::Text | Kind::OptionalText | Kind::TimeOfDay => s(v),
        Kind::Whole { .. } => v.as_i64().map(|n| n.to_string()).or_else(|| v.as_u64().map(|n| n.to_string())).unwrap_or_default(),
        Kind::Gigabytes => v.as_u64().map(gigabytes).unwrap_or_default(),
        Kind::Decimal { .. } => decimal_text(v),
        Kind::Hosts => v.as_array().map(|a| a.iter().map(s).collect::<Vec<_>>().join("\n")).unwrap_or_default(),
        Kind::Lines { .. } => v
            .as_object()
            .map(|o| o.iter().map(|(k, t)| format!("{k}: {}", s(t))).collect::<Vec<_>>().join("\n"))
            .unwrap_or_default(),
        Kind::LiveNow => v
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|e| {
                        let domains: Vec<String> = e.get("domains").and_then(Value::as_array).map(|d| d.iter().map(s).collect()).unwrap_or_default();
                        format!("{} {}", e.get("did").map(s).unwrap_or_default(), domains.join(" ")).trim().to_string()
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default(),
        Kind::Communities => serde_json::to_string_pretty(v).unwrap_or_default(),
        Kind::Topics => v
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|e| {
                        let part = |k: &str| e.get(k).map(s).unwrap_or_default();
                        let mut parts = vec![part("topic"), part("link"), part("display_name"), part("description")];
                        while parts.len() > 2 && parts.last().is_some_and(String::is_empty) {
                            parts.pop();
                        }
                        parts.join(" | ")
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default(),
    }
}

/// The value in words, for "current", "default" and "app.toml says".
#[must_use]
pub fn words(kind: &Kind, v: &Value, ctx: &Context) -> String {
    if is_choice(kind) {
        let text = form_text(kind, v);
        return options(kind, ctx)
            .into_iter()
            .find(|(value, _)| *value == text)
            .map_or_else(|| if text.is_empty() { "not set".to_string() } else { text.clone() }, |(_, label)| label);
    }
    let text = form_text(kind, v);
    match kind {
        Kind::Whole { unit, .. } => format!("{text} {unit}"),
        Kind::Gigabytes => format!("{text} GB"),
        Kind::Decimal { unit, .. } => format!("{text} {unit}"),
        Kind::OptionalText if v.is_null() => "Prism's own text".to_string(),
        Kind::Communities => {
            let names: Vec<&str> = v.as_array().map(|a| a.iter().filter_map(|c| c.get("name").and_then(Value::as_str)).collect()).unwrap_or_default();
            if names.is_empty() { "none".to_string() } else { names.join(", ") }
        }
        Kind::TimeOfDay => format!("{text} UTC"),
        _ if text.is_empty() => "none".to_string(),
        Kind::Hosts | Kind::Lines { .. } | Kind::LiveNow | Kind::Topics => text.lines().collect::<Vec<_>>().join("; "),
        _ => text,
    }
}

fn number(text: &str) -> String {
    text.trim().replace([',', '_', ' '], "")
}

/// A form's text as the value to save, or what is wrong with it, in plain
/// words.
///
/// # Errors
///
/// What is wrong with the text.
#[allow(clippy::too_many_lines)]
pub fn parse(kind: &Kind, text: &str, ctx: &Context) -> Result<Value, String> {
    let lines = || text.lines().map(str::trim).filter(|l| !l.is_empty());
    match kind {
        Kind::Switch => match text.trim() {
            "on" => Ok(Value::Bool(true)),
            "off" => Ok(Value::Bool(false)),
            _ => Err("choose on or off".to_string()),
        },
        Kind::Choice(_) | Kind::Network | Kind::Service(_) => {
            let t = text.trim();
            if options(kind, ctx).iter().any(|(v, _)| v == t) {
                Ok(Value::String(t.to_string()))
            } else if matches!(kind, Kind::Service(_)) && t == "proxy" {
                Err("this service had no address set at install, so it cannot be turned on here".to_string())
            } else {
                Err("choose one of the choices offered".to_string())
            }
        }
        Kind::Whole { min, max, .. } => {
            let n: i64 = number(text).parse().map_err(|_| format!("enter a whole number from {min} to {max}"))?;
            if (*min..=*max).contains(&n) { Ok(Value::from(n)) } else { Err(format!("enter a whole number from {min} to {max}")) }
        }
        Kind::Gigabytes => {
            let gb: f64 = number(text).parse().map_err(|_| "enter a size in GB, such as 200".to_string())?;
            if !(gb.is_finite() && gb > 0.0 && gb <= 1_000_000.0) {
                return Err("enter a size in GB above 0".to_string());
            }
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let bytes = (gb * GB).round() as u64;
            Ok(Value::from(bytes))
        }
        Kind::Decimal { max, .. } => {
            let f: f64 = number(text).parse().map_err(|_| "enter a number, such as 10 or 2.5".to_string())?;
            if !(f.is_finite() && f > 0.0 && f <= *max) {
                return Err(format!("enter a number above 0 and at most {max}"));
            }
            Number::from_f64(f).map(Value::Number).ok_or_else(|| "enter a number".to_string())
        }
        Kind::Text => Ok(Value::String(text.trim().to_string())),
        Kind::OptionalText => {
            let t = text.trim();
            Ok(if t.is_empty() { Value::Null } else { Value::String(t.to_string()) })
        }
        Kind::TimeOfDay => {
            let t = text.trim().to_string();
            UtcTimeOfDay::try_from(t.clone()).map_err(|_| "enter a time as HH:MM, such as 04:00".to_string())?;
            Ok(Value::String(t))
        }
        Kind::Hosts => {
            let mut out = Vec::new();
            for line in lines().flat_map(|l| l.split([',', ' '])).map(str::trim).filter(|l| !l.is_empty()) {
                let host = line.to_ascii_lowercase();
                pds_pattern(&host).map_err(|_| format!("{line} is not a server's name (or *. and a name)"))?;
                out.push(Value::String(host));
            }
            Ok(Value::Array(out))
        }
        Kind::Lines { left } => {
            let mut out = Map::new();
            for line in lines() {
                let (k, t) = line.split_once(':').ok_or_else(|| format!("each line needs a {left}, a colon, then the text: {line}"))?;
                let (k, t) = (k.trim().trim_start_matches('#'), t.trim());
                if k.is_empty() || t.is_empty() {
                    return Err(format!("each line needs a {left}, a colon, then the text: {line}"));
                }
                out.insert(k.to_string(), Value::String(t.to_string()));
            }
            Ok(Value::Object(out))
        }
        Kind::LiveNow => {
            let mut out = Vec::new();
            for line in lines() {
                let mut parts = line.split_whitespace();
                let did = parts.next().unwrap_or_default();
                if !proto_blue::syntax::Did::is_valid(did) {
                    return Err(format!("each line starts with an account's DID: {line}"));
                }
                let domains: Vec<Value> = parts.map(|d| Value::String(d.to_ascii_lowercase())).collect();
                if domains.is_empty() {
                    return Err(format!("name at least one site after the DID: {line}"));
                }
                out.push(serde_json::json!({ "did": did, "domains": domains }));
            }
            Ok(Value::Array(out))
        }
        Kind::Communities => {
            let v: Value = serde_json::from_str(text.trim()).map_err(|e| format!("enter the list as JSON: {e}"))?;
            if v.is_array() { Ok(v) } else { Err("enter the list as a JSON array".to_string()) }
        }
        Kind::Topics => {
            let mut out = Vec::new();
            for line in lines() {
                let parts: Vec<&str> = line.split('|').map(str::trim).collect();
                let (topic, link) = match parts.as_slice() {
                    [t, l, ..] if !t.is_empty() && !l.is_empty() => (*t, *l),
                    _ => return Err(format!("each line needs a topic and a link, separated by |: {line}")),
                };
                let mut entry = Map::new();
                entry.insert("topic".into(), Value::String(topic.to_string()));
                entry.insert("link".into(), Value::String(link.to_string()));
                for (k, i) in [("display_name", 2), ("description", 3)] {
                    if let Some(v) = parts.get(i).filter(|v| !v.is_empty()) {
                        entry.insert(k.into(), Value::String((*v).to_string()));
                    }
                }
                out.push(Value::Object(entry));
            }
            Ok(Value::Array(out))
        }
    }
}

/// The word a change needs typed, if any: turning a switch on (or off)
/// whose field asks for it.
#[must_use]
pub fn typed_word(field: &Field, from: &Value, to: &Value) -> Option<&'static str> {
    match field.typed {
        Typed::TurningOn(word) if to.as_bool() == Some(true) && from.as_bool() != Some(true) => Some(word),
        Typed::TurningOff(word) if to.as_bool() == Some(false) && from.as_bool() != Some(false) => Some(word),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::overlay;
    use super::*;
    use crate::config::FileConfig;

    fn networks() -> Vec<String> {
        FileConfig::default().constellation.networks.iter().map(|n| n.name.clone()).collect()
    }

    /// T-A8, §8.7: every registry key parses at its path, and its default
    /// is the one `FileConfig::default()` has. Each key's default is written
    /// alone (with the rest of its unit) into an empty file; the parsed
    /// configuration has that value and is otherwise the default.
    #[test]
    fn every_key_parses_at_its_path_with_its_default() {
        let defaults = FileConfig::default();
        let json = overlay::json(&defaults);
        let nets = networks();
        let all = keys(&nets);
        assert!(all.len() > 60, "{}", all.len());
        for key in &all {
            assert!(known(key), "{key} is known by its shape");
            let unit = unit_keys(&unit_of(key), &nets);
            assert!(unit.contains(key));
            let overrides: Vec<(String, Value)> = unit.iter().map(|k| (k.clone(), value_at(&json, k))).collect();
            let cfg = overlay::build(&toml::Table::new(), &overrides, &defaults).unwrap_or_else(|e| panic!("{key}: {e}"));
            let v = value_at(&json, key);
            assert!(!v.is_null() || key.starts_with("honest_501.public_"), "{key} has a default");
            assert!(overlay::same(&value_at(&overlay::json(&cfg), key), &v), "{key}");
            // An extra service written out is a block of its own; every
            // other key leaves the configuration exactly the default.
            if !key.starts_with("services.") {
                assert_eq!(cfg, defaults, "{key}");
            }
            // The default reads back through the form unchanged.
            let (_, f) = lookup(key, &nets).expect("lookup");
            let ctx = Context { networks: nets.clone(), proxied: vec!["push".into(), "age_assurance".into(), "contact".into()] };
            let back = parse(&f.kind, &form_text(&f.kind, &v), &ctx).unwrap_or_else(|e| panic!("{key}: {e}"));
            assert!(overlay::same(&back, &v), "{key}: {back} != {v}");
        }
        // No duplicate keys, and none of §8.6's read-only ones.
        let mut sorted = all.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), all.len());
        for key in &all {
            for fixed in ["http.", "metrics.", "net.", "firehose.", "farsight.url", "admin.", "db.", "kryphocron.", "scope.policy", "indexing.public_scope"] {
                assert!(!key.starts_with(fixed), "{key} is not editable (§8.6)");
            }
        }
        assert!(!all.iter().any(|k| k.contains("[community]")), "the community network has no background rate");
        // The live keys are exactly §8.5's.
        let live: Vec<&String> = all.iter().filter(|k| is_live(k)).collect();
        assert_eq!(live.len(), super::super::live::LIVE_KEYS.len());
    }

    #[test]
    fn units_group_the_keys_valid_only_together() {
        assert_eq!(unit_of("farsight.fallback.enabled"), unit_of("farsight.fallback.network"));
        assert_eq!(unit_of("farsight.memory.enabled"), unit_of("farsight.memory.max_total"));
        assert_eq!(unit_of("private_read.floor_ms.get_posts"), "private_read.floor_ms");
        assert_eq!(unit_keys("private_read.floor_ms", &[]).len(), 12);
        assert_eq!(unit_keys("rate_limits.anon_read", &[]), ["rate_limits.anon_read.per_sec", "rate_limits.anon_read.burst"]);
        assert_eq!(unit_of("constellation.networks[bluesky].background_burst"), "constellation.networks[bluesky]");
        assert_eq!(unit_of("community.auto_enroll"), "community.auto_enroll");
        assert!(known("constellation.networks[x].background_burst"));
        assert!(!known("constellation.networks[community].background_burst"));
        assert!(!known("constellation.networks[x].appview_url"));
        assert!(!known("http.bind"));
    }

    #[test]
    fn forms_parse_in_plain_words() {
        let ctx = Context { networks: networks(), proxied: vec!["push".into()] };
        let k = |key: &str| lookup(key, &ctx.networks).expect("key").1.kind;
        assert_eq!(parse(&k("private_read.max_chain"), "101", &ctx), Err("enter a whole number from 1 to 100".into()));
        assert_eq!(parse(&k("community.max_members"), "1,000", &ctx), Ok(json!(1000)));
        assert_eq!(parse(&k("storage.budget_bytes"), "200", &ctx), Ok(json!(214_748_364_800u64)));
        assert_eq!(form_text(&k("storage.budget_bytes"), &json!(214_748_364_800u64)), "200");
        assert_eq!(parse(&k("community.enroll_pds_patterns"), "PDS.example.com\n*.example.net", &ctx), Ok(json!(["pds.example.com", "*.example.net"])));
        assert!(parse(&k("community.enroll_pds_patterns"), "https://x", &ctx).is_err());
        assert_eq!(parse(&k("trends.descriptions"), "#rust: about Rust\n", &ctx), Ok(json!({"rust": "about Rust"})));
        assert_eq!(
            parse(&k("unspecced.live_now"), "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa twitch.tv YouTube.com", &ctx),
            Ok(json!([{"did": "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "domains": ["twitch.tv", "youtube.com"]}]))
        );
        assert_eq!(
            parse(&k("trends.suggested_topics"), "#x | /hashtag/x | X", &ctx),
            Ok(json!([{"topic": "#x", "link": "/hashtag/x", "display_name": "X"}]))
        );
        assert_eq!(parse(&k("honest_501.public_off_message"), "  ", &ctx), Ok(Value::Null));
        assert!(parse(&k("retention.run_at_utc"), "4am", &ctx).is_err());
        assert_eq!(parse(&k("services.push.state"), "proxy", &ctx), Ok(json!("proxy")));
        assert!(parse(&k("services.contact.state"), "proxy", &ctx).is_err(), "no address set at install");
        assert!(parse(&k("farsight.fallback.network"), "community", &ctx).is_err());
        assert_eq!(words(&k("threadlens.access"), &json!("signed_in"), &ctx), "Anyone signed in");
        let (_, fallback) = lookup("farsight.fallback.enabled", &ctx.networks).expect("key");
        assert_eq!(typed_word(&fallback, &json!(false), &json!(true)), Some("ON"));
        assert_eq!(typed_word(&fallback, &json!(true), &json!(false)), None);
    }
}
