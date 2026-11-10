//! The moderation gate (design-v2 §7.10): the single check every read path
//! runs before consulting any source. It reads the local `labels` table and
//! `actors.account_status`.
//!
//! | Input | Gate |
//! |---|---|
//! | `!takedown` on the record URI or its author DID | `Hidden(Takedown)` |
//! | author status `deactivated` / `takendown` / `suspended` | `Hidden(…)` |
//! | author `deleted` | `Hidden(Deleted)` |
//! | `!hide` on the record | `Collapsed` |
//! | otherwise | `Visible` |
//!
//! Refract (refract design §7.4) adds, from the request's snapshot and
//! serve-time check ([`RefractView`]), with no extra query: an account in
//! `wholesale` is `Hidden(Takedown)`, and a record its author's collection
//! rule hides, or with a gone community label, is `Hidden(RecordRemoved)`.
//!
//! Account status is known only for DIDs with `actors` rows; for other DIDs
//! the gate is `Visible` unless a label applies. Public paths read the
//! status only of DIDs in `scope_dids` and members ([`gate_many_among`];
//! design-v4 §11 G-9), so a row kept because of a private tie is treated
//! as absent there.

use std::collections::{HashMap, HashSet};

use sqlx::PgPool;

use crate::db::locks::uri_author;
pub use crate::refract::RefractView;

/// Why a subject is hidden.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HiddenReason {
    Takedown,
    Deactivated,
    Suspended,
    Deleted,
    /// A record Refract hides (refract design §7.4): its author's collection
    /// rule hides it, or it has a gone community label. Record subjects
    /// only.
    RecordRemoved,
}

/// The gate's verdict for one subject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    Visible,
    /// `!hide`: omitted from lists and threads; a direct `getPosts` returns
    /// it with the label.
    Collapsed,
    Hidden(HiddenReason),
}

impl Gate {
    #[must_use]
    pub fn is_hidden(self) -> bool {
        matches!(self, Self::Hidden(_))
    }

    /// Visible in lists and threads.
    #[must_use]
    pub fn listable(self) -> bool {
        self == Self::Visible
    }

    /// The profile error for a hidden actor (§5.6, §7.10).
    #[must_use]
    pub fn profile_error(self) -> Option<(&'static str, &'static str)> {
        match self {
            Self::Hidden(HiddenReason::Deactivated) => Some(("AccountDeactivated", "Account is deactivated")),
            Self::Hidden(HiddenReason::Takedown | HiddenReason::Suspended) => {
                Some(("AccountTakedown", "Account has been suspended"))
            }
            // `RecordRemoved` applies to records only; a profile never sees it.
            Self::Hidden(HiddenReason::Deleted | HiddenReason::RecordRemoved) => Some(("InvalidRequest", "Profile not found")),
            Self::Visible | Self::Collapsed => None,
        }
    }
}

fn status_reason(status: &str) -> Option<HiddenReason> {
    match status {
        "deactivated" => Some(HiddenReason::Deactivated),
        "takendown" => Some(HiddenReason::Takedown),
        "suspended" => Some(HiddenReason::Suspended),
        "deleted" => Some(HiddenReason::Deleted),
        _ => None,
    }
}

/// The gate for many subjects (at-URIs or DIDs) in two queries.
///
/// # Errors
///
/// Database errors.
pub async fn gate_many(pool: &PgPool, subjects: &[String], refract: RefractView<'_>) -> Result<HashMap<String, Gate>, sqlx::Error> {
    gate_many_among(pool, subjects, None, refract).await
}

/// [`gate_many`], reading `account_status` only for the authors in
/// `readable` when it is given: any other author's row is treated as
/// absent. Public paths pass the DIDs in `scope_dids` and the members
/// (design-v4 §11 G-9, §II-7.10), through `Hydrator::gate_many`.
///
/// # Errors
///
/// Database errors.
pub async fn gate_many_among(
    pool: &PgPool,
    subjects: &[String],
    readable: Option<&HashSet<String>>,
    refract: RefractView<'_>,
) -> Result<HashMap<String, Gate>, sqlx::Error> {
    gate_many_suspended(pool, subjects, readable, refract, None).await
}

/// Relay suspensions, in full under `suspended_accounts = "refract_only"`:
/// a non-active status from one of `relays` hides any subject, stored or
/// not, unless `exempt` says it is a member or Refract lists it.
pub struct RelaySuspensions<'a> {
    pub relays: &'a [String],
    pub exempt: &'a (dyn Fn(&str) -> bool + Sync),
}

/// The gate's account rule for SQL reads, as one inlinable expression that
/// is true when the account is hidden: `status_hidden` (the read's own test
/// of `actors.account_status`), or a non-active status from a takedown
/// relay in the bound `text[]` named by `relays`, unless the account is a
/// member or Refract lists it. `id` and `did` name the account (SQL
/// expressions of its `dids` id and its DID text). The relays are empty
/// outside full and under `suspended_accounts = "all"`
/// ([`crate::hydration::Hydrator::hidden_relays`]), so then only the status
/// decides. `relays` appears once, so a query builder can bind it in place.
#[must_use]
pub fn hidden_status_sql(status_hidden: &str, id: &str, did: &str, relays: &str) -> String {
    format!("({status_hidden} OR {})", relay_suspended_sql(id, did, relays))
}

/// [`hidden_status_sql`] for a statement that carries the relays as a
/// literal list (`relays_literal`, an SQL `text[]` constant) instead of a
/// bound parameter: the suspension test runs only when the list is not
/// empty, which PostgreSQL decides when it plans the statement, so with no
/// relays the read costs what its status test costs.
#[must_use]
pub fn hidden_status_sql_listed(status_hidden: &str, id: &str, did: &str, relays_literal: &str) -> String {
    format!("({status_hidden} OR (cardinality({relays_literal}) > 0 AND {}))", relay_suspended_sql(id, did, relays_literal))
}

/// A non-active status from one of `relays`, unless the account is a member
/// or Refract lists it.
fn relay_suspended_sql(id: &str, did: &str, relays: &str) -> String {
    format!(
        "EXISTS (SELECT 1 FROM relay_account_status hs WHERE hs.account = {id} AND hs.relay = ANY({relays}::text[]) \
                   AND NOT EXISTS (SELECT 1 FROM members hm WHERE hm.did = {did}) \
                   AND NOT EXISTS (SELECT 1 FROM refract_dids hr WHERE hr.did = {did} AND hr.state IN ('restored', 'grace')))"
    )
}

/// [`hidden_status_sql`] pushed onto a query builder, binding `relays`.
pub fn push_hidden_status(qb: &mut sqlx::QueryBuilder<'_, sqlx::Postgres>, status_hidden: &str, id: &str, did: &str, relays: Vec<String>) {
    const MARK: &str = "\u{1}relays\u{1}";
    let sql = hidden_status_sql(status_hidden, id, did, MARK);
    let (before, after) = sql.split_once(MARK).unwrap_or((sql.as_str(), ""));
    qb.push(before.to_string()).push_bind(relays).push(after.to_string());
}

/// [`gate_many_among`] with relay suspensions.
///
/// # Errors
///
/// Database errors.
pub async fn gate_many_suspended(
    pool: &PgPool,
    subjects: &[String],
    readable: Option<&HashSet<String>>,
    refract: RefractView<'_>,
    suspensions: Option<RelaySuspensions<'_>>,
) -> Result<HashMap<String, Gate>, sqlx::Error> {
    let mut authors: HashSet<String> = HashSet::new();
    let mut keys: HashSet<String> = HashSet::new();
    for s in subjects {
        keys.insert(s.clone());
        let did = uri_author(s).unwrap_or(s);
        if readable.is_none_or(|r| r.contains(did)) {
            authors.insert(did.to_string());
        }
        keys.insert(did.to_string());
    }
    let keys: Vec<String> = keys.into_iter().collect();
    let authors_v: Vec<String> = authors.into_iter().collect();
    let labels: Vec<(String, String)> = sqlx::query_as(
        "SELECT subject, val FROM labels WHERE subject = ANY($1) AND val IN ('!takedown', '!hide')",
    )
    .bind(&keys)
    .fetch_all(pool)
    .await?;
    let statuses: Vec<(String, String)> =
        sqlx::query_as("SELECT d.did, a.account_status FROM dids d JOIN actors a ON a.id = d.id WHERE d.did = ANY($1) AND a.account_status <> 'active'")
            .bind(&authors_v)
            .fetch_all(pool)
            .await?;
    // A relay's suspension is a public fact: read for every subject.
    let relay_suspended: HashSet<String> = match &suspensions {
        Some(r) if !r.relays.is_empty() => {
            let every: Vec<String> = subjects.iter().map(|s| uri_author(s).unwrap_or(s).to_string()).collect();
            let rows: Vec<String> = sqlx::query_scalar(
                "SELECT DISTINCT d.did FROM relay_account_status x JOIN dids d ON d.id = x.account WHERE d.did = ANY($1) AND x.relay = ANY($2)",
            )
            .bind(&every)
            .bind(r.relays)
            .fetch_all(pool)
            .await?;
            rows.into_iter().filter(|d| !(r.exempt)(d)).collect()
        }
        _ => HashSet::new(),
    };
    let takedown: HashSet<&str> = labels.iter().filter(|(_, v)| v == "!takedown").map(|(s, _)| s.as_str()).collect();
    let hide: HashSet<&str> = labels.iter().filter(|(_, v)| v == "!hide").map(|(s, _)| s.as_str()).collect();
    let status: HashMap<&str, HiddenReason> = statuses
        .iter()
        .filter_map(|(d, s)| status_reason(s).map(|r| (d.as_str(), r)))
        .collect();
    Ok(subjects
        .iter()
        .map(|s| {
            let author = uri_author(s).unwrap_or(s);
            let gate = if takedown.contains(s.as_str()) || takedown.contains(author) || refract.account_hidden(author) {
                Gate::Hidden(HiddenReason::Takedown)
            } else if let Some(r) = status.get(author) {
                Gate::Hidden(*r)
            } else if relay_suspended.contains(author) {
                Gate::Hidden(HiddenReason::Suspended)
            } else if refract.record_hidden(s) {
                Gate::Hidden(HiddenReason::RecordRemoved)
            } else if s.starts_with("at://") && hide.contains(s.as_str()) {
                Gate::Collapsed
            } else {
                Gate::Visible
            };
            (s.clone(), gate)
        })
        .collect())
}

/// The gate for one subject.
///
/// # Errors
///
/// Database errors.
pub async fn gate(pool: &PgPool, subject: &str, refract: RefractView<'_>) -> Result<Gate, sqlx::Error> {
    let s = subject.to_string();
    Ok(gate_many(pool, std::slice::from_ref(&s), refract)
        .await?
        .remove(&s)
        .unwrap_or(Gate::Visible))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::fresh_db;

    const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
    const C: &str = "did:plc:cccccccccccccccccccccccc";

    /// The SQL form of the account rule: the read's own status test, or a
    /// takedown relay's non-active status for an account that is neither a
    /// member nor Refract-listed; no relays bound, only the status decides.
    #[tokio::test]
    async fn the_sql_account_rule_hides_relay_suspensions_unless_exempt() {
        let Some(db) = fresh_db("hidden_status_sql").await else {
            return;
        };
        let p = &db.pool;
        crate::test_support::actors::put(p, C, &[("account_status", "'deactivated'")]).await;
        sqlx::raw_sql(&format!(
            "INSERT INTO dids (did) VALUES ('{A}'), ('{B}') ON CONFLICT DO NOTHING; \
             INSERT INTO relay_account_status (account, relay, status) SELECT id, 'bsky', 'takendown' FROM dids WHERE did IN ('{A}', '{B}'); \
             INSERT INTO members (did, source) VALUES ('{B}', 'admin');"
        ))
        .execute(p)
        .await
        .expect("seed");
        let sql = format!(
            "SELECT d.did FROM dids d LEFT JOIN actors a ON a.id = d.id WHERE {} ORDER BY 1",
            hidden_status_sql("COALESCE(a.account_status, 'active') <> 'active'", "d.id", "d.did", "$1")
        );
        let hidden = |relays: Vec<String>| {
            let sql = sql.clone();
            async move { sqlx::query_scalar::<_, String>(&sql).bind(relays).fetch_all(p).await.expect("query") }
        };
        assert_eq!(hidden(vec!["bsky".into()]).await, [A, C], "the member is exempt");
        assert_eq!(hidden(Vec::new()).await, [C], "no relays: the status alone");
        assert_eq!(hidden(vec!["other".into()]).await, [C], "another relay's rows hide nobody");
        sqlx::query("INSERT INTO refract_dids (did, community, state, source) VALUES ($1, 'community-a', 'restored', 'fill')")
            .bind(A)
            .execute(p)
            .await
            .expect("restored");
        assert_eq!(hidden(vec!["bsky".into()]).await, [C], "Refract's listing is exempt");
        db.drop_db().await;
    }

    /// Every public, gate-shaped SQL read tests an account's status through
    /// the one fragment: no bare `account_status` predicate outside a
    /// `hidden_status_sql` (or `push_hidden_status`) call.
    #[test]
    fn public_reads_test_status_through_the_one_fragment() {
        let files = [
            ("search", include_str!("../hydration/search.rs")),
            ("suggestions", include_str!("../hydration/suggestions.rs")),
            ("trends", include_str!("../hydration/trends.rs")),
            ("starter_packs", include_str!("../hydration/starter_packs.rs")),
            ("notifications", include_str!("../hydration/notifications.rs")),
            ("actor", include_str!("../hydration/actor.rs")),
            ("lens", include_str!("../hydration/lens.rs")),
        ];
        let mut seen = 0;
        for (name, src) in files {
            let code = src.split("#[cfg(test)]").next().unwrap_or(src);
            for (i, _) in code.match_indices("account_status") {
                let call = ["hidden_status_sql(", "push_hidden_status("].iter().filter_map(|c| code[..i].rfind(c)).max();
                let inside = call.is_some_and(|at| !code[at..i].contains(';'));
                assert!(inside, "{name}: a bare account_status predicate: {}", &code[code[..i].rfind('\n').unwrap_or(0)..i]);
                seen += 1;
            }
        }
        assert!(seen >= 6, "the reads were found ({seen})");
    }

    #[tokio::test]
    async fn labels_and_account_status() {
        let Some(db) = fresh_db("moderation_gate").await else {
            return;
        };
        let p = &db.pool;
        let post_a = format!("at://{A}/app.bsky.feed.post/1");
        let post_b = format!("at://{B}/app.bsky.feed.post/1");
        let post_c = format!("at://{C}/app.bsky.feed.post/1");
        for (s, v) in [(A, "!takedown"), (post_b.as_str(), "!hide"), (post_c.as_str(), "porn")] {
            sqlx::query("INSERT INTO labels (subject, val) VALUES ($1, $2)")
                .bind(s)
                .bind(v)
                .execute(p)
                .await
                .expect("label");
        }
        crate::test_support::actors::put(p, C, &[("account_status", "'deactivated'")]).await;
        let g = gate_many(p, &[post_a.clone(), post_b.clone(), post_c.clone(), B.to_string(), C.to_string()], RefractView::empty())
            .await
            .expect("gate");
        assert_eq!(g[&post_a], Gate::Hidden(HiddenReason::Takedown), "author takedown hides the post");
        assert_eq!(g[&post_b], Gate::Collapsed);
        assert_eq!(g[&post_c], Gate::Hidden(HiddenReason::Deactivated));
        assert_eq!(g[B], Gate::Visible);
        assert_eq!(g[C].profile_error().map(|e| e.0), Some("AccountDeactivated"));
        assert_eq!(gate(p, A, RefractView::empty()).await.expect("gate").profile_error().map(|e| e.0), Some("AccountTakedown"));
        db.drop_db().await;
    }

    /// Refract design §7.4: the snapshot hides with no query; a local
    /// takedown and account status come first; the check overrides only
    /// `records`; `RecordRemoved` is a record subject's reason.
    #[tokio::test]
    async fn refract_snapshot_hides_what_it_names() {
        use std::sync::Arc;

        use crate::aggregations::ScopeSets;
        use crate::indexing::PublicMode;
        use crate::refract::{RefractCheck, RefractHidden, RefractSnapshot};

        let Some(db) = fresh_db("moderation_gate_refract").await else {
            return;
        };
        let p = &db.pool;
        let post_a = format!("at://{A}/app.bsky.feed.post/1");
        let post_b = format!("at://{B}/app.bsky.feed.post/1");
        let like_b = format!("at://{B}/app.bsky.feed.like/1");
        let profile_b = format!("at://{B}/app.bsky.actor.profile/self");
        let post_c = format!("at://{C}/app.bsky.feed.post/1");
        crate::test_support::actors::put(p, C, &[("account_status", "'deactivated'")]).await;
        let mut sets = ScopeSets::default();
        sets.public.mode = PublicMode::Off;
        sets.scope.insert(B.into());
        let mut hidden = RefractHidden::default();
        // A: a non-member outside S, ending; B: in S, pending; C: a gone
        // record label on a deactivated account.
        hidden.update(A, |r| r.ending = true);
        hidden.update(B, |r| r.pending = true);
        hidden.add_record(&post_c, None);
        sets.refract_hidden = Arc::new(hidden);
        let snap = RefractSnapshot::of(&sets);
        let check = RefractCheck::default();
        let view = RefractView { snapshot: &snap, check: &check };
        let subjects = [post_a.clone(), A.to_string(), post_b.clone(), like_b.clone(), profile_b.clone(), B.to_string(), post_c.clone()];
        let g = gate_many(p, &subjects, view).await.expect("gate");
        assert_eq!(g[&post_a], Gate::Hidden(HiddenReason::Takedown));
        assert_eq!(g[A].profile_error().map(|e| e.0), Some("AccountTakedown"), "what Bluesky says for the same DID");
        assert_eq!(g[&post_b], Gate::Hidden(HiddenReason::RecordRemoved));
        assert_eq!(g[&like_b], Gate::Hidden(HiddenReason::RecordRemoved));
        assert_eq!(g[&profile_b], Gate::Visible, "an S DID's profile is never hidden for Refract");
        assert_eq!(g[B], Gate::Visible);
        assert_eq!(g[&post_c], Gate::Hidden(HiddenReason::Deactivated), "account status comes first");
        // The check un-hides a record label only.
        let mut check = RefractCheck::default();
        check.note(&post_c, false);
        check.note(&post_b, false);
        let view = RefractView { snapshot: &snap, check: &check };
        let g = gate_many_among(p, std::slice::from_ref(&post_b), None, view).await.expect("gate");
        assert_eq!(g[&post_b], Gate::Hidden(HiddenReason::RecordRemoved), "never over a collection rule");
        // In On mode an S DID is never hidden for Refract.
        sets.public.mode = PublicMode::On;
        let snap = RefractSnapshot::of(&sets);
        let view = RefractView { snapshot: &snap, check: &RefractCheck::default() };
        let g = gate_many(p, &[post_b.clone(), post_a.clone()], view).await.expect("gate");
        assert_eq!(g[&post_b], Gate::Visible);
        assert_eq!(g[&post_a], Gate::Hidden(HiddenReason::Takedown));
        db.drop_db().await;
    }
}
