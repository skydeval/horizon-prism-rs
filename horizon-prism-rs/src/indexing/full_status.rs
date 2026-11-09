//! Full indexing's part of `admin indexing status` and Health: the options
//! set aside at startup, the relays' copy tiers and readers, the crawl and
//! each group's history, the labelers, the suspensions, and the warnings
//! that follow from them, in plain words.

use serde_json::{Value, json};

use super::public_mode::PublicIndexing;
use super::{Breadth, crawl};

/// What full indexing shares with status beyond the switch.
#[derive(Default)]
pub struct FullBoard {
    /// The options set aside at startup, in words.
    pub(super) set_aside: Vec<String>,
    /// The crawl's progress and figures.
    pub(super) crawl: Option<crawl::status::CrawlBoard>,
}

impl PublicIndexing {
    /// With the options set aside at startup: logged once, counted, and
    /// shown on Health.
    pub fn set_full_options_aside(&self, words: Vec<String>) {
        for w in &words {
            tracing::warn!(event = "full_option_set_aside", detail = %w, "a full indexing option was set aside");
        }
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!(super::full::METRIC_SET_ASIDE).set(words.len() as f64);
        if let Ok(mut b) = self.full_board.lock() {
            b.set_aside = words;
        }
    }

    /// With the crawl's board.
    pub fn attach_crawl(&self, board: crawl::status::CrawlBoard) {
        if let Ok(mut b) = self.full_board.lock() {
            b.crawl = Some(board);
        }
    }

    /// Full indexing's status: always the options set aside; outside
    /// breadth Community also the crawl, each group's history, the
    /// labelers and the suspensions; and the warnings, in words.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn full_status(&self) -> Result<Value, sqlx::Error> {
        let (set_aside, board) = self.full_board.lock().map(|b| (b.set_aside.clone(), b.crawl.clone())).unwrap_or_default();
        let breadth = self.breadth();
        let relays: Vec<Value> = self
            .tier_board()
            .relays()
            .into_iter()
            .map(|(label, t)| json!({ "relay": label, "tier": t.tier.as_str(), "left_out": t.left_out, "stamped": t.stamped }))
            .collect();
        let mut out = json!({ "set_aside": set_aside, "relays": relays, "kinds": self.full_kinds.iter().map(|g| g.as_str()).collect::<Vec<_>>() });
        if breadth != Breadth::Community {
            out["crawl"] = board.map_or(Value::Null, |b| b.to_json());
            out["groups"] = sqlx::query_scalar::<_, Value>("SELECT value FROM worker_state WHERE key = $1")
                .bind(super::full::GROUPS_KEY)
                .fetch_optional(&self.pool)
                .await?
                .unwrap_or(Value::Null);
            out["labels"] = self.labels_status().await?;
            let suspended: Vec<(String, String, i64)> = sqlx::query_as("SELECT relay, status, count(*) FROM relay_account_status GROUP BY relay, status ORDER BY relay, status")
                .fetch_all(&self.pool)
                .await?;
            out["suspended"] = suspended.into_iter().map(|(relay, status, n)| json!({ "relay": relay, "status": status, "accounts": n })).collect();
        }
        out["warnings"] = json!(warnings(&out, breadth));
        Ok(out)
    }

    /// Each subscribed labeler: replaying or caught up, its cursor, and the
    /// subjects that carry a label of it whose signature could not be
    /// checked.
    async fn labels_status(&self) -> Result<Value, sqlx::Error> {
        let labels = self.scope.read_guard().await.full.labels.clone();
        let Some(labels) = labels else {
            return Ok(Value::Null);
        };
        let unverified: std::collections::HashMap<String, i64> = super::labels::store::unverified_counts(&self.pool).await?.into_iter().collect();
        let settings = labels.settings();
        let sources: Vec<Value> = settings
            .sources
            .iter()
            .map(|src| {
                let p = labels.progress(src);
                json!({
                    "labeler": src,
                    "replaying": p.as_ref().is_some_and(|p| p.replay && !p.caught_up),
                    "caught_up": p.as_ref().is_some_and(|p| p.caught_up),
                    "cursor": p.as_ref().and_then(|p| p.cursor),
                    "from_the_tip": p.as_ref().is_some_and(|p| !p.replay),
                    "unverified_subjects": unverified.get(src).copied().unwrap_or(0),
                })
            })
            .collect();
        Ok(json!({ "mode": if settings.sources.is_empty() { "upstream" } else { "subscribe" }, "backfill": settings.backfill, "sources": sources }))
    }

    /// `admin indexing full-complete --incomplete [group]`.
    ///
    /// # Errors
    ///
    /// Why nothing was marked, in words.
    pub async fn complete_by_hand(&self, group: Option<&str>) -> Result<Value, String> {
        let generation = {
            let sets = self.scope.read_guard().await;
            if sets.public.breadth != Breadth::Full {
                return Err("Full indexing is not running.".into());
            }
            i64::try_from(sets.public.crawl_generation).unwrap_or(i64::MAX)
        };
        match crawl::state::complete_by_hand(&self.pool, generation, group).await {
            Ok(marked) => {
                for (g, kept) in &marked {
                    tracing::info!(event = "full_group_complete_by_hand", group = %g, incomplete = kept, "a group was marked complete by hand");
                }
                Ok(json!({ "marked": marked.into_iter().map(|(g, n)| json!({ "group": g, "incomplete_accounts": n })).collect::<Vec<_>>() }))
            }
            Err(Ok(words)) => Err(words),
            Err(Err(e)) => Err(e.to_string()),
        }
    }
}

fn s(v: &Value) -> &str {
    v.as_str().unwrap_or_default()
}

/// The warnings of full's status in words: the options set aside, the
/// relays set aside or with an older reader, a group no relay passes, an
/// export set aside, local serving blocked by a gap, a paused crawl, and
/// labelers whose labels cannot all be checked.
#[must_use]
pub fn warnings(full: &Value, breadth: Breadth) -> Vec<String> {
    let mut out: Vec<String> = full["set_aside"].as_array().into_iter().flatten().map(|w| s(w).to_string()).collect();
    if breadth != Breadth::Full {
        return out;
    }
    let relays: Vec<&Value> = full["relays"].as_array().into_iter().flatten().collect();
    for r in &relays {
        if s(&r["tier"]) == "aside" {
            let left: Vec<&str> = r["left_out"].as_array().into_iter().flatten().map(s).collect();
            out.push(if left.is_empty() {
                format!("Relay {} is set aside for full indexing: its reader was replaced by one that stamps nothing after running a filter. List it in firehose.old_filtered_relays, or run `admin firehose forget-cap {}` if its filter is gone.", s(&r["relay"]), s(&r["relay"]))
            } else {
                format!("Relay {} is set aside for full indexing: its filter leaves out {}.", s(&r["relay"]), left.join(", "))
            });
        } else if r["stamped"] == json!(false) {
            out.push(format!(
                "Relay {}'s reader is older and sends no capability stamp: each account's continuity is reduced, and the periodic re-list is what finds missed commits. A newer relay reader gives it back.",
                s(&r["relay"])
            ));
        }
    }
    // A group none of the relays passes: no account's copies of it count.
    if !relays.is_empty() {
        for g in full["kinds"].as_array().into_iter().flatten().map(s) {
            let Some(group) = crate::config::FullGroup::parse(g) else { continue };
            let cols = super::full::collections(group);
            let left_by_all = relays.iter().all(|r| s(&r["tier"]) == "aside" && r["left_out"].as_array().is_some_and(|l| l.iter().any(|c| cols.contains(&s(c)))));
            if left_by_all {
                out.push(format!("No relay passes the {g} group: every relay's filter leaves part of it out, so full indexing reads it from today's sources."));
            }
        }
    }
    let crawl = &full["crawl"];
    if let Some(why) = crawl.pointer("/export/set_aside").and_then(Value::as_str) {
        out.push(format!("full_crawl.identity_source = \"plc_export\" is set aside ({why}): the crawl reads each account's directory entry instead."));
    }
    if crawl["paused"] == json!(true) {
        out.push("The history crawl is paused: the database volume's free space is under full.pause_free_percent. Live ingest goes on.".to_string());
    }
    let figures = &crawl["figures"];
    for g in figures["open_gaps"].as_array().into_iter().flatten().filter(|g| g["blocking"] == json!(true)) {
        let relay = g["relay"].as_str().map(|r| format!(" on relay {r}")).unwrap_or_default();
        out.push(format!(
            "Full local serving is blocked: a {} gap{relay} is open since {}. Interaction data and accounts outside S read today's sources until its re-list ends.",
            s(&g["kind"]),
            s(&g["opened_at"])
        ));
    }
    if let Some(share) = figures["blocked_share_7d"].as_f64().filter(|x| *x > 0.0) {
        out.push(format!("Full local serving was blocked {:.0}% of the last 7 days.", (share * 100.0).max(1.0)));
    }
    for l in full.pointer("/labels/sources").and_then(Value::as_array).into_iter().flatten() {
        let n = l["unverified_subjects"].as_i64().unwrap_or(0);
        if n > 0 {
            out.push(format!(
                "{n} subjects carry a label of {} whose signature could not be checked: those labels are not applied, and those subjects read the labeler's own answer until it is compared.",
                s(&l["labeler"])
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The warnings name each set-aside option, each relay set aside or
    /// with an older reader, a group no relay passes, a set-aside export, a
    /// paused crawl, an open blocking gap, the blocked share and unverified
    /// labels; outside full only the options.
    #[test]
    fn full_warnings_in_words() {
        let full = json!({
            "set_aside": ["full.labels = \"subscribe\" with no label_sources: each labeler's labels are read per request instead."],
            "kinds": ["posts", "likes"],
            "relays": [
                {"relay": "bsky", "tier": "aside", "left_out": ["app.bsky.feed.like"], "stamped": true},
                {"relay": "other", "tier": "aside", "left_out": ["app.bsky.feed.like"], "stamped": true},
            ],
            "crawl": {"paused": true, "export": {"active": false, "set_aside": "export: HTTP 404"},
                      "figures": {"open_gaps": [{"kind": "trim", "relay": "bsky", "blocking": true, "opened_at": "2026-11-09T01:00:00Z"}, {"kind": "periodic", "blocking": false, "opened_at": "x"}], "blocked_share_7d": 0.25}},
            "labels": {"sources": [{"labeler": "did:plc:example1", "unverified_subjects": 3}]},
        });
        let w = warnings(&full, Breadth::Full).join(" | ");
        for needle in [
            "read per request instead",
            "Relay bsky is set aside for full indexing: its filter leaves out app.bsky.feed.like",
            "No relay passes the likes group",
            "is set aside (export: HTTP 404)",
            "The history crawl is paused",
            "a trim gap on relay bsky is open since 2026-11-09T01:00:00Z",
            "blocked 25% of the last 7 days",
            "3 subjects carry a label of did:plc:example1",
        ] {
            assert!(w.contains(needle), "{needle}: {w}");
        }
        assert!(!w.contains("No relay passes the posts group") && !w.contains("periodic gap"));
        let older = json!({"relays": [{"relay": "bsky", "tier": "whole", "left_out": [], "stamped": false}], "kinds": ["posts"]});
        assert!(warnings(&older, Breadth::Full)[0].contains("Relay bsky's reader is older"));
        assert_eq!(warnings(&full, Breadth::Community).len(), 1, "outside full only the options");
    }
}
