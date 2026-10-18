//! Rule 4's gauges (design-v3 §14.2, §4.7.6, §4.3 "List purpose").
//!
//! Counts read from the rows, exported by the storage sampler every 300 s,
//! beside `members_total`, from which the storage bound is computed. Each
//! `{source}` gauge is set for both sources on every sample, 0 when no row
//! has it, so a gauge never keeps a stale value.

use sqlx::PgPool;

use crate::hydration::lists::PURPOSE_MOD;

/// `|L_sub|`: the subscribed lists, those `active`.
pub const METRIC_SUBSCRIPTIONS: &str = "list_subscriptions_total";
/// members × 2 × `subscribed_max_per_member` × `subscribed_max_items` (§4.7.6).
pub const METRIC_ROWS_BOUND: &str = "list_subscription_rows_bound";
/// References past their member's budget, by source.
pub const METRIC_UNADMITTED: &str = "list_references_unadmitted";
/// Change markers the list phase has not applied yet.
pub const METRIC_CHANGES_PENDING: &str = "list_reference_changes_pending";
/// References that take effect on a list that is not a moderation list.
pub const METRIC_DORMANT: &str = "list_references_dormant";
/// Active lists whose record a complete listing lacked, purpose still stored.
pub const METRIC_RECORDS_UNLISTED: &str = "list_records_unlisted";
/// Waiting references a completion found with their list record unlisted.
pub const METRIC_REFERENCES_UNLISTED: &str = "list_references_unlisted";
/// Owners on the slow lane: `slow_until > now()` (R3-193).
pub const METRIC_SLOW_OWNERS: &str = "list_sync_slow_owners";
/// Owners backing off after a cut: `cut_until > now()` (R3-202).
pub const METRIC_CUT_OWNERS: &str = "list_sync_cut_owners";
/// Owners in `O_sub` whose last complete listing is more than 8 days old
/// (§4.4 item 5; R3-229).
pub const METRIC_WEEKLY_OVERDUE: &str = "list_sync_weekly_overdue";
/// How old a last completion is before the weekly run counts as overdue.
pub const WEEKLY_OVERDUE_DAYS: i32 = 8;

/// The two sources of a reference, in the order the gauges are set.
pub const SOURCES: [&str; 2] = ["block", "mute"];

/// The worst-case `subscribed_list_items` rows (§4.7.6): every member fills
/// both sources with references to distinct lists, each at the item cap.
#[must_use]
pub fn rows_bound(members: u64, max_per_member: u32, max_items: u32) -> u64 {
    members
        .saturating_mul(2)
        .saturating_mul(u64::from(max_per_member))
        .saturating_mul(u64::from(max_items))
}

/// Export the bound and log it (`event = "list_subscription_rows_bound"`).
/// Called once at startup; the sampler sets the gauge after that.
pub fn log_rows_bound(members: u64, max_per_member: u32, max_items: u32) {
    let bound = rows_bound(members, max_per_member, max_items);
    metrics::gauge!(METRIC_ROWS_BOUND).set(bound as f64);
    tracing::info!(
        event = "list_subscription_rows_bound",
        members,
        subscribed_max_per_member = max_per_member,
        subscribed_max_items = max_items,
        rows = bound,
        "worst-case subscribed_list_items rows for the current member count",
    );
}

/// One sample of the row counts, the `{source}` ones in [`SOURCES`] order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListSample {
    pub subscriptions: i64,
    pub unadmitted: [i64; 2],
    pub changes_pending: i64,
    pub dormant: [i64; 2],
    pub records_unlisted: i64,
    pub references_unlisted: [i64; 2],
    pub slow_owners: i64,
    pub cut_owners: i64,
    pub weekly_overdue: i64,
}

/// Place `(source, count)` rows into [`SOURCES`] order; absent ones are 0.
fn by_source(rows: Vec<(String, i64)>) -> [i64; 2] {
    let mut out = [0; 2];
    for (source, n) in rows {
        if let Some(i) = SOURCES.iter().position(|s| *s == source) {
            out[i] = n;
        }
    }
    out
}

/// `list_subscriptions_total`: the `active` lists (`subscribed_lists_state`).
///
/// # Errors
///
/// Database errors.
pub async fn subscriptions(pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT count(*) FROM subscribed_lists WHERE state = 'active'")
        .fetch_one(pool)
        .await
}

/// `list_references_unadmitted{source}`. No index has `NOT admitted`, so
/// this reads the table; it holds at most one row per member reference
/// (§4.7.6).
///
/// # Errors
///
/// Database errors.
pub async fn unadmitted(pool: &PgPool) -> Result<[i64; 2], sqlx::Error> {
    let rows = sqlx::query_as("SELECT source, count(*) FROM list_references WHERE NOT admitted GROUP BY source")
        .fetch_all(pool)
        .await?;
    Ok(by_source(rows))
}

/// `list_reference_changes_pending`: markers not yet drained.
///
/// # Errors
///
/// Database errors.
pub async fn changes_pending(pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT count(*) FROM list_reference_changes")
        .fetch_one(pool)
        .await
}

/// `list_references_dormant{source}`: references that take effect (admitted,
/// effective, list `active`) on a list whose stored purpose is not
/// `modlist`, a NULL purpose included (§4.3 "List purpose").
///
/// # Errors
///
/// Database errors.
pub async fn dormant(pool: &PgPool) -> Result<[i64; 2], sqlx::Error> {
    let rows = sqlx::query_as(
        "SELECT r.source, count(*) FROM list_references r JOIN subscribed_lists l ON l.list_uri = r.list_uri \
         WHERE r.admitted AND r.effective AND l.state = 'active' AND l.purpose IS DISTINCT FROM $1 \
         GROUP BY r.source",
    )
    .bind(PURPOSE_MOD)
    .fetch_all(pool)
    .await?;
    Ok(by_source(rows))
}

/// `list_records_unlisted`: active lists with `unlisted_since` set and a
/// stored purpose.
///
/// # Errors
///
/// Database errors.
pub async fn records_unlisted(pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT count(*) FROM subscribed_lists \
         WHERE state = 'active' AND unlisted_since IS NOT NULL AND purpose IS NOT NULL",
    )
    .fetch_one(pool)
    .await
}

/// `list_references_unlisted{source}`: waiting references (admitted, not
/// effective, list `active`, as the claim reads them) with `unlisted_at` set.
///
/// # Errors
///
/// Database errors.
pub async fn references_unlisted(pool: &PgPool) -> Result<[i64; 2], sqlx::Error> {
    let rows = sqlx::query_as(
        "SELECT r.source, count(*) FROM list_references r JOIN subscribed_lists l ON l.list_uri = r.list_uri \
         WHERE l.state = 'active' AND r.admitted AND NOT r.effective AND r.unlisted_at IS NOT NULL \
         GROUP BY r.source",
    )
    .fetch_all(pool)
    .await?;
    Ok(by_source(rows))
}

/// `list_sync_slow_owners`, `list_sync_cut_owners` and
/// `list_sync_weekly_overdue`, from `subscribed_owners`. The overdue count
/// is of owners with a subscribed list (`O_sub`) whose `last_completed_at`
/// is more than [`WEEKLY_OVERDUE_DAYS`] old; an owner that never completed
/// has no age and is not counted.
///
/// # Errors
///
/// Database errors.
pub async fn owners(pool: &PgPool) -> Result<(i64, i64, i64), sqlx::Error> {
    sqlx::query_as(
        "SELECT count(*) FILTER (WHERE o.slow_until > now()), count(*) FILTER (WHERE o.cut_until > now()), \
           count(*) FILTER (WHERE o.last_completed_at < now() - make_interval(days => $1) \
                              AND EXISTS (SELECT 1 FROM subscribed_lists l WHERE l.owner_did = o.owner_did)) \
         FROM subscribed_owners o",
    )
    .bind(WEEKLY_OVERDUE_DAYS)
    .fetch_one(pool)
    .await
}

/// Read every count.
///
/// # Errors
///
/// The first database error.
pub async fn sample(pool: &PgPool) -> Result<ListSample, sqlx::Error> {
    let (slow_owners, cut_owners, weekly_overdue) = owners(pool).await?;
    Ok(ListSample {
        subscriptions: subscriptions(pool).await?,
        unadmitted: unadmitted(pool).await?,
        changes_pending: changes_pending(pool).await?,
        dormant: dormant(pool).await?,
        records_unlisted: records_unlisted(pool).await?,
        references_unlisted: references_unlisted(pool).await?,
        slow_owners,
        cut_owners,
        weekly_overdue,
    })
}

/// Export a sample.
pub fn record(sample: &ListSample) {
    metrics::gauge!(METRIC_SUBSCRIPTIONS).set(sample.subscriptions as f64);
    metrics::gauge!(METRIC_CHANGES_PENDING).set(sample.changes_pending as f64);
    metrics::gauge!(METRIC_RECORDS_UNLISTED).set(sample.records_unlisted as f64);
    metrics::gauge!(METRIC_SLOW_OWNERS).set(sample.slow_owners as f64);
    metrics::gauge!(METRIC_CUT_OWNERS).set(sample.cut_owners as f64);
    metrics::gauge!(METRIC_WEEKLY_OVERDUE).set(sample.weekly_overdue as f64);
    for (i, source) in SOURCES.into_iter().enumerate() {
        metrics::gauge!(METRIC_UNADMITTED, "source" => source).set(sample.unadmitted[i] as f64);
        metrics::gauge!(METRIC_DORMANT, "source" => source).set(sample.dormant[i] as f64);
        metrics::gauge!(METRIC_REFERENCES_UNLISTED, "source" => source).set(sample.references_unlisted[i] as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::fresh_db;

    const OWNER: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";

    fn list(n: u8) -> String {
        format!("at://{OWNER}/app.bsky.graph.list/3klist{n}")
    }

    fn member(n: u8) -> String {
        format!("did:plc:mmmmmmmmmmmmmmmmmmmmmmm{n}")
    }

    #[test]
    fn the_bound_is_members_times_both_budgets() {
        // §4.7.6: 1000 members at the defaults is 2.5 × 10⁹ rows.
        assert_eq!(rows_bound(1000, 25, 50_000), 2_500_000_000);
        assert_eq!(rows_bound(1, 25, 50_000), 2_500_000);
        assert_eq!(rows_bound(0, 25, 50_000), 0);
        assert_eq!(rows_bound(u64::MAX, 2, 2), u64::MAX);
    }

    #[test]
    fn metric_names_match_design() {
        assert_eq!(
            [
                METRIC_SUBSCRIPTIONS,
                METRIC_ROWS_BOUND,
                METRIC_UNADMITTED,
                METRIC_CHANGES_PENDING,
                METRIC_DORMANT,
                METRIC_RECORDS_UNLISTED,
                METRIC_REFERENCES_UNLISTED,
            ],
            [
                "list_subscriptions_total",
                "list_subscription_rows_bound",
                "list_references_unadmitted",
                "list_reference_changes_pending",
                "list_references_dormant",
                "list_records_unlisted",
                "list_references_unlisted",
            ]
        );
    }

    /// Each count reads only the rows its definition names.
    #[tokio::test]
    async fn the_counts_follow_the_rows() {
        let Some(db) = fresh_db("subscriptions_gauges").await else {
            return;
        };
        let p = &db.pool;
        assert_eq!(sample(p).await.expect("db"), ListSample::default());

        let exec = |sql: String| async move { sqlx::query(&sql).execute(p).await.expect("seed") };
        // Lists: 1 modlist, 2 curatelist, 3 no purpose yet, 4 unlisted with
        // a purpose, 5 unlisted with none, 6 unreferenced (being released).
        let rows = [
            (1, "'active'", "'app.bsky.graph.defs#modlist'", "NULL"),
            (2, "'active'", "'app.bsky.graph.defs#curatelist'", "NULL"),
            (3, "'active'", "NULL", "NULL"),
            (4, "'active'", "'app.bsky.graph.defs#modlist'", "now()"),
            (5, "'active'", "NULL", "now()"),
            (6, "'unreferenced'", "'app.bsky.graph.defs#curatelist'", "now()"),
        ];
        for (n, state, purpose, since) in rows {
            exec(format!(
                "INSERT INTO subscribed_lists (list_uri, owner_did, state, purpose, unlisted_since) \
                 VALUES ('{}', '{OWNER}', {state}, {purpose}, {since})",
                list(n)
            ))
            .await;
        }
        // References: (member, list, source, admitted, effective, unlisted_at).
        let refs = [
            (1, 1, "block", true, true, false),  // applies
            (1, 2, "block", true, true, false),  // dormant: curatelist
            (2, 2, "mute", true, true, false),   // dormant: curatelist
            (2, 3, "mute", true, true, false),   // dormant: no purpose
            (3, 3, "block", true, false, false), // waiting
            (3, 4, "block", true, false, true),  // waiting, unlisted
            (3, 5, "mute", true, false, true),   // waiting, unlisted
            (4, 1, "block", false, false, false), // unadmitted
            (4, 2, "block", false, false, false), // unadmitted
            (4, 3, "mute", false, false, false), // unadmitted
            (5, 6, "mute", true, true, false),   // on an unreferenced list: not dormant
            (5, 6, "block", true, false, true),  // on an unreferenced list: not waiting
        ];
        for (m, l, source, admitted, effective, unlisted) in refs {
            let unlisted_at = if unlisted { "now()" } else { "NULL" };
            exec(format!(
                "INSERT INTO list_references (member_did, list_uri, source, admitted, effective, unlisted_at) \
                 VALUES ('{}', '{}', '{source}', {admitted}, {effective}, {unlisted_at})",
                member(m),
                list(l)
            ))
            .await;
        }
        for n in [1, 1, 3] {
            exec(format!("INSERT INTO list_reference_changes (list_uri) VALUES ('{}')", list(n))).await;
        }

        assert_eq!(subscriptions(p).await.expect("db"), 5);
        assert_eq!(unadmitted(p).await.expect("db"), [2, 1]);
        assert_eq!(changes_pending(p).await.expect("db"), 3);
        assert_eq!(dormant(p).await.expect("db"), [1, 2]);
        assert_eq!(records_unlisted(p).await.expect("db"), 1);
        assert_eq!(references_unlisted(p).await.expect("db"), [1, 1]);
        assert_eq!(
            sample(p).await.expect("db"),
            ListSample {
                subscriptions: 5,
                unadmitted: [2, 1],
                changes_pending: 3,
                dormant: [1, 2],
                records_unlisted: 1,
                references_unlisted: [1, 1],
                slow_owners: 0,
                cut_owners: 0,
                weekly_overdue: 0,
            }
        );

        // Owners: slow, cut, both, expired, and the weekly age. OWNER holds
        // the lists above; OTHER holds none, so it is never overdue.
        const OTHER: &str = "did:plc:oooooooooooooooooooooooo";
        let owner_rows = [
            (OWNER, "now() + interval '1 day'", "NULL", "now() - interval '9 days'"),
            (OTHER, "now() + interval '1 day'", "now() + interval '1 day'", "now() - interval '9 days'"),
            ("did:plc:pppppppppppppppppppppppp", "now() - interval '1 day'", "now() + interval '2 days'", "NULL"),
        ];
        for (did, slow, cut, completed) in owner_rows {
            exec(format!(
                "INSERT INTO subscribed_owners (owner_did, slow_until, cut_until, last_completed_at) VALUES ('{did}', {slow}, {cut}, {completed})"
            ))
            .await;
        }
        assert_eq!(owners(p).await.expect("db"), (2, 2, 1));
        // Exported, for the sampler's next pass.
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        record(&sample(p).await.expect("db"));
        let text = handle.render();
        for (name, n) in [(METRIC_SLOW_OWNERS, 2.0), (METRIC_CUT_OWNERS, 2.0), (METRIC_WEEKLY_OVERDUE, 1.0)] {
            assert_eq!(crate::test_support::series(&text, name, &[]), Some(n), "{name}:\n{text}");
        }
        exec(format!("UPDATE subscribed_owners SET last_completed_at = now() WHERE owner_did = '{OWNER}'")).await;
        assert_eq!(owners(p).await.expect("db").2, 0);
        db.drop_db().await;
    }
}
