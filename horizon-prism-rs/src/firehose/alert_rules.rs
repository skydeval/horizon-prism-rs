//! The consumer's throughput alerts (firehose throughput design §4.3), kept
//! identical to the rules `docker/README.md` ships under "Metrics and alert
//! examples". Both tests below hold them to that.

/// One Prometheus alerting rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rule {
    pub alert: &'static str,
    pub expr: &'static str,
    pub for_: &'static str,
}

/// Backlog capacity: entries per busy second in full, gap-free steps.
/// Defined only over intervals with a backlog.
pub const BACKLOG_CAPACITY: &str =
    "rate(firehose_entries_processed_backlog_total[5m]) / rate(firehose_consumer_backlog_busy_seconds_total[5m])";
/// The share of time spent on full, gap-free steps.
pub const BACKLOG_FRACTION: &str = "rate(firehose_consumer_backlog_busy_seconds_total[5m])";
/// What the worker processed, every path.
pub const PROCESSED_RATE: &str = "sum without (path) (rate(firehose_entries_processed_total[5m]))";
/// What the stream gained.
pub const INPUT_RATE: &str = "rate(firehose_stream_entries_added_total[5m])";
/// The share of the lag sampler's calls that failed or timed out.
pub const SAMPLER_FAILURE_SHARE: &str = "sum without (call) (rate(firehose_lag_sample_failures_total[5m])) / sum without (call) (rate(firehose_lag_sample_calls_total[5m]))";

/// The input is at or near capacity, or the worker is recovering from a
/// long stall: no margin for a burst. The same condition as
/// `firehose_headroom_warn`.
pub const BACKLOG_PERSISTENT: Rule = Rule {
    alert: "FirehoseBacklogPersistent",
    expr: "rate(firehose_consumer_backlog_busy_seconds_total[5m]) > 0.5",
    for_: "15m",
};
/// Processed below 0.95 × input; also a dead worker.
pub const SLOWER_THAN_INPUT: Rule = Rule {
    alert: "FirehoseConsumerSlowerThanInput",
    expr: "sum without (path) (rate(firehose_entries_processed_total[5m])) < 0.95 * rate(firehose_stream_entries_added_total[5m])",
    for_: "5m",
};
/// The group is still missing after the worker should have recreated it.
pub const GROUP_MISSING: Rule = Rule {
    alert: "FirehoseGroupMissing",
    expr: "firehose_consumer_group_present == 0",
    for_: "2m",
};
/// Redis unreachable, hung or stuck: more than half the sampler's calls
/// failed or timed out.
pub const SAMPLER_FAILING: Rule = Rule {
    alert: "FirehoseSamplerFailing",
    expr: "sum without (call) (rate(firehose_lag_sample_failures_total[5m])) / sum without (call) (rate(firehose_lag_sample_calls_total[5m])) > 0.5",
    for_: "5m",
};

pub const RULES: [Rule; 4] = [BACKLOG_PERSISTENT, SLOWER_THAN_INPUT, GROUP_MISSING, SAMPLER_FAILING];

#[cfg(test)]
mod tests {
    use super::*;

    const LABELLED: [(&str, &str); 3] = [
        ("firehose_entries_processed_total", "sum without (path)"),
        ("firehose_lag_sample_failures_total", "sum without (call)"),
        ("firehose_lag_sample_calls_total", "sum without (call)"),
    ];

    /// Every `rate(<name>…)` in `expr` sits directly inside `wrap (…)`.
    fn every_rate_wrapped(expr: &str, name: &str, wrap: &str) -> bool {
        let needle = format!("rate({name}");
        let wrapped = format!("{wrap} ({needle}");
        expr.match_indices(&needle).all(|(i, _)| expr[..i + needle.len()].ends_with(&wrapped))
    }

    #[test]
    fn rules_are_well_formed() {
        let exprs: Vec<&str> = RULES
            .iter()
            .map(|r| r.expr)
            .chain([BACKLOG_CAPACITY, BACKLOG_FRACTION, PROCESSED_RATE, INPUT_RATE, SAMPLER_FAILURE_SHARE])
            .collect();
        for expr in &exprs {
            for (name, wrap) in LABELLED {
                if expr.contains(name) {
                    assert!(every_rate_wrapped(expr, name, wrap), "{expr}: {name} outside `{wrap}`");
                }
            }
        }
        // These two name unlabelled series only.
        for rule in [BACKLOG_PERSISTENT, GROUP_MISSING] {
            assert!(!rule.expr.contains('{') && !rule.expr.contains("without"), "{}", rule.expr);
            for (name, _) in LABELLED {
                assert!(!rule.expr.contains(name), "{}", rule.expr);
            }
        }
        // The recipes are the rules' own terms.
        assert_eq!(BACKLOG_PERSISTENT.expr, format!("{BACKLOG_FRACTION} > 0.5"));
        assert_eq!(SLOWER_THAN_INPUT.expr, format!("{PROCESSED_RATE} < 0.95 * {INPUT_RATE}"));
        assert_eq!(SAMPLER_FAILING.expr, format!("{SAMPLER_FAILURE_SHARE} > 0.5"));
        for rule in RULES {
            assert!(!rule.alert.is_empty() && !rule.for_.is_empty());
        }
    }

    /// `docker/README.md` sits beside the crate; a build tree with the crate
    /// alone skips this, unless `HPRS_REQUIRE_REPO_TEXT=1`.
    #[test]
    fn readme_carries_the_rules() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../docker/README.md");
        let Ok(text) = std::fs::read_to_string(&path) else {
            assert!(
                std::env::var("HPRS_REQUIRE_REPO_TEXT").as_deref() != Ok("1"),
                "{} not found and HPRS_REQUIRE_REPO_TEXT=1",
                path.display()
            );
            eprintln!("{} not found; skipping the README rules check", path.display());
            return;
        };
        let lines: Vec<&str> = text.lines().map(str::trim).collect();
        for rule in RULES {
            let alert = format!("- alert: {}", rule.alert);
            let expr = format!("expr: {}", rule.expr);
            let for_ = format!("for: {}", rule.for_);
            let at = lines.iter().position(|l| *l == alert).unwrap_or_else(|| panic!("README lacks `{alert}`"));
            assert_eq!(lines.get(at + 1), Some(&expr.as_str()), "README's {} expr", rule.alert);
            assert_eq!(lines.get(at + 2), Some(&for_.as_str()), "README's {} for", rule.alert);
        }
        println!("README rules verified");
    }
}
