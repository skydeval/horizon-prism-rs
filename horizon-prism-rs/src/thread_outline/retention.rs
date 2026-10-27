//! The outline's retention task (thread outline design §3.5): daily, in
//! composite-key batches, rows seen longer ago than `window_days`.

use async_trait::async_trait;
use sqlx::PgPool;

use crate::db::retention::{Bind, RetentionReport, RetentionSettings, RetentionTask, ScopeGuard, Step, batched_composite, run_steps};

/// The `thread_outline` retention task.
pub struct OutlineRetention {
    window_days: u32,
}

impl OutlineRetention {
    #[must_use]
    pub fn new(window_days: u32) -> Self {
        Self { window_days }
    }

    /// Its steps: posts, deletes of unseen posts and holes, by `seen_at`.
    pub(crate) fn steps(&self) -> Vec<Step> {
        let days = Bind::Days(i32::try_from(self.window_days).unwrap_or(i32::MAX));
        let pred = |alias: &str| format!("{alias}.seen_at < now() - make_interval(days => $1)");
        vec![
            batched_composite("outline_posts", &["author", "rkey"], "op", &pred("op"), days.clone()),
            batched_composite("outline_deleted", &["author", "rkey"], "od", &pred("od"), days.clone()),
            batched_composite("outline_holes", &["author", "rkey"], "oh", &pred("oh"), days),
        ]
    }
}

#[async_trait]
impl RetentionTask for OutlineRetention {
    fn name(&self) -> &'static str {
        "thread_outline"
    }

    /// No row here depends on scope; the batches still take the guard, as
    /// every retention batch does.
    async fn run(&self, pool: &PgPool, scope: &dyn ScopeGuard, _settings: &RetentionSettings) -> Result<RetentionReport, sqlx::Error> {
        run_steps(pool, scope, &self.steps()).await
    }
}
