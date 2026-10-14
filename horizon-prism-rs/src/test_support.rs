//! Shared helpers for Postgres-backed tests. They run when
//! `HPRS_TEST_DATABASE_URL` names a server the tests may create databases on;
//! otherwise [`fresh_db`] returns `None` and the test passes without doing
//! anything.

use sqlx::PgPool;

pub const ENV: &str = "HPRS_TEST_DATABASE_URL";

/// A migrated, uniquely named database, dropped by [`TestDb::drop_db`].
pub struct TestDb {
    admin: PgPool,
    pub pool: PgPool,
    name: String,
}

impl TestDb {
    pub async fn drop_db(self) {
        self.pool.close().await;
        let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name))
            .execute(&self.admin)
            .await;
    }
}

/// Create and migrate a fresh database, or `None` when the env var is unset.
pub async fn fresh_db(test: &str) -> Option<TestDb> {
    let db = empty_db(test).await?;
    crate::db::migrations::MIGRATOR
        .run(&db.pool)
        .await
        .expect("apply migrations");
    Some(db)
}

/// Serve `router` on an ephemeral loopback port (fake upstreams), returning
/// its `http://127.0.0.1:<port>` base URL. Use with
/// `NetClient::loopback_for_tests`.
pub async fn serve_fake(router: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("fake upstream");
    });
    format!("http://{addr}")
}

/// In a Prometheus rendering, the value of the series `name` whose labels
/// include every one of `labels` (each `key="value"`), if there is one.
/// Read metrics with a recorder set by `metrics::set_default_local_recorder`
/// in a `#[tokio::test]`, whose runtime runs on the test's thread.
#[must_use]
pub fn series(text: &str, name: &str, labels: &[&str]) -> Option<f64> {
    text.lines()
        .filter(|l| l.strip_prefix(name).is_some_and(|rest| rest.starts_with('{')))
        .find(|l| labels.iter().all(|label| l.contains(label)))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
}

/// A fresh database with no migrations applied (migration tests).
pub async fn empty_db(test: &str) -> Option<TestDb> {
    let Ok(admin_url) = std::env::var(ENV) else {
        eprintln!("{ENV} unset; skipping Postgres-backed test {test}");
        return None;
    };
    let admin = PgPool::connect(&admin_url).await.expect("connect admin database");
    let name = format!("hprs_test_{}", rand::random::<u32>());
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .expect("create test database");
    let base = admin_url.rsplit_once('/').expect("URL with a database path").0;
    let pool = PgPool::connect(&format!("{base}/{name}"))
        .await
        .expect("connect test database");
    Some(TestDb { admin, pool, name })
}
