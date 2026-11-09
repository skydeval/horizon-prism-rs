//! The label subscriber: one `com.atproto.label.subscribeLabels` connection
//! per subscribed labeler while breadth is Full, each with its cursor
//! persisted beside the labels it stored.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures::StreamExt;
use proto_blue::lex_data::LexValue;
use sqlx::PgPool;
use tokio_tungstenite::tungstenite::Message;

use super::history::KeyHistory;
use super::store::{self, Row};
use super::{ExternalLabels, METRIC_LAG, METRIC_REJECTED, METRIC_STORED, Progress};
use crate::aggregations::ScopeState;
use crate::config::FullLabels;
use crate::indexing::Breadth;
use crate::net::NetClient;
use crate::refract::labels::{Label, Labelers, verify};

/// A replay counts as caught up once a frame's newest label was issued
/// within this long of now, or once no frame came for [`IDLE`].
pub const TIP: Duration = Duration::from_secs(120);
/// No frame for this long: a replay is at the tip.
pub const IDLE: Duration = Duration::from_secs(30);
/// No frame for this long: the connection is dialled again.
pub const SILENT: Duration = Duration::from_secs(600);
/// Labels stored in one transaction at most while replaying.
pub const BATCH: usize = 500;
/// How long a replay waits for more frames before it stores a batch.
pub const FLUSH: Duration = Duration::from_millis(250);
/// The current key is read again at most this often after a live label
/// fails its signature.
pub const KEY_REREAD: Duration = Duration::from_secs(60);

/// What a session ended with.
#[derive(Debug)]
enum End {
    /// Full was left, or the labels stopped being wanted.
    Stopped,
    /// The connection closed or failed; dial again.
    Again(String),
}

/// Follows every subscribed labeler while breadth is Full.
pub struct Subscriber {
    pool: PgPool,
    scope: ScopeState,
    labels: Arc<ExternalLabels>,
    labelers: Labelers,
    net: NetClient,
    plc_url: String,
    tls: Option<Arc<rustls::ClientConfig>>,
}

/// The TLS of every labeler connection: ring and the webpki roots, as the
/// HTTP clients use.
fn tls() -> Option<Arc<rustls::ClientConfig>> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let cfg = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .ok()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Some(Arc::new(cfg))
}

/// The `subscribeLabels` URL of a labeler endpoint, from `cursor` when one
/// is given. A plain `http` endpoint is followed only in tests.
#[must_use]
pub fn stream_url(endpoint: &str, cursor: Option<i64>) -> Option<String> {
    let base = endpoint.trim_end_matches('/');
    let ws = if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if cfg!(test)
        && let Some(rest) = base.strip_prefix("http://")
    {
        format!("ws://{rest}")
    } else {
        return None;
    };
    Some(match cursor {
        Some(c) => format!("{ws}/xrpc/com.atproto.label.subscribeLabels?cursor={c}"),
        None => format!("{ws}/xrpc/com.atproto.label.subscribeLabels"),
    })
}

/// A `#labels` frame's `seq` and labels, as JSON.
#[must_use]
pub fn labels_frame(body: &LexValue) -> Option<(i64, Vec<serde_json::Value>)> {
    let map: &BTreeMap<String, LexValue> = body.as_map()?;
    let seq = match map.get("seq") {
        Some(LexValue::Integer(n)) => *n,
        _ => return None,
    };
    let labels = match map.get("labels") {
        Some(LexValue::Array(ls)) => ls.iter().map(proto_blue::lex_json::lex_to_json).collect(),
        _ => Vec::new(),
    };
    Some((seq, labels))
}

/// The keys a session checks labels with.
#[derive(Default)]
struct Keys {
    current: Option<String>,
    reread_at: Option<Instant>,
    /// The directory log, read once per session when a replayed label first
    /// fails.
    history: Option<Option<KeyHistory>>,
}

impl Subscriber {
    #[must_use]
    pub fn new(pool: PgPool, scope: ScopeState, labels: Arc<ExternalLabels>, labelers: Labelers, net: NetClient, plc_url: &str) -> Self {
        Self { pool, scope, labels, labelers, net, plc_url: plc_url.to_string(), tls: tls() }
    }

    #[must_use]
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { Arc::new(self).run().await })
    }

    /// Whether labels are followed now, and if so whether a new
    /// subscription replays history: breadth Full outside the off rule, and
    /// not a trial that runs live only.
    async fn wanted(&self) -> Option<bool> {
        let settings = self.labels.settings();
        if settings.mode != FullLabels::Subscribe || settings.sources.is_empty() {
            return None;
        }
        let sets = self.scope.read_guard().await;
        if sets.public.breadth != Breadth::Full || sets.public.mode.off_rule() {
            return None;
        }
        let live_only = sets.trial.as_ref().is_some_and(|t| t.state == crate::indexing::trial::TrialState::Running && !t.history);
        Some(settings.backfill && !live_only)
    }

    async fn run(self: Arc<Self>) {
        let mut running: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            running.retain(|t| !t.is_finished());
            match (self.wanted().await, running.is_empty()) {
                (Some(replay), true) => {
                    if let Ok(stored) = store::load_progress(&self.pool).await {
                        for (did, p) in stored {
                            self.labels.set_progress(&did, p);
                        }
                    }
                    for did in self.labels.settings().sources.clone() {
                        let me = Arc::clone(&self);
                        running.push(tokio::spawn(async move { me.follow(did, replay).await }));
                    }
                    tracing::info!(event = "full_labels_started", labelers = running.len(), replay, "following the subscribed labelers");
                }
                (None, idle) => {
                    for t in running.drain(..) {
                        t.abort();
                    }
                    self.labels.clear_progress();
                    if !idle {
                        tracing::info!(event = "full_labels_stopped", "the label subscriptions stopped: full indexing is off");
                    }
                }
                _ => {}
            }
        }
    }

    /// Follow `did` until full is left, dialling again with backoff.
    async fn follow(self: Arc<Self>, did: String, replay: bool) {
        let mut backoff = Duration::from_secs(1);
        loop {
            match self.session(&did, replay).await {
                Ok(End::Stopped) => return,
                Ok(End::Again(why)) | Err(why) => {
                    tracing::warn!(event = "full_labels_reconnect", labeler = %did, reason = %why, retry_secs = backoff.as_secs(), "a label subscription closed; it is dialled again");
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(60));
        }
    }

    /// One connection to `did`'s labeler.
    async fn session(&self, did: &str, replay: bool) -> Result<End, String> {
        let mut progress = match self.labels.progress(did) {
            Some(p) => p,
            None => {
                let p = Progress::start(replay, Utc::now());
                if !self.save(did, Vec::new(), &p).await.map_err(|e| e.to_string())? {
                    return Ok(End::Stopped);
                }
                p
            }
        };
        let doc = self.labelers.doc(did, false).await.ok_or("the labeler's document could not be read")?;
        let endpoint = doc.endpoint.ok_or("the labeler's document names no #atproto_labeler endpoint")?;
        let url = stream_url(&endpoint, progress.cursor).ok_or("the labeler's endpoint is not https")?;
        let mut keys = Keys { current: doc.key, ..Keys::default() };
        let connector = self.tls.clone().map(tokio_tungstenite::Connector::Rustls);
        let (mut ws, _) = tokio::time::timeout(Duration::from_secs(30), tokio_tungstenite::connect_async_tls_with_config(url.as_str(), None, false, connector))
            .await
            .map_err(|_| "connecting timed out".to_string())?
            .map_err(|e| e.to_string())?;
        tracing::info!(event = "full_labels_connected", labeler = %did, cursor = ?progress.cursor, replay = progress.replay, caught_up = progress.caught_up, "a label subscription is connected");
        let mut pending: Vec<Row> = Vec::new();
        let mut pending_seq: Option<i64> = None;
        let mut last_frame = Instant::now();
        loop {
            let wait = if pending.is_empty() { IDLE } else { FLUSH };
            let next = tokio::time::timeout(wait, ws.next()).await;
            let msg = match next {
                Err(_) => {
                    if pending_seq.is_some() {
                        progress.cursor = pending_seq.take();
                        if !self.save(did, std::mem::take(&mut pending), &progress).await.map_err(|e| e.to_string())? {
                            return Ok(End::Stopped);
                        }
                    } else if !progress.caught_up && last_frame.elapsed() >= IDLE {
                        progress.caught_up = true;
                        if !self.save(did, Vec::new(), &progress).await.map_err(|e| e.to_string())? {
                            return Ok(End::Stopped);
                        }
                        tracing::info!(event = "full_labels_caught_up", labeler = %did, "a label replay reached the tip");
                    } else if last_frame.elapsed() >= SILENT {
                        return Ok(End::Again("no frame for ten minutes".into()));
                    } else if self.wanted().await.is_none() {
                        return Ok(End::Stopped);
                    }
                    continue;
                }
                Ok(None) => return Ok(End::Again("the labeler closed the connection".into())),
                Ok(Some(Err(e))) => return Ok(End::Again(e.to_string())),
                Ok(Some(Ok(m))) => m,
            };
            let bytes = match msg {
                Message::Binary(b) => b,
                Message::Close(_) => return Ok(End::Again("the labeler closed the connection".into())),
                _ => continue,
            };
            last_frame = Instant::now();
            match proto_blue::ws::Frame::decode(&bytes) {
                Ok(proto_blue::ws::Frame::Message(m)) if m.r#type.as_deref() == Some("#labels") => {
                    let Some((seq, raw)) = labels_frame(&m.body) else {
                        metrics::counter!(METRIC_REJECTED, "src" => did.to_string(), "reason" => "malformed").increment(1);
                        continue;
                    };
                    let replaying = progress.replay && !progress.caught_up;
                    let mut newest: Option<DateTime<Utc>> = None;
                    for v in raw {
                        let Some(label) = Label::parse(&v) else {
                            metrics::counter!(METRIC_REJECTED, "src" => did.to_string(), "reason" => "malformed").increment(1);
                            continue;
                        };
                        newest = newest.max(label.cts);
                        if let Some(row) = self.checked(did, label, replaying, &mut keys).await {
                            pending.push(row);
                        }
                    }
                    pending_seq = Some(seq);
                    if let Some(at) = newest {
                        let lag = (Utc::now() - at).num_milliseconds().max(0);
                        #[allow(clippy::cast_precision_loss)]
                        metrics::gauge!(METRIC_LAG, "src" => did.to_string()).set(lag as f64 / 1000.0);
                        if !progress.caught_up && Utc::now() - at <= chrono::TimeDelta::from_std(TIP).unwrap_or_default() {
                            progress.caught_up = true;
                            tracing::info!(event = "full_labels_caught_up", labeler = %did, seq, "a label replay reached the tip");
                        }
                    }
                    if !replaying || pending.len() >= BATCH || progress.caught_up {
                        progress.cursor = pending_seq.take();
                        if !self.save(did, std::mem::take(&mut pending), &progress).await.map_err(|e| e.to_string())? {
                            return Ok(End::Stopped);
                        }
                    }
                }
                Ok(proto_blue::ws::Frame::Message(m)) => {
                    if m.r#type.as_deref() == Some("#info") {
                        tracing::warn!(event = "full_labels_info", labeler = %did, info = ?m.body, "a labeler sent an info frame");
                    }
                }
                Ok(proto_blue::ws::Frame::Error(e)) => {
                    if e.error == "FutureCursor" {
                        // The labeler has no such cursor: start over.
                        progress = Progress::start(progress.replay, Utc::now());
                        if !self.save(did, Vec::new(), &progress).await.map_err(|e| e.to_string())? {
                            return Ok(End::Stopped);
                        }
                    }
                    return Ok(End::Again(format!("{}: {}", e.error, e.message.unwrap_or_default())));
                }
                Err(e) => {
                    metrics::counter!(METRIC_REJECTED, "src" => did.to_string(), "reason" => "malformed").increment(1);
                    tracing::debug!(event = "full_labels_bad_frame", labeler = %did, error = %e, "a frame did not decode");
                }
            }
        }
    }

    /// The row of a received label, or `None` when it is dropped: not the
    /// labeler's own, or a live label whose signature fails after the key
    /// is read again. A replayed label that fails is checked against the
    /// key in effect at its `cts`, and stored unverified when that fails
    /// too.
    async fn checked(&self, did: &str, label: Label, replaying: bool, keys: &mut Keys) -> Option<Row> {
        if label.src != did {
            metrics::counter!(METRIC_REJECTED, "src" => did.to_string(), "reason" => "src").increment(1);
            return None;
        }
        if label.cts.is_none() {
            metrics::counter!(METRIC_REJECTED, "src" => did.to_string(), "reason" => "malformed").increment(1);
            return None;
        }
        if keys.current.as_deref().is_some_and(|k| verify(&label, k)) {
            metrics::counter!(METRIC_STORED, "src" => did.to_string(), "key" => "current").increment(1);
            return Row::of(&label, true);
        }
        if !replaying {
            if keys.reread_at.is_none_or(|at| at.elapsed() >= KEY_REREAD) {
                keys.reread_at = Some(Instant::now());
                if let Some(doc) = self.labelers.doc(did, true).await {
                    keys.current = doc.key;
                }
                if keys.current.as_deref().is_some_and(|k| verify(&label, k)) {
                    metrics::counter!(METRIC_STORED, "src" => did.to_string(), "key" => "current").increment(1);
                    return Row::of(&label, true);
                }
            }
            metrics::counter!(METRIC_REJECTED, "src" => did.to_string(), "reason" => "signature").increment(1);
            return None;
        }
        if keys.history.is_none() {
            keys.history = Some(super::history::fetch(&self.net, &self.plc_url, did).await);
        }
        let old = keys.history.as_ref().and_then(Option::as_ref).zip(label.cts).and_then(|(h, at)| h.key_at(at).map(str::to_string));
        if old.is_some_and(|k| verify(&label, &k)) {
            metrics::counter!(METRIC_STORED, "src" => did.to_string(), "key" => "history").increment(1);
            return Row::of(&label, true);
        }
        metrics::counter!(METRIC_STORED, "src" => did.to_string(), "key" => "unverified").increment(1);
        Row::of(&label, false)
    }

    /// Store `rows` and `progress` in one transaction, under the scope read
    /// guard and only in breadth Full: `false` when full was left.
    async fn save(&self, did: &str, rows: Vec<Row>, progress: &Progress) -> Result<bool, sqlx::Error> {
        let sets = self.scope.read_guard().await;
        if sets.public.breadth != Breadth::Full || sets.public.mode.off_rule() {
            return Ok(false);
        }
        let mut tx = self.pool.begin().await?;
        store::upsert(&mut tx, rows).await?;
        store::save_progress(&mut tx, did, progress).await?;
        tx.commit().await?;
        drop(sets);
        self.labels.set_progress(did, progress.clone());
        Ok(true)
    }
}
