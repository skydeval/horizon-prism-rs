//! The private read pipeline, per record (design-v2 §6.3), and the sealed
//! per-record witness `RecordAuthz` (§3.2).
//!
//! Steps 3–12 run here; the handler owns steps 1–2 (clock, auth gate) and
//! 13 (the whole-handler floor). Every failure in steps 3–11 is the same
//! uniform miss, and increments `private_read_outcome_total{outcome}` with a
//! server-side reason that never reaches the wire.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};

use kryphocron::authority::{ViewPrivate, issue_user};
use kryphocron::ingress::from_xrpc_request;
use kryphocron::{
    AtUri, BoundUserProof, CodecError, CodecId, Did, EncodedRecord, Nsid, OracleSet, ReadAuthorization,
    RecordContentContext, RecordKey, RecordValidation, ResourceId, RotationGenerationMark, StructuralRepresentation,
    TargetRepresentation, TraceId,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::sync::Mutex;

use super::emit::PrivateEmit;
use super::oracle::{OracleSnapshot, SnapshotInputs};
use super::{readiness, schema};
use crate::db::locks::uri_author;
use crate::firehose::classifier::NSID_POST_PRIVATE;
use crate::firehose::watermark::Watermarks;
use crate::hydration::actor::{ProfileContext, ProfileKind};
use crate::hydration::ts;
use crate::kryphocron_glue::{PipelineGlue, VerifiedCaller};
use crate::moderation::{self, Gate};
use crate::sync::HeadProber;

/// Metric: per-record outcomes (server-side only).
pub(super) const METRIC_OUTCOMES: &str = "private_read_outcome_total";
/// The decode deadline after handler start (§6.3 step 11).
pub(super) const DECODE_BUDGET: Duration = Duration::from_secs(2);

/// The per-record witness (§3.2): the kryphocron `ReadAuthorization` together
/// with the resource it was bound for. Not `Clone`; fields private. Its only
/// constructor takes the bound proof.
pub(super) struct RecordAuthz {
    authz: ReadAuthorization,
    resource: ResourceId,
}

impl RecordAuthz {
    pub(super) fn new(bound: &BoundUserProof<'_, ViewPrivate>) -> Self {
        Self {
            authz: ReadAuthorization::from_view_private(bound),
            resource: bound.subject().clone(),
        }
    }

    pub(super) fn authz(&self) -> &ReadAuthorization {
        &self.authz
    }

    /// The bound resource's at-uri.
    pub(super) fn uri(&self) -> String {
        format!(
            "at://{}/{}/{}",
            self.resource.did().as_str(),
            self.resource.nsid().as_str(),
            self.resource.rkey().as_str()
        )
    }
}

impl std::fmt::Debug for RecordAuthz {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordAuthz").field("resource", &self.resource).finish_non_exhaustive()
    }
}

/// Why a record was a uniform miss (metrics only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Miss {
    NotPrivate,
    Moderation,
    NoRow,
    OwnerNotReady,
    AudienceOwnerNotReady,
    HeadCheck,
    Stale,
    IssueDenied,
    BindDenied,
    AuditUnavailable,
    Malformed,
    CodecMismatch,
    DecodeFailed,
    NotUtf8,
    Db,
}

impl Miss {
    fn label(self) -> &'static str {
        match self {
            Self::NotPrivate => "not_private",
            Self::Moderation => "moderation",
            Self::NoRow => "no_row",
            Self::OwnerNotReady => "owner_not_ready",
            Self::AudienceOwnerNotReady => "audience_owner_not_ready",
            Self::HeadCheck => "head_check",
            Self::Stale => "stale",
            Self::IssueDenied => "issue_denied",
            Self::BindDenied => "bind_denied",
            Self::AuditUnavailable => "audit_unavailable",
            Self::Malformed => "malformed",
            Self::CodecMismatch => "codec_mismatch",
            Self::DecodeFailed => "decode_failed",
            Self::NotUtf8 => "not_utf8",
            Self::Db => "db_error",
        }
    }
}

impl From<sqlx::Error> for Miss {
    fn from(e: sqlx::Error) -> Self {
        tracing::warn!(event = "private_read_db_error", error = %e, "private read failed on the database");
        Self::Db
    }
}

/// What the pipeline needs, shared by the private handlers.
#[derive(Clone)]
pub(super) struct PipelineEnv {
    pub pool: PgPool,
    pub glue: PipelineGlue,
    pub prober: HeadProber,
    pub watermarks: Watermarks,
    /// `kryphocron.oracle_freshness_bound_secs`.
    pub freshness_bound: Duration,
    pub community_patterns: Arc<Vec<String>>,
    /// `sync.max_head_fetches_per_request`.
    pub max_head_fetches: usize,
}

/// Per-request state: repo-head results by DID, and the synchronous head
/// fetch budget.
pub(super) struct RequestState {
    heads: Mutex<HashMap<String, bool>>,
    fetch_budget: AtomicUsize,
}

impl RequestState {
    pub(super) fn new(max_head_fetches: usize) -> Self {
        Self {
            heads: Mutex::new(HashMap::new()),
            fetch_budget: AtomicUsize::new(max_head_fetches),
        }
    }
}

/// The repo-head check (§6.11.3): `applied_rev ≥ head_rev`, with the head no
/// older than `sync.head_max_age_secs` or fetched now within the budget.
async fn head_ok(env: &PipelineEnv, rs: &RequestState, did: &str) -> Result<bool, sqlx::Error> {
    if let Some(ok) = rs.heads.lock().await.get(did) {
        return Ok(*ok);
    }
    let allow = rs
        .fetch_budget
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        .is_ok();
    let ok = match env.prober.read_head_with(did, allow).await? {
        None => false,
        Some(head) => {
            let applied: Option<String> = sqlx::query_scalar("SELECT applied_rev FROM sync_state WHERE did = $1")
                .bind(did)
                .fetch_optional(&env.pool)
                .await?
                .flatten();
            applied.is_some_and(|a| a >= head)
        }
    };
    rs.heads.lock().await.insert(did.to_string(), ok);
    Ok(ok)
}

fn trace_id() -> TraceId {
    TraceId::from_bytes(rand::random())
}

/// Read one private record for `caller` (§6.3 steps 3–12), counting the
/// outcome.
pub(super) async fn read(
    env: &PipelineEnv,
    rs: &RequestState,
    caller: &VerifiedCaller,
    uri: &str,
    authors: &ProfileContext,
    start: Instant,
) -> Result<PrivateEmit<Value>, Miss> {
    // Boxed: the bind and decode futures are large, and a page holds up to
    // `bind_concurrency` of them.
    let result = Box::pin(read_inner(env, rs, caller, uri, authors, start)).await;
    let label = match &result {
        Ok(_) => "hit",
        Err(m) => m.label(),
    };
    metrics::counter!(METRIC_OUTCOMES, "outcome" => label).increment(1);
    tracing::debug!(event = "private_read", uri, outcome = label, "private read");
    result
}

async fn read_inner(
    env: &PipelineEnv,
    rs: &RequestState,
    caller: &VerifiedCaller,
    uri: &str,
    authors: &ProfileContext,
    start: Instant,
) -> Result<PrivateEmit<Value>, Miss> {
    // 3. Parse and pre-validate.
    let mut parts = uri.strip_prefix("at://").ok_or(Miss::NotPrivate)?.splitn(3, '/');
    let (Some(owner_s), Some(nsid_s), Some(rkey_s)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(Miss::NotPrivate);
    };
    if nsid_s != NSID_POST_PRIVATE {
        return Err(Miss::NotPrivate);
    }
    let owner = Did::new(owner_s).map_err(|_| Miss::NotPrivate)?;
    let nsid = Nsid::new(nsid_s).map_err(|_| Miss::NotPrivate)?;
    let rkey = RecordKey::new(rkey_s).map_err(|_| Miss::NotPrivate)?;
    let requester = Did::new(&caller.did).map_err(|_| Miss::NotPrivate)?;
    if moderation::gate(&env.pool, uri).await? != Gate::Visible {
        return Err(Miss::Moderation);
    }
    let facts = schema::post_facts(&env.pool, uri).await?.ok_or(Miss::NoRow)?;
    if facts.owner != owner_s {
        return Err(Miss::NoRow);
    }
    if !readiness::owner_ready(&env.pool, owner_s).await? {
        return Err(Miss::OwnerNotReady);
    }
    let audience_owner = facts.audience_uri.as_deref().and_then(uri_author);
    if let Some(a) = audience_owner.filter(|a| *a != owner_s)
        && !readiness::owner_ready(&env.pool, a).await?
    {
        return Err(Miss::AudienceOwnerNotReady);
    }
    let mut head_dids = vec![owner_s, caller.did.as_str()];
    if let Some(a) = audience_owner.filter(|a| *a != owner_s) {
        head_dids.push(a);
    }
    for did in head_dids {
        if !head_ok(env, rs, did).await? {
            return Err(Miss::HeadCheck);
        }
    }

    // 4. The oracle snapshot.
    let resource = ResourceId::new(owner.clone(), nsid.clone(), rkey.clone());
    let watermark = env.watermarks.private_watermark().map(SystemTime::from);
    let snap = OracleSnapshot::load(
        &env.pool,
        SnapshotInputs {
            requester: &caller.did,
            resource: &resource,
            uri,
            watermark,
            bound: env.freshness_bound,
            community_patterns: &env.community_patterns,
        },
    )
    .await?;

    // 5. Host freshness: the only block-freshness enforcement point.
    let now = SystemTime::now();
    let fresh = watermark.is_some_and(|w| now.duration_since(w).unwrap_or(Duration::ZERO) <= env.freshness_bound);
    if !fresh {
        return Err(Miss::Stale);
    }

    // 6–8. Context, issue, bind. `ctx` borrows `snap` and the glue's sinks.
    let trace = trace_id();
    let bound = {
        let ctx = from_xrpc_request(caller.jwt.clone(), trace, env.glue.audit_sinks(), OracleSet::new(&snap, &snap, &snap));
        let proof = issue_user::<ViewPrivate>(&ctx, resource.clone()).map_err(|_| Miss::IssueDenied)?;
        Box::pin(proof.bind(&ctx, &resource)).await.map_err(|e| match e {
            kryphocron::BindError::AuditUnavailable | kryphocron::BindError::AuditPanicked => Miss::AuditUnavailable,
            _ => Miss::BindDenied,
        })?
    };

    // 9. The witness. From here on content is loaded only through it.
    let rauth = RecordAuthz::new(&bound);

    // 10. Structural validation behind the witness.
    let (has_text, has_encoded, has_codec, has_generation) =
        schema::validation_flags(&env.pool, &rauth).await?.ok_or(Miss::NoRow)?;
    kryphocron::validate_record_for_read(
        rauth.authz(),
        &RecordValidation::post_private(has_text, has_encoded, has_codec, has_generation),
        nsid.clone(),
        trace,
        env.glue.substrate_sink(),
        now,
    )
    .map_err(|_| Miss::Malformed)?;

    // 11. Decode.
    let text = match schema::encoded(&env.pool, &rauth).await? {
        Some(row) => {
            let codec = CodecId::new(row.codec.unwrap_or_default()).map_err(|_| Miss::Malformed)?;
            let generation = row
                .generation
                .map(RotationGenerationMark::new)
                .transpose()
                .map_err(|_| Miss::Malformed)?;
            let audience_list = facts.audience_uri.as_deref().and_then(|u| AtUri::new(u).ok());
            let context = RecordContentContext::new(
                nsid.clone(),
                rkey.clone(),
                owner.clone(),
                audience_list,
                requester,
                TargetRepresentation::structural_only(StructuralRepresentation::Resource {
                    did: owner.clone(),
                    nsid: nsid.clone(),
                }),
                trace,
                Default::default(),
            );
            let bytes = kryphocron::decode_record_content(
                rauth.authz(),
                env.glue.at_rest_hooks(),
                env.glue.user_sink(),
                &EncodedRecord::new(codec, row.content, generation),
                &context,
                start + DECODE_BUDGET,
                SystemTime::now(),
            )
            .await
            .map_err(|e| match e {
                CodecError::UnknownOrWrongCodec { .. } => Miss::CodecMismatch,
                _ => Miss::DecodeFailed,
            })?;
            String::from_utf8(bytes).map_err(|_| Miss::NotUtf8)?
        }
        None => schema::plaintext(&env.pool, &rauth).await?.ok_or(Miss::Malformed)?,
    };

    // 12. The view, emitted through the witness.
    let meta = schema::view_meta(&env.pool, &rauth).await?.ok_or(Miss::NoRow)?;
    let mut record = json!({
        "$type": NSID_POST_PRIVATE,
        "text": text,
        "createdAt": ts(meta.created_at),
    });
    if let Some(a) = &meta.audience_uri {
        record["audienceList"] = json!(a);
    }
    if let (Some(root), Some(parent)) = (&meta.reply_root_uri, &meta.reply_parent_uri) {
        // Strong refs; a cid is absent only for rows ingested before
        // migration 0002 that no resync has refreshed yet.
        let strong_ref = |uri: &str, cid: &Option<String>| match cid {
            Some(c) => json!({"uri": uri, "cid": c}),
            None => json!({"uri": uri}),
        };
        record["reply"] = json!({
            "root": strong_ref(root, &meta.reply_root_cid),
            "parent": strong_ref(parent, &meta.reply_parent_cid),
        });
    }
    let view = json!({
        "uri": rauth.uri(),
        "cid": meta.cid,
        "author": authors.view(owner_s, ProfileKind::Basic),
        "record": record,
        "indexedAt": ts(meta.indexed_at),
        "labels": [],
    });
    Ok(PrivateEmit::new(&rauth, view))
}
