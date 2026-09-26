//! Audit sinks (design-v2 §6.5; locked decision #7: log-only in v1).
//!
//! | `AuditSinks::new` argument | v1 implementation |
//! |---|---|
//! | `user` | `TerminatedSinkGuard<LatchFlag<LogUserSink>>` |
//! | `channel`, `substrate`, `moderation` | `PanicContained<Log*Sink>` |
//! | `fallback` | `PanicContainedFallback<LogFallbackSink>` |
//! | `inspection_queue` | `NoInspectionNotifications` |
//! | `correlation_key` | `CorrelationKey::from_bytes(APP_CORRELATION_KEY_HEX)` |
//!
//! **Panic-safety requirement (normative).** No sink passed to
//! `AuditSinks::new` lets a panic escape its trait method:
//! - the user sink is wrapped in kryphocron's `TerminatedSinkGuard`, which
//!   catches, latches and fails every later bind closed; [`LatchFlag`] records
//!   the latch for `/xrpc/_health`;
//! - the other sinks are wrapped in [`PanicContained`] /
//!   [`PanicContainedFallback`], which catch, log a static ERROR, count
//!   `audit_sink_panics_total{sink}`, and return `Ok(())`.
//!
//! The containment path formats nothing from the event payload.

use std::panic::{AssertUnwindSafe, RefUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use kryphocron::audit::{
    ChannelAuditEvent, CompositeOpId, FallbackAuditEvent, ModerationAuditEvent, SubstrateAuditEvent, UserAuditEvent,
};
use kryphocron::{
    AuditError, AuditSinks, BindOutcomeRepr, CapabilityKind, ChannelAuditSink, CorrelationKey, FallbackAuditSink,
    ModerationAuditSink, NoInspectionNotifications, SinkKind, SubstrateAuditSink, TerminatedSinkGuard, TraceId,
    UserAuditSink,
};

/// Metric: sink panics caught by the host wrappers.
pub const METRIC_SINK_PANICS: &str = "audit_sink_panics_total";

// Proof obligation for every `Log*Sink::record` below: no `unwrap`/`expect`,
// no indexing, and only derived `Debug` formatting of kryphocron's structural
// event types. The wrappers are the enforcement; these comments are the
// reasoning.

fn user_event_name(e: &UserAuditEvent) -> (&'static str, bool) {
    match e {
        UserAuditEvent::CapabilityBound { outcome, .. } => ("capability_bound", matches!(outcome, BindOutcomeRepr::Success)),
        UserAuditEvent::CapabilityIssuanceDenied { .. } => ("capability_issuance_denied", false),
        UserAuditEvent::ReborrowFailed { .. } => ("reborrow_failed", false),
        UserAuditEvent::DerivedContext { .. } => ("derived_context", true),
        UserAuditEvent::CompositeRollbackMarker { .. } => ("composite_rollback_marker", false),
        UserAuditEvent::ContentEncoded { .. } => ("content_encoded", true),
        UserAuditEvent::ContentEncodeFailed { .. } => ("content_encode_failed", false),
        UserAuditEvent::ContentDecodeFailed { .. } => ("content_decode_failed", false),
        _ => ("user_event", false),
    }
}

/// User-class log sink.
#[derive(Debug, Default, Clone, Copy)]
pub struct LogUserSink;

impl UserAuditSink for LogUserSink {
    fn record(&self, event: UserAuditEvent) -> Result<(), AuditError> {
        // No unwrap, no indexing; derived Debug only (see module note).
        let (name, grant) = user_event_name(&event);
        if grant {
            tracing::info!(target: "audit", class = "user", event = name, details = ?event, "audit");
        } else {
            tracing::warn!(target: "audit", class = "user", event = name, details = ?event, "audit");
        }
        Ok(())
    }
}

/// Channel-class log sink (unreached in v1: the AppView issues no channel
/// proofs).
#[derive(Debug, Default, Clone, Copy)]
pub struct LogChannelSink;

impl ChannelAuditSink for LogChannelSink {
    fn record(&self, event: ChannelAuditEvent) -> Result<(), AuditError> {
        // No unwrap, no indexing; derived Debug only (see module note).
        let grant = matches!(event, ChannelAuditEvent::ChannelBound { .. });
        if grant {
            tracing::info!(target: "audit", class = "channel", details = ?event, "audit");
        } else {
            tracing::warn!(target: "audit", class = "channel", details = ?event, "audit");
        }
        Ok(())
    }
}

/// Substrate-class log sink (reached by `validate_record_for_read`
/// rejections).
#[derive(Debug, Default, Clone, Copy)]
pub struct LogSubstrateSink;

impl SubstrateAuditSink for LogSubstrateSink {
    fn record(&self, event: SubstrateAuditEvent) -> Result<(), AuditError> {
        // No unwrap, no indexing; derived Debug only (see module note).
        let grant = matches!(event, SubstrateAuditEvent::ScopeBound { .. });
        if grant {
            tracing::info!(target: "audit", class = "substrate", details = ?event, "audit");
        } else {
            tracing::warn!(target: "audit", class = "substrate", details = ?event, "audit");
        }
        Ok(())
    }
}

/// Moderation-class log sink (unreached in v1: no moderation capabilities).
#[derive(Debug, Default, Clone, Copy)]
pub struct LogModerationSink;

impl ModerationAuditSink for LogModerationSink {
    fn record(&self, event: ModerationAuditEvent) -> Result<(), AuditError> {
        // No unwrap, no indexing; derived Debug only (see module note).
        tracing::warn!(target: "audit", class = "moderation", details = ?event, "audit");
        Ok(())
    }
}

/// Fallback log sink: every method is an ERROR event.
#[derive(Debug, Default, Clone, Copy)]
pub struct LogFallbackSink;

impl FallbackAuditSink for LogFallbackSink {
    fn record_panic(&self, sink: SinkKind, trace_id: TraceId, capability: CapabilityKind, at: SystemTime) {
        // No unwrap, no indexing; derived Debug only (see module note).
        tracing::error!(target: "audit", class = "fallback", ?sink, ?trace_id, ?capability, ?at, "audit sink panicked (substrate report)");
    }

    fn record_composite_failure(
        &self,
        trace_id: TraceId,
        composite_op_id: CompositeOpId,
        sinks_committed: &[SinkKind],
        sinks_failed: &[SinkKind],
        at: SystemTime,
    ) {
        tracing::error!(target: "audit", class = "fallback", ?trace_id, ?composite_op_id, ?sinks_committed, ?sinks_failed, ?at, "composite audit failure");
    }

    fn record_event(&self, event: FallbackAuditEvent) {
        tracing::error!(target: "audit", class = "fallback", details = ?event, "audit fallback event");
    }
}

/// The static caught-panic report: no payload formatting, no allocation from
/// the event.
fn report_caught(sink: &'static str) {
    tracing::error!(target: "audit", sink, "audit sink panicked; event dropped");
    metrics::counter!(METRIC_SINK_PANICS, "sink" => sink).increment(1);
}

/// Host wrapper containing panics in a channel, substrate or moderation sink.
#[derive(Debug, Default, Clone, Copy)]
pub struct PanicContained<S>(pub S);

impl<S: ChannelAuditSink> ChannelAuditSink for PanicContained<S> {
    fn record(&self, event: ChannelAuditEvent) -> Result<(), AuditError> {
        match catch_unwind(AssertUnwindSafe(|| self.0.record(event))) {
            Ok(r) => r,
            Err(_) => {
                report_caught("channel");
                Ok(())
            }
        }
    }
}

impl<S: SubstrateAuditSink> SubstrateAuditSink for PanicContained<S> {
    fn record(&self, event: SubstrateAuditEvent) -> Result<(), AuditError> {
        match catch_unwind(AssertUnwindSafe(|| self.0.record(event))) {
            Ok(r) => r,
            Err(_) => {
                report_caught("substrate");
                Ok(())
            }
        }
    }
}

impl<S: ModerationAuditSink> ModerationAuditSink for PanicContained<S> {
    fn record(&self, event: ModerationAuditEvent) -> Result<(), AuditError> {
        match catch_unwind(AssertUnwindSafe(|| self.0.record(event))) {
            Ok(r) => r,
            Err(_) => {
                report_caught("moderation");
                Ok(())
            }
        }
    }
}

/// Host wrapper containing panics in every fallback-sink method.
#[derive(Debug, Default, Clone, Copy)]
pub struct PanicContainedFallback<S>(pub S);

impl<S: FallbackAuditSink> FallbackAuditSink for PanicContainedFallback<S> {
    fn record_panic(&self, sink: SinkKind, trace_id: TraceId, capability: CapabilityKind, at: SystemTime) {
        if catch_unwind(AssertUnwindSafe(|| self.0.record_panic(sink, trace_id, capability, at))).is_err() {
            report_caught("fallback");
        }
    }

    fn record_composite_failure(
        &self,
        trace_id: TraceId,
        composite_op_id: CompositeOpId,
        sinks_committed: &[SinkKind],
        sinks_failed: &[SinkKind],
        at: SystemTime,
    ) {
        let call = || self.0.record_composite_failure(trace_id, composite_op_id, sinks_committed, sinks_failed, at);
        if catch_unwind(AssertUnwindSafe(call)).is_err() {
            report_caught("fallback");
        }
    }

    fn record_event(&self, event: FallbackAuditEvent) {
        if catch_unwind(AssertUnwindSafe(|| self.0.record_event(event))).is_err() {
            report_caught("fallback");
        }
    }
}

/// Sets the shared latch flag if dropped while armed (i.e. during unwinding
/// out of the inner sink). The store cannot panic.
struct LatchOnUnwind<'a> {
    flag: &'a AtomicBool,
    armed: bool,
}

impl Drop for LatchOnUnwind<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.flag.store(true, Ordering::SeqCst);
        }
    }
}

/// The user sink's inner wrapper: records a panic in the shared
/// `audit_latched` flag, then lets it reach `TerminatedSinkGuard`.
pub struct LatchFlag {
    inner: Box<dyn UserAuditSink + RefUnwindSafe>,
    flag: Arc<AtomicBool>,
}

impl LatchFlag {
    #[must_use]
    pub fn new(inner: Box<dyn UserAuditSink + RefUnwindSafe>, flag: Arc<AtomicBool>) -> Self {
        Self { inner, flag }
    }
}

impl UserAuditSink for LatchFlag {
    fn record(&self, event: UserAuditEvent) -> Result<(), AuditError> {
        let mut guard = LatchOnUnwind {
            flag: &self.flag,
            armed: true,
        };
        let result = self.inner.record(event);
        guard.armed = false;
        result
    }
}

/// The process-lifetime sinks and correlation key (owned by `GlueContext`).
pub struct GlueSinks {
    user: TerminatedSinkGuard<LatchFlag>,
    channel: Box<dyn ChannelAuditSink>,
    substrate: Box<dyn SubstrateAuditSink>,
    moderation: Box<dyn ModerationAuditSink>,
    fallback: Box<dyn FallbackAuditSink>,
    inspection: NoInspectionNotifications,
    correlation: CorrelationKey,
    latched: Arc<AtomicBool>,
}

impl GlueSinks {
    /// The v1 log sinks.
    #[must_use]
    pub fn log(correlation: CorrelationKey) -> Self {
        Self::with(
            Box::new(LogUserSink),
            Box::new(PanicContained(LogSubstrateSink)),
            correlation,
        )
    }

    /// The v1 wiring with a replaceable user sink and (already wrapped)
    /// substrate sink, for tests that inject panicking sinks.
    #[must_use]
    pub fn with(
        user: Box<dyn UserAuditSink + RefUnwindSafe>,
        substrate: Box<dyn SubstrateAuditSink>,
        correlation: CorrelationKey,
    ) -> Self {
        let latched = Arc::new(AtomicBool::new(false));
        Self {
            user: TerminatedSinkGuard::new(LatchFlag::new(user, Arc::clone(&latched))),
            channel: Box::new(PanicContained(LogChannelSink)),
            substrate,
            moderation: Box::new(PanicContained(LogModerationSink)),
            fallback: Box::new(PanicContainedFallback(LogFallbackSink)),
            inspection: NoInspectionNotifications,
            correlation,
            latched,
        }
    }

    /// A fresh `AuditSinks<'_>` borrowing the process-lifetime sinks (§6.3
    /// step 6).
    #[must_use]
    pub fn audit_sinks(&self) -> AuditSinks<'_> {
        AuditSinks::new(
            &self.user,
            self.channel.as_ref(),
            self.substrate.as_ref(),
            self.moderation.as_ref(),
            self.fallback.as_ref(),
            &self.inspection,
            &self.correlation,
        )
    }

    #[must_use]
    pub fn user_sink(&self) -> &dyn UserAuditSink {
        &self.user
    }

    #[must_use]
    pub fn substrate_sink(&self) -> &dyn SubstrateAuditSink {
        self.substrate.as_ref()
    }

    /// The shared latch: `/xrpc/_health` reports `audit_ok = !latched`.
    #[must_use]
    pub fn latch(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.latched)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Test sinks that always panic.
    pub(crate) struct PanickingUser;
    impl UserAuditSink for PanickingUser {
        fn record(&self, _event: UserAuditEvent) -> Result<(), AuditError> {
            panic!("user sink boom")
        }
    }

    pub(crate) struct PanickingSubstrate;
    impl SubstrateAuditSink for PanickingSubstrate {
        fn record(&self, _event: SubstrateAuditEvent) -> Result<(), AuditError> {
            panic!("substrate sink boom")
        }
    }

    struct PanickingChannel;
    impl ChannelAuditSink for PanickingChannel {
        fn record(&self, _event: ChannelAuditEvent) -> Result<(), AuditError> {
            panic!("channel sink boom")
        }
    }

    struct PanickingFallback;
    impl FallbackAuditSink for PanickingFallback {
        fn record_panic(&self, _: SinkKind, _: TraceId, _: CapabilityKind, _: SystemTime) {
            panic!("fallback boom")
        }
        fn record_composite_failure(&self, _: TraceId, _: CompositeOpId, _: &[SinkKind], _: &[SinkKind], _: SystemTime) {
            panic!("fallback boom")
        }
        fn record_event(&self, _: FallbackAuditEvent) {
            panic!("fallback boom")
        }
    }

    fn substrate_event() -> SubstrateAuditEvent {
        SubstrateAuditEvent::MalformedRecordRejected {
            trace_id: TraceId::from_bytes([1; 16]),
            nsid: kryphocron::Nsid::new("tools.kryphocron.feed.postPrivate").expect("nsid"),
            requester: kryphocron::Did::new("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa").expect("did"),
            reason: kryphocron::audit::MalformedRecordReason::NeitherTextNorEncodedContent,
            at: SystemTime::now(),
        }
    }

    #[test]
    fn contained_sinks_swallow_panics_and_return_ok() {
        let quiet = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let substrate = PanicContained(PanickingSubstrate);
        assert!(SubstrateAuditSink::record(&substrate, substrate_event()).is_ok());
        let fallback = PanicContainedFallback(PanickingFallback);
        fallback.record_panic(SinkKind::User, TraceId::from_bytes([2; 16]), CapabilityKind::ViewPrivate, SystemTime::now());
        fallback.record_composite_failure(
            TraceId::from_bytes([2; 16]),
            CompositeOpId::from_bytes([3; 16]),
            &[SinkKind::User],
            &[SinkKind::Substrate],
            SystemTime::now(),
        );
        let _channel = PanicContained(PanickingChannel);
        std::panic::set_hook(quiet);
        // The inner log sinks themselves never fail.
        assert!(SubstrateAuditSink::record(&PanicContained(LogSubstrateSink), substrate_event()).is_ok());
    }

    #[test]
    fn user_sink_panic_latches_the_flag_and_the_guard() {
        let quiet = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let sinks = GlueSinks::with(
            Box::new(PanickingUser),
            Box::new(PanicContained(LogSubstrateSink)),
            CorrelationKey::from_bytes([0; 32]),
        );
        let latch = sinks.latch();
        assert!(!latch.load(Ordering::SeqCst));
        let event = UserAuditEvent::ContentDecodeFailed {
            trace_id: TraceId::from_bytes([1; 16]),
            requester: kryphocron::Did::new("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa").expect("did"),
            subject_repr: kryphocron::TargetRepresentation::structural_only(
                kryphocron::StructuralRepresentation::Resource {
                    did: kryphocron::Did::new("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa").expect("did"),
                    nsid: kryphocron::Nsid::new("tools.kryphocron.feed.postPrivate").expect("nsid"),
                },
            ),
            codec: None,
            stored_codec: None,
            error_class: kryphocron::CodecErrorClass::UnknownOrWrongCodec,
            at: SystemTime::now(),
        };
        assert_eq!(sinks.user_sink().record(event.clone()), Err(AuditError::Unavailable));
        std::panic::set_hook(quiet);
        assert!(latch.load(Ordering::SeqCst), "the host latch records the panic");
        // Every later record fails closed without reaching the inner sink.
        assert_eq!(sinks.user_sink().record(event), Err(AuditError::Unavailable));
    }
}
