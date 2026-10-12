//! Notification preferences (design-v3 §5.6): `getPreferences`,
//! `putPreferencesV2`, the legacy `putPreferences`, and the filter the
//! notification reads apply from them.
//!
//! - A member with no stored row has the defaults.
//! - `chat` is never stored: it is always returned as its default.
//! - `list: false` leaves a reason out of `listNotifications` and
//!   `getUnreadCount`; `include: "follows"` keeps only notifications from
//!   accounts the member follows. `push` has no effect.

use serde_json::{Map, Value, json};

use super::{Hydrator, ViewError, ViewResult, Viewer};

/// Keys whose value is a `filterablePreference` (`include`, `list`, `push`).
const FILTERABLE: [&str; 8] = ["follow", "like", "likeViaRepost", "mention", "quote", "reply", "repost", "repostViaRepost"];
/// Keys whose value is a `preference` (`list`, `push`).
const PLAIN: [&str; 4] = ["starterpackJoined", "subscribedPost", "unverified", "verified"];
/// The `include` values of a `filterablePreference`.
const INCLUDES: [&str; 2] = ["all", "follows"];

/// Each notification reason with the preference key that governs it.
const REASON_KEYS: [(&str, &str); 12] = [
    ("follow", "follow"),
    ("like", "like"),
    ("like-via-repost", "likeViaRepost"),
    ("mention", "mention"),
    ("quote", "quote"),
    ("reply", "reply"),
    ("repost", "repost"),
    ("repost-via-repost", "repostViaRepost"),
    ("starterpack-joined", "starterpackJoined"),
    ("subscribed-post", "subscribedPost"),
    ("unverified", "unverified"),
    ("verified", "verified"),
];

/// The 13-key object a member with no stored row has.
#[must_use]
pub fn defaults() -> Value {
    let mut out = Map::new();
    out.insert("chat".into(), json!({"include": "all", "push": true}));
    for key in FILTERABLE {
        out.insert(key.into(), json!({"include": "all", "list": true, "push": true}));
    }
    for key in PLAIN {
        out.insert(key.into(), json!({"list": true, "push": true}));
    }
    Value::Object(out)
}

/// `stored` over the defaults: every key present, `chat` the default.
fn complete(stored: Option<&Value>) -> Value {
    let mut out = defaults();
    for key in FILTERABLE.into_iter().chain(PLAIN) {
        if let Some(value) = stored.and_then(|s| s.get(key)).filter(|v| v.is_object()) {
            out[key] = value.clone();
        }
    }
    out
}

/// One supplied preference, checked against its lexicon shape.
fn checked(key: &str, value: &Value, filterable: bool) -> Result<Value, String> {
    let flag = |name: &str| value.get(name).and_then(Value::as_bool).ok_or_else(|| format!("{key}.{name} must be a boolean"));
    let mut out = json!({"list": flag("list")?, "push": flag("push")?});
    if filterable {
        let include = value.get("include").and_then(Value::as_str).filter(|i| INCLUDES.contains(i));
        out["include"] = json!(include.ok_or_else(|| format!("{key}.include must be one of all, follows"))?);
    }
    Ok(out)
}

/// `input`'s keys merged over `current`. `chat` and unknown keys are
/// ignored.
fn merged(mut current: Value, input: &Value) -> Result<Value, String> {
    for (keys, filterable) in [(&FILTERABLE[..], true), (&PLAIN[..], false)] {
        for key in keys {
            if let Some(value) = input.get(*key) {
                current[*key] = checked(key, value, filterable)?;
            }
        }
    }
    Ok(current)
}

/// What a member's preferences leave out of the notification reads.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct ReasonFilter {
    /// Reasons with `list: false`.
    pub hidden: Vec<String>,
    /// Reasons kept only from accounts the member follows.
    pub follows_only: Vec<String>,
}

impl ReasonFilter {
    fn of(prefs: &Value) -> Self {
        let mut out = Self::default();
        for (reason, key) in REASON_KEYS {
            let pref = &prefs[key];
            if pref["list"] == false {
                out.hidden.push(reason.to_string());
            } else if pref["include"] == "follows" {
                out.follows_only.push(reason.to_string());
            }
        }
        out
    }
}

impl Hydrator {
    async fn stored_prefs(&self, me: &str) -> Result<Option<Value>, sqlx::Error> {
        sqlx::query_scalar("SELECT prefs FROM notification_preferences_v2 WHERE did = $1")
            .bind(me)
            .fetch_optional(self.pool())
            .await
    }

    /// The reasons `me`'s preferences hide or narrow.
    pub(super) async fn reason_filter(&self, me: &str) -> Result<ReasonFilter, sqlx::Error> {
        Ok(self.stored_prefs(me).await?.map(|stored| ReasonFilter::of(&complete(Some(&stored)))).unwrap_or_default())
    }

    /// `app.bsky.notification.getPreferences`.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn get_notification_preferences(&self, viewer: &Viewer) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        let prefs = complete(self.stored_prefs(me).await?.as_ref());
        Ok(Ok(Self::emit(json!({ "preferences": prefs }))))
    }

    /// `app.bsky.notification.putPreferencesV2`: the supplied keys replace
    /// the member's, and the whole object is returned.
    ///
    /// # Errors
    ///
    /// Database errors.
    pub async fn put_notification_preferences(&self, viewer: &Viewer, input: &Value) -> ViewResult<Value> {
        let Some(me) = viewer.did.as_deref() else {
            return Ok(Err(ViewError::new("AuthRequired", "Authentication required")));
        };
        // Read and write under the member's row lock, so two puts of
        // different keys both land.
        let mut tx = self.pool().begin().await?;
        sqlx::query("INSERT INTO notification_preferences_v2 (did, prefs) VALUES ($1, '{}') ON CONFLICT (did) DO NOTHING")
            .bind(me)
            .execute(&mut *tx)
            .await?;
        let stored: Value = sqlx::query_scalar("SELECT prefs FROM notification_preferences_v2 WHERE did = $1 FOR UPDATE")
            .bind(me)
            .fetch_one(&mut *tx)
            .await?;
        let prefs = match merged(complete(Some(&stored)), input) {
            Ok(prefs) => prefs,
            // Dropping the transaction undoes the placeholder row.
            Err(message) => return Ok(Err(ViewError::new("InvalidRequest", &message))),
        };
        let mut kept = prefs.clone();
        if let Some(object) = kept.as_object_mut() {
            object.remove("chat");
        }
        sqlx::query("UPDATE notification_preferences_v2 SET prefs = $2, updated_at = now() WHERE did = $1")
            .bind(me)
            .bind(&kept)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(Ok(Self::emit(json!({ "preferences": prefs }))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_have_thirteen_keys_and_merge_validates() {
        let d = defaults();
        assert_eq!(d.as_object().map(Map::len), Some(13));
        assert_eq!(d["like"], json!({"include": "all", "list": true, "push": true}));
        assert_eq!(d["verified"], json!({"list": true, "push": true}));
        // A supplied key replaces the stored one; `chat` and unknown keys
        // are ignored.
        let input = json!({
            "like": {"include": "follows", "list": true, "push": false},
            "subscribedPost": {"list": false, "push": true},
            "chat": {"include": "accepted", "push": false},
            "other": 1,
        });
        let m = merged(defaults(), &input).expect("valid");
        assert_eq!(m["like"]["include"], "follows");
        assert_eq!(m["chat"], d["chat"]);
        assert!(m.get("other").is_none());
        assert_eq!(
            ReasonFilter::of(&m),
            ReasonFilter {
                hidden: vec!["subscribed-post".into()],
                follows_only: vec!["like".into()],
            }
        );
        // An unknown `include`, or a missing flag, is refused.
        assert!(merged(defaults(), &json!({"like": {"include": "mutuals", "list": true, "push": true}})).is_err());
        assert!(merged(defaults(), &json!({"verified": {"list": true}})).is_err());
        // A stored object missing keys reads as complete.
        assert_eq!(complete(Some(&json!({"reply": {"include": "follows", "list": true, "push": true}})))["quote"], d["quote"]);
    }
}
