//! `refract_dids` states (refract design §4.2) and the in-memory sets built
//! from the rows: R-live, the candidates, and `refract_hidden`.
//!
//! Memory is always set from the committed rows: a writer that changed
//! Refract rows takes the write guard, commits, then calls [`refresh`] for
//! the DIDs it touched while still holding the guard (§4.3's protocol), so
//! in-memory changes apply in commit order.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use super::communities::Communities;
use crate::aggregations::ScopeSets;
use crate::db::locks::uri_author;

/// A `refract_dids` state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum State {
    Candidate,
    Rejected,
    Restored,
    Grace,
    Ending,
}

impl State {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::Rejected => "rejected",
            Self::Restored => "restored",
            Self::Grace => "grace",
            Self::Ending => "ending",
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "candidate" => Some(Self::Candidate),
            "rejected" => Some(Self::Rejected),
            "restored" => Some(Self::Restored),
            "grace" => Some(Self::Grace),
            "ending" => Some(Self::Ending),
            _ => None,
        }
    }

    /// In R: `restored` or `grace` (Terms).
    #[must_use]
    pub fn in_r(self) -> bool {
        matches!(self, Self::Restored | Self::Grace)
    }

    /// The states whose labels are stored (§4.2, D-2).
    #[must_use]
    pub fn keeps_labels(self) -> bool {
        matches!(self, Self::Candidate | Self::Restored | Self::Grace | Self::Ending)
    }
}

/// The synthetic omission label (§4.5).
pub const ABSENT: &str = "!refract-absent";

/// One DID's in-memory facts as read from the rows.
#[derive(Debug, Clone)]
struct Facts {
    state: State,
    community: String,
    awaiting: bool,
}

/// Set the in-memory Refract sets of `dids` (every DID with a row when
/// `None`) from the committed rows. `sets` is the write guard the caller
/// holds.
///
/// # Errors
///
/// Database errors; memory is unchanged on error.
pub async fn refresh(pool: &PgPool, sets: &mut ScopeSets, dids: Option<&[String]>, communities: &Communities) -> Result<(), sqlx::Error> {
    let rows: Vec<(String, String, String, Option<DateTime<Utc>>)> = sqlx::query_as(
        "SELECT did, state, community, awaiting_labels_since FROM refract_dids WHERE $1::text[] IS NULL OR did = ANY($1)",
    )
    .bind(dids)
    .fetch_all(pool)
    .await?;
    let pending: HashSet<String> = sqlx::query_scalar("SELECT DISTINCT did FROM refract_pending_dids WHERE $1::text[] IS NULL OR did = ANY($1)")
        .bind(dids)
        .fetch_all(pool)
        .await?
        .into_iter()
        .collect();
    let labels: Vec<(String, String, String, Option<DateTime<Utc>>)> = sqlx::query_as(
        "SELECT labeler_did, subject, did, exp FROM refract_labels WHERE gone AND ($1::text[] IS NULL OR did = ANY($1))",
    )
    .bind(dids)
    .fetch_all(pool)
    .await?;
    let facts: HashMap<String, Facts> = rows
        .into_iter()
        .filter_map(|(did, state, community, awaiting)| {
            State::parse(&state).map(|state| (did, Facts { state, community, awaiting: awaiting.is_some() }))
        })
        .collect();
    // Start from nothing for the DIDs read.
    let restored = Arc::make_mut(&mut sets.restored);
    let hidden = Arc::make_mut(&mut sets.refract_hidden);
    match dids {
        Some(dids) => {
            for did in dids {
                restored.remove(did);
                sets.candidates.remove(did);
                hidden.forget(did);
            }
        }
        None => {
            restored.clear();
            sets.candidates.clear();
            *hidden = super::RefractHidden::default();
        }
    }
    for (did, f) in &facts {
        let is_pending = pending.contains(did);
        match f.state {
            State::Candidate => {
                sets.candidates.insert(did.clone(), f.community.clone());
            }
            State::Rejected => {}
            State::Restored | State::Grace => {
                if !is_pending {
                    restored.insert(did.clone(), f.community.clone());
                }
                hidden.update(did, |r| {
                    r.pending = is_pending;
                    r.awaiting_labels = f.awaiting;
                });
            }
            State::Ending => hidden.update(did, |r| {
                r.ending = true;
                r.pending = is_pending;
            }),
        }
    }
    // Labels count only from the DID's own community's labeler, and only
    // for a DID in R or `ending`; a candidate's never hide anything (§4.2).
    for (labeler, subject, did, exp) in labels {
        let Some(f) = facts.get(&did) else {
            continue;
        };
        if !matches!(f.state, State::Restored | State::Grace | State::Ending) || communities.labeler_of(&f.community) != Some(labeler.as_str()) {
            continue;
        }
        if subject == did {
            hidden.update(&did, |r| r.add_gone(exp));
        } else if uri_author(&subject) == Some(did.as_str()) {
            hidden.add_record(&subject, exp);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn states_round_trip() {
        for s in [State::Candidate, State::Rejected, State::Restored, State::Grace, State::Ending] {
            assert_eq!(State::parse(s.as_str()), Some(s));
        }
        assert_eq!(State::parse("gone"), None);
        assert!(State::Grace.in_r() && !State::Ending.in_r() && State::Ending.keeps_labels() && !State::Rejected.keeps_labels());
    }
}
