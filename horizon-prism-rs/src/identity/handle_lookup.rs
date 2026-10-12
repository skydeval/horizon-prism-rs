//! Finding the DID a handle names, with the reason kept when it cannot be
//! found (design-v3 §5.11).
//!
//! A handle names its DID in one of two places: a DNS TXT record at
//! `_atproto.<handle>`, or the file `https://<handle>/.well-known/atproto-did`.
//! Each lookup ends in one of three ways:
//! - [`Step::Found`]: it named a DID;
//! - [`Step::Confirmed`]: the place was reached and names no DID (the name
//!   does not exist, the file is not there);
//! - [`Step::Transient`]: nothing can be said (a timeout, a server error).
//!
//! The difference matters to the caller: "confirmed" is "no such handle",
//! and may be remembered for a while; "transient" is "try again", and must
//! not be. **Anything not listed as confirmed is transient.**

use std::error::Error;
use std::time::Duration;

use async_trait::async_trait;
use proto_blue::syntax::Did;

use crate::net::resolver::{AllAddressesBlocked, HickoryLookup, TxtAnswer, no_such_name};
use crate::net::{BodyCap, NetClient, NetError, RequestOptions};

/// The well-known file is one line.
const WELL_KNOWN_BODY_CAP: BodyCap = BodyCap(2048);
const WELL_KNOWN_TIMEOUT: Duration = Duration::from_secs(3);

/// How one lookup ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    Found(String),
    Confirmed,
    Transient,
}

/// A source of TXT records (DNS in production).
#[async_trait]
pub trait TxtSource: Send + Sync {
    async fn txt(&self, name: &str) -> TxtAnswer;
}

#[async_trait]
impl TxtSource for HickoryLookup {
    async fn txt(&self, name: &str) -> TxtAnswer {
        HickoryLookup::txt(self, name).await
    }
}

/// The TXT lookup's outcome. One `did=` record with a valid DID is found.
/// None, more than one (as the reference resolver), or a value that is not
/// a DID, is confirmed.
#[must_use]
pub fn classify_txt(answer: TxtAnswer) -> Step {
    match answer {
        TxtAnswer::Failed => Step::Transient,
        TxtAnswer::NoSuchName => Step::Confirmed,
        TxtAnswer::Records(records) => {
            let mut dids = records.iter().filter_map(|r| r.strip_prefix("did="));
            match (dids.next(), dids.next()) {
                (Some(did), None) if Did::is_valid(did) => Step::Found(did.to_string()),
                _ => Step::Confirmed,
            }
        }
    }
}

/// The well-known fetch's outcome from its response.
#[must_use]
pub fn classify_response(status: u16, body: &[u8]) -> Step {
    match status {
        200..=299 => {
            let line = String::from_utf8_lossy(body);
            let did = line.lines().next().unwrap_or_default().trim();
            if Did::is_valid(did) { Step::Found(did.to_string()) } else { Step::Confirmed }
        }
        408 | 429 => Step::Transient,
        400..=499 => Step::Confirmed,
        _ => Step::Transient,
    }
}

/// Whether anything in `error`'s chain of causes shows that the host is
/// not there to answer: its name does not resolve, every address of it is
/// blocked, it refuses connections, or its certificate is not valid.
fn host_is_not_there(error: &(dyn Error + 'static)) -> bool {
    let mut cause: Option<&(dyn Error + 'static)> = Some(error);
    while let Some(e) = cause {
        if e.downcast_ref::<AllAddressesBlocked>().is_some() {
            return true;
        }
        if let Some(dns) = e.downcast_ref::<hickory_resolver::net::NetError>() {
            return no_such_name(dns);
        }
        if matches!(e.downcast_ref::<rustls::Error>(), Some(rustls::Error::InvalidCertificate(_))) {
            return true;
        }
        if let Some(io) = e.downcast_ref::<std::io::Error>() {
            if io.kind() == std::io::ErrorKind::ConnectionRefused {
                return true;
            }
            // An `io::Error` can carry its cause inside, off the chain.
            if let Some(inner) = io.get_ref()
                && host_is_not_there(inner)
            {
                return true;
            }
        }
        cause = e.source();
    }
    false
}

/// The well-known fetch's outcome from a failure to get a response.
#[must_use]
pub fn classify_failure(error: &NetError) -> Step {
    let confirmed = match error {
        // Too many redirects, or one to a blocked address.
        NetError::Redirect(_) | NetError::BlockedAddress { .. } => true,
        NetError::Connect(cause) => host_is_not_there(cause),
        _ => false,
    };
    if confirmed { Step::Confirmed } else { Step::Transient }
}

/// The DID to verify, if either lookup found one. When both did and they
/// differ, the TXT record wins, as the reference resolver prefers DNS.
#[must_use]
pub fn candidate<'a>(txt: &'a Step, well_known: &'a Step) -> Option<&'a str> {
    match (txt, well_known) {
        (Step::Found(did), _) | (_, Step::Found(did)) => Some(did),
        _ => None,
    }
}

/// With no DID found: confirmed only when both lookups are.
#[must_use]
pub fn neither_found(txt: &Step, well_known: &Step) -> Step {
    if *txt == Step::Confirmed && *well_known == Step::Confirmed { Step::Confirmed } else { Step::Transient }
}

/// The two lookups for one handle.
pub struct HandleProbe<'a> {
    pub txt: &'a dyn TxtSource,
    pub net: &'a NetClient,
}

impl HandleProbe<'_> {
    /// Both lookups, at once: `(txt, well_known)`.
    pub async fn run(&self, handle: &str) -> (Step, Step) {
        let name = format!("_atproto.{handle}");
        let url = format!("https://{handle}/.well-known/atproto-did");
        let options = RequestOptions::new(WELL_KNOWN_BODY_CAP).timeout(WELL_KNOWN_TIMEOUT);
        let (txt, well_known) = futures::join!(self.txt.txt(&name), self.net.get(&url, &options));
        let well_known = match well_known {
            Ok(response) => classify_response(response.status.as_u16(), &response.body),
            Err(error) => classify_failure(&error),
        };
        (classify_txt(txt), well_known)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DID: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    const OTHER: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

    fn records(r: &[&str]) -> TxtAnswer {
        TxtAnswer::Records(r.iter().map(|s| (*s).to_string()).collect())
    }

    #[test]
    fn a_txt_answer_is_found_confirmed_or_transient() {
        assert_eq!(classify_txt(records(&["v=spf1 -all", &format!("did={DID}")])), Step::Found(DID.into()));
        // No `did=` record, two of them, a value that is not a DID, no name.
        assert_eq!(classify_txt(records(&["v=spf1 -all"])), Step::Confirmed);
        assert_eq!(classify_txt(records(&[&format!("did={DID}"), &format!("did={OTHER}")])), Step::Confirmed);
        assert_eq!(classify_txt(records(&["did=nonsense"])), Step::Confirmed);
        assert_eq!(classify_txt(TxtAnswer::NoSuchName), Step::Confirmed);
        // A lookup that failed says nothing.
        assert_eq!(classify_txt(TxtAnswer::Failed), Step::Transient);
    }

    #[test]
    fn a_well_known_response_is_classified_by_status_and_body() {
        // The first line, trimmed.
        assert_eq!(classify_response(200, format!("  {DID} \nmore\n").as_bytes()), Step::Found(DID.into()));
        assert_eq!(classify_response(200, b"<html>not a did</html>"), Step::Confirmed);
        assert_eq!(classify_response(404, b""), Step::Confirmed);
        assert_eq!(classify_response(403, b""), Step::Confirmed);
        for transient in [408, 429, 500, 502, 503, 301] {
            assert_eq!(classify_response(transient, b""), Step::Transient, "{transient}");
        }
    }

    #[test]
    fn a_failure_to_connect_is_confirmed_only_for_listed_causes() {
        assert_eq!(classify_failure(&NetError::Timeout), Step::Transient);
        assert_eq!(classify_failure(&NetError::BlockedAddress { host: "10.0.0.1".into() }), Step::Confirmed);
        assert_eq!(classify_failure(&NetError::BodyTooLarge { cap: 1 }), Step::Transient);
        // The causes a connection error can carry.
        let refused = std::io::Error::from(std::io::ErrorKind::ConnectionRefused);
        assert!(host_is_not_there(&refused));
        let reset = std::io::Error::from(std::io::ErrorKind::ConnectionReset);
        assert!(!host_is_not_there(&reset));
        let blocked = std::io::Error::other(AllAddressesBlocked { host: "h".into() });
        assert!(host_is_not_there(&blocked));
        let certificate = std::io::Error::other(rustls::Error::InvalidCertificate(rustls::CertificateError::Expired));
        assert!(host_is_not_there(&certificate));
        let handshake = std::io::Error::other(rustls::Error::HandshakeNotComplete);
        assert!(!host_is_not_there(&handshake));
    }

    #[test]
    fn the_txt_record_wins_and_only_two_confirmations_confirm() {
        let (txt, wk) = (Step::Found(DID.into()), Step::Found(OTHER.into()));
        assert_eq!(candidate(&txt, &wk), Some(DID));
        assert_eq!(candidate(&Step::Transient, &wk), Some(OTHER));
        assert_eq!(candidate(&Step::Confirmed, &Step::Transient), None);
        assert_eq!(neither_found(&Step::Confirmed, &Step::Confirmed), Step::Confirmed);
        assert_eq!(neither_found(&Step::Confirmed, &Step::Transient), Step::Transient);
        assert_eq!(neither_found(&Step::Transient, &Step::Transient), Step::Transient);
    }
}
