//! The DIDs Farsight accepts (design-v3 §5.14.2, §5.14.5 step 1).
//!
//! Farsight takes fewer DIDs than the generic syntax allows, and one entry
//! outside its grammar fails a whole `checkBlocks` request. So every DID is
//! filtered here before a call, and a `did:web` is put in Farsight's
//! canonical form, which is then used for the call, for matching the answer,
//! and as the cache key.
//!
//! - `did:plc:` followed by exactly 24 characters of lower-case base32
//!   (`a-z`, `2-7`).
//! - `did:web:` followed by a hostname with no path. A port is accepted only
//!   for `localhost`, written `%3A<digits>`. The canonical form has the
//!   hostname in lower case and the port separator as `%3A`.

/// Longest DID the ATProto DID syntax allows.
const MAX_DID_LEN: usize = 2048;

/// The canonical form of a DID Farsight accepts, or `None` for one it would
/// reject.
#[must_use]
pub fn canonical(did: &str) -> Option<String> {
    if did.is_empty() || did.len() > MAX_DID_LEN || !did.is_ascii() {
        return None;
    }
    if let Some(id) = did.strip_prefix("did:plc:") {
        let ok = id.len() == 24 && id.bytes().all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b));
        return ok.then(|| did.to_string());
    }
    let id = did.strip_prefix("did:web:")?;
    if id.is_empty() || id.contains(':') || !id.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'%' | b'-')) {
        return None;
    }
    let (host, port) = match id.find('%') {
        Some(i) => {
            let (host, rest) = id.split_at(i);
            let port = rest.strip_prefix("%3A").or_else(|| rest.strip_prefix("%3a"))?;
            (host, Some(port))
        }
        None => (id, None),
    };
    match port {
        Some(port) => {
            let ok = host.eq_ignore_ascii_case("localhost") && !port.is_empty() && port.len() <= 5 && port.bytes().all(|b| b.is_ascii_digit());
            ok.then(|| format!("did:web:{}%3A{port}", host.to_ascii_lowercase()))
        }
        None => hostname(host).then(|| format!("did:web:{}", host.to_ascii_lowercase())),
    }
}

/// A hostname as the ATProto handle rules define one: two or more labels of
/// 1 to 63 characters of `[a-zA-Z0-9-]`, none starting or ending with `-`,
/// at most 253 characters in all, and a last label that does not start with
/// a digit.
fn hostname(host: &str) -> bool {
    if host.is_empty() || host.len() > 253 {
        return false;
    }
    let labels: Vec<&str> = host.split('.').collect();
    labels.len() >= 2
        && labels.iter().all(|l| {
            !l.is_empty()
                && l.len() <= 63
                && !l.starts_with('-')
                && !l.ends_with('-')
                && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        })
        && labels.last().is_some_and(|tld| !tld.starts_with(|c: char| c.is_ascii_digit()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plc_dids() {
        let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
        assert_eq!(canonical(did).as_deref(), Some(did));
        assert_eq!(canonical("did:plc:abcdefghijklmnopqrstuvwx").as_deref(), Some("did:plc:abcdefghijklmnopqrstuvwx"));
        assert_eq!(canonical("did:plc:234567aaaaaaaaaaaaaaaaaa").as_deref(), Some("did:plc:234567aaaaaaaaaaaaaaaaaa"));
        for bad in [
            "did:plc:aaaaaaaaaaaaaaaaaaaaaaa",   // 23
            "did:plc:aaaaaaaaaaaaaaaaaaaaaaaaa", // 25
            "did:plc:AAAAAAAAAAAAAAAAAAAAAAAA",
            "did:plc:aaaaaaaaaaaaaaaaaaaaaaa1",
            "did:plc:aaaaaaaaaaaaaaaaaaaaaaa0",
            "did:plc:",
        ] {
            assert_eq!(canonical(bad), None, "{bad}");
        }
    }

    #[test]
    fn web_dids_take_their_canonical_form() {
        assert_eq!(canonical("did:web:Example.COM").as_deref(), Some("did:web:example.com"));
        assert_eq!(canonical("did:web:a-b.example.co").as_deref(), Some("did:web:a-b.example.co"));
        assert_eq!(canonical("did:web:localhost%3a8080").as_deref(), Some("did:web:localhost%3A8080"));
        assert_eq!(canonical("did:web:LOCALHOST%3A1").as_deref(), Some("did:web:localhost%3A1"));
        for bad in [
            "did:web:example",              // one label
            "did:web:example.com:path",     // a path
            "did:web:example.com%3A8080",   // a port off localhost
            "did:web:localhost%3A",         // empty port
            "did:web:localhost%3A123456",   // port too long
            "did:web:localhost%2F",         // another escape
            "did:web:-a.example.com",
            "did:web:a.example.1com",
            "did:web:a..example.com",
            "did:web:",
            "did:web:ex_ample.com",
        ] {
            assert_eq!(canonical(bad), None, "{bad}");
        }
    }

    #[test]
    fn everything_else_is_rejected() {
        for bad in ["", "did:key:z6Mk", "did:example:123", "alice.example.com", "did:plc", "did:web:exämple.com"] {
            assert_eq!(canonical(bad), None, "{bad}");
        }
        assert_eq!(canonical(&format!("did:web:{}.com", "a".repeat(2100))), None);
    }
}
