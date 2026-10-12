//! Splitting a lookup into `checkBlocks` calls (design-v3 §5.14.4).
//!
//! A chunk holds at most 100 accounts, and its whole request target (path
//! and encoded query string) is at most 6 KB, so a page of long `did:web`
//! identifiers does not produce a request line that a proxy in front of
//! Farsight refuses.

/// Accounts in one call (`checkBlocks`'s own limit).
pub const MAX_ACCOUNTS: usize = 100;
/// Bytes of one call's request target.
pub const MAX_TARGET_BYTES: usize = 6 * 1024;
/// The method's path.
pub const CHECK_BLOCKS_PATH: &str = "/xrpc/app.nearhorizon.farsight.query.checkBlocks";

/// A value's length once form-encoded in a query string.
fn encoded_len(value: &str) -> usize {
    url::form_urlencoded::byte_serialize(value.as_bytes()).map(str::len).sum()
}

/// The request target of a call with no `others`: the path and the actor.
fn base_len(actor: &str) -> usize {
    CHECK_BLOCKS_PATH.len() + "?actor=".len() + encoded_len(actor)
}

/// `others` in order, cut into chunks that each fit one call for `actor`.
/// An account too long to fit a call by itself is left out.
#[must_use]
pub fn chunks(actor: &str, others: &[String]) -> Vec<Vec<String>> {
    let base = base_len(actor);
    let mut out: Vec<Vec<String>> = Vec::new();
    let mut current: Vec<String> = Vec::new();
    let mut len = base;
    for other in others {
        let cost = "&others=".len() + encoded_len(other);
        if base + cost > MAX_TARGET_BYTES {
            continue;
        }
        if current.len() == MAX_ACCOUNTS || len + cost > MAX_TARGET_BYTES {
            out.push(std::mem::take(&mut current));
            len = base;
        }
        current.push(other.clone());
        len += cost;
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// A chunk cut into two halves, for the one retry of an answer over the
/// response cap. A chunk of one account has no halves.
#[must_use]
pub fn halves(chunk: &[String]) -> Option<(Vec<String>, Vec<String>)> {
    (chunk.len() >= 2).then(|| {
        let (a, b) = chunk.split_at(chunk.len() / 2);
        (a.to_vec(), b.to_vec())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ACTOR: &str = "did:plc:vvvvvvvvvvvvvvvvvvvvvvvv";

    fn plc(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("did:plc:{:a>24}", format!("{i:x}").replace(['0', '1', '8', '9'], "b"))).collect()
    }

    #[test]
    fn chunks_hold_at_most_a_hundred_accounts_in_order() {
        let dids = plc(250);
        let c = chunks(ACTOR, &dids);
        assert_eq!(c.iter().map(Vec::len).collect::<Vec<_>>(), [100, 100, 50]);
        assert_eq!(c.concat(), dids);
        assert!(chunks(ACTOR, &[]).is_empty());
        assert_eq!(chunks(ACTOR, &dids[..100]).len(), 1);
    }

    #[test]
    fn long_identifiers_make_smaller_chunks() {
        // About 260 bytes each once encoded: far fewer than 100 fit in 6 KB.
        let host = format!("{}.{}.{}.example.com", "a".repeat(60), "b".repeat(60), "c".repeat(60));
        let dids: Vec<String> = (0..60).map(|i| format!("did:web:n{i}.{host}")).collect();
        let c = chunks(ACTOR, &dids);
        assert!(c.len() > 1);
        assert_eq!(c.concat(), dids);
        for chunk in &c {
            let target: usize = base_len(ACTOR) + chunk.iter().map(|d| "&others=".len() + encoded_len(d)).sum::<usize>();
            assert!(target <= MAX_TARGET_BYTES, "{target}");
            assert!(chunk.len() < 30);
        }
        // The colon of a DID costs three bytes on the wire.
        assert_eq!(encoded_len("did:plc:x"), "did%3Aplc%3Ax".len());
    }

    #[test]
    fn halves_split_a_chunk_once() {
        let dids = plc(5);
        let (a, b) = halves(&dids).expect("halves");
        assert_eq!((a.len(), b.len()), (2, 3));
        assert_eq!([a, b].concat(), dids);
        assert!(halves(&dids[..1]).is_none());
        assert!(halves(&[]).is_none());
    }
}
