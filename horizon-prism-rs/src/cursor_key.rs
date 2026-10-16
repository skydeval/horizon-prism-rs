//! The sealed-cursor key (design-v3 §5.15.14; private posting §3.6),
//! shared by Thread Lens and the private tier: one implementation, two
//! keys, so either surface's key can rotate alone.
//!
//! A cursor is a position encrypted and authenticated with a server key
//! (ChaCha20-Poly1305, a random nonce per cursor) and bound to associated
//! data the caller names, so a client can neither read it, nor alter it,
//! nor use it under another binding. A cursor grants nothing: whatever it
//! leads to goes through the same checks as on a first request.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// A cursor is at most this long as sent.
pub const MAX_CURSOR_LEN: usize = 1024;
const NONCE_LEN: usize = 12;

/// Seals and opens cursors. Cheap to clone.
#[derive(Clone)]
pub struct CursorKey {
    cipher: ChaCha20Poly1305,
}

impl std::fmt::Debug for CursorKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CursorKey(<redacted>)")
    }
}

impl CursorKey {
    #[must_use]
    pub fn new(key: &[u8; 32]) -> Self {
        Self {
            cipher: ChaCha20Poly1305::new(Key::from_slice(key)),
        }
    }

    /// A key that lives as long as the process (Thread Lens without a
    /// configured key). The private tier never uses one (C-14).
    #[must_use]
    pub fn generated() -> Self {
        Self::new(&rand::random::<[u8; 32]>())
    }

    /// Seal `position`, bound to `aad`. `None` when the cursor would be
    /// longer than a cursor may be.
    #[must_use]
    pub fn seal<P: Serialize>(&self, aad: &str, position: &P) -> Option<String> {
        let plain = serde_json::to_vec(position).ok()?;
        let nonce: [u8; NONCE_LEN] = rand::random();
        let sealed = self.cipher.encrypt(Nonce::from_slice(&nonce), Payload { msg: &plain, aad: aad.as_bytes() }).ok()?;
        let mut bytes = nonce.to_vec();
        bytes.extend(sealed);
        let cursor = URL_SAFE_NO_PAD.encode(bytes);
        (cursor.len() <= MAX_CURSOR_LEN).then_some(cursor)
    }

    /// Open a cursor presented with `aad`. `None` when it fails
    /// authentication, was issued under other associated data or another
    /// key, or is not a cursor at all.
    #[must_use]
    pub fn open<P: DeserializeOwned>(&self, aad: &str, cursor: &str) -> Option<P> {
        if cursor.len() > MAX_CURSOR_LEN {
            return None;
        }
        let bytes = URL_SAFE_NO_PAD.decode(cursor).ok()?;
        if bytes.len() <= NONCE_LEN {
            return None;
        }
        let (nonce, sealed) = bytes.split_at(NONCE_LEN);
        let plain = self.cipher.decrypt(Nonce::from_slice(nonce), Payload { msg: sealed, aad: aad.as_bytes() }).ok()?;
        serde_json::from_slice(&plain).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two keys made from two configured values do not open each other's
    /// cursors: Thread Lens's key and the private tier's are independent.
    #[test]
    fn keys_are_independent() {
        let (lens, private) = (CursorKey::new(&[1; 32]), CursorKey::new(&[2; 32]));
        let cursor = private.seal("m|did|a|f", &("2026-10-15T00:00:00Z", "3kkey")).expect("sealed");
        assert_eq!(private.open::<(String, String)>("m|did|a|f", &cursor), Some(("2026-10-15T00:00:00Z".into(), "3kkey".into())));
        assert_eq!(lens.open::<(String, String)>("m|did|a|f", &cursor), None);
        assert_eq!(private.open::<(String, String)>("m|other|a|f", &cursor), None);
        // The same configured value opens across instances (a restart, or
        // another replica).
        assert!(CursorKey::new(&[2; 32]).open::<(String, String)>("m|did|a|f", &cursor).is_some());
    }
}
