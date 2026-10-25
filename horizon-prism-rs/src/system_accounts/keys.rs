//! The system-accounts keys (design §1 D-2 to D-4): four subkeys derived from
//! `APP_SYSTEM_ACCOUNTS_KEY_HEX` with BLAKE3's key derivation, one per use,
//! so no use can stand in for another.
//!
//! - **index**: BLAKE3 in keyed mode over a listed account's DID, the
//!   reverse lookup; deterministic, opaque without the key.
//! - **seal**: ChaCha20-Poly1305 with a random nonce per entry, the
//!   associated data binding the sealed DID to its owner and its index.
//! - **token**: keyed hashes of used tokens (the once-only rule).
//! - **check**: a value stored at first start, so another key is caught.

use std::sync::Arc;

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};

const CONTEXT_SEAL: &str = "horizon-prism-rs 2026-10-24 system accounts seal";
const CONTEXT_INDEX: &str = "horizon-prism-rs 2026-10-24 system accounts index";
const CONTEXT_TOKEN: &str = "horizon-prism-rs 2026-10-24 system accounts token";
const CONTEXT_CHECK: &str = "horizon-prism-rs 2026-10-24 system accounts check";
const AAD_PREFIX: &[u8] = b"sysacct-entry-v1\0";
const NONCE_LEN: usize = 12;

struct Inner {
    seal: ChaCha20Poly1305,
    index: [u8; 32],
    token: [u8; 32],
    check: [u8; 32],
}

/// The derived keys. Cheap to clone; `Debug` prints nothing of them.
#[derive(Clone)]
pub struct Keys(Arc<Inner>);

impl std::fmt::Debug for Keys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Keys(<redacted>)")
    }
}

impl Keys {
    #[must_use]
    pub fn new(secret: &[u8; 32]) -> Self {
        let seal = blake3::derive_key(CONTEXT_SEAL, secret);
        Self(Arc::new(Inner {
            seal: ChaCha20Poly1305::new(Key::from_slice(&seal)),
            index: blake3::derive_key(CONTEXT_INDEX, secret),
            token: blake3::derive_key(CONTEXT_TOKEN, secret),
            check: blake3::derive_key(CONTEXT_CHECK, secret),
        }))
    }

    /// The reverse-lookup index of a listed account's DID.
    #[must_use]
    pub fn index(&self, did: &str) -> [u8; 32] {
        *blake3::keyed_hash(&self.0.index, did.as_bytes()).as_bytes()
    }

    /// A keyed hash of a used token's identifying bytes.
    #[must_use]
    pub(crate) fn token_hash(&self, material: &[u8]) -> [u8; 32] {
        *blake3::keyed_hash(&self.0.token, material).as_bytes()
    }

    /// The value stored to recognise this key.
    #[must_use]
    pub(crate) fn check(&self) -> [u8; 32] {
        self.0.check
    }

    /// `did` sealed for `owner`'s entry `index`: nonce ‖ ciphertext. `None`
    /// only if the cipher refuses, which it does not for a DID's length.
    #[must_use]
    pub(crate) fn seal(&self, owner: &str, index: &[u8; 32], did: &str) -> Option<Vec<u8>> {
        let nonce: [u8; NONCE_LEN] = rand::random();
        let aad = aad(owner, index);
        let sealed = self
            .0
            .seal
            .encrypt(Nonce::from_slice(&nonce), Payload { msg: did.as_bytes(), aad: &aad })
            .ok()?;
        let mut out = nonce.to_vec();
        out.extend(sealed);
        Some(out)
    }

    /// The DID sealed for `owner`'s entry `index`. `None` when it fails
    /// authentication: another key, owner or index, or altered bytes.
    #[must_use]
    pub(crate) fn open(&self, owner: &str, index: &[u8], sealed: &[u8]) -> Option<String> {
        let index: &[u8; 32] = index.try_into().ok()?;
        if sealed.len() <= NONCE_LEN {
            return None;
        }
        let (nonce, body) = sealed.split_at(NONCE_LEN);
        let aad = aad(owner, index);
        let plain = self.0.seal.decrypt(Nonce::from_slice(nonce), Payload { msg: body, aad: &aad }).ok()?;
        String::from_utf8(plain).ok()
    }
}

fn aad(owner: &str, index: &[u8; 32]) -> Vec<u8> {
    let mut aad = AAD_PREFIX.to_vec();
    aad.extend_from_slice(owner.as_bytes());
    aad.push(0);
    aad.extend_from_slice(index);
    aad
}
