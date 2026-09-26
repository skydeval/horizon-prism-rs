//! The AppView's service identity (design-v2 §6.8): its did:web DID, its
//! secp256k1 signing key (`APP_SERVICE_SIGNING_KEY_HEX`), the Multikey
//! encoding published in `did.json`, and the `ServiceIdentity` passed to
//! `verify_jwt` as `local_audience`.

use k256::ecdsa::{SigningKey, VerifyingKey};
use kryphocron::{Did, KeyId, PublicKey, ServiceIdentity, SignatureAlgorithm};
use serde_json::{Value, json};

/// Multicodec varint prefix for a secp256k1 public key (0xe7).
pub const SECP256K1_PUB_PREFIX: [u8; 2] = [0xe7, 0x01];

const BASE58_ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

/// Base58btc (Bitcoin alphabet).
#[must_use]
pub fn base58btc(bytes: &[u8]) -> String {
    let zeros = bytes.iter().take_while(|b| **b == 0).count();
    // Little-endian base-58 digits.
    let mut digits: Vec<u8> = Vec::with_capacity(bytes.len() * 138 / 100 + 1);
    for &byte in &bytes[zeros..] {
        let mut carry = u32::from(byte);
        for d in &mut digits {
            carry += u32::from(*d) << 8;
            *d = u8::try_from(carry % 58).unwrap_or(0);
            carry /= 58;
        }
        while carry > 0 {
            digits.push(u8::try_from(carry % 58).unwrap_or(0));
            carry /= 58;
        }
    }
    let mut out = String::with_capacity(zeros + digits.len());
    for _ in 0..zeros {
        out.push('1');
    }
    for d in digits.iter().rev() {
        out.push(char::from(BASE58_ALPHABET[usize::from(*d)]));
    }
    out
}

/// The Multikey `publicKeyMultibase` for a compressed secp256k1 key:
/// `z` + base58btc(0xe7 0x01 ‖ key).
#[must_use]
pub fn multikey(compressed: &[u8]) -> String {
    let mut buf = Vec::with_capacity(2 + compressed.len());
    buf.extend_from_slice(&SECP256K1_PUB_PREFIX);
    buf.extend_from_slice(compressed);
    format!("z{}", base58btc(&buf))
}

/// A service key failure.
#[derive(Debug, thiserror::Error)]
pub enum ServiceKeyError {
    #[error("APP_SERVICE_SIGNING_KEY_HEX is not a valid secp256k1 secret key")]
    InvalidKey,
    #[error("APP_PUBLIC_HOSTNAME does not form a valid did:web: {0}")]
    InvalidDid(String),
}

/// The AppView's signing identity.
#[derive(Clone)]
pub struct ServiceKey {
    hostname: String,
    did: String,
    signing: SigningKey,
}

impl std::fmt::Debug for ServiceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceKey").field("did", &self.did).finish_non_exhaustive()
    }
}

impl ServiceKey {
    /// # Errors
    ///
    /// An invalid secret or hostname.
    pub fn new(hostname: &str, secret: &[u8; 32]) -> Result<Self, ServiceKeyError> {
        let signing = SigningKey::from_slice(secret).map_err(|_| ServiceKeyError::InvalidKey)?;
        let did = format!("did:web:{hostname}");
        Did::new(&did).map_err(|e| ServiceKeyError::InvalidDid(format!("{e:?}")))?;
        Ok(Self {
            hostname: hostname.to_string(),
            did,
            signing,
        })
    }

    #[must_use]
    pub fn did(&self) -> &str {
        &self.did
    }

    /// The 33-byte compressed public key.
    #[must_use]
    pub fn compressed_public_key(&self) -> Vec<u8> {
        VerifyingKey::from(&self.signing)
            .to_encoded_point(true)
            .as_bytes()
            .to_vec()
    }

    #[must_use]
    pub fn public_key_multibase(&self) -> String {
        multikey(&self.compressed_public_key())
    }

    /// `did:key` form of the public key (`admin keygen`).
    #[must_use]
    pub fn did_key(&self) -> String {
        format!("did:key:{}", self.public_key_multibase())
    }

    /// The `/.well-known/did.json` document (§6.8).
    #[must_use]
    pub fn did_document(&self) -> Value {
        let did = &self.did;
        json!({
            "@context": ["https://www.w3.org/ns/did/v1", "https://w3id.org/security/multikey/v1"],
            "id": did,
            "verificationMethod": [{
                "id": format!("{did}#atproto"),
                "type": "Multikey",
                "controller": did,
                "publicKeyMultibase": self.public_key_multibase(),
            }],
            "service": [{
                "id": "#bsky_appview",
                "type": "BskyAppView",
                "serviceEndpoint": format!("https://{}", self.hostname),
            }],
        })
    }

    /// `local_audience` for `verify_jwt` (§6.8): the real DID, a key id
    /// derived from the compressed key, and a deliberate zero placeholder key
    /// (`verify_jwt` reads only the service DID).
    ///
    /// # Errors
    ///
    /// An invalid DID (checked at construction, so unreachable in practice).
    pub fn service_identity(&self) -> Result<ServiceIdentity, ServiceKeyError> {
        let did = Did::new(&self.did).map_err(|e| ServiceKeyError::InvalidDid(format!("{e:?}")))?;
        let key_id = KeyId::from_bytes(*blake3::hash(&self.compressed_public_key()).as_bytes());
        Ok(ServiceIdentity::new(
            did,
            key_id,
            PublicKey::new(SignatureAlgorithm::Es256K, [0u8; 32]),
            None,
        ))
    }
}

/// A fresh key set for `admin keygen`: (signing key hex, did:key, correlation
/// key hex).
#[must_use]
pub fn keygen() -> (String, String, String) {
    // A random 32-byte string is a valid scalar except with negligible
    // probability; retry in that case.
    let (secret, key) = loop {
        let secret: [u8; 32] = rand::random();
        if let Ok(key) = ServiceKey::new("keygen.invalid", &secret) {
            break (secret, key);
        }
    };
    let correlation: [u8; 32] = rand::random();
    (hex::encode(secret), key.did_key(), hex::encode(correlation))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base58_vectors() {
        assert_eq!(base58btc(b""), "");
        assert_eq!(base58btc(&[0, 0, 1]), "112");
        assert_eq!(base58btc(b"hello world"), "StV1DL6CwTryKyV");
        assert_eq!(base58btc(&[0xff; 4]), "7YXq9G");
    }

    #[test]
    fn secp256k1_multikeys_have_the_zq3s_prefix() {
        // Any 0xe7 0x01-prefixed 33-byte compressed key encodes as `zQ3s…`
        // (the ATProto secp256k1 did:key prefix).
        let key = ServiceKey::new("appview.example.com", &[9u8; 32]).expect("key");
        let compressed = key.compressed_public_key();
        assert_eq!(compressed.len(), 33);
        let mb = multikey(&compressed);
        assert!(mb.starts_with("zQ3s"), "{mb}");
        assert!(mb.len() > 45 && mb.len() < 52, "{mb}");
    }

    #[test]
    fn did_document_shape_and_identity() {
        let key = ServiceKey::new("appview.example.com", &[7u8; 32]).expect("key");
        let doc = key.did_document();
        assert_eq!(doc["id"], "did:web:appview.example.com");
        assert_eq!(doc["verificationMethod"][0]["id"], "did:web:appview.example.com#atproto");
        assert_eq!(doc["verificationMethod"][0]["type"], "Multikey");
        let mb = doc["verificationMethod"][0]["publicKeyMultibase"].as_str().expect("mb");
        assert!(mb.starts_with("zQ3s"), "{mb}");
        assert_eq!(doc["service"][0]["id"], "#bsky_appview");
        assert_eq!(doc["service"][0]["serviceEndpoint"], "https://appview.example.com");
        // §6.8: the audience identity's DID equals the did.json id.
        let identity = key.service_identity().expect("identity");
        assert_eq!(identity.service_did().as_str(), doc["id"].as_str().expect("id"));
        assert!(ServiceKey::new("appview.example.com", &[0u8; 32]).is_err());
    }

    #[test]
    fn keygen_outputs() {
        let (secret, did_key, correlation) = keygen();
        assert_eq!(secret.len(), 64);
        assert_eq!(correlation.len(), 64);
        assert!(did_key.starts_with("did:key:zQ3s"), "{did_key}");
    }
}
