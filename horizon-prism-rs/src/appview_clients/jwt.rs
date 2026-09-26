//! Outbound service JWTs (design-v2 §6.8, §8.1 `service_jwt` mode).
//!
//! `iss = did:web:<host>`, `aud` = the target network's `appview_did`,
//! `lxm` = the called method, `exp = iat + 60 s`, a random 16-byte `jti`,
//! ES256K with a low-S signature, signed by `APP_SERVICE_SIGNING_KEY_HEX`.
//! No user identity is carried: the upstream sees the AppView itself as the
//! viewer, which has no blocks.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::json;

use crate::kryphocron_glue::ServiceKey;

/// Token lifetime.
pub const TTL_SECS: i64 = 60;

/// Mint a service JWT for one call of `lxm` to `aud`, issued at `now`
/// (Unix seconds).
#[must_use]
pub fn mint(key: &ServiceKey, aud: &str, lxm: &str, now: i64) -> String {
    let header = URL_SAFE_NO_PAD.encode(br#"{"typ":"JWT","alg":"ES256K"}"#);
    let jti: [u8; 16] = rand::random();
    let claims = json!({
        "iss": key.did(),
        "aud": aud,
        "lxm": lxm,
        "iat": now,
        "exp": now + TTL_SECS,
        "jti": hex::encode(jti),
    });
    let payload = URL_SAFE_NO_PAD.encode(claims.to_string());
    let signing_input = format!("{header}.{payload}");
    let signature = key.sign_es256k(signing_input.as_bytes());
    format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(signature))
}

#[cfg(test)]
mod tests {
    use k256::ecdsa::signature::Verifier;
    use k256::ecdsa::{Signature, SigningKey, VerifyingKey};
    use serde_json::Value;

    use super::*;
    use crate::kryphocron_glue::jwt::tests::{AUDIENCE_HOST, StubFetcher, verifier};

    const CALLER_HOST: &str = "prism.example.com";
    const SECRET: [u8; 32] = [11u8; 32];
    const METHOD: &str = "app.bsky.feed.getTimeline";

    fn base58_decode(s: &str) -> Vec<u8> {
        const ALPHABET: &str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
        let mut out: Vec<u8> = Vec::new();
        for c in s.chars() {
            let mut carry = u32::try_from(ALPHABET.find(c).expect("base58 char")).expect("small");
            for b in out.iter_mut().rev() {
                carry += u32::from(*b) * 58;
                *b = u8::try_from(carry & 0xff).expect("byte");
                carry >>= 8;
            }
            while carry > 0 {
                out.insert(0, u8::try_from(carry & 0xff).expect("byte"));
                carry >>= 8;
            }
        }
        let zeros = s.chars().take_while(|c| *c == '1').count();
        [vec![0; zeros], out].concat()
    }

    /// The verifying key published in `did.json`.
    fn did_json_key(key: &ServiceKey) -> VerifyingKey {
        let doc = key.did_document();
        let mb = doc["verificationMethod"][0]["publicKeyMultibase"].as_str().expect("multibase");
        let bytes = base58_decode(mb.strip_prefix('z').expect("base58btc"));
        let compressed = bytes.strip_prefix(&[0xe7, 0x01][..]).expect("secp256k1 multicodec");
        VerifyingKey::from_sec1_bytes(compressed).expect("key")
    }

    #[test]
    fn claims_and_low_s_signature_verify_against_did_json() {
        let key = ServiceKey::new(CALLER_HOST, &SECRET).expect("key");
        let now = 1_790_000_000;
        let token = mint(&key, "did:web:api.community-b.example", "app.bsky.feed.getPosts", now);
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
        let header: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).expect("b64")).expect("json");
        assert_eq!(header, json!({"typ": "JWT", "alg": "ES256K"}));
        let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).expect("b64")).expect("json");
        assert_eq!(claims["iss"], format!("did:web:{CALLER_HOST}"));
        assert_eq!(claims["aud"], "did:web:api.community-b.example");
        assert_eq!(claims["lxm"], "app.bsky.feed.getPosts");
        assert_eq!(claims["iat"], now);
        assert_eq!(claims["exp"], now + 60);
        let jti = claims["jti"].as_str().expect("jti");
        assert_eq!(hex::decode(jti).expect("hex").len(), 16);
        assert_ne!(jti, serde_json::from_slice::<Value>(&URL_SAFE_NO_PAD.decode(mint(&key, "a", "b", now).split('.').nth(1).expect("payload")).expect("b64")).expect("json")["jti"].as_str().expect("jti"));

        let signature = Signature::from_slice(&URL_SAFE_NO_PAD.decode(parts[2]).expect("b64")).expect("r||s");
        assert!(signature.normalize_s().is_none(), "signature must be low-S");
        did_json_key(&key)
            .verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &signature)
            .expect("verifies with the did.json key");
    }

    #[test]
    fn signatures_are_always_low_s() {
        let key = ServiceKey::new(CALLER_HOST, &SECRET).expect("key");
        for i in 0..64u32 {
            let token = mint(&key, "did:web:x", &format!("m{i}"), i64::from(i));
            let sig = Signature::from_slice(&URL_SAFE_NO_PAD.decode(token.rsplit('.').next().expect("sig")).expect("b64"))
                .expect("sig");
            assert!(sig.normalize_s().is_none(), "token {i} is high-S");
        }
    }

    #[tokio::test]
    async fn round_trips_through_the_kryphocron_verifier() {
        // Another AppView's verifier, resolving our did:web to the key our
        // did.json publishes, accepts the token.
        let key = ServiceKey::new(CALLER_HOST, &SECRET).expect("key");
        let fetcher = StubFetcher::default();
        fetcher.set_key(key.did(), SigningKey::from_slice(&SECRET).expect("signing"));
        let (verifier, _drainer) = verifier(fetcher, 10, 4);
        let token = mint(&key, &format!("did:web:{AUDIENCE_HOST}"), METHOD, chrono::Utc::now().timestamp());
        let caller = verifier
            .verify_request_jwt(Some(&format!("Bearer {token}")), METHOD)
            .await
            .expect("accepted");
        assert_eq!(caller.did, key.did());
        // lxm binds the method.
        assert!(verifier.verify_request_jwt(Some(&format!("Bearer {token}")), "app.bsky.feed.getPosts").await.is_err());
    }
}
