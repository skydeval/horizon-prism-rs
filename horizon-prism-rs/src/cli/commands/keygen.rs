//! `admin keygen` (design-v2 §3.3, §6.8): a fresh secp256k1 private key hex
//! (`APP_SERVICE_SIGNING_KEY_HEX`), its did:key multibase, and a 32-byte
//! `APP_CORRELATION_KEY_HEX`. No configuration is read and nothing is
//! written.

/// Render the three lines.
#[must_use]
pub fn render() -> String {
    let (secret, did_key, correlation) = crate::kryphocron_glue::keygen();
    format!("APP_SERVICE_SIGNING_KEY_HEX={secret}\n# public key: {did_key}\nAPP_CORRELATION_KEY_HEX={correlation}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prints_env_lines_and_the_did_key() {
        let out = render();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 3);
        let secret = lines[0].strip_prefix("APP_SERVICE_SIGNING_KEY_HEX=").expect("signing key line");
        assert_eq!(secret.len(), 64);
        assert!(secret.bytes().all(|b| b.is_ascii_hexdigit()));
        assert!(lines[1].starts_with("# public key: did:key:zQ3s"), "{}", lines[1]);
        let corr = lines[2].strip_prefix("APP_CORRELATION_KEY_HEX=").expect("correlation line");
        assert_eq!(corr.len(), 64);
        assert_ne!(render(), out, "fresh keys every run");
    }
}
