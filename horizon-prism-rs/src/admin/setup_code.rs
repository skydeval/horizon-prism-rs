//! The first-run setup code (admin-ui design §5.2).
//!
//! While no operator exists, `serve` holds one code: 128 bits from the OS
//! random source, shown as `prs-XXXXX-XXXXX-XXXXX-XXXXX-XXXXXX` (Crockford
//! base32). Its BLAKE3 hash and its times live in the single row of
//! `admin_setup_code`; the code itself only in a 0600 file in the data
//! directory, so it can be printed again. The code is printed at `warn`
//! (event `admin_setup_code`, the only line that carries it) at every start
//! and every 10 minutes while no operator exists. It is consumed in the
//! transaction that creates the first operator.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tokio::task::JoinHandle;

/// Crockford base32.
const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
/// The code's prefix, as shown.
const PREFIX: &str = "prs";
/// Digits in a code.
const DIGITS: usize = 26;
/// A code is good for this long from its creation.
pub const LIFETIME: Duration = Duration::from_secs(24 * 3600);
/// While no operator exists, the code is printed again this often.
pub const REPRINT: Duration = Duration::from_secs(600);
/// The file holding the code, in the data directory.
pub const FILE_NAME: &str = "admin-setup-code";

/// The file holding the code under `data_dir`.
#[must_use]
pub fn file_path(data_dir: &Path) -> PathBuf {
    data_dir.join(FILE_NAME)
}

fn encode(bytes: [u8; 16]) -> String {
    // 128 bits → 26 digits (the top digit carries 3 bits).
    let n = u128::from_be_bytes(bytes);
    (0..DIGITS).rev().map(|i| char::from(ALPHABET[usize::try_from((n >> (i * 5)) & 31).unwrap_or(0)])).collect()
}

/// A new code from the OS random source.
#[must_use]
pub fn generate() -> String {
    let digits = encode(rand::random::<[u8; 16]>());
    format!(
        "{PREFIX}-{}-{}-{}-{}-{}",
        &digits[0..5],
        &digits[5..10],
        &digits[10..15],
        &digits[15..20],
        &digits[20..26]
    )
}

/// The form a code is compared in: upper case, without hyphens and spaces
/// or the `prs` prefix, with look-alikes mapped (`O`→`0`, `I`/`L`→`1`).
#[must_use]
pub fn normalize(s: &str) -> String {
    let up: String = s
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '-')
        .map(|c| c.to_ascii_uppercase())
        .collect();
    // The prefix is dropped only from a whole code, so a code whose digits
    // begin with the same letters is never cut.
    let body = match up.strip_prefix("PRS") {
        Some(rest) if rest.len() == DIGITS => rest,
        _ => up.as_str(),
    };
    body.chars()
        .map(|c| match c {
            'O' => '0',
            'I' | 'L' => '1',
            c => c,
        })
        .collect()
}

/// Whether `code` has a code's shape: 26 Crockford digits once
/// normalized. An empty or cut-off line is not one.
#[must_use]
pub fn well_formed(code: &str) -> bool {
    let n = normalize(code);
    n.len() == DIGITS && n.bytes().all(|b| ALPHABET.contains(&b))
}

/// The stored hash of a code: BLAKE3 of its normalized form.
#[must_use]
pub fn hash(code: &str) -> [u8; 32] {
    *blake3::hash(normalize(code).as_bytes()).as_bytes()
}

/// The code in the file, if the file holds a well-formed one.
#[must_use]
pub fn read_file(path: &Path) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    let code = text.lines().next()?.trim().to_string();
    well_formed(&code).then_some(code)
}

/// Write the code to its file, mode 0600, replacing any file there.
///
/// # Errors
///
/// I/O errors.
pub fn write_file(path: &Path, code: &str) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    let _ = std::fs::remove_file(&tmp);
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp)?;
    file.write_all(format!("{code}\n").as_bytes())?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)
}

/// Delete the code's file, best effort.
pub fn delete_file(path: &Path) {
    let _ = std::fs::remove_file(path);
}

/// The code in force while setup is open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCode {
    pub code: String,
    pub expires_at: DateTime<Utc>,
}

/// A failure keeping or making the code.
#[derive(Debug, thiserror::Error)]
pub enum SetupCodeError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("writing the setup code file {path}: {source}")]
    File { path: String, source: std::io::Error },
}

/// Whether any operator exists.
///
/// # Errors
///
/// Database errors.
pub async fn operators_exist(pool: &PgPool) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM admin_operators)").fetch_one(pool).await
}

/// The stored row: hash and expiry.
async fn row(pool: &PgPool) -> Result<Option<(Vec<u8>, DateTime<Utc>)>, sqlx::Error> {
    sqlx::query_as("SELECT code_hash, expires_at FROM admin_setup_code WHERE id = 1").fetch_optional(pool).await
}

/// The code in force, read without changing anything: the file's code when
/// it is well-formed, its hash is the row's, and the row is unexpired.
///
/// # Errors
///
/// Database errors.
pub async fn current(pool: &PgPool, path: &Path) -> Result<Option<OpenCode>, sqlx::Error> {
    let Some((stored, expires_at)) = row(pool).await? else {
        return Ok(None);
    };
    let Some(code) = read_file(path) else {
        return Ok(None);
    };
    let matches = crate::admin::auth::ct_eq(&hash(&code), &stored);
    Ok((matches && expires_at > Utc::now()).then_some(OpenCode { code, expires_at }))
}

/// Startup (§5.2). With an operator: the row and the file are deleted and
/// nothing is printed. Without one: the current code is kept if the row is
/// unexpired and the file holds a well-formed code of the row's hash;
/// otherwise a new code is made, written and its hash stored. Returns the
/// code in force, for the caller to print.
///
/// # Errors
///
/// Database errors, or the file could not be written (no code is then
/// stored, so no code exists that nobody could read).
pub async fn at_startup(pool: &PgPool, path: &Path) -> Result<Option<OpenCode>, SetupCodeError> {
    if operators_exist(pool).await? {
        sqlx::query("DELETE FROM admin_setup_code").execute(pool).await?;
        delete_file(path);
        return Ok(None);
    }
    if let Some(open) = current(pool, path).await? {
        return Ok(Some(open));
    }
    let code = generate();
    write_file(path, &code).map_err(|source| SetupCodeError::File {
        path: path.display().to_string(),
        source,
    })?;
    let expires_at = Utc::now() + chrono::Duration::from_std(LIFETIME).unwrap_or_default();
    sqlx::query(
        "INSERT INTO admin_setup_code (id, code_hash, created_at, expires_at) VALUES (1, $1, now(), $2) \
         ON CONFLICT (id) DO UPDATE SET code_hash = EXCLUDED.code_hash, created_at = now(), expires_at = EXCLUDED.expires_at",
    )
    .bind(hash(&code).as_slice())
    .bind(expires_at)
    .execute(pool)
    .await?;
    Ok(Some(OpenCode { code, expires_at }))
}

/// Print the code: the one log line that carries it.
pub fn print(open: &OpenCode, public_hostname: &str) {
    let expiry = open.expires_at.format("%Y-%m-%d %H:%M UTC");
    tracing::warn!(
        event = "admin_setup_code",
        setup_code = %open.code,
        expires_at = %open.expires_at.to_rfc3339(),
        "No operator yet. Open https://{public_hostname}/admin/setup and enter this code before {expiry}: {}",
        open.code
    );
}

/// Print the code again every [`REPRINT`] while no operator exists; once
/// one does, delete the file and stop.
#[must_use]
pub fn spawn_reprint(pool: PgPool, path: PathBuf, public_hostname: String) -> JoinHandle<()> {
    spawn_reprint_every(pool, path, public_hostname, REPRINT)
}

/// [`spawn_reprint`] at another interval (tests).
#[must_use]
pub fn spawn_reprint_every(pool: PgPool, path: PathBuf, public_hostname: String, every: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(every).await;
            match operators_exist(&pool).await {
                Ok(true) => {
                    delete_file(&path);
                    return;
                }
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(event = "admin_setup_code_check_failed", %error, "checking for an operator failed");
                    continue;
                }
            }
            match current(&pool, &path).await {
                Ok(Some(open)) => print(&open, &public_hostname),
                Ok(None) => tracing::warn!(
                    event = "admin_setup_code_expired",
                    "No operator yet, and the setup code has expired. Restart Prism for a new one, or run `admin operator add`."
                ),
                Err(error) => tracing::warn!(event = "admin_setup_code_check_failed", %error, "reading the setup code failed"),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shape_and_normalization() {
        let code = generate();
        assert_eq!(code.len(), "prs-XXXXX-XXXXX-XXXXX-XXXXX-XXXXXX".len());
        assert!(code.starts_with("prs-"));
        assert!(well_formed(&code));
        let body = normalize(&code);
        assert_eq!(body.len(), 26);
        // Lower case, no hyphens, spaces, and look-alikes all match.
        assert_eq!(normalize(&code.to_lowercase()), body);
        assert_eq!(normalize(&code.replace('-', "")), body);
        assert_eq!(normalize(&code.replace('-', " ")), body);
        assert_eq!(normalize(&body), body, "the prefix is optional");
        assert_eq!(normalize("0oIl1"), "00111");
        assert_eq!(hash(&code), hash(&code.to_lowercase().replace('-', "")));
        // Not a code: empty, cut off, or a character outside the alphabet.
        for bad in ["", "prs-", &code[..20], "prs-UUUUU-UUUUU-UUUUU-UUUUU-UUUUUU"] {
            assert!(!well_formed(bad), "{bad:?}");
        }
        // Two codes differ.
        assert_ne!(generate(), generate());
    }

    #[test]
    fn a_code_whose_digits_begin_like_the_prefix_is_kept_whole() {
        let digits = "PRS00000000000000000000000";
        assert_eq!(normalize(digits), digits);
        assert_eq!(normalize(&format!("prs-{digits}")), digits);
    }

    #[test]
    fn the_file_is_private_and_a_damaged_one_holds_no_code() {
        let dir = std::env::temp_dir().join(format!("hprs-setup-code-{}", rand::random::<u32>()));
        std::fs::create_dir_all(&dir).expect("dir");
        let path = file_path(&dir);
        assert_eq!(read_file(&path), None, "missing");
        let code = generate();
        write_file(&path, &code).expect("write");
        assert_eq!(read_file(&path), Some(code.clone()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&path).expect("meta").permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        for damaged in ["", "\n", "prs-ABC\n", "garbage line\n"] {
            std::fs::write(&path, damaged).expect("damage");
            assert_eq!(read_file(&path), None, "{damaged:?}");
        }
        delete_file(&path);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
