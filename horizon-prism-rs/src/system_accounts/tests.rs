//! System-accounts tests (design §11).

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use k256::ecdsa::SigningKey;
use serde_json::{Value, json};
use tokio::net::TcpListener;

use super::store::test_rows;
use super::tokens::{lifetime_ok, used_token};
use super::*;
use crate::aggregations::ScopeSets;
use crate::hydration::testing::hydrator;
use crate::honest_501::PublicMessages;
use crate::kryphocron_glue::jwt::tests::{AUDIENCE_HOST, StubFetcher, mint_claims, signing_key, verifier};
use crate::test_support::{TestDb, fresh_db};
use crate::xrpc::router::{FinalConfig, finalize};
use crate::xrpc::{AppState, Authenticator, RateLimits};

const A: &str = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
const C: &str = "did:plc:cccccccccccccccccccccccc";
const D: &str = "did:plc:dddddddddddddddddddddddd";
const SECRET: [u8; 32] = [9; 32];

fn aud() -> String {
    format!("did:web:{AUDIENCE_HOST}")
}

fn key_of(did: &str) -> SigningKey {
    // One key per DID, from its bytes.
    signing_key(did.bytes().fold(7u8, |a, b| a.wrapping_mul(31).wrapping_add(b)).max(2))
}

/// A service token for `iss` and `lxm`, living `lifetime` seconds, with a
/// fresh random `jti` unless one is given (`Some("")`: none).
fn token_with(iss: &str, lxm: &str, lifetime: i64, jti: Option<&str>, aud: &str) -> String {
    let now = chrono::Utc::now().timestamp();
    let mut claims = json!({"iss": iss, "aud": aud, "lxm": lxm, "iat": now, "exp": now + lifetime});
    match jti {
        None => claims["jti"] = json!(hex::encode(rand::random::<[u8; 16]>())),
        Some("") => {}
        Some(j) => claims["jti"] = json!(j),
    }
    mint_claims(&key_of(iss), &claims)
}

fn token(iss: &str, lxm: &str) -> String {
    token_with(iss, lxm, 60, None, &aud())
}

fn proof(iss: &str) -> String {
    token(iss, CONFIRM_SYSTEM_ACCOUNT)
}

struct World {
    db: TestDb,
    base: String,
    client: reqwest::Client,
    keys: Keys,
    app: AppState,
    _drainer: Box<dyn Send>,
}

fn roomy() -> RateLimits {
    let cfg = crate::config::FileConfig::parse(concat!(
        "[rate_limits.system_accounts]\nper_sec = 1000.0\nburst = 10000\n",
        "[rate_limits.anon_read]\nper_sec = 1000.0\nburst = 10000\n",
        "[rate_limits.global]\nper_sec = 1000.0\nburst = 10000\n",
    ))
    .expect("parse");
    RateLimits::from_config(&cfg.rate_limits)
}

/// A served app with the feature on (or off), every DID in `dids` holding
/// its own key.
async fn world(name: &str, enabled: bool, dids: &[&str]) -> Option<World> {
    let db = fresh_db(name).await?;
    let fetcher = StubFetcher::default();
    for d in dids {
        fetcher.set_key(d, key_of(d));
    }
    let (auth, drainer) = verifier(fetcher, 100, 16);
    let app = AppState {
        hydrator: hydrator(&db.pool, ScopeSets::default()),
        auth: Arc::new(auth),
        limits: roomy(),
        enroller: None,
        trusted_proxy: "172.30.0.10".parse().expect("ip"),
        did_document: Arc::new(json!({})),
        membership: crate::xrpc::handlers::membership::MembershipSettings::default().with_system_accounts(enabled),
    };
    let secret = crate::config::Secret32::from_hex(&hex::encode(SECRET)).expect("secret");
    let keys = at_startup(&db.pool, enabled.then_some(&secret)).await.expect("startup");
    assert_eq!(keys.is_some(), enabled);
    let routes = crate::xrpc::router(app.clone()).merge(handlers::router(handlers::SystemAccountsState { app: app.clone(), keys }));
    let routes = finalize(routes, app.clone(), FinalConfig { not_ready_retry_secs: 5, public: PublicMessages::default() });
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, routes.into_make_service_with_connect_info::<SocketAddr>()).await.expect("serve");
    });
    Some(World { db, base: format!("http://{addr}"), client: reqwest::Client::new(), keys: Keys::new(&SECRET), app, _drainer: Box::new(drainer) })
}

impl World {
    async fn call(&self, method: &str, bearer: Option<&str>, body: Option<Value>) -> (u16, Value) {
        let url = format!("{}/xrpc/{method}", self.base);
        let mut req = match body {
            Some(b) => self.client.post(url).json(&b),
            None if method == GET_SYSTEM_ACCOUNTS || method == GET_SYSTEM_ACCOUNT_STATUS => self.client.get(url),
            None => self.client.post(url),
        };
        if let Some(t) = bearer {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        let r = req.send().await.expect("request");
        let status = r.status().as_u16();
        if status != 501 {
            assert_eq!(r.headers().get("cache-control").and_then(|v| v.to_str().ok()), Some("private, no-store"), "{method}");
        }
        (status, r.json().await.unwrap_or(Value::Null))
    }

    async fn add(&self, owner: &str, account: &str) -> (u16, Value) {
        let body = json!({"account": account, "proof": proof(account)});
        self.call(ADD_SYSTEM_ACCOUNT, Some(&token(owner, ADD_SYSTEM_ACCOUNT)), Some(body)).await
    }

    async fn list(&self, owner: &str) -> Value {
        let (status, v) = self.call(GET_SYSTEM_ACCOUNTS, Some(&token(owner, GET_SYSTEM_ACCOUNTS)), None).await;
        assert_eq!(status, 200, "{v}");
        v
    }

    async fn registered(&self, did: &str) -> bool {
        let (status, v) = self.call(GET_SYSTEM_ACCOUNT_STATUS, Some(&token(did, GET_SYSTEM_ACCOUNT_STATUS)), None).await;
        assert_eq!(status, 200, "{v}");
        v["registered"].as_bool().expect("registered")
    }
}

fn dids(v: &Value) -> Vec<String> {
    v["accounts"].as_array().expect("accounts").iter().map(|a| a["did"].as_str().expect("did").to_string()).collect()
}

/// T-1: sealing round-trips and is bound to its owner and index; the index
/// is deterministic and depends on the key; the subkeys differ.
#[test]
fn keys_seal_index_and_derive_apart() {
    let k = Keys::new(&SECRET);
    let other = Keys::new(&[8; 32]);
    let i = k.index(B);
    assert_eq!(i, k.index(B));
    assert_ne!(i, k.index(C));
    assert_ne!(i, other.index(B));
    let sealed = k.seal(A, &i, B).expect("sealed");
    assert!(!sealed.windows(B.len()).any(|w| w == B.as_bytes()));
    assert_eq!(k.open(A, &i, &sealed).as_deref(), Some(B));
    assert_eq!(k.open(C, &i, &sealed), None, "another owner");
    assert_eq!(k.open(A, &k.index(C), &sealed), None, "another index");
    assert_eq!(other.open(A, &i, &sealed), None, "another key");
    let mut altered = sealed.clone();
    *altered.last_mut().expect("byte") ^= 1;
    assert_eq!(k.open(A, &i, &altered), None);
    // Two seals of one DID differ (a nonce each).
    assert_ne!(k.seal(A, &i, B), Some(sealed));
    // No subkey is another's.
    assert_ne!(k.token_hash(B.as_bytes()), k.index(B));
    assert_ne!(k.check(), k.index(""));
    assert_eq!(format!("{k:?}"), "Keys(<redacted>)");
}

/// T-3 (unit) and R1-1: the lifetime rule, and a token's stored identity:
/// by issuer and `jti`, else by its signing input, so a re-encoded
/// signature segment is the same token.
#[tokio::test]
async fn token_rules_unit() {
    let fetcher = StubFetcher::default();
    fetcher.set_key(A, key_of(A));
    let (v, _d) = verifier(fetcher, 100, 16);
    for (lifetime, ok) in [(60, true), (65, true), (66, false), (3_600, false)] {
        let t = token_with(A, GET_SYSTEM_ACCOUNTS, lifetime, None, &aud());
        let caller = v.verify(Some(&t), GET_SYSTEM_ACCOUNTS).await.expect("verifies");
        assert_eq!(lifetime_ok(&caller.jwt), ok, "{lifetime}");
    }
    let k = Keys::new(&SECRET);
    let exp = std::time::SystemTime::now();
    let one = token_with(A, ADD_SYSTEM_ACCOUNT, 60, Some("j1"), &aud());
    let again = token_with(A, ADD_SYSTEM_ACCOUNT, 60, Some("j1"), &aud());
    assert_eq!(used_token(&k, &one, A, exp).hash, used_token(&k, &format!("Bearer {again}"), A, exp).hash, "same issuer and jti");
    assert_ne!(used_token(&k, &one, A, exp).hash, used_token(&k, &one, B, exp).hash, "the issuer scopes the jti");
    let bare = token_with(A, ADD_SYSTEM_ACCOUNT, 60, Some(""), &aud());
    let mut parts: Vec<String> = bare.split('.').map(str::to_string).collect();
    // The signature segment re-encoded with padding: other bytes, same
    // signing input.
    let sig = URL_SAFE_NO_PAD.decode(&parts[2]).expect("sig");
    parts[2] = base64::engine::general_purpose::URL_SAFE.encode(sig);
    let reencoded = parts.join(".");
    assert_ne!(reencoded, bare);
    assert_eq!(used_token(&k, &bare, A, exp).hash, used_token(&k, &reencoded, A, exp).hash);
    assert_eq!(
        used_token(&k, &bare, A, exp).expires_at,
        chrono::DateTime::<chrono::Utc>::from(exp + TOKEN_MEMORY_SKEW)
    );
    assert_eq!(TOKEN_MEMORY_SKEW, kryphocron::verification::JwtVerificationConfig::atproto_service_auth().max_clock_skew);
}

/// T-2: the startup key check.
#[tokio::test]
async fn startup_key_check() {
    let Some(db) = fresh_db("sysacct_key_check").await else {
        return;
    };
    let p = &db.pool;
    let s1 = crate::config::Secret32::from_hex(&"01".repeat(32)).expect("key");
    let s2 = crate::config::Secret32::from_hex(&"02".repeat(32)).expect("key");
    assert!(at_startup(p, None).await.expect("off").is_none());
    assert_eq!(test_rows::count(p, "key").await, 0);
    let k1 = at_startup(p, Some(&s1)).await.expect("first").expect("on");
    assert_eq!(test_rows::count(p, "key").await, 1);
    assert!(at_startup(p, Some(&s1)).await.expect("same").is_some());
    // Another key with nothing sealed replaces the check.
    assert!(at_startup(p, Some(&s2)).await.expect("replaced").is_some());
    assert!(at_startup(p, Some(&s1)).await.expect("back").is_some());
    // An entry sealed under s1: s2 and no key both refuse.
    let index = k1.index(B);
    sqlx::query("INSERT INTO sysacct_lists (owner_did) VALUES ($1)").bind(A).execute(p).await.expect("list");
    sqlx::query("INSERT INTO sysacct_entries (owner_did, member_index, member_sealed, added_at) VALUES ($1, $2, $3, now())")
        .bind(A)
        .bind(index.as_slice())
        .bind(k1.seal(A, &index, B).expect("sealed"))
        .execute(p)
        .await
        .expect("entry");
    let mismatch = at_startup(p, Some(&s2)).await.expect_err("mismatch");
    assert!(mismatch.contains("APP_SYSTEM_ACCOUNTS_KEY_HEX is not the key"), "{mismatch}");
    let unset = at_startup(p, None).await.expect_err("unset");
    assert!(unset.contains("APP_SYSTEM_ACCOUNTS_KEY_HEX is not set"), "{unset}");
    assert!(at_startup(p, Some(&s1)).await.expect("right key").is_some());
    db.drop_db().await;
}

/// The log lines written while a test runs.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Logs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("logs").extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// T-7, T-8, T-11: the five methods end to end; `rev` only grows, across an
/// emptied list too; idempotent add; leave; and afterwards no `sysacct_`
/// row holds B's DID, and no log line at any level names A or B.
#[tokio::test]
async fn methods_end_to_end_store_nothing_in_clear_and_log_no_did() {
    let logs = Logs::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let Some(w) = world("sysacct_end_to_end", true, &[A, B, C, D]).await else {
        return;
    };
    let empty = w.list(A).await;
    assert_eq!(empty, json!({"accounts": [], "rev": 0}));
    assert!(!w.registered(B).await);

    let (status, v) = w.add(A, B).await;
    assert_eq!((status, &v), (200, &json!({"rev": 1})), "{v}");
    let (status, v) = w.add(A, C).await;
    assert_eq!((status, &v), (200, &json!({"rev": 2})), "{v}");
    let first = w.list(A).await;
    assert_eq!(dids(&first), [B, C]);
    assert_eq!(first["rev"], 2);
    let added_at = first["accounts"][0]["addedAt"].clone();
    assert!(added_at.as_str().is_some_and(|s| s.ends_with('Z')), "{added_at}");
    // Idempotent: the same rev, addedAt kept.
    let (status, v) = w.add(A, B).await;
    assert_eq!((status, &v), (200, &json!({"rev": 2})), "{v}");
    assert_eq!(w.list(A).await["accounts"][0]["addedAt"], added_at);
    // D lists B too; B's status is yes; A's list is A's only.
    assert_eq!(w.add(D, B).await.0, 200);
    assert!(w.registered(B).await);
    assert!(!w.registered(A).await);
    assert_eq!(dids(&w.list(D).await), [B]);

    // Remove: hard delete, rev grows; removing again is success, same rev.
    let remove = |owner: &str, account: &str| {
        let t = token(owner, REMOVE_SYSTEM_ACCOUNT);
        let body = json!({"account": account});
        let w = &w;
        async move { w.call(REMOVE_SYSTEM_ACCOUNT, Some(&t), Some(body)).await }
    };
    assert_eq!(remove(A, C).await, (200, json!({"rev": 3})));
    assert_eq!(remove(A, C).await, (200, json!({"rev": 3})));
    // A list never written: rev 0, and no list row made.
    assert_eq!(remove(C, B).await, (200, json!({"rev": 0})));
    assert_eq!(test_rows::rev(&w.db.pool, C).await, None);

    // Leave: B leaves A's and D's lists; both revs grow; status no.
    let (status, v) = w.call(LEAVE_SYSTEM_ACCOUNTS, Some(&token(B, LEAVE_SYSTEM_ACCOUNTS)), None).await;
    assert_eq!((status, &v), (200, &json!({})), "{v}");
    assert!(!w.registered(B).await);
    let a = w.list(A).await;
    assert_eq!((dids(&a).len(), &a["rev"]), (0, &json!(4)), "{a}");
    assert_eq!(w.list(D).await["rev"], 2);
    // An emptied list keeps its rev; the next write is higher still.
    assert_eq!(w.add(A, C).await, (200, json!({"rev": 5})));
    // Leaving with no list holding the caller is the same empty answer.
    let (status, v) = w.call(LEAVE_SYSTEM_ACCOUNTS, Some(&token(B, LEAVE_SYSTEM_ACCOUNTS)), Some(json!({}))).await;
    assert_eq!((status, &v), (200, &json!({})));

    // T-8: no row holds a listed DID in clear.
    for row in test_rows::all_bytes(&w.db.pool).await {
        for listed in [B, C] {
            assert!(!row.windows(listed.len()).any(|win| win == listed.as_bytes()), "{listed} in a sysacct_ row");
        }
    }
    let text = String::from_utf8(logs.0.lock().expect("logs").clone()).expect("utf-8");
    assert!(!text.is_empty(), "the subscriber captured the run");
    for did in [A, B, C, D] {
        assert!(!text.contains(did), "{did} logged:\n{text}");
    }
    w.db.drop_db().await;
}

/// T-3, T-4, T-5, T-6: the token rules and the proof checks over HTTP.
#[tokio::test]
async fn token_rules_and_proof_checks() {
    let Some(w) = world("sysacct_token_rules", true, &[A, B, C]).await else {
        return;
    };
    let add = |bearer: String, account: &str, proof: String| {
        let w = &w;
        let body = json!({"account": account, "proof": proof});
        async move { w.call(ADD_SYSTEM_ACCOUNT, Some(&bearer), Some(body)).await }
    };
    let err = |v: &Value| v["error"].as_str().unwrap_or_default().to_string();

    // Lifetime: 65 s accepted, 66 s refused, for bearer and proof.
    let (s, v) = add(token_with(A, ADD_SYSTEM_ACCOUNT, 66, None, &aud()), B, proof(B)).await;
    assert_eq!((s, err(&v)), (401, "InvalidToken".into()), "{v}");
    let (s, v) = add(token(A, ADD_SYSTEM_ACCOUNT), B, token_with(B, CONFIRM_SYSTEM_ACCOUNT, 66, None, &aud())).await;
    assert_eq!((s, err(&v)), (400, "InvalidProof".into()), "{v}");
    let (s, v) = add(token_with(A, ADD_SYSTEM_ACCOUNT, 65, None, &aud()), B, token_with(B, CONFIRM_SYSTEM_ACCOUNT, 65, None, &aud())).await;
    assert_eq!(s, 200, "{v}");
    // Reads keep the usual rule: an hour.
    let (s, v) = w.call(GET_SYSTEM_ACCOUNTS, Some(&token_with(A, GET_SYSTEM_ACCOUNTS, 3_600, None, &aud())), None).await;
    assert_eq!(s, 200, "{v}");

    // Once, counted at commit.
    let bearer = token(A, ADD_SYSTEM_ACCOUNT);
    let p = proof(C);
    assert_eq!(add(bearer.clone(), C, p.clone()).await.0, 200);
    let (s, v) = add(bearer.clone(), C, proof(C)).await;
    assert_eq!((s, err(&v)), (401, "InvalidToken".into()), "a used bearer: {v}");
    let (s, v) = add(token(A, ADD_SYSTEM_ACCOUNT), C, p.clone()).await;
    assert_eq!((s, err(&v)), (400, "InvalidProof".into()), "a used proof: {v}");
    // A refused request leaves its tokens usable: the resend after a 401
    // with a fresh bearer and the same body succeeds.
    let p2 = proof(B);
    let (s, _) = add(token_with(A, ADD_SYSTEM_ACCOUNT, 66, None, &aud()), B, p2.clone()).await;
    assert_eq!(s, 401);
    let (s, v) = add(token(A, ADD_SYSTEM_ACCOUNT), B, p2).await;
    assert_eq!(s, 200, "the proof of a refused request is still usable: {v}");
    // A jti-less token is used once too.
    let bare = token_with(A, REMOVE_SYSTEM_ACCOUNT, 60, Some(""), &aud());
    let body = json!({"account": D});
    assert_eq!(w.call(REMOVE_SYSTEM_ACCOUNT, Some(&bare), Some(body.clone())).await.0, 200);
    assert_eq!(w.call(REMOVE_SYSTEM_ACCOUNT, Some(&bare), Some(body)).await.0, 401);
    // Leave's bearer too.
    let leave = token(B, LEAVE_SYSTEM_ACCOUNTS);
    assert_eq!(w.call(LEAVE_SYSTEM_ACCOUNTS, Some(&leave), None).await.0, 200);
    assert_eq!(w.call(LEAVE_SYSTEM_ACCOUNTS, Some(&leave), None).await.0, 401);

    // Bare aud and #bsky_appview both accepted, for bearer and proof;
    // another fragment refused.
    let frag = format!("{}#bsky_appview", aud());
    let (s, v) = add(token_with(A, ADD_SYSTEM_ACCOUNT, 60, None, &frag), B, token_with(B, CONFIRM_SYSTEM_ACCOUNT, 60, None, &frag)).await;
    assert_eq!(s, 200, "{v}");
    let other = format!("{}#atproto_labeler", aud());
    let (s, v) = add(token(A, ADD_SYSTEM_ACCOUNT), B, token_with(B, CONFIRM_SYSTEM_ACCOUNT, 60, None, &other)).await;
    assert_eq!((s, err(&v)), (400, "InvalidProof".into()), "{v}");

    // Proof checks: wrong lxm, another issuer, a forgery, the requester
    // itself, an iss with a fragment, a missing proof.
    let (s, v) = add(token(A, ADD_SYSTEM_ACCOUNT), B, token(B, ADD_SYSTEM_ACCOUNT)).await;
    assert_eq!((s, err(&v)), (400, "InvalidProof".into()), "wrong lxm: {v}");
    let (s, v) = add(token(A, ADD_SYSTEM_ACCOUNT), B, proof(C)).await;
    assert_eq!((s, err(&v)), (400, "InvalidProof".into()), "another issuer: {v}");
    let now = chrono::Utc::now().timestamp();
    let forged = mint_claims(&signing_key(200), &json!({"iss": B, "aud": aud(), "lxm": CONFIRM_SYSTEM_ACCOUNT, "iat": now, "exp": now + 60, "jti": "f"}));
    let (s, v) = add(token(A, ADD_SYSTEM_ACCOUNT), B, forged).await;
    assert_eq!((s, err(&v)), (400, "InvalidProof".into()), "forged: {v}");
    let (s, v) = add(token(A, ADD_SYSTEM_ACCOUNT), A, proof(A)).await;
    assert_eq!((s, err(&v)), (400, "InvalidRequest".into()), "self: {v}");
    let fragment_iss = mint_claims(&key_of(B), &json!({"iss": format!("{B}#atproto"), "aud": aud(), "lxm": CONFIRM_SYSTEM_ACCOUNT, "iat": now, "exp": now + 60, "jti": "g"}));
    let (s, v) = add(token(A, ADD_SYSTEM_ACCOUNT), B, fragment_iss).await;
    assert_eq!((s, err(&v)), (400, "InvalidProof".into()), "the verifier refuses an iss with a fragment: {v}");
    let (s, v) = w.call(ADD_SYSTEM_ACCOUNT, Some(&token(A, ADD_SYSTEM_ACCOUNT)), Some(json!({"account": B}))).await;
    assert_eq!((s, err(&v)), (400, "InvalidRequest".into()), "{v}");
    let (s, v) = w.call(ADD_SYSTEM_ACCOUNT, Some(&token(A, ADD_SYSTEM_ACCOUNT)), Some(json!({"account": "nope", "proof": "x"}))).await;
    assert_eq!((s, err(&v)), (400, "InvalidRequest".into()), "{v}");
    // A bearer minted for another method, and no bearer.
    let (s, _) = add(token(A, REMOVE_SYSTEM_ACCOUNT), B, proof(B)).await;
    assert_eq!(s, 401);
    let (s, v) = w.call(GET_SYSTEM_ACCOUNTS, None, None).await;
    assert_eq!((s, err(&v)), (401, "AuthRequired".into()));
    // Bodies: too large, not an object.
    let big = json!({"account": B, "proof": "x".repeat(MAX_BODY)});
    let (s, v) = w.call(ADD_SYSTEM_ACCOUNT, Some(&token(A, ADD_SYSTEM_ACCOUNT)), Some(big)).await;
    assert_eq!((s, err(&v)), (400, "InvalidRequest".into()), "{v}");
    let (s, v) = w.call(LEAVE_SYSTEM_ACCOUNTS, Some(&token(B, LEAVE_SYSTEM_ACCOUNTS)), Some(json!([1]))).await;
    assert_eq!((s, err(&v)), (400, "InvalidRequest".into()), "{v}");
    // The proof-only name has no route.
    let (s, v) = w.call(CONFIRM_SYSTEM_ACCOUNT, Some(&proof(B)), Some(json!({}))).await;
    assert_eq!((s, err(&v)), (501, "MethodNotImplemented".into()), "{v}");
    w.db.drop_db().await;
}

/// T-7 (cap): 31 entries, the 32nd is ListFull and leaves its tokens
/// usable; used tokens are swept once expired.
#[tokio::test]
async fn the_cap_is_31_and_used_tokens_are_swept() {
    let others: Vec<String> = (0..32).map(|i| format!("did:plc:{i:0>24}")).collect();
    let mut all: Vec<&str> = others.iter().map(String::as_str).collect();
    all.push(A);
    let Some(w) = world("sysacct_cap", true, &all).await else {
        return;
    };
    for (n, b) in others.iter().take(31).enumerate() {
        let (s, v) = w.add(A, b).await;
        assert_eq!((s, v["rev"].as_i64()), (200, Some(i64::try_from(n).expect("n") + 1)), "{v}");
    }
    let bearer = token(A, ADD_SYSTEM_ACCOUNT);
    let p = proof(&others[31]);
    let body = json!({"account": others[31], "proof": p});
    let (s, v) = w.call(ADD_SYSTEM_ACCOUNT, Some(&bearer), Some(body.clone())).await;
    assert_eq!((s, v["error"].as_str()), (400, Some("ListFull")), "{v}");
    // The refused request used nothing: after a remove, the same request
    // (same bearer, same proof) succeeds.
    let remove = json!({"account": others[0]});
    assert_eq!(w.call(REMOVE_SYSTEM_ACCOUNT, Some(&token(A, REMOVE_SYSTEM_ACCOUNT)), Some(remove)).await.0, 200);
    let (s, v) = w.call(ADD_SYSTEM_ACCOUNT, Some(&bearer), Some(body)).await;
    assert_eq!(s, 200, "{v}");
    assert_eq!(dids(&w.list(A).await).len(), 31);
    // Expired used tokens go at the next write.
    let before = test_rows::count(&w.db.pool, "tokens").await;
    assert!(before >= 64, "{before}");
    test_rows::expire_tokens(&w.db.pool).await;
    assert_eq!(w.call(LEAVE_SYSTEM_ACCOUNTS, Some(&token(&others[1], LEAVE_SYSTEM_ACCOUNTS)), None).await.0, 200);
    let after = test_rows::count(&w.db.pool, "tokens").await;
    assert!(before <= 100, "one sweep takes them all: {before}");
    assert_eq!(after, 1, "only the leave's own bearer is left: {before} -> {after}");
    w.db.drop_db().await;
}

/// T-9: off, every method answers 501 before authentication and the
/// feature is unlisted; on, it is listed for a non-member.
#[tokio::test]
async fn off_is_501_and_unlisted_on_is_listed_for_everyone() {
    let Some(w) = world("sysacct_off", false, &[A, B]).await else {
        return;
    };
    for m in METHODS {
        let body = (m != GET_SYSTEM_ACCOUNTS && m != GET_SYSTEM_ACCOUNT_STATUS).then(|| json!({"account": B, "proof": proof(B)}));
        for bearer in [None, Some("garbage".to_string()), Some(token(A, m))] {
            let (s, v) = w.call(m, bearer.as_deref(), body.clone()).await;
            assert_eq!((s, v["error"].as_str()), (501, Some("MethodNotImplemented")), "{m} {v}");
        }
    }
    assert!(!w.app.membership.features().contains(&FEATURE));
    assert!(crate::xrpc::handlers::membership::MembershipSettings::default().with_system_accounts(true).features().contains(&FEATURE));
    w.db.drop_db().await;
    // On: getMembership lists it for a caller who is not a member.
    let Some(on) = world("sysacct_on_listed", true, &[A]).await else {
        return;
    };
    let get = crate::xrpc::handlers::membership::GET_MEMBERSHIP;
    let r = on
        .client
        .get(format!("{}/xrpc/{get}", on.base))
        .header("authorization", format!("Bearer {}", token(A, get)))
        .send()
        .await
        .expect("request");
    let v: Value = r.json().await.expect("json");
    assert_eq!(v["status"], "notEligible", "{v}");
    assert!(v["features"].as_array().expect("features").iter().any(|f| f == FEATURE), "{v}");
    on.db.drop_db().await;
}

/// T-12: the three writes cost 4 of the per-DID bucket's tokens, reads 1.
#[test]
fn writes_cost_four() {
    use crate::xrpc::rate_limit::{Family, cost};
    for m in [ADD_SYSTEM_ACCOUNT, REMOVE_SYSTEM_ACCOUNT, LEAVE_SYSTEM_ACCOUNTS] {
        assert_eq!(cost(m), 4, "{m}");
    }
    assert_eq!((cost(GET_SYSTEM_ACCOUNTS), cost(GET_SYSTEM_ACCOUNT_STATUS)), (1, 1));
    let limits = RateLimits::new();
    let ip = "203.0.113.7".parse().expect("ip");
    for _ in 0..5 {
        limits.charge(ADD_SYSTEM_ACCOUNT, Family::SystemAccounts, Some(A), ip).expect("burst 20 holds five writes");
    }
    assert!(limits.charge(ADD_SYSTEM_ACCOUNT, Family::SystemAccounts, Some(A), ip).is_err());
    limits.charge(GET_SYSTEM_ACCOUNTS, Family::SystemAccounts, Some(B), ip).expect("another DID");
}

/// T-13: the five methods are served (per DID, never anonymous) and kept in
/// every public-indexing mode.
#[test]
fn methods_are_served_and_kept() {
    use crate::xrpc::rate_limit::{Family, SERVED};
    for m in METHODS {
        let s = SERVED.iter().find(|s| s.nsid == m).expect("served");
        assert_eq!((s.family, s.anonymous), (Family::SystemAccounts, false), "{m}");
        assert!(crate::honest_501::KEPT_METHODS.contains(&m), "{m}");
    }
    assert!(!SERVED.iter().any(|s| s.nsid == CONFIRM_SYSTEM_ACCOUNT));
}

/// T-10 (SP-3): a DID known only as a sealed listed account is queued on an
/// `#account` event, without counting as queued, and the `account_deleted`
/// purge removes it from every list; an owner is purged with its list.
#[tokio::test]
async fn deletion_reaches_a_did_known_only_as_a_listed_account() {
    use crate::aggregations::{PurgeKind, Purger, ScopeState};
    use crate::config::ScopePolicy;
    use crate::firehose::router::route_identity;
    use crate::firehose::stream::IdentityKind;
    use crate::private_tier::DisabledPrivateTier;

    let Some(w) = world("sysacct_deletion", true, &[A, B, C, D]).await else {
        return;
    };
    let p = &w.db.pool;
    assert_eq!(w.add(A, B).await.0, 200);
    assert_eq!(w.add(D, B).await.0, 200);
    assert_eq!(w.add(C, D).await.0, 200);
    let refresh = |did: &'static str| async move {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM identity_refresh WHERE did = $1").bind(did).fetch_one(p).await.expect("count")
    };
    // Without the key, a listed account is not found; with it, it is
    // queued, and `queued` (what the live feed shows) stays false.
    assert!(!route_identity(p, IdentityKind::Account, B, None).await.expect("route"));
    assert_eq!(refresh(B).await, 0);
    assert!(!route_identity(p, IdentityKind::Account, B, Some(&w.keys)).await.expect("route"));
    assert_eq!(refresh(B).await, 1);
    // An owner is found with or without the key.
    assert!(!route_identity(p, IdentityKind::Tombstone, C, None).await.expect("route"));
    assert_eq!(refresh(C).await, 1);
    // A stranger is not.
    assert!(!route_identity(p, IdentityKind::Account, "did:plc:stranger", Some(&w.keys)).await.expect("route"));
    assert_eq!(refresh("did:plc:stranger").await, 0);

    // The refresher's `deleted` hook: the account_deleted purge.
    let scope = ScopeState::new(ScopeSets::default(), ScopePolicy::MembersAndOneHop, 10);
    let purger = Purger::new(p.clone(), scope, Arc::new(DisabledPrivateTier)).with_system_accounts(Some(w.keys.clone()));
    let (a_rev, d_rev) = (test_rows::rev(p, A).await, test_rows::rev(p, D).await);
    purger.purge(B, PurgeKind::AccountDeleted).await.expect("purge");
    assert!(!w.registered(B).await);
    assert_eq!(dids(&w.list(A).await).len(), 0);
    assert_eq!(dids(&w.list(D).await).len(), 0);
    assert!(test_rows::rev(p, A).await > a_rev && test_rows::rev(p, D).await > d_rev, "every list B left has a new rev");
    // An owner's list goes with it; the account it listed is no longer on
    // any list.
    assert!(w.registered(D).await);
    purger.purge(C, PurgeKind::AccountDeleted).await.expect("purge");
    assert_eq!(test_rows::rev(p, C).await, None);
    assert!(!w.registered(D).await);
    assert_eq!(test_rows::count(p, "entries").await, 0);
    // Removing a member does nothing to lists (design §1 D-5).
    assert_eq!(w.add(A, D).await.0, 200);
    purger.purge(A, PurgeKind::MemberRemoved).await.expect("member removed");
    assert_eq!(dids(&w.list(A).await), [D]);
    w.db.drop_db().await;
}
