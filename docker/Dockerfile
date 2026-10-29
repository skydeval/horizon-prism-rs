# horizon-prism-rs image (design v1 §11.4).
# Build context: repository root (compose sets context: ..).

# ---- builder ----
FROM rust:1.90-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# kryphocron-lexicons' build script invokes proto-blue-codegen as a subprocess.
RUN cargo install proto-blue-codegen --locked --version 0.3.3
# The official image installs the minimal rustup profile, which omits clippy.
RUN rustup component add clippy

WORKDIR /build
# Whole crate directory: Cargo.toml, Cargo.lock, src/, migrations/ (embedded by
# sqlx::migrate!) and clippy.toml (the disallowed codec surface, enforced by the
# clippy step below). target/ is excluded by .dockerignore.
COPY horizon-prism-rs/ ./
# The custom lexicons, compiled in by build.rs from ../lexicons (the admin
# Instance page serves them).
COPY lexicons/ /lexicons/

# Private-table source check (design §4.2): private tables may only be named
# under src/private_tier/. Any other source file naming one fails the build.
# The match is on substrings of any identifier, so no config key, function,
# metric, index or test outside src/private_tier/ may contain a table name, and
# no migration file name may contain one either (cli/commands/migrate_status.rs
# lists every name as a literal; private posting §3.1).
# grep exits 0 (matches), 1 (no matches) or 2 (error); anything but 0/1 fails.
RUN set -e; \
    test -d src; \
    rc=0; grep -rlE 'private_(posts|audiences|tombstones|owner_ready|owner_eligible|bookmarks|threadgates|postgates|quote_refs|notifications|notification_seen|notification_prefs|activity_subscriptions|post_fanout|root_gate_fetches|appearance)' --include='*.rs' src > /tmp/private_refs || rc=$?; \
    if [ "$rc" -gt 1 ]; then echo "private-table source check: grep failed ($rc)"; exit 1; fi; \
    leaks="$(grep -v '^src/private_tier/' /tmp/private_refs || true)"; \
    if [ -n "$leaks" ]; then echo "private-tier table referenced outside src/private_tier/:"; echo "$leaks"; exit 1; fi

# System-accounts source check (system-accounts design §4.4): the sysacct_
# tables may only be named under src/system_accounts/, so no other method,
# view or admin page reads them.
RUN set -e; \
    rc=0; grep -rl 'sysacct_' --include='*.rs' src > /tmp/sysacct_refs || rc=$?; \
    if [ "$rc" -gt 1 ]; then echo "system-accounts source check: grep failed ($rc)"; exit 1; fi; \
    leaks="$(grep -v '^src/system_accounts/' /tmp/sysacct_refs || true)"; \
    if [ -n "$leaks" ]; then echo "system-accounts table referenced outside src/system_accounts/:"; echo "$leaks"; exit 1; fi

# A deploy image never carries the stand-in exception: no cargo config or
# flags that could turn debug assertions on in a release build.
ENV RUSTFLAGS="" CARGO_ENCODED_RUSTFLAGS="" CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=false
RUN test ! -e .cargo/config.toml && test ! -e .cargo/config

# Enforces the private-tier codec guard (design §6.3); a violation fails the build.
RUN cargo clippy --release --locked --all-targets -- -D warnings
RUN cargo build --release --locked --bin horizon-prism-rs
RUN mkdir -p /out && cp target/release/horizon-prism-rs /out/horizon-prism-rs
# The lock: the binary that ships is grepped for the feature's marker, and a
# missing file (grep exit 2) fails the build too.
RUN rc=0; grep -a -q 'HPRS_STANDIN_HOSTS_BUILD' /out/horizon-prism-rs || rc=$?; \
    if [ "$rc" -eq 0 ]; then echo 'refusing: this binary carries the standin-hosts feature'; exit 1; fi; \
    if [ "$rc" -ne 1 ]; then echo "standin check: grep failed ($rc)"; exit 1; fi

# Writable data dir for kryphocron at-rest state, owned by distroless nonroot (65532).
RUN mkdir -p /out/data

# ---- runtime ----
FROM gcr.io/distroless/cc-debian12:nonroot

WORKDIR /app
COPY --from=builder /out/horizon-prism-rs /app/horizon-prism-rs
COPY --from=builder --chown=65532:65532 /out/data /app/data

USER nonroot:nonroot
EXPOSE 3030 9091

ENTRYPOINT ["/app/horizon-prism-rs"]
CMD ["serve"]
