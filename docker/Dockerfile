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
# sqlx::migrate!), and clippy.toml once committed. target/ is excluded by .dockerignore.
COPY horizon-prism-rs/ ./

# Private-table source check (design §4.2): private tables may only be named
# under src/private_tier/. Any other source file naming one fails the build.
# grep exits 0 (matches), 1 (no matches) or 2 (error); anything but 0/1 fails.
RUN set -e; \
    test -d src; \
    rc=0; grep -rlE 'private_(posts|audiences|blocks|tombstones|owner_ready|owner_eligible)' --include='*.rs' src > /tmp/private_refs || rc=$?; \
    if [ "$rc" -gt 1 ]; then echo "private-table source check: grep failed ($rc)"; exit 1; fi; \
    leaks="$(grep -v '^src/private_tier/' /tmp/private_refs || true)"; \
    if [ -n "$leaks" ]; then echo "private-tier table referenced outside src/private_tier/:"; echo "$leaks"; exit 1; fi

# Enforces the private-tier codec guard (design §6.3); a violation fails the build.
RUN cargo clippy --release --locked --all-targets -- -D warnings
RUN cargo build --release --locked --bin horizon-prism-rs

# Writable data dir for kryphocron at-rest state, owned by distroless nonroot (65532).
RUN mkdir -p /out/data

# ---- runtime ----
FROM gcr.io/distroless/cc-debian12:nonroot

WORKDIR /app
COPY --from=builder /build/target/release/horizon-prism-rs /app/horizon-prism-rs
COPY --from=builder --chown=65532:65532 /out/data /app/data

USER nonroot:nonroot
EXPOSE 3030 9091

ENTRYPOINT ["/app/horizon-prism-rs"]
CMD ["serve"]
