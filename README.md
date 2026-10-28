# Horizon-Prism-rs

Horizon-Prism-rs is an atproto AppView for a community. It indexes the community's members and the accounts they interact with, and serves the stock `app.bsky.*` API to them, plus the `app.nearhorizon.*` extensions used by the Horizon client. It works alongside the public Bluesky network: what it does not hold, it fetches from other AppViews.

It is a stack you run yourself, not a hosted service. This README calls it Horizon-Prism from here on.

## Features

- **Stock AppView API.** Timelines, threads, profiles, notifications, search, lists, bookmarks and the rest of `app.bsky.*` for members.
- **Private posts.** Posts shown only to an audience the author picks. See [private posts](docs/design/private-posts.md).
- **Thread Lens.** A thread view that can show replies the ordinary view leaves out. See [Thread Lens](docs/design/thread-lens.md).
- **Restored accounts (Refract).** Optional. Keeps accounts that Bluesky's AppView took down readable while a listed community still hosts them. See [Refract](docs/design/refract.md).
- **Pronouns and plural systems.** Served for any account without sign-in. See [pronouns and plural](docs/design/pronouns-and-plural.md).
- **Community scope.** Indexes members and the accounts they touch, not the whole network. Public indexing can be turned off.
- **Operator tools.** A web admin UI and an `admin` CLI.

## Requirements

- A Linux host with Docker and Docker Compose.
- A public DNS name for the AppView, with ports 80 and 443 reachable (Caddy obtains the certificate).
- Outbound network access. The relay sidecar (horizon-firehose) is built from source at a pinned commit.
- Disk for Postgres (the default storage budget is 200 GiB) and about 6 GB of memory for Redis.
- At least one community PDS whose accounts will be members, crawled by the relays.

Building outside Docker needs Rust 1.90 or newer. The crate is in `horizon-prism-rs/`.

## Quick start

```sh
cd docker
cp .env.example .env              # set hostname, ACME email, Postgres password
docker compose build
docker compose run --rm --no-deps app admin keygen   # copy the keys into .env
openssl rand -hex 32              # APP_PRIVATE_CURSOR_KEY_HEX in .env
# add your community PDS hosts to config/app.toml, then:
# set APP_ALLOW_STARTUP_MIGRATE=1 in .env for the first start
docker compose up -d
```

Then close the migration gate, become the operator at `https://<your-host>/admin/setup`, and add members. [docs/running.md](docs/running.md) has every step.

## Documentation

Guides:

- [Running](docs/running.md): deploy, first start, migrations, upgrades, backups, health checks.
- [Configuration](docs/configuration.md): `app.toml`, environment, live settings, optional services.
- [Admin](docs/admin.md): the web admin UI and the `admin` CLI.
- [API](docs/api.md): methods, feature discovery, auth, errors, rate limits.
- [Operations](docs/operations.md): metrics, logs, degraded mode, common issues.

Design: [docs/design/](docs/design/README.md) has one short document per area.

Lexicons for the `app.nearhorizon.*` methods are in [`lexicons/`](lexicons/). The detailed operator runbook is [`docker/README.md`](docker/README.md).

## Background

Horizon-Prism-rs is not a fork of aurora-prism. It was built based on it: almost a successor, but not quite.

## Status

In active development. Expect schema migrations and config changes between versions; read [CHANGELOG.md](CHANGELOG.md) before upgrading.

## Working on it

After cloning, run `git config core.hooksPath .githooks` once. The hooks run `tools/name-guard.py`, which refuses commits and pushes holding a word from its list; the `name_guard` test checks the same over the whole tree.
