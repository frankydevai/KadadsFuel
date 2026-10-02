# Setup

## Configuration

Create a Python 3.12 environment and install dependencies:

```sh
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.txt
cp .env.example .env
```

Fill the blank required values in `.env` with your private development credentials. Use a separate database and Telegram bot for development. Keep `.env` untracked. The shared configuration validates QuickManage, Samsara, Telegram, and database settings even when the dashboard uses bootstrap mode.

Set `PILOT_ACCOUNT_NUMBER` to the actual account identifier. Set `FTS_PRICE_CUSTOMER` and `LOVES_PRICE_CUSTOMER` to the exact expected supplier-customer identities in your files. Do not copy account or driver details into public examples. `VALHALLA_URL` and `VALHALLA_API_SECRET` are required for `BOT_MODE=active`; live planning has no alternate routing-provider fallback.

For an exposed dashboard, set a unique random `DASHBOARD_SECRET` and configure `DASHBOARD_ADMIN_EMAIL` and `DASHBOARD_ADMIN_PASSWORD`. Do not rely on source defaults. Run it behind HTTPS; its login session cookie requires a secure connection. Keep dashboard authentication values in service variables, never in frontend files or browser links shared with others.

For a dashboard running only on this Mac over HTTP, bind it to `127.0.0.1` and explicitly set `DASHBOARD_COOKIE_SECURE=false` in the private local configuration. This allows the login session at `http://127.0.0.1:8080`. Keep the default `DASHBOARD_COOKIE_SECURE=true` for HTTPS deployments.

## Database

For a new, disposable development database only, review `schema.sql` and initialize it explicitly:

```sh
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f schema.sql
```

Export `DATABASE_URL` into that shell before running this command; the command does not read `.env` itself. Both services must use the same Kadads database. The application can seed the bundled price-free station catalog if the station table is empty.

For an existing database, back up first and review the required additive SQL separately. Preserve price receipts, quotes, advice history, outcomes, and connection evidence. Leave `RUN_SCHEMA_ON_STARTUP=false` for mature databases. Operators apply reviewed SQL changes explicitly; `BOT_MODE` is not a schema migration command. The supported service modes are `active` for the bot and `bootstrap` for the dashboard.

## Start the services

Run the bot with the environment's `BOT_MODE=active`:

```sh
python -m dieselup.main
```

It starts polling and scheduled checks under a database singleton lock. A restart retains the configured truck scope and messaging mode. Silent mode still permits incoming commands, supplier-file downloads, and read-only Telegram group checks, while blocking output.

Run the dashboard in a separate process with bootstrap mode:

```sh
BOT_MODE=bootstrap uvicorn dieselup.dashboard.server:create_app --factory --host 0.0.0.0 --port 8080
```

The dashboard process does not start bot polling or its scheduler. In a deployed dashboard service, set `BOT_MODE=bootstrap` as a service variable. Bot health is available at `/health`; the dashboard also exposes `/health`. A successful HTTP health response alone does not verify every upstream connection or a valid truck trip.

## Railway

Use two services built from the repository root and `Dockerfile`:

| Service | Configuration file | `BOT_MODE` | Start command |
| --- | --- | --- | --- |
| Bot | `railway.json` | `active` | `python -m dieselup.main` |
| Dashboard | `railway.dashboard.json` | `bootstrap` | `uvicorn dieselup.dashboard.server:create_app --factory --host 0.0.0.0 --port ${PORT:-8080}` |

Set the dashboard's Railway config file path to `railway.dashboard.json`. Both services need their required private environment variables. Keep `TEST_TRUCK_UNITS=6682,8089,8217`, `AUTO_LINK_ENABLED=false`, and `RUN_SCHEMA_ON_STARTUP=false` on both. A GitHub connection does not change service variables or import private historical data.

For an authorized three-driver activation: deploy and verify the recipient guard while `TELEGRAM_MESSAGING_MODE=silent`; verify every selected driver's actual ready group mapping; then change messaging to `live` and verify the same guarded release and restricted scope. Preserve queue history rather than replaying old driver warnings. If deployment is pending, keep the existing silent deployment until the guard is confirmed live.

## Verify a change

Run `python -m pytest -q` with test-only configuration. Before production activation, check exact deployment versions, scheduler freshness, upstream GPS and fuel freshness, assigned loads, forward truck routes, current supplier prices, authenticated dashboard data, and real recipient readiness. Synthetic simulations demonstrate behavior but do not establish actual visits, purchases, or savings.
