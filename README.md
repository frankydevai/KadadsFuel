# Kadads Fuel

Kadads combines a Telegram fuel-advice bot with an authenticated operations dashboard. Both services use the `dieselup` Python package and the same PostgreSQL database. QuickManage supplies assigned loads, Samsara supplies truck location and fuel observations, and a private authenticated Valhalla server supplies truck routes.

## What it does

- Matches the assigned truck to its actual load and remaining trip before planning from its current location.
- Evaluates forward stops using truck routing, fuel reserves, current contracted prices, detour cost, and stop cost. Missing or stale evidence can suppress advice.
- Records advice, delivery attempts, observed stops, missed stops, and unplanned fueling for dashboard review. A stop without a supported fuel increase is not automatically treated as a fuel purchase.
- Shows driver connections, truck details, fuel prices, advice history, and evidence-based outcomes. Cost differences are estimates unless supported by reconciled purchase evidence; unknown prices do not become invented savings.
- Imports supplier spreadsheets with source, pricing-date, matching, and excluded-row history.

## Repository layout

| Path | Purpose |
| --- | --- |
| `dieselup/main.py` | Bot startup, Telegram polling, scheduler, and singleton leadership |
| `dieselup/core/` | Trip validation, planning, compliance, outcomes, and processing scope |
| `dieselup/ingestion/` | QuickManage, Samsara, and supplier-price ingestion |
| `dieselup/bot/` | Commands, delivery, and recipient restrictions |
| `dieselup/dashboard/` | FastAPI dashboard, API, and frontend assets |
| `schema.sql` | Database schema for reviewed initialization and migrations |
| `tests/` | Isolated regression tests using synthetic or mocked evidence |
| `data/pilot_locations.csv` | Price-free station catalog |
| `railway.json` | Bot service configuration |
| `railway.dashboard.json` | Dashboard service configuration |

## Run and test

Use Python 3.12. Follow [SETUP.md](SETUP.md) to configure credentials, initialize a separate development database, and start each service.

```sh
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

Run tests with private production variables unset and without a populated production `.env` file. The test fixtures supply isolated fake credentials; their delivery simulations do not authorize sending production Telegram messages.

## Test-period controls

The repository includes the selected-driver messaging guard. Keep these settings for the current three-truck trial:

```dotenv
TEST_TRUCK_UNITS=6682,8089,8217
AUTO_LINK_ENABLED=false
TELEGRAM_MESSAGING_MODE=silent
RUN_SCHEMA_ON_STARTUP=false
```

Activation requires deploying and verifying the recipient guard first, then checking every selected driver's actual group connection. In restricted live mode, only a unique, ready, unpaused driver connection for an allowed truck can receive output. Admin, dispatch, duplicate, conflicting, unmapped, and unverifiable recipients are blocked. Empty `TEST_TRUCK_UNITS` expands processing to the fleet; preserve the explicit list during this trial.

The configuration template stays silent. The owner has authorized live messaging for the three selected driver groups after the guarded release is verified. Publishing this repository does not deploy it or change a running service's messaging settings. Run one active bot, either on the Mac or Railway, to avoid duplicate polling and alerts.

## QuickManage navigation rules

QuickManage assigns the physical truck and supplies the trip status. Samsara supplies the current GPS, motion and fuel readings. The owner's status rules determine the remaining route:

| QuickManage status | Navigation |
| --- | --- |
| `dispatched` / `dispatching` | Current truck location → ordered shipper pickups → delivery |
| `in_transit` | Current truck location → remaining delivery |
| `reserved` / `upcoming` | Record the next load separately; preserve the current trip |

These rules do not mark a customer stop completed or establish a fuel visit. Conflicting assignments, contradictory explicit stop progress, ambiguous remaining deliveries, stale sensors and unresolved locations hold advice. Plans record the phase, coordinate source and route context; a changed phase replaces old advice without a missed-stop penalty. An unsent plan held while resting is freshly checked again when the truck moves. Accepted driver messages remain protected from duplicate delivery.

ZIP centers cannot guide trucks. Full street addresses can match a unique saved Samsara facility. Optional Census street interpolation is disabled by default and requires owner authorization before commercial addresses are transmitted. It is separately limited to the three trial trucks and tagged as an approximate fuel-route point; it cannot prove a visit, fueling or driver savings. Valhalla supplies the truck route after locations are resolved.

## Fuel-price policy

**Synergy Carriers sheets supply Love's prices. FTS Plus sheets supply Pilot/Flying J prices.** A supplier/account check and exact catalog matching keep networks separate. Configuring FTS for Pilot/Flying J prevents fallback to a different native Pilot account.

Supplier dates and explicit date captions are retained. An undated upload automatically uses its original upload day in `America/New_York`, recorded as `upload_day`; a date caption is not required. Price age, invalid values, unsupported brands, and station matching are still checked. Valid Love's rows can import while invalid rows retain exclusion reasons. Historical receipts and quotes are preserved.

Love's route recommendations remain held pending reliable station-identity integration. Uploaded prices must be checked against accepted import records before being described as current. This remains a controlled trial, with no claim of industry readiness or verified driver savings.

## Deployment and data

The bot and dashboard are separate Railway services sharing this repository and database. Existing services currently use direct uploads; connecting GitHub is an optional later deployment configuration change. Choose `railway.json` for the bot and `railway.dashboard.json` for the dashboard, using the repository root for each build.

Keep credentials, driver rosters, group identifiers, database exports, and private verification reports outside Git. Mature databases must keep startup schema execution disabled. Review and apply additive changes explicitly, with backups and verification; never reset production to make a deployment work.
