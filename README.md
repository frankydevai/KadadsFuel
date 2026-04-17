# DieselUp — AI Fuel Optimization for Trucking Fleets

Real-time Telegram bot that monitors truck fuel levels via Samsara GPS,
pulls active loads from QuickManage TMS, and sends drivers the cheapest
fuel stop on their exact route — ranked by true net cost after IFTA
quarterly settlement.

## How It Works

1. Samsara GPS detects truck fuel below 35%
2. Bot pulls active load from QuickManage (shipper → all stops → receiver)
3. Searches all Pilot/Flying J, Love's, TA/Petro stops along the route corridor
4. Ranks every stop by: `card_price + (home_state_rate - stop_state_rate)`
5. Sends single cheapest stop to driver's Telegram group instantly
6. Re-alerts every 20 min if driver doesn't act

## Files

| File | Purpose |
|---|---|
| `main.py` | Main polling loop + background threads |
| `state_machine.py` | Core alert logic per truck |
| `telegram_bot.py` | All Telegram commands + alerts |
| `truck_stop_finder.py` | IFTA-aware stop search algorithm |
| `samsara_client.py` | GPS + MPG + idle from Samsara API |
| `quickmanage_client.py` | QuickManage TMS integration |
| `ifta.py` | IFTA rates all 48 states + auto-scraper |
| `route_planner.py` | Full A→B route fuel planning |
| `efs_importer.py` | EFS fuel card CSV importer |
| `database.py` | PostgreSQL schema + all queries |
| `config.py` | All environment variables |
| `california.py` | CA border reminder logic |
| `price_updater.py` | Pilot CSV + Love's XLSX parser |
| `route_reader.py` | QM Notifier message parser |
| `yard_geofence.py` | Yard detection |

## Railway Environment Variables

```
SAMSARA_API_TOKEN        Your Samsara API token (Read Vehicles + Fuel & Energy)
TELEGRAM_BOT_TOKEN       Your Telegram bot token from @BotFather
DISPATCHER_GROUP_ID      Telegram group ID for dispatcher
ADMIN_CHAT_ID            Your personal Telegram chat ID (owner reports here)
QM_CLIENT_ID             QuickManage OAuth2 client ID
QM_CLIENT_SECRET         QuickManage OAuth2 client secret
IFTA_HOME_STATE          Fleet base state e.g. FL, IN, TX, OH
DATABASE_URL             PostgreSQL connection string (auto-set by Railway)
DEFAULT_TANK_GAL         Tank size in gallons (default: 150)
DEFAULT_MPG              Fallback MPG if Samsara data missing (default: 6.5)
CA_BORDER_FUEL_THRESHOLD Fuel % threshold for CA border reminder (default: 70)
YARD_1                   Yard geofence: Name:lat:lng:radius_miles
```

## Setup

1. Deploy to Railway — connect GitHub repo
2. Add PostgreSQL plugin in Railway
3. Set all environment variables above
4. Upload fuel prices:
   - Send EFS CSV file to bot in admin Telegram chat
   - Bot auto-detects format and imports all stops
5. Add bot to each driver's Telegram group
   - Bot auto-assigns truck by matching number in group name
   - Group name format: `1769 (32%) Driver Name` or `Truck 0792 Name`
6. Run `/checkall` in admin chat to verify trucks are showing

## Telegram Commands

```
/findstop <truck>      Find cheapest stops within range
/route <truck>         Show active QM load
/planroute <truck>     Full A→B IFTA fuel plan
/newalert <truck>      Force fresh alert now
/stopvisits <truck>    Fuel stop visit history
/compliance [truck]    Stop compliance report
/truckstats [truck]    MPG + idle from Samsara
/checkall              Instant fleet fuel status
/routelist             All trucks with active loads
/findload <trip#>      Search QM by trip number
/dbstats               Fuel stop DB breakdown
/addtruck /setgroup /listtruck /removetruck
```

## Alert Format

```
🟡 Low Fuel Alert — Truck 1769
⛽ Fuel: 32%   🧭 63 mph ENE
📍 Truck Location
🌐 35.15234, -90.14352

⛽ Pilot/J West Memphis #607
📌 3400 Service Loop Rd, West Memphis, AR
🛣 18.4 mi ahead
💰 Retail:  $5.40/gal
💳 Card:    $4.32/gal  (save $1.08/gal)
📋 IFTA:    +$0.115/gal owed → true cost $4.435
💵 Fill 102 gal → Pump: $441 · After IFTA: $452
🗺 Open in Google Maps
```

## IFTA Logic

- Home state set via `IFTA_HOME_STATE` env var — different per customer
- Net cost = `card_price + (home_state_rate - stop_state_rate)`
- Q1 2026 rates hardcoded for all 48 states
- Auto-scraped quarterly from official Colorado IFTA source
- Surcharge states handled: KY, VA, NY, NM, IN

## Weekly Owner Report

Sent every Monday 08:00 UTC to `ADMIN_CHAT_ID` only — never to drivers.
Shows: total savings, IFTA settlement estimate, fleet MPG,
idle hours, stop compliance rate, fuel by state breakdown.
