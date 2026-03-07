# FleetFuel Bot — Setup Guide

## Step 1 — Create Your Telegram Bot

1. Open Telegram → search **@BotFather** → send `/newbot`
2. Follow the steps, pick a name and username
3. BotFather gives you a token like `7123456789:AAFxxx...` — save this as `TELEGRAM_BOT_TOKEN`

## Step 2 — Get Your Telegram IDs

**Your personal user ID (ADMIN_CHAT_ID):**
1. Message **@userinfobot** on Telegram
2. It replies with your ID, e.g. `123456789`

**Your dispatcher group ID (DISPATCHER_GROUP_ID):**
1. Create a Telegram group (or use existing one)
2. Add your bot to the group
3. Message **@userinfobot** inside the group
4. It replies with the group ID, e.g. `-1009876543210` (starts with -100)

## Step 3 — Railway PostgreSQL

1. Go to [railway.app](https://railway.app) → your project
2. Click **+ New** → **Database** → **Add PostgreSQL**
3. Railway automatically sets `DATABASE_URL` — you don't need to copy it manually if deploying on Railway

If running locally, click the PostgreSQL service → **Connect** tab → copy the **DATABASE_URL** and paste it in your `.env` file.

## Step 4 — Create .env File

Create a file called `.env` in your project folder:

```
SAMSARA_API_TOKEN       = your_samsara_token
TELEGRAM_BOT_TOKEN      = your_bot_token
DISPATCHER_GROUP_ID     = -1009876543210
ADMIN_CHAT_ID           = 123456789
DATABASE_URL            = postgresql://postgres:password@host:port/railway

YARD_1 = Main Yard:28.0000:-81.0000:0.5
YARD_2 = Second Yard:29.0000:-82.0000:0.5
```

**How to get yard coordinates:**
Open Google Maps → right-click your yard location → the lat/lng numbers appear at the top of the menu. The last number `0.5` is the radius in miles.

**Optional tuning (defaults are fine to start):**
```
FUEL_ALERT_THRESHOLD_PCT = 35
DEFAULT_TANK_GAL         = 150
DEFAULT_MPG              = 6.5
CA_BORDER_FUEL_THRESHOLD = 70
CA_BORDER_REMINDER_MILES = 150
```

## Step 5 — Install & Run Locally

```bash
pip install -r requirements.txt
python main.py
```

**Expected startup logs:**
```
✅ Database schema ready.
FleetFuel Bot online. Monitoring fuel levels.
```

## Step 6 — Register Your Trucks

After first run, connect to your PostgreSQL database and insert your trucks.
Use **TablePlus**, **DBeaver**, or **pgAdmin** with your DATABASE_URL connection string.

```sql
INSERT INTO trucks (vehicle_name, telegram_group_id) VALUES
  ('Unit 4821', '-1009876543210'),
  ('Unit 4822', '-1009876543210'),
  ('Unit 4823', '-1009876543210');
```

> `vehicle_name` must exactly match what Samsara shows for that truck.
> In beta mode all alerts go to the same DISPATCHER_GROUP_ID.

**Optional — set known tank size and MPG per truck:**
```sql
UPDATE trucks SET tank_capacity_gal = 200, tank_size_known = TRUE WHERE vehicle_name = 'Unit 4821';
UPDATE trucks SET avg_mpg = 7.2, mpg_known = TRUE WHERE vehicle_name = 'Unit 4821';
```

## Step 7 — Upload Fuel Prices

Upload price files manually from your personal chat with the bot.

**First time only — Pilot locations (never changes):**
1. Send `all_locations.csv` to your private chat with the bot
2. Bot replies: `✅ Pilot locations cached — 848 stores saved.`

**Daily — Pilot prices:**
1. Go to pilotflyingj.com → Fuel Prices → Download
2. Send `Fuel_Prices.csv` to the bot
3. Bot replies: `✅ Pilot: loaded 845 stops with prices.`

**Daily — Love's prices:**
1. Go to loves.com → Fuel Prices → Download
2. Send `LovesSearchResults.xlsx` to the bot
3. Bot replies: `✅ Love's: loaded 606 stops.`

## Step 8 — Deploy to Railway

Create a `railway.toml` in your project root:
```toml
[deploy]
startCommand = "python main.py"
```

Push to GitHub → connect repo in Railway → deploy.
Add all `.env` variables in Railway → your service → **Variables** tab.

---

## What the Alert Looks Like

```
🟡 LOW FUEL ALERT — Unit 4821
⛽ Fuel: 32%  ·  📍 58 mph W

🏆 Recommended Fuel Stop
Pilot Travel Center
Address: 100 Travel Plaza Dr, Shamrock, TX 79079
Diesel #2: $3.389/gal
31.2 mi away

Book ahead to save time.

📍 [map pin]
```

---

## Troubleshooting

**No alerts sending?**
Check that `vehicle_name` in the trucks table exactly matches the name in Samsara.

**"No diesel stops found"?**
Upload your price files first — bot has no stops until you send the CSV/XLSX files.

**Prices not updating after upload?**
Make sure you sent the file to your private chat with the bot, not a group.
Make sure `ADMIN_CHAT_ID` matches your personal Telegram user ID exactly.

**Bot not responding to uploads?**
Confirm bot token is correct and `python main.py` is running with no errors.
