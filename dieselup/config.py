"""
Typed configuration loaded from environment variables and an optional .env file.

Other modules import `settings` from here. On startup, any missing or invalid
variable causes sys.exit(1) with a precise error — we never run with bad config.
"""
from __future__ import annotations

import sys
import re
from typing import Literal

from pydantic import AliasChoices, Field, ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    DASHBOARD_SECRET: str = ''
    DASHBOARD_ADMIN_EMAIL: str = ''
    DASHBOARD_ADMIN_PASSWORD: str = Field(default='', repr=False)

    CORRIDOR_MILES: float = 5.0

    REJECT_OVER: float = 0.15

    # Carrier identity changes labels, never optimization logic.
    COMPANY_NAME: str = "DieselUp Carrier"
    BOT_MODE: Literal["active", "bootstrap"] = "active"
    TELEGRAM_MESSAGING_MODE: Literal["live", "silent"] = "silent"
    TEST_TRUCK_UNITS: str = "6682,8089,8217"  # empty means the full fleet
    AUTO_LINK_ENABLED: bool = False
    MAX_ADVICE_GPS_AGE_MINUTES: float = 5.0
    MAX_ADVICE_FUEL_AGE_MINUTES: float = 30.0

    # One carrier deployment talks to exactly one TMS.  DataTruck remains
    # supported for the original carrier; new carrier projects can select the
    # documented read-only QuickManage adapter with Railway variables only.
    TMS_PROVIDER: Literal["datatruck", "quickmanage"] = "datatruck"
    DATATRUCK_COMPANY_SLUG: str = ""
    DATATRUCK_API_TOKEN: str = ""
    QUICKMANAGE_BASE_URL: str = Field(
        default="https://api.quickmanage.com",
        validation_alias=AliasChoices("QUICKMANAGE_BASE_URL", "QM_BASE_URL"),
    )
    QUICKMANAGE_CLIENT_ID: str = Field(
        default="",
        validation_alias=AliasChoices("QUICKMANAGE_CLIENT_ID", "QM_CLIENT_ID"),
    )
    QUICKMANAGE_CLIENT_SECRET: str = Field(
        default="",
        validation_alias=AliasChoices("QUICKMANAGE_CLIENT_SECRET", "QM_CLIENT_SECRET"),
    )
    QUICKMANAGE_CREDENTIALS: str = ""  # optional client_id:client_secret shortcut
    SAMSARA_API_TOKEN: str
    PILOT_ACCOUNT_NUMBER: str = ""
    LOVES_PRICE_CUSTOMER: str = ""
    FTS_PRICE_CUSTOMER: str = ""
    TELEGRAM_BOT_TOKEN: str
    TELEGRAM_ADMIN_CHAT_ID: int
    TELEGRAM_DISPATCH_CHAT_ID: int | None = None
    DATABASE_URL: str
    IFTA_HOME_STATE: str = "NJ"

    FLEET_DEFAULT_MPG: float = 6.5
    TANK_CAPACITY_GALLONS: int = 200        # full tank = always fill to this
    SAFETY_FLOOR_GALLONS: int = 30          # min gallons on arrival at stop
    MAX_ARRIVAL_FUEL_GALLONS: int = 80      # reject stop if truck arrives too full to fill
    DELIVERY_RESERVE_PCT: int = 30          # min % of tank required at delivery

    # Trial-period override of the CLAUDE.md IFTA hard rule. "your_price" picks
    # the stop with the lowest pump-discounted price; "ifta_adjusted" picks by
    # IFTA-adjusted true cost. Both rankings are always stored in
    # stop_events.candidates so the weekly report can compute the delta.
    RANK_STRATEGY: Literal["your_price", "ifta_adjusted"] = "your_price"

    # Legacy routing settings retained only for configuration compatibility.
    # Kadads live fuel planning never reads them; private Valhalla is exclusive.
    ORS_API_KEY: str = Field(
        default="",
        validation_alias=AliasChoices("ORS_API_KEY", "HERE_API_KEY"),
    )

    # Dedicated authenticated Valhalla VPS — the only Kadads routing provider.
    VALHALLA_URL: str = ""
    VALHALLA_API_SECRET: str = ""
    VALHALLA_TIMEOUT_SECONDS: float = 15.0
    VALHALLA_MAX_MATRIX_LOCATIONS: int = 48
    VALHALLA_CONFIG: str = ""

    # Legacy compatibility only; ignored by the Kadads live planning path.
    TOMTOM_API_KEY: str = ""

    # TUNABLE knobs for core/fuel_plan.plan_fuel — how aggressively the planner
    # trades detours / extra stops against per-gallon savings. Config, never
    # literals inside the DP. Wrong values yield plans that look optimal but
    # lose money in reality, so tune against real per-truck economics:
    #   COST_PER_MILE     ≈ fuel $/mi (price ÷ mpg) + a wear allowance.
    #   STOP_TIME_PENALTY ≈ defensible $/hr driver time × avg stop hrs (~0.3-0.5).
    COST_PER_MILE: float = 0.55         # deadhead fuel + wear per off-route mile
    # ≈ 30-40 min real stop time (entry/exit, queue, pump, log) at a defensible
    # ~$30-40/hr driver + HOS opportunity cost. The old $12 undervalued a stop
    # and made the DP over-eager to add extra stops for small per-gallon wins.
    STOP_TIME_PENALTY: float = 20.0     # $ cost of making one extra fuel stop

    # Existing general purchase floor plus the stricter bridge/transit floor.
    # The lane planner uses the larger value, so a carrier-level legacy value
    # of 50 cannot reintroduce 20-30 gallon nuisance bridge stops. Emergency
    # reserve relaxation may still use a smaller life-safety purchase when no
    # strict shipper-to-delivery plan is physically feasible.
    MIN_FUEL_PURCHASE_GALLONS: float = 50.0
    BRIDGE_MIN_FUEL_PURCHASE_GALLONS: float = 90.0

    # QuickManage can take several minutes to page through the full active +
    # recently-delivered order window. Keep this below the 15-minute scheduler
    # interval, but long enough that every page can be examined before alerting.
    LOAD_SYNC_ORDER_ENUMERATION_TIMEOUT_SECONDS: float = 600.0

    # Hard lane-quality guard. A contracted stop must be meaningfully on the
    # route; cheap fuel 70-150 miles off the interstate is not a recommendation,
    # it is a route error. The planner still charges detour cost inside this
    # limit, but candidates beyond it are dropped before optimization.
    MAX_STOP_DETOUR_MILES: float = 10.0
    MAX_FUEL_PRICE_AGE_DAYS: int = 3  # fail closed when the contracted feed is stale

    # Driver rest detection — driver Telegram messages (briefings, approach reminders)
    # are suppressed when the truck is stationary and has been stopped long enough
    # that the driver is likely resting. Dispatch always gets messages regardless.
    # Wrong-stop and missed-stop alerts are never suppressed (truck is actively moving).
    #
    # Detection: speed < DRIVER_REST_SPEED_MPH AND GPS age >= DRIVER_REST_MINUTES
    #   Samsara reduces GPS update frequency when ignition is off, so GPS age growing
    #   above 60 min with speed = 0 reliably indicates the truck has been parked >= 1 hr.
    DRIVER_REST_SPEED_MPH: float = 2.0   # below this mph → truck considered stopped
    DRIVER_REST_MINUTES: int = 60        # GPS must be this stale (parked) before suppressing

    @field_validator("IFTA_HOME_STATE")
    @classmethod
    def _validate_home_state(cls, v: str) -> str:
        if len(v) != 2 or not v.isalpha() or v != v.upper():
            raise ValueError("must be a 2-letter uppercase US state code (e.g. 'NJ')")
        return v

    @field_validator("TEST_TRUCK_UNITS")
    @classmethod
    def _validate_test_trucks(cls, value: str) -> str:
        if not value.strip():
            return ""
        units = [part.strip().upper() for part in value.split(",")]
        if any(not re.fullmatch(r"\d+[A-Z]?", unit) for unit in units):
            raise ValueError("TEST_TRUCK_UNITS must contain comma-separated truck numbers")
        return ",".join(sorted({unit.lstrip("0") or "0" for unit in units}))

    @field_validator(
        "SAMSARA_API_TOKEN",
        "PILOT_ACCOUNT_NUMBER",
        "TELEGRAM_BOT_TOKEN",
        "DATABASE_URL",
    )
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must not be empty")
        return v.strip()

    @model_validator(mode="after")
    def _validate_tms_credentials(self) -> "Settings":
        if self.TMS_PROVIDER == "datatruck":
            if not self.DATATRUCK_COMPANY_SLUG.strip():
                raise ValueError("DATATRUCK_COMPANY_SLUG is required for TMS_PROVIDER=datatruck")
            if not self.DATATRUCK_API_TOKEN.strip():
                raise ValueError("DATATRUCK_API_TOKEN is required for TMS_PROVIDER=datatruck")
            return self

        client_id = self.QUICKMANAGE_CLIENT_ID.strip()
        client_secret = self.QUICKMANAGE_CLIENT_SECRET.strip()
        combined = self.QUICKMANAGE_CREDENTIALS.strip()
        if combined:
            if ":" not in combined:
                raise ValueError(
                    "QUICKMANAGE_CREDENTIALS must be client_id:client_secret"
                )
            combined_id, combined_secret = combined.split(":", 1)
            client_id = client_id or combined_id.strip()
            client_secret = client_secret or combined_secret.strip()
        if not client_id or not client_secret:
            raise ValueError(
                "QuickManage requires QUICKMANAGE_CLIENT_ID and "
                "QUICKMANAGE_CLIENT_SECRET (or QUICKMANAGE_CREDENTIALS)"
            )
        self.QUICKMANAGE_CLIENT_ID = client_id
        self.QUICKMANAGE_CLIENT_SECRET = client_secret
        self.QUICKMANAGE_BASE_URL = self.QUICKMANAGE_BASE_URL.strip().rstrip("/")
        return self

    @model_validator(mode="after")
    def _validate_private_valhalla(self) -> "Settings":
        """Active Kadads routing must never start without its private server."""
        self.VALHALLA_URL = self.VALHALLA_URL.strip().rstrip("/")
        self.VALHALLA_API_SECRET = self.VALHALLA_API_SECRET.strip()
        if self.BOT_MODE != "active":
            return self
        if not self.VALHALLA_URL:
            raise ValueError("VALHALLA_URL is required when BOT_MODE=active")
        if not self.VALHALLA_URL.startswith(("http://", "https://")):
            raise ValueError("VALHALLA_URL must start with http:// or https://")
        if not self.VALHALLA_API_SECRET:
            raise ValueError("VALHALLA_API_SECRET is required when BOT_MODE=active")
        return self

    @field_validator(
        "FLEET_DEFAULT_MPG",
        "TANK_CAPACITY_GALLONS",
        "SAFETY_FLOOR_GALLONS",
        "MAX_ARRIVAL_FUEL_GALLONS",
        "DELIVERY_RESERVE_PCT",
        "MIN_FUEL_PURCHASE_GALLONS",
        "BRIDGE_MIN_FUEL_PURCHASE_GALLONS",
        "LOAD_SYNC_ORDER_ENUMERATION_TIMEOUT_SECONDS",
        "MAX_FUEL_PRICE_AGE_DAYS",
        "MAX_ADVICE_GPS_AGE_MINUTES",
        "MAX_ADVICE_FUEL_AGE_MINUTES",
    )
    @classmethod
    def _positive(cls, v):
        if v <= 0:
            raise ValueError("must be a positive number")
        return v

    @field_validator("COST_PER_MILE", "STOP_TIME_PENALTY", "MAX_STOP_DETOUR_MILES")
    @classmethod
    def _non_negative(cls, v):
        if v < 0:
            raise ValueError("must be >= 0")
        return v

    @field_validator("VALHALLA_MAX_MATRIX_LOCATIONS")
    @classmethod
    def _matrix_needs_origin_and_destination(cls, v: int) -> int:
        if v < 2:
            raise ValueError("must be at least 2 for origin and destination")
        return v


try:
    settings = Settings()
except ValidationError as exc:
    sys.stderr.write("FATAL: invalid configuration — refusing to start.\n")
    for err in exc.errors():
        field = ".".join(str(p) for p in err["loc"]) or "<root>"
        sys.stderr.write(f"  - {field}: {err['msg']}\n")
    sys.stderr.write(
        "\nSet the missing variables in the environment or in a .env file at the project root.\n"
        "See .env.example for the full list.\n"
    )
    sys.exit(1)
