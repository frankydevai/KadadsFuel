"""
IFTA-adjusted true cost per gallon.

The optimizer ranks fuel stops by this value, not by pump price. A low pump
price in a low-tax state can lose money once IFTA reconciles miles driven
against the home state's tax rate at quarter end.

    true_cost_per_gallon = your_price + (home_state_rate - stop_state_rate)

Home state is read from settings.IFTA_HOME_STATE (NJ for this deployment).
Pure function; no I/O, no DB.
"""
from __future__ import annotations

from dieselup.clients.ifta_rates import get_rate
from dieselup.config import settings


def true_cost_per_gallon(your_price: float, stop_state: str) -> float:
    """Return the IFTA-adjusted true cost per gallon for fueling in stop_state.

    Raises ValueError if either the home state or stop_state has no IFTA rate
    on file — propagated from dieselup.clients.ifta_rates.get_rate.
    """
    home_rate = get_rate(settings.IFTA_HOME_STATE)
    stop_rate = get_rate(stop_state)
    return your_price + (home_rate - stop_rate)
