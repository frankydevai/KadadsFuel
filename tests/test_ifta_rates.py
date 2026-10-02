import pytest

from dieselup.clients.ifta_rates import get_rate
from dieselup.core.ifta import true_cost_per_gallon


def test_ontario_ifta_rate_is_supported():
    assert get_rate("ON") == pytest.approx(0.2490)
    assert true_cost_per_gallon(3.50, "ON") == pytest.approx(3.7460)
