"""TMS provider selector for one carrier deployment."""
from __future__ import annotations

from typing import Any, AsyncIterator, Protocol

from dieselup.clients.datatruck import DataTruckClient, DataTruckError
from dieselup.clients.quickmanage import QuickManageClient, QuickManageError
from dieselup.config import settings


class TMSClient(Protocol):
    async def __aenter__(self) -> "TMSClient": ...
    async def __aexit__(self, *exc: Any) -> None: ...
    def iter_orders(
        self,
        filters: Any = None,
        *,
        max_pages: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]: ...
    async def get_order(self, order_id: int | str) -> dict[str, Any]: ...


TMS_ERRORS = (DataTruckError, QuickManageError)


def make_tms_client() -> TMSClient:
    if settings.TMS_PROVIDER == "quickmanage":
        return QuickManageClient()
    return DataTruckClient()
