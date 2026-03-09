"""LiteGraph SDK configuration and thread-safe per-graph context."""

import os
import threading
from contextlib import contextmanager

from litegraph_sdk import configure as _sdk_configure

LITEGRAPH_ENDPOINT = os.getenv("LITEGRAPH_ENDPOINT", "http://localhost:8701")
LITEGRAPH_TENANT_GUID = os.getenv(
    "LITEGRAPH_TENANT_GUID", "00000000-0000-0000-0000-000000000000"
)
LITEGRAPH_ACCESS_KEY = os.getenv("LITEGRAPH_ACCESS_KEY", "litegraphadmin")

# The Python SDK uses a global singleton client, so we serialize graph-scoped
# operations with a lock to avoid races in the FastAPI thread-pool.
_lock = threading.Lock()


def configure_global() -> None:
    """Call once at startup (no graph scope)."""
    _sdk_configure(
        endpoint=LITEGRAPH_ENDPOINT,
        tenant_guid=LITEGRAPH_TENANT_GUID,
        access_key=LITEGRAPH_ACCESS_KEY,
    )


@contextmanager
def graph_ctx(graph_guid: str):
    """Thread-safe context that pins the SDK to a specific graph."""
    with _lock:
        _sdk_configure(
            endpoint=LITEGRAPH_ENDPOINT,
            tenant_guid=LITEGRAPH_TENANT_GUID,
            access_key=LITEGRAPH_ACCESS_KEY,
            graph_guid=graph_guid,
        )
        yield
