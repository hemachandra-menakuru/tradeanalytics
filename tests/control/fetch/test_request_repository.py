"""Unit tests — FetchRequestRepository parallel manifest writes."""

import json
import threading

import pytest
from unittest.mock import MagicMock

from src.control.fetch.request_builder import FetchRequestBuilder
from src.control.fetch.request_repository import FetchRequestRepository


def _requests(n):
    b = FetchRequestBuilder(raw_bucket="test-bucket")
    from datetime import date
    reqs = []
    for i in range(n):
        reqs += b.build(
            batch_id="batch_x", stream="daily", load_type="INCREMENTAL",
            bar_interval="1d", start_date=date(2026, 7, 1), end_date=date(2026, 7, 2),
            batch_size_days=30, instrument_id=i + 1, symbol=f"SYM{i}",
            vendor="ibkr", vendor_instrument_id=str(1000 + i),
        )
    return reqs


def _repo(fs_put, workers=16):
    return FetchRequestRepository(
        spark=MagicMock(), catalog="tradeanalytics",
        raw_bucket="test-bucket", fs_put=fs_put, manifest_workers=workers,
    )


def test_all_manifests_written_concurrently():
    written = {}
    lock = threading.Lock()

    def fs_put(path, contents, overwrite):
        with lock:
            written[path] = json.loads(contents)

    reqs = _requests(50)
    _repo(fs_put)._write_manifests_parallel(reqs)

    assert len(written) == 50
    # every request produced a manifest at its pending path, contract v2
    for r in reqs:
        path = f"s3://test-bucket/control/fetch/ibkr/pending/{r.request_key}.json"
        assert path in written
        assert written[path]["contract_version"] == "2"


def test_single_request_uses_direct_write():
    written = {}
    def fs_put(path, contents, overwrite):
        written[path] = contents
    reqs = _requests(1)
    _repo(fs_put)._write_manifests_parallel(reqs)
    assert len(written) == 1


def test_manifest_write_failure_propagates():
    def fs_put(path, contents, overwrite):
        raise IOError("s3 unavailable")
    reqs = _requests(5)
    with pytest.raises(RuntimeError, match="manifest writes failed"):
        _repo(reqs and fs_put)._write_manifests_parallel(reqs)
