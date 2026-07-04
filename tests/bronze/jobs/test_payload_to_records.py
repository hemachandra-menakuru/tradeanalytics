"""Unit tests — payload_to_records (pure transform, raw payload → OHLCVRecord dicts)."""

import pytest

from src.bronze.jobs.raw_to_bronze_job import payload_to_records


def _payload(**overrides):
    p = {
        "payload_format": "ohlcv_json_v1",
        "request_key": "batch_daily_20260704_124236_ibkr_SPY_003",
        "batch_id": "batch_daily_20260704_124236",
        "stream": "daily",
        "load_type": "INITIAL_LOAD",
        "instrument": {
            "instrument_id": 505, "symbol": "SPY", "vendor": "ibkr",
            "vendor_instrument_id": "756733", "currency": "USD",
            "exchange_mic": "ARCX",
        },
        "fetch": {"bar_interval": "1d", "start_date": "2018-01-01",
                  "end_date": "2018-12-31", "what_to_show": "TRADES", "use_rth": True},
        "fetched_at": "2026-07-04T12:43:12+00:00",
        "fetched_by": "fetch_agent_ibkr_v2",
        "record_count": 2,
        "bars": [
            {"bar_date": "2018-01-02", "open": 267.84, "high": 268.81,
             "low": 267.4, "close": 268.77, "volume": 86655749},
            {"bar_date": "2018-01-03", "open": 268.96, "high": 270.64,
             "low": 268.96, "close": 270.47, "volume": 90070416},
        ],
    }
    p.update(overrides)
    return p


def test_transform_produces_contract_keys():
    recs = payload_to_records(_payload())
    assert len(recs) == 2
    r = recs[0]
    # OHLCVRecord identity contract — bar_date/bar_interval, never date/interval
    assert r["symbol"] == "SPY"
    assert r["bar_date"] == "2018-01-02"
    assert r["bar_interval"] == "1d"
    assert r["source"] == "ibkr"          # data vendor, not transport
    assert "date" not in r and "interval" not in r


def test_prices_and_volume_typed():
    r = payload_to_records(_payload())[1]
    assert isinstance(r["open"], float) and r["close"] == 270.47
    assert isinstance(r["volume"], int) and r["volume"] == 90070416


def test_context_and_audit_fields():
    r = payload_to_records(_payload())[0]
    assert r["currency"] == "USD"
    assert r["exchange"] == "ARCX"
    assert r["data_as_of"] == "2026-07-04T12:43:12+00:00"
    assert r["source_version"] == "fetch_agent_ibkr_v2"
    assert r["dividend_amount"] == 0.0
    assert r["is_amended"] is False


def test_missing_currency_defaults_usd():
    p = _payload()
    p["instrument"]["currency"] = None
    assert payload_to_records(p)[0]["currency"] == "USD"


def test_empty_bars_gives_empty_list():
    assert payload_to_records(_payload(bars=[])) == []


def test_unknown_format_rejected():
    with pytest.raises(ValueError, match="ohlcv_json_v1"):
        payload_to_records(_payload(payload_format="ohlcv_json_v2"))
