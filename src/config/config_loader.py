"""
TradeAnalytics Config Loader
============================
Loads and merges all config files into a single dot-accessible config object.
Adding new config keys requires only editing the YAML — no code changes needed.

Load order (later overrides earlier):
  1. config/{environment}.yml       — infrastructure (S3, Databricks, cluster)
  2. config/sources.yml             — data provider settings (shared)
  3. config/streams/daily.yml       — daily stream config + validation thresholds
  4. config/streams/intraday.yml    — intraday stream config (Phase 3)
  5. config/streams/tick.yml        — tick stream config (Phase 5)
  6. config/risk.yml                — risk parameters
  7. config/logging.yml             — logging settings
  8. .env                           — secrets (never committed)
  9. Environment variables          — runtime overrides (highest priority)

Usage:
    from src.config.config_loader import ConfigLoader

    config = ConfigLoader.load()

    # Infrastructure
    bucket  = config.aws.s3.raw
    catalog = config.databricks.catalog

    # Sources
    source  = config.sources.primary          # "ibkr"
    fallback = config.sources.fallback        # "yahoo"

    # Streams
    if config.daily.enabled:
        table = config.daily.table
        threshold = config.daily.validation.thresholds.price_change_pct_threshold

    # Risk
    max_risk = config.risk.max_position_risk_pct

    # Safe access with default
    value = config.get("daily.validation.thresholds.gap_threshold", default=15.0)
"""

import os
import logging
import yaml
from pathlib import Path
from typing import Any, Optional, Set
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

_SENTINEL = object()


# ── Dot-accessible config node ─────────────────────────────────────────────────

class ConfigNode:
    """
    Wraps a dict and provides recursive dot-notation access.
    Any nested dict becomes a ConfigNode automatically.
    """

    def __init__(self, data: dict, _path: str = ""):
        object.__setattr__(self, "_data", data)
        object.__setattr__(self, "_path", _path)

    def __getattr__(self, key: str) -> Any:
        data = object.__getattribute__(self, "_data")
        path = object.__getattribute__(self, "_path")

        if key not in data:
            full_path = f"{path}.{key}" if path else key
            raise AttributeError(
                f"Config key '{full_path}' not found. "
                f"Available keys at this level: {list(data.keys())}"
            )

        value = data[key]
        full_path = f"{path}.{key}" if path else key

        if isinstance(value, dict):
            return ConfigNode(value, _path=full_path)
        return value

    def __repr__(self) -> str:
        data = object.__getattribute__(self, "_data")
        path = object.__getattribute__(self, "_path")
        return f"ConfigNode(path='{path}', keys={list(data.keys())})"

    def get(self, dot_path: str, default: Any = None) -> Any:
        result = self._traverse(dot_path)
        return default if result is _SENTINEL else result

    def has(self, dot_path: str) -> bool:
        return self._traverse(dot_path) is not _SENTINEL

    def _traverse(self, dot_path: str) -> Any:
        try:
            node = self
            for key in dot_path.split("."):
                node = getattr(node, key)
            return node
        except AttributeError:
            return _SENTINEL

    def to_dict(self) -> dict:
        data = object.__getattribute__(self, "_data")
        result = {}
        for key, value in data.items():
            if isinstance(value, dict):
                result[key] = ConfigNode(value).to_dict()
            else:
                result[key] = value
        return result

    def keys(self):
        return object.__getattribute__(self, "_data").keys()


# ── Deep merge utility ─────────────────────────────────────────────────────────

def _deep_merge(base: dict, override: dict) -> dict:
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


# ── Repo root detection ────────────────────────────────────────────────────────

def _find_repo_root() -> Path:
    """Locate repo root by finding databricks.yml."""
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "databricks.yml").exists():
            return parent
    raise FileNotFoundError(
        "Cannot locate repo root — databricks.yml not found."
    )


# ── Config Loader ──────────────────────────────────────────────────────────────

class ConfigLoader:
    """
    Singleton config loader. Reads all YAML config files and merges them
    into a single dot-accessible ConfigNode.

    Config file load order:
      Infrastructure → Sources → Streams → Risk → Logging → Secrets → Env vars

    Streams are auto-discovered from config/streams/*.yml
    Each stream file merges under its own top-level key (daily, intraday, tick)
    """

    _instance: Optional[ConfigNode] = None
    _env: Optional[str] = None

    @classmethod
    def load(
        cls,
        environment: str = None,
        force_reload: bool = False,
        repo_root: Path = None,
    ) -> ConfigNode:
        env = environment or os.getenv("ENVIRONMENT", "dev")

        if cls._instance is not None and not force_reload and cls._env == env:
            return cls._instance

        root = repo_root or _find_repo_root()

        # Load .env for local development
        env_file = root / ".env"
        if env_file.exists():
            load_dotenv(env_file, override=False)
            logger.debug(f"Loaded secrets from {env_file}")

        # ── Config file load order ─────────────────────────────────────────
        config_files = [
            ("infrastructure", root / "config" / f"{env}.yml"),
            ("sources",        root / "config" / "sources.yml"),
            ("risk",           root / "config" / "risk.yml"),
            ("logging",        root / "config" / "logging.yml"),
            # Phase 3: ("indicators",    root / "config" / "indicators.yml"),
            # Phase 4: ("notifications", root / "config" / "notifications.yml"),
        ]

        # Auto-discover stream configs from config/streams/*.yml
        streams_dir = root / "config" / "streams"
        if streams_dir.exists():
            for stream_file in sorted(streams_dir.glob("*.yml")):
                stream_name = stream_file.stem
                config_files.append((f"stream_{stream_name}", stream_file))
                logger.debug(f"Discovered stream config: {stream_file.name}")

        # ──────────────────────────────────────────────────────────────────

        merged = {}
        for label, path in config_files:
            if not path.exists():
                raise FileNotFoundError(
                    f"{label.capitalize()} config not found: {path}"
                )
            with open(path) as f:
                raw = yaml.safe_load(f) or {}
            merged = _deep_merge(merged, raw)
            logger.info(f"Loaded {label} config: {path.name}")

        # Inject secrets from environment variables
        merged = cls._inject_env_secrets(merged)

        config = ConfigNode(merged, _path="config")
        cls._instance = config
        cls._env = env

        # Log which streams are active
        for stream in ["daily", "intraday", "tick"]:
            if stream in merged:
                enabled = merged[stream].get("enabled", False)
                phase   = merged[stream].get("phase", "?")
                status  = "ACTIVE" if enabled else f"disabled (Phase {phase})"
                logger.info(f"Stream '{stream}': {status}")

        return config

    @classmethod
    def _inject_env_secrets(cls, config: dict) -> dict:
        """
        Inject environment variables into config dict.
        To add new secrets: add mapping here + entry in .env.example
        """
        env_mappings = {
            # IBKR credentials
            "IBKR_ACCOUNT_ID":  ("sources", "ibkr", "account_id"),
            "IBKR_USERNAME":    ("sources", "ibkr", "username"),
            "IBKR_PASSWORD":    ("sources", "ibkr", "password"),
            # Polygon (future)
            "POLYGON_API_KEY":  ("sources", "polygon", "api_key"),
        }

        for env_var, path in env_mappings.items():
            value = os.getenv(env_var)
            if value:
                # Navigate to correct nested dict
                target = config
                for key in path[:-1]:
                    if key not in target:
                        target[key] = {}
                    target = target[key]
                target[path[-1]] = value
                logger.debug(f"Injected: {env_var} → config.{'.'.join(path)}")

        return config

    @classmethod
    def reset(cls):
        """Reset cached instance — use in tests only."""
        cls._instance = None
        cls._env = None
