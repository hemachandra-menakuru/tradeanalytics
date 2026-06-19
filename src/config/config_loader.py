"""
TradeAnalytics Config Loader
============================
Loads and merges all config files into a single dot-accessible config object.
Adding new config keys requires only editing the YAML — no code changes needed.

Load order (later overrides earlier):
  1. config/{environment}.yml  — infrastructure config
  2. config/risk.yml           — risk parameters  
  3. config/logging.yml        — logging settings
  4. .env                      — secrets (never committed to Git)
  5. Environment variables     — runtime overrides (highest priority)

Usage:
    from src.config.config_loader import ConfigLoader
    
    config = ConfigLoader.load()
    
    bucket  = config.aws.s3.raw
    risk    = config.risk.max_position_risk_pct
    catalog = config.databricks.catalog
    
    # Safe access with default
    value = config.get("some.key", default="fallback")
    
    # Check if key exists
    if config.has("feature_flags.new_feature"):
        ...
    
    # Convert back to plain dict
    d = config.to_dict()
"""

import os
import logging
import yaml
from pathlib import Path
from typing import Any, Optional
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

_SENTINEL = object()  # unique object to distinguish "not found" from None


# ── Dot-accessible config node ─────────────────────────────────────────────────

class ConfigNode:
    """
    Wraps a dict and provides recursive dot-notation access.
    Any nested dict becomes a ConfigNode automatically.

    Example:
        node = ConfigNode({"aws": {"s3": {"raw": "my-bucket"}}})
        node.aws.s3.raw  # → "my-bucket"
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
        """
        Safe access using dot-separated path string.
        Returns default if key does not exist.

        Example:
            config.get("aws.s3.raw", default="fallback-bucket")
        """
        result = self._traverse(dot_path)
        return default if result is _SENTINEL else result

    def has(self, dot_path: str) -> bool:
        """
        Check if a dot-separated config path exists.
        Uses sentinel to correctly handle keys whose value is None or False.
        """
        return self._traverse(dot_path) is not _SENTINEL

    def _traverse(self, dot_path: str) -> Any:
        """
        Traverse the config tree using a dot-separated path.
        Returns _SENTINEL if any key in the path is missing.
        """
        try:
            node = self
            for key in dot_path.split("."):
                node = getattr(node, key)
            return node
        except AttributeError:
            return _SENTINEL

    def to_dict(self) -> dict:
        """Convert back to plain dict."""
        data = object.__getattribute__(self, "_data")
        result = {}
        for key, value in data.items():
            if isinstance(value, dict):
                result[key] = ConfigNode(value).to_dict()
            else:
                result[key] = value
        return result

    def keys(self):
        """Return top-level keys at this node."""
        return object.__getattribute__(self, "_data").keys()


# ── Deep merge utility ─────────────────────────────────────────────────────────

def _deep_merge(base: dict, override: dict) -> dict:
    """
    Recursively merge override into base.
    Override values take precedence. Lists are replaced entirely.
    """
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
        "Cannot locate repo root — databricks.yml not found. "
        "Ensure you are running from within the tradeanalytics repo."
    )


# ── Config Loader ──────────────────────────────────────────────────────────────

class ConfigLoader:
    """
    Singleton config loader. Reads all YAML config files and merges them
    into a single dot-accessible ConfigNode.

    To add new config files in future phases:
      1. Create the YAML file in config/
      2. Add one line to the config_files list below
      3. Access immediately via config.your.new.key
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
        """
        Load and return the merged config. Cached after first call.

        Args:
            environment:  'dev' | 'prod' (default: ENVIRONMENT env var or 'dev')
            force_reload: Re-read all files even if already loaded
            repo_root:    Override repo root path (useful for testing)

        Returns:
            ConfigNode: Dot-accessible merged config object
        """
        env = environment or os.getenv("ENVIRONMENT", "dev")

        if cls._instance is not None and not force_reload and cls._env == env:
            return cls._instance

        root = repo_root or _find_repo_root()

        # Load .env for local development
        env_file = root / ".env"
        if env_file.exists():
            load_dotenv(env_file, override=False)
            logger.debug(f"Loaded secrets from {env_file}")

        # ── Add new config files here as the project grows ──────────────────
        config_files = [
            ("infrastructure", root / "config" / f"{env}.yml"),
            ("data",           root / "config" / "data.yml"),
            ("risk",           root / "config" / "risk.yml"),
            ("logging",        root / "config" / "logging.yml"),
            # Phase 3: ("indicators",    root / "config" / "indicators.yml"),
            # Phase 3: ("backtest",      root / "config" / "backtest.yml"),
            # Phase 4: ("notifications", root / "config" / "notifications.yml"),
        ]
        # ────────────────────────────────────────────────────────────────────

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

        logger.info(
            f"Config loaded: environment={env}, "
            f"source={config.data.source}, "
            f"catalog={config.databricks.catalog}"
        )

        return config

    @classmethod
    def _inject_env_secrets(cls, config: dict) -> dict:
        """
        Inject environment variables into config dict.

        To add new secrets:
          1. Add to .env.example with a comment
          2. Add the mapping below
          3. Access via config.section.key in code
        """
        env_mappings = {
            "IBKR_ACCOUNT_ID": ("ibkr", "account_id"),
            "IBKR_USERNAME":   ("ibkr", "username"),
            "IBKR_PASSWORD":   ("ibkr", "password"),
        }

        for env_var, (section, key) in env_mappings.items():
            value = os.getenv(env_var)
            if value:
                if section not in config:
                    config[section] = {}
                config[section][key] = value
                logger.debug(f"Injected: {env_var} → config.{section}.{key}")
            else:
                if section in config and key not in config[section]:
                    config[section][key] = ""

        return config

    @classmethod
    def reset(cls):
        """Reset cached instance — use in tests only."""
        cls._instance = None
        cls._env = None
