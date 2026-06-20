"""
TradeAnalytics Market Data Provider Factory
============================================
Reads config.sources.primary and returns the correct provider instance.

Config path (post stream-config refactor):
  OLD: config.data.source          NEW: config.sources.primary
  OLD: config.data.fallback_source NEW: config.sources.fallback
"""

from __future__ import annotations

import logging
from typing import Dict, Optional, Type

from src.config.config_loader import ConfigNode
from src.ingestion.base.market_data_provider import MarketDataProvider

logger = logging.getLogger(__name__)


class MarketDataFactory:
    """
    Factory that creates and returns provider instances based on config.
    Uses a registry pattern — providers self-register at import time.
    """

    _registry: Dict[str, Type[MarketDataProvider]] = {}

    @classmethod
    def register(cls, name: str, provider_class: Type[MarketDataProvider]) -> None:
        if name in cls._registry:
            logger.warning(f"Provider '{name}' already registered — overwriting.")
        cls._registry[name] = provider_class
        logger.debug(f"Registered provider: '{name}' → {provider_class.__name__}")

    @classmethod
    def get_provider(cls, config: ConfigNode) -> MarketDataProvider:
        """Create and return the configured primary provider."""
        source = config.sources.primary
        return cls._create(source, config)

    @classmethod
    def get_fallback_provider(cls, config: ConfigNode) -> MarketDataProvider:
        """Create and return the configured fallback provider."""
        source = config.sources.fallback
        return cls._create(source, config)

    @classmethod
    def get_provider_by_name(cls, name: str, config: ConfigNode) -> MarketDataProvider:
        return cls._create(name, config)

    @classmethod
    def _create(cls, name: str, config: ConfigNode) -> MarketDataProvider:
        cls._ensure_providers_registered()
        if name not in cls._registry:
            raise ValueError(
                f"Unknown data source '{name}'. "
                f"Registered: {sorted(cls._registry.keys())}."
            )
        provider_class = cls._registry[name]
        provider = provider_class(config)
        logger.info(f"Created provider: {provider_class.__name__}")
        return provider

    @classmethod
    def registered_providers(cls) -> Dict[str, str]:
        cls._ensure_providers_registered()
        return {name: cls.__name__ for name, cls in cls._registry.items()}

    @classmethod
    def _ensure_providers_registered(cls) -> None:
        if not cls._registry:
            from src.ingestion.providers import ibkr_provider   # noqa
            from src.ingestion.providers import yahoo_provider  # noqa
