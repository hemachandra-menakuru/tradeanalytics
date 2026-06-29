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
from typing import Dict, List, Optional, Type

from src.shared.config.config_loader import ConfigNode
from src.shared.base.data_provider import HistoricalDataProvider
from src.bronze.base.market_data_provider import MarketDataProvider

logger = logging.getLogger(__name__)


class MarketDataFactory:
    """
    Factory that creates and returns provider instances based on config.
    Uses a registry pattern — providers self-register at import time.

    Accepts any HistoricalDataProvider subclass — not just full MarketDataProvider.
    This allows future providers to implement only the capabilities they support.
    """

    _registry: Dict[str, Type[HistoricalDataProvider]] = {}
    _providers_loaded: bool = False

    @classmethod
    def register(cls, name: str, provider_class: Type[HistoricalDataProvider]) -> None:
        if name in cls._registry:
            logger.warning(f"Provider '{name}' already registered — overwriting.")
        cls._registry[name] = provider_class
        logger.debug(f"Registered provider: '{name}' → {provider_class.__name__}")

    @classmethod
    def get_provider(cls, config: ConfigNode) -> HistoricalDataProvider:
        """Create and return the configured primary provider."""
        source = config.sources.primary
        return cls._create(source, config)

    @classmethod
    def get_fallback_provider(cls, config: ConfigNode) -> HistoricalDataProvider:
        """Create and return the configured fallback provider."""
        source = config.sources.fallback
        return cls._create(source, config)

    @classmethod
    def get_provider_chain(cls, config: ConfigNode) -> List[HistoricalDataProvider]:
        """
        Return all providers in priority order from config.sources.priority.
        Falls back to [primary, fallback] if priority list not configured.
        """
        priority = config.get("sources.priority", default=None)
        if priority:
            names = list(priority)
        else:
            names = [config.sources.primary, config.sources.fallback]

        providers = []
        for name in names:
            try:
                providers.append(cls._create(name, config))
            except Exception as e:
                logger.warning(f"Could not instantiate provider '{name}': {e} — skipping")
        return providers

    @classmethod
    def get_provider_by_name(cls, name: str, config: ConfigNode) -> HistoricalDataProvider:
        return cls._create(name, config)

    @classmethod
    def _create(cls, name: str, config: ConfigNode) -> HistoricalDataProvider:
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
        # Always load both providers exactly once — regardless of manual registrations.
        # A partial registry (e.g. only yahoo registered manually) must not block ibkr loading.
        if not cls._providers_loaded:
            from src.bronze.providers import ibkr_provider   # noqa
            from src.bronze.providers import yahoo_provider  # noqa
            cls._providers_loaded = True
