"""
TradeAnalytics Market Data Provider Factory
============================================
Reads config.data.source and returns the correct provider instance.
Switching providers requires only a config change — no code changes.

Usage:
    from src.ingestion.factory.provider_factory import MarketDataFactory

    config = ConfigLoader.load()
    provider = MarketDataFactory.get_provider(config)
    fallback = MarketDataFactory.get_fallback_provider(config)

    # Check capabilities before use
    if not provider.supports_interval("1h"):
        provider = fallback

Adding a new provider:
    1. Create src/ingestion/providers/new_provider.py
    2. Implement MarketDataProvider ABC
    3. Add one line: MarketDataFactory.register("new_name", NewProvider)
    4. Add to config: data.source: new_name
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

    # Registry: provider_name → provider class
    _registry: Dict[str, Type[MarketDataProvider]] = {}

    @classmethod
    def register(cls, name: str, provider_class: Type[MarketDataProvider]) -> None:
        """
        Register a provider class under a name.
        Called at module import time by each provider.

        Args:
            name:           Must match config data.source value e.g. "ibkr"
            provider_class: Class that implements MarketDataProvider
        """
        if name in cls._registry:
            logger.warning(
                f"Provider '{name}' already registered — overwriting. "
                f"Previous: {cls._registry[name].__name__}, "
                f"New: {provider_class.__name__}"
            )
        cls._registry[name] = provider_class
        logger.debug(f"Registered provider: '{name}' → {provider_class.__name__}")

    @classmethod
    def get_provider(cls, config: ConfigNode) -> MarketDataProvider:
        """
        Create and return the configured primary provider.

        Reads: config.data.source
        e.g. if dev.yml has data.source: ibkr → returns IBKRProvider(config)

        Raises:
            ValueError: if configured source is not registered
        """
        source = config.data.source
        return cls._create(source, config)

    @classmethod
    def get_fallback_provider(cls, config: ConfigNode) -> MarketDataProvider:
        """
        Create and return the configured fallback provider.

        Reads: config.data.fallback_source
        Used when primary provider health check fails.
        """
        source = config.data.fallback_source
        return cls._create(source, config)

    @classmethod
    def get_provider_by_name(
        cls,
        name: str,
        config: ConfigNode,
    ) -> MarketDataProvider:
        """
        Create and return a specific provider by name.
        Useful for testing or explicit provider selection.
        """
        return cls._create(name, config)

    @classmethod
    def _create(cls, name: str, config: ConfigNode) -> MarketDataProvider:
        """Create provider instance. Raises if not registered."""
        cls._ensure_providers_registered()

        if name not in cls._registry:
            raise ValueError(
                f"Unknown data source '{name}'. "
                f"Registered providers: {sorted(cls._registry.keys())}. "
                f"Check config data.source or register the provider."
            )

        provider_class = cls._registry[name]
        provider = provider_class(config)

        logger.info(
            f"Created provider: {provider_class.__name__} "
            f"(options={provider.supports_options}, "
            f"realtime={provider.supports_realtime})"
        )
        return provider

    @classmethod
    def registered_providers(cls) -> Dict[str, str]:
        """Returns dict of registered provider names → class names."""
        cls._ensure_providers_registered()
        return {name: cls.__name__ for name, cls in cls._registry.items()}

    @classmethod
    def _ensure_providers_registered(cls) -> None:
        """
        Import provider modules to trigger self-registration.
        Called before any factory operation.
        """
        if not cls._registry:
            # Import triggers the register() calls at bottom of each file
            from src.ingestion.providers import ibkr_provider   # noqa: F401
            from src.ingestion.providers import yahoo_provider  # noqa: F401


# ── Self-registration — triggered when provider modules are imported ───────────
# Each provider registers itself at the bottom of its module file.
# The factory triggers imports via _ensure_providers_registered().
