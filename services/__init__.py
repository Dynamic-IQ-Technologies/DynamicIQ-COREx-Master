# Services package
"""
Services Module

Patent-Eligible Technical Components:
- Exchange Chain Service (persistence layer for DAG)
- Future: AI Execution Advisory Service
- Future: Performance Metrics Service
"""

from services.exchange_chain_service import (
    ExchangeChainService,
    get_exchange_chain_service
)

__all__ = [
    'ExchangeChainService',
    'get_exchange_chain_service'
]
