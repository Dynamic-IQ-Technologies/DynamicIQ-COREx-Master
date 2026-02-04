"""
Engines Module

Patent-Eligible Technical Components:
- Exchange Dependency Graph Engine (DAG for O(1) dependency resolution)
- Deterministic Event Processing Engine (idempotent, hash-linked events)
- AI Execution Path Modifier (dynamic scheduling, caching, locking)
- Performance Profiler (instrumentation and metrics)
- Exchange Orchestrator (unified integration layer)
"""

from engines.exchange_graph import (
    ExchangeDependencyGraph,
    ExchangeChainNode,
    ExchangeDependencyEdge,
    DeterministicExchangeEvent,
    DependencyType,
    NodeState,
    get_exchange_graph,
    reset_exchange_graph
)

from engines.event_engine import (
    DeterministicEventEngine,
    ProcessedEvent,
    EventType,
    ProcessingStatus,
    get_event_engine,
    reset_event_engine
)

from engines.ai_executor import (
    AIExecutionPathModifier,
    ExecutionModification,
    RiskLevel,
    ExecutionHint,
    RiskVector,
    get_ai_modifier,
    reset_ai_modifier
)

from engines.performance_profiler import (
    PerformanceProfiler,
    PerformanceMetric,
    LatencyMeasurement,
    get_profiler,
    reset_profiler,
    profile_function
)

from engines.orchestrator import (
    ExchangeOrchestrator,
    get_orchestrator,
    reset_orchestrator
)

from engines.asc_ai import (
    ASCEngine,
    asc_engine,
    transaction_guard,
    asc_instrument,
    AnomalyType,
    Severity,
    CorrectionStatus,
    CorrectionType
)

__all__ = [
    'ExchangeDependencyGraph',
    'ExchangeChainNode',
    'ExchangeDependencyEdge',
    'DeterministicExchangeEvent',
    'DependencyType',
    'NodeState',
    'get_exchange_graph',
    'reset_exchange_graph',
    'DeterministicEventEngine',
    'ProcessedEvent',
    'EventType',
    'ProcessingStatus',
    'get_event_engine',
    'reset_event_engine',
    'AIExecutionPathModifier',
    'ExecutionModification',
    'RiskLevel',
    'ExecutionHint',
    'RiskVector',
    'get_ai_modifier',
    'reset_ai_modifier',
    'PerformanceProfiler',
    'PerformanceMetric',
    'LatencyMeasurement',
    'get_profiler',
    'reset_profiler',
    'profile_function',
    'ExchangeOrchestrator',
    'get_orchestrator',
    'reset_orchestrator',
    'ASCEngine',
    'asc_engine',
    'transaction_guard',
    'asc_instrument',
    'AnomalyType',
    'Severity',
    'CorrectionStatus',
    'CorrectionType'
]
