"""
Performance Instrumentation System

Patent-Eligible Technical Implementation:
This module provides comprehensive performance monitoring and instrumentation
for the ERP system, capturing measurable improvements in computational efficiency.

Technical Improvements Measured:
- Query reduction metrics (database I/O optimization)
- Cache hit ratios (memory efficiency)
- Latency before vs after graph resolution
- Event replay time vs full data reconstruction
- CPU and memory utilization patterns
"""

import json
import threading
import time
import functools
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from collections import defaultdict
from contextlib import contextmanager

from models import Database


@dataclass
class PerformanceMetric:
    """
    Individual performance measurement.
    
    Technical Specification:
    - Timestamped for temporal analysis
    - Categorized for aggregation
    - Contains context for debugging
    """
    metric_type: str
    metric_name: str
    value: float
    unit: str
    context: Optional[Dict[str, Any]]
    recorded_at: datetime


@dataclass
class LatencyMeasurement:
    """
    Latency measurement with before/after comparison.
    
    Technical Specification:
    - Captures operation timing
    - Compares optimized vs baseline
    - Calculates improvement percentage
    """
    operation: str
    baseline_ms: float
    optimized_ms: float
    improvement_percent: float
    sample_count: int
    recorded_at: datetime


class PerformanceProfiler:
    """
    Comprehensive performance instrumentation system.
    
    Technical Implementation:
    - Automatic metric collection via decorators
    - Real-time aggregation for dashboards
    - Historical storage for trend analysis
    - Comparison metrics for optimization validation
    """
    
    def __init__(self):
        self.db = Database()
        self._metrics: Dict[str, List[PerformanceMetric]] = defaultdict(list)
        self._latency_samples: Dict[str, List[float]] = defaultdict(list)
        self._baseline_latencies: Dict[str, float] = {}
        self._cache_stats = {'hits': 0, 'misses': 0}
        self._query_counts = {'graph': 0, 'database': 0, 'cache': 0}
        self._lock = threading.RLock()
        self._session_start = datetime.now()
        self._ensure_tables_exist()
    
    def _ensure_tables_exist(self):
        """Create performance tables."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_type TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT,
                context TEXT,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS latency_comparisons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation TEXT NOT NULL,
                baseline_ms REAL NOT NULL,
                optimized_ms REAL NOT NULL,
                improvement_percent REAL NOT NULL,
                sample_count INTEGER NOT NULL,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS performance_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_type TEXT NOT NULL,
                snapshot_data TEXT NOT NULL,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_perf_type ON performance_metrics(metric_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_perf_recorded ON performance_metrics(recorded_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_latency_op ON latency_comparisons(operation)')
        
        conn.commit()
        conn.close()
    
    def record_metric(
        self,
        metric_type: str,
        metric_name: str,
        value: float,
        unit: str = '',
        context: Optional[Dict[str, Any]] = None,
        persist: bool = True
    ):
        """
        Record a performance metric.
        
        Technical Specification:
        - Stores in memory for real-time access
        - Optionally persists to database
        - Aggregates by type for dashboards
        """
        metric = PerformanceMetric(
            metric_type=metric_type,
            metric_name=metric_name,
            value=value,
            unit=unit,
            context=context,
            recorded_at=datetime.now()
        )
        
        with self._lock:
            self._metrics[metric_type].append(metric)
            
            if len(self._metrics[metric_type]) > 1000:
                self._metrics[metric_type] = self._metrics[metric_type][-500:]
        
        if persist:
            self._persist_metric(metric)
    
    def _persist_metric(self, metric: PerformanceMetric):
        """Persist metric to database."""
        conn = self.db.get_connection()
        conn.execute('''
            INSERT INTO performance_metrics 
            (metric_type, metric_name, value, unit, context, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            metric.metric_type, metric.metric_name, metric.value,
            metric.unit, json.dumps(metric.context) if metric.context else None,
            metric.recorded_at.isoformat()
        ))
        conn.commit()
        conn.close()
    
    def record_cache_hit(self):
        """Record a cache hit."""
        with self._lock:
            self._cache_stats['hits'] += 1
        self.record_metric('cache', 'hit', 1, 'count', persist=False)
    
    def record_cache_miss(self):
        """Record a cache miss."""
        with self._lock:
            self._cache_stats['misses'] += 1
        self.record_metric('cache', 'miss', 1, 'count', persist=False)
    
    def record_query(self, query_type: str = 'database'):
        """Record a query execution."""
        with self._lock:
            self._query_counts[query_type] = self._query_counts.get(query_type, 0) + 1
    
    @contextmanager
    def measure_latency(self, operation: str):
        """
        Context manager for latency measurement.
        
        Usage:
            with profiler.measure_latency('graph_traversal'):
                result = graph.traverse(...)
        
        Technical Specification:
        - Captures start and end time
        - Records to latency samples
        - Enables before/after comparison
        """
        start_time = time.perf_counter()
        try:
            yield
        finally:
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            with self._lock:
                self._latency_samples[operation].append(elapsed_ms)
                if len(self._latency_samples[operation]) > 1000:
                    self._latency_samples[operation] = self._latency_samples[operation][-500:]
            
            self.record_metric('latency', operation, elapsed_ms, 'ms', persist=False)
    
    def set_baseline_latency(self, operation: str, baseline_ms: float):
        """
        Set baseline latency for comparison.
        
        Technical Specification:
        - Used to compare optimized vs unoptimized performance
        - Enables percentage improvement calculation
        """
        with self._lock:
            self._baseline_latencies[operation] = baseline_ms
    
    def get_latency_improvement(self, operation: str) -> Optional[LatencyMeasurement]:
        """
        Calculate latency improvement vs baseline.
        
        Technical Specification:
        - Compares average optimized latency to baseline
        - Returns improvement percentage
        - Persists comparison for reporting
        """
        with self._lock:
            samples = self._latency_samples.get(operation, [])
            baseline = self._baseline_latencies.get(operation)
        
        if not samples or baseline is None:
            return None
        
        avg_optimized = sum(samples) / len(samples)
        improvement = ((baseline - avg_optimized) / baseline) * 100 if baseline > 0 else 0
        
        measurement = LatencyMeasurement(
            operation=operation,
            baseline_ms=baseline,
            optimized_ms=avg_optimized,
            improvement_percent=improvement,
            sample_count=len(samples),
            recorded_at=datetime.now()
        )
        
        conn = self.db.get_connection()
        conn.execute('''
            INSERT INTO latency_comparisons 
            (operation, baseline_ms, optimized_ms, improvement_percent, sample_count)
            VALUES (?, ?, ?, ?, ?)
        ''', (operation, baseline, avg_optimized, improvement, len(samples)))
        conn.commit()
        conn.close()
        
        return measurement
    
    def get_cache_hit_ratio(self) -> float:
        """
        Calculate cache hit ratio.
        
        Technical Specification:
        - Returns ratio of hits to total accesses
        - Key metric for memory efficiency
        """
        with self._lock:
            total = self._cache_stats['hits'] + self._cache_stats['misses']
            if total == 0:
                return 0.0
            return self._cache_stats['hits'] / total
    
    def get_query_reduction(self, baseline_queries: int = None) -> Dict[str, Any]:
        """
        Calculate query reduction metrics.
        
        Technical Specification:
        - Compares current queries to baseline
        - Calculates I/O reduction percentage
        """
        with self._lock:
            current_queries = sum(self._query_counts.values())
            graph_queries = self._query_counts.get('graph', 0)
            db_queries = self._query_counts.get('database', 0)
            cache_queries = self._query_counts.get('cache', 0)
        
        reduction = 0
        if baseline_queries and baseline_queries > 0:
            reduction = ((baseline_queries - db_queries) / baseline_queries) * 100
        
        return {
            'total_queries': current_queries,
            'graph_queries': graph_queries,
            'database_queries': db_queries,
            'cache_queries': cache_queries,
            'query_reduction_percent': reduction,
            'baseline_queries': baseline_queries
        }
    
    def create_snapshot(self) -> Dict[str, Any]:
        """
        Create comprehensive performance snapshot.
        
        Technical Specification:
        - Captures all current metrics
        - Persists for historical analysis
        - Returns complete system state
        """
        snapshot = {
            'timestamp': datetime.now().isoformat(),
            'session_duration_seconds': (datetime.now() - self._session_start).seconds,
            'cache_stats': {
                'hit_ratio': self.get_cache_hit_ratio(),
                'hits': self._cache_stats['hits'],
                'misses': self._cache_stats['misses']
            },
            'query_stats': dict(self._query_counts),
            'latency_summary': {},
            'metric_counts': {}
        }
        
        with self._lock:
            for operation, samples in self._latency_samples.items():
                if samples:
                    snapshot['latency_summary'][operation] = {
                        'average_ms': sum(samples) / len(samples),
                        'min_ms': min(samples),
                        'max_ms': max(samples),
                        'sample_count': len(samples)
                    }
            
            for metric_type, metrics in self._metrics.items():
                snapshot['metric_counts'][metric_type] = len(metrics)
        
        conn = self.db.get_connection()
        conn.execute('''
            INSERT INTO performance_snapshots (snapshot_type, snapshot_data)
            VALUES ('comprehensive', ?)
        ''', (json.dumps(snapshot, default=str),))
        conn.commit()
        conn.close()
        
        return snapshot
    
    def get_aggregated_metrics(
        self,
        metric_type: Optional[str] = None,
        since: Optional[datetime] = None,
        aggregate_by: str = 'hour'
    ) -> List[Dict[str, Any]]:
        """
        Get aggregated metrics for reporting.
        
        Technical Specification:
        - Groups metrics by time period
        - Calculates averages, min, max
        - Supports filtering by type and time
        """
        conn = self.db.get_connection()
        
        time_format = {
            'minute': '%Y-%m-%d %H:%M',
            'hour': '%Y-%m-%d %H',
            'day': '%Y-%m-%d'
        }.get(aggregate_by, '%Y-%m-%d %H')
        
        query = f'''
            SELECT 
                metric_type,
                metric_name,
                strftime('{time_format}', recorded_at) as period,
                AVG(value) as avg_value,
                MIN(value) as min_value,
                MAX(value) as max_value,
                COUNT(*) as sample_count
            FROM performance_metrics
            WHERE 1=1
        '''
        params = []
        
        if metric_type:
            query += ' AND metric_type = ?'
            params.append(metric_type)
        
        if since:
            query += ' AND recorded_at >= ?'
            params.append(since.isoformat())
        
        query += f' GROUP BY metric_type, metric_name, period ORDER BY period DESC LIMIT 100'
        
        rows = conn.execute(query, params).fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    def get_performance_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive performance report.
        
        Technical Specification:
        - Summarizes all performance dimensions
        - Includes improvement metrics
        - Provides actionable insights
        """
        report = {
            'generated_at': datetime.now().isoformat(),
            'session_info': {
                'start_time': self._session_start.isoformat(),
                'duration_seconds': (datetime.now() - self._session_start).seconds
            },
            'cache_performance': {
                'hit_ratio': self.get_cache_hit_ratio(),
                'total_accesses': self._cache_stats['hits'] + self._cache_stats['misses'],
                'efficiency_rating': 'excellent' if self.get_cache_hit_ratio() > 0.8 else 
                                    'good' if self.get_cache_hit_ratio() > 0.6 else 'needs_improvement'
            },
            'query_performance': self.get_query_reduction(),
            'latency_analysis': {},
            'optimization_summary': {
                'improvements': [],
                'areas_for_improvement': []
            }
        }
        
        for operation, samples in self._latency_samples.items():
            if samples:
                avg = sum(samples) / len(samples)
                baseline = self._baseline_latencies.get(operation)
                
                report['latency_analysis'][operation] = {
                    'average_ms': avg,
                    'baseline_ms': baseline,
                    'improvement_percent': ((baseline - avg) / baseline * 100) if baseline else None,
                    'samples': len(samples)
                }
                
                if baseline and avg < baseline * 0.8:
                    report['optimization_summary']['improvements'].append({
                        'operation': operation,
                        'improvement': f'{((baseline - avg) / baseline * 100):.1f}%'
                    })
                elif baseline and avg > baseline:
                    report['optimization_summary']['areas_for_improvement'].append({
                        'operation': operation,
                        'regression': f'{((avg - baseline) / baseline * 100):.1f}%'
                    })
        
        return report


def profile_function(profiler: 'PerformanceProfiler', operation_name: str):
    """
    Decorator for automatic function profiling.
    
    Usage:
        @profile_function(profiler, 'my_operation')
        def my_function():
            ...
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with profiler.measure_latency(operation_name):
                return func(*args, **kwargs)
        return wrapper
    return decorator


_profiler_instance: Optional[PerformanceProfiler] = None
_profiler_lock = threading.Lock()


def get_profiler() -> PerformanceProfiler:
    """Singleton accessor for Performance Profiler."""
    global _profiler_instance
    with _profiler_lock:
        if _profiler_instance is None:
            _profiler_instance = PerformanceProfiler()
        return _profiler_instance


def reset_profiler() -> None:
    """Reset global profiler instance (for testing)."""
    global _profiler_instance
    with _profiler_lock:
        _profiler_instance = None
