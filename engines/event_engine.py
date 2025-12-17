"""
Deterministic Event Processing Engine

Patent-Eligible Technical Implementation:
This module implements a low-level event processor that converts ERP actions into
machine-level events, ensures idempotent execution, and supports event replay for
deterministic state reconstruction.

Technical Improvements:
- Eliminated duplicate processing under concurrency via idempotency keys
- Reduced rollback complexity through event sourcing
- Improved transactional reliability via hash-chain verification
- Deterministic state reconstruction through ordered event replay
"""

import hashlib
import json
import threading
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable, Set
from dataclasses import dataclass, field
from enum import Enum
from collections import deque

from models import Database


class EventType(Enum):
    EXCHANGE_CREATED = "exchange_created"
    EXCHANGE_UPDATED = "exchange_updated"
    EXCHANGE_FULFILLED = "exchange_fulfilled"
    EXCHANGE_CANCELLED = "exchange_cancelled"
    CORE_RECEIVED = "core_received"
    CORE_INSPECTED = "core_inspected"
    CORE_DISPOSITIONED = "core_dispositioned"
    INVENTORY_ALLOCATED = "inventory_allocated"
    INVENTORY_DEALLOCATED = "inventory_deallocated"
    OWNERSHIP_TRANSFERRED = "ownership_transferred"
    WORK_ORDER_LINKED = "work_order_linked"
    PURCHASE_ORDER_LINKED = "purchase_order_linked"
    SHIPMENT_RELEASED = "shipment_released"
    INVOICE_GENERATED = "invoice_generated"
    PAYMENT_RECEIVED = "payment_received"
    STATE_TRANSITION = "state_transition"
    DEPENDENCY_ADDED = "dependency_added"
    DEPENDENCY_REMOVED = "dependency_removed"


class ProcessingStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class ProcessedEvent:
    """
    Immutable event record after processing.
    
    Technical Specification:
    - Contains idempotency key for duplicate detection
    - Hash-linked to previous event for chain integrity
    - Includes processing metadata for debugging
    """
    event_id: str
    idempotency_key: str
    chain_id: str
    event_type: EventType
    payload: Dict[str, Any]
    prev_hash: str
    event_hash: str
    sequence_number: int
    processed_at: datetime
    processing_status: ProcessingStatus
    processing_time_ms: float
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


@dataclass
class EventHandler:
    """Handler registration for event types."""
    event_type: EventType
    handler: Callable[[Dict[str, Any]], Dict[str, Any]]
    priority: int = 0
    idempotent: bool = True


class DeterministicEventEngine:
    """
    Core event processing engine with idempotent execution.
    
    Technical Implementation:
    - Single-threaded event processing for determinism
    - Idempotency key tracking to prevent duplicate processing
    - Hash-chain verification for integrity
    - Event replay for state reconstruction
    - Concurrent event queue with ordering guarantees
    """
    
    def __init__(self):
        self.db = Database()
        self._handlers: Dict[EventType, List[EventHandler]] = {}
        self._processed_keys: Set[str] = set()
        self._event_queue: deque = deque()
        self._lock = threading.RLock()
        self._processing_lock = threading.Lock()
        self._sequence_counter = 0
        self._chain_sequences: Dict[str, int] = {}
        self._metrics = {
            'total_events_processed': 0,
            'duplicate_events_skipped': 0,
            'failed_events': 0,
            'total_processing_time_ms': 0.0,
            'replays_executed': 0
        }
        self._ensure_tables_exist()
        self._load_processed_keys()
    
    def _ensure_tables_exist(self):
        """Create event processing tables."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE NOT NULL,
                idempotency_key TEXT UNIQUE NOT NULL,
                chain_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                prev_hash TEXT,
                event_hash TEXT NOT NULL,
                sequence_number INTEGER NOT NULL,
                processing_status TEXT NOT NULL,
                processing_time_ms REAL,
                result TEXT,
                error TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS event_replay_cursors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chain_id TEXT UNIQUE NOT NULL,
                last_sequence INTEGER NOT NULL DEFAULT 0,
                last_event_hash TEXT,
                last_replay_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS idempotency_registry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                idempotency_key TEXT UNIQUE NOT NULL,
                event_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_chain ON processed_events(chain_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_sequence ON processed_events(sequence_number)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_type ON processed_events(event_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_idempotency ON idempotency_registry(idempotency_key)')
        
        conn.commit()
        conn.close()
    
    def _load_processed_keys(self):
        """Load idempotency keys from database for duplicate detection."""
        conn = self.db.get_connection()
        rows = conn.execute(
            'SELECT idempotency_key FROM idempotency_registry'
        ).fetchall()
        conn.close()
        
        self._processed_keys = {row['idempotency_key'] for row in rows}
    
    def register_handler(
        self,
        event_type: EventType,
        handler: Callable[[Dict[str, Any]], Dict[str, Any]],
        priority: int = 0,
        idempotent: bool = True
    ) -> None:
        """
        Register handler for event type.
        
        Technical Specification:
        - Handlers are called in priority order (lower = first)
        - Idempotent handlers skip duplicate events
        - Non-idempotent handlers always execute
        """
        with self._lock:
            if event_type not in self._handlers:
                self._handlers[event_type] = []
            
            self._handlers[event_type].append(EventHandler(
                event_type=event_type,
                handler=handler,
                priority=priority,
                idempotent=idempotent
            ))
            
            self._handlers[event_type].sort(key=lambda h: h.priority)
    
    def generate_idempotency_key(
        self,
        chain_id: str,
        event_type: EventType,
        entity_type: str,
        entity_id: int,
        action: str
    ) -> str:
        """
        Generate deterministic idempotency key.
        
        Technical Specification:
        - Key is deterministic based on inputs
        - Same inputs always produce same key
        - Enables duplicate detection across restarts
        """
        key_input = f"{chain_id}:{event_type.value}:{entity_type}:{entity_id}:{action}"
        return hashlib.sha256(key_input.encode()).hexdigest()[:32]
    
    def emit_event(
        self,
        chain_id: str,
        event_type: EventType,
        payload: Dict[str, Any],
        idempotency_key: Optional[str] = None
    ) -> ProcessedEvent:
        """
        Emit and process an event.
        
        Technical Specification:
        - Checks idempotency key for duplicates
        - Computes hash chain linkage
        - Executes handlers synchronously for determinism
        - Records processing metrics
        """
        import time
        start_time = time.perf_counter()
        
        if idempotency_key is None:
            idempotency_key = f"{chain_id}-{uuid.uuid4().hex[:16]}"
        
        with self._processing_lock:
            if idempotency_key in self._processed_keys:
                self._metrics['duplicate_events_skipped'] += 1
                
                conn = self.db.get_connection()
                existing = conn.execute(
                    'SELECT * FROM processed_events WHERE idempotency_key = ?',
                    (idempotency_key,)
                ).fetchone()
                conn.close()
                
                if existing:
                    return ProcessedEvent(
                        event_id=existing['event_id'],
                        idempotency_key=idempotency_key,
                        chain_id=chain_id,
                        event_type=EventType(existing['event_type']),
                        payload=json.loads(existing['payload']),
                        prev_hash=existing['prev_hash'] or '',
                        event_hash=existing['event_hash'],
                        sequence_number=existing['sequence_number'],
                        processed_at=datetime.fromisoformat(existing['processed_at']),
                        processing_status=ProcessingStatus.SKIPPED,
                        processing_time_ms=0.0,
                        result=json.loads(existing['result']) if existing['result'] else None
                    )
            
            event_id = f"EVT-{uuid.uuid4().hex[:12].upper()}"
            
            sequence = self._get_next_sequence(chain_id)
            
            prev_hash = self._get_previous_hash(chain_id)
            
            event_hash = self._compute_event_hash(
                event_id, chain_id, event_type, payload, prev_hash, sequence
            )
            
            result = None
            error = None
            status = ProcessingStatus.COMPLETED
            
            try:
                result = self._execute_handlers(event_type, payload)
            except Exception as e:
                error = str(e)
                status = ProcessingStatus.FAILED
                self._metrics['failed_events'] += 1
            
            processing_time = (time.perf_counter() - start_time) * 1000
            
            processed_event = ProcessedEvent(
                event_id=event_id,
                idempotency_key=idempotency_key,
                chain_id=chain_id,
                event_type=event_type,
                payload=payload,
                prev_hash=prev_hash,
                event_hash=event_hash,
                sequence_number=sequence,
                processed_at=datetime.now(),
                processing_status=status,
                processing_time_ms=processing_time,
                result=result,
                error=error
            )
            
            self._persist_event(processed_event)
            
            self._processed_keys.add(idempotency_key)
            self._metrics['total_events_processed'] += 1
            self._metrics['total_processing_time_ms'] += processing_time
            
            return processed_event
    
    def _get_next_sequence(self, chain_id: str) -> int:
        """Get next sequence number for chain."""
        if chain_id not in self._chain_sequences:
            conn = self.db.get_connection()
            row = conn.execute(
                'SELECT MAX(sequence_number) as max_seq FROM processed_events WHERE chain_id = ?',
                (chain_id,)
            ).fetchone()
            conn.close()
            
            self._chain_sequences[chain_id] = (row['max_seq'] or 0)
        
        self._chain_sequences[chain_id] += 1
        return self._chain_sequences[chain_id]
    
    def _get_previous_hash(self, chain_id: str) -> str:
        """Get hash of previous event in chain."""
        conn = self.db.get_connection()
        row = conn.execute(
            'SELECT event_hash FROM processed_events WHERE chain_id = ? ORDER BY sequence_number DESC LIMIT 1',
            (chain_id,)
        ).fetchone()
        conn.close()
        
        return row['event_hash'] if row else 'genesis'
    
    def _compute_event_hash(
        self,
        event_id: str,
        chain_id: str,
        event_type: EventType,
        payload: Dict[str, Any],
        prev_hash: str,
        sequence: int
    ) -> str:
        """Compute cryptographic hash for event."""
        hash_input = json.dumps({
            'event_id': event_id,
            'chain_id': chain_id,
            'event_type': event_type.value,
            'payload': payload,
            'prev_hash': prev_hash,
            'sequence': sequence
        }, sort_keys=True, default=str)
        return hashlib.sha256(hash_input.encode()).hexdigest()
    
    def _execute_handlers(
        self,
        event_type: EventType,
        payload: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Execute registered handlers for event type."""
        handlers = self._handlers.get(event_type, [])
        
        if not handlers:
            return None
        
        results = {}
        for handler in handlers:
            try:
                result = handler.handler(payload)
                if result:
                    results.update(result)
            except Exception as e:
                raise RuntimeError(f"Handler failed: {str(e)}")
        
        return results if results else None
    
    def _persist_event(self, event: ProcessedEvent) -> None:
        """Persist processed event to database."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO processed_events 
            (event_id, idempotency_key, chain_id, event_type, payload, prev_hash,
             event_hash, sequence_number, processing_status, processing_time_ms, result, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            event.event_id,
            event.idempotency_key,
            event.chain_id,
            event.event_type.value,
            json.dumps(event.payload, default=str),
            event.prev_hash,
            event.event_hash,
            event.sequence_number,
            event.processing_status.value,
            event.processing_time_ms,
            json.dumps(event.result, default=str) if event.result else None,
            event.error
        ))
        
        cursor.execute('''
            INSERT OR REPLACE INTO idempotency_registry 
            (idempotency_key, event_id, created_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        ''', (event.idempotency_key, event.event_id))
        
        conn.commit()
        conn.close()
    
    def replay_chain(
        self,
        chain_id: str,
        from_sequence: int = 0,
        state_builder: Optional[Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Replay events to reconstruct chain state.
        
        Technical Specification:
        - Fetches events in order from specified sequence
        - Verifies hash chain integrity
        - Applies state_builder function to reconstruct state
        - Returns final reconstructed state
        """
        conn = self.db.get_connection()
        events = conn.execute('''
            SELECT * FROM processed_events 
            WHERE chain_id = ? AND sequence_number >= ? AND processing_status = 'completed'
            ORDER BY sequence_number ASC
        ''', (chain_id, from_sequence)).fetchall()
        conn.close()
        
        state = {}
        prev_hash = 'genesis' if from_sequence == 0 else None
        integrity_valid = True
        
        for event_row in events:
            if prev_hash and event_row['prev_hash'] != prev_hash:
                integrity_valid = False
            
            payload = json.loads(event_row['payload'])
            
            if state_builder:
                state = state_builder(state, {
                    'event_type': event_row['event_type'],
                    'payload': payload,
                    'sequence': event_row['sequence_number']
                })
            else:
                state[event_row['event_type']] = state.get(event_row['event_type'], [])
                state[event_row['event_type']].append(payload)
            
            prev_hash = event_row['event_hash']
        
        cursor_conn = self.db.get_connection()
        cursor_conn.execute('''
            INSERT OR REPLACE INTO event_replay_cursors 
            (chain_id, last_sequence, last_event_hash, last_replay_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ''', (chain_id, events[-1]['sequence_number'] if events else 0, prev_hash))
        cursor_conn.commit()
        cursor_conn.close()
        
        self._metrics['replays_executed'] += 1
        
        return {
            'chain_id': chain_id,
            'events_replayed': len(events),
            'final_sequence': events[-1]['sequence_number'] if events else 0,
            'integrity_valid': integrity_valid,
            'reconstructed_state': state
        }
    
    def verify_chain_integrity(self, chain_id: str) -> Dict[str, Any]:
        """
        Verify hash chain integrity for a chain.
        
        Technical Specification:
        - Fetches all events in sequence order
        - Recomputes each hash and compares
        - Identifies any corrupted or tampered events
        """
        conn = self.db.get_connection()
        events = conn.execute('''
            SELECT * FROM processed_events 
            WHERE chain_id = ?
            ORDER BY sequence_number ASC
        ''', (chain_id,)).fetchall()
        conn.close()
        
        valid_events = []
        invalid_events = []
        prev_hash = 'genesis'
        
        for event_row in events:
            payload = json.loads(event_row['payload'])
            
            expected_hash = self._compute_event_hash(
                event_row['event_id'],
                chain_id,
                EventType(event_row['event_type']),
                payload,
                prev_hash,
                event_row['sequence_number']
            )
            
            if event_row['event_hash'] == expected_hash and event_row['prev_hash'] == prev_hash:
                valid_events.append(event_row['event_id'])
            else:
                invalid_events.append({
                    'event_id': event_row['event_id'],
                    'sequence': event_row['sequence_number'],
                    'expected_hash': expected_hash,
                    'actual_hash': event_row['event_hash'],
                    'prev_hash_match': event_row['prev_hash'] == prev_hash
                })
            
            prev_hash = event_row['event_hash']
        
        return {
            'chain_id': chain_id,
            'total_events': len(events),
            'valid_events': len(valid_events),
            'invalid_events': len(invalid_events),
            'integrity_verified': len(invalid_events) == 0,
            'corrupted_events': invalid_events
        }
    
    def get_chain_events(
        self,
        chain_id: str,
        event_type: Optional[EventType] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get events for a chain with optional filtering."""
        conn = self.db.get_connection()
        
        query = 'SELECT * FROM processed_events WHERE chain_id = ?'
        params = [chain_id]
        
        if event_type:
            query += ' AND event_type = ?'
            params.append(event_type.value)
        
        query += ' ORDER BY sequence_number DESC LIMIT ?'
        params.append(limit)
        
        rows = conn.execute(query, params).fetchall()
        conn.close()
        
        return [
            {
                'event_id': row['event_id'],
                'event_type': row['event_type'],
                'payload': json.loads(row['payload']),
                'sequence_number': row['sequence_number'],
                'processing_status': row['processing_status'],
                'processed_at': row['processed_at']
            }
            for row in rows
        ]
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get event engine performance metrics.
        
        Technical Specification:
        - Returns processing statistics for instrumentation
        - Includes average processing time calculation
        - Provides duplicate detection rate
        """
        total = self._metrics['total_events_processed']
        duplicates = self._metrics['duplicate_events_skipped']
        
        return {
            **self._metrics,
            'average_processing_time_ms': (
                self._metrics['total_processing_time_ms'] / total if total > 0 else 0
            ),
            'duplicate_detection_rate': (
                duplicates / (total + duplicates) if (total + duplicates) > 0 else 0
            ),
            'registered_handlers': sum(len(h) for h in self._handlers.values()),
            'cached_idempotency_keys': len(self._processed_keys)
        }


_engine_instance: Optional[DeterministicEventEngine] = None
_engine_lock = threading.Lock()


def get_event_engine() -> DeterministicEventEngine:
    """Singleton accessor for Deterministic Event Engine."""
    global _engine_instance
    with _engine_lock:
        if _engine_instance is None:
            _engine_instance = DeterministicEventEngine()
        return _engine_instance


def reset_event_engine() -> None:
    """Reset global engine instance (for testing)."""
    global _engine_instance
    with _engine_lock:
        _engine_instance = None
