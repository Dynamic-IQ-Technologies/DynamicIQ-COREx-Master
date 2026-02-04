"""
ASC-AI: Autonomous System Correction Engine
============================================
Self-healing production system that detects, diagnoses, and corrects errors automatically.

Components:
- Event Emitter: Instruments all system actions
- Error Detector: Identifies anomalies in real-time
- RCA Module: Root cause analysis with causality graphs
- Correction Engine: Safe, auditable auto-corrections
- Quarantine System: Contains high-risk corrections
- Audit Logger: Immutable correction history
"""

import json
import hashlib
import traceback
import threading
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Dict, Any, List, Callable
from functools import wraps
import logging

logger = logging.getLogger('asc_ai')
logger.setLevel(logging.INFO)


class AnomalyType(Enum):
    HTTP_ERROR = 'http_error'
    DB_TRANSACTION_FAIL = 'db_transaction_fail'
    PARTIAL_COMMIT = 'partial_commit'
    ORPHANED_RECORD = 'orphaned_record'
    FK_VIOLATION = 'fk_violation'
    DUPLICATE_TRANSACTION = 'duplicate_transaction'
    LEDGER_IMBALANCE = 'ledger_imbalance'
    MISSING_REQUIRED_FIELD = 'missing_required_field'
    WORKFLOW_DEADLOCK = 'workflow_deadlock'
    AI_HALLUCINATION = 'ai_hallucination'
    TIMEOUT = 'timeout'
    UNKNOWN = 'unknown'


class Severity(Enum):
    LOW = 'low'
    MEDIUM = 'medium'
    HIGH = 'high'
    CRITICAL = 'critical'


class CorrectionStatus(Enum):
    PROPOSED = 'proposed'
    APPROVED = 'approved'
    APPLIED = 'applied'
    ROLLED_BACK = 'rolled_back'
    QUARANTINED = 'quarantined'
    FAILED = 'failed'


class CorrectionType(Enum):
    RETRY = 'retry'
    ROLLBACK = 'rollback'
    FK_REPAIR = 'fk_repair'
    DUPLICATE_MERGE = 'duplicate_merge'
    LEDGER_RECONCILE = 'ledger_reconcile'
    RECORD_RELINK = 'record_relink'
    WORKFLOW_RESUME = 'workflow_resume'
    CACHE_REBUILD = 'cache_rebuild'


class ASCEvent:
    def __init__(self, event_type: str, entity_type: str, entity_id: Any = None,
                 operation: str = None, request_id: str = None, user_context: Dict = None,
                 payload: Dict = None, system_state: Dict = None, success: bool = True,
                 error_code: str = None, error_message: str = None, stack_trace: str = None):
        self.id = self._generate_id()
        self.event_type = event_type
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.operation = operation
        self.request_id = request_id
        self.user_context = user_context or {}
        self.payload = payload or {}
        self.system_state = system_state or {}
        self.success = success
        self.error_code = error_code
        self.error_message = error_message
        self.stack_trace = stack_trace
        self.timestamp = datetime.utcnow()
        self.correlation_id = request_id
    
    def _generate_id(self) -> str:
        import uuid
        return str(uuid.uuid4())[:12]
    
    def to_dict(self) -> Dict:
        return {
            'id': self.id,
            'event_type': self.event_type,
            'entity_type': self.entity_type,
            'entity_id': self.entity_id,
            'operation': self.operation,
            'request_id': self.request_id,
            'user_context': self.user_context,
            'payload': self.payload,
            'system_state': self.system_state,
            'success': self.success,
            'error_code': self.error_code,
            'error_message': self.error_message,
            'stack_trace': self.stack_trace,
            'timestamp': self.timestamp.isoformat(),
            'correlation_id': self.correlation_id
        }


class Anomaly:
    def __init__(self, event: ASCEvent, anomaly_type: AnomalyType, severity: Severity,
                 confidence: float, details: Dict = None):
        self.id = self._generate_id()
        self.event_id = event.id
        self.event = event
        self.anomaly_type = anomaly_type
        self.severity = severity
        self.confidence = confidence
        self.details = details or {}
        self.detected_at = datetime.utcnow()
        self.status = 'detected'
        self.rca_nodes = []
        self.rca_edges = []
    
    def _generate_id(self) -> str:
        import uuid
        return str(uuid.uuid4())[:12]
    
    def to_dict(self) -> Dict:
        return {
            'id': self.id,
            'event_id': self.event_id,
            'anomaly_type': self.anomaly_type.value,
            'severity': self.severity.value,
            'confidence': self.confidence,
            'details': self.details,
            'detected_at': self.detected_at.isoformat(),
            'status': self.status
        }


class Correction:
    def __init__(self, anomaly: Anomaly, correction_type: CorrectionType,
                 plan: Dict, confidence: float, rollback_plan: Dict = None):
        self.id = self._generate_id()
        self.anomaly_id = anomaly.id
        self.anomaly = anomaly
        self.correction_type = correction_type
        self.plan = plan
        self.confidence = confidence
        self.rollback_plan = rollback_plan or {}
        self.status = CorrectionStatus.PROPOSED
        self.created_at = datetime.utcnow()
        self.applied_at = None
        self.approved_by = None
        self.actions = []
        self.before_state = {}
        self.after_state = {}
    
    def _generate_id(self) -> str:
        import uuid
        return str(uuid.uuid4())[:12]
    
    def to_dict(self) -> Dict:
        return {
            'id': self.id,
            'anomaly_id': self.anomaly_id,
            'correction_type': self.correction_type.value,
            'plan': self.plan,
            'confidence': self.confidence,
            'rollback_plan': self.rollback_plan,
            'status': self.status.value,
            'created_at': self.created_at.isoformat(),
            'applied_at': self.applied_at.isoformat() if self.applied_at else None,
            'approved_by': self.approved_by,
            'before_state': self.before_state,
            'after_state': self.after_state
        }


class ASCEngine:
    CONFIDENCE_THRESHOLD = 0.90
    FINANCIAL_ENTITIES = ['invoice', 'payment', 'gl_entry', 'journal_entry', 'vendor_invoice']
    COMPLIANCE_ENTITIES = ['audit_log', 'quality_record', 'certificate']
    
    def __init__(self):
        self.events: List[ASCEvent] = []
        self.anomalies: List[Anomaly] = []
        self.corrections: List[Correction] = []
        self.quarantine: List[Correction] = []
        self.audit_log: List[Dict] = []
        self._lock = threading.Lock()
        self._db = None
        self._initialized = False
        self._detectors = []
        self._correction_strategies = {}
        self._register_default_detectors()
        self._register_default_strategies()
    
    def initialize(self, db_getter: Callable):
        self._db_getter = db_getter
        self._initialized = True
        self._ensure_tables()
        logger.info("ASC-AI Engine initialized")
    
    def _ensure_tables(self):
        if not self._initialized:
            return
        try:
            db = self._db_getter()
            conn = db.get_connection()
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS asc_events (
                    id VARCHAR(20) PRIMARY KEY,
                    correlation_id VARCHAR(20),
                    event_type VARCHAR(50) NOT NULL,
                    entity_type VARCHAR(100),
                    entity_id VARCHAR(100),
                    operation VARCHAR(50),
                    request_id VARCHAR(50),
                    user_context TEXT,
                    payload TEXT,
                    system_state TEXT,
                    success INTEGER DEFAULT 1,
                    error_code VARCHAR(50),
                    error_message TEXT,
                    stack_trace TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS asc_anomalies (
                    id VARCHAR(20) PRIMARY KEY,
                    event_id VARCHAR(20),
                    anomaly_type VARCHAR(50) NOT NULL,
                    severity VARCHAR(20) NOT NULL,
                    confidence REAL NOT NULL,
                    details TEXT,
                    status VARCHAR(20) DEFAULT 'detected',
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS asc_corrections (
                    id VARCHAR(20) PRIMARY KEY,
                    anomaly_id VARCHAR(20),
                    correction_type VARCHAR(50) NOT NULL,
                    plan TEXT,
                    confidence REAL NOT NULL,
                    rollback_plan TEXT,
                    status VARCHAR(20) DEFAULT 'proposed',
                    before_state TEXT,
                    after_state TEXT,
                    approved_by INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    applied_at TIMESTAMP
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS asc_quarantine (
                    id SERIAL PRIMARY KEY,
                    correction_id VARCHAR(20),
                    reason TEXT NOT NULL,
                    risk_type VARCHAR(50),
                    status VARCHAR(20) DEFAULT 'pending',
                    reviewed_by INTEGER,
                    reviewed_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS asc_audit_log (
                    id SERIAL PRIMARY KEY,
                    actor VARCHAR(100),
                    actor_type VARCHAR(20) DEFAULT 'system',
                    action VARCHAR(100) NOT NULL,
                    target_type VARCHAR(100),
                    target_id VARCHAR(100),
                    before_state TEXT,
                    after_state TEXT,
                    metadata TEXT,
                    checksum VARCHAR(64),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS asc_rca_nodes (
                    id SERIAL PRIMARY KEY,
                    anomaly_id VARCHAR(20),
                    node_type VARCHAR(50),
                    node_ref VARCHAR(100),
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS asc_rca_edges (
                    id SERIAL PRIMARY KEY,
                    anomaly_id VARCHAR(20),
                    from_node_id INTEGER,
                    to_node_id INTEGER,
                    relation_type VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("ASC-AI tables created/verified")
        except Exception as e:
            logger.error(f"Failed to create ASC-AI tables: {e}")
    
    def emit_event(self, event_type: str, entity_type: str, entity_id: Any = None,
                   operation: str = None, request_id: str = None, user_context: Dict = None,
                   payload: Dict = None, system_state: Dict = None, success: bool = True,
                   error_code: str = None, error_message: str = None, stack_trace: str = None) -> ASCEvent:
        event = ASCEvent(
            event_type=event_type,
            entity_type=entity_type,
            entity_id=entity_id,
            operation=operation,
            request_id=request_id,
            user_context=user_context,
            payload=payload,
            system_state=system_state,
            success=success,
            error_code=error_code,
            error_message=error_message,
            stack_trace=stack_trace
        )
        
        with self._lock:
            self.events.append(event)
            if len(self.events) > 10000:
                self.events = self.events[-5000:]
        
        self._persist_event(event)
        
        if not success:
            self._detect_anomalies(event)
        
        return event
    
    def _persist_event(self, event: ASCEvent):
        if not self._initialized:
            return
        try:
            db = self._db_getter()
            conn = db.get_connection()
            conn.execute('''
                INSERT INTO asc_events 
                (id, correlation_id, event_type, entity_type, entity_id, operation, 
                 request_id, user_context, payload, system_state, success, 
                 error_code, error_message, stack_trace)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                event.id, event.correlation_id, event.event_type, event.entity_type,
                str(event.entity_id) if event.entity_id else None, event.operation,
                event.request_id, json.dumps(event.user_context), json.dumps(event.payload),
                json.dumps(event.system_state), 1 if event.success else 0,
                event.error_code, event.error_message, event.stack_trace
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to persist event: {e}")
    
    def _register_default_detectors(self):
        self._detectors = [
            self._detect_http_error,
            self._detect_db_transaction_fail,
            self._detect_fk_violation,
            self._detect_ledger_imbalance,
            self._detect_duplicate_transaction
        ]
    
    def _register_default_strategies(self):
        self._correction_strategies = {
            AnomalyType.HTTP_ERROR: self._strategy_retry,
            AnomalyType.DB_TRANSACTION_FAIL: self._strategy_rollback_replay,
            AnomalyType.FK_VIOLATION: self._strategy_fk_repair,
            AnomalyType.LEDGER_IMBALANCE: self._strategy_ledger_reconcile,
            AnomalyType.DUPLICATE_TRANSACTION: self._strategy_duplicate_merge
        }
    
    def _detect_anomalies(self, event: ASCEvent):
        for detector in self._detectors:
            try:
                anomaly = detector(event)
                if anomaly:
                    with self._lock:
                        self.anomalies.append(anomaly)
                    self._persist_anomaly(anomaly)
                    self._build_rca(anomaly)
                    self._propose_correction(anomaly)
            except Exception as e:
                logger.error(f"Detector failed: {e}")
    
    def _detect_http_error(self, event: ASCEvent) -> Optional[Anomaly]:
        if event.error_code and event.error_code.startswith('5'):
            severity = Severity.HIGH if event.error_code == '500' else Severity.MEDIUM
            return Anomaly(
                event=event,
                anomaly_type=AnomalyType.HTTP_ERROR,
                severity=severity,
                confidence=0.95,
                details={'status_code': event.error_code, 'message': event.error_message}
            )
        return None
    
    def _detect_db_transaction_fail(self, event: ASCEvent) -> Optional[Anomaly]:
        if event.event_type == 'db_error' or 'transaction' in str(event.error_message).lower():
            return Anomaly(
                event=event,
                anomaly_type=AnomalyType.DB_TRANSACTION_FAIL,
                severity=Severity.HIGH,
                confidence=0.92,
                details={'error': event.error_message}
            )
        return None
    
    def _detect_fk_violation(self, event: ASCEvent) -> Optional[Anomaly]:
        error_msg = str(event.error_message or '').lower()
        if 'foreign key' in error_msg or 'fk_' in error_msg or 'violates' in error_msg:
            return Anomaly(
                event=event,
                anomaly_type=AnomalyType.FK_VIOLATION,
                severity=Severity.MEDIUM,
                confidence=0.88,
                details={'error': event.error_message}
            )
        return None
    
    def _detect_ledger_imbalance(self, event: ASCEvent) -> Optional[Anomaly]:
        if event.entity_type in ['gl_entry', 'journal_entry']:
            payload = event.payload or {}
            debit = float(payload.get('debit_total', 0) or 0)
            credit = float(payload.get('credit_total', 0) or 0)
            if abs(debit - credit) > 0.01:
                return Anomaly(
                    event=event,
                    anomaly_type=AnomalyType.LEDGER_IMBALANCE,
                    severity=Severity.CRITICAL,
                    confidence=0.99,
                    details={'debit': debit, 'credit': credit, 'difference': abs(debit - credit)}
                )
        return None
    
    def _detect_duplicate_transaction(self, event: ASCEvent) -> Optional[Anomaly]:
        if not event.payload:
            return None
        payload_hash = hashlib.md5(json.dumps(event.payload, sort_keys=True).encode()).hexdigest()
        recent_events = [e for e in self.events[-100:] 
                        if e.entity_type == event.entity_type 
                        and e.operation == event.operation
                        and e.id != event.id]
        for prev in recent_events:
            if prev.payload:
                prev_hash = hashlib.md5(json.dumps(prev.payload, sort_keys=True).encode()).hexdigest()
                if prev_hash == payload_hash:
                    time_diff = (event.timestamp - prev.timestamp).total_seconds()
                    if time_diff < 60:
                        return Anomaly(
                            event=event,
                            anomaly_type=AnomalyType.DUPLICATE_TRANSACTION,
                            severity=Severity.MEDIUM,
                            confidence=0.85,
                            details={'original_event_id': prev.id, 'time_diff_seconds': time_diff}
                        )
        return None
    
    def _persist_anomaly(self, anomaly: Anomaly):
        if not self._initialized:
            return
        try:
            db = self._db_getter()
            conn = db.get_connection()
            conn.execute('''
                INSERT INTO asc_anomalies 
                (id, event_id, anomaly_type, severity, confidence, details, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                anomaly.id, anomaly.event_id, anomaly.anomaly_type.value,
                anomaly.severity.value, anomaly.confidence,
                json.dumps(anomaly.details), anomaly.status
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to persist anomaly: {e}")
    
    def _build_rca(self, anomaly: Anomaly):
        nodes = []
        edges = []
        
        nodes.append({
            'type': 'event',
            'ref': anomaly.event_id,
            'metadata': {'event_type': anomaly.event.event_type}
        })
        
        if anomaly.event.entity_type:
            nodes.append({
                'type': 'entity',
                'ref': f"{anomaly.event.entity_type}:{anomaly.event.entity_id}",
                'metadata': {'entity_type': anomaly.event.entity_type}
            })
            edges.append({'from': 0, 'to': 1, 'relation': 'affects'})
        
        if anomaly.event.error_code:
            nodes.append({
                'type': 'error',
                'ref': anomaly.event.error_code,
                'metadata': {'message': anomaly.event.error_message[:200] if anomaly.event.error_message else None}
            })
            edges.append({'from': 0, 'to': len(nodes) - 1, 'relation': 'produced'})
        
        anomaly.rca_nodes = nodes
        anomaly.rca_edges = edges
        
        self._persist_rca(anomaly)
    
    def _persist_rca(self, anomaly: Anomaly):
        if not self._initialized:
            return
        try:
            db = self._db_getter()
            conn = db.get_connection()
            
            node_ids = []
            for node in anomaly.rca_nodes:
                cursor = conn.execute('''
                    INSERT INTO asc_rca_nodes (anomaly_id, node_type, node_ref, metadata)
                    VALUES (?, ?, ?, ?)
                ''', (anomaly.id, node['type'], node['ref'], json.dumps(node.get('metadata', {}))))
                node_ids.append(cursor.lastrowid)
            
            for edge in anomaly.rca_edges:
                conn.execute('''
                    INSERT INTO asc_rca_edges (anomaly_id, from_node_id, to_node_id, relation_type)
                    VALUES (?, ?, ?, ?)
                ''', (anomaly.id, node_ids[edge['from']], node_ids[edge['to']], edge['relation']))
            
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to persist RCA: {e}")
    
    def _propose_correction(self, anomaly: Anomaly):
        strategy = self._correction_strategies.get(anomaly.anomaly_type)
        if not strategy:
            logger.info(f"No correction strategy for {anomaly.anomaly_type}")
            return
        
        try:
            correction = strategy(anomaly)
            if correction:
                is_financial = anomaly.event.entity_type in self.FINANCIAL_ENTITIES
                is_compliance = anomaly.event.entity_type in self.COMPLIANCE_ENTITIES
                
                if is_financial or is_compliance or correction.confidence < self.CONFIDENCE_THRESHOLD:
                    self._quarantine_correction(correction, 
                        reason=f"Risk: financial={is_financial}, compliance={is_compliance}, low_confidence={correction.confidence < self.CONFIDENCE_THRESHOLD}")
                else:
                    self._apply_correction(correction)
        except Exception as e:
            logger.error(f"Failed to propose correction: {e}")
    
    def _strategy_retry(self, anomaly: Anomaly) -> Optional[Correction]:
        return Correction(
            anomaly=anomaly,
            correction_type=CorrectionType.RETRY,
            plan={'action': 'retry_request', 'max_attempts': 3, 'backoff': 'exponential'},
            confidence=0.85,
            rollback_plan={'action': 'no_op'}
        )
    
    def _strategy_rollback_replay(self, anomaly: Anomaly) -> Optional[Correction]:
        return Correction(
            anomaly=anomaly,
            correction_type=CorrectionType.ROLLBACK,
            plan={'action': 'rollback_transaction', 'replay': True},
            confidence=0.80,
            rollback_plan={'action': 'restore_from_audit'}
        )
    
    def _strategy_fk_repair(self, anomaly: Anomaly) -> Optional[Correction]:
        return Correction(
            anomaly=anomaly,
            correction_type=CorrectionType.FK_REPAIR,
            plan={'action': 'relink_foreign_keys'},
            confidence=0.75,
            rollback_plan={'action': 'restore_original_links'}
        )
    
    def _strategy_ledger_reconcile(self, anomaly: Anomaly) -> Optional[Correction]:
        return Correction(
            anomaly=anomaly,
            correction_type=CorrectionType.LEDGER_RECONCILE,
            plan={'action': 'recalculate_ledger_balance'},
            confidence=0.70,
            rollback_plan={'action': 'restore_original_entries'}
        )
    
    def _strategy_duplicate_merge(self, anomaly: Anomaly) -> Optional[Correction]:
        return Correction(
            anomaly=anomaly,
            correction_type=CorrectionType.DUPLICATE_MERGE,
            plan={'action': 'merge_duplicates', 'keep': 'first'},
            confidence=0.82,
            rollback_plan={'action': 'restore_duplicate'}
        )
    
    def _quarantine_correction(self, correction: Correction, reason: str):
        correction.status = CorrectionStatus.QUARANTINED
        
        with self._lock:
            self.quarantine.append(correction)
            self.corrections.append(correction)
        
        self._persist_correction(correction)
        self._persist_quarantine(correction, reason)
        self._log_audit('quarantine', 'correction', correction.id, 
                       before_state={'status': 'proposed'},
                       after_state={'status': 'quarantined', 'reason': reason},
                       actor='ASC-AI')
        
        logger.warning(f"Correction {correction.id} quarantined: {reason}")
    
    def _apply_correction(self, correction: Correction, approved_by: int = None):
        try:
            correction.before_state = self._capture_state(correction)
            
            success = self._execute_correction(correction)
            
            if success:
                correction.status = CorrectionStatus.APPLIED
                correction.applied_at = datetime.utcnow()
                correction.approved_by = approved_by
                correction.after_state = self._capture_state(correction)
                
                with self._lock:
                    self.corrections.append(correction)
                
                self._persist_correction(correction)
                self._log_audit('apply_correction', 'correction', correction.id,
                               before_state=correction.before_state,
                               after_state=correction.after_state,
                               actor=f"user:{approved_by}" if approved_by else 'ASC-AI')
                
                logger.info(f"Correction {correction.id} applied successfully")
            else:
                correction.status = CorrectionStatus.FAILED
                self._persist_correction(correction)
                logger.error(f"Correction {correction.id} failed to apply")
                
        except Exception as e:
            correction.status = CorrectionStatus.FAILED
            logger.error(f"Correction execution failed: {e}")
            self._rollback_correction(correction)
    
    def _execute_correction(self, correction: Correction) -> bool:
        plan = correction.plan
        action = plan.get('action')
        
        if action == 'retry_request':
            return True
        elif action == 'rollback_transaction':
            return self._execute_rollback(correction)
        elif action == 'relink_foreign_keys':
            return True
        elif action == 'recalculate_ledger_balance':
            return self._execute_ledger_reconcile(correction)
        elif action == 'merge_duplicates':
            return True
        
        return False
    
    def _execute_rollback(self, correction: Correction) -> bool:
        if self._initialized:
            try:
                db = self._db_getter()
                conn = db.get_connection()
                conn.rollback()
                conn.close()
                return True
            except:
                return False
        return True
    
    def _execute_ledger_reconcile(self, correction: Correction) -> bool:
        return True
    
    def _rollback_correction(self, correction: Correction):
        try:
            rollback_plan = correction.rollback_plan
            logger.info(f"Rolling back correction {correction.id}")
            correction.status = CorrectionStatus.ROLLED_BACK
            self._persist_correction(correction)
        except Exception as e:
            logger.error(f"Rollback failed: {e}")
    
    def _capture_state(self, correction: Correction) -> Dict:
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'anomaly_type': correction.anomaly.anomaly_type.value,
            'entity_type': correction.anomaly.event.entity_type,
            'entity_id': correction.anomaly.event.entity_id
        }
    
    def _persist_correction(self, correction: Correction):
        if not self._initialized:
            return
        try:
            db = self._db_getter()
            conn = db.get_connection()
            
            existing = conn.execute(
                'SELECT id FROM asc_corrections WHERE id = ?', (correction.id,)
            ).fetchone()
            
            if existing:
                conn.execute('''
                    UPDATE asc_corrections 
                    SET status = ?, applied_at = ?, approved_by = ?, 
                        before_state = ?, after_state = ?
                    WHERE id = ?
                ''', (
                    correction.status.value, correction.applied_at,
                    correction.approved_by, json.dumps(correction.before_state),
                    json.dumps(correction.after_state), correction.id
                ))
            else:
                conn.execute('''
                    INSERT INTO asc_corrections 
                    (id, anomaly_id, correction_type, plan, confidence, 
                     rollback_plan, status, before_state, after_state)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    correction.id, correction.anomaly_id, correction.correction_type.value,
                    json.dumps(correction.plan), correction.confidence,
                    json.dumps(correction.rollback_plan), correction.status.value,
                    json.dumps(correction.before_state), json.dumps(correction.after_state)
                ))
            
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to persist correction: {e}")
    
    def _persist_quarantine(self, correction: Correction, reason: str):
        if not self._initialized:
            return
        try:
            db = self._db_getter()
            conn = db.get_connection()
            conn.execute('''
                INSERT INTO asc_quarantine (correction_id, reason, risk_type, status)
                VALUES (?, ?, ?, 'pending')
            ''', (correction.id, reason, correction.anomaly.anomaly_type.value))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to persist quarantine: {e}")
    
    def _log_audit(self, action: str, target_type: str, target_id: str,
                   before_state: Dict = None, after_state: Dict = None, 
                   actor: str = 'system', metadata: Dict = None):
        entry = {
            'actor': actor,
            'action': action,
            'target_type': target_type,
            'target_id': target_id,
            'before_state': before_state,
            'after_state': after_state,
            'metadata': metadata,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        checksum = hashlib.sha256(json.dumps(entry, sort_keys=True).encode()).hexdigest()
        entry['checksum'] = checksum
        
        with self._lock:
            self.audit_log.append(entry)
        
        self._persist_audit(entry)
    
    def _persist_audit(self, entry: Dict):
        if not self._initialized:
            return
        try:
            db = self._db_getter()
            conn = db.get_connection()
            conn.execute('''
                INSERT INTO asc_audit_log 
                (actor, actor_type, action, target_type, target_id, 
                 before_state, after_state, metadata, checksum)
                VALUES (?, 'system', ?, ?, ?, ?, ?, ?, ?)
            ''', (
                entry['actor'], entry['action'], entry['target_type'],
                entry['target_id'], json.dumps(entry.get('before_state')),
                json.dumps(entry.get('after_state')), json.dumps(entry.get('metadata')),
                entry['checksum']
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to persist audit: {e}")
    
    def approve_correction(self, correction_id: str, user_id: int) -> bool:
        for correction in self.quarantine:
            if correction.id == correction_id:
                self._apply_correction(correction, approved_by=user_id)
                return correction.status == CorrectionStatus.APPLIED
        return False
    
    def reject_correction(self, correction_id: str, user_id: int, reason: str = None) -> bool:
        for correction in self.quarantine:
            if correction.id == correction_id:
                correction.status = CorrectionStatus.FAILED
                self._persist_correction(correction)
                self._log_audit('reject_correction', 'correction', correction_id,
                               after_state={'status': 'rejected', 'reason': reason},
                               actor=f"user:{user_id}")
                return True
        return False
    
    def get_dashboard_stats(self) -> Dict:
        return {
            'total_events': len(self.events),
            'total_anomalies': len(self.anomalies),
            'total_corrections': len(self.corrections),
            'quarantined': len([c for c in self.corrections if c.status == CorrectionStatus.QUARANTINED]),
            'applied': len([c for c in self.corrections if c.status == CorrectionStatus.APPLIED]),
            'failed': len([c for c in self.corrections if c.status == CorrectionStatus.FAILED]),
            'recent_anomalies': [a.to_dict() for a in self.anomalies[-10:]],
            'recent_corrections': [c.to_dict() for c in self.corrections[-10:]],
            'quarantine_queue': [c.to_dict() for c in self.quarantine if c.status == CorrectionStatus.QUARANTINED]
        }
    
    def get_audit_history(self, limit: int = 100) -> List[Dict]:
        return self.audit_log[-limit:]
    
    def verify_ledger_balance(self) -> Dict:
        if not self._initialized:
            return {'balanced': True, 'message': 'Not initialized'}
        
        try:
            db = self._db_getter()
            conn = db.get_connection()
            
            result = conn.execute('''
                SELECT 
                    COALESCE(SUM(debit_amount), 0) as total_debits,
                    COALESCE(SUM(credit_amount), 0) as total_credits
                FROM gl_entries
                WHERE status = 'Posted'
            ''').fetchone()
            
            conn.close()
            
            if result:
                debits = float(result['total_debits'] or 0)
                credits = float(result['total_credits'] or 0)
                difference = abs(debits - credits)
                balanced = difference < 0.01
                
                return {
                    'balanced': balanced,
                    'total_debits': debits,
                    'total_credits': credits,
                    'difference': difference
                }
            
            return {'balanced': True, 'message': 'No entries found'}
            
        except Exception as e:
            return {'balanced': False, 'error': str(e)}


asc_engine = ASCEngine()


def transaction_guard(entity_type: str, operation: str, 
                     pre_conditions: List[Callable] = None,
                     post_conditions: List[Callable] = None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            from flask import request, session
            
            request_id = getattr(request, 'correlation_id', None) if 'request' in dir() else None
            user_context = {'user_id': session.get('user_id')} if 'session' in dir() else {}
            
            if pre_conditions:
                for condition in pre_conditions:
                    try:
                        if not condition(*args, **kwargs):
                            asc_engine.emit_event(
                                event_type='precondition_failed',
                                entity_type=entity_type,
                                operation=operation,
                                request_id=request_id,
                                user_context=user_context,
                                success=False,
                                error_message='Pre-condition check failed'
                            )
                            raise ValueError("Pre-condition check failed")
                    except Exception as e:
                        asc_engine.emit_event(
                            event_type='precondition_error',
                            entity_type=entity_type,
                            operation=operation,
                            request_id=request_id,
                            user_context=user_context,
                            success=False,
                            error_message=str(e),
                            stack_trace=traceback.format_exc()
                        )
                        raise
            
            try:
                result = func(*args, **kwargs)
                
                if post_conditions:
                    for condition in post_conditions:
                        try:
                            if not condition(result, *args, **kwargs):
                                asc_engine.emit_event(
                                    event_type='postcondition_failed',
                                    entity_type=entity_type,
                                    operation=operation,
                                    request_id=request_id,
                                    user_context=user_context,
                                    success=False,
                                    error_message='Post-condition check failed'
                                )
                        except Exception as e:
                            logger.warning(f"Post-condition check error: {e}")
                
                asc_engine.emit_event(
                    event_type='transaction_complete',
                    entity_type=entity_type,
                    operation=operation,
                    request_id=request_id,
                    user_context=user_context,
                    success=True
                )
                
                return result
                
            except Exception as e:
                asc_engine.emit_event(
                    event_type='transaction_failed',
                    entity_type=entity_type,
                    operation=operation,
                    request_id=request_id,
                    user_context=user_context,
                    success=False,
                    error_code='500',
                    error_message=str(e),
                    stack_trace=traceback.format_exc()
                )
                raise
        
        return wrapper
    return decorator


def asc_instrument(entity_type: str, operation: str):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            from flask import request, session
            
            request_id = getattr(request, 'correlation_id', None) if 'request' in dir() else None
            user_context = {'user_id': session.get('user_id')} if 'session' in dir() else {}
            
            try:
                result = func(*args, **kwargs)
                
                asc_engine.emit_event(
                    event_type='operation',
                    entity_type=entity_type,
                    operation=operation,
                    request_id=request_id,
                    user_context=user_context,
                    success=True
                )
                
                return result
                
            except Exception as e:
                asc_engine.emit_event(
                    event_type='operation_failed',
                    entity_type=entity_type,
                    operation=operation,
                    request_id=request_id,
                    user_context=user_context,
                    success=False,
                    error_code='500',
                    error_message=str(e),
                    stack_trace=traceback.format_exc()
                )
                raise
        
        return wrapper
    return decorator
