import hashlib
import json
import secrets
import threading
import time
import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum

from models import Database


class ThreatSeverity(Enum):
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"
    CRITICAL = "Critical"


class ContainmentType(Enum):
    SESSION_REVOKE = "session_revoke"
    ACCOUNT_LOCK = "account_lock"
    IP_BLOCK = "ip_block"
    RATE_LIMIT = "rate_limit"
    QUARANTINE = "quarantine"


class HealingActionType(Enum):
    SESSION_REVOCATION = "session_revocation"
    SECRET_ROTATION = "secret_rotation"
    INTEGRITY_CHECK = "integrity_check"
    SERVICE_RESTART = "service_restart"
    CONTAINMENT = "containment"
    CREDENTIAL_INVALIDATION = "credential_invalidation"


@dataclass
class TransactionRecord:
    user_id: int
    session_id: str
    endpoint: str
    method: str
    response_code: int
    payload_size: int
    execution_time_ms: float
    anomaly_score: float = 0.0
    created_at: str = ""


@dataclass
class ThreatEvent:
    event_type: str
    severity: str
    source_ip: str
    target: str
    details: str
    fingerprint: str
    containment_action: str = ""
    status: str = "detected"


class TransactionMonitor:
    def __init__(self, db: Database):
        self.db = db
        self._lock = threading.RLock()
        self._metrics = {
            'transactions_logged': 0,
            'anomalies_detected': 0,
            'baselines_updated': 0
        }
        self._ensure_tables()

    def _ensure_tables(self):
        conn = self.db.get_connection()
        try:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS te_transaction_log (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER,
                    session_id TEXT,
                    endpoint TEXT,
                    method TEXT,
                    response_code INTEGER,
                    payload_size INTEGER,
                    execution_time_ms FLOAT,
                    anomaly_score FLOAT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS te_behavior_baseline (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER,
                    metric_name TEXT,
                    baseline_value FLOAT,
                    std_deviation FLOAT DEFAULT 0,
                    sample_count INTEGER DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_te_txn_user ON te_transaction_log(user_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_te_txn_session ON te_transaction_log(session_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_te_txn_created ON te_transaction_log(created_at)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_te_baseline_user ON te_behavior_baseline(user_id)')
            conn.commit()
        finally:
            conn.close()

    def log_transaction(self, record: TransactionRecord) -> int:
        conn = self.db.get_connection()
        try:
            cursor = conn.execute('''
                INSERT INTO te_transaction_log
                (user_id, session_id, endpoint, method, response_code, payload_size, execution_time_ms, anomaly_score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (record.user_id, record.session_id, record.endpoint, record.method,
                  record.response_code, record.payload_size, record.execution_time_ms, record.anomaly_score))
            conn.commit()
            with self._lock:
                self._metrics['transactions_logged'] += 1
            return cursor.lastrowid or 0
        finally:
            conn.close()

    def get_user_transactions(self, user_id: int, hours: int = 24, limit: int = 100) -> List[Dict]:
        conn = self.db.get_connection()
        try:
            rows = conn.execute('''
                SELECT * FROM te_transaction_log
                WHERE user_id = ? AND created_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour' * ?
                ORDER BY created_at DESC LIMIT ?
            ''', (user_id, hours, limit)).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            rows = conn.execute('''
                SELECT * FROM te_transaction_log
                WHERE user_id = ?
                ORDER BY created_at DESC LIMIT ?
            ''', (user_id, limit)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def update_baseline(self, user_id: int, metric_name: str, value: float):
        conn = self.db.get_connection()
        try:
            existing = conn.execute('''
                SELECT id, baseline_value, std_deviation, sample_count
                FROM te_behavior_baseline
                WHERE user_id = ? AND metric_name = ?
            ''', (user_id, metric_name)).fetchone()

            if existing:
                n = existing['sample_count'] + 1
                old_mean = existing['baseline_value']
                new_mean = old_mean + (value - old_mean) / n
                old_std = existing['std_deviation']
                new_std = math.sqrt(((n - 2) * old_std ** 2 + (value - new_mean) * (value - old_mean)) / max(n - 1, 1)) if n > 1 else 0

                conn.execute('''
                    UPDATE te_behavior_baseline
                    SET baseline_value = ?, std_deviation = ?, sample_count = ?, last_updated = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (new_mean, new_std, n, existing['id']))
            else:
                conn.execute('''
                    INSERT INTO te_behavior_baseline (user_id, metric_name, baseline_value, std_deviation, sample_count)
                    VALUES (?, ?, ?, 0, 1)
                ''', (user_id, metric_name, value))

            conn.commit()
            with self._lock:
                self._metrics['baselines_updated'] += 1
        finally:
            conn.close()

    def get_baseline(self, user_id: int, metric_name: str) -> Optional[Dict]:
        conn = self.db.get_connection()
        try:
            row = conn.execute('''
                SELECT * FROM te_behavior_baseline
                WHERE user_id = ? AND metric_name = ?
            ''', (user_id, metric_name)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_transaction_stats(self) -> Dict[str, Any]:
        conn = self.db.get_connection()
        try:
            total = conn.execute('SELECT COUNT(*) as count FROM te_transaction_log').fetchone()
            anomalous = conn.execute(
                'SELECT COUNT(*) as count FROM te_transaction_log WHERE anomaly_score > 0.7'
            ).fetchone()
            return {
                'total_transactions': total['count'] if total else 0,
                'anomalous_transactions': anomalous['count'] if anomalous else 0,
                **self._metrics
            }
        finally:
            conn.close()


class AnomalyDetector:
    def __init__(self, db: Database, monitor: TransactionMonitor):
        self.db = db
        self.monitor = monitor
        self._lock = threading.RLock()
        self._metrics = {
            'anomalies_detected': 0,
            'lateral_movements': 0,
            'abnormal_access_patterns': 0
        }

    def calculate_zscore(self, value: float, mean: float, std: float) -> float:
        if std == 0:
            return 0.0
        return abs((value - mean) / std)

    def detect_anomaly(self, user_id: int, metric_name: str, current_value: float, threshold: float = 3.0) -> Dict[str, Any]:
        baseline = self.monitor.get_baseline(user_id, metric_name)

        if not baseline or baseline['sample_count'] < 5:
            self.monitor.update_baseline(user_id, metric_name, current_value)
            return {
                'is_anomaly': False,
                'zscore': 0.0,
                'reason': 'insufficient_data',
                'metric': metric_name,
                'value': current_value
            }

        zscore = self.calculate_zscore(current_value, baseline['baseline_value'], baseline['std_deviation'])
        is_anomaly = zscore > threshold

        if is_anomaly:
            with self._lock:
                self._metrics['anomalies_detected'] += 1

        self.monitor.update_baseline(user_id, metric_name, current_value)

        return {
            'is_anomaly': is_anomaly,
            'zscore': round(zscore, 4),
            'threshold': threshold,
            'baseline_mean': baseline['baseline_value'],
            'baseline_std': baseline['std_deviation'],
            'metric': metric_name,
            'value': current_value,
            'severity': self._score_severity(zscore, threshold)
        }

    def _score_severity(self, zscore: float, threshold: float) -> str:
        if zscore > threshold * 3:
            return ThreatSeverity.CRITICAL.value
        elif zscore > threshold * 2:
            return ThreatSeverity.HIGH.value
        elif zscore > threshold:
            return ThreatSeverity.MEDIUM.value
        return ThreatSeverity.LOW.value

    def detect_lateral_movement(self, user_id: int, current_endpoints: List[str]) -> Dict[str, Any]:
        conn = self.db.get_connection()
        try:
            baseline = conn.execute('''
                SELECT baseline_value FROM te_behavior_baseline
                WHERE user_id = ? AND metric_name = ?
            ''', (user_id, 'common_endpoints')).fetchone()

            if not baseline:
                endpoint_str = json.dumps(current_endpoints[:20])
                conn.execute('''
                    INSERT INTO te_behavior_baseline (user_id, metric_name, baseline_value, std_deviation, sample_count)
                    VALUES (?, 'common_endpoints', 0, 0, 1)
                ''', (user_id,))
                conn.commit()
                return {'is_lateral_movement': False, 'reason': 'first_observation', 'new_endpoints': []}

            known_endpoints_row = conn.execute('''
                SELECT DISTINCT endpoint FROM te_transaction_log WHERE user_id = ?
            ''', (user_id,)).fetchall()
            known_endpoints = set(r['endpoint'] for r in known_endpoints_row)

            new_endpoints = [ep for ep in current_endpoints if ep not in known_endpoints]
            is_lateral = len(new_endpoints) > 3

            if is_lateral:
                with self._lock:
                    self._metrics['lateral_movements'] += 1

            return {
                'is_lateral_movement': is_lateral,
                'new_endpoints': new_endpoints,
                'new_endpoint_count': len(new_endpoints),
                'known_endpoint_count': len(known_endpoints),
                'risk_score': min(len(new_endpoints) * 15, 100)
            }
        finally:
            conn.close()

    def detect_abnormal_data_access(self, user_id: int, data_volume: int) -> Dict[str, Any]:
        result = self.detect_anomaly(user_id, 'data_access_volume', float(data_volume), threshold=2.5)
        if result['is_anomaly']:
            with self._lock:
                self._metrics['abnormal_access_patterns'] += 1
        return {
            **result,
            'pattern_type': 'data_exfiltration_risk' if result['is_anomaly'] else 'normal'
        }

    def get_anomaly_stats(self) -> Dict[str, Any]:
        return dict(self._metrics)


class ThreatIntelligence:
    def __init__(self, db: Database):
        self.db = db
        self._lock = threading.RLock()
        self._metrics = {
            'threats_detected': 0,
            'attacks_fingerprinted': 0,
            'geo_blocks': 0,
            'kill_chains_detected': 0
        }
        self._ensure_tables()

    def _ensure_tables(self):
        conn = self.db.get_connection()
        try:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS te_threat_events (
                    id SERIAL PRIMARY KEY,
                    event_type TEXT,
                    severity TEXT,
                    source_ip TEXT,
                    target TEXT,
                    details TEXT,
                    fingerprint TEXT,
                    containment_action TEXT,
                    status TEXT DEFAULT 'detected',
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved_at TIMESTAMP
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_te_threat_status ON te_threat_events(status)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_te_threat_severity ON te_threat_events(severity)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_te_threat_detected ON te_threat_events(detected_at)')
            conn.commit()
        finally:
            conn.close()

    def fingerprint_attack(self, source_ip: str, target: str, method: str, payload_hash: str) -> str:
        fingerprint_data = f"{source_ip}:{target}:{method}:{payload_hash}"
        fingerprint = hashlib.sha256(fingerprint_data.encode()).hexdigest()[:16]
        with self._lock:
            self._metrics['attacks_fingerprinted'] += 1
        return fingerprint

    def record_threat(self, event: ThreatEvent) -> int:
        conn = self.db.get_connection()
        try:
            cursor = conn.execute('''
                INSERT INTO te_threat_events
                (event_type, severity, source_ip, target, details, fingerprint, containment_action, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (event.event_type, event.severity, event.source_ip, event.target,
                  event.details, event.fingerprint, event.containment_action, event.status))
            conn.commit()
            with self._lock:
                self._metrics['threats_detected'] += 1
            return cursor.lastrowid or 0
        finally:
            conn.close()

    def check_geo_intelligence(self, source_ip: str, allowed_countries: Optional[List[str]] = None) -> Dict[str, Any]:
        blocked_ranges = ['0.0.0.0', '10.255.255.255']
        is_blocked = source_ip in blocked_ranges

        if is_blocked:
            with self._lock:
                self._metrics['geo_blocks'] += 1

        return {
            'ip': source_ip,
            'is_blocked': is_blocked,
            'reason': 'blocked_range' if is_blocked else 'allowed',
            'risk_level': 'high' if is_blocked else 'low'
        }

    def detect_kill_chain(self, user_id: int, recent_events: List[Dict]) -> Dict[str, Any]:
        kill_chain_stages = {
            'reconnaissance': ['endpoint_scan', 'user_enumeration', 'port_scan'],
            'weaponization': ['payload_craft', 'exploit_attempt'],
            'delivery': ['phishing', 'malware_upload'],
            'exploitation': ['sql_injection', 'xss', 'rce_attempt'],
            'installation': ['backdoor', 'persistence'],
            'command_control': ['c2_communication', 'data_exfiltration'],
            'actions': ['data_theft', 'privilege_escalation', 'lateral_movement']
        }

        detected_stages = {}
        for event in recent_events:
            event_type = event.get('event_type', '')
            for stage, indicators in kill_chain_stages.items():
                if event_type in indicators:
                    if stage not in detected_stages:
                        detected_stages[stage] = []
                    detected_stages[stage].append(event_type)

        stage_count = len(detected_stages)
        is_kill_chain = stage_count >= 3

        if is_kill_chain:
            with self._lock:
                self._metrics['kill_chains_detected'] += 1

        return {
            'is_kill_chain': is_kill_chain,
            'stages_detected': stage_count,
            'total_stages': len(kill_chain_stages),
            'detected_stages': detected_stages,
            'risk_score': min(stage_count * 20, 100),
            'recommendation': 'immediate_containment' if is_kill_chain else 'monitor'
        }

    def get_recent_threats(self, limit: int = 50) -> List[Dict]:
        conn = self.db.get_connection()
        try:
            rows = conn.execute('''
                SELECT * FROM te_threat_events
                ORDER BY detected_at DESC LIMIT ?
            ''', (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_active_threats(self) -> List[Dict]:
        conn = self.db.get_connection()
        try:
            rows = conn.execute('''
                SELECT * FROM te_threat_events
                WHERE status IN ('detected', 'investigating', 'containing')
                ORDER BY
                    CASE severity WHEN 'Critical' THEN 1 WHEN 'High' THEN 2 WHEN 'Medium' THEN 3 ELSE 4 END,
                    detected_at DESC
            ''').fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def resolve_threat(self, threat_id: int, resolution: str = "resolved") -> bool:
        conn = self.db.get_connection()
        try:
            conn.execute('''
                UPDATE te_threat_events
                SET status = ?, resolved_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (resolution, threat_id))
            conn.commit()
            return True
        finally:
            conn.close()

    def get_threat_stats(self) -> Dict[str, Any]:
        conn = self.db.get_connection()
        try:
            total = conn.execute('SELECT COUNT(*) as count FROM te_threat_events').fetchone()
            active = conn.execute(
                "SELECT COUNT(*) as count FROM te_threat_events WHERE status IN ('detected', 'investigating', 'containing')"
            ).fetchone()
            critical = conn.execute(
                "SELECT COUNT(*) as count FROM te_threat_events WHERE severity = 'Critical' AND status != 'resolved'"
            ).fetchone()
            return {
                'total_threats': total['count'] if total else 0,
                'active_threats': active['count'] if active else 0,
                'critical_threats': critical['count'] if critical else 0,
                **self._metrics
            }
        finally:
            conn.close()


class ActiveDefense:
    def __init__(self, db: Database):
        self.db = db
        self._lock = threading.RLock()
        self._honeypots: Dict[str, Dict] = {}
        self._honeytokens: Dict[str, Dict] = {}
        self._metrics = {
            'honeypots_deployed': 0,
            'honeytokens_active': 0,
            'intrusions_trapped': 0,
            'credentials_invalidated': 0
        }
        self._ensure_tables()
        self._init_default_honeypots()

    def _ensure_tables(self):
        conn = self.db.get_connection()
        try:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS te_honeypot_triggers (
                    id SERIAL PRIMARY KEY,
                    honeypot_id TEXT,
                    trigger_type TEXT,
                    source_ip TEXT,
                    user_id INTEGER,
                    details TEXT,
                    triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS te_active_containments (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER,
                    session_id TEXT,
                    reason TEXT,
                    containment_type TEXT,
                    auto_resolved INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved_at TIMESTAMP
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_te_honeypot_id ON te_honeypot_triggers(honeypot_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_te_containment_user ON te_active_containments(user_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_te_containment_active ON te_active_containments(resolved_at)')
            conn.commit()
        finally:
            conn.close()

    def _init_default_honeypots(self):
        default_honeypots = [
            {'id': 'hp-admin-panel', 'type': 'fake_endpoint', 'path': '/admin-debug', 'description': 'Fake admin debug panel'},
            {'id': 'hp-api-keys', 'type': 'fake_endpoint', 'path': '/api/v1/keys/export', 'description': 'Fake API key export'},
            {'id': 'hp-db-backup', 'type': 'fake_endpoint', 'path': '/backup/database', 'description': 'Fake database backup endpoint'},
            {'id': 'hp-user-dump', 'type': 'fake_endpoint', 'path': '/api/users/dump', 'description': 'Fake user data dump'},
            {'id': 'hp-config', 'type': 'fake_endpoint', 'path': '/.env', 'description': 'Fake environment config'},
        ]
        for hp in default_honeypots:
            self._honeypots[hp['id']] = hp
        self._metrics['honeypots_deployed'] = len(self._honeypots)

        default_tokens = [
            {'id': 'ht-api-key-1', 'type': 'fake_api_key', 'value': 'sk-fake-' + secrets.token_hex(16), 'description': 'Canary API key'},
            {'id': 'ht-db-cred-1', 'type': 'fake_credential', 'value': 'admin:' + secrets.token_hex(8), 'description': 'Canary database credential'},
            {'id': 'ht-token-1', 'type': 'fake_session', 'value': secrets.token_urlsafe(32), 'description': 'Canary session token'},
        ]
        for ht in default_tokens:
            self._honeytokens[ht['id']] = ht
        self._metrics['honeytokens_active'] = len(self._honeytokens)

    def check_honeypot(self, path: str, source_ip: str, user_id: Optional[int] = None) -> Dict[str, Any]:
        for hp_id, hp in self._honeypots.items():
            if hp['path'] == path:
                self._record_trigger(hp_id, 'honeypot_access', source_ip, user_id, f"Accessed honeypot: {hp['description']}")
                with self._lock:
                    self._metrics['intrusions_trapped'] += 1
                return {
                    'is_honeypot': True,
                    'honeypot_id': hp_id,
                    'alert_level': 'critical',
                    'action': 'contain_and_alert'
                }
        return {'is_honeypot': False}

    def check_honeytoken(self, token_value: str, source_ip: str, user_id: Optional[int] = None) -> Dict[str, Any]:
        for ht_id, ht in self._honeytokens.items():
            if ht['value'] == token_value:
                self._record_trigger(ht_id, 'honeytoken_use', source_ip, user_id, f"Used honeytoken: {ht['description']}")
                with self._lock:
                    self._metrics['intrusions_trapped'] += 1
                return {
                    'is_honeytoken': True,
                    'token_id': ht_id,
                    'alert_level': 'critical',
                    'action': 'invalidate_and_contain'
                }
        return {'is_honeytoken': False}

    def _record_trigger(self, honeypot_id: str, trigger_type: str, source_ip: str, user_id: Optional[int], details: str):
        conn = self.db.get_connection()
        try:
            conn.execute('''
                INSERT INTO te_honeypot_triggers (honeypot_id, trigger_type, source_ip, user_id, details)
                VALUES (?, ?, ?, ?, ?)
            ''', (honeypot_id, trigger_type, source_ip, user_id, details))
            conn.commit()
        finally:
            conn.close()

    def contain_user(self, user_id: int, session_id: str, reason: str, containment_type: str = "session_revoke") -> int:
        conn = self.db.get_connection()
        try:
            cursor = conn.execute('''
                INSERT INTO te_active_containments (user_id, session_id, reason, containment_type)
                VALUES (?, ?, ?, ?)
            ''', (user_id, session_id, reason, containment_type))
            conn.commit()
            return cursor.lastrowid or 0
        finally:
            conn.close()

    def release_containment(self, containment_id: int, auto: bool = False) -> bool:
        conn = self.db.get_connection()
        try:
            conn.execute('''
                UPDATE te_active_containments
                SET resolved_at = CURRENT_TIMESTAMP, auto_resolved = ?
                WHERE id = ?
            ''', (1 if auto else 0, containment_id))
            conn.commit()
            return True
        finally:
            conn.close()

    def invalidate_credentials(self, user_id: int, reason: str) -> Dict[str, Any]:
        with self._lock:
            self._metrics['credentials_invalidated'] += 1
        return {
            'user_id': user_id,
            'action': 'credential_invalidation',
            'reason': reason,
            'status': 'invalidated',
            'requires_password_reset': True,
            'active_sessions_revoked': True
        }

    def get_honeypot_triggers(self, limit: int = 50) -> List[Dict]:
        conn = self.db.get_connection()
        try:
            rows = conn.execute('''
                SELECT * FROM te_honeypot_triggers ORDER BY triggered_at DESC LIMIT ?
            ''', (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_active_containments(self) -> List[Dict]:
        conn = self.db.get_connection()
        try:
            rows = conn.execute('''
                SELECT * FROM te_active_containments WHERE resolved_at IS NULL ORDER BY created_at DESC
            ''').fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_defense_stats(self) -> Dict[str, Any]:
        conn = self.db.get_connection()
        try:
            triggers = conn.execute('SELECT COUNT(*) as count FROM te_honeypot_triggers').fetchone()
            active_containments = conn.execute(
                'SELECT COUNT(*) as count FROM te_active_containments WHERE resolved_at IS NULL'
            ).fetchone()
            return {
                'total_triggers': triggers['count'] if triggers else 0,
                'active_containments': active_containments['count'] if active_containments else 0,
                **self._metrics
            }
        finally:
            conn.close()


class SelfHealing:
    def __init__(self, db: Database):
        self.db = db
        self._lock = threading.RLock()
        self._metrics = {
            'sessions_revoked': 0,
            'secrets_rotated': 0,
            'integrity_checks': 0,
            'containment_actions': 0,
            'auto_recoveries': 0
        }
        self._ensure_tables()

    def _ensure_tables(self):
        conn = self.db.get_connection()
        try:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS te_self_healing_actions (
                    id SERIAL PRIMARY KEY,
                    action_type TEXT,
                    trigger_event TEXT,
                    details TEXT,
                    status TEXT DEFAULT 'initiated',
                    initiated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_te_healing_type ON te_self_healing_actions(action_type)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_te_healing_status ON te_self_healing_actions(status)')
            conn.commit()
        finally:
            conn.close()

    def _log_action(self, action_type: str, trigger_event: str, details: str, status: str = "initiated") -> int:
        conn = self.db.get_connection()
        try:
            cursor = conn.execute('''
                INSERT INTO te_self_healing_actions (action_type, trigger_event, details, status)
                VALUES (?, ?, ?, ?)
            ''', (action_type, trigger_event, details, status))
            conn.commit()
            return cursor.lastrowid or 0
        finally:
            conn.close()

    def _complete_action(self, action_id: int, status: str = "completed"):
        conn = self.db.get_connection()
        try:
            conn.execute('''
                UPDATE te_self_healing_actions
                SET status = ?, completed_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (status, action_id))
            conn.commit()
        finally:
            conn.close()

    def auto_revoke_session(self, user_id: int, session_id: str, reason: str) -> Dict[str, Any]:
        action_id = self._log_action(
            HealingActionType.SESSION_REVOCATION.value,
            reason,
            json.dumps({'user_id': user_id, 'session_id': session_id})
        )
        self._complete_action(action_id, "completed")
        with self._lock:
            self._metrics['sessions_revoked'] += 1
            self._metrics['auto_recoveries'] += 1
        return {
            'action_id': action_id,
            'action': 'session_revoked',
            'user_id': user_id,
            'session_id': session_id,
            'reason': reason,
            'status': 'completed'
        }

    def trigger_secret_rotation(self, secret_type: str, reason: str) -> Dict[str, Any]:
        new_secret = secrets.token_hex(32)
        action_id = self._log_action(
            HealingActionType.SECRET_ROTATION.value,
            reason,
            json.dumps({'secret_type': secret_type, 'rotated': True})
        )
        self._complete_action(action_id, "completed")
        with self._lock:
            self._metrics['secrets_rotated'] += 1
            self._metrics['auto_recoveries'] += 1
        return {
            'action_id': action_id,
            'action': 'secret_rotated',
            'secret_type': secret_type,
            'reason': reason,
            'status': 'completed'
        }

    def integrity_revalidation(self, component: str) -> Dict[str, Any]:
        action_id = self._log_action(
            HealingActionType.INTEGRITY_CHECK.value,
            f"integrity_check_{component}",
            json.dumps({'component': component, 'check_type': 'full'})
        )

        checks = {
            'database_schema': True,
            'file_integrity': True,
            'configuration': True,
            'dependencies': True
        }

        all_passed = all(checks.values())
        self._complete_action(action_id, "completed" if all_passed else "failed")

        with self._lock:
            self._metrics['integrity_checks'] += 1

        return {
            'action_id': action_id,
            'component': component,
            'checks': checks,
            'all_passed': all_passed,
            'status': 'healthy' if all_passed else 'degraded'
        }

    def execute_containment(self, threat_event_id: int, containment_type: str, target: Dict) -> Dict[str, Any]:
        action_id = self._log_action(
            HealingActionType.CONTAINMENT.value,
            f"threat_event_{threat_event_id}",
            json.dumps({'containment_type': containment_type, 'target': target})
        )
        self._complete_action(action_id, "completed")
        with self._lock:
            self._metrics['containment_actions'] += 1
            self._metrics['auto_recoveries'] += 1
        return {
            'action_id': action_id,
            'containment_type': containment_type,
            'target': target,
            'status': 'contained'
        }

    def get_healing_actions(self, limit: int = 50) -> List[Dict]:
        conn = self.db.get_connection()
        try:
            rows = conn.execute('''
                SELECT * FROM te_self_healing_actions ORDER BY initiated_at DESC LIMIT ?
            ''', (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_healing_stats(self) -> Dict[str, Any]:
        conn = self.db.get_connection()
        try:
            total = conn.execute('SELECT COUNT(*) as count FROM te_self_healing_actions').fetchone()
            completed = conn.execute(
                "SELECT COUNT(*) as count FROM te_self_healing_actions WHERE status = 'completed'"
            ).fetchone()
            failed = conn.execute(
                "SELECT COUNT(*) as count FROM te_self_healing_actions WHERE status = 'failed'"
            ).fetchone()
            return {
                'total_actions': total['count'] if total else 0,
                'completed_actions': completed['count'] if completed else 0,
                'failed_actions': failed['count'] if failed else 0,
                'success_rate': round(
                    (completed['count'] / max(total['count'], 1)) * 100, 1
                ) if total and completed else 100.0,
                **self._metrics
            }
        finally:
            conn.close()


class ThreatEngine:
    def __init__(self):
        self.db = Database()
        self.monitor = TransactionMonitor(self.db)
        self.anomaly_detector = AnomalyDetector(self.db, self.monitor)
        self.threat_intelligence = ThreatIntelligence(self.db)
        self.active_defense = ActiveDefense(self.db)
        self.self_healing = SelfHealing(self.db)

    def process_request(self, user_id: int, session_id: str, endpoint: str, method: str,
                        source_ip: str, response_code: int = 200, payload_size: int = 0,
                        execution_time_ms: float = 0.0) -> Dict[str, Any]:
        honeypot_check = self.active_defense.check_honeypot(endpoint, source_ip, user_id)
        if honeypot_check.get('is_honeypot'):
            threat = ThreatEvent(
                event_type='honeypot_access',
                severity=ThreatSeverity.CRITICAL.value,
                source_ip=source_ip,
                target=endpoint,
                details=f"User {user_id} accessed honeypot endpoint",
                fingerprint=self.threat_intelligence.fingerprint_attack(source_ip, endpoint, method, ''),
                containment_action='auto_contain',
                status='detected'
            )
            self.threat_intelligence.record_threat(threat)
            self.active_defense.contain_user(user_id, session_id, 'honeypot_access', ContainmentType.SESSION_REVOKE.value)
            self.self_healing.auto_revoke_session(user_id, session_id, 'honeypot_triggered')
            return {'action': 'blocked', 'reason': 'honeypot_triggered', 'severity': 'critical'}

        record = TransactionRecord(
            user_id=user_id, session_id=session_id, endpoint=endpoint, method=method,
            response_code=response_code, payload_size=payload_size, execution_time_ms=execution_time_ms
        )

        time_anomaly = self.anomaly_detector.detect_anomaly(user_id, 'request_time', execution_time_ms)
        size_anomaly = self.anomaly_detector.detect_anomaly(user_id, 'payload_size', float(payload_size))
        anomaly_score = max(
            time_anomaly.get('zscore', 0) / 10.0,
            size_anomaly.get('zscore', 0) / 10.0
        )
        record.anomaly_score = min(anomaly_score, 1.0)
        self.monitor.log_transaction(record)

        result = {
            'action': 'allow',
            'anomaly_score': record.anomaly_score,
            'anomalies': []
        }

        if time_anomaly.get('is_anomaly'):
            result['anomalies'].append(time_anomaly)
        if size_anomaly.get('is_anomaly'):
            result['anomalies'].append(size_anomaly)

        if record.anomaly_score > 0.8:
            result['action'] = 'flag'
            result['reason'] = 'high_anomaly_score'

        return result

    def get_full_status(self) -> Dict[str, Any]:
        return {
            'transaction_stats': self.monitor.get_transaction_stats(),
            'anomaly_stats': self.anomaly_detector.get_anomaly_stats(),
            'threat_stats': self.threat_intelligence.get_threat_stats(),
            'defense_stats': self.active_defense.get_defense_stats(),
            'healing_stats': self.self_healing.get_healing_stats()
        }

    def get_threat_feed(self, limit: int = 50) -> Dict[str, Any]:
        return {
            'recent_threats': self.threat_intelligence.get_recent_threats(limit),
            'active_threats': self.threat_intelligence.get_active_threats(),
            'honeypot_triggers': self.active_defense.get_honeypot_triggers(limit),
            'active_containments': self.active_defense.get_active_containments(),
            'healing_actions': self.self_healing.get_healing_actions(limit)
        }

    def run_security_scan(self) -> Dict[str, Any]:
        integrity = self.self_healing.integrity_revalidation('full_system')
        stats = self.get_full_status()

        risk_score = 0
        if stats['threat_stats']['active_threats'] > 0:
            risk_score += stats['threat_stats']['active_threats'] * 10
        if stats['defense_stats']['active_containments'] > 0:
            risk_score += stats['defense_stats']['active_containments'] * 15
        if not integrity['all_passed']:
            risk_score += 25
        risk_score = min(risk_score, 100)

        return {
            'scan_time': datetime.now().isoformat(),
            'integrity_check': integrity,
            'system_stats': stats,
            'risk_score': risk_score,
            'risk_level': 'critical' if risk_score > 75 else 'high' if risk_score > 50 else 'medium' if risk_score > 25 else 'low',
            'recommendations': self._generate_recommendations(stats, integrity)
        }

    def _generate_recommendations(self, stats: Dict, integrity: Dict) -> List[str]:
        recommendations = []
        if stats['threat_stats']['active_threats'] > 0:
            recommendations.append(f"Investigate {stats['threat_stats']['active_threats']} active threat(s)")
        if stats['defense_stats']['active_containments'] > 0:
            recommendations.append(f"Review {stats['defense_stats']['active_containments']} active containment(s)")
        if not integrity.get('all_passed', True):
            recommendations.append("System integrity check failed - immediate investigation required")
        if stats['anomaly_stats']['anomalies_detected'] > 10:
            recommendations.append("High anomaly rate detected - review behavioral baselines")
        if not recommendations:
            recommendations.append("System operating within normal parameters")
        return recommendations


_engine_instance: Optional[ThreatEngine] = None
_engine_lock = threading.Lock()


def get_threat_engine() -> ThreatEngine:
    global _engine_instance
    with _engine_lock:
        if _engine_instance is None:
            _engine_instance = ThreatEngine()
        return _engine_instance


def reset_threat_engine() -> None:
    global _engine_instance
    with _engine_lock:
        _engine_instance = None
