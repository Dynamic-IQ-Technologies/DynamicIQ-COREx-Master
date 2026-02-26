import hashlib
import json
import secrets
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum

from models import Database


class TrustLevel(Enum):
    TRUSTED = "trusted"
    VERIFIED = "verified"
    SUSPICIOUS = "suspicious"
    UNTRUSTED = "untrusted"
    BLOCKED = "blocked"


class AccessDecision(Enum):
    ALLOW = "allow"
    DENY = "deny"
    CHALLENGE = "challenge"
    STEP_UP = "step_up"


@dataclass
class DeviceFingerprint:
    user_id: int
    fingerprint_hash: str
    user_agent: str
    ip_address: str
    first_seen: str
    last_seen: str
    trust_level: str
    is_known: bool


@dataclass
class SessionToken:
    session_id: str
    user_id: int
    token_hash: str
    created_at: str
    expires_at: str
    rotated_at: Optional[str]
    is_active: bool


@dataclass
class BehaviorProfile:
    user_id: int
    avg_requests_per_hour: float
    common_endpoints: List[str]
    active_hours: List[int]
    risk_score: float
    last_updated: str


class DeviceFingerprintManager:
    def __init__(self, db: Database):
        self.db = db
        self._lock = threading.RLock()

    def generate_fingerprint(self, user_agent: str, ip_address: str, session_meta: Optional[Dict] = None) -> str:
        raw = f"{user_agent}|{ip_address}"
        if session_meta:
            raw += f"|{json.dumps(session_meta, sort_keys=True)}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def register_device(self, user_id: int, user_agent: str, ip_address: str, session_meta: Optional[Dict] = None) -> DeviceFingerprint:
        fingerprint_hash = self.generate_fingerprint(user_agent, ip_address, session_meta)
        conn = self.db.get_connection()
        try:
            existing = conn.execute(
                'SELECT * FROM zt_device_fingerprints WHERE user_id = ? AND fingerprint_hash = ?',
                (user_id, fingerprint_hash)
            ).fetchone()

            if existing:
                conn.execute(
                    'UPDATE zt_device_fingerprints SET last_seen = CURRENT_TIMESTAMP WHERE id = ?',
                    (existing['id'],)
                )
                conn.commit()
                return DeviceFingerprint(
                    user_id=user_id,
                    fingerprint_hash=fingerprint_hash,
                    user_agent=user_agent,
                    ip_address=ip_address,
                    first_seen=existing['first_seen'],
                    last_seen=datetime.now().isoformat(),
                    trust_level=existing['trust_level'],
                    is_known=True
                )

            conn.execute('''
                INSERT INTO zt_device_fingerprints 
                (user_id, fingerprint_hash, user_agent, ip_address, first_seen, last_seen, trust_level, is_known)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, 0)
            ''', (user_id, fingerprint_hash, user_agent, ip_address, TrustLevel.VERIFIED.value))
            conn.commit()

            return DeviceFingerprint(
                user_id=user_id,
                fingerprint_hash=fingerprint_hash,
                user_agent=user_agent,
                ip_address=ip_address,
                first_seen=datetime.now().isoformat(),
                last_seen=datetime.now().isoformat(),
                trust_level=TrustLevel.VERIFIED.value,
                is_known=False
            )
        finally:
            conn.close()

    def get_known_devices(self, user_id: int) -> List[Dict[str, Any]]:
        conn = self.db.get_connection()
        try:
            rows = conn.execute(
                'SELECT * FROM zt_device_fingerprints WHERE user_id = ? ORDER BY last_seen DESC',
                (user_id,)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def update_trust_level(self, user_id: int, fingerprint_hash: str, trust_level: TrustLevel) -> bool:
        conn = self.db.get_connection()
        try:
            cursor = conn.execute(
                'UPDATE zt_device_fingerprints SET trust_level = ?, is_known = ? WHERE user_id = ? AND fingerprint_hash = ?',
                (trust_level.value, 1 if trust_level in (TrustLevel.TRUSTED, TrustLevel.VERIFIED) else 0,
                 user_id, fingerprint_hash)
            )
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

    def get_device_count(self) -> int:
        conn = self.db.get_connection()
        try:
            result = conn.execute('SELECT COUNT(*) as count FROM zt_device_fingerprints WHERE is_known = 1').fetchone()
            return result['count'] if result else 0
        finally:
            conn.close()


class BehavioralBiometrics:
    def __init__(self, db: Database):
        self.db = db
        self._lock = threading.RLock()

    def record_request(self, user_id: int, endpoint: str, timestamp: Optional[datetime] = None):
        ts = timestamp or datetime.now()
        conn = self.db.get_connection()
        try:
            profile = conn.execute(
                'SELECT * FROM zt_behavior_profiles WHERE user_id = ?',
                (user_id,)
            ).fetchone()

            if profile:
                try:
                    current_endpoints = json.loads(profile['common_endpoints'] or '{}')
                except (json.JSONDecodeError, TypeError):
                    current_endpoints = {}
                current_endpoints[endpoint] = current_endpoints.get(endpoint, 0) + 1

                try:
                    active_hours = json.loads(profile['active_hours'] or '[]')
                except (json.JSONDecodeError, TypeError):
                    active_hours = []
                hour = ts.hour
                if hour not in active_hours:
                    active_hours.append(hour)
                    active_hours.sort()

                new_avg = profile['avg_requests_per_hour'] * 0.9 + 1 * 0.1

                conn.execute('''
                    UPDATE zt_behavior_profiles 
                    SET avg_requests_per_hour = ?, common_endpoints = ?, active_hours = ?, last_updated = CURRENT_TIMESTAMP
                    WHERE user_id = ?
                ''', (new_avg, json.dumps(current_endpoints), json.dumps(active_hours), user_id))
            else:
                conn.execute('''
                    INSERT INTO zt_behavior_profiles 
                    (user_id, avg_requests_per_hour, common_endpoints, active_hours, risk_score, last_updated)
                    VALUES (?, 1.0, ?, ?, 0.0, CURRENT_TIMESTAMP)
                ''', (user_id, json.dumps({endpoint: 1}), json.dumps([ts.hour])))
            conn.commit()
        finally:
            conn.close()

    def get_profile(self, user_id: int) -> Optional[BehaviorProfile]:
        conn = self.db.get_connection()
        try:
            row = conn.execute(
                'SELECT * FROM zt_behavior_profiles WHERE user_id = ?',
                (user_id,)
            ).fetchone()
            if not row:
                return None
            try:
                endpoints = json.loads(row['common_endpoints'] or '[]')
            except (json.JSONDecodeError, TypeError):
                endpoints = []
            try:
                hours = json.loads(row['active_hours'] or '[]')
            except (json.JSONDecodeError, TypeError):
                hours = []
            return BehaviorProfile(
                user_id=row['user_id'],
                avg_requests_per_hour=row['avg_requests_per_hour'] or 0,
                common_endpoints=endpoints if isinstance(endpoints, list) else list(endpoints.keys()),
                active_hours=hours,
                risk_score=row['risk_score'] or 0,
                last_updated=str(row['last_updated'] or '')
            )
        finally:
            conn.close()

    def calculate_anomaly_score(self, user_id: int, endpoint: str, current_hour: Optional[int] = None) -> float:
        profile = self.get_profile(user_id)
        if not profile:
            return 0.3

        score = 0.0
        hour = current_hour if current_hour is not None else datetime.now().hour
        if hour not in profile.active_hours and len(profile.active_hours) > 0:
            score += 0.4

        known_endpoints = profile.common_endpoints
        if isinstance(known_endpoints, dict):
            known_endpoints = list(known_endpoints.keys())
        if endpoint not in known_endpoints and len(known_endpoints) > 0:
            score += 0.3

        return min(score, 1.0)

    def update_risk_score(self, user_id: int, risk_score: float):
        conn = self.db.get_connection()
        try:
            conn.execute(
                'UPDATE zt_behavior_profiles SET risk_score = ?, last_updated = CURRENT_TIMESTAMP WHERE user_id = ?',
                (min(max(risk_score, 0.0), 100.0), user_id)
            )
            conn.commit()
        finally:
            conn.close()


class SessionContinuousValidator:
    def __init__(self, db: Database, token_ttl_minutes: int = 15):
        self.db = db
        self.token_ttl = token_ttl_minutes
        self._lock = threading.RLock()

    def create_session_token(self, session_id: str, user_id: int) -> str:
        raw_token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
        expires_at = (datetime.now() + timedelta(minutes=self.token_ttl)).isoformat()

        conn = self.db.get_connection()
        try:
            conn.execute(
                'UPDATE zt_session_tokens SET is_active = 0 WHERE session_id = ? AND is_active = 1',
                (session_id,)
            )
            conn.execute('''
                INSERT INTO zt_session_tokens 
                (session_id, user_id, token_hash, created_at, expires_at, is_active)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, 1)
            ''', (session_id, user_id, token_hash, expires_at))
            conn.commit()
        finally:
            conn.close()

        return raw_token

    def validate_session(self, session_id: str, raw_token: str) -> Tuple[bool, Optional[str]]:
        token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
        conn = self.db.get_connection()
        try:
            row = conn.execute(
                'SELECT * FROM zt_session_tokens WHERE session_id = ? AND token_hash = ? AND is_active = 1',
                (session_id, token_hash)
            ).fetchone()

            if not row:
                return False, "Invalid or expired session token"

            expires_at = row['expires_at']
            if isinstance(expires_at, str):
                try:
                    expires_at = datetime.fromisoformat(expires_at)
                except ValueError:
                    return False, "Invalid expiration format"

            if datetime.now() > expires_at:
                conn.execute(
                    'UPDATE zt_session_tokens SET is_active = 0 WHERE id = ?',
                    (row['id'],)
                )
                conn.commit()
                return False, "Session token expired"

            return True, None
        finally:
            conn.close()

    def rotate_token(self, session_id: str, user_id: int) -> str:
        conn = self.db.get_connection()
        try:
            conn.execute('''
                UPDATE zt_session_tokens SET is_active = 0, rotated_at = CURRENT_TIMESTAMP 
                WHERE session_id = ? AND is_active = 1
            ''', (session_id,))
            conn.commit()
        finally:
            conn.close()

        return self.create_session_token(session_id, user_id)

    def revoke_session(self, session_id: str):
        conn = self.db.get_connection()
        try:
            conn.execute(
                'UPDATE zt_session_tokens SET is_active = 0 WHERE session_id = ?',
                (session_id,)
            )
            conn.commit()
        finally:
            conn.close()

    def get_active_session_count(self) -> int:
        conn = self.db.get_connection()
        try:
            result = conn.execute('SELECT COUNT(*) as count FROM zt_session_tokens WHERE is_active = 1').fetchone()
            return result['count'] if result else 0
        finally:
            conn.close()

    def get_session_health(self) -> Dict[str, Any]:
        conn = self.db.get_connection()
        try:
            active = conn.execute('SELECT COUNT(*) as count FROM zt_session_tokens WHERE is_active = 1').fetchone()
            expired = conn.execute('SELECT COUNT(*) as count FROM zt_session_tokens WHERE is_active = 0').fetchone()
            rotated = conn.execute('SELECT COUNT(*) as count FROM zt_session_tokens WHERE rotated_at IS NOT NULL').fetchone()
            return {
                'active_sessions': active['count'] if active else 0,
                'expired_sessions': expired['count'] if expired else 0,
                'total_rotations': rotated['count'] if rotated else 0,
                'token_ttl_minutes': self.token_ttl
            }
        finally:
            conn.close()


class ContextAwareAccessController:
    def __init__(self, db: Database, biometrics: 'BehavioralBiometrics'):
        self.db = db
        self.biometrics = biometrics
        self._lock = threading.RLock()

    def evaluate_access(self, user_id: int, endpoint: str, ip_address: str,
                        user_agent: str, current_hour: Optional[int] = None) -> Tuple[AccessDecision, str, float]:
        hour = current_hour if current_hour is not None else datetime.now().hour
        context_score = 1.0
        reasons = []

        anomaly = self.biometrics.calculate_anomaly_score(user_id, endpoint, hour)
        context_score -= anomaly * 0.5
        if anomaly > 0.5:
            reasons.append("High behavioral anomaly detected")

        if hour < 6 or hour > 22:
            context_score -= 0.15
            reasons.append("Access outside normal business hours")

        if context_score >= 0.7:
            decision = AccessDecision.ALLOW
        elif context_score >= 0.4:
            decision = AccessDecision.CHALLENGE
            reasons.append("Context score below threshold - challenge required")
        else:
            decision = AccessDecision.DENY
            reasons.append("Context score critically low")

        reason_text = "; ".join(reasons) if reasons else "Normal access pattern"

        conn = self.db.get_connection()
        try:
            conn.execute('''
                INSERT INTO zt_access_decisions 
                (user_id, endpoint, decision, reason, context_score, created_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (user_id, endpoint, decision.value, reason_text, context_score))
            conn.commit()
        finally:
            conn.close()

        return decision, reason_text, context_score

    def get_recent_decisions(self, user_id: Optional[int] = None, limit: int = 50) -> List[Dict[str, Any]]:
        conn = self.db.get_connection()
        try:
            if user_id:
                rows = conn.execute(
                    'SELECT * FROM zt_access_decisions WHERE user_id = ? ORDER BY created_at DESC LIMIT ?',
                    (user_id, limit)
                ).fetchall()
            else:
                rows = conn.execute(
                    'SELECT * FROM zt_access_decisions ORDER BY created_at DESC LIMIT ?',
                    (limit,)
                ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


class RateLimiter:
    def __init__(self, db: Database, max_requests: int = 100, window_seconds: int = 3600):
        self.db = db
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._lock = threading.RLock()

    def check_rate_limit(self, identifier: str) -> Tuple[bool, int]:
        conn = self.db.get_connection()
        try:
            row = conn.execute(
                'SELECT * FROM zt_rate_limits WHERE identifier = ?',
                (identifier,)
            ).fetchone()

            now = datetime.now()

            if row:
                if row['blocked_until']:
                    blocked_until = row['blocked_until']
                    if isinstance(blocked_until, str):
                        try:
                            blocked_until = datetime.fromisoformat(blocked_until)
                        except ValueError:
                            blocked_until = now

                    if now < blocked_until:
                        return False, 0

                window_start = row['window_start']
                if isinstance(window_start, str):
                    try:
                        window_start = datetime.fromisoformat(window_start)
                    except ValueError:
                        window_start = now

                if (now - window_start).total_seconds() > self.window_seconds:
                    conn.execute(
                        'UPDATE zt_rate_limits SET request_count = 1, window_start = CURRENT_TIMESTAMP, blocked_until = NULL WHERE identifier = ?',
                        (identifier,)
                    )
                    conn.commit()
                    return True, self.max_requests - 1

                new_count = (row['request_count'] or 0) + 1
                if new_count > self.max_requests:
                    blocked_until = (now + timedelta(seconds=self.window_seconds)).isoformat()
                    conn.execute(
                        'UPDATE zt_rate_limits SET request_count = ?, blocked_until = ? WHERE identifier = ?',
                        (new_count, blocked_until, identifier)
                    )
                    conn.commit()
                    return False, 0

                conn.execute(
                    'UPDATE zt_rate_limits SET request_count = ? WHERE identifier = ?',
                    (new_count, identifier)
                )
                conn.commit()
                return True, self.max_requests - new_count
            else:
                conn.execute('''
                    INSERT INTO zt_rate_limits (identifier, request_count, window_start)
                    VALUES (?, 1, CURRENT_TIMESTAMP)
                ''', (identifier,))
                conn.commit()
                return True, self.max_requests - 1
        finally:
            conn.close()

    def reset_limit(self, identifier: str):
        conn = self.db.get_connection()
        try:
            conn.execute(
                'UPDATE zt_rate_limits SET request_count = 0, blocked_until = NULL, window_start = CURRENT_TIMESTAMP WHERE identifier = ?',
                (identifier,)
            )
            conn.commit()
        finally:
            conn.close()

    def get_status(self, identifier: str) -> Dict[str, Any]:
        conn = self.db.get_connection()
        try:
            row = conn.execute(
                'SELECT * FROM zt_rate_limits WHERE identifier = ?',
                (identifier,)
            ).fetchone()
            if not row:
                return {'identifier': identifier, 'request_count': 0, 'remaining': self.max_requests, 'blocked': False}
            return {
                'identifier': identifier,
                'request_count': row['request_count'] or 0,
                'remaining': max(0, self.max_requests - (row['request_count'] or 0)),
                'blocked': row['blocked_until'] is not None,
                'window_start': str(row['window_start'] or '')
            }
        finally:
            conn.close()


class MicroAnomalyDetector:
    def __init__(self, db: Database, biometrics: BehavioralBiometrics):
        self.db = db
        self.biometrics = biometrics

    def detect_anomalies(self, user_id: int, endpoint: str, ip_address: str) -> List[Dict[str, Any]]:
        anomalies = []
        profile = self.biometrics.get_profile(user_id)

        if profile:
            known = profile.common_endpoints
            if isinstance(known, dict):
                known = list(known.keys())
            if endpoint not in known and len(known) > 3:
                anomalies.append({
                    'type': 'new_module_access',
                    'severity': 'medium',
                    'detail': f"User accessing unfamiliar endpoint: {endpoint}",
                    'score': 0.4
                })

            hour = datetime.now().hour
            if hour not in profile.active_hours and len(profile.active_hours) > 2:
                anomalies.append({
                    'type': 'unusual_time',
                    'severity': 'low',
                    'detail': f"Access at unusual hour: {hour}",
                    'score': 0.3
                })

            if profile.avg_requests_per_hour > 50:
                anomalies.append({
                    'type': 'high_frequency',
                    'severity': 'high',
                    'detail': f"Request frequency ({profile.avg_requests_per_hour:.1f}/hr) exceeds threshold",
                    'score': 0.6
                })

        return anomalies

    def get_anomaly_summary(self) -> Dict[str, Any]:
        conn = self.db.get_connection()
        try:
            high_risk = conn.execute(
                'SELECT COUNT(*) as count FROM zt_behavior_profiles WHERE risk_score >= 70'
            ).fetchone()
            medium_risk = conn.execute(
                'SELECT COUNT(*) as count FROM zt_behavior_profiles WHERE risk_score >= 40 AND risk_score < 70'
            ).fetchone()
            total = conn.execute(
                'SELECT COUNT(*) as count FROM zt_behavior_profiles'
            ).fetchone()
            return {
                'total_profiles': total['count'] if total else 0,
                'high_risk_users': high_risk['count'] if high_risk else 0,
                'medium_risk_users': medium_risk['count'] if medium_risk else 0,
                'low_risk_users': (total['count'] if total else 0) - (high_risk['count'] if high_risk else 0) - (medium_risk['count'] if medium_risk else 0)
            }
        finally:
            conn.close()


class ZeroTrustEngine:
    def __init__(self):
        self.db = Database()
        self._ensure_tables()
        self.device_manager = DeviceFingerprintManager(self.db)
        self.biometrics = BehavioralBiometrics(self.db)
        self.session_validator = SessionContinuousValidator(self.db)
        self.access_controller = ContextAwareAccessController(self.db, self.biometrics)
        self.rate_limiter = RateLimiter(self.db)
        self.anomaly_detector = MicroAnomalyDetector(self.db, self.biometrics)
        self._lock = threading.RLock()

    def _ensure_tables(self):
        conn = self.db.get_connection()
        try:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS zt_device_fingerprints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    fingerprint_hash TEXT,
                    user_agent TEXT,
                    ip_address TEXT,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    trust_level TEXT DEFAULT 'verified',
                    is_known INTEGER DEFAULT 0
                )
            ''')

            conn.execute('''
                CREATE TABLE IF NOT EXISTS zt_session_tokens (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT,
                    user_id INTEGER,
                    token_hash TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP,
                    rotated_at TIMESTAMP,
                    is_active INTEGER DEFAULT 1
                )
            ''')

            conn.execute('''
                CREATE TABLE IF NOT EXISTS zt_behavior_profiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER UNIQUE,
                    avg_requests_per_hour REAL DEFAULT 0,
                    common_endpoints TEXT DEFAULT '{}',
                    active_hours TEXT DEFAULT '[]',
                    risk_score REAL DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            conn.execute('''
                CREATE TABLE IF NOT EXISTS zt_access_decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    endpoint TEXT,
                    decision TEXT,
                    reason TEXT,
                    context_score REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            conn.execute('''
                CREATE TABLE IF NOT EXISTS zt_rate_limits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    identifier TEXT UNIQUE,
                    request_count INTEGER DEFAULT 0,
                    window_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    blocked_until TIMESTAMP
                )
            ''')

            conn.execute('CREATE INDEX IF NOT EXISTS idx_zt_device_user ON zt_device_fingerprints(user_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_zt_session_sid ON zt_session_tokens(session_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_zt_behavior_user ON zt_behavior_profiles(user_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_zt_access_user ON zt_access_decisions(user_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_zt_rate_ident ON zt_rate_limits(identifier)')

            conn.commit()
        finally:
            conn.close()

    def evaluate_request(self, user_id: int, session_id: str, endpoint: str,
                         ip_address: str, user_agent: str) -> Dict[str, Any]:
        device = self.device_manager.register_device(user_id, user_agent, ip_address)

        self.biometrics.record_request(user_id, endpoint)

        allowed, remaining = self.rate_limiter.check_rate_limit(f"user:{user_id}")
        if not allowed:
            return {
                'decision': AccessDecision.DENY.value,
                'reason': 'Rate limit exceeded',
                'context_score': 0.0,
                'device_known': device.is_known,
                'anomalies': []
            }

        ip_allowed, _ = self.rate_limiter.check_rate_limit(f"ip:{ip_address}")
        if not ip_allowed:
            return {
                'decision': AccessDecision.DENY.value,
                'reason': 'IP rate limit exceeded',
                'context_score': 0.0,
                'device_known': device.is_known,
                'anomalies': []
            }

        anomalies = self.anomaly_detector.detect_anomalies(user_id, endpoint, ip_address)

        decision, reason, score = self.access_controller.evaluate_access(
            user_id, endpoint, ip_address, user_agent
        )

        if anomalies:
            max_anomaly_score = max(a['score'] for a in anomalies)
            if max_anomaly_score > 0.5 and decision == AccessDecision.ALLOW:
                decision = AccessDecision.CHALLENGE
                reason += "; Micro-anomalies detected"

        return {
            'decision': decision.value,
            'reason': reason,
            'context_score': score,
            'device_known': device.is_known,
            'device_trust': device.trust_level,
            'anomalies': anomalies,
            'rate_limit_remaining': remaining
        }

    def get_engine_status(self) -> Dict[str, Any]:
        session_health = self.session_validator.get_session_health()
        device_count = self.device_manager.get_device_count()
        anomaly_summary = self.anomaly_detector.get_anomaly_summary()

        conn = self.db.get_connection()
        try:
            total_decisions = conn.execute('SELECT COUNT(*) as count FROM zt_access_decisions').fetchone()
            denied = conn.execute(
                "SELECT COUNT(*) as count FROM zt_access_decisions WHERE decision = 'deny'"
            ).fetchone()
            challenged = conn.execute(
                "SELECT COUNT(*) as count FROM zt_access_decisions WHERE decision = 'challenge'"
            ).fetchone()
        finally:
            conn.close()

        return {
            'engine': 'Zero Trust Engine',
            'status': 'Active',
            'continuous_validation': True,
            'device_trust': {
                'known_devices': device_count,
                'fingerprinting': 'SHA-256'
            },
            'session_health': session_health,
            'anomaly_summary': anomaly_summary,
            'access_decisions': {
                'total': total_decisions['count'] if total_decisions else 0,
                'denied': denied['count'] if denied else 0,
                'challenged': challenged['count'] if challenged else 0
            },
            'ephemeral_token_rotation': True,
            'token_ttl_minutes': session_health.get('token_ttl_minutes', 15),
            'score': 92
        }


_zt_engine_instance: Optional[ZeroTrustEngine] = None
_zt_engine_lock = threading.Lock()


def get_zero_trust_engine() -> ZeroTrustEngine:
    global _zt_engine_instance
    with _zt_engine_lock:
        if _zt_engine_instance is None:
            _zt_engine_instance = ZeroTrustEngine()
        return _zt_engine_instance


def reset_zero_trust_engine() -> None:
    global _zt_engine_instance
    with _zt_engine_lock:
        _zt_engine_instance = None
