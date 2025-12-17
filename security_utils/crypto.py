"""
Cryptographic Security Layer

Patent-Eligible Technical Implementation:
This module implements cryptographic mechanisms for data integrity verification,
role-scoped access control, and tamper-evident audit trails at the data structure
and event propagation levels.

Technical Improvements:
- Hash-chained exchange events for integrity verification
- Role-scoped cryptographic access keys
- Tamper-evident audit trails with verification capability
- Reduced audit overhead through efficient verification
"""

import hashlib
import hmac
import json
import secrets
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum

from models import Database


class AccessLevel(Enum):
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"
    AUDIT = "audit"


@dataclass
class AccessKey:
    """
    Role-scoped cryptographic access key.
    
    Technical Specification:
    - HMAC-based key generation
    - Scoped to specific chains and roles
    - Time-limited with expiration
    """
    key_id: str
    key_hash: str
    chain_id: str
    role: str
    access_level: AccessLevel
    created_at: datetime
    expires_at: datetime
    is_active: bool


@dataclass
class AuditEntry:
    """
    Tamper-evident audit trail entry.
    
    Technical Specification:
    - Hash-linked to previous entry
    - Contains cryptographic signature
    - Verifiable independently
    """
    entry_id: str
    chain_id: str
    action_type: str
    actor_id: int
    target_entity: str
    target_id: int
    payload_hash: str
    prev_hash: str
    entry_hash: str
    signature: str
    created_at: datetime


class CryptoSecurityManager:
    """
    Cryptographic security management system.
    
    Technical Implementation:
    - Hash chain integrity verification
    - Role-based cryptographic access control
    - Tamper-evident audit logging
    - Efficient verification algorithms
    """
    
    def __init__(self, secret_key: Optional[str] = None):
        self.db = Database()
        self._secret_key = secret_key or secrets.token_hex(32)
        self._access_keys: Dict[str, AccessKey] = {}
        self._lock = threading.RLock()
        self._metrics = {
            'keys_generated': 0,
            'keys_verified': 0,
            'keys_revoked': 0,
            'audit_entries': 0,
            'verifications_passed': 0,
            'verifications_failed': 0
        }
        self._ensure_tables_exist()
    
    def _ensure_tables_exist(self):
        """Create security tables."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS access_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key_id TEXT UNIQUE NOT NULL,
                key_hash TEXT NOT NULL,
                chain_id TEXT NOT NULL,
                role TEXT NOT NULL,
                access_level TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                is_active INTEGER DEFAULT 1,
                created_by INTEGER
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS secure_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_id TEXT UNIQUE NOT NULL,
                chain_id TEXT NOT NULL,
                action_type TEXT NOT NULL,
                actor_id INTEGER NOT NULL,
                target_entity TEXT NOT NULL,
                target_id INTEGER NOT NULL,
                payload_hash TEXT NOT NULL,
                prev_hash TEXT,
                entry_hash TEXT NOT NULL,
                signature TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS integrity_verifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chain_id TEXT NOT NULL,
                verification_type TEXT NOT NULL,
                entries_checked INTEGER NOT NULL,
                entries_valid INTEGER NOT NULL,
                entries_invalid INTEGER NOT NULL,
                verified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                verified_by INTEGER
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_keys_chain ON access_keys(chain_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_keys_active ON access_keys(is_active)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_audit_chain ON secure_audit_log(chain_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_audit_entry ON secure_audit_log(entry_id)')
        
        conn.commit()
        conn.close()
    
    def generate_access_key(
        self,
        chain_id: str,
        role: str,
        access_level: AccessLevel,
        validity_hours: int = 24,
        created_by: Optional[int] = None
    ) -> Tuple[str, AccessKey]:
        """
        Generate role-scoped cryptographic access key.
        
        Technical Specification:
        - HMAC-SHA256 key generation
        - Scoped to specific chain and role
        - Time-limited validity
        
        Returns:
            Tuple of (raw_key, AccessKey object)
        """
        import uuid
        
        key_id = f"KEY-{uuid.uuid4().hex[:12].upper()}"
        raw_key = secrets.token_urlsafe(32)
        
        key_hash = hmac.new(
            self._secret_key.encode(),
            f"{key_id}:{raw_key}:{chain_id}:{role}".encode(),
            hashlib.sha256
        ).hexdigest()
        
        access_key = AccessKey(
            key_id=key_id,
            key_hash=key_hash,
            chain_id=chain_id,
            role=role,
            access_level=access_level,
            created_at=datetime.now(),
            expires_at=datetime.now() + timedelta(hours=validity_hours),
            is_active=True
        )
        
        conn = self.db.get_connection()
        conn.execute('''
            INSERT INTO access_keys 
            (key_id, key_hash, chain_id, role, access_level, expires_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            key_id, key_hash, chain_id, role, access_level.value,
            access_key.expires_at.isoformat(), created_by
        ))
        conn.commit()
        conn.close()
        
        with self._lock:
            self._access_keys[key_id] = access_key
            self._metrics['keys_generated'] += 1
        
        return raw_key, access_key
    
    def verify_access_key(
        self,
        key_id: str,
        raw_key: str,
        chain_id: str,
        required_level: AccessLevel
    ) -> Tuple[bool, Optional[str]]:
        """
        Verify access key and check permissions.
        
        Technical Specification:
        - Validates HMAC signature
        - Checks expiration
        - Verifies access level
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        conn = self.db.get_connection()
        key_row = conn.execute(
            'SELECT * FROM access_keys WHERE key_id = ? AND is_active = 1',
            (key_id,)
        ).fetchone()
        conn.close()
        
        if not key_row:
            self._metrics['verifications_failed'] += 1
            return False, "Key not found or inactive"
        
        role = key_row['role']
        expected_hash = hmac.new(
            self._secret_key.encode(),
            f"{key_id}:{raw_key}:{chain_id}:{role}".encode(),
            hashlib.sha256
        ).hexdigest()
        
        if not hmac.compare_digest(expected_hash, key_row['key_hash']):
            self._metrics['verifications_failed'] += 1
            return False, "Invalid key signature"
        
        expires_at = datetime.fromisoformat(key_row['expires_at'])
        if datetime.now() > expires_at:
            self._metrics['verifications_failed'] += 1
            return False, "Key expired"
        
        if key_row['chain_id'] != chain_id:
            self._metrics['verifications_failed'] += 1
            return False, "Key not authorized for this chain"
        
        access_levels = {
            AccessLevel.READ.value: 1,
            AccessLevel.WRITE.value: 2,
            AccessLevel.ADMIN.value: 3,
            AccessLevel.AUDIT.value: 4
        }
        
        key_level = access_levels.get(key_row['access_level'], 0)
        required = access_levels.get(required_level.value, 0)
        
        if key_level < required:
            self._metrics['verifications_failed'] += 1
            return False, "Insufficient access level"
        
        self._metrics['keys_verified'] += 1
        self._metrics['verifications_passed'] += 1
        return True, None
    
    def revoke_access_key(self, key_id: str) -> bool:
        """Revoke an access key."""
        conn = self.db.get_connection()
        cursor = conn.execute(
            'UPDATE access_keys SET is_active = 0 WHERE key_id = ?',
            (key_id,)
        )
        conn.commit()
        conn.close()
        
        with self._lock:
            if key_id in self._access_keys:
                del self._access_keys[key_id]
            self._metrics['keys_revoked'] += 1
        
        return cursor.rowcount > 0
    
    def create_audit_entry(
        self,
        chain_id: str,
        action_type: str,
        actor_id: int,
        target_entity: str,
        target_id: int,
        payload: Dict[str, Any]
    ) -> AuditEntry:
        """
        Create tamper-evident audit entry.
        
        Technical Specification:
        - Hash-linked to previous entry
        - Includes cryptographic signature
        - Persisted for verification
        """
        import uuid
        
        entry_id = f"AUD-{uuid.uuid4().hex[:12].upper()}"
        
        payload_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=str).encode()
        ).hexdigest()
        
        conn = self.db.get_connection()
        prev_entry = conn.execute('''
            SELECT entry_hash FROM secure_audit_log 
            WHERE chain_id = ? 
            ORDER BY id DESC LIMIT 1
        ''', (chain_id,)).fetchone()
        
        prev_hash = prev_entry['entry_hash'] if prev_entry else 'genesis'
        
        entry_data = json.dumps({
            'entry_id': entry_id,
            'chain_id': chain_id,
            'action_type': action_type,
            'actor_id': actor_id,
            'target_entity': target_entity,
            'target_id': target_id,
            'payload_hash': payload_hash,
            'prev_hash': prev_hash
        }, sort_keys=True)
        
        entry_hash = hashlib.sha256(entry_data.encode()).hexdigest()
        
        signature = hmac.new(
            self._secret_key.encode(),
            entry_hash.encode(),
            hashlib.sha256
        ).hexdigest()
        
        audit_entry = AuditEntry(
            entry_id=entry_id,
            chain_id=chain_id,
            action_type=action_type,
            actor_id=actor_id,
            target_entity=target_entity,
            target_id=target_id,
            payload_hash=payload_hash,
            prev_hash=prev_hash,
            entry_hash=entry_hash,
            signature=signature,
            created_at=datetime.now()
        )
        
        conn.execute('''
            INSERT INTO secure_audit_log 
            (entry_id, chain_id, action_type, actor_id, target_entity, target_id,
             payload_hash, prev_hash, entry_hash, signature)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            entry_id, chain_id, action_type, actor_id, target_entity, target_id,
            payload_hash, prev_hash, entry_hash, signature
        ))
        conn.commit()
        conn.close()
        
        self._metrics['audit_entries'] += 1
        
        return audit_entry
    
    def verify_audit_chain(
        self,
        chain_id: str,
        verified_by: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Verify integrity of entire audit chain.
        
        Technical Specification:
        - Validates hash chain linkage
        - Verifies cryptographic signatures
        - Reports any tampering detected
        """
        conn = self.db.get_connection()
        entries = conn.execute('''
            SELECT * FROM secure_audit_log 
            WHERE chain_id = ?
            ORDER BY id ASC
        ''', (chain_id,)).fetchall()
        
        valid_entries = []
        invalid_entries = []
        prev_hash = 'genesis'
        
        for entry in entries:
            entry_data = json.dumps({
                'entry_id': entry['entry_id'],
                'chain_id': entry['chain_id'],
                'action_type': entry['action_type'],
                'actor_id': entry['actor_id'],
                'target_entity': entry['target_entity'],
                'target_id': entry['target_id'],
                'payload_hash': entry['payload_hash'],
                'prev_hash': entry['prev_hash']
            }, sort_keys=True)
            
            expected_hash = hashlib.sha256(entry_data.encode()).hexdigest()
            
            expected_signature = hmac.new(
                self._secret_key.encode(),
                expected_hash.encode(),
                hashlib.sha256
            ).hexdigest()
            
            is_valid = (
                entry['entry_hash'] == expected_hash and
                entry['prev_hash'] == prev_hash and
                hmac.compare_digest(entry['signature'], expected_signature)
            )
            
            if is_valid:
                valid_entries.append(entry['entry_id'])
            else:
                invalid_entries.append({
                    'entry_id': entry['entry_id'],
                    'hash_valid': entry['entry_hash'] == expected_hash,
                    'chain_valid': entry['prev_hash'] == prev_hash,
                    'signature_valid': hmac.compare_digest(entry['signature'], expected_signature)
                })
            
            prev_hash = entry['entry_hash']
        
        conn.execute('''
            INSERT INTO integrity_verifications 
            (chain_id, verification_type, entries_checked, entries_valid, entries_invalid, verified_by)
            VALUES (?, 'full_chain', ?, ?, ?, ?)
        ''', (chain_id, len(entries), len(valid_entries), len(invalid_entries), verified_by))
        conn.commit()
        conn.close()
        
        return {
            'chain_id': chain_id,
            'total_entries': len(entries),
            'valid_entries': len(valid_entries),
            'invalid_entries': len(invalid_entries),
            'integrity_verified': len(invalid_entries) == 0,
            'tampered_entries': invalid_entries,
            'verified_at': datetime.now().isoformat()
        }
    
    def get_chain_access_keys(self, chain_id: str) -> List[Dict[str, Any]]:
        """Get all active access keys for a chain."""
        conn = self.db.get_connection()
        rows = conn.execute('''
            SELECT key_id, role, access_level, created_at, expires_at
            FROM access_keys 
            WHERE chain_id = ? AND is_active = 1
            ORDER BY created_at DESC
        ''', (chain_id,)).fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    def get_audit_trail(
        self,
        chain_id: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get audit trail for a chain."""
        conn = self.db.get_connection()
        rows = conn.execute('''
            SELECT entry_id, action_type, actor_id, target_entity, target_id, created_at
            FROM secure_audit_log 
            WHERE chain_id = ?
            ORDER BY id DESC LIMIT ?
        ''', (chain_id, limit)).fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get security metrics.
        
        Technical Specification:
        - Returns key management statistics
        - Includes verification results
        - Provides audit trail counts
        """
        return {
            **self._metrics,
            'active_keys': len(self._access_keys),
            'verification_success_rate': (
                self._metrics['verifications_passed'] / 
                max(1, self._metrics['verifications_passed'] + self._metrics['verifications_failed'])
            )
        }


_security_instance: Optional[CryptoSecurityManager] = None
_security_lock = threading.Lock()


def get_security_manager(secret_key: Optional[str] = None) -> CryptoSecurityManager:
    """Singleton accessor for Crypto Security Manager."""
    global _security_instance
    with _security_lock:
        if _security_instance is None:
            _security_instance = CryptoSecurityManager(secret_key)
        return _security_instance


def reset_security_manager() -> None:
    """Reset global security instance (for testing)."""
    global _security_instance
    with _security_lock:
        _security_instance = None
