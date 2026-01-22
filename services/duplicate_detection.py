"""
Duplicate Detection Service for Dynamic.IQ-COREx
Enterprise-grade duplicate record prevention with multi-layer detection
"""
import re
import json
import hashlib
from datetime import datetime
from difflib import SequenceMatcher
from models import Database

RECORD_TYPE_CONFIG = {
    'customers': {
        'table': 'customers',
        'display_name': 'Customer',
        'key_fields': ['name', 'code', 'email'],
        'exact_fields': ['code', 'email'],
        'fuzzy_fields': ['name', 'address'],
        'display_field': 'name',
        'id_field': 'id'
    },
    'suppliers': {
        'table': 'suppliers',
        'display_name': 'Supplier',
        'key_fields': ['name', 'supplier_code', 'email'],
        'exact_fields': ['supplier_code', 'email'],
        'fuzzy_fields': ['name', 'address'],
        'display_field': 'name',
        'id_field': 'id'
    },
    'products': {
        'table': 'products',
        'display_name': 'Part/Item',
        'key_fields': ['part_number', 'name', 'manufacturer_part_number'],
        'exact_fields': ['part_number', 'manufacturer_part_number'],
        'fuzzy_fields': ['name', 'description'],
        'display_field': 'name',
        'id_field': 'id'
    },
    'work_orders': {
        'table': 'work_orders',
        'display_name': 'Work Order',
        'key_fields': ['work_order_number'],
        'exact_fields': ['work_order_number'],
        'fuzzy_fields': [],
        'display_field': 'work_order_number',
        'id_field': 'id'
    },
    'purchase_orders': {
        'table': 'purchase_orders',
        'display_name': 'Purchase Order',
        'key_fields': ['po_number'],
        'exact_fields': ['po_number'],
        'fuzzy_fields': [],
        'display_field': 'po_number',
        'id_field': 'id'
    },
    'sales_orders': {
        'table': 'sales_orders',
        'display_name': 'Sales Order',
        'key_fields': ['order_number'],
        'exact_fields': ['order_number'],
        'fuzzy_fields': [],
        'display_field': 'order_number',
        'id_field': 'id'
    },
    'assets': {
        'table': 'assets',
        'display_name': 'Asset/Equipment',
        'key_fields': ['asset_tag', 'serial_number', 'name'],
        'exact_fields': ['asset_tag', 'serial_number'],
        'fuzzy_fields': ['name', 'description'],
        'display_field': 'name',
        'id_field': 'id'
    },
    'labor_resources': {
        'table': 'labor_resources',
        'display_name': 'Employee',
        'key_fields': ['employee_code', 'email', 'name'],
        'exact_fields': ['employee_code', 'email'],
        'fuzzy_fields': ['name'],
        'display_field': 'name',
        'id_field': 'id'
    },
    'leads': {
        'table': 'leads',
        'display_name': 'Lead',
        'key_fields': ['company_name', 'email', 'phone'],
        'exact_fields': ['email', 'phone'],
        'fuzzy_fields': ['company_name', 'contact_name'],
        'display_field': 'company_name',
        'id_field': 'id'
    }
}


class DuplicateDetectionService:
    """Centralized service for duplicate record detection"""
    
    def __init__(self):
        self.db = Database()
    
    def normalize_text(self, text):
        """Normalize text for comparison - case insensitive, trimmed, special chars removed"""
        if not text:
            return ''
        text = str(text).lower().strip()
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text
    
    def get_hash(self, text):
        """Generate hash for text"""
        normalized = self.normalize_text(text)
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def levenshtein_distance(self, s1, s2):
        """Calculate Levenshtein distance between two strings"""
        if len(s1) < len(s2):
            return self.levenshtein_distance(s2, s1)
        if len(s2) == 0:
            return len(s1)
        
        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        return previous_row[-1]
    
    def similarity_score(self, s1, s2):
        """Calculate similarity score between 0 and 1"""
        if not s1 or not s2:
            return 0.0
        s1_norm = self.normalize_text(s1)
        s2_norm = self.normalize_text(s2)
        
        if s1_norm == s2_norm:
            return 1.0
        
        return SequenceMatcher(None, s1_norm, s2_norm).ratio()
    
    def token_similarity(self, s1, s2):
        """Calculate token-based similarity for multi-word strings"""
        if not s1 or not s2:
            return 0.0
        
        tokens1 = set(self.normalize_text(s1).split())
        tokens2 = set(self.normalize_text(s2).split())
        
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = tokens1.intersection(tokens2)
        union = tokens1.union(tokens2)
        
        return len(intersection) / len(union) if union else 0.0
    
    def get_config(self, record_type):
        """Get duplicate detection configuration for a record type"""
        conn = self.db.get_connection()
        
        config = conn.execute('''
            SELECT * FROM duplicate_detection_config WHERE record_type = ?
        ''', (record_type,)).fetchone()
        
        conn.close()
        
        if config:
            return {
                'is_enabled': bool(config['is_enabled']),
                'detection_mode': config['detection_mode'],
                'similarity_threshold': config['similarity_threshold'],
                'key_fields': json.loads(config['key_fields']) if config['key_fields'] else None,
                'match_weights': json.loads(config['match_weights']) if config['match_weights'] else None,
                'allow_override': bool(config['allow_override']),
                'override_roles': config['override_roles'].split(',') if config['override_roles'] else ['Admin']
            }
        
        return {
            'is_enabled': True,
            'detection_mode': 'soft',
            'similarity_threshold': 0.85,
            'key_fields': None,
            'match_weights': None,
            'allow_override': True,
            'override_roles': ['Admin', 'Manager']
        }
    
    def check_exact_match(self, record_type, field_values, exclude_id=None):
        """Check for exact matches on key fields"""
        if record_type not in RECORD_TYPE_CONFIG:
            return []
        
        type_config = RECORD_TYPE_CONFIG[record_type]
        conn = self.db.get_connection()
        matches = []
        
        for field in type_config.get('exact_fields', []):
            value = field_values.get(field)
            if not value:
                continue
            
            query = f'''
                SELECT * FROM {type_config["table"]} 
                WHERE {field} = ?
            '''
            params = [value]
            
            if exclude_id:
                query += f' AND {type_config["id_field"]} != ?'
                params.append(exclude_id)
            
            results = conn.execute(query, params).fetchall()
            
            for row in results:
                matches.append({
                    'id': row[type_config['id_field']],
                    'display_value': row.get(type_config['display_field'], f"ID: {row[type_config['id_field']]}"),
                    'match_type': 'exact',
                    'match_field': field,
                    'similarity_score': 1.0,
                    'record_data': dict(row)
                })
        
        conn.close()
        return matches
    
    def check_normalized_match(self, record_type, field_values, exclude_id=None):
        """Check for normalized matches (case-insensitive, trimmed)"""
        if record_type not in RECORD_TYPE_CONFIG:
            return []
        
        type_config = RECORD_TYPE_CONFIG[record_type]
        conn = self.db.get_connection()
        matches = []
        
        for field in type_config.get('key_fields', []):
            value = field_values.get(field)
            if not value:
                continue
            
            normalized_value = self.normalize_text(value)
            
            query = f'SELECT * FROM {type_config["table"]}'
            if exclude_id:
                query += f' WHERE {type_config["id_field"]} != ?'
                results = conn.execute(query, (exclude_id,)).fetchall()
            else:
                results = conn.execute(query).fetchall()
            
            for row in results:
                existing_value = row.get(field, '')
                if self.normalize_text(existing_value) == normalized_value:
                    if not any(m['id'] == row[type_config['id_field']] and m['match_field'] == field for m in matches):
                        matches.append({
                            'id': row[type_config['id_field']],
                            'display_value': row.get(type_config['display_field'], f"ID: {row[type_config['id_field']]}"),
                            'match_type': 'normalized',
                            'match_field': field,
                            'similarity_score': 1.0,
                            'record_data': dict(row)
                        })
        
        conn.close()
        return matches
    
    def check_fuzzy_match(self, record_type, field_values, threshold=0.85, exclude_id=None):
        """Check for fuzzy matches using similarity scoring"""
        if record_type not in RECORD_TYPE_CONFIG:
            return []
        
        type_config = RECORD_TYPE_CONFIG[record_type]
        conn = self.db.get_connection()
        matches = []
        
        fuzzy_fields = type_config.get('fuzzy_fields', [])
        if not fuzzy_fields:
            conn.close()
            return []
        
        query = f'SELECT * FROM {type_config["table"]}'
        if exclude_id:
            query += f' WHERE {type_config["id_field"]} != ?'
            results = conn.execute(query, (exclude_id,)).fetchall()
        else:
            results = conn.execute(query).fetchall()
        
        for row in results:
            best_score = 0
            best_field = None
            
            for field in fuzzy_fields:
                new_value = field_values.get(field)
                existing_value = row.get(field)
                
                if not new_value or not existing_value:
                    continue
                
                seq_score = self.similarity_score(new_value, existing_value)
                token_score = self.token_similarity(new_value, existing_value)
                combined_score = (seq_score * 0.6) + (token_score * 0.4)
                
                if combined_score > best_score:
                    best_score = combined_score
                    best_field = field
            
            if best_score >= threshold:
                matches.append({
                    'id': row[type_config['id_field']],
                    'display_value': row.get(type_config['display_field'], f"ID: {row[type_config['id_field']]}"),
                    'match_type': 'fuzzy',
                    'match_field': best_field,
                    'similarity_score': round(best_score, 3),
                    'record_data': dict(row)
                })
        
        conn.close()
        return matches
    
    def detect_duplicates(self, record_type, field_values, exclude_id=None):
        """
        Main duplicate detection method - runs all detection layers
        Returns: {
            'has_duplicates': bool,
            'is_exact_match': bool,
            'duplicates': list,
            'highest_score': float,
            'can_override': bool,
            'detection_mode': str
        }
        """
        config = self.get_config(record_type)
        
        if not config['is_enabled']:
            return {
                'has_duplicates': False,
                'is_exact_match': False,
                'duplicates': [],
                'highest_score': 0,
                'can_override': True,
                'detection_mode': 'disabled'
            }
        
        all_matches = []
        is_exact = False
        
        exact_matches = self.check_exact_match(record_type, field_values, exclude_id)
        if exact_matches:
            is_exact = True
            all_matches.extend(exact_matches)
        
        normalized_matches = self.check_normalized_match(record_type, field_values, exclude_id)
        for match in normalized_matches:
            if not any(m['id'] == match['id'] for m in all_matches):
                all_matches.append(match)
        
        threshold = config.get('similarity_threshold', 0.85)
        fuzzy_matches = self.check_fuzzy_match(record_type, field_values, threshold, exclude_id)
        for match in fuzzy_matches:
            if not any(m['id'] == match['id'] for m in all_matches):
                all_matches.append(match)
        
        highest_score = max([m['similarity_score'] for m in all_matches], default=0)
        
        can_override = config['allow_override'] and not is_exact
        if is_exact and config['detection_mode'] == 'hard':
            can_override = False
        
        return {
            'has_duplicates': len(all_matches) > 0,
            'is_exact_match': is_exact,
            'duplicates': all_matches,
            'highest_score': highest_score,
            'can_override': can_override,
            'detection_mode': config['detection_mode'],
            'override_roles': config['override_roles']
        }
    
    def log_detection_event(self, record_type, action_type, source_data, duplicates, 
                           user_decision, justification=None, user_id=None, ip_address=None):
        """Log duplicate detection event for audit trail"""
        conn = self.db.get_connection()
        
        highest_score = max([d['similarity_score'] for d in duplicates], default=0) if duplicates else 0
        
        conn.execute('''
            INSERT INTO duplicate_detection_log 
            (record_type, action_type, source_record_data, detected_duplicates, 
             highest_similarity_score, match_details, user_decision, justification,
             override_approved, performed_by, ip_address)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            record_type,
            action_type,
            json.dumps(source_data) if source_data else None,
            json.dumps([{'id': d['id'], 'score': d['similarity_score'], 'type': d['match_type']} for d in duplicates]) if duplicates else None,
            highest_score,
            json.dumps([{'field': d['match_field'], 'type': d['match_type']} for d in duplicates]) if duplicates else None,
            user_decision,
            justification,
            1 if user_decision == 'override' else 0,
            user_id,
            ip_address
        ))
        
        conn.commit()
        conn.close()
    
    def save_config(self, record_type, config_data, user_id=None):
        """Save duplicate detection configuration for a record type"""
        conn = self.db.get_connection()
        
        existing = conn.execute(
            'SELECT id FROM duplicate_detection_config WHERE record_type = ?', 
            (record_type,)
        ).fetchone()
        
        if existing:
            conn.execute('''
                UPDATE duplicate_detection_config SET
                    is_enabled = ?,
                    detection_mode = ?,
                    similarity_threshold = ?,
                    key_fields = ?,
                    match_weights = ?,
                    allow_override = ?,
                    override_roles = ?,
                    modified_by = ?,
                    modified_at = CURRENT_TIMESTAMP
                WHERE record_type = ?
            ''', (
                1 if config_data.get('is_enabled', True) else 0,
                config_data.get('detection_mode', 'soft'),
                config_data.get('similarity_threshold', 0.85),
                json.dumps(config_data.get('key_fields')) if config_data.get('key_fields') else None,
                json.dumps(config_data.get('match_weights')) if config_data.get('match_weights') else None,
                1 if config_data.get('allow_override', True) else 0,
                ','.join(config_data.get('override_roles', ['Admin'])),
                user_id,
                record_type
            ))
        else:
            conn.execute('''
                INSERT INTO duplicate_detection_config 
                (record_type, is_enabled, detection_mode, similarity_threshold, 
                 key_fields, match_weights, allow_override, override_roles, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                record_type,
                1 if config_data.get('is_enabled', True) else 0,
                config_data.get('detection_mode', 'soft'),
                config_data.get('similarity_threshold', 0.85),
                json.dumps(config_data.get('key_fields')) if config_data.get('key_fields') else None,
                json.dumps(config_data.get('match_weights')) if config_data.get('match_weights') else None,
                1 if config_data.get('allow_override', True) else 0,
                ','.join(config_data.get('override_roles', ['Admin'])),
                user_id
            ))
        
        conn.commit()
        conn.close()
    
    def get_all_configs(self):
        """Get all duplicate detection configurations"""
        conn = self.db.get_connection()
        
        configs = conn.execute('SELECT * FROM duplicate_detection_config ORDER BY record_type').fetchall()
        conn.close()
        
        result = {}
        for config in configs:
            result[config['record_type']] = {
                'id': config['id'],
                'is_enabled': bool(config['is_enabled']),
                'detection_mode': config['detection_mode'],
                'similarity_threshold': config['similarity_threshold'],
                'key_fields': json.loads(config['key_fields']) if config['key_fields'] else None,
                'allow_override': bool(config['allow_override']),
                'override_roles': config['override_roles'].split(',') if config['override_roles'] else ['Admin']
            }
        
        for record_type in RECORD_TYPE_CONFIG:
            if record_type not in result:
                result[record_type] = {
                    'id': None,
                    'is_enabled': True,
                    'detection_mode': 'soft',
                    'similarity_threshold': 0.85,
                    'key_fields': RECORD_TYPE_CONFIG[record_type].get('key_fields'),
                    'allow_override': True,
                    'override_roles': ['Admin', 'Manager']
                }
        
        return result
    
    def get_audit_logs(self, record_type=None, limit=100):
        """Get duplicate detection audit logs"""
        conn = self.db.get_connection()
        
        query = '''
            SELECT ddl.*, u.username as performed_by_name
            FROM duplicate_detection_log ddl
            LEFT JOIN users u ON ddl.performed_by = u.id
        '''
        params = []
        
        if record_type:
            query += ' WHERE ddl.record_type = ?'
            params.append(record_type)
        
        query += ' ORDER BY ddl.performed_at DESC LIMIT ?'
        params.append(limit)
        
        logs = conn.execute(query, params).fetchall()
        conn.close()
        
        return [dict(log) for log in logs]


def get_duplicate_service():
    """Factory function to get DuplicateDetectionService instance"""
    return DuplicateDetectionService()
