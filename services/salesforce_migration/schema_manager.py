"""
Dynamic ERP Schema Manager
Auto-creates tables from Salesforce object schemas
"""
import json
from datetime import datetime
from models import Database


class SchemaManager:
    """Manages dynamic ERP table creation from Salesforce schemas"""
    
    RESERVED_COLUMNS = ['id', 'created_at', 'updated_at', 'sf_id', 'sf_last_modified']
    
    def __init__(self, connection_id):
        self.connection_id = connection_id
    
    def generate_table_name(self, sf_object_name):
        """Generate ERP table name from Salesforce object name"""
        name = sf_object_name.replace('__c', '').replace('__r', '')
        
        result = ['sf_']
        for i, char in enumerate(name):
            if char.isupper() and i > 0:
                result.append('_')
            result.append(char.lower())
        
        return ''.join(result)
    
    def generate_column_definition(self, field):
        """Generate SQLite column definition from field metadata"""
        col_type = field['erp_column_type'] or field['field_type'] or 'TEXT'
        
        definition = f"{field['erp_column_name']} {col_type}"
        
        if field['is_unique']:
            definition += " UNIQUE"
        
        return definition
    
    def build_create_table_sql(self, object_metadata, fields):
        """Build CREATE TABLE SQL statement"""
        table_name = object_metadata['erp_table_name'] or self.generate_table_name(object_metadata['object_name'])
        
        columns = [
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "sf_id TEXT UNIQUE NOT NULL",
            "sf_created_date TIMESTAMP",
            "sf_last_modified_date TIMESTAMP",
            "sf_owner_id TEXT",
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "migration_id INTEGER"
        ]
        
        for field in fields:
            if field['field_name'].lower() in ['id', 'createddate', 'lastmodifieddate', 'ownerid']:
                continue
            
            col_name = field['erp_column_name']
            if col_name in self.RESERVED_COLUMNS:
                col_name = f"sf_{col_name}"
            
            col_def = f"{col_name} {field['field_type'] or 'TEXT'}"
            columns.append(col_def)
        
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n    " + ",\n    ".join(columns) + "\n)"
        
        return {
            'table_name': table_name,
            'sql': create_sql,
            'column_count': len(columns)
        }
    
    def preview_schema(self, object_id):
        """Preview schema creation without executing"""
        db = Database()
        conn = db.get_connection()
        
        try:
            obj = conn.execute('''
                SELECT * FROM sf_object_metadata WHERE id = ?
            ''', (object_id,)).fetchone()
            
            if not obj:
                return {'success': False, 'error': 'Object not found'}
            
            fields = conn.execute('''
                SELECT * FROM sf_field_metadata WHERE object_metadata_id = ?
            ''', (object_id,)).fetchall()
            
            schema_info = self.build_create_table_sql(dict(obj), [dict(f) for f in fields])
            
            return {
                'success': True,
                'object_name': obj['object_name'],
                'table_name': schema_info['table_name'],
                'sql': schema_info['sql'],
                'column_count': schema_info['column_count']
            }
        finally:
            conn.close()
    
    def create_table(self, object_id, approved_by=None):
        """Create ERP table for Salesforce object"""
        db = Database()
        conn = db.get_connection()
        
        try:
            obj = conn.execute('''
                SELECT * FROM sf_object_metadata WHERE id = ?
            ''', (object_id,)).fetchone()
            
            if not obj:
                return {'success': False, 'error': 'Object not found'}
            
            fields = conn.execute('''
                SELECT * FROM sf_field_metadata WHERE object_metadata_id = ?
            ''', (object_id,)).fetchall()
            
            schema_info = self.build_create_table_sql(dict(obj), [dict(f) for f in fields])
            
            conn.execute(schema_info['sql'])
            
            conn.execute('''
                UPDATE sf_object_metadata
                SET erp_table_name = ?, erp_table_exists = 1
                WHERE id = ?
            ''', (schema_info['table_name'], object_id))
            
            conn.execute('''
                INSERT INTO sf_audit_events
                (connection_id, event_type, event_category, event_description, object_name, user_id, created_at)
                VALUES (?, 'SCHEMA_CREATED', 'Schema', ?, ?, ?, datetime('now'))
            ''', (
                self.connection_id,
                f"Created table {schema_info['table_name']} with {schema_info['column_count']} columns",
                obj['object_name'],
                approved_by
            ))
            
            conn.commit()
            
            return {
                'success': True,
                'table_name': schema_info['table_name'],
                'column_count': schema_info['column_count']
            }
        except Exception as e:
            conn.rollback()
            return {'success': False, 'error': str(e)}
        finally:
            conn.close()
    
    def check_table_exists(self, table_name):
        """Check if ERP table already exists"""
        db = Database()
        conn = db.get_connection()
        
        try:
            result = conn.execute('''
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            ''', (table_name,)).fetchone()
            
            return result is not None
        finally:
            conn.close()
    
    def get_table_schema(self, table_name):
        """Get existing table schema"""
        db = Database()
        conn = db.get_connection()
        
        try:
            columns = conn.execute(f'PRAGMA table_info({table_name})').fetchall()
            
            return {
                'success': True,
                'table_name': table_name,
                'columns': [
                    {
                        'name': col[1],
                        'type': col[2],
                        'not_null': col[3],
                        'pk': col[5]
                    }
                    for col in columns
                ]
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}
        finally:
            conn.close()
    
    def create_all_pending_schemas(self, approved_by=None):
        """Create tables for all discovered objects without ERP tables"""
        db = Database()
        conn = db.get_connection()
        
        try:
            pending = conn.execute('''
                SELECT id, object_name FROM sf_object_metadata
                WHERE connection_id = ? AND erp_table_exists = 0
                ORDER BY migration_priority
            ''', (self.connection_id,)).fetchall()
            
            created = []
            errors = []
            
            for obj in pending:
                result = self.create_table(obj['id'], approved_by)
                if result['success']:
                    created.append({
                        'object_name': obj['object_name'],
                        'table_name': result['table_name']
                    })
                else:
                    errors.append({
                        'object_name': obj['object_name'],
                        'error': result['error']
                    })
            
            return {
                'success': True,
                'created_count': len(created),
                'error_count': len(errors),
                'created': created,
                'errors': errors
            }
        finally:
            conn.close()
