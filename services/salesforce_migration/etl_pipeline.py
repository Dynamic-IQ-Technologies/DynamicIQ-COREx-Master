"""
Salesforce ETL Pipeline
Handles data extraction, transformation, and loading with dependency ordering
"""
import json
from datetime import datetime
from models import Database


class ETLPipeline:
    """Manages ETL operations for Salesforce data migration"""
    
    BATCH_SIZE = 2000
    
    def __init__(self, sf_client, connection_id, migration_id):
        self.sf_client = sf_client
        self.connection_id = connection_id
        self.migration_id = migration_id
    
    def build_soql_query(self, object_name, fields):
        """Build SOQL query for object extraction"""
        field_names = [f['field_name'] for f in fields if f['sf_data_type'] not in ['address', 'location']]
        
        if 'Id' not in field_names:
            field_names.insert(0, 'Id')
        
        return f"SELECT {', '.join(field_names)} FROM {object_name}"
    
    def extract_object_data(self, object_metadata_id, migration_object_id):
        """Extract all data from a Salesforce object"""
        db = Database()
        conn = db.get_connection()
        
        try:
            obj = conn.execute('''
                SELECT om.*, GROUP_CONCAT(fm.field_name) as field_list
                FROM sf_object_metadata om
                LEFT JOIN sf_field_metadata fm ON fm.object_metadata_id = om.id
                WHERE om.id = ?
                GROUP BY om.id
            ''', (object_metadata_id,)).fetchone()
            
            if not obj:
                return {'success': False, 'error': 'Object not found'}
            
            fields = conn.execute('''
                SELECT * FROM sf_field_metadata WHERE object_metadata_id = ?
            ''', (object_metadata_id,)).fetchall()
            
            soql = self.build_soql_query(obj['object_name'], [dict(f) for f in fields])
            
            conn.execute('''
                UPDATE sf_migration_objects
                SET status = 'Extracting', start_time = datetime('now')
                WHERE id = ?
            ''', (migration_object_id,))
            conn.commit()
            
            result = self.sf_client.query(soql)
            
            if not result['success']:
                conn.execute('''
                    UPDATE sf_migration_objects
                    SET status = 'Error', error_message = ?
                    WHERE id = ?
                ''', (result.get('error'), migration_object_id))
                conn.commit()
                return result
            
            conn.execute('''
                UPDATE sf_migration_objects
                SET source_count = ?, status = 'Extracted'
                WHERE id = ?
            ''', (len(result['records']), migration_object_id))
            conn.commit()
            
            return {
                'success': True,
                'object_name': obj['object_name'],
                'record_count': len(result['records']),
                'records': result['records'],
                'fields': [dict(f) for f in fields]
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}
        finally:
            conn.close()
    
    def transform_record(self, record, field_mappings):
        """Transform Salesforce record to ERP format"""
        transformed = {
            'sf_id': record.get('Id'),
            'sf_created_date': record.get('CreatedDate'),
            'sf_last_modified_date': record.get('LastModifiedDate'),
            'sf_owner_id': record.get('OwnerId')
        }
        
        for field in field_mappings:
            sf_name = field['field_name']
            erp_name = field['erp_column_name']
            
            if sf_name in ['Id', 'CreatedDate', 'LastModifiedDate', 'OwnerId']:
                continue
            
            value = record.get(sf_name)
            
            if value is not None and field.get('transformation_rule'):
                value = self.apply_transformation(value, field['transformation_rule'])
            
            if field['sf_data_type'] == 'boolean':
                value = 1 if value else 0
            elif field['sf_data_type'] in ['datetime', 'date'] and value:
                pass
            elif field['sf_data_type'] == 'reference' and isinstance(value, dict):
                value = value.get('Id')
            
            transformed[erp_name] = value
        
        return transformed
    
    def apply_transformation(self, value, rule):
        """Apply transformation rule to field value"""
        try:
            rule_config = json.loads(rule)
            rule_type = rule_config.get('type')
            
            if rule_type == 'picklist_map':
                mapping = rule_config.get('mapping', {})
                return mapping.get(value, value)
            elif rule_type == 'uppercase':
                return value.upper() if value else value
            elif rule_type == 'lowercase':
                return value.lower() if value else value
            elif rule_type == 'prefix':
                return f"{rule_config.get('value', '')}{value}"
            
            return value
        except:
            return value
    
    def load_records(self, table_name, records, field_mappings, migration_object_id, batch_number):
        """Load transformed records into ERP table"""
        db = Database()
        conn = db.get_connection()
        
        try:
            conn.execute('''
                INSERT INTO sf_migration_batches
                (migration_object_id, batch_number, batch_size, status, start_time)
                VALUES (?, ?, ?, 'Loading', datetime('now'))
            ''', (migration_object_id, batch_number, len(records)))
            batch_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
            conn.commit()
            
            success_count = 0
            error_count = 0
            
            for record in records:
                try:
                    transformed = self.transform_record(record, field_mappings)
                    transformed['migration_id'] = self.migration_id
                    
                    columns = list(transformed.keys())
                    placeholders = ', '.join(['?' for _ in columns])
                    values = [transformed.get(c) for c in columns]
                    
                    sql = f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                    conn.execute(sql, values)
                    success_count += 1
                except Exception as e:
                    error_count += 1
                    conn.execute('''
                        INSERT INTO sf_migration_errors
                        (migration_id, migration_object_id, batch_id, sf_record_id, 
                         error_type, error_message, record_data, created_at)
                        VALUES (?, ?, ?, ?, 'LOAD_ERROR', ?, ?, datetime('now'))
                    ''', (
                        self.migration_id,
                        migration_object_id,
                        batch_id,
                        record.get('Id'),
                        str(e),
                        json.dumps(record)[:1000]
                    ))
            
            conn.execute('''
                UPDATE sf_migration_batches
                SET status = 'Complete', records_processed = ?, 
                    records_success = ?, records_failed = ?, end_time = datetime('now')
                WHERE id = ?
            ''', (len(records), success_count, error_count, batch_id))
            
            conn.commit()
            
            return {
                'success': True,
                'batch_id': batch_id,
                'processed': len(records),
                'success_count': success_count,
                'error_count': error_count
            }
        except Exception as e:
            conn.rollback()
            return {'success': False, 'error': str(e)}
        finally:
            conn.close()
    
    def migrate_object(self, object_metadata_id, migration_object_id):
        """Full migration pipeline for a single object"""
        extract_result = self.extract_object_data(object_metadata_id, migration_object_id)
        
        if not extract_result['success']:
            return extract_result
        
        db = Database()
        conn = db.get_connection()
        obj = conn.execute('SELECT * FROM sf_object_metadata WHERE id = ?', (object_metadata_id,)).fetchone()
        conn.close()
        
        if not obj or not obj['erp_table_name']:
            return {'success': False, 'error': 'ERP table not created'}
        
        records = extract_result['records']
        fields = extract_result['fields']
        table_name = obj['erp_table_name']
        
        total_success = 0
        total_errors = 0
        batch_number = 1
        
        for i in range(0, len(records), self.BATCH_SIZE):
            batch = records[i:i + self.BATCH_SIZE]
            result = self.load_records(table_name, batch, fields, migration_object_id, batch_number)
            
            if result['success']:
                total_success += result['success_count']
                total_errors += result['error_count']
            
            batch_number += 1
        
        db = Database()
        conn = db.get_connection()
        conn.execute('''
            UPDATE sf_migration_objects
            SET status = 'Complete', target_count = ?, inserted_count = ?, 
                error_count = ?, end_time = datetime('now')
            WHERE id = ?
        ''', (total_success, total_success, total_errors, migration_object_id))
        conn.commit()
        conn.close()
        
        return {
            'success': True,
            'object_name': extract_result['object_name'],
            'extracted': len(records),
            'loaded': total_success,
            'errors': total_errors
        }
    
    def run_full_migration(self):
        """Run complete migration for all objects in dependency order"""
        db = Database()
        conn = db.get_connection()
        
        try:
            migration_objects = conn.execute('''
                SELECT mo.*, om.object_name, om.erp_table_name
                FROM sf_migration_objects mo
                JOIN sf_object_metadata om ON mo.object_metadata_id = om.id
                WHERE mo.migration_id = ? AND mo.status = 'Pending'
                ORDER BY om.migration_priority
            ''', (self.migration_id,)).fetchall()
            
            conn.execute('''
                UPDATE sf_migrations
                SET status = 'Running', start_time = datetime('now')
                WHERE id = ?
            ''', (self.migration_id,))
            conn.commit()
            
            results = []
            
            for mig_obj in migration_objects:
                result = self.migrate_object(mig_obj['object_metadata_id'], mig_obj['id'])
                results.append({
                    'object_name': mig_obj['object_name'],
                    'result': result
                })
            
            conn.execute('''
                UPDATE sf_migrations
                SET status = 'Complete', end_time = datetime('now'),
                    completed_objects = (SELECT COUNT(*) FROM sf_migration_objects WHERE migration_id = ? AND status = 'Complete')
                WHERE id = ?
            ''', (self.migration_id, self.migration_id))
            conn.commit()
            
            return {
                'success': True,
                'objects_processed': len(results),
                'results': results
            }
        except Exception as e:
            conn.execute('''
                UPDATE sf_migrations SET status = 'Error' WHERE id = ?
            ''', (self.migration_id,))
            conn.commit()
            return {'success': False, 'error': str(e)}
        finally:
            conn.close()
