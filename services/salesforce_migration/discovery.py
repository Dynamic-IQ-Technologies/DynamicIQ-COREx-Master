"""
Salesforce Object Discovery Service
Auto-discovers all Salesforce objects, fields, and relationships
"""
import json
from datetime import datetime
from models import Database

STANDARD_OBJECTS = [
    'Account', 'Contact', 'Lead', 'Opportunity', 'Case', 'Task', 'Event',
    'Campaign', 'CampaignMember', 'Product2', 'Pricebook2', 'PricebookEntry',
    'Order', 'OrderItem', 'Asset', 'Contract', 'Quote', 'QuoteLineItem',
    'User', 'Profile', 'UserRole', 'Group', 'Note', 'Attachment',
    'ContentDocument', 'ContentVersion', 'Document', 'Folder'
]

DEPENDENCY_ORDER = {
    'User': 0,
    'Profile': 0,
    'UserRole': 0,
    'Account': 10,
    'Contact': 20,
    'Lead': 20,
    'Product2': 10,
    'Pricebook2': 11,
    'PricebookEntry': 12,
    'Opportunity': 30,
    'OpportunityLineItem': 31,
    'Campaign': 20,
    'CampaignMember': 25,
    'Case': 40,
    'Order': 50,
    'OrderItem': 51,
    'Asset': 40,
    'Contract': 40,
    'Quote': 45,
    'QuoteLineItem': 46,
    'Task': 60,
    'Event': 60,
    'Note': 70,
    'Attachment': 70,
    'ContentDocument': 70,
    'ContentVersion': 71,
}


class DiscoveryService:
    """Discovers Salesforce objects and builds schema catalog"""
    
    def __init__(self, sf_client, connection_id):
        self.sf_client = sf_client
        self.connection_id = connection_id
    
    def discover_all_objects(self, include_custom=True):
        """Discover all accessible Salesforce objects"""
        result = self.sf_client.describe_global()
        
        if not result['success']:
            return result
        
        discovered = []
        for obj in result['objects']:
            if not obj.get('queryable', False):
                continue
            
            is_custom = obj.get('custom', False)
            
            if is_custom and not include_custom:
                continue
            
            discovered.append({
                'object_name': obj.get('name'),
                'object_label': obj.get('label'),
                'is_custom': is_custom,
                'is_queryable': obj.get('queryable', True),
                'key_prefix': obj.get('keyPrefix'),
                'object_type': 'Custom' if is_custom else 'Standard'
            })
        
        return {
            'success': True,
            'objects': discovered,
            'count': len(discovered)
        }
    
    def discover_object_fields(self, object_name):
        """Discover all fields for a specific object"""
        result = self.sf_client.describe_object(object_name)
        
        if not result['success']:
            return result
        
        metadata = result['metadata']
        fields = []
        
        for field in metadata.get('fields', []):
            field_info = {
                'field_name': field.get('name'),
                'field_label': field.get('label'),
                'field_type': self._map_sf_type(field.get('type')),
                'sf_data_type': field.get('type'),
                'length': field.get('length'),
                'precision_val': field.get('precision'),
                'scale': field.get('scale'),
                'is_required': not field.get('nillable', True) and field.get('createable', False),
                'is_unique': field.get('unique', False),
                'is_reference': field.get('type') == 'reference',
                'reference_to': ','.join(field.get('referenceTo', [])) if field.get('referenceTo') else None,
                'picklist_values': json.dumps([pv.get('value') for pv in field.get('picklistValues', [])]) if field.get('picklistValues') else None
            }
            fields.append(field_info)
        
        return {
            'success': True,
            'object_name': object_name,
            'fields': fields,
            'field_count': len(fields),
            'relationships': metadata.get('childRelationships', [])
        }
    
    def _map_sf_type(self, sf_type):
        """Map Salesforce data type to SQLite/ERP type"""
        type_mapping = {
            'id': 'TEXT',
            'string': 'TEXT',
            'textarea': 'TEXT',
            'phone': 'TEXT',
            'email': 'TEXT',
            'url': 'TEXT',
            'picklist': 'TEXT',
            'multipicklist': 'TEXT',
            'reference': 'TEXT',
            'boolean': 'INTEGER',
            'int': 'INTEGER',
            'double': 'REAL',
            'currency': 'REAL',
            'percent': 'REAL',
            'date': 'DATE',
            'datetime': 'TIMESTAMP',
            'time': 'TEXT',
            'base64': 'BLOB',
            'address': 'TEXT',
            'location': 'TEXT',
            'encryptedstring': 'TEXT'
        }
        return type_mapping.get(sf_type.lower() if sf_type else 'string', 'TEXT')
    
    def save_object_metadata(self, object_data):
        """Save discovered object metadata to database"""
        db = Database()
        conn = db.get_connection()
        
        try:
            record_count = self.sf_client.get_record_count(object_data['object_name'])
            
            priority = DEPENDENCY_ORDER.get(object_data['object_name'], 100)
            
            cursor = conn.execute('''
                INSERT OR REPLACE INTO sf_object_metadata 
                (connection_id, object_name, object_label, object_type, is_custom, 
                 is_queryable, record_count, key_prefix, migration_priority, discovered_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ''', (
                self.connection_id,
                object_data['object_name'],
                object_data['object_label'],
                object_data['object_type'],
                1 if object_data['is_custom'] else 0,
                1 if object_data['is_queryable'] else 0,
                record_count.get('count', 0) if record_count.get('success') else 0,
                object_data['key_prefix'],
                priority
            ))
            
            object_id = cursor.lastrowid
            conn.commit()
            
            return {'success': True, 'object_id': object_id}
        except Exception as e:
            conn.rollback()
            return {'success': False, 'error': str(e)}
        finally:
            conn.close()
    
    def save_field_metadata(self, object_id, fields):
        """Save discovered field metadata to database"""
        db = Database()
        conn = db.get_connection()
        
        try:
            for field in fields:
                erp_column = self._generate_erp_column_name(field['field_name'])
                
                conn.execute('''
                    INSERT OR REPLACE INTO sf_field_metadata
                    (object_metadata_id, field_name, field_label, field_type, sf_data_type,
                     length, precision_val, scale, is_required, is_unique, is_reference,
                     reference_to, picklist_values, erp_column_name, erp_column_type, mapping_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Auto')
                ''', (
                    object_id,
                    field['field_name'],
                    field['field_label'],
                    field['field_type'],
                    field['sf_data_type'],
                    field.get('length'),
                    field.get('precision_val'),
                    field.get('scale'),
                    1 if field.get('is_required') else 0,
                    1 if field.get('is_unique') else 0,
                    1 if field.get('is_reference') else 0,
                    field.get('reference_to'),
                    field.get('picklist_values'),
                    erp_column,
                    field['field_type']
                ))
            
            conn.commit()
            return {'success': True, 'field_count': len(fields)}
        except Exception as e:
            conn.rollback()
            return {'success': False, 'error': str(e)}
        finally:
            conn.close()
    
    def _generate_erp_column_name(self, sf_field_name):
        """Generate ERP-compatible column name from Salesforce field name"""
        name = sf_field_name.replace('__c', '').replace('__r', '')
        
        result = []
        for i, char in enumerate(name):
            if char.isupper() and i > 0:
                result.append('_')
            result.append(char.lower())
        
        return ''.join(result)
    
    def run_full_discovery(self, include_custom=True, log_audit=True):
        """Run complete discovery of all Salesforce objects and fields"""
        db = Database()
        conn = db.get_connection()
        
        objects_result = self.discover_all_objects(include_custom)
        
        if not objects_result['success']:
            return objects_result
        
        discovered_objects = []
        
        for obj in objects_result['objects']:
            obj_result = self.save_object_metadata(obj)
            
            if obj_result['success']:
                object_id = obj_result['object_id']
                
                fields_result = self.discover_object_fields(obj['object_name'])
                
                if fields_result['success']:
                    self.save_field_metadata(object_id, fields_result['fields'])
                    discovered_objects.append({
                        'object_name': obj['object_name'],
                        'object_id': object_id,
                        'field_count': fields_result['field_count']
                    })
        
        if log_audit:
            conn.execute('''
                INSERT INTO sf_audit_events
                (connection_id, event_type, event_category, event_description, record_count, created_at)
                VALUES (?, 'DISCOVERY_COMPLETE', 'Discovery', ?, ?, datetime('now'))
            ''', (
                self.connection_id,
                f'Discovered {len(discovered_objects)} objects',
                len(discovered_objects)
            ))
            conn.commit()
        
        conn.close()
        
        return {
            'success': True,
            'objects_discovered': len(discovered_objects),
            'objects': discovered_objects
        }
