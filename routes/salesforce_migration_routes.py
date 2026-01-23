"""
Salesforce Data Migration Agent Routes
Handles connection management, discovery, migration, and audit
"""
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database
from auth import login_required, role_required
from datetime import datetime
import json
import os

sf_migration_bp = Blueprint('sf_migration_routes', __name__)

SF_STATUSES = ['Draft', 'Pending Approval', 'Ready', 'Running', 'Complete', 'Error', 'Cancelled']
MIGRATION_TYPES = ['Full', 'Incremental', 'Selective']


@sf_migration_bp.route('/salesforce-migration')
@login_required
@role_required('Admin')
def dashboard():
    """Salesforce Migration Agent Dashboard"""
    db = Database()
    conn = db.get_connection()
    
    connections = conn.execute('''
        SELECT c.*, 
               (SELECT COUNT(*) FROM sf_object_metadata WHERE connection_id = c.id) as object_count,
               (SELECT COUNT(*) FROM sf_migrations WHERE connection_id = c.id) as migration_count
        FROM sf_connections c
        ORDER BY c.created_at DESC
    ''').fetchall()
    
    migrations = conn.execute('''
        SELECT m.*, c.connection_name,
               u.username as created_by_name
        FROM sf_migrations m
        JOIN sf_connections c ON m.connection_id = c.id
        LEFT JOIN users u ON m.created_by = u.id
        ORDER BY m.created_at DESC
        LIMIT 10
    ''').fetchall()
    
    stats = {
        'total_connections': len(connections),
        'active_connections': len([c for c in connections if c['status'] == 'Connected']),
        'total_migrations': conn.execute('SELECT COUNT(*) as cnt FROM sf_migrations').fetchone()['cnt'],
        'total_objects': conn.execute('SELECT COUNT(*) as cnt FROM sf_object_metadata').fetchone()['cnt'],
        'total_errors': conn.execute("SELECT COUNT(*) as cnt FROM sf_migration_errors WHERE resolution_status = 'Open'").fetchone()['cnt']
    }
    
    conn.close()
    
    return render_template('salesforce_migration/dashboard.html',
        connections=connections,
        migrations=migrations,
        stats=stats
    )


@sf_migration_bp.route('/salesforce-migration/connections')
@login_required
@role_required('Admin')
def list_connections():
    """List all Salesforce connections"""
    db = Database()
    conn = db.get_connection()
    
    connections = conn.execute('''
        SELECT c.*, 
               (SELECT COUNT(*) FROM sf_object_metadata WHERE connection_id = c.id) as object_count
        FROM sf_connections c
        ORDER BY c.created_at DESC
    ''').fetchall()
    
    conn.close()
    
    return render_template('salesforce_migration/connections.html',
        connections=connections
    )


@sf_migration_bp.route('/salesforce-migration/connections/new', methods=['GET', 'POST'])
@login_required
@role_required('Admin')
def new_connection():
    """Create new Salesforce connection"""
    if request.method == 'POST':
        db = Database()
        conn = db.get_connection()
        
        try:
            connection_name = request.form['connection_name']
            instance_url = request.form['instance_url'].rstrip('/')
            client_id = request.form['client_id']
            client_secret = request.form.get('client_secret', '')
            api_version = request.form.get('api_version', 'v59.0')
            sandbox = 1 if request.form.get('sandbox') else 0
            
            cursor = conn.execute('''
                INSERT INTO sf_connections 
                (connection_name, instance_url, client_id, client_secret_encrypted, 
                 api_version, sandbox, status, created_by, created_at)
                VALUES (?, ?, ?, ?, ?, ?, 'Disconnected', ?, datetime('now'))
            ''', (
                connection_name, instance_url, client_id, client_secret,
                api_version, sandbox, session['user_id']
            ))
            
            connection_id = cursor.lastrowid
            
            conn.execute('''
                INSERT INTO sf_audit_events
                (connection_id, event_type, event_category, event_description, user_id, created_at)
                VALUES (?, 'CONNECTION_CREATED', 'Connection', 'New Salesforce connection created', ?, datetime('now'))
            ''', (connection_id, session['user_id']))
            
            conn.commit()
            flash(f'Connection "{connection_name}" created successfully!', 'success')
            return redirect(url_for('sf_migration_routes.view_connection', id=connection_id))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error creating connection: {str(e)}', 'danger')
        finally:
            conn.close()
    
    return render_template('salesforce_migration/connection_form.html', connection=None)


@sf_migration_bp.route('/salesforce-migration/connections/<int:id>')
@login_required
@role_required('Admin')
def view_connection(id):
    """View Salesforce connection details"""
    db = Database()
    conn = db.get_connection()
    
    connection = conn.execute('SELECT * FROM sf_connections WHERE id = ?', (id,)).fetchone()
    
    if not connection:
        flash('Connection not found', 'danger')
        conn.close()
        return redirect(url_for('sf_migration_routes.list_connections'))
    
    objects = conn.execute('''
        SELECT om.*, 
               (SELECT COUNT(*) FROM sf_field_metadata WHERE object_metadata_id = om.id) as field_count
        FROM sf_object_metadata om
        WHERE om.connection_id = ?
        ORDER BY om.migration_priority, om.object_name
    ''', (id,)).fetchall()
    
    migrations = conn.execute('''
        SELECT * FROM sf_migrations
        WHERE connection_id = ?
        ORDER BY created_at DESC
        LIMIT 5
    ''', (id,)).fetchall()
    
    audit_events = conn.execute('''
        SELECT ae.*, u.username
        FROM sf_audit_events ae
        LEFT JOIN users u ON ae.user_id = u.id
        WHERE ae.connection_id = ?
        ORDER BY ae.created_at DESC
        LIMIT 20
    ''', (id,)).fetchall()
    
    conn.close()
    
    return render_template('salesforce_migration/connection_view.html',
        connection=connection,
        objects=objects,
        migrations=migrations,
        audit_events=audit_events
    )


@sf_migration_bp.route('/salesforce-migration/connections/<int:id>/discover', methods=['POST'])
@login_required
@role_required('Admin')
def run_discovery(id):
    """Run object discovery for a connection"""
    db = Database()
    conn = db.get_connection()
    
    try:
        connection = conn.execute('SELECT * FROM sf_connections WHERE id = ?', (id,)).fetchone()
        
        if not connection:
            return jsonify({'success': False, 'error': 'Connection not found'}), 404
        
        include_custom = request.form.get('include_custom', 'true') == 'true'
        
        conn.execute('''
            INSERT INTO sf_audit_events
            (connection_id, event_type, event_category, event_description, user_id, created_at)
            VALUES (?, 'DISCOVERY_STARTED', 'Discovery', 'Object discovery initiated', ?, datetime('now'))
        ''', (id, session['user_id']))
        conn.commit()
        
        flash('Discovery process initiated. This may take a few minutes for large orgs.', 'info')
        flash('Note: Full discovery requires valid Salesforce OAuth credentials configured.', 'warning')
        
    except Exception as e:
        flash(f'Error starting discovery: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('sf_migration_routes.view_connection', id=id))


@sf_migration_bp.route('/salesforce-migration/connections/<int:id>/test', methods=['POST'])
@login_required
@role_required('Admin')
def test_connection(id):
    """Test Salesforce connection"""
    db = Database()
    conn = db.get_connection()
    
    try:
        connection = conn.execute('SELECT * FROM sf_connections WHERE id = ?', (id,)).fetchone()
        
        if not connection:
            return jsonify({'success': False, 'error': 'Connection not found'}), 404
        
        flash('Connection test requires valid OAuth tokens. Please complete OAuth flow first.', 'info')
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        conn.close()
    
    return redirect(url_for('sf_migration_routes.view_connection', id=id))


@sf_migration_bp.route('/salesforce-migration/migrations')
@login_required
@role_required('Admin')
def list_migrations():
    """List all migrations"""
    db = Database()
    conn = db.get_connection()
    
    migrations = conn.execute('''
        SELECT m.*, c.connection_name, u.username as created_by_name,
               (SELECT COUNT(*) FROM sf_migration_objects WHERE migration_id = m.id) as object_count
        FROM sf_migrations m
        JOIN sf_connections c ON m.connection_id = c.id
        LEFT JOIN users u ON m.created_by = u.id
        ORDER BY m.created_at DESC
    ''').fetchall()
    
    conn.close()
    
    return render_template('salesforce_migration/migrations.html',
        migrations=migrations,
        sf_statuses=SF_STATUSES
    )


@sf_migration_bp.route('/salesforce-migration/migrations/new', methods=['GET', 'POST'])
@login_required
@role_required('Admin')
def new_migration():
    """Create new migration"""
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            migration_name = request.form['migration_name']
            connection_id = int(request.form['connection_id'])
            migration_type = request.form.get('migration_type', 'Full')
            selected_objects = request.form.getlist('objects')
            
            cursor = conn.execute('''
                INSERT INTO sf_migrations
                (migration_name, connection_id, migration_type, status, 
                 total_objects, created_by, created_at)
                VALUES (?, ?, ?, 'Draft', ?, ?, datetime('now'))
            ''', (
                migration_name, connection_id, migration_type,
                len(selected_objects) if selected_objects else 0,
                session['user_id']
            ))
            
            migration_id = cursor.lastrowid
            
            if selected_objects:
                for i, obj_id in enumerate(selected_objects):
                    conn.execute('''
                        INSERT INTO sf_migration_objects
                        (migration_id, object_metadata_id, sequence_order, status)
                        VALUES (?, ?, ?, 'Pending')
                    ''', (migration_id, int(obj_id), i))
            
            conn.execute('''
                INSERT INTO sf_audit_events
                (migration_id, connection_id, event_type, event_category, event_description, user_id, created_at)
                VALUES (?, ?, 'MIGRATION_CREATED', 'Migration', 'Migration definition created', ?, datetime('now'))
            ''', (migration_id, connection_id, session['user_id']))
            
            conn.commit()
            flash(f'Migration "{migration_name}" created successfully!', 'success')
            return redirect(url_for('sf_migration_routes.view_migration', id=migration_id))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error creating migration: {str(e)}', 'danger')
    
    connections = conn.execute('''
        SELECT c.*, 
               (SELECT COUNT(*) FROM sf_object_metadata WHERE connection_id = c.id) as object_count
        FROM sf_connections c
        WHERE (SELECT COUNT(*) FROM sf_object_metadata WHERE connection_id = c.id) > 0
        ORDER BY c.connection_name
    ''').fetchall()
    
    conn.close()
    
    return render_template('salesforce_migration/migration_form.html',
        migration=None,
        connections=connections,
        migration_types=MIGRATION_TYPES
    )


@sf_migration_bp.route('/salesforce-migration/migrations/<int:id>')
@login_required
@role_required('Admin')
def view_migration(id):
    """View migration details"""
    db = Database()
    conn = db.get_connection()
    
    migration = conn.execute('''
        SELECT m.*, c.connection_name, u.username as created_by_name
        FROM sf_migrations m
        JOIN sf_connections c ON m.connection_id = c.id
        LEFT JOIN users u ON m.created_by = u.id
        WHERE m.id = ?
    ''', (id,)).fetchone()
    
    if not migration:
        flash('Migration not found', 'danger')
        conn.close()
        return redirect(url_for('sf_migration_routes.list_migrations'))
    
    migration_objects = conn.execute('''
        SELECT mo.*, om.object_name, om.object_label, om.record_count as source_estimate,
               om.erp_table_name, om.erp_table_exists
        FROM sf_migration_objects mo
        JOIN sf_object_metadata om ON mo.object_metadata_id = om.id
        WHERE mo.migration_id = ?
        ORDER BY mo.sequence_order
    ''', (id,)).fetchall()
    
    errors = conn.execute('''
        SELECT me.*, om.object_name
        FROM sf_migration_errors me
        LEFT JOIN sf_migration_objects mo ON me.migration_object_id = mo.id
        LEFT JOIN sf_object_metadata om ON mo.object_metadata_id = om.id
        WHERE me.migration_id = ?
        ORDER BY me.created_at DESC
        LIMIT 50
    ''', (id,)).fetchall()
    
    reconciliation = conn.execute('''
        SELECT * FROM sf_reconciliation_results
        WHERE migration_id = ?
        ORDER BY object_name
    ''', (id,)).fetchall()
    
    conn.close()
    
    return render_template('salesforce_migration/migration_view.html',
        migration=migration,
        migration_objects=migration_objects,
        errors=errors,
        reconciliation=reconciliation,
        sf_statuses=SF_STATUSES
    )


@sf_migration_bp.route('/salesforce-migration/migrations/<int:id>/approve', methods=['POST'])
@login_required
@role_required('Admin')
def approve_migration(id):
    """Approve migration for execution"""
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('''
            UPDATE sf_migrations
            SET status = 'Ready', approved_by = ?, approved_at = datetime('now')
            WHERE id = ? AND status = 'Pending Approval'
        ''', (session['user_id'], id))
        
        conn.execute('''
            INSERT INTO sf_audit_events
            (migration_id, event_type, event_category, event_description, user_id, created_at)
            VALUES (?, 'MIGRATION_APPROVED', 'Migration', 'Migration approved for execution', ?, datetime('now'))
        ''', (id, session['user_id']))
        
        conn.commit()
        flash('Migration approved and ready for execution', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error approving migration: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('sf_migration_routes.view_migration', id=id))


@sf_migration_bp.route('/salesforce-migration/objects/<int:id>')
@login_required
@role_required('Admin')
def view_object(id):
    """View object metadata and field mappings"""
    db = Database()
    conn = db.get_connection()
    
    obj = conn.execute('''
        SELECT om.*, c.connection_name
        FROM sf_object_metadata om
        JOIN sf_connections c ON om.connection_id = c.id
        WHERE om.id = ?
    ''', (id,)).fetchone()
    
    if not obj:
        flash('Object not found', 'danger')
        conn.close()
        return redirect(url_for('sf_migration_routes.dashboard'))
    
    fields = conn.execute('''
        SELECT * FROM sf_field_metadata
        WHERE object_metadata_id = ?
        ORDER BY field_name
    ''', (id,)).fetchall()
    
    conn.close()
    
    return render_template('salesforce_migration/object_view.html',
        object=obj,
        fields=fields
    )


@sf_migration_bp.route('/salesforce-migration/objects/<int:id>/preview-schema')
@login_required
@role_required('Admin')
def preview_schema(id):
    """Preview ERP table schema for an object"""
    from services.salesforce_migration.schema_manager import SchemaManager
    
    db = Database()
    conn = db.get_connection()
    
    obj = conn.execute('SELECT connection_id FROM sf_object_metadata WHERE id = ?', (id,)).fetchone()
    
    if not obj:
        conn.close()
        return jsonify({'success': False, 'error': 'Object not found'}), 404
    
    conn.close()
    
    schema_manager = SchemaManager(obj['connection_id'])
    result = schema_manager.preview_schema(id)
    
    return jsonify(result)


@sf_migration_bp.route('/salesforce-migration/objects/<int:id>/create-schema', methods=['POST'])
@login_required
@role_required('Admin')
def create_schema(id):
    """Create ERP table for an object"""
    from services.salesforce_migration.schema_manager import SchemaManager
    
    db = Database()
    conn = db.get_connection()
    
    obj = conn.execute('SELECT connection_id FROM sf_object_metadata WHERE id = ?', (id,)).fetchone()
    
    if not obj:
        conn.close()
        flash('Object not found', 'danger')
        return redirect(url_for('sf_migration_routes.dashboard'))
    
    conn.close()
    
    schema_manager = SchemaManager(obj['connection_id'])
    result = schema_manager.create_table(id, session['user_id'])
    
    if result['success']:
        flash(f"Table '{result['table_name']}' created with {result['column_count']} columns", 'success')
    else:
        flash(f"Error creating table: {result['error']}", 'danger')
    
    return redirect(url_for('sf_migration_routes.view_object', id=id))


@sf_migration_bp.route('/salesforce-migration/audit')
@login_required
@role_required('Admin')
def audit_log():
    """View audit trail"""
    db = Database()
    conn = db.get_connection()
    
    events = conn.execute('''
        SELECT ae.*, u.username, c.connection_name, m.migration_name
        FROM sf_audit_events ae
        LEFT JOIN users u ON ae.user_id = u.id
        LEFT JOIN sf_connections c ON ae.connection_id = c.id
        LEFT JOIN sf_migrations m ON ae.migration_id = m.id
        ORDER BY ae.created_at DESC
        LIMIT 200
    ''').fetchall()
    
    conn.close()
    
    return render_template('salesforce_migration/audit_log.html',
        events=events
    )


@sf_migration_bp.route('/salesforce-migration/api/connection/<int:id>/objects')
@login_required
@role_required('Admin')
def api_get_objects(id):
    """API to get objects for a connection"""
    db = Database()
    conn = db.get_connection()
    
    objects = conn.execute('''
        SELECT id, object_name, object_label, record_count, erp_table_exists
        FROM sf_object_metadata
        WHERE connection_id = ?
        ORDER BY migration_priority, object_name
    ''', (id,)).fetchall()
    
    conn.close()
    
    return jsonify([dict(o) for o in objects])
