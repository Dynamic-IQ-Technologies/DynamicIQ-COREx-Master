from flask import Blueprint, render_template, request, jsonify, make_response
from models import Database, AuditLogger
from auth import login_required, role_required
from datetime import datetime
import json
import csv
import io

audit_bp = Blueprint('audit_routes', __name__)

@audit_bp.route('/audit/<record_type>/<record_id>')
@login_required
@role_required('Admin', 'Planner', 'Accountant', 'Procurement')
def view_audit_trail(record_type, record_id):
    """View audit trail for a specific record"""
    db = Database()
    conn = db.get_connection()
    
    # Get audit trail entries
    audit_entries_raw = AuditLogger.get_audit_trail(conn, record_type, record_id, limit=500)
    
    # Convert Row objects to dictionaries and parse changed fields JSON
    audit_entries = []
    for entry in audit_entries_raw:
        entry_dict = dict(entry)
        if entry_dict['changed_fields']:
            try:
                entry_dict['changes_parsed'] = json.loads(entry_dict['changed_fields'])
            except:
                entry_dict['changes_parsed'] = None
        else:
            entry_dict['changes_parsed'] = None
        audit_entries.append(entry_dict)
    
    conn.close()
    
    return render_template('audit/audit_trail.html', 
                         audit_entries=audit_entries,
                         record_type=record_type,
                         record_id=record_id)

@audit_bp.route('/audit/<record_type>/<record_id>/export')
@login_required
@role_required('Admin', 'Planner', 'Accountant', 'Procurement')
def export_audit_trail(record_type, record_id):
    """Export audit trail to CSV"""
    db = Database()
    conn = db.get_connection()
    
    audit_entries = AuditLogger.get_audit_trail(conn, record_type, record_id, limit=1000)
    
    # Create CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow(['Action Type', 'Modified By', 'Date/Time', 'Changed Fields', 'IP Address'])
    
    # Write data
    for entry in audit_entries:
        changed_fields_str = ''
        if entry['changed_fields']:
            try:
                changes = json.loads(entry['changed_fields'])
                changed_fields_str = '; '.join([
                    f"{field}: {data['old']} → {data['new']}" 
                    for field, data in changes.items()
                ])
            except:
                changed_fields_str = entry['changed_fields']
        
        writer.writerow([
            entry['action_type'],
            entry['modified_by_name'],
            entry['modified_at'],
            changed_fields_str,
            entry['ip_address'] or ''
        ])
    
    # Create response
    output.seek(0)
    response = make_response(output.getvalue())
    response.headers['Content-Disposition'] = f'attachment; filename=audit_trail_{record_type}_{record_id}_{datetime.now().strftime("%Y%m%d")}.csv'
    response.headers['Content-Type'] = 'text/csv'
    
    conn.close()
    return response
