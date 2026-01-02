from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session
import sqlite3
from datetime import datetime, timedelta
import json
import os
from auth import login_required

qms_bp = Blueprint('qms', __name__, url_prefix='/qms')

def get_db():
    conn = sqlite3.connect('mrp.db')
    conn.row_factory = sqlite3.Row
    return conn

def generate_sop_number(conn):
    """Generate next SOP number"""
    result = conn.execute('''
        SELECT sop_number FROM qms_sops 
        WHERE sop_number LIKE 'SOP-%'
        ORDER BY CAST(SUBSTR(sop_number, 5) AS INTEGER) DESC 
        LIMIT 1
    ''').fetchone()
    
    if result:
        try:
            last_num = int(result['sop_number'].split('-')[1])
            return f'SOP-{last_num + 1:04d}'
        except:
            pass
    return 'SOP-0001'

def generate_wi_number(conn):
    """Generate next Work Instruction number"""
    result = conn.execute('''
        SELECT wi_number FROM qms_work_instructions 
        WHERE wi_number LIKE 'WI-%'
        ORDER BY CAST(SUBSTR(wi_number, 4) AS INTEGER) DESC 
        LIMIT 1
    ''').fetchone()
    
    if result:
        try:
            last_num = int(result['wi_number'].split('-')[1])
            return f'WI-{last_num + 1:04d}'
        except:
            pass
    return 'WI-0001'

def generate_deviation_number(conn):
    """Generate next Deviation number"""
    result = conn.execute('''
        SELECT deviation_number FROM qms_deviations 
        WHERE deviation_number LIKE 'DEV-%'
        ORDER BY CAST(SUBSTR(deviation_number, 5) AS INTEGER) DESC 
        LIMIT 1
    ''').fetchone()
    
    if result:
        try:
            last_num = int(result['deviation_number'].split('-')[1])
            return f'DEV-{last_num + 1:04d}'
        except:
            pass
    return 'DEV-0001'

def generate_capa_number(conn):
    """Generate next CAPA number"""
    result = conn.execute('''
        SELECT capa_number FROM qms_capa 
        WHERE capa_number LIKE 'CAPA-%'
        ORDER BY CAST(SUBSTR(capa_number, 6) AS INTEGER) DESC 
        LIMIT 1
    ''').fetchone()
    
    if result:
        try:
            last_num = int(result['capa_number'].split('-')[1])
            return f'CAPA-{last_num + 1:04d}'
        except:
            pass
    return 'CAPA-0001'

def log_qms_audit(conn, document_type, document_id, action, user_id, user_name, 
                  field_changed=None, old_value=None, new_value=None, notes=None):
    """Log QMS audit trail entry"""
    conn.execute('''
        INSERT INTO qms_audit_trail (document_type, document_id, action, field_changed,
            old_value, new_value, user_id, user_name, ip_address, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (document_type, document_id, action, field_changed, old_value, new_value,
          user_id, user_name, request.remote_addr, notes))

# ============== QMS Dashboard ==============

@qms_bp.route('/')
@login_required
def dashboard():
    """QMS Dashboard with compliance metrics and analytics"""
    conn = get_db()
    
    # Get KPIs
    sop_stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) as active,
            SUM(CASE WHEN status = 'Draft' THEN 1 ELSE 0 END) as draft,
            SUM(CASE WHEN approval_status = 'Pending' THEN 1 ELSE 0 END) as pending_approval,
            SUM(CASE WHEN review_date < date('now') AND status = 'Active' THEN 1 ELSE 0 END) as overdue_review
        FROM qms_sops
    ''').fetchone()
    
    wi_stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) as active,
            SUM(CASE WHEN status = 'Draft' THEN 1 ELSE 0 END) as draft
        FROM qms_work_instructions
    ''').fetchone()
    
    deviation_stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Open' THEN 1 ELSE 0 END) as open,
            SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END) as closed,
            SUM(CASE WHEN severity = 'Critical' AND status = 'Open' THEN 1 ELSE 0 END) as critical_open
        FROM qms_deviations
    ''').fetchone()
    
    capa_stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Open' THEN 1 ELSE 0 END) as open,
            SUM(CASE WHEN status = 'In Progress' THEN 1 ELSE 0 END) as in_progress,
            SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END) as closed,
            SUM(CASE WHEN target_date < date('now') AND status NOT IN ('Closed', 'Verified') THEN 1 ELSE 0 END) as overdue
        FROM qms_capa
    ''').fetchone()
    
    # Recent activity
    recent_sops = conn.execute('''
        SELECT s.*, u.username as prepared_by_name 
        FROM qms_sops s
        LEFT JOIN users u ON s.prepared_by = u.id
        ORDER BY s.created_at DESC LIMIT 5
    ''').fetchall()
    
    recent_deviations = conn.execute('''
        SELECT d.*, u.username as reported_by_name 
        FROM qms_deviations d
        LEFT JOIN users u ON d.reported_by = u.id
        ORDER BY d.created_at DESC LIMIT 5
    ''').fetchall()
    
    open_capa = conn.execute('''
        SELECT c.*, u.username as assigned_to_name 
        FROM qms_capa c
        LEFT JOIN users u ON c.assigned_to = u.id
        WHERE c.status NOT IN ('Closed', 'Verified')
        ORDER BY 
            CASE c.priority WHEN 'Critical' THEN 1 WHEN 'High' THEN 2 WHEN 'Medium' THEN 3 ELSE 4 END,
            c.target_date
        LIMIT 5
    ''').fetchall()
    
    # Categories for sidebar
    categories = conn.execute('''
        SELECT c.*, COUNT(s.id) as sop_count
        FROM qms_sop_categories c
        LEFT JOIN qms_sops s ON c.id = s.category_id
        WHERE c.status = 'Active'
        GROUP BY c.id
        ORDER BY c.sort_order, c.name
    ''').fetchall()
    
    conn.close()
    
    return render_template('qms/dashboard.html',
                          sop_stats=dict(sop_stats) if sop_stats else {},
                          wi_stats=dict(wi_stats) if wi_stats else {},
                          deviation_stats=dict(deviation_stats) if deviation_stats else {},
                          capa_stats=dict(capa_stats) if capa_stats else {},
                          recent_sops=[dict(r) for r in recent_sops],
                          recent_deviations=[dict(r) for r in recent_deviations],
                          open_capa=[dict(r) for r in open_capa],
                          categories=[dict(c) for c in categories])


# ============== SOP Management ==============

@qms_bp.route('/sops')
@login_required
def sop_list():
    """List all SOPs"""
    conn = get_db()
    
    status_filter = request.args.get('status', '')
    category_filter = request.args.get('category', '')
    search = request.args.get('search', '')
    
    query = '''
        SELECT s.*, c.name as category_name, u.username as prepared_by_name,
               a.username as approved_by_name
        FROM qms_sops s
        LEFT JOIN qms_sop_categories c ON s.category_id = c.id
        LEFT JOIN users u ON s.prepared_by = u.id
        LEFT JOIN users a ON s.approved_by = a.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND s.status = ?'
        params.append(status_filter)
    if category_filter:
        query += ' AND s.category_id = ?'
        params.append(category_filter)
    if search:
        query += ' AND (s.sop_number LIKE ? OR s.title LIKE ?)'
        params.extend([f'%{search}%', f'%{search}%'])
    
    query += ' ORDER BY s.sop_number'
    
    sops = conn.execute(query, params).fetchall()
    categories = conn.execute('SELECT * FROM qms_sop_categories WHERE status = ? ORDER BY name', ('Active',)).fetchall()
    
    conn.close()
    
    return render_template('qms/sop_list.html',
                          sops=[dict(s) for s in sops],
                          categories=[dict(c) for c in categories],
                          status_filter=status_filter,
                          category_filter=category_filter,
                          search=search)


@qms_bp.route('/sops/new', methods=['GET', 'POST'])
@login_required
def sop_create():
    """Create new SOP"""
    conn = get_db()
    
    if request.method == 'POST':
        sop_number = generate_sop_number(conn)
        
        conn.execute('''
            INSERT INTO qms_sops (sop_number, title, category_id, purpose, scope,
                responsibilities, procedure_content, references_text, definitions,
                applicable_roles, applicable_modules, compliance_standards,
                effective_date, review_date, prepared_by, status, approval_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Draft', 'Pending')
        ''', (
            sop_number,
            request.form.get('title'),
            request.form.get('category_id') or None,
            request.form.get('purpose'),
            request.form.get('scope'),
            request.form.get('responsibilities'),
            request.form.get('procedure_content'),
            request.form.get('references_text'),
            request.form.get('definitions'),
            request.form.get('applicable_roles'),
            request.form.get('applicable_modules'),
            request.form.get('compliance_standards'),
            request.form.get('effective_date') or None,
            request.form.get('review_date') or None,
            session.get('user_id')
        ))
        
        sop_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        
        log_qms_audit(conn, 'SOP', sop_id, 'Created', session.get('user_id'), 
                     session.get('username'), notes=f'Created SOP {sop_number}')
        
        conn.commit()
        conn.close()
        
        flash(f'SOP {sop_number} created successfully', 'success')
        return redirect(url_for('qms.sop_view', sop_id=sop_id))
    
    categories = conn.execute('SELECT * FROM qms_sop_categories WHERE status = ? ORDER BY name', ('Active',)).fetchall()
    users = conn.execute('SELECT id, username FROM users ORDER BY username').fetchall()
    
    conn.close()
    
    return render_template('qms/sop_form.html',
                          sop=None,
                          categories=[dict(c) for c in categories],
                          users=[dict(u) for u in users])


@qms_bp.route('/sops/<int:sop_id>')
@login_required
def sop_view(sop_id):
    """View SOP details"""
    conn = get_db()
    
    sop = conn.execute('''
        SELECT s.*, c.name as category_name, 
               u1.username as prepared_by_name,
               u2.username as reviewed_by_name,
               u3.username as approved_by_name
        FROM qms_sops s
        LEFT JOIN qms_sop_categories c ON s.category_id = c.id
        LEFT JOIN users u1 ON s.prepared_by = u1.id
        LEFT JOIN users u2 ON s.reviewed_by = u2.id
        LEFT JOIN users u3 ON s.approved_by = u3.id
        WHERE s.id = ?
    ''', (sop_id,)).fetchone()
    
    if not sop:
        flash('SOP not found', 'danger')
        conn.close()
        return redirect(url_for('qms.sop_list'))
    
    # Get version history
    versions = conn.execute('''
        SELECT v.*, u.username as created_by_name
        FROM qms_sop_versions v
        LEFT JOIN users u ON v.created_by = u.id
        WHERE v.sop_id = ?
        ORDER BY v.created_at DESC
    ''', (sop_id,)).fetchall()
    
    # Get related work instructions
    work_instructions = conn.execute('''
        SELECT * FROM qms_work_instructions WHERE sop_id = ? ORDER BY wi_number
    ''', (sop_id,)).fetchall()
    
    # Get acknowledgments
    acknowledgments = conn.execute('''
        SELECT a.*, u.username
        FROM qms_acknowledgments a
        JOIN users u ON a.user_id = u.id
        WHERE a.document_type = 'SOP' AND a.document_id = ?
        ORDER BY a.acknowledged_at DESC
    ''', (sop_id,)).fetchall()
    
    # Get audit trail
    audit_trail = conn.execute('''
        SELECT * FROM qms_audit_trail
        WHERE document_type = 'SOP' AND document_id = ?
        ORDER BY timestamp DESC LIMIT 20
    ''', (sop_id,)).fetchall()
    
    conn.close()
    
    return render_template('qms/sop_view.html',
                          sop=dict(sop),
                          versions=[dict(v) for v in versions],
                          work_instructions=[dict(w) for w in work_instructions],
                          acknowledgments=[dict(a) for a in acknowledgments],
                          audit_trail=[dict(a) for a in audit_trail])


@qms_bp.route('/sops/<int:sop_id>/edit', methods=['GET', 'POST'])
@login_required
def sop_edit(sop_id):
    """Edit SOP"""
    conn = get_db()
    
    sop = conn.execute('SELECT * FROM qms_sops WHERE id = ?', (sop_id,)).fetchone()
    if not sop:
        flash('SOP not found', 'danger')
        conn.close()
        return redirect(url_for('qms.sop_list'))
    
    if request.method == 'POST':
        # Save current version to history
        conn.execute('''
            INSERT INTO qms_sop_versions (sop_id, revision, revision_date, change_summary,
                procedure_content, created_by)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (sop_id, sop['revision'], datetime.now().strftime('%Y-%m-%d'),
              request.form.get('change_summary', 'Updated'), sop['procedure_content'],
              session.get('user_id')))
        
        # Increment revision
        current_rev = sop['revision'] or 'A'
        if current_rev.isalpha():
            new_rev = chr(ord(current_rev) + 1) if current_rev != 'Z' else 'AA'
        else:
            new_rev = chr(ord('A') + int(current_rev))
        
        conn.execute('''
            UPDATE qms_sops SET
                title = ?, category_id = ?, revision = ?, revision_date = date('now'),
                purpose = ?, scope = ?, responsibilities = ?, procedure_content = ?,
                references_text = ?, definitions = ?, applicable_roles = ?,
                applicable_modules = ?, compliance_standards = ?,
                effective_date = ?, review_date = ?, updated_at = datetime('now')
            WHERE id = ?
        ''', (
            request.form.get('title'),
            request.form.get('category_id') or None,
            new_rev,
            request.form.get('purpose'),
            request.form.get('scope'),
            request.form.get('responsibilities'),
            request.form.get('procedure_content'),
            request.form.get('references_text'),
            request.form.get('definitions'),
            request.form.get('applicable_roles'),
            request.form.get('applicable_modules'),
            request.form.get('compliance_standards'),
            request.form.get('effective_date') or None,
            request.form.get('review_date') or None,
            sop_id
        ))
        
        log_qms_audit(conn, 'SOP', sop_id, 'Updated', session.get('user_id'),
                     session.get('username'), notes=f'Revision {current_rev} -> {new_rev}')
        
        conn.commit()
        conn.close()
        
        flash('SOP updated successfully', 'success')
        return redirect(url_for('qms.sop_view', sop_id=sop_id))
    
    categories = conn.execute('SELECT * FROM qms_sop_categories WHERE status = ? ORDER BY name', ('Active',)).fetchall()
    users = conn.execute('SELECT id, username FROM users ORDER BY username').fetchall()
    
    conn.close()
    
    return render_template('qms/sop_form.html',
                          sop=dict(sop),
                          categories=[dict(c) for c in categories],
                          users=[dict(u) for u in users])


@qms_bp.route('/sops/<int:sop_id>/approve', methods=['POST'])
@login_required
def sop_approve(sop_id):
    """Approve SOP"""
    user_role = session.get('role', '')
    if user_role not in ['admin', 'manager', 'quality']:
        flash('You do not have permission to approve SOPs', 'danger')
        return redirect(url_for('qms.sop_view', sop_id=sop_id))
    
    conn = get_db()
    
    conn.execute('''
        UPDATE qms_sops SET
            status = 'Active',
            approval_status = 'Approved',
            approved_by = ?,
            approved_date = datetime('now')
        WHERE id = ?
    ''', (session.get('user_id'), sop_id))
    
    log_qms_audit(conn, 'SOP', sop_id, 'Approved', session.get('user_id'),
                 session.get('username'))
    
    conn.commit()
    conn.close()
    
    flash('SOP approved and activated', 'success')
    return redirect(url_for('qms.sop_view', sop_id=sop_id))


@qms_bp.route('/sops/<int:sop_id>/acknowledge', methods=['POST'])
@login_required
def sop_acknowledge(sop_id):
    """Acknowledge SOP"""
    conn = get_db()
    
    sop = conn.execute('SELECT sop_number, revision FROM qms_sops WHERE id = ?', (sop_id,)).fetchone()
    
    # Check if already acknowledged this revision
    existing = conn.execute('''
        SELECT id FROM qms_acknowledgments 
        WHERE user_id = ? AND document_type = 'SOP' AND document_id = ? AND document_revision = ?
    ''', (session.get('user_id'), sop_id, sop['revision'])).fetchone()
    
    if existing:
        flash('You have already acknowledged this SOP revision', 'info')
    else:
        conn.execute('''
            INSERT INTO qms_acknowledgments (user_id, document_type, document_id, 
                document_revision, ip_address)
            VALUES (?, 'SOP', ?, ?, ?)
        ''', (session.get('user_id'), sop_id, sop['revision'], request.remote_addr))
        
        log_qms_audit(conn, 'SOP', sop_id, 'Acknowledged', session.get('user_id'),
                     session.get('username'), notes=f'Revision {sop["revision"]}')
        
        conn.commit()
        flash('SOP acknowledged successfully', 'success')
    
    conn.close()
    return redirect(url_for('qms.sop_view', sop_id=sop_id))


# ============== Work Instructions ==============

@qms_bp.route('/work-instructions')
@login_required
def wi_list():
    """List all Work Instructions"""
    conn = get_db()
    
    status_filter = request.args.get('status', '')
    module_filter = request.args.get('module', '')
    search = request.args.get('search', '')
    
    query = '''
        SELECT wi.*, s.sop_number, s.title as sop_title, u.username as prepared_by_name
        FROM qms_work_instructions wi
        LEFT JOIN qms_sops s ON wi.sop_id = s.id
        LEFT JOIN users u ON wi.prepared_by = u.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND wi.status = ?'
        params.append(status_filter)
    if module_filter:
        query += ' AND wi.erp_module = ?'
        params.append(module_filter)
    if search:
        query += ' AND (wi.wi_number LIKE ? OR wi.title LIKE ?)'
        params.extend([f'%{search}%', f'%{search}%'])
    
    query += ' ORDER BY wi.wi_number'
    
    work_instructions = conn.execute(query, params).fetchall()
    
    # Get unique modules for filter
    modules = conn.execute('SELECT DISTINCT erp_module FROM qms_work_instructions WHERE erp_module IS NOT NULL').fetchall()
    
    # Get SOPs for auto-generate modal
    sops = conn.execute('SELECT id, sop_number, title FROM qms_sops WHERE status = ? ORDER BY sop_number', ('Active',)).fetchall()
    
    conn.close()
    
    return render_template('qms/wi_list.html',
                          work_instructions=[dict(w) for w in work_instructions],
                          modules=[m['erp_module'] for m in modules],
                          status_filter=status_filter,
                          module_filter=module_filter,
                          search=search,
                          transaction_capabilities=ERP_TRANSACTION_CAPABILITIES,
                          sops=[dict(s) for s in sops])


@qms_bp.route('/work-instructions/new', methods=['GET', 'POST'])
@login_required
def wi_create():
    """Create new Work Instruction"""
    conn = get_db()
    
    if request.method == 'POST':
        wi_number = generate_wi_number(conn)
        
        conn.execute('''
            INSERT INTO qms_work_instructions (wi_number, title, sop_id, description,
                prerequisites, safety_requirements, tools_required, materials_required,
                erp_module, erp_transaction, applicable_roles, estimated_time_minutes,
                difficulty_level, effective_date, prepared_by, status, approval_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Draft', 'Pending')
        ''', (
            wi_number,
            request.form.get('title'),
            request.form.get('sop_id') or None,
            request.form.get('description'),
            request.form.get('prerequisites'),
            request.form.get('safety_requirements'),
            request.form.get('tools_required'),
            request.form.get('materials_required'),
            request.form.get('erp_module'),
            request.form.get('erp_transaction'),
            request.form.get('applicable_roles'),
            request.form.get('estimated_time_minutes') or None,
            request.form.get('difficulty_level', 'Intermediate'),
            request.form.get('effective_date') or None,
            session.get('user_id')
        ))
        
        wi_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        
        log_qms_audit(conn, 'WorkInstruction', wi_id, 'Created', session.get('user_id'),
                     session.get('username'), notes=f'Created WI {wi_number}')
        
        conn.commit()
        conn.close()
        
        flash(f'Work Instruction {wi_number} created successfully', 'success')
        return redirect(url_for('qms.wi_view', wi_id=wi_id))
    
    sops = conn.execute('SELECT id, sop_number, title FROM qms_sops WHERE status = ? ORDER BY sop_number', ('Active',)).fetchall()
    
    # ERP Modules list
    erp_modules = [
        'Products', 'Inventory', 'Work Orders', 'Purchase Orders', 'Sales Orders',
        'Suppliers', 'Customers', 'Shipping', 'Receiving', 'Invoicing', 'Service',
        'NDT', 'Tools', 'RFQ', 'Reports', 'Accounting'
    ]
    
    conn.close()
    
    return render_template('qms/wi_form.html',
                          wi=None,
                          sops=[dict(s) for s in sops],
                          erp_modules=erp_modules)


@qms_bp.route('/work-instructions/<int:wi_id>')
@login_required
def wi_view(wi_id):
    """View Work Instruction details"""
    conn = get_db()
    
    wi = conn.execute('''
        SELECT wi.*, s.sop_number, s.title as sop_title,
               u1.username as prepared_by_name,
               u2.username as approved_by_name
        FROM qms_work_instructions wi
        LEFT JOIN qms_sops s ON wi.sop_id = s.id
        LEFT JOIN users u1 ON wi.prepared_by = u1.id
        LEFT JOIN users u2 ON wi.approved_by = u2.id
        WHERE wi.id = ?
    ''', (wi_id,)).fetchone()
    
    if not wi:
        flash('Work Instruction not found', 'danger')
        conn.close()
        return redirect(url_for('qms.wi_list'))
    
    # Get steps
    steps = conn.execute('''
        SELECT * FROM qms_wi_steps 
        WHERE work_instruction_id = ? 
        ORDER BY step_number
    ''', (wi_id,)).fetchall()
    
    # Get acknowledgments
    acknowledgments = conn.execute('''
        SELECT a.*, u.username
        FROM qms_acknowledgments a
        JOIN users u ON a.user_id = u.id
        WHERE a.document_type = 'WorkInstruction' AND a.document_id = ?
        ORDER BY a.acknowledged_at DESC
    ''', (wi_id,)).fetchall()
    
    conn.close()
    
    return render_template('qms/wi_view.html',
                          wi=dict(wi),
                          steps=[dict(s) for s in steps],
                          acknowledgments=[dict(a) for a in acknowledgments])


@qms_bp.route('/work-instructions/<int:wi_id>/edit', methods=['GET', 'POST'])
@login_required
def wi_edit(wi_id):
    """Edit Work Instruction"""
    conn = get_db()
    
    wi = conn.execute('SELECT * FROM qms_work_instructions WHERE id = ?', (wi_id,)).fetchone()
    if not wi:
        flash('Work Instruction not found', 'danger')
        conn.close()
        return redirect(url_for('qms.wi_list'))
    
    if request.method == 'POST':
        conn.execute('''
            UPDATE qms_work_instructions SET
                title = ?, sop_id = ?, description = ?, prerequisites = ?,
                safety_requirements = ?, tools_required = ?, materials_required = ?,
                erp_module = ?, erp_transaction = ?, applicable_roles = ?,
                estimated_time_minutes = ?, difficulty_level = ?, effective_date = ?,
                updated_at = datetime('now')
            WHERE id = ?
        ''', (
            request.form.get('title'),
            request.form.get('sop_id') or None,
            request.form.get('description'),
            request.form.get('prerequisites'),
            request.form.get('safety_requirements'),
            request.form.get('tools_required'),
            request.form.get('materials_required'),
            request.form.get('erp_module'),
            request.form.get('erp_transaction'),
            request.form.get('applicable_roles'),
            request.form.get('estimated_time_minutes') or None,
            request.form.get('difficulty_level', 'Intermediate'),
            request.form.get('effective_date') or None,
            wi_id
        ))
        
        log_qms_audit(conn, 'WorkInstruction', wi_id, 'Updated', session.get('user_id'),
                     session.get('username'))
        
        conn.commit()
        conn.close()
        
        flash('Work Instruction updated successfully', 'success')
        return redirect(url_for('qms.wi_view', wi_id=wi_id))
    
    sops = conn.execute('SELECT id, sop_number, title FROM qms_sops WHERE status = ? ORDER BY sop_number', ('Active',)).fetchall()
    
    erp_modules = [
        'Products', 'Inventory', 'Work Orders', 'Purchase Orders', 'Sales Orders',
        'Suppliers', 'Customers', 'Shipping', 'Receiving', 'Invoicing', 'Service',
        'NDT', 'Tools', 'RFQ', 'Reports', 'Accounting'
    ]
    
    conn.close()
    
    return render_template('qms/wi_form.html',
                          wi=dict(wi),
                          sops=[dict(s) for s in sops],
                          erp_modules=erp_modules)


@qms_bp.route('/work-instructions/<int:wi_id>/steps', methods=['POST'])
@login_required
def wi_add_step(wi_id):
    """Add step to Work Instruction"""
    conn = get_db()
    
    # Get next step number
    result = conn.execute('''
        SELECT MAX(step_number) as max_step FROM qms_wi_steps WHERE work_instruction_id = ?
    ''', (wi_id,)).fetchone()
    next_step = (result['max_step'] or 0) + 1
    
    conn.execute('''
        INSERT INTO qms_wi_steps (work_instruction_id, step_number, title, instructions,
            expected_result, verification_required, verification_type, warning_text,
            caution_text, note_text, estimated_seconds)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        wi_id,
        next_step,
        request.form.get('title'),
        request.form.get('instructions'),
        request.form.get('expected_result'),
        1 if request.form.get('verification_required') else 0,
        request.form.get('verification_type'),
        request.form.get('warning_text'),
        request.form.get('caution_text'),
        request.form.get('note_text'),
        request.form.get('estimated_seconds') or None
    ))
    
    conn.commit()
    conn.close()
    
    flash('Step added successfully', 'success')
    return redirect(url_for('qms.wi_view', wi_id=wi_id))


# ============== Deviations ==============

@qms_bp.route('/deviations')
@login_required
def deviation_list():
    """List all Deviations"""
    conn = get_db()
    
    status_filter = request.args.get('status', '')
    severity_filter = request.args.get('severity', '')
    
    query = '''
        SELECT d.*, u.username as reported_by_name, a.username as assigned_to_name
        FROM qms_deviations d
        LEFT JOIN users u ON d.reported_by = u.id
        LEFT JOIN users a ON d.assigned_to = a.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND d.status = ?'
        params.append(status_filter)
    if severity_filter:
        query += ' AND d.severity = ?'
        params.append(severity_filter)
    
    query += ' ORDER BY d.created_at DESC'
    
    deviations = conn.execute(query, params).fetchall()
    conn.close()
    
    return render_template('qms/deviation_list.html',
                          deviations=[dict(d) for d in deviations],
                          status_filter=status_filter,
                          severity_filter=severity_filter)


@qms_bp.route('/deviations/new', methods=['GET', 'POST'])
@login_required
def deviation_create():
    """Create new Deviation"""
    conn = get_db()
    
    if request.method == 'POST':
        deviation_number = generate_deviation_number(conn)
        
        conn.execute('''
            INSERT INTO qms_deviations (deviation_number, deviation_type, severity,
                sop_id, work_instruction_id, erp_module, erp_transaction_id,
                reported_by, description, root_cause, immediate_action,
                assigned_to, due_date, capa_required)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            deviation_number,
            request.form.get('deviation_type'),
            request.form.get('severity', 'Minor'),
            request.form.get('sop_id') or None,
            request.form.get('work_instruction_id') or None,
            request.form.get('erp_module'),
            request.form.get('erp_transaction_id'),
            session.get('user_id'),
            request.form.get('description'),
            request.form.get('root_cause'),
            request.form.get('immediate_action'),
            request.form.get('assigned_to') or None,
            request.form.get('due_date') or None,
            1 if request.form.get('capa_required') else 0
        ))
        
        deviation_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        
        log_qms_audit(conn, 'Deviation', deviation_id, 'Created', session.get('user_id'),
                     session.get('username'), notes=f'Created {deviation_number}')
        
        conn.commit()
        conn.close()
        
        flash(f'Deviation {deviation_number} created successfully', 'success')
        return redirect(url_for('qms.deviation_list'))
    
    sops = conn.execute('SELECT id, sop_number, title FROM qms_sops ORDER BY sop_number').fetchall()
    wis = conn.execute('SELECT id, wi_number, title FROM qms_work_instructions ORDER BY wi_number').fetchall()
    users = conn.execute('SELECT id, username FROM users ORDER BY username').fetchall()
    
    deviation_types = ['Process', 'Documentation', 'Training', 'Equipment', 'Material', 'Other']
    
    conn.close()
    
    return render_template('qms/deviation_form.html',
                          deviation=None,
                          sops=[dict(s) for s in sops],
                          wis=[dict(w) for w in wis],
                          users=[dict(u) for u in users],
                          deviation_types=deviation_types)


@qms_bp.route('/deviations/<int:deviation_id>/close', methods=['POST'])
@login_required
def deviation_close(deviation_id):
    """Close deviation"""
    conn = get_db()
    
    conn.execute('''
        UPDATE qms_deviations SET
            status = 'Closed',
            closed_date = datetime('now'),
            closed_by = ?,
            closure_notes = ?,
            updated_at = datetime('now')
        WHERE id = ?
    ''', (session.get('user_id'), request.form.get('closure_notes'), deviation_id))
    
    log_qms_audit(conn, 'Deviation', deviation_id, 'Closed', session.get('user_id'),
                 session.get('username'))
    
    conn.commit()
    conn.close()
    
    flash('Deviation closed successfully', 'success')
    return redirect(url_for('qms.deviation_list'))


# ============== CAPA ==============

@qms_bp.route('/capa')
@login_required
def capa_list():
    """List all CAPAs"""
    conn = get_db()
    
    status_filter = request.args.get('status', '')
    priority_filter = request.args.get('priority', '')
    
    query = '''
        SELECT c.*, u.username as assigned_to_name, o.username as owner_name
        FROM qms_capa c
        LEFT JOIN users u ON c.assigned_to = u.id
        LEFT JOIN users o ON c.owner_id = o.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND c.status = ?'
        params.append(status_filter)
    if priority_filter:
        query += ' AND c.priority = ?'
        params.append(priority_filter)
    
    query += ' ORDER BY CASE c.priority WHEN "Critical" THEN 1 WHEN "High" THEN 2 WHEN "Medium" THEN 3 ELSE 4 END, c.target_date'
    
    capas = conn.execute(query, params).fetchall()
    conn.close()
    
    return render_template('qms/capa_list.html',
                          capas=[dict(c) for c in capas],
                          status_filter=status_filter,
                          priority_filter=priority_filter)


@qms_bp.route('/capa/new', methods=['GET', 'POST'])
@login_required
def capa_create():
    """Create new CAPA"""
    conn = get_db()
    
    if request.method == 'POST':
        capa_number = generate_capa_number(conn)
        
        conn.execute('''
            INSERT INTO qms_capa (capa_number, capa_type, priority, source_type, source_id,
                title, description, root_cause_analysis, corrective_action, preventive_action,
                verification_method, effectiveness_criteria, assigned_to, owner_id,
                target_date, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            capa_number,
            request.form.get('capa_type'),
            request.form.get('priority', 'Medium'),
            request.form.get('source_type'),
            request.form.get('source_id') or None,
            request.form.get('title'),
            request.form.get('description'),
            request.form.get('root_cause_analysis'),
            request.form.get('corrective_action'),
            request.form.get('preventive_action'),
            request.form.get('verification_method'),
            request.form.get('effectiveness_criteria'),
            request.form.get('assigned_to') or None,
            request.form.get('owner_id') or session.get('user_id'),
            request.form.get('target_date') or None,
            session.get('user_id')
        ))
        
        capa_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        
        log_qms_audit(conn, 'CAPA', capa_id, 'Created', session.get('user_id'),
                     session.get('username'), notes=f'Created {capa_number}')
        
        conn.commit()
        conn.close()
        
        flash(f'CAPA {capa_number} created successfully', 'success')
        return redirect(url_for('qms.capa_list'))
    
    users = conn.execute('SELECT id, username FROM users ORDER BY username').fetchall()
    deviations = conn.execute('SELECT id, deviation_number, description FROM qms_deviations WHERE capa_required = 1 AND capa_id IS NULL').fetchall()
    
    capa_types = ['Corrective', 'Preventive', 'Both']
    source_types = ['Deviation', 'Audit', 'Customer Complaint', 'Internal Review', 'Regulatory', 'Other']
    
    conn.close()
    
    return render_template('qms/capa_form.html',
                          capa=None,
                          users=[dict(u) for u in users],
                          deviations=[dict(d) for d in deviations],
                          capa_types=capa_types,
                          source_types=source_types)


@qms_bp.route('/capa/<int:capa_id>')
@login_required
def capa_view(capa_id):
    """View CAPA details"""
    conn = get_db()
    
    capa = conn.execute('''
        SELECT c.*, u1.username as assigned_to_name,
               u2.username as owner_name,
               u3.username as created_by_name, u4.username as verified_by_name
        FROM qms_capa c
        LEFT JOIN users u1 ON c.assigned_to = u1.id
        LEFT JOIN users u2 ON c.owner_id = u2.id
        LEFT JOIN users u3 ON c.created_by = u3.id
        LEFT JOIN users u4 ON c.verified_by = u4.id
        WHERE c.id = ?
    ''', (capa_id,)).fetchone()
    
    if not capa:
        flash('CAPA not found', 'danger')
        conn.close()
        return redirect(url_for('qms.capa_list'))
    
    # Get audit trail
    audit_trail = conn.execute('''
        SELECT * FROM qms_audit_trail
        WHERE document_type = 'CAPA' AND document_id = ?
        ORDER BY timestamp DESC
    ''', (capa_id,)).fetchall()
    
    conn.close()
    
    return render_template('qms/capa_view.html',
                          capa=dict(capa),
                          audit_trail=[dict(a) for a in audit_trail])


@qms_bp.route('/capa/<int:capa_id>/update-status', methods=['POST'])
@login_required
def capa_update_status(capa_id):
    """Update CAPA status"""
    conn = get_db()
    new_status = request.form.get('status')
    
    update_fields = ['status = ?', 'updated_at = datetime("now")']
    params = [new_status]
    
    if new_status == 'Closed':
        update_fields.append('completion_date = datetime("now")')
    elif new_status == 'Verified':
        update_fields.extend(['verified_date = datetime("now")', 'verified_by = ?', 'effectiveness_verified = 1'])
        params.append(session.get('user_id'))
    
    params.append(capa_id)
    
    conn.execute(f'UPDATE qms_capa SET {", ".join(update_fields)} WHERE id = ?', params)
    
    log_qms_audit(conn, 'CAPA', capa_id, 'Status Changed', session.get('user_id'),
                 session.get('username'), field_changed='status', new_value=new_status)
    
    conn.commit()
    conn.close()
    
    flash(f'CAPA status updated to {new_status}', 'success')
    return redirect(url_for('qms.capa_view', capa_id=capa_id))


# ============== Categories ==============

@qms_bp.route('/categories')
@login_required
def category_list():
    """List SOP Categories"""
    conn = get_db()
    
    categories = conn.execute('''
        SELECT c.*, COUNT(s.id) as sop_count
        FROM qms_sop_categories c
        LEFT JOIN qms_sops s ON c.id = s.category_id
        GROUP BY c.id
        ORDER BY c.sort_order, c.name
    ''').fetchall()
    
    conn.close()
    
    return render_template('qms/category_list.html',
                          categories=[dict(c) for c in categories])


@qms_bp.route('/categories/new', methods=['GET', 'POST'])
@login_required
def category_create():
    """Create new Category"""
    conn = get_db()
    
    if request.method == 'POST':
        conn.execute('''
            INSERT INTO qms_sop_categories (code, name, description, sort_order)
            VALUES (?, ?, ?, ?)
        ''', (
            request.form.get('code'),
            request.form.get('name'),
            request.form.get('description'),
            request.form.get('sort_order') or 0
        ))
        
        conn.commit()
        conn.close()
        
        flash('Category created successfully', 'success')
        return redirect(url_for('qms.category_list'))
    
    conn.close()
    return render_template('qms/category_form.html', category=None)


# ============== QMS AI Manager ==============

@qms_bp.route('/ai-manager')
@login_required
def ai_manager():
    """QMS AI Manager Dashboard"""
    conn = get_db()
    
    # Get compliance summary
    sop_stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) as active,
            SUM(CASE WHEN review_date < date('now') AND status = 'Active' THEN 1 ELSE 0 END) as overdue
        FROM qms_sops
    ''').fetchone()
    
    deviation_stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Open' THEN 1 ELSE 0 END) as open,
            SUM(CASE WHEN severity = 'Critical' AND status = 'Open' THEN 1 ELSE 0 END) as critical
        FROM qms_deviations
    ''').fetchone()
    
    capa_stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status NOT IN ('Closed', 'Verified') THEN 1 ELSE 0 END) as open,
            SUM(CASE WHEN target_date < date('now') AND status NOT IN ('Closed', 'Verified') THEN 1 ELSE 0 END) as overdue
        FROM qms_capa
    ''').fetchone()
    
    # Get recent AI analyses
    recent_analyses = conn.execute('''
        SELECT a.*, u.username
        FROM qms_ai_analyses a
        LEFT JOIN users u ON a.user_id = u.id
        ORDER BY a.created_at DESC LIMIT 10
    ''').fetchall()
    
    conn.close()
    
    return render_template('qms/ai_manager.html',
                          sop_stats=dict(sop_stats) if sop_stats else {},
                          deviation_stats=dict(deviation_stats) if deviation_stats else {},
                          capa_stats=dict(capa_stats) if capa_stats else {},
                          recent_analyses=[dict(a) for a in recent_analyses])


@qms_bp.route('/ai-manager/analyze', methods=['POST'])
@login_required
def ai_analyze():
    """Run AI analysis"""
    conn = get_db()
    analysis_type = request.form.get('analysis_type')
    context = request.form.get('context', '')
    
    try:
        from openai import OpenAI
        
        client = OpenAI(
            api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
            base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
        )
        
        # Build context based on analysis type
        if analysis_type == 'compliance_review':
            # Get compliance data
            sops = conn.execute('SELECT sop_number, title, status, review_date FROM qms_sops').fetchall()
            deviations = conn.execute('SELECT deviation_number, deviation_type, severity, status FROM qms_deviations WHERE status = "Open"').fetchall()
            capas = conn.execute('SELECT capa_number, capa_type, priority, status FROM qms_capa WHERE status NOT IN ("Closed", "Verified")').fetchall()
            
            context_data = {
                'sops': [dict(s) for s in sops],
                'open_deviations': [dict(d) for d in deviations],
                'open_capas': [dict(c) for c in capas]
            }
            
            prompt = f"""As a QMS AI Manager, analyze the following compliance data and provide:
1. Overall compliance health score (0-100)
2. Key compliance risks identified
3. Prioritized recommendations for improvement
4. Areas requiring immediate attention

Context: {context}

Data:
{json.dumps(context_data, indent=2)}

Provide a structured analysis with actionable recommendations."""

        elif analysis_type == 'sop_generation':
            prompt = f"""As a QMS AI Manager, generate a comprehensive Standard Operating Procedure (SOP) for:
{context}

The SOP should include:
1. Purpose and Scope
2. Responsibilities (roles involved)
3. Definitions (key terms)
4. Detailed Procedure Steps
5. References (related documents/standards)
6. Revision History template

Format the output in a clear, professional structure suitable for an aerospace/MRO organization."""

        elif analysis_type == 'deviation_analysis':
            # Get recent deviations
            deviations = conn.execute('''
                SELECT deviation_number, deviation_type, severity, description, root_cause, status
                FROM qms_deviations
                ORDER BY created_at DESC LIMIT 20
            ''').fetchall()
            
            context_data = [dict(d) for d in deviations]
            
            prompt = f"""As a QMS AI Manager, analyze the following deviation data and provide:
1. Pattern analysis - common deviation types and root causes
2. Trend identification
3. Systemic issues that may be causing multiple deviations
4. Preventive recommendations to reduce future deviations
5. Training or process improvement suggestions

Data:
{json.dumps(context_data, indent=2)}

Provide actionable insights for continuous improvement."""

        elif analysis_type == 'process_guidance':
            prompt = f"""As a QMS AI Manager for an aerospace MRO ERP system, provide detailed guidance for:
{context}

Include:
1. Step-by-step process instructions
2. Best practices and tips
3. Common pitfalls to avoid
4. Quality checkpoints
5. Related SOPs or work instructions that should be referenced

Keep the guidance practical and applicable to daily operations."""

        else:
            prompt = f"As a QMS AI Manager, analyze and respond to: {context}"
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an expert QMS AI Manager for an aerospace MRO organization. Provide professional, actionable guidance focused on quality, compliance, and continuous improvement."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=2000
        )
        
        ai_response = response.choices[0].message.content
        
        # Save analysis
        conn.execute('''
            INSERT INTO qms_ai_analyses (analysis_type, context, request_data, response_data, user_id)
            VALUES (?, ?, ?, ?, ?)
        ''', (analysis_type, context, prompt, ai_response, session.get('user_id')))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'analysis': ai_response})
        
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)})


@qms_bp.route('/ai-manager/generate-sop', methods=['POST'])
@login_required
def ai_generate_sop():
    """Generate SOP using AI"""
    data = request.get_json()
    process_name = data.get('process_name', '')
    process_description = data.get('description', '')
    erp_module = data.get('erp_module', '')
    
    try:
        from openai import OpenAI
        
        client = OpenAI(
            api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
            base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
        )
        
        prompt = f"""Generate a comprehensive Standard Operating Procedure (SOP) for an aerospace MRO organization.

Process Name: {process_name}
Description: {process_description}
ERP Module: {erp_module}

Generate a complete SOP with the following structure (return as JSON):
{{
    "title": "SOP title",
    "purpose": "Purpose statement",
    "scope": "Scope of the procedure",
    "responsibilities": "Roles and their responsibilities",
    "definitions": "Key terms and definitions",
    "procedure_content": "Detailed step-by-step procedure with numbered steps",
    "references_text": "Related documents and standards",
    "compliance_standards": "Applicable compliance standards (AS9100, FAA, etc.)",
    "applicable_roles": "Comma-separated list of roles (Admin, Planner, Technician, etc.)",
    "applicable_modules": "Comma-separated list of ERP modules this applies to"
}}

Ensure the procedure is professional, detailed, and follows aerospace industry best practices."""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an expert QMS consultant specializing in aerospace MRO operations. Generate professional, compliant SOPs."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.5,
            max_tokens=2000,
            response_format={"type": "json_object"}
        )
        
        sop_data = json.loads(response.choices[0].message.content or '{}')
        
        return jsonify({'success': True, 'sop': sop_data})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@qms_bp.route('/ai-manager/generate-iso-sop', methods=['POST'])
@login_required
def ai_generate_iso_sop():
    """Generate ISO-compliant SOP using AI and save to database"""
    data = request.get_json()
    process_name = data.get('process_name', '')
    category_id = data.get('category_id', '')
    erp_module = data.get('erp_module', '')
    context = data.get('context', '')
    standards = data.get('standards', ['AS9100', 'ISO 9001'])
    
    if not process_name:
        return jsonify({'success': False, 'error': 'Process name is required'})
    
    try:
        from openai import OpenAI
        
        client = OpenAI(
            api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
            base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
        )
        
        standards_str = ', '.join(standards)
        
        prompt = f"""Generate a comprehensive ISO-compliant Standard Operating Procedure (SOP) for an aerospace MRO organization.

Process Name: {process_name}
ERP Module: {erp_module if erp_module else 'General'}
Compliance Standards: {standards_str}
Additional Context: {context if context else 'Standard aerospace MRO operations'}

Generate a complete SOP following {standards_str} requirements with the following structure (return as JSON):
{{
    "title": "Full SOP title",
    "purpose": "Clear purpose statement (2-3 sentences)",
    "scope": "Detailed scope including what is covered and what is excluded",
    "responsibilities": "Section with roles and their specific responsibilities in bulleted format",
    "definitions": "Key terms and definitions relevant to this procedure",
    "procedure_content": "Detailed step-by-step procedure with numbered main steps and sub-steps. Include quality checks, documentation requirements, and verification points. Format with clear sections.",
    "references_text": "List of related documents, standards, and regulatory references",
    "compliance_standards": "{standards_str}",
    "applicable_roles": "Comma-separated list of applicable roles (e.g., Admin, Planner, Technician, Quality, Manager)",
    "applicable_modules": "{erp_module if erp_module else 'General'}"
}}

Ensure the procedure is:
1. Professional and detailed enough for actual use
2. Compliant with {standards_str} documentation requirements
3. Includes clear acceptance criteria and verification steps
4. References appropriate forms, records, and documentation
5. Follows aerospace industry best practices"""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an expert QMS consultant specializing in aerospace MRO operations. Generate professional, compliant SOPs that meet regulatory requirements."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.5,
            max_tokens=3000,
            response_format={"type": "json_object"}
        )
        
        sop_data = json.loads(response.choices[0].message.content or '{}')
        
        def to_string(val):
            if isinstance(val, list):
                return '\n'.join(str(item) for item in val)
            return str(val) if val else ''
        
        conn = get_db()
        
        count = conn.execute('SELECT COUNT(*) FROM qms_sops').fetchone()[0]
        sop_number = f"SOP-{(count + 1):04d}"
        
        cursor = conn.execute('''
            INSERT INTO qms_sops (
                sop_number, title, purpose, scope, responsibilities, definitions,
                procedure_content, references_text, compliance_standards,
                category_id, status, revision, applicable_roles, applicable_modules,
                prepared_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ''', (
            sop_number,
            to_string(sop_data.get('title', process_name)),
            to_string(sop_data.get('purpose', '')),
            to_string(sop_data.get('scope', '')),
            to_string(sop_data.get('responsibilities', '')),
            to_string(sop_data.get('definitions', '')),
            to_string(sop_data.get('procedure_content', '')),
            to_string(sop_data.get('references_text', '')),
            to_string(sop_data.get('compliance_standards', standards_str)),
            category_id if category_id else None,
            'Draft',
            1,
            to_string(sop_data.get('applicable_roles', '')),
            to_string(sop_data.get('applicable_modules', erp_module)),
            session.get('user_id')
        ))
        
        sop_id = cursor.lastrowid
        
        log_qms_audit(conn, 'SOP', sop_id, 'Created', session.get('user_id'),
                     session.get('username'), notes=f'AI-generated SOP {sop_number}')
        
        conn.execute('''
            INSERT INTO qms_ai_analyses (analysis_type, context, request_data, response_data, user_id)
            VALUES (?, ?, ?, ?, ?)
        ''', ('iso_sop_generation', process_name, prompt, json.dumps(sop_data), session.get('user_id')))
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True, 
            'sop_id': sop_id,
            'sop_number': sop_number,
            'redirect_url': url_for('qms.sop_view', sop_id=sop_id)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ============== Audit Trail ==============

@qms_bp.route('/audit-trail')
@login_required
def audit_trail():
    """View QMS Audit Trail"""
    conn = get_db()
    
    document_type = request.args.get('type', '')
    action_filter = request.args.get('action', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    
    query = 'SELECT * FROM qms_audit_trail WHERE 1=1'
    params = []
    
    if document_type:
        query += ' AND document_type = ?'
        params.append(document_type)
    if action_filter:
        query += ' AND action = ?'
        params.append(action_filter)
    if date_from:
        query += ' AND date(timestamp) >= ?'
        params.append(date_from)
    if date_to:
        query += ' AND date(timestamp) <= ?'
        params.append(date_to)
    
    query += ' ORDER BY timestamp DESC LIMIT 500'
    
    entries = conn.execute(query, params).fetchall()
    conn.close()
    
    return render_template('qms/audit_trail.html',
                          entries=[dict(e) for e in entries],
                          document_type=document_type,
                          action_filter=action_filter,
                          date_from=date_from,
                          date_to=date_to)


# ============== Training Records ==============

@qms_bp.route('/training')
@login_required
def training_list():
    """List training records"""
    conn = get_db()
    
    records = conn.execute('''
        SELECT t.*, u.username,
               CASE t.document_type 
                   WHEN 'SOP' THEN (SELECT sop_number FROM qms_sops WHERE id = t.document_id)
                   WHEN 'WorkInstruction' THEN (SELECT wi_number FROM qms_work_instructions WHERE id = t.document_id)
               END as document_number
        FROM qms_training_records t
        JOIN users u ON t.user_id = u.id
        ORDER BY t.training_date DESC
    ''').fetchall()
    
    conn.close()
    
    return render_template('qms/training_list.html',
                          records=[dict(r) for r in records])


@qms_bp.route('/training/new', methods=['GET', 'POST'])
@login_required  
def training_create():
    """Create training record"""
    conn = get_db()
    
    if request.method == 'POST':
        conn.execute('''
            INSERT INTO qms_training_records (user_id, document_type, document_id,
                training_type, training_date, trainer_id, score, passed, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            request.form.get('user_id'),
            request.form.get('document_type'),
            request.form.get('document_id'),
            request.form.get('training_type', 'Initial'),
            request.form.get('training_date'),
            request.form.get('trainer_id') or None,
            request.form.get('score') or None,
            1 if request.form.get('passed') else 0,
            request.form.get('notes')
        ))
        
        conn.commit()
        conn.close()
        
        flash('Training record created successfully', 'success')
        return redirect(url_for('qms.training_list'))
    
    users = conn.execute('SELECT id, username FROM users ORDER BY username').fetchall()
    sops = conn.execute('SELECT id, sop_number, title FROM qms_sops ORDER BY sop_number').fetchall()
    wis = conn.execute('SELECT id, wi_number, title FROM qms_work_instructions ORDER BY wi_number').fetchall()
    
    conn.close()
    
    return render_template('qms/training_form.html',
                          record=None,
                          users=[dict(u) for u in users],
                          sops=[dict(s) for s in sops],
                          wis=[dict(w) for w in wis])


# ============== Auto-Generate Work Instructions ==============

# Define all ERP transaction capabilities
ERP_TRANSACTION_CAPABILITIES = {
    'Products': [
        {'code': 'PROD-CREATE', 'name': 'Create Product', 'description': 'Create a new product in the system'},
        {'code': 'PROD-EDIT', 'name': 'Edit Product', 'description': 'Modify existing product information'},
        {'code': 'PROD-BOM', 'name': 'Manage Bill of Materials', 'description': 'Create and edit product BOM structures'},
        {'code': 'PROD-SERIAL', 'name': 'Manage Serialized Products', 'description': 'Handle serial number tracking for products'},
    ],
    'Inventory': [
        {'code': 'INV-RECEIVE', 'name': 'Receive Inventory', 'description': 'Receive items into inventory'},
        {'code': 'INV-ADJUST', 'name': 'Adjust Inventory', 'description': 'Perform inventory adjustments'},
        {'code': 'INV-TRANSFER', 'name': 'Transfer Inventory', 'description': 'Transfer inventory between locations'},
        {'code': 'INV-COUNT', 'name': 'Physical Count', 'description': 'Conduct physical inventory counts'},
        {'code': 'INV-ALLOCATE', 'name': 'Allocate Inventory', 'description': 'Allocate inventory to work orders'},
    ],
    'Work Orders': [
        {'code': 'WO-CREATE', 'name': 'Create Work Order', 'description': 'Create a new manufacturing work order'},
        {'code': 'WO-RELEASE', 'name': 'Release Work Order', 'description': 'Release work order for production'},
        {'code': 'WO-MATERIAL', 'name': 'Issue Materials', 'description': 'Issue materials to work order'},
        {'code': 'WO-LABOR', 'name': 'Record Labor', 'description': 'Record labor time against work order'},
        {'code': 'WO-COMPLETE', 'name': 'Complete Work Order', 'description': 'Complete work order and receive finished goods'},
        {'code': 'WO-CLOSE', 'name': 'Close Work Order', 'description': 'Close work order after completion'},
    ],
    'Purchase Orders': [
        {'code': 'PO-CREATE', 'name': 'Create Purchase Order', 'description': 'Create a new purchase order'},
        {'code': 'PO-APPROVE', 'name': 'Approve Purchase Order', 'description': 'Approve purchase order for sending'},
        {'code': 'PO-SEND', 'name': 'Send to Supplier', 'description': 'Send purchase order to supplier'},
        {'code': 'PO-RECEIVE', 'name': 'Receive Goods', 'description': 'Receive goods against purchase order'},
        {'code': 'PO-PARTIAL', 'name': 'Partial Receipt', 'description': 'Process partial receipt of goods'},
        {'code': 'PO-CLOSE', 'name': 'Close Purchase Order', 'description': 'Close purchase order after completion'},
    ],
    'Sales Orders': [
        {'code': 'SO-CREATE', 'name': 'Create Sales Order', 'description': 'Create a new sales order'},
        {'code': 'SO-CONFIRM', 'name': 'Confirm Sales Order', 'description': 'Confirm and process sales order'},
        {'code': 'SO-SHIP', 'name': 'Ship Sales Order', 'description': 'Process shipment for sales order'},
        {'code': 'SO-INVOICE', 'name': 'Generate Invoice', 'description': 'Generate invoice from sales order'},
        {'code': 'SO-CLOSE', 'name': 'Close Sales Order', 'description': 'Close sales order after completion'},
    ],
    'Service Work Orders': [
        {'code': 'SWO-CREATE', 'name': 'Create Service Work Order', 'description': 'Create a service work order'},
        {'code': 'SWO-LABOR', 'name': 'Record Service Labor', 'description': 'Record labor for service work'},
        {'code': 'SWO-PARTS', 'name': 'Add Service Parts', 'description': 'Add parts to service work order'},
        {'code': 'SWO-COMPLETE', 'name': 'Complete Service', 'description': 'Complete service work order'},
    ],
    'NDT Operations': [
        {'code': 'NDT-CREATE', 'name': 'Create NDT Work Order', 'description': 'Create NDT inspection work order'},
        {'code': 'NDT-SCHEDULE', 'name': 'Schedule Inspection', 'description': 'Schedule NDT inspection'},
        {'code': 'NDT-PERFORM', 'name': 'Perform Inspection', 'description': 'Perform NDT inspection and record results'},
        {'code': 'NDT-REVIEW', 'name': 'Level III Review', 'description': 'Level III technician review and approval'},
        {'code': 'NDT-REPORT', 'name': 'Generate NDT Report', 'description': 'Generate inspection report'},
    ],
    'RFQ': [
        {'code': 'RFQ-CREATE', 'name': 'Create RFQ', 'description': 'Create request for quotation'},
        {'code': 'RFQ-SEND', 'name': 'Send to Suppliers', 'description': 'Send RFQ to selected suppliers'},
        {'code': 'RFQ-QUOTE', 'name': 'Enter Quotes', 'description': 'Enter supplier quote responses'},
        {'code': 'RFQ-COMPARE', 'name': 'Compare Quotes', 'description': 'Compare and analyze quotes'},
        {'code': 'RFQ-AWARD', 'name': 'Award RFQ', 'description': 'Award RFQ to selected supplier'},
    ],
    'Shipping': [
        {'code': 'SHIP-CREATE', 'name': 'Create Shipment', 'description': 'Create shipment record'},
        {'code': 'SHIP-PACK', 'name': 'Pack Items', 'description': 'Pack items for shipment'},
        {'code': 'SHIP-LABEL', 'name': 'Generate Labels', 'description': 'Generate shipping labels'},
        {'code': 'SHIP-DISPATCH', 'name': 'Dispatch Shipment', 'description': 'Dispatch shipment to carrier'},
    ],
    'Invoicing': [
        {'code': 'INV-CREATE', 'name': 'Create Invoice', 'description': 'Create customer invoice'},
        {'code': 'INV-SEND', 'name': 'Send Invoice', 'description': 'Send invoice to customer'},
        {'code': 'INV-PAYMENT', 'name': 'Record Payment', 'description': 'Record payment against invoice'},
        {'code': 'INV-CREDIT', 'name': 'Issue Credit Memo', 'description': 'Issue credit memo'},
    ],
    'Capacity Planning': [
        {'code': 'CAP-SCHEDULE', 'name': 'Schedule Production', 'description': 'Schedule production load'},
        {'code': 'CAP-RESOURCE', 'name': 'Assign Resources', 'description': 'Assign labor resources to work centers'},
        {'code': 'CAP-OVERRIDE', 'name': 'Override Capacity', 'description': 'Override capacity constraints'},
    ],
    'Quality (QMS)': [
        {'code': 'QMS-SOP', 'name': 'Create SOP', 'description': 'Create standard operating procedure'},
        {'code': 'QMS-WI', 'name': 'Create Work Instruction', 'description': 'Create work instruction'},
        {'code': 'QMS-DEV', 'name': 'Report Deviation', 'description': 'Report quality deviation'},
        {'code': 'QMS-CAPA', 'name': 'Create CAPA', 'description': 'Create corrective/preventive action'},
        {'code': 'QMS-AUDIT', 'name': 'Conduct Audit', 'description': 'Conduct quality audit'},
    ],
    'Tools Management': [
        {'code': 'TOOL-CHECKOUT', 'name': 'Checkout Tool', 'description': 'Check out tool for use'},
        {'code': 'TOOL-CHECKIN', 'name': 'Checkin Tool', 'description': 'Return tool after use'},
        {'code': 'TOOL-CAL', 'name': 'Record Calibration', 'description': 'Record tool calibration'},
    ],
    'Time Clock': [
        {'code': 'TIME-IN', 'name': 'Clock In', 'description': 'Clock in for work shift'},
        {'code': 'TIME-OUT', 'name': 'Clock Out', 'description': 'Clock out from work shift'},
        {'code': 'TIME-WO', 'name': 'Assign to Work Order', 'description': 'Assign time to work order task'},
    ],
}


@qms_bp.route('/work-instructions/auto-generate')
@login_required
def wi_auto_generate():
    """Auto-generate work instructions page"""
    conn = get_db()
    
    # Get existing SOPs for linking
    sops = conn.execute('SELECT id, sop_number, title FROM qms_sops WHERE status = ? ORDER BY sop_number', ('Active',)).fetchall()
    
    # Get recently generated work instructions
    recent_wis = conn.execute('''
        SELECT wi.*, u.username as created_by_name
        FROM qms_work_instructions wi
        LEFT JOIN users u ON wi.prepared_by = u.id
        WHERE wi.erp_transaction IS NOT NULL
        ORDER BY wi.created_at DESC
        LIMIT 10
    ''').fetchall()
    
    conn.close()
    
    return render_template('qms/wi_auto_generate.html',
                          transaction_capabilities=ERP_TRANSACTION_CAPABILITIES,
                          sops=[dict(s) for s in sops],
                          recent_wis=[dict(w) for w in recent_wis])


@qms_bp.route('/work-instructions/auto-generate/generate', methods=['POST'])
@login_required
def wi_generate():
    """Generate work instruction using AI"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'})
        
        transaction_code = data.get('transaction_code')
        transaction_name = data.get('transaction_name')
        transaction_description = data.get('description')
        module_name = data.get('module')
        sop_id = data.get('sop_id')
        additional_context = data.get('additional_context', '')
        
        # Validate required fields
        if not transaction_code or not transaction_name or not module_name:
            return jsonify({'success': False, 'error': 'Missing required fields: transaction_code, transaction_name, and module are required'})
        
        # Validate module exists
        if module_name not in ERP_TRANSACTION_CAPABILITIES:
            return jsonify({'success': False, 'error': f'Invalid module: {module_name}'})
        
        # Validate OpenAI configuration
        if not os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'):
            return jsonify({'success': False, 'error': 'OpenAI API key not configured'})
        
        conn = get_db()
        from openai import OpenAI
        
        client = OpenAI(
            api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
            base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
        )
        
        prompt = f"""Generate a comprehensive, standardized Work Instruction for an aerospace MRO ERP system.

Transaction: {transaction_name}
Code: {transaction_code}
Module: {module_name}
Description: {transaction_description}
{f'Additional Context: {additional_context}' if additional_context else ''}

Create a detailed work instruction with the following sections:

1. **TITLE**: Clear, descriptive title for the work instruction

2. **PURPOSE**: Why this procedure is needed (2-3 sentences)

3. **PREREQUISITES**: 
   - Required user roles/permissions
   - Required system access
   - Any preparatory steps needed

4. **SAFETY REQUIREMENTS**: Any safety or compliance considerations

5. **TOOLS/EQUIPMENT REQUIRED**: System access, equipment, or tools needed

6. **MATERIALS/DOCUMENTS REQUIRED**: Forms, reference documents, or materials needed

7. **STEP-BY-STEP PROCEDURE**: Detailed numbered steps including:
   - Navigation path in the ERP system
   - Specific fields to complete
   - Required vs optional fields
   - Business rules and validations
   - Expected system responses
   - Screenshots guidance (describe what should be visible)

8. **VERIFICATION/QUALITY CHECKPOINTS**: Steps to verify correct completion

9. **COMMON ERRORS AND TROUBLESHOOTING**: Potential issues and resolutions

10. **RELATED TRANSACTIONS**: Other procedures that may follow or precede this one

Format the output as structured JSON with these keys:
{{
    "title": "string",
    "purpose": "string",
    "prerequisites": "string (multi-line with bullet points)",
    "safety_requirements": "string",
    "tools_required": "string",
    "materials_required": "string",
    "procedure_steps": [
        {{"step_number": 1, "action": "string", "expected_result": "string", "notes": "string or null"}}
    ],
    "verification_checkpoints": "string",
    "troubleshooting": "string",
    "related_transactions": "string"
}}"""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an expert technical writer specializing in ERP system documentation for aerospace MRO organizations. Generate clear, compliant, and comprehensive work instructions following AS9100 and ISO 9001 standards. Always respond with valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=3000,
            response_format={"type": "json_object"}
        )
        
        ai_response = response.choices[0].message.content or '{}'
        generated_content = json.loads(ai_response)
        
        # Generate WI number
        wi_number = generate_wi_number(conn)
        
        # Format procedure steps for storage
        procedure_steps = generated_content.get('procedure_steps', [])
        formatted_steps = json.dumps(procedure_steps)
        
        # Create work instruction with all generated fields
        cursor = conn.execute('''
            INSERT INTO qms_work_instructions (
                wi_number, title, sop_id, revision, revision_date, effective_date,
                description, prerequisites, safety_requirements, tools_required,
                materials_required, erp_module, erp_transaction, applicable_roles,
                verification_checkpoints, troubleshooting, related_transactions,
                status, prepared_by, created_at
            ) VALUES (?, ?, ?, 'A', date('now'), date('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Draft', ?, datetime('now'))
        ''', (
            wi_number,
            generated_content.get('title', transaction_name),
            sop_id if sop_id else None,
            generated_content.get('purpose', ''),
            generated_content.get('prerequisites', ''),
            generated_content.get('safety_requirements', ''),
            generated_content.get('tools_required', ''),
            generated_content.get('materials_required', ''),
            module_name,
            transaction_code,
            'All',
            generated_content.get('verification_checkpoints', ''),
            generated_content.get('troubleshooting', ''),
            generated_content.get('related_transactions', ''),
            session.get('user_id')
        ))
        
        wi_id = cursor.lastrowid
        
        # Insert procedure steps
        for step in procedure_steps:
            conn.execute('''
                INSERT INTO qms_work_instruction_steps (
                    work_instruction_id, step_number, action, expected_result, notes
                ) VALUES (?, ?, ?, ?, ?)
            ''', (
                wi_id,
                step.get('step_number', 0),
                step.get('action', ''),
                step.get('expected_result', ''),
                step.get('notes')
            ))
        
        # Log audit
        log_qms_audit(conn, 'Work Instruction', wi_id, 'Auto-Generated', 
                     session.get('user_id'), session.get('username'),
                     notes=f'Transaction: {transaction_code}')
        
        # Save AI analysis
        conn.execute('''
            INSERT INTO qms_ai_analyses (analysis_type, context, request_data, response_data, user_id)
            VALUES ('work_instruction_generation', ?, ?, ?, ?)
        ''', (transaction_code, prompt, ai_response, session.get('user_id')))
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'wi_id': wi_id,
            'wi_number': wi_number,
            'title': generated_content.get('title'),
            'content': generated_content,
            'message': f'Work Instruction {wi_number} generated successfully'
        })
        
    except json.JSONDecodeError as e:
        try:
            conn.close()
        except:
            pass
        return jsonify({'success': False, 'error': f'Failed to parse AI response: {str(e)}'})
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        return jsonify({'success': False, 'error': f'Error generating work instruction: {str(e)}'})


@qms_bp.route('/work-instructions/auto-generate/single', methods=['POST'])
@login_required
def wi_auto_generate_single():
    """Generate a single work instruction from form data"""
    module_name = request.form.get('module')
    transaction_code = request.form.get('transaction_code')
    sop_id = request.form.get('sop_id') or None
    additional_context = request.form.get('additional_context', '')
    
    if not module_name or not transaction_code:
        return jsonify({'success': False, 'error': 'Module and transaction are required'})
    
    if module_name not in ERP_TRANSACTION_CAPABILITIES:
        return jsonify({'success': False, 'error': f'Invalid module: {module_name}'})
    
    txn = next((t for t in ERP_TRANSACTION_CAPABILITIES[module_name] if t['code'] == transaction_code), None)
    if not txn:
        return jsonify({'success': False, 'error': f'Transaction {transaction_code} not found in {module_name}'})
    
    if not os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'):
        return jsonify({'success': False, 'error': 'OpenAI API key not configured'})
    
    conn = get_db()
    
    try:
        from openai import OpenAI
        
        client = OpenAI(
            api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
            base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
        )
        
        prompt = f"""Generate a comprehensive, standardized Work Instruction for an aerospace MRO ERP system.

Transaction: {txn['name']}
Code: {transaction_code}
Module: {module_name}
Description: {txn['description']}
{f'Additional Context: {additional_context}' if additional_context else ''}

Create a detailed work instruction with the following sections and output as JSON:
{{
    "title": "string",
    "purpose": "string",
    "prerequisites": "string (multi-line with bullet points)",
    "safety_requirements": "string",
    "tools_required": "string",
    "materials_required": "string",
    "procedure_steps": [
        {{"step_number": 1, "action": "string", "expected_result": "string", "notes": "string or null"}}
    ],
    "verification_checkpoints": "string",
    "troubleshooting": "string",
    "related_transactions": "string"
}}"""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an expert technical writer for aerospace MRO ERP documentation. Generate clear, AS9100/ISO 9001 compliant work instructions. Always respond with valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=3000,
            response_format={"type": "json_object"}
        )
        
        generated_content = json.loads(response.choices[0].message.content or '{}')
        wi_number = generate_wi_number(conn)
        
        cursor = conn.execute('''
            INSERT INTO qms_work_instructions (
                wi_number, title, sop_id, revision, revision_date, effective_date,
                description, prerequisites, safety_requirements, tools_required,
                materials_required, erp_module, erp_transaction, applicable_roles,
                verification_checkpoints, troubleshooting, related_transactions,
                status, prepared_by, created_at
            ) VALUES (?, ?, ?, 'A', date('now'), date('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Draft', ?, datetime('now'))
        ''', (
            wi_number,
            generated_content.get('title', txn['name']),
            sop_id if sop_id else None,
            generated_content.get('purpose', ''),
            generated_content.get('prerequisites', ''),
            generated_content.get('safety_requirements', ''),
            generated_content.get('tools_required', ''),
            generated_content.get('materials_required', ''),
            module_name,
            transaction_code,
            'All',
            generated_content.get('verification_checkpoints', ''),
            generated_content.get('troubleshooting', ''),
            generated_content.get('related_transactions', ''),
            session.get('user_id')
        ))
        
        wi_id = cursor.lastrowid
        
        for step in generated_content.get('procedure_steps', []):
            conn.execute('''
                INSERT INTO qms_work_instruction_steps (
                    work_instruction_id, step_number, action, expected_result, notes
                ) VALUES (?, ?, ?, ?, ?)
            ''', (wi_id, step.get('step_number', 0), step.get('action', ''), step.get('expected_result', ''), step.get('notes')))
        
        log_qms_audit(conn, 'Work Instruction', wi_id, 'Auto-Generated', 
                     session.get('user_id'), session.get('username'),
                     notes=f'Transaction: {transaction_code}')
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'wi_id': wi_id,
            'wi_number': wi_number,
            'redirect_url': url_for('qms.wi_view', wi_id=wi_id)
        })
        
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)})


@qms_bp.route('/work-instructions/auto-generate/bulk', methods=['POST'])
@login_required
def wi_auto_generate_bulk():
    """Bulk generate work instructions for multiple transactions"""
    data = request.get_json()
    module_name = data.get('module')
    sop_id = data.get('sop_id')
    
    # If module is provided, get all transactions for that module
    if module_name and module_name in ERP_TRANSACTION_CAPABILITIES:
        transactions = [
            {'code': t['code'], 'name': t['name'], 'description': t['description'], 'module': module_name}
            for t in ERP_TRANSACTION_CAPABILITIES[module_name]
        ]
    else:
        transactions = data.get('transactions', [])
    
    if not transactions:
        return jsonify({'success': False, 'error': 'No transactions to generate', 'generated_count': 0})
    
    results = []
    generated_count = 0
    
    for txn in transactions:
        try:
            response = wi_generate_single(
                txn['code'], txn['name'], txn['description'], 
                txn.get('module', module_name), sop_id
            )
            if response.get('success'):
                generated_count += 1
            results.append({
                'code': txn['code'],
                'success': response.get('success', False),
                'wi_number': response.get('wi_number'),
                'error': response.get('error')
            })
        except Exception as e:
            results.append({
                'code': txn['code'],
                'success': False,
                'error': str(e)
            })
    
    return jsonify({
        'success': True,
        'results': results,
        'generated_count': generated_count,
        'generated': generated_count,
        'failed': len(transactions) - generated_count
    })


def wi_generate_single(transaction_code, transaction_name, description, module_name, sop_id):
    """Internal function to generate a single work instruction"""
    conn = get_db()
    
    try:
        from openai import OpenAI
        
        client = OpenAI(
            api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
            base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
        )
        
        prompt = f"""Generate a Work Instruction for an aerospace MRO ERP system.

Transaction: {transaction_name}
Code: {transaction_code}
Module: {module_name}
Description: {description}

Create a work instruction with:
1. Title
2. Purpose (2-3 sentences)
3. Prerequisites (required permissions, system access)
4. Safety requirements
5. Tools/equipment required
6. Materials/documents required
7. Step-by-step procedure (5-10 numbered steps with actions and expected results)
8. Verification checkpoints
9. Troubleshooting tips

Format as JSON:
{{
    "title": "string",
    "purpose": "string",
    "prerequisites": "string",
    "safety_requirements": "string",
    "tools_required": "string",
    "materials_required": "string",
    "procedure_steps": [{{"step_number": 1, "action": "string", "expected_result": "string"}}],
    "verification_checkpoints": "string",
    "troubleshooting": "string",
    "related_transactions": "string"
}}"""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an expert ERP documentation writer. Generate clear work instructions. Respond with valid JSON only."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=2000,
            response_format={"type": "json_object"}
        )
        
        generated_content = json.loads(response.choices[0].message.content or '{}')
        wi_number = generate_wi_number(conn)
        
        cursor = conn.execute('''
            INSERT INTO qms_work_instructions (
                wi_number, title, sop_id, revision, revision_date, effective_date,
                description, prerequisites, safety_requirements, tools_required,
                materials_required, erp_module, erp_transaction, applicable_roles,
                verification_checkpoints, troubleshooting, related_transactions,
                status, prepared_by, created_at
            ) VALUES (?, ?, ?, 'A', date('now'), date('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Draft', ?, datetime('now'))
        ''', (
            wi_number,
            generated_content.get('title', transaction_name),
            sop_id if sop_id else None,
            generated_content.get('purpose', ''),
            generated_content.get('prerequisites', ''),
            generated_content.get('safety_requirements', ''),
            generated_content.get('tools_required', ''),
            generated_content.get('materials_required', ''),
            module_name,
            transaction_code,
            'All',
            generated_content.get('verification_checkpoints', ''),
            generated_content.get('troubleshooting', ''),
            generated_content.get('related_transactions', ''),
            session.get('user_id')
        ))
        
        wi_id = cursor.lastrowid
        
        for step in generated_content.get('procedure_steps', []):
            conn.execute('''
                INSERT INTO qms_work_instruction_steps (
                    work_instruction_id, step_number, action, expected_result, notes
                ) VALUES (?, ?, ?, ?, ?)
            ''', (wi_id, step.get('step_number', 0), step.get('action', ''), 
                  step.get('expected_result', ''), None))
        
        log_qms_audit(conn, 'Work Instruction', wi_id, 'Auto-Generated', 
                     session.get('user_id'), session.get('username'))
        
        conn.commit()
        conn.close()
        
        return {'success': True, 'wi_id': wi_id, 'wi_number': wi_number}
        
    except Exception as e:
        conn.close()
        return {'success': False, 'error': str(e)}


@qms_bp.route('/api/transaction-capabilities')
@login_required
def get_transaction_capabilities():
    """API endpoint to get all transaction capabilities"""
    return jsonify(ERP_TRANSACTION_CAPABILITIES)
