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
    users = conn.execute('SELECT id, username, full_name FROM users WHERE status = ? ORDER BY username', ('active',)).fetchall()
    
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
               u1.username as prepared_by_name, u1.full_name as prepared_by_full,
               u2.username as reviewed_by_name, u2.full_name as reviewed_by_full,
               u3.username as approved_by_name, u3.full_name as approved_by_full
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
        SELECT a.*, u.username, u.full_name
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
    users = conn.execute('SELECT id, username, full_name FROM users WHERE status = ? ORDER BY username', ('active',)).fetchall()
    
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
    
    conn.close()
    
    return render_template('qms/wi_list.html',
                          work_instructions=[dict(w) for w in work_instructions],
                          modules=[m['erp_module'] for m in modules],
                          status_filter=status_filter,
                          module_filter=module_filter,
                          search=search)


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
               u1.username as prepared_by_name, u1.full_name as prepared_by_full,
               u2.username as approved_by_name, u2.full_name as approved_by_full
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
        SELECT a.*, u.username, u.full_name
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
    users = conn.execute('SELECT id, username, full_name FROM users WHERE status = ? ORDER BY username', ('active',)).fetchall()
    
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
    
    users = conn.execute('SELECT id, username, full_name FROM users WHERE status = ? ORDER BY username', ('active',)).fetchall()
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
        SELECT c.*, u1.username as assigned_to_name, u1.full_name as assigned_to_full,
               u2.username as owner_name, u2.full_name as owner_full,
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
        SELECT t.*, u.username, u.full_name,
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
    
    users = conn.execute('SELECT id, username, full_name FROM users WHERE status = ? ORDER BY username', ('active',)).fetchall()
    sops = conn.execute('SELECT id, sop_number, title FROM qms_sops ORDER BY sop_number').fetchall()
    wis = conn.execute('SELECT id, wi_number, title FROM qms_work_instructions ORDER BY wi_number').fetchall()
    
    conn.close()
    
    return render_template('qms/training_form.html',
                          record=None,
                          users=[dict(u) for u in users],
                          sops=[dict(s) for s in sops],
                          wis=[dict(w) for w in wis])
