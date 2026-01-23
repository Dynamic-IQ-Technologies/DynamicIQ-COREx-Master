from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from functools import wraps
from models import Database, AuditLogger
import uuid
import json
import os
from datetime import datetime
from werkzeug.utils import secure_filename

unplanned_receipt_bp = Blueprint('unplanned_receipt_routes', __name__)

UPLOAD_FOLDER = 'uploads/unplanned_receipts'
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'pdf', 'doc', 'docx'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def role_required(*roles):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if session.get('role') not in roles:
                flash('You do not have permission to access this page.', 'danger')
                return redirect(url_for('dashboard'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def generate_intake_number():
    db = Database()
    conn = db.get_connection()
    today = datetime.now()
    prefix = f"UPR-{today.strftime('%Y%m')}"
    result = conn.execute(
        "SELECT COUNT(*) as cnt FROM unplanned_receipts WHERE intake_number LIKE ?",
        (f"{prefix}%",)
    ).fetchone()
    count = (result['cnt'] if result else 0) + 1
    conn.close()
    return f"{prefix}-{count:04d}"

def log_audit(conn, receipt_id, action_type, description, old_status=None, new_status=None, changed_fields=None):
    conn.execute('''
        INSERT INTO unplanned_receipt_audit 
        (receipt_id, action_type, action_description, old_status, new_status, changed_fields, performed_by, ip_address, user_agent)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        receipt_id,
        action_type,
        description,
        old_status,
        new_status,
        json.dumps(changed_fields) if changed_fields else None,
        session.get('user_id'),
        request.remote_addr,
        request.headers.get('User-Agent')
    ))


@unplanned_receipt_bp.route('/unplanned-receipts')
@login_required
@role_required('Admin', 'Production Staff', 'Procurement', 'Quality', 'Receiving')
def list_receipts():
    db = Database()
    conn = db.get_connection()
    
    status_filter = request.args.get('status', '')
    classification_filter = request.args.get('classification', '')
    priority_filter = request.args.get('priority', '')
    
    query = '''
        SELECT ur.*, u.username as received_by_name,
               au.username as approved_by_name
        FROM unplanned_receipts ur
        LEFT JOIN users u ON ur.received_by = u.id
        LEFT JOIN users au ON ur.approval_authority = au.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND ur.status = ?'
        params.append(status_filter)
    if classification_filter:
        query += ' AND ur.classification = ?'
        params.append(classification_filter)
    if priority_filter:
        query += ' AND ur.priority = ?'
        params.append(priority_filter)
    
    query += ' ORDER BY ur.created_at DESC'
    
    receipts = conn.execute(query, params).fetchall()
    
    status_counts = conn.execute('''
        SELECT status, COUNT(*) as cnt FROM unplanned_receipts GROUP BY status
    ''').fetchall()
    
    conn.close()
    
    return render_template('unplanned_receipts/list.html',
                         receipts=receipts,
                         status_counts={r['status']: r['cnt'] for r in status_counts},
                         status_filter=status_filter,
                         classification_filter=classification_filter,
                         priority_filter=priority_filter)


@unplanned_receipt_bp.route('/unplanned-receipts/register', methods=['GET', 'POST'])
@login_required
@role_required('Admin', 'Production Staff', 'Procurement', 'Receiving')
def register_receipt():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        intake_number = generate_intake_number()
        
        physical_location = request.form.get('physical_location', '').strip()
        part_number = request.form.get('part_number', '').strip()
        item_description = request.form.get('item_description', '').strip()
        quantity = float(request.form.get('quantity_received', 1))
        condition = request.form.get('condition_at_receipt', 'Unknown')
        serial_numbers = request.form.get('serial_numbers', '').strip()
        classification = request.form.get('classification', 'Unknown Part')
        intake_notes = request.form.get('intake_notes', '').strip()
        priority = request.form.get('priority', 'Normal')
        
        if not physical_location or not item_description:
            flash('Physical location and item description are required.', 'danger')
            conn.close()
            return redirect(url_for('unplanned_receipt_routes.register_receipt'))
        
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO unplanned_receipts (
                intake_number, received_by, physical_location, part_number, item_description,
                quantity_received, condition_at_receipt, serial_numbers,
                classification, intake_notes, priority, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Registered')
        ''', (
            intake_number,
            session.get('user_id'),
            physical_location,
            part_number or None,
            item_description,
            quantity,
            condition,
            serial_numbers,
            classification,
            intake_notes,
            priority
        ))
        receipt_id = cursor.lastrowid
        
        log_audit(conn, receipt_id, 'Created', f'Intake record {intake_number} registered', new_status='Registered')
        
        conn.commit()
        conn.close()
        
        flash(f'Unplanned receipt {intake_number} registered successfully.', 'success')
        return redirect(url_for('unplanned_receipt_routes.view_receipt', id=receipt_id))
    
    conn.close()
    return render_template('unplanned_receipts/register.html')


@unplanned_receipt_bp.route('/unplanned-receipts/<int:id>')
@login_required
@role_required('Admin', 'Production Staff', 'Procurement', 'Quality', 'Receiving')
def view_receipt(id):
    db = Database()
    conn = db.get_connection()
    
    receipt = conn.execute('''
        SELECT ur.*, u.username as received_by_name,
               au.username as approved_by_name,
               cu.username as closed_by_name,
               p.code as product_code, p.name as product_name,
               wo.wo_number
        FROM unplanned_receipts ur
        LEFT JOIN users u ON ur.received_by = u.id
        LEFT JOIN users au ON ur.approval_authority = au.id
        LEFT JOIN users cu ON ur.closed_by = cu.id
        LEFT JOIN products p ON ur.linked_product_id = p.id
        LEFT JOIN work_orders wo ON ur.linked_work_order_id = wo.id
        WHERE ur.id = ?
    ''', (id,)).fetchone()
    
    if not receipt:
        conn.close()
        flash('Receipt not found.', 'danger')
        return redirect(url_for('unplanned_receipt_routes.list_receipts'))
    
    attachments = conn.execute('''
        SELECT ua.*, u.username as uploaded_by_name
        FROM unplanned_receipt_attachments ua
        LEFT JOIN users u ON ua.uploaded_by = u.id
        WHERE ua.receipt_id = ?
        ORDER BY ua.uploaded_at DESC
    ''', (id,)).fetchall()
    
    audit_trail = conn.execute('''
        SELECT ura.*, u.username
        FROM unplanned_receipt_audit ura
        LEFT JOIN users u ON ura.performed_by = u.id
        WHERE ura.receipt_id = ?
        ORDER BY ura.performed_at DESC
    ''', (id,)).fetchall()
    
    products = conn.execute('SELECT id, code, name FROM products ORDER BY code').fetchall()
    
    conn.close()
    
    return render_template('unplanned_receipts/view.html',
                         receipt=receipt,
                         attachments=attachments,
                         audit_trail=audit_trail,
                         products=products)


@unplanned_receipt_bp.route('/unplanned-receipts/<int:id>/update-status', methods=['POST'])
@login_required
@role_required('Admin', 'Production Staff', 'Procurement', 'Quality')
def update_status(id):
    db = Database()
    conn = db.get_connection()
    
    receipt = conn.execute('SELECT * FROM unplanned_receipts WHERE id = ?', (id,)).fetchone()
    if not receipt:
        conn.close()
        return jsonify({'success': False, 'error': 'Receipt not found'})
    
    new_status = request.form.get('status')
    notes = request.form.get('notes', '')
    
    valid_transitions = {
        'Registered': ['Under Review', 'Rejected'],
        'Under Review': ['Approved for Inventory', 'Approved for Work Order', 'Rejected', 'Registered'],
        'Approved for Inventory': ['Closed'],
        'Approved for Work Order': ['Closed'],
        'Rejected': ['Closed'],
        'Closed': []
    }
    
    old_status = receipt['status']
    if new_status not in valid_transitions.get(old_status, []):
        conn.close()
        flash(f'Invalid status transition from {old_status} to {new_status}.', 'danger')
        return redirect(url_for('unplanned_receipt_routes.view_receipt', id=id))
    
    update_fields = {'status': new_status, 'last_updated': datetime.now().isoformat()}
    
    if new_status in ['Approved for Inventory', 'Approved for Work Order']:
        update_fields['approval_authority'] = session.get('user_id')
        update_fields['approval_date'] = datetime.now().isoformat()
        update_fields['approval_notes'] = notes
        update_fields['decision_type'] = 'Convert to Inventory' if new_status == 'Approved for Inventory' else 'Create Work Order'
    elif new_status == 'Rejected':
        update_fields['rejection_reason'] = notes
    elif new_status == 'Closed':
        update_fields['closed_at'] = datetime.now().isoformat()
        update_fields['closed_by'] = session.get('user_id')
    
    set_clause = ', '.join([f'{k} = ?' for k in update_fields.keys()])
    conn.execute(f'UPDATE unplanned_receipts SET {set_clause} WHERE id = ?',
                list(update_fields.values()) + [id])
    
    log_audit(conn, id, 'Status Change', f'Status changed from {old_status} to {new_status}. {notes}',
             old_status=old_status, new_status=new_status)
    
    conn.commit()
    conn.close()
    
    flash(f'Status updated to {new_status}.', 'success')
    return redirect(url_for('unplanned_receipt_routes.view_receipt', id=id))


@unplanned_receipt_bp.route('/unplanned-receipts/<int:id>/convert-to-inventory', methods=['POST'])
@login_required
@role_required('Admin', 'Procurement')
def convert_to_inventory(id):
    db = Database()
    conn = db.get_connection()
    
    receipt = conn.execute('SELECT * FROM unplanned_receipts WHERE id = ?', (id,)).fetchone()
    if not receipt:
        conn.close()
        return jsonify({'success': False, 'error': 'Receipt not found'})
    
    if receipt['status'] != 'Approved for Inventory':
        conn.close()
        flash('Receipt must be approved for inventory conversion first.', 'danger')
        return redirect(url_for('unplanned_receipt_routes.view_receipt', id=id))
    
    product_id = request.form.get('product_id')
    create_new = request.form.get('create_new_product') == 'on'
    provisional_cost = float(request.form.get('provisional_cost', 0))
    warehouse = request.form.get('warehouse_location', 'Main')
    bin_location = request.form.get('bin_location', '')
    
    if create_new:
        new_code = f"UPR-{receipt['intake_number'].split('-')[-1]}"
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO products (code, name, description, category, base_uom)
            VALUES (?, ?, ?, 'Unplanned Receipt', 'EA')
        ''', (new_code, receipt['item_description'][:100], receipt['item_description']))
        product_id = cursor.lastrowid
    else:
        product_id = int(product_id)
    
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO inventory (
            product_id, quantity, unit_cost, condition, warehouse_location, bin_location,
            status, source, last_received_date
        ) VALUES (?, ?, ?, ?, ?, ?, 'Controlled Receipt', ?, CURRENT_DATE)
    ''', (
        product_id,
        receipt['quantity_received'],
        provisional_cost,
        receipt['condition_at_receipt'],
        warehouse,
        bin_location,
        f"Unplanned Receipt {receipt['intake_number']}"
    ))
    inventory_id = cursor.lastrowid
    
    conn.execute('''
        UPDATE unplanned_receipts 
        SET linked_product_id = ?, linked_inventory_id = ?, provisional_cost = ?,
            status = 'Closed', closed_at = CURRENT_TIMESTAMP, closed_by = ?,
            last_updated = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (product_id, inventory_id, provisional_cost, session.get('user_id'), id))
    
    log_audit(conn, id, 'Inventory Conversion', 
             f'Converted to inventory ID {inventory_id}, Product ID {product_id}',
             old_status='Approved for Inventory', new_status='Closed',
             changed_fields={'inventory_id': inventory_id, 'product_id': product_id})
    
    conn.commit()
    conn.close()
    
    flash(f'Successfully converted to inventory record #{inventory_id}.', 'success')
    return redirect(url_for('inventory_routes.view_inventory', id=inventory_id))


@unplanned_receipt_bp.route('/unplanned-receipts/<int:id>/create-work-order', methods=['POST'])
@login_required
@role_required('Admin', 'Production Staff')
def create_work_order(id):
    db = Database()
    conn = db.get_connection()
    
    receipt = conn.execute('SELECT * FROM unplanned_receipts WHERE id = ?', (id,)).fetchone()
    if not receipt:
        conn.close()
        return jsonify({'success': False, 'error': 'Receipt not found'})
    
    if receipt['status'] != 'Approved for Work Order':
        conn.close()
        flash('Receipt must be approved for work order processing first.', 'danger')
        return redirect(url_for('unplanned_receipt_routes.view_receipt', id=id))
    
    wo_type = request.form.get('work_order_type', 'Inspection')
    priority = request.form.get('priority', 'Normal')
    notes = request.form.get('notes', '')
    
    today = datetime.now()
    prefix = f"WO-{today.strftime('%Y%m')}"
    result = conn.execute(
        "SELECT COUNT(*) as cnt FROM work_orders WHERE wo_number LIKE ?",
        (f"{prefix}%",)
    ).fetchone()
    count = (result['cnt'] if result else 0) + 1
    wo_number = f"{prefix}-{count:04d}"
    
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO work_orders (
            wo_number, status, priority, workorder_type,
            notes, created_at
        ) VALUES (?, 'Draft', ?, ?, ?, CURRENT_TIMESTAMP)
    ''', (
        wo_number,
        priority,
        wo_type,
        f"Source: Unplanned Receipt {receipt['intake_number']}\nDescription: {receipt['item_description'][:200]}\nSerial Numbers: {receipt['serial_numbers'] or 'N/A'}\n{notes}"
    ))
    wo_id = cursor.lastrowid
    
    conn.execute('''
        UPDATE unplanned_receipts 
        SET linked_work_order_id = ?, last_updated = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (wo_id, id))
    
    log_audit(conn, id, 'Work Order Created', 
             f'Created work order {wo_number} (ID: {wo_id})',
             changed_fields={'work_order_id': wo_id, 'wo_number': wo_number})
    
    conn.commit()
    conn.close()
    
    flash(f'Work order {wo_number} created successfully.', 'success')
    return redirect(url_for('operations_routes.view_work_order', id=wo_id))


@unplanned_receipt_bp.route('/unplanned-receipts/<int:id>/edit', methods=['GET', 'POST'])
@login_required
@role_required('Admin', 'Production Staff', 'Procurement')
def edit_receipt(id):
    db = Database()
    conn = db.get_connection()
    
    receipt = conn.execute('SELECT * FROM unplanned_receipts WHERE id = ?', (id,)).fetchone()
    if not receipt:
        conn.close()
        flash('Receipt not found.', 'danger')
        return redirect(url_for('unplanned_receipt_routes.list_receipts'))
    
    if receipt['status'] in ['Closed', 'Approved for Inventory', 'Approved for Work Order']:
        conn.close()
        flash('Cannot edit a closed or approved receipt.', 'warning')
        return redirect(url_for('unplanned_receipt_routes.view_receipt', id=id))
    
    if request.method == 'POST':
        changes = {}
        
        fields = ['physical_location', 'part_number', 'item_description', 'quantity_received', 
                 'condition_at_receipt', 'serial_numbers', 'classification', 
                 'intake_notes', 'priority']
        
        for field in fields:
            old_val = receipt[field]
            new_val = request.form.get(field, '').strip()
            if field == 'quantity_received':
                new_val = float(new_val) if new_val else 0
            if str(old_val) != str(new_val):
                changes[field] = {'old': old_val, 'new': new_val}
        
        if changes:
            set_clause = ', '.join([f'{k} = ?' for k in changes.keys()])
            values = [v['new'] for v in changes.values()]
            conn.execute(f'UPDATE unplanned_receipts SET {set_clause}, last_updated = CURRENT_TIMESTAMP WHERE id = ?',
                        values + [id])
            
            log_audit(conn, id, 'Updated', 'Record details updated', changed_fields=changes)
            conn.commit()
            flash('Receipt updated successfully.', 'success')
        
        conn.close()
        return redirect(url_for('unplanned_receipt_routes.view_receipt', id=id))
    
    conn.close()
    return render_template('unplanned_receipts/edit.html', receipt=receipt)


@unplanned_receipt_bp.route('/api/unplanned-receipts/stats')
@login_required
def get_stats():
    db = Database()
    conn = db.get_connection()
    
    stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Registered' THEN 1 ELSE 0 END) as pending,
            SUM(CASE WHEN status = 'Under Review' THEN 1 ELSE 0 END) as under_review,
            SUM(CASE WHEN status IN ('Approved for Inventory', 'Approved for Work Order') THEN 1 ELSE 0 END) as approved,
            SUM(CASE WHEN status = 'Rejected' THEN 1 ELSE 0 END) as rejected,
            SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END) as closed,
            SUM(CASE WHEN priority = 'Urgent' AND status NOT IN ('Closed', 'Rejected') THEN 1 ELSE 0 END) as urgent
        FROM unplanned_receipts
    ''').fetchone()
    
    conn.close()
    
    return jsonify({
        'total': stats['total'] or 0,
        'pending': stats['pending'] or 0,
        'under_review': stats['under_review'] or 0,
        'approved': stats['approved'] or 0,
        'rejected': stats['rejected'] or 0,
        'closed': stats['closed'] or 0,
        'urgent': stats['urgent'] or 0
    })
