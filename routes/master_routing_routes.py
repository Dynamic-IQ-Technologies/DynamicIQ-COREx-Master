from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database, AuditLogger
from auth import login_required, role_required
from datetime import datetime

master_routing_bp = Blueprint('master_routing_routes', __name__)

ROUTING_TYPES = ['Manufacturing', 'Repair', 'Inspection', 'Overhaul', 'Calibration']
ROUTING_STATUSES = ['Draft', 'Under Review', 'Approved', 'Active', 'Obsolete']
OPERATION_TYPES = ['Build', 'Disassembly', 'Inspection', 'Test', 'Repair', 'Reassembly', 'Setup', 'Cleanup']
INSPECTION_TYPES = ['In-Process', 'Final', 'First Article', 'Receiving', 'Source']

@master_routing_bp.route('/master-routings')
@login_required
def list_routings():
    db = Database()
    conn = db.get_connection()
    
    status_filter = request.args.get('status', '')
    type_filter = request.args.get('type', '')
    search = request.args.get('search', '')
    
    query = '''
        SELECT mr.*, p.code as product_code, p.name as product_name,
               u.username as created_by_name, ua.username as approved_by_name,
               (SELECT COUNT(*) FROM master_routing_operations WHERE routing_id = mr.id) as operation_count
        FROM master_routings mr
        LEFT JOIN products p ON mr.product_id = p.id
        LEFT JOIN users u ON mr.created_by = u.id
        LEFT JOIN users ua ON mr.approved_by = ua.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND mr.status = ?'
        params.append(status_filter)
    
    if type_filter:
        query += ' AND mr.routing_type = ?'
        params.append(type_filter)
    
    if search:
        query += ' AND (mr.routing_code LIKE ? OR mr.routing_name LIKE ? OR p.code LIKE ?)'
        params.extend([f'%{search}%', f'%{search}%', f'%{search}%'])
    
    query += ' ORDER BY mr.created_at DESC'
    
    routings = conn.execute(query, params).fetchall()
    conn.close()
    
    return render_template('master_routings/list.html', 
                         routings=routings,
                         routing_types=ROUTING_TYPES,
                         routing_statuses=ROUTING_STATUSES,
                         status_filter=status_filter,
                         type_filter=type_filter,
                         search=search)

def generate_routing_code(conn):
    """Generate next routing code in sequence (MR-000001, MR-000002, etc.)"""
    last_routing = conn.execute('''
        SELECT routing_code FROM master_routings 
        WHERE routing_code LIKE 'MR-%'
        ORDER BY CAST(SUBSTR(routing_code, 4) AS INTEGER) DESC 
        LIMIT 1
    ''').fetchone()
    
    if last_routing:
        try:
            last_number = int(last_routing['routing_code'].split('-')[1])
            next_number = last_number + 1
        except (ValueError, IndexError):
            next_number = 1
    else:
        next_number = 1
    
    return f'MR-{next_number:06d}'

@master_routing_bp.route('/master-routings/create', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def create_routing():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        routing_code = generate_routing_code(conn)
        routing_name = request.form.get('routing_name', '').strip()
        description = request.form.get('description', '').strip()
        routing_type = request.form.get('routing_type', 'Manufacturing')
        product_id = request.form.get('product_id') or None
        product_category = request.form.get('product_category', '').strip() or None
        default_work_order_type = request.form.get('default_work_order_type', 'Production')
        regulatory_basis = request.form.get('regulatory_basis', '').strip() or None
        effective_date = request.form.get('effective_date') or None
        
        if not routing_name:
            flash('Routing Name is required.', 'danger')
            conn.close()
            return redirect(url_for('master_routing_routes.create_routing'))
        
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO master_routings (
                routing_code, routing_name, description, routing_type, product_id,
                product_category, default_work_order_type, regulatory_basis, effective_date,
                created_by, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Draft')
        ''', (routing_code, routing_name, description, routing_type, product_id,
              product_category, default_work_order_type, regulatory_basis, effective_date,
              session.get('user_id')))
        
        routing_id = cursor.lastrowid
        
        AuditLogger.log_change(
            conn=conn,
            record_type='master_routing',
            record_id=routing_id,
            action_type='Created',
            modified_by=session['user_id'],
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        conn.close()
        
        flash(f'Master Routing {routing_code} created successfully.', 'success')
        return redirect(url_for('master_routing_routes.view_routing', id=routing_id))
    
    products = conn.execute('SELECT id, code, name FROM products ORDER BY code').fetchall()
    next_routing_code = generate_routing_code(conn)
    conn.close()
    
    return render_template('master_routings/create.html',
                         routing_types=ROUTING_TYPES,
                         products=products,
                         next_routing_code=next_routing_code)

@master_routing_bp.route('/master-routings/<int:id>')
@login_required
def view_routing(id):
    db = Database()
    conn = db.get_connection()
    
    routing = conn.execute('''
        SELECT mr.*, p.code as product_code, p.name as product_name,
               u.username as created_by_name, ua.username as approved_by_name
        FROM master_routings mr
        LEFT JOIN products p ON mr.product_id = p.id
        LEFT JOIN users u ON mr.created_by = u.id
        LEFT JOIN users ua ON mr.approved_by = ua.id
        WHERE mr.id = ?
    ''', (id,)).fetchone()
    
    if not routing:
        flash('Master Routing not found.', 'danger')
        conn.close()
        return redirect(url_for('master_routing_routes.list_routings'))
    
    operations = conn.execute('''
        SELECT mro.*, wc.name as work_center_name
        FROM master_routing_operations mro
        LEFT JOIN work_centers wc ON mro.work_center_id = wc.id
        WHERE mro.routing_id = ?
        ORDER BY mro.sequence_number
    ''', (id,)).fetchall()
    
    operation_details = []
    for op in operations:
        materials = conn.execute('''
            SELECT mrm.*, p.code as product_code, p.name as product_name
            FROM master_routing_materials mrm
            JOIN products p ON mrm.product_id = p.id
            WHERE mrm.operation_id = ?
        ''', (op['id'],)).fetchall()
        
        quality_checks = conn.execute('''
            SELECT * FROM master_routing_quality_checks WHERE operation_id = ?
        ''', (op['id'],)).fetchall()
        
        operation_details.append({
            'operation': op,
            'materials': materials,
            'quality_checks': quality_checks
        })
    
    work_centers = conn.execute('SELECT id, name FROM work_centers ORDER BY name').fetchall()
    products = conn.execute('SELECT id, code, name FROM products ORDER BY code').fetchall()
    
    conn.close()
    
    return render_template('master_routings/view.html',
                         routing=routing,
                         operation_details=operation_details,
                         work_centers=work_centers,
                         products=products,
                         operation_types=OPERATION_TYPES,
                         inspection_types=INSPECTION_TYPES,
                         routing_statuses=ROUTING_STATUSES)

@master_routing_bp.route('/master-routings/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def edit_routing(id):
    db = Database()
    conn = db.get_connection()
    
    routing = conn.execute('SELECT * FROM master_routings WHERE id = ?', (id,)).fetchone()
    if not routing:
        flash('Master Routing not found.', 'danger')
        conn.close()
        return redirect(url_for('master_routing_routes.list_routings'))
    
    if routing['status'] in ['Active', 'Obsolete']:
        flash('Cannot edit Active or Obsolete routings. Create a new revision instead.', 'warning')
        conn.close()
        return redirect(url_for('master_routing_routes.view_routing', id=id))
    
    if request.method == 'POST':
        routing_name = request.form.get('routing_name', '').strip()
        description = request.form.get('description', '').strip()
        routing_type = request.form.get('routing_type', 'Manufacturing')
        product_id = request.form.get('product_id') or None
        product_category = request.form.get('product_category', '').strip() or None
        default_work_order_type = request.form.get('default_work_order_type', 'Production')
        regulatory_basis = request.form.get('regulatory_basis', '').strip() or None
        effective_date = request.form.get('effective_date') or None
        
        conn.execute('''
            UPDATE master_routings SET
                routing_name = ?, description = ?, routing_type = ?, product_id = ?,
                product_category = ?, default_work_order_type = ?, regulatory_basis = ?,
                effective_date = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (routing_name, description, routing_type, product_id, product_category,
              default_work_order_type, regulatory_basis, effective_date, id))
        
        conn.commit()
        conn.close()
        
        flash('Master Routing updated successfully.', 'success')
        return redirect(url_for('master_routing_routes.view_routing', id=id))
    
    products = conn.execute('SELECT id, code, name FROM products ORDER BY code').fetchall()
    conn.close()
    
    return render_template('master_routings/edit.html',
                         routing=routing,
                         routing_types=ROUTING_TYPES,
                         products=products)

@master_routing_bp.route('/master-routings/<int:id>/add-operation', methods=['POST'])
@role_required('Admin', 'Planner')
def add_operation(id):
    db = Database()
    conn = db.get_connection()
    
    routing = conn.execute('SELECT status FROM master_routings WHERE id = ?', (id,)).fetchone()
    if not routing or routing['status'] in ['Active', 'Obsolete']:
        flash('Cannot modify this routing.', 'danger')
        conn.close()
        return redirect(url_for('master_routing_routes.view_routing', id=id))
    
    operation_name = request.form.get('operation_name', '').strip()
    operation_code = request.form.get('operation_code', '').strip() or None
    description = request.form.get('description', '').strip() or None
    instructions = request.form.get('instructions', '').strip() or None
    operation_type = request.form.get('operation_type', 'Build')
    work_center_id = request.form.get('work_center_id') or None
    department = request.form.get('department', '').strip() or None
    standard_labor_hours = float(request.form.get('standard_labor_hours', 0) or 0)
    setup_time = float(request.form.get('setup_time', 0) or 0)
    run_time = float(request.form.get('run_time', 0) or 0)
    skill_required = request.form.get('skill_required', '').strip() or None
    certification_required = request.form.get('certification_required', '').strip() or None
    tooling_required = request.form.get('tooling_required', '').strip() or None
    is_mandatory = 1 if request.form.get('is_mandatory') else 0
    is_inspection_gate = 1 if request.form.get('is_inspection_gate') else 0
    
    max_seq = conn.execute('SELECT COALESCE(MAX(sequence_number), 0) as max_seq FROM master_routing_operations WHERE routing_id = ?', (id,)).fetchone()['max_seq']
    sequence_number = max_seq + 10
    
    conn.execute('''
        INSERT INTO master_routing_operations (
            routing_id, sequence_number, operation_code, operation_name, description,
            instructions, operation_type, work_center_id, department, standard_labor_hours,
            setup_time, run_time, skill_required, certification_required, tooling_required,
            is_mandatory, is_inspection_gate
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (id, sequence_number, operation_code, operation_name, description, instructions,
          operation_type, work_center_id, department, standard_labor_hours, setup_time,
          run_time, skill_required, certification_required, tooling_required,
          is_mandatory, is_inspection_gate))
    
    conn.commit()
    conn.close()
    
    flash('Operation added successfully.', 'success')
    return redirect(url_for('master_routing_routes.view_routing', id=id))

@master_routing_bp.route('/master-routings/<int:id>/operations/<int:op_id>/delete', methods=['POST'])
@role_required('Admin', 'Planner')
def delete_operation(id, op_id):
    db = Database()
    conn = db.get_connection()
    
    routing = conn.execute('SELECT status FROM master_routings WHERE id = ?', (id,)).fetchone()
    if not routing or routing['status'] in ['Active', 'Obsolete']:
        flash('Cannot modify this routing.', 'danger')
        conn.close()
        return redirect(url_for('master_routing_routes.view_routing', id=id))
    
    conn.execute('DELETE FROM master_routing_operations WHERE id = ? AND routing_id = ?', (op_id, id))
    conn.commit()
    conn.close()
    
    flash('Operation deleted.', 'success')
    return redirect(url_for('master_routing_routes.view_routing', id=id))

@master_routing_bp.route('/master-routings/<int:id>/operations/<int:op_id>/add-material', methods=['POST'])
@role_required('Admin', 'Planner')
def add_material(id, op_id):
    db = Database()
    conn = db.get_connection()
    
    product_id = request.form.get('product_id')
    quantity_required = float(request.form.get('quantity_required', 1) or 1)
    scrap_percentage = float(request.form.get('scrap_percentage', 0) or 0)
    issue_method = request.form.get('issue_method', 'Manual')
    notes = request.form.get('notes', '').strip() or None
    
    conn.execute('''
        INSERT INTO master_routing_materials (operation_id, product_id, quantity_required, scrap_percentage, issue_method, notes)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (op_id, product_id, quantity_required, scrap_percentage, issue_method, notes))
    
    conn.commit()
    conn.close()
    
    flash('Material added to operation.', 'success')
    return redirect(url_for('master_routing_routes.view_routing', id=id))

@master_routing_bp.route('/master-routings/<int:id>/materials/<int:mat_id>/delete', methods=['POST'])
@role_required('Admin', 'Planner')
def delete_material(id, mat_id):
    db = Database()
    conn = db.get_connection()
    conn.execute('DELETE FROM master_routing_materials WHERE id = ?', (mat_id,))
    conn.commit()
    conn.close()
    
    flash('Material removed.', 'success')
    return redirect(url_for('master_routing_routes.view_routing', id=id))

@master_routing_bp.route('/master-routings/<int:id>/operations/<int:op_id>/add-quality-check', methods=['POST'])
@role_required('Admin', 'Planner')
def add_quality_check(id, op_id):
    db = Database()
    conn = db.get_connection()
    
    check_name = request.form.get('check_name', '').strip()
    check_code = request.form.get('check_code', '').strip() or None
    description = request.form.get('description', '').strip() or None
    inspection_type = request.form.get('inspection_type', 'In-Process')
    acceptance_criteria = request.form.get('acceptance_criteria', '').strip() or None
    required_signoff_role = request.form.get('required_signoff_role', '').strip() or None
    is_mandatory = 1 if request.form.get('is_mandatory') else 0
    
    conn.execute('''
        INSERT INTO master_routing_quality_checks (operation_id, check_name, check_code, description, inspection_type, acceptance_criteria, required_signoff_role, is_mandatory)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (op_id, check_name, check_code, description, inspection_type, acceptance_criteria, required_signoff_role, is_mandatory))
    
    conn.commit()
    conn.close()
    
    flash('Quality check added.', 'success')
    return redirect(url_for('master_routing_routes.view_routing', id=id))

@master_routing_bp.route('/master-routings/<int:id>/quality-checks/<int:qc_id>/delete', methods=['POST'])
@role_required('Admin', 'Planner')
def delete_quality_check(id, qc_id):
    db = Database()
    conn = db.get_connection()
    conn.execute('DELETE FROM master_routing_quality_checks WHERE id = ?', (qc_id,))
    conn.commit()
    conn.close()
    
    flash('Quality check removed.', 'success')
    return redirect(url_for('master_routing_routes.view_routing', id=id))

@master_routing_bp.route('/master-routings/<int:id>/update-status', methods=['POST'])
@role_required('Admin', 'Planner')
def update_status(id):
    db = Database()
    conn = db.get_connection()
    
    routing = conn.execute('SELECT * FROM master_routings WHERE id = ?', (id,)).fetchone()
    if not routing:
        flash('Routing not found.', 'danger')
        conn.close()
        return redirect(url_for('master_routing_routes.list_routings'))
    
    new_status = request.form.get('status')
    old_status = routing['status']
    
    valid_transitions = {
        'Draft': ['Under Review', 'Obsolete'],
        'Under Review': ['Draft', 'Approved'],
        'Approved': ['Active', 'Draft'],
        'Active': ['Obsolete'],
        'Obsolete': []
    }
    
    if new_status not in valid_transitions.get(old_status, []):
        flash(f'Cannot transition from {old_status} to {new_status}.', 'danger')
        conn.close()
        return redirect(url_for('master_routing_routes.view_routing', id=id))
    
    update_fields = {'status': new_status}
    
    if new_status == 'Approved':
        op_count = conn.execute('SELECT COUNT(*) as cnt FROM master_routing_operations WHERE routing_id = ?', (id,)).fetchone()['cnt']
        if op_count == 0:
            flash('Cannot approve routing without any operations.', 'warning')
            conn.close()
            return redirect(url_for('master_routing_routes.view_routing', id=id))
        
        conn.execute('''
            UPDATE master_routings SET status = ?, approved_by = ?, approved_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (new_status, session.get('user_id'), id))
    else:
        conn.execute('UPDATE master_routings SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?', (new_status, id))
    
    AuditLogger.log_change(
        conn=conn,
        record_type='master_routing',
        record_id=id,
        action_type=f'Status Changed: {old_status} → {new_status}',
        modified_by=session['user_id'],
        ip_address=request.remote_addr,
        user_agent=request.headers.get('User-Agent')
    )
    
    conn.commit()
    conn.close()
    
    flash(f'Routing status updated to {new_status}.', 'success')
    return redirect(url_for('master_routing_routes.view_routing', id=id))

@master_routing_bp.route('/master-routings/<int:id>/apply-to-workorder/<int:wo_id>', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def apply_to_workorder(id, wo_id):
    db = Database()
    conn = db.get_connection()
    
    routing = conn.execute('SELECT * FROM master_routings WHERE id = ? AND status IN (?, ?)', (id, 'Approved', 'Active')).fetchone()
    if not routing:
        flash('Only approved or active routings can be applied.', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
    
    operations = conn.execute('''
        SELECT * FROM master_routing_operations WHERE routing_id = ? ORDER BY sequence_number
    ''', (id,)).fetchall()
    
    wo = conn.execute('SELECT wo_number FROM work_orders WHERE id = ?', (wo_id,)).fetchone()
    
    for op in operations:
        task_number = f"{wo['wo_number']}-T{op['sequence_number']:03d}"
        
        existing = conn.execute('SELECT id FROM work_order_tasks WHERE task_number = ?', (task_number,)).fetchone()
        if existing:
            continue
        
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO work_order_tasks (
                task_number, work_order_id, task_name, description, category,
                sequence_number, priority, planned_hours, work_center_id, status
            ) VALUES (?, ?, ?, ?, ?, ?, 'Medium', ?, ?, 'Not Started')
        ''', (task_number, wo_id, op['operation_name'], op['description'] or op['instructions'],
              op['operation_type'], op['sequence_number'], op['standard_labor_hours'], op['work_center_id']))
        
        task_id = cursor.lastrowid
        
        materials = conn.execute('SELECT * FROM master_routing_materials WHERE operation_id = ?', (op['id'],)).fetchall()
        for mat in materials:
            conn.execute('''
                INSERT INTO work_order_task_materials (task_id, product_id, required_qty, status)
                VALUES (?, ?, ?, 'Pending')
            ''', (task_id, mat['product_id'], mat['quantity_required']))
    
    conn.execute('UPDATE work_orders SET master_routing_id = ? WHERE id = ?', (id, wo_id))
    
    conn.commit()
    conn.close()
    
    flash(f'Applied {len(operations)} operations from routing to work order.', 'success')
    return redirect(url_for('workorder_routes.view_workorder', id=wo_id))

@master_routing_bp.route('/api/master-routings/for-product/<int:product_id>')
@login_required
def get_routings_for_product(product_id):
    db = Database()
    conn = db.get_connection()
    
    routings = conn.execute('''
        SELECT id, routing_code, routing_name, routing_type, revision, status
        FROM master_routings
        WHERE (product_id = ? OR product_id IS NULL) AND status IN ('Approved', 'Active')
        ORDER BY product_id DESC, routing_code
    ''', (product_id,)).fetchall()
    
    conn.close()
    return jsonify([dict(r) for r in routings])


def apply_routing_to_work_order(conn, wo_id, routing_id=None, product_id=None):
    """
    Apply master routing operations and materials to a work order.
    If routing_id is provided, use that specific routing.
    If only product_id is provided, find the best matching active/approved routing for the product.
    Returns the routing_id used, or None if no routing was applied.
    """
    if not routing_id and product_id:
        routing = conn.execute('''
            SELECT id FROM master_routings
            WHERE product_id = ? AND status IN ('Active', 'Approved')
            ORDER BY CASE status WHEN 'Active' THEN 1 WHEN 'Approved' THEN 2 END, revision DESC
            LIMIT 1
        ''', (product_id,)).fetchone()
        if not routing:
            routing = conn.execute('''
                SELECT id FROM master_routings
                WHERE product_id IS NULL AND status IN ('Active', 'Approved')
                ORDER BY CASE status WHEN 'Active' THEN 1 WHEN 'Approved' THEN 2 END, routing_code
                LIMIT 1
            ''').fetchone()
        if routing:
            routing_id = routing['id']
    
    if not routing_id:
        return None
    
    wo = conn.execute('SELECT wo_number FROM work_orders WHERE id = ?', (wo_id,)).fetchone()
    if not wo:
        return None
    
    operations = conn.execute('''
        SELECT mro.*, wc.name as work_center_name
        FROM master_routing_operations mro
        LEFT JOIN work_centers wc ON mro.work_center_id = wc.id
        WHERE mro.routing_id = ?
        ORDER BY mro.sequence_number
    ''', (routing_id,)).fetchall()
    
    for op in operations:
        task_number = f"{wo['wo_number']}-T{op['sequence_number']:03d}"
        
        existing = conn.execute('SELECT id FROM work_order_tasks WHERE task_number = ?', (task_number,)).fetchone()
        if existing:
            continue
        
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO work_order_tasks (
                task_number, work_order_id, task_name, description, category,
                sequence_number, priority, planned_hours, work_center_id, status
            ) VALUES (?, ?, ?, ?, ?, ?, 'Medium', ?, ?, 'Not Started')
        ''', (task_number, wo_id, op['operation_name'], op['description'] or op['instructions'],
              op['operation_type'], op['sequence_number'], op['standard_labor_hours'], op['work_center_id']))
        
        task_id = cursor.lastrowid
        
        materials = conn.execute('SELECT * FROM master_routing_materials WHERE operation_id = ?', (op['id'],)).fetchall()
        for mat in materials:
            conn.execute('''
                INSERT INTO work_order_task_materials (task_id, product_id, required_qty, status)
                VALUES (?, ?, ?, 'Pending')
            ''', (task_id, mat['product_id'], mat['quantity_required']))
    
    conn.execute('UPDATE work_orders SET master_routing_id = ? WHERE id = ?', (routing_id, wo_id))
    
    return routing_id
