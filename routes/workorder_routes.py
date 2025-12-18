from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import Database, AuditLogger
from mrp_logic import MRPEngine
from auth import login_required, role_required
from datetime import datetime

workorder_bp = Blueprint('workorder_routes', __name__)

@workorder_bp.route('/workorders')
@login_required
def list_workorders():
    db = Database()
    conn = db.get_connection()
    
    # Get filter parameters
    status_filter = request.args.get('status', '')
    disposition_filter = request.args.get('disposition', '')
    priority_filter = request.args.get('priority', '')
    operational_status_filter = request.args.get('operational_status', '')
    customer_filter = request.args.get('customer', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    search = request.args.get('search', '')
    
    # Get sort parameters
    sort_by = request.args.get('sort_by', 'planned_start_date')
    sort_order = request.args.get('sort_order', 'DESC')
    
    # Build dynamic query
    query = '''
        SELECT wo.*, p.code, p.name, c.customer_number, c.name as customer_full_name,
               wos.name as stage_name, wos.color as stage_color
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN customers c ON wo.customer_id = c.id
        LEFT JOIN work_order_stages wos ON wo.stage_id = wos.id
        WHERE 1=1
    '''
    params = []
    
    # Apply filters
    if status_filter:
        query += ' AND wo.status = ?'
        params.append(status_filter)
    
    if disposition_filter:
        query += ' AND wo.disposition = ?'
        params.append(disposition_filter)
    
    if priority_filter:
        query += ' AND wo.priority = ?'
        params.append(priority_filter)
    
    if operational_status_filter:
        query += ' AND wo.operational_status = ?'
        params.append(operational_status_filter)
    
    if customer_filter:
        query += ' AND wo.customer_id = ?'
        params.append(int(customer_filter))
    
    if date_from:
        query += ' AND wo.planned_start_date >= ?'
        params.append(date_from)
    
    if date_to:
        query += ' AND wo.planned_start_date <= ?'
        params.append(date_to)
    
    if search:
        query += ''' AND (wo.wo_number LIKE ? OR p.code LIKE ? OR p.name LIKE ? 
                     OR c.customer_number LIKE ? OR c.name LIKE ?)'''
        search_param = f'%{search}%'
        params.extend([search_param] * 5)
    
    # Validate and apply sorting
    valid_sort_columns = {
        'wo_number': 'wo.wo_number',
        'product': 'p.code',
        'customer': 'c.customer_number',
        'quantity': 'wo.quantity',
        'disposition': 'wo.disposition',
        'status': 'wo.status',
        'operational_status': 'wo.operational_status',
        'priority': 'wo.priority',
        'planned_start_date': 'wo.planned_start_date',
        'planned_end_date': 'wo.planned_end_date'
    }
    
    sort_column = valid_sort_columns.get(sort_by, 'wo.planned_start_date')
    sort_direction = 'ASC' if sort_order.upper() == 'ASC' else 'DESC'
    query += f' ORDER BY {sort_column} {sort_direction}'
    
    workorders = conn.execute(query, params).fetchall()
    
    # Get distinct values for filter dropdowns
    customers = conn.execute('SELECT id, customer_number, name FROM customers ORDER BY customer_number').fetchall()
    statuses = conn.execute('SELECT DISTINCT status FROM work_orders WHERE status IS NOT NULL ORDER BY status').fetchall()
    dispositions = conn.execute('SELECT DISTINCT disposition FROM work_orders WHERE disposition IS NOT NULL ORDER BY disposition').fetchall()
    priorities = conn.execute('SELECT DISTINCT priority FROM work_orders WHERE priority IS NOT NULL ORDER BY priority').fetchall()
    operational_statuses = conn.execute('SELECT DISTINCT operational_status FROM work_orders WHERE operational_status IS NOT NULL ORDER BY operational_status').fetchall()
    stages = conn.execute('SELECT * FROM work_order_stages WHERE is_active = 1 ORDER BY sequence').fetchall()
    
    conn.close()
    
    return render_template('workorders/list.html', 
                         workorders=workorders,
                         customers=customers,
                         statuses=statuses,
                         dispositions=dispositions,
                         priorities=priorities,
                         operational_statuses=operational_statuses,
                         stages=stages,
                         filters={
                             'status': status_filter,
                             'disposition': disposition_filter,
                             'priority': priority_filter,
                             'operational_status': operational_status_filter,
                             'customer': customer_filter,
                             'date_from': date_from,
                             'date_to': date_to,
                             'search': search,
                             'sort_by': sort_by,
                             'sort_order': sort_order
                         })

@workorder_bp.route('/workorders/list-json')
@login_required
def list_workorders_json():
    from flask import jsonify
    db = Database()
    conn = db.get_connection()
    workorders = conn.execute('''
        SELECT wo.id, wo.wo_number, wo.status, p.code as product_code, p.name as product_name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        ORDER BY wo.planned_start_date DESC
    ''').fetchall()
    conn.close()
    return jsonify([dict(wo) for wo in workorders])

@workorder_bp.route('/workorders/create', methods=['GET', 'POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def create_workorder():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        max_attempts = 5
        wo_number = None
        wo_id = None
        
        for attempt in range(max_attempts):
            try:
                last_wo = conn.execute('''
                    SELECT wo_number FROM work_orders 
                    WHERE wo_number LIKE 'WO-%'
                    ORDER BY CAST(SUBSTR(wo_number, 4) AS INTEGER) DESC 
                    LIMIT 1
                ''').fetchone()
                
                if last_wo:
                    try:
                        last_number = int(last_wo['wo_number'].split('-')[1])
                        next_number = last_number + 1
                    except (ValueError, IndexError):
                        next_number = 1
                else:
                    next_number = 1
                
                wo_number = f'WO-{next_number:06d}'
                
                # Get customer_id and populate customer_name from customer record
                customer_id = request.form.get('customer_id')
                customer_name = None
                if customer_id:
                    customer_id = int(customer_id)
                    customer = conn.execute('SELECT name FROM customers WHERE id = ?', (customer_id,)).fetchone()
                    if customer:
                        customer_name = customer['name']
                else:
                    customer_id = None
                
                stage_id = request.form.get('stage_id')
                stage_id = int(stage_id) if stage_id else None
                
                conn.execute('''
                    INSERT INTO work_orders 
                    (wo_number, product_id, quantity, disposition, status, priority, planned_start_date, planned_end_date, labor_cost, overhead_cost, customer_id, customer_name, operational_status, stage_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    wo_number,
                    int(request.form['product_id']),
                    float(request.form['quantity']),
                    request.form.get('disposition', 'Manufacture'),
                    request.form['status'],
                    request.form.get('priority', 'Medium'),
                    request.form.get('planned_start_date'),
                    request.form.get('planned_end_date'),
                    float(request.form.get('labor_cost', 0)),
                    float(request.form.get('overhead_cost', 0)),
                    customer_id,
                    customer_name,
                    request.form.get('operational_status') or None,
                    stage_id
                ))
                
                wo_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
                
                # Log audit trail
                AuditLogger.log_change(
                    conn=conn,
                    record_type='work_order',
                    record_id=wo_id,
                    action_type='Created',
                    modified_by=session.get('user_id'),
                    ip_address=request.remote_addr,
                    user_agent=request.headers.get('User-Agent')
                )
                
                conn.commit()
                break
                
            except Exception as e:
                if 'UNIQUE constraint failed' in str(e) and attempt < max_attempts - 1:
                    conn.rollback()
                    continue
                else:
                    conn.close()
                    flash(f'Error creating work order: {str(e)}', 'danger')
                    return redirect(url_for('workorder_routes.list_workorders'))
        
        conn.close()
        
        if wo_id:
            mrp = MRPEngine()
            mrp.calculate_requirements(wo_id)
            
            flash(f'Work Order {wo_number} created successfully! Material requirements calculated.', 'success')
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        else:
            flash('Failed to create work order after multiple attempts', 'danger')
            return redirect(url_for('workorder_routes.list_workorders'))
    
    products = conn.execute('SELECT * FROM products WHERE product_type="Finished Good" ORDER BY code').fetchall()
    customers = conn.execute('SELECT * FROM customers WHERE status = "Active" ORDER BY name').fetchall()
    stages = conn.execute('SELECT * FROM work_order_stages WHERE is_active = 1 ORDER BY sequence').fetchall()
    
    last_wo = conn.execute('''
        SELECT wo_number FROM work_orders 
        WHERE wo_number LIKE 'WO-%'
        ORDER BY CAST(SUBSTR(wo_number, 4) AS INTEGER) DESC 
        LIMIT 1
    ''').fetchone()
    
    if last_wo:
        try:
            last_number = int(last_wo['wo_number'].split('-')[1])
            next_number = last_number + 1
        except (ValueError, IndexError):
            next_number = 1
    else:
        next_number = 1
    
    next_wo_number = f'WO-{next_number:06d}'
    
    conn.close()
    
    return render_template('workorders/create.html', products=products, customers=customers, stages=stages, next_wo_number=next_wo_number)

@workorder_bp.route('/workorders/<int:id>')
@login_required
def view_workorder(id):
    db = Database()
    conn = db.get_connection()
    mrp = MRPEngine()
    
    workorder = conn.execute('''
        SELECT wo.*, p.code, p.name, p.unit_of_measure, p.description as product_description,
               c.customer_number, c.name as customer_full_name, c.email as customer_email, c.phone as customer_phone,
               wos.name as stage_name, wos.color as stage_color
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN customers c ON wo.customer_id = c.id
        LEFT JOIN work_order_stages wos ON wo.stage_id = wos.id
        WHERE wo.id=?
    ''', (id,)).fetchone()
    
    requirements = conn.execute('''
        SELECT 
            mr.*, 
            p.code, 
            p.name, 
            p.unit_of_measure,
            COALESCE(
                (SELECT SUM(mi.quantity_issued) 
                 FROM material_issues mi 
                 WHERE mi.work_order_id = mr.work_order_id 
                   AND mi.product_id = mr.product_id), 0
            ) as quantity_issued
        FROM material_requirements mr
        JOIN products p ON mr.product_id = p.id
        WHERE mr.work_order_id=?
    ''', (id,)).fetchall()
    
    # Get all products for the Add Material dropdown
    all_products = conn.execute('SELECT * FROM products ORDER BY code').fetchall()
    
    # Get task summary for this work order
    task_summary = conn.execute('''
        SELECT 
            COUNT(*) as total_tasks,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed_tasks,
            SUM(CASE WHEN status = 'In Progress' THEN 1 ELSE 0 END) as in_progress_tasks,
            SUM(planned_hours) as total_planned_hours,
            SUM(actual_hours) as total_actual_hours,
            SUM(planned_labor_cost) as total_planned_labor_cost,
            SUM(actual_labor_cost) as total_actual_labor_cost
        FROM work_order_tasks
        WHERE work_order_id = ?
    ''', (id,)).fetchone()
    
    # Get all tasks for this work order
    tasks = conn.execute('''
        SELECT 
            wot.*,
            (SELECT COUNT(*) FROM labor_issuance WHERE task_id = wot.id) as labor_count
        FROM work_order_tasks wot
        WHERE wot.work_order_id = ?
        ORDER BY wot.sequence_number, wot.id
    ''', (id,)).fetchall()
    
    # Get active task templates for Apply Template feature
    task_templates = conn.execute('''
        SELECT tt.id, tt.template_code, tt.template_name, tt.category,
               (SELECT COUNT(*) FROM task_template_items WHERE template_id = tt.id) as item_count
        FROM task_templates tt
        WHERE tt.status = 'Active'
        ORDER BY tt.template_name
    ''').fetchall()
    
    cost_info = mrp.calculate_work_order_cost(id)
    
    conn.close()
    
    return render_template('workorders/view.html', 
                         workorder=workorder, 
                         requirements=requirements,
                         cost_info=cost_info,
                         all_products=all_products,
                         task_summary=task_summary,
                         tasks=tasks,
                         task_templates=task_templates)

@workorder_bp.route('/workorders/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def edit_workorder(id):
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            # Get old record for audit
            old_record = conn.execute('SELECT * FROM work_orders WHERE id=?', (id,)).fetchone()
            
            # Check if work order is completed
            if old_record['status'] == 'Completed':
                flash('Cannot edit a completed work order.', 'danger')
                conn.close()
                return redirect(url_for('workorder_routes.view_workorder', id=id))
            
            # Get customer_id and populate customer_name from customer record
            customer_id = request.form.get('customer_id')
            customer_name = None
            if customer_id:
                customer_id = int(customer_id)
                customer = conn.execute('SELECT name FROM customers WHERE id = ?', (customer_id,)).fetchone()
                if customer:
                    customer_name = customer['name']
            else:
                customer_id = None
            
            stage_id = request.form.get('stage_id')
            stage_id = int(stage_id) if stage_id else None
            
            # Get product description for auto-population if description not provided
            product_id = int(request.form['product_id'])
            description = request.form.get('description', '').strip()
            if not description:
                product = conn.execute('SELECT description FROM products WHERE id = ?', (product_id,)).fetchone()
                if product:
                    description = product['description'] or ''
            
            # Update work order
            conn.execute('''
                UPDATE work_orders 
                SET product_id = ?,
                    quantity = ?,
                    disposition = ?,
                    status = ?,
                    priority = ?,
                    serial_number = ?,
                    description = ?,
                    planned_start_date = ?,
                    planned_end_date = ?,
                    labor_cost = ?,
                    overhead_cost = ?,
                    customer_id = ?,
                    customer_name = ?,
                    operational_status = ?,
                    stage_id = ?
                WHERE id = ?
            ''', (
                product_id,
                float(request.form['quantity']),
                request.form.get('disposition', 'Manufacture'),
                request.form['status'],
                request.form.get('priority', 'Medium'),
                request.form.get('serial_number', '').strip() or None,
                description or None,
                request.form.get('planned_start_date') or None,
                request.form.get('planned_end_date') or None,
                float(request.form.get('labor_cost', 0)),
                float(request.form.get('overhead_cost', 0)),
                customer_id,
                customer_name,
                request.form.get('operational_status') or None,
                stage_id,
                id
            ))
            
            # Get new record for audit
            new_record = conn.execute('SELECT * FROM work_orders WHERE id=?', (id,)).fetchone()
            
            # Log audit trail
            changes = AuditLogger.compare_records(dict(old_record), dict(new_record))
            if changes:
                AuditLogger.log_change(
                    conn=conn,
                    record_type='work_order',
                    record_id=id,
                    action_type='Updated',
                    modified_by=session.get('user_id'),
                    changed_fields=changes,
                    ip_address=request.remote_addr,
                    user_agent=request.headers.get('User-Agent')
                )
            
            # Check if product changed (before committing)
            product_changed = old_record['product_id'] != int(request.form['product_id'])
            
            # Commit the work order changes first
            conn.commit()
            
            # Recalculate material requirements AFTER commit if product changed
            if product_changed:
                # Delete old requirements
                conn.execute('DELETE FROM material_requirements WHERE work_order_id = ?', (id,))
                conn.commit()
                
                # Calculate new requirements (MRPEngine uses its own connection)
                mrp = MRPEngine()
                mrp.calculate_requirements(id)
                
                flash('Work Order updated successfully! Material requirements recalculated.', 'success')
            else:
                flash('Work Order updated successfully!', 'success')
            
            conn.close()
            
            return redirect(url_for('workorder_routes.view_workorder', id=id))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error updating work order: {str(e)}', 'danger')
            return redirect(url_for('workorder_routes.edit_workorder', id=id))
    
    # GET request - show edit form
    workorder = conn.execute('''
        SELECT wo.*, p.code, p.name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.id=?
    ''', (id,)).fetchone()
    
    if not workorder:
        flash('Work Order not found.', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    if workorder['status'] == 'Completed':
        flash('Cannot edit a completed work order.', 'warning')
        conn.close()
        return redirect(url_for('workorder_routes.view_workorder', id=id))
    
    products = conn.execute('SELECT * FROM products WHERE product_type="Finished Good" ORDER BY code').fetchall()
    customers = conn.execute('SELECT * FROM customers WHERE status = "Active" ORDER BY name').fetchall()
    stages = conn.execute('SELECT * FROM work_order_stages WHERE is_active = 1 ORDER BY sequence').fetchall()
    
    conn.close()
    
    return render_template('workorders/edit.html', workorder=workorder, products=products, customers=customers, stages=stages)

@workorder_bp.route('/workorders/<int:id>/update-status', methods=['POST'])
@role_required('Admin', 'Production Staff')
def update_workorder_status(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        # Get old record for audit
        old_record = conn.execute('SELECT * FROM work_orders WHERE id=?', (id,)).fetchone()
        
        new_status = request.form['status']
        conn.execute('UPDATE work_orders SET status=? WHERE id=?', (new_status, id))
        
        if new_status == 'Completed':
            conn.execute('UPDATE work_orders SET actual_end_date=CURRENT_DATE WHERE id=?', (id,))
            
            # Get work order details for GL posting
            wo = conn.execute('''
                SELECT wo.*, p.name as product_name, p.code as product_code
                FROM work_orders wo
                JOIN products p ON wo.product_id = p.id
                WHERE wo.id = ?
            ''', (id,)).fetchone()
            
            # Calculate total WIP cost (Material + Labor + Overhead)
            material_cost = wo['material_cost'] or 0
            labor_cost = wo['labor_cost'] or 0
            overhead_cost = wo['overhead_cost'] or 0
            total_wip_cost = material_cost + labor_cost + overhead_cost
            
            # Only post GL entry if there are accumulated costs
            if total_wip_cost > 0:
                # Create GL entry: Transfer WIP to Finished Goods
                # DR: Finished Goods Inventory (1150)
                # CR: WIP - Work in Process (1140)
                gl_lines = [
                    {
                        'account_code': '1150',  # Finished Goods Inventory
                        'debit': total_wip_cost,
                        'credit': 0,
                        'description': f'Completed production - {wo["product_code"]} {wo["product_name"]} ({wo["wo_number"]})'
                    },
                    {
                        'account_code': '1140',  # WIP - Work in Process
                        'debit': 0,
                        'credit': total_wip_cost,
                        'description': f'WIP transferred to FG - {wo["wo_number"]}'
                    }
                ]
                
                from models import GLAutoPost
                from datetime import datetime
                
                GLAutoPost.create_auto_journal_entry(
                    conn=conn,
                    entry_date=datetime.now().strftime('%Y-%m-%d'),
                    description=f'Work Order Completion - {wo["wo_number"]}',
                    transaction_source='Work Order Completion',
                    reference_type='work_order',
                    reference_id=id,
                    lines=gl_lines,
                    created_by=session['user_id']
                )
                
                # Update finished goods inventory
                inventory = conn.execute('''
                    SELECT * FROM inventory WHERE product_id = ?
                ''', (wo['product_id'],)).fetchone()
                
                inventory_id = None
                if inventory:
                    # Update existing inventory
                    new_quantity = inventory['quantity'] + wo['quantity']
                    conn.execute('''
                        UPDATE inventory 
                        SET quantity = ?,
                            last_updated = CURRENT_TIMESTAMP
                        WHERE product_id = ?
                    ''', (new_quantity, wo['product_id']))
                    inventory_id = inventory['id']
                else:
                    # Create new inventory record
                    product = conn.execute('''
                        SELECT unit_of_measure FROM products WHERE id = ?
                    ''', (wo['product_id'],)).fetchone()
                    
                    cursor = conn.execute('''
                        INSERT INTO inventory (product_id, quantity, unit_of_measure, location)
                        VALUES (?, ?, ?, ?)
                    ''', (wo['product_id'], wo['quantity'], 
                          product['unit_of_measure'], 'Finished Goods'))
                    inventory_id = cursor.lastrowid
                
                # Link work order to created/updated inventory record
                if inventory_id:
                    conn.execute('''
                        UPDATE work_orders SET inventory_id = ? WHERE id = ?
                    ''', (inventory_id, id))
                
                # Update product cost based on actual production cost
                unit_cost = total_wip_cost / wo['quantity'] if wo['quantity'] > 0 else 0
                conn.execute('''
                    UPDATE products 
                    SET cost = ?
                    WHERE id = ?
                ''', (unit_cost, wo['product_id']))
                
                flash(f'Work Order completed! Transferred ${total_wip_cost:,.2f} from WIP to Finished Goods.', 'success')
            else:
                flash(f'Work Order status updated to {new_status}!', 'success')
                
        elif new_status == 'In Progress':
            conn.execute('UPDATE work_orders SET actual_start_date=CURRENT_DATE WHERE id=?', (id,))
            flash(f'Work Order status updated to {new_status}!', 'success')
        else:
            flash(f'Work Order status updated to {new_status}!', 'success')
        
        # Get new record for audit
        new_record = conn.execute('SELECT * FROM work_orders WHERE id=?', (id,)).fetchone()
        
        # Log audit trail
        changes = AuditLogger.compare_records(dict(old_record), dict(new_record))
        if changes:
            AuditLogger.log_change(
                conn=conn,
                record_type='work_order',
                record_id=id,
                action_type='Updated',
                modified_by=session.get('user_id'),
                changed_fields=changes,
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        flash(f'Error updating work order: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('workorder_routes.view_workorder', id=id))

@workorder_bp.route('/workorders/<int:wo_id>/materials/add', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def add_material_requirement(wo_id):
    db = Database()
    conn = db.get_connection()
    
    try:
        product_id = int(request.form['product_id'])
        required_quantity = int(request.form['required_quantity'])
        
        # Check if this material requirement already exists
        existing = conn.execute('''
            SELECT id FROM material_requirements 
            WHERE work_order_id = ? AND product_id = ?
        ''', (wo_id, product_id)).fetchone()
        
        if existing:
            flash('This material is already in the requirements list. Use Edit to update it.', 'warning')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        # Get available quantity from inventory
        inventory = conn.execute('''
            SELECT quantity FROM inventory WHERE product_id = ?
        ''', (product_id,)).fetchone()
        
        available_quantity = inventory['quantity'] if inventory else 0
        shortage_quantity = max(0, required_quantity - available_quantity)
        status = 'Satisfied' if shortage_quantity == 0 else 'Shortage'
        
        conn.execute('''
            INSERT INTO material_requirements 
            (work_order_id, product_id, required_quantity, available_quantity, shortage_quantity, status)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (wo_id, product_id, required_quantity, available_quantity, shortage_quantity, status))
        
        conn.commit()
        flash('Material requirement added successfully!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error adding material requirement: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('workorder_routes.view_workorder', id=wo_id))

@workorder_bp.route('/workorders/<int:wo_id>/materials/<int:req_id>/edit', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def edit_material_requirement(wo_id, req_id):
    db = Database()
    conn = db.get_connection()
    
    try:
        required_quantity = int(request.form['required_quantity'])
        
        # Get current material requirement
        req = conn.execute('''
            SELECT product_id FROM material_requirements WHERE id = ?
        ''', (req_id,)).fetchone()
        
        if not req:
            flash('Material requirement not found.', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        # Get available quantity from inventory
        inventory = conn.execute('''
            SELECT quantity FROM inventory WHERE product_id = ?
        ''', (req['product_id'],)).fetchone()
        
        available_quantity = inventory['quantity'] if inventory else 0
        shortage_quantity = max(0, required_quantity - available_quantity)
        status = 'Satisfied' if shortage_quantity == 0 else 'Shortage'
        
        conn.execute('''
            UPDATE material_requirements 
            SET required_quantity = ?, available_quantity = ?, shortage_quantity = ?, status = ?
            WHERE id = ?
        ''', (required_quantity, available_quantity, shortage_quantity, status, req_id))
        
        conn.commit()
        flash('Material requirement updated successfully!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error updating material requirement: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('workorder_routes.view_workorder', id=wo_id))

@workorder_bp.route('/workorders/<int:wo_id>/materials/<int:req_id>/delete', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def delete_material_requirement(wo_id, req_id):
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('DELETE FROM material_requirements WHERE id = ?', (req_id,))
        conn.commit()
        flash('Material requirement deleted successfully!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting material requirement: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('workorder_routes.view_workorder', id=wo_id))

@workorder_bp.route('/workorders/<int:wo_id>/allocate-material/<int:requirement_id>', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def allocate_material(wo_id, requirement_id):
    """Allocate material to work order"""
    db = Database()
    conn = db.get_connection()
    
    try:
        quantity_to_allocate = float(request.form.get('quantity', 0))
        
        # Get requirement details
        requirement = conn.execute('''
            SELECT mr.*, p.code, p.name
            FROM material_requirements mr
            JOIN products p ON mr.product_id = p.id
            WHERE mr.id = ? AND mr.work_order_id = ?
        ''', (requirement_id, wo_id)).fetchone()
        
        if not requirement:
            flash('Material requirement not found.', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        # Check available inventory
        inventory = conn.execute('''
            SELECT quantity FROM inventory WHERE product_id = ?
        ''', (requirement['product_id'],)).fetchone()
        
        available_qty = inventory['quantity'] if inventory else 0
        current_allocated = requirement['allocated_quantity'] or 0
        
        # Validate allocation
        if quantity_to_allocate <= 0:
            flash('Allocation quantity must be greater than zero.', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        if current_allocated + quantity_to_allocate > requirement['required_quantity']:
            flash(f'Cannot allocate more than required quantity ({requirement["required_quantity"]}).', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        if quantity_to_allocate > available_qty:
            flash(f'Insufficient inventory. Available: {available_qty}, Requested: {quantity_to_allocate}', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        # Update allocated quantity
        new_allocated_qty = current_allocated + quantity_to_allocate
        
        # Determine allocation status
        if new_allocated_qty >= requirement['required_quantity']:
            allocation_status = 'Fully Allocated'
        elif new_allocated_qty > 0:
            allocation_status = 'Partially Allocated'
        else:
            allocation_status = 'Not Allocated'
        
        # Update material requirement
        conn.execute('''
            UPDATE material_requirements
            SET allocated_quantity = ?,
                allocation_status = ?,
                allocated_by = ?,
                allocated_at = ?
            WHERE id = ?
        ''', (new_allocated_qty, allocation_status, session.get('user_id'), datetime.now(), requirement_id))
        
        # Log audit trail
        AuditLogger.log_change(
            conn=conn,
            record_type='material_allocation',
            record_id=requirement_id,
            action_type='Allocated',
            modified_by=session.get('user_id'),
            changed_fields=f'Allocated {quantity_to_allocate} units of {requirement["code"]} to WO',
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        conn.close()
        
        flash(f'Successfully allocated {quantity_to_allocate} units of {requirement["code"]}. Total allocated: {new_allocated_qty}', 'success')
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error allocating material: {str(e)}', 'danger')
    
    return redirect(url_for('workorder_routes.view_workorder', id=wo_id))

@workorder_bp.route('/workorders/<int:wo_id>/deallocate-material/<int:requirement_id>', methods=['POST'])
@role_required('Admin', 'Planner')
def deallocate_material(wo_id, requirement_id):
    """Deallocate material from work order"""
    db = Database()
    conn = db.get_connection()
    
    try:
        quantity_to_deallocate = float(request.form.get('quantity', 0))
        
        # Get requirement details
        requirement = conn.execute('''
            SELECT mr.*, p.code
            FROM material_requirements mr
            JOIN products p ON mr.product_id = p.id
            WHERE mr.id = ? AND mr.work_order_id = ?
        ''', (requirement_id, wo_id)).fetchone()
        
        if not requirement:
            flash('Material requirement not found.', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        current_allocated = requirement['allocated_quantity'] or 0
        issued_qty = requirement['issued_quantity'] or 0
        
        # Validate deallocation
        if quantity_to_deallocate <= 0:
            flash('Deallocation quantity must be greater than zero.', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        if quantity_to_deallocate > (current_allocated - issued_qty):
            flash(f'Cannot deallocate more than allocated but not issued quantity ({current_allocated - issued_qty}).', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        # Update allocated quantity
        new_allocated_qty = current_allocated - quantity_to_deallocate
        
        # Determine allocation status
        if issued_qty > 0:
            allocation_status = 'Partially Issued'
        elif new_allocated_qty >= requirement['required_quantity']:
            allocation_status = 'Fully Allocated'
        elif new_allocated_qty > 0:
            allocation_status = 'Partially Allocated'
        else:
            allocation_status = 'Not Allocated'
        
        # Update material requirement
        conn.execute('''
            UPDATE material_requirements
            SET allocated_quantity = ?,
                allocation_status = ?
            WHERE id = ?
        ''', (new_allocated_qty, allocation_status, requirement_id))
        
        # Log audit trail
        AuditLogger.log_change(
            conn=conn,
            record_type='material_allocation',
            record_id=requirement_id,
            action_type='Deallocated',
            modified_by=session.get('user_id'),
            changed_fields=f'Deallocated {quantity_to_deallocate} units of {requirement["code"]} from WO',
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        conn.close()
        
        flash(f'Successfully deallocated {quantity_to_deallocate} units of {requirement["code"]}.', 'success')
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error deallocating material: {str(e)}', 'danger')
    
    return redirect(url_for('workorder_routes.view_workorder', id=wo_id))

@workorder_bp.route('/workorders/<int:wo_id>/issue-material/<int:requirement_id>', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def issue_material(wo_id, requirement_id):
    """Issue allocated material to work order floor"""
    db = Database()
    conn = db.get_connection()
    
    try:
        quantity_to_issue = float(request.form.get('quantity', 0))
        
        # Get requirement details
        requirement = conn.execute('''
            SELECT mr.*, p.code, p.name, p.cost
            FROM material_requirements mr
            JOIN products p ON mr.product_id = p.id
            WHERE mr.id = ? AND mr.work_order_id = ?
        ''', (requirement_id, wo_id)).fetchone()
        
        if not requirement:
            flash('Material requirement not found.', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        current_allocated = requirement['allocated_quantity'] or 0
        current_issued = requirement['issued_quantity'] or 0
        
        # Validate issuance
        if quantity_to_issue <= 0:
            flash('Issue quantity must be greater than zero.', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        if quantity_to_issue > (current_allocated - current_issued):
            flash(f'Cannot issue more than allocated quantity. Allocated: {current_allocated}, Already Issued: {current_issued}', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        # Check inventory
        inventory = conn.execute('SELECT quantity FROM inventory WHERE product_id = ?', (requirement['product_id'],)).fetchone()
        available_qty = inventory['quantity'] if inventory else 0
        
        if quantity_to_issue > available_qty:
            flash(f'Insufficient inventory. Available: {available_qty}', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        # Update issued quantity
        new_issued_qty = current_issued + quantity_to_issue
        
        # Determine allocation status
        if new_issued_qty >= requirement['required_quantity']:
            allocation_status = 'Fully Issued'
        elif new_issued_qty > 0:
            allocation_status = 'Partially Issued'
        else:
            allocation_status = current_allocated >= requirement['required_quantity'] if current_allocated else 'Not Allocated'
        
        # Update material requirement
        conn.execute('''
            UPDATE material_requirements
            SET issued_quantity = ?,
                allocation_status = ?,
                issued_by = ?,
                issued_at = ?
            WHERE id = ?
        ''', (new_issued_qty, allocation_status, session.get('user_id'), datetime.now(), requirement_id))
        
        # Deduct from inventory
        conn.execute('''
            UPDATE inventory
            SET quantity = quantity - ?
            WHERE product_id = ?
        ''', (quantity_to_issue, requirement['product_id']))
        
        # Post to GL: DR WIP, CR Inventory
        material_cost = quantity_to_issue * (requirement['cost'] or 0)
        
        # DR: WIP (1140)
        conn.execute('''
            INSERT INTO general_ledger (account_id, entry_date, description, debit, credit, reference_type, reference_id, created_by)
            VALUES (?, ?, ?, ?, 0, ?, ?, ?)
        ''', (11, datetime.now().strftime('%Y-%m-%d'), 
              f'Material issued to WO-{wo_id}: {requirement["code"]}',
              material_cost, 'work_order', wo_id, session.get('user_id')))
        
        # CR: Inventory (1100)
        conn.execute('''
            INSERT INTO general_ledger (account_id, entry_date, description, debit, credit, reference_type, reference_id, created_by)
            VALUES (?, ?, ?, 0, ?, ?, ?, ?)
        ''', (1, datetime.now().strftime('%Y-%m-%d'),
              f'Material issued to WO-{wo_id}: {requirement["code"]}',
              material_cost, 'work_order', wo_id, session.get('user_id')))
        
        # Log audit trail
        AuditLogger.log_change(
            conn=conn,
            record_type='material_issuance',
            record_id=requirement_id,
            action_type='Issued',
            modified_by=session.get('user_id'),
            changed_fields=f'Issued {quantity_to_issue} units of {requirement["code"]} to WO',
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        conn.close()
        
        flash(f'Successfully issued {quantity_to_issue} units of {requirement["code"]} to work order floor.', 'success')
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error issuing material: {str(e)}', 'danger')
    
    return redirect(url_for('workorder_routes.view_workorder', id=wo_id))

@workorder_bp.route('/workorders/<int:wo_id>/return-material/<int:requirement_id>', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def return_material(wo_id, requirement_id):
    """Return issued material from work order floor back to inventory"""
    db = Database()
    conn = db.get_connection()
    
    try:
        quantity_to_return = float(request.form.get('quantity', 0))
        
        # Get requirement details
        requirement = conn.execute('''
            SELECT mr.*, p.code, p.name, p.cost
            FROM material_requirements mr
            JOIN products p ON mr.product_id = p.id
            WHERE mr.id = ? AND mr.work_order_id = ?
        ''', (requirement_id, wo_id)).fetchone()
        
        if not requirement:
            flash('Material requirement not found.', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        current_issued = requirement['issued_quantity'] or 0
        
        # Validate return
        if quantity_to_return <= 0:
            flash('Return quantity must be greater than zero.', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        if quantity_to_return > current_issued:
            flash(f'Cannot return more than issued quantity ({current_issued}).', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        # Update issued quantity
        new_issued_qty = current_issued - quantity_to_return
        current_allocated = requirement['allocated_quantity'] or 0
        
        # Determine allocation status
        if new_issued_qty > 0:
            allocation_status = 'Partially Issued'
        elif current_allocated >= requirement['required_quantity']:
            allocation_status = 'Fully Allocated'
        elif current_allocated > 0:
            allocation_status = 'Partially Allocated'
        else:
            allocation_status = 'Not Allocated'
        
        # Update material requirement
        conn.execute('''
            UPDATE material_requirements
            SET issued_quantity = ?,
                allocation_status = ?
            WHERE id = ?
        ''', (new_issued_qty, allocation_status, requirement_id))
        
        # Add back to inventory
        conn.execute('''
            UPDATE inventory
            SET quantity = quantity + ?
            WHERE product_id = ?
        ''', (quantity_to_return, requirement['product_id']))
        
        # Reverse GL posting: DR Inventory, CR WIP
        material_cost = quantity_to_return * (requirement['cost'] or 0)
        
        # DR: Inventory (1100)
        conn.execute('''
            INSERT INTO general_ledger (account_id, entry_date, description, debit, credit, reference_type, reference_id, created_by)
            VALUES (?, ?, ?, ?, 0, ?, ?, ?)
        ''', (1, datetime.now().strftime('%Y-%m-%d'),
              f'Material returned from WO-{wo_id}: {requirement["code"]}',
              material_cost, 'work_order', wo_id, session.get('user_id')))
        
        # CR: WIP (1140)
        conn.execute('''
            INSERT INTO general_ledger (account_id, entry_date, description, debit, credit, reference_type, reference_id, created_by)
            VALUES (?, ?, ?, 0, ?, ?, ?, ?)
        ''', (11, datetime.now().strftime('%Y-%m-%d'),
              f'Material returned from WO-{wo_id}: {requirement["code"]}',
              material_cost, 'work_order', wo_id, session.get('user_id')))
        
        # Log audit trail
        AuditLogger.log_change(
            conn=conn,
            record_type='material_return',
            record_id=requirement_id,
            action_type='Returned',
            modified_by=session.get('user_id'),
            changed_fields=f'Returned {quantity_to_return} units of {requirement["code"]} from WO',
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        conn.close()
        
        flash(f'Successfully returned {quantity_to_return} units of {requirement["code"]} to inventory.', 'success')
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error returning material: {str(e)}', 'danger')
    
    return redirect(url_for('workorder_routes.view_workorder', id=wo_id))

@workorder_bp.route('/workorders/<int:id>/traveler')
@login_required
def work_order_traveler(id):
    from models import CompanySettings
    db = Database()
    conn = db.get_connection()
    
    # Get work order details
    workorder = conn.execute('''
        SELECT wo.*, p.code, p.name, p.unit_of_measure, p.description
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.id=?
    ''', (id,)).fetchone()
    
    if not workorder:
        flash('Work Order not found.', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    # Get material requirements
    requirements = conn.execute('''
        SELECT 
            mr.*, 
            p.code, 
            p.name, 
            p.unit_of_measure,
            COALESCE(
                (SELECT SUM(mi.quantity_issued) 
                 FROM material_issues mi 
                 WHERE mi.work_order_id = mr.work_order_id 
                   AND mi.product_id = mr.product_id), 0
            ) as quantity_issued
        FROM material_requirements mr
        JOIN products p ON mr.product_id = p.id
        WHERE mr.work_order_id=?
        ORDER BY p.code
    ''', (id,)).fetchall()
    
    # Get all tasks for this work order
    tasks = conn.execute('''
        SELECT 
            wot.*,
            (lr.first_name || ' ' || lr.last_name) as assigned_resource_name
        FROM work_order_tasks wot
        LEFT JOIN labor_resources lr ON wot.assigned_resource_id = lr.id
        WHERE wot.work_order_id = ?
        ORDER BY wot.sequence_number, wot.id
    ''', (id,)).fetchall()
    
    # Get company settings
    company_settings = CompanySettings.get_or_create_default()
    
    conn.close()
    
    return render_template('workorders/traveler.html', 
                         workorder=workorder, 
                         requirements=requirements,
                         tasks=tasks,
                         company_settings=company_settings,
                         now=datetime.now)


@workorder_bp.route('/api/workorders/<int:id>/update-stage', methods=['POST'])
@login_required
def api_update_workorder_stage(id):
    """API endpoint to update a single work order's stage"""
    from flask import jsonify
    
    data = request.get_json()
    stage_id = data.get('stage_id')
    
    db = Database()
    conn = db.get_connection()
    
    try:
        if stage_id:
            stage_id = int(stage_id)
        else:
            stage_id = None
        
        conn.execute('UPDATE work_orders SET stage_id = ? WHERE id = ?', (stage_id, id))
        conn.commit()
        conn.close()
        
        return jsonify({'success': True})
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500


@workorder_bp.route('/api/workorders/mass-update', methods=['POST'])
@role_required('Admin', 'Planner')
def api_mass_update_workorders():
    """API endpoint to mass update multiple work orders"""
    from flask import jsonify
    
    data = request.get_json()
    wo_ids = data.get('wo_ids', [])
    updates = data.get('updates', {})
    
    if not wo_ids:
        return jsonify({'success': False, 'error': 'No work orders selected'}), 400
    
    if not updates:
        return jsonify({'success': False, 'error': 'No updates specified'}), 400
    
    db = Database()
    conn = db.get_connection()
    
    try:
        updated_count = 0
        
        for wo_id in wo_ids:
            old_wo = conn.execute('SELECT * FROM work_orders WHERE id = ?', (wo_id,)).fetchone()
            if not old_wo:
                continue
            
            update_fields = []
            update_values = []
            
            if 'status' in updates:
                update_fields.append('status = ?')
                update_values.append(updates['status'])
            
            if 'priority' in updates:
                update_fields.append('priority = ?')
                update_values.append(updates['priority'])
            
            if 'operational_status' in updates:
                update_fields.append('operational_status = ?')
                update_values.append(updates['operational_status'])
            
            if 'disposition' in updates:
                update_fields.append('disposition = ?')
                update_values.append(updates['disposition'])
            
            if 'planned_start_date' in updates:
                update_fields.append('planned_start_date = ?')
                update_values.append(updates['planned_start_date'] or None)
            
            if 'planned_end_date' in updates:
                update_fields.append('planned_end_date = ?')
                update_values.append(updates['planned_end_date'] or None)
            
            if 'stage_id' in updates:
                update_fields.append('stage_id = ?')
                update_values.append(int(updates['stage_id']) if updates['stage_id'] else None)
            
            if update_fields:
                update_values.append(wo_id)
                conn.execute(f'''
                    UPDATE work_orders 
                    SET {', '.join(update_fields)}
                    WHERE id = ?
                ''', update_values)
                
                new_wo = conn.execute('SELECT * FROM work_orders WHERE id = ?', (wo_id,)).fetchone()
                
                AuditLogger.log_change(
                    'work_orders',
                    wo_id,
                    'UPDATE',
                    session.get('user_id'),
                    dict(old_wo) if old_wo else {},
                    dict(new_wo) if new_wo else {}
                )
                
                updated_count += 1
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'updated_count': updated_count,
            'message': f'Successfully updated {updated_count} work orders'
        })
        
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500


# Work Order Stages Management Routes
@workorder_bp.route('/workorders/stages')
@role_required('Admin')
def list_stages():
    """List all work order stages"""
    db = Database()
    conn = db.get_connection()
    
    stages = conn.execute('''
        SELECT wos.*, 
               (SELECT COUNT(*) FROM work_orders WHERE stage_id = wos.id) as usage_count
        FROM work_order_stages wos
        ORDER BY wos.sequence, wos.name
    ''').fetchall()
    
    conn.close()
    return render_template('workorders/stages.html', stages=stages)


@workorder_bp.route('/workorders/stages/create', methods=['POST'])
@role_required('Admin')
def create_stage():
    """Create a new work order stage"""
    from flask import jsonify
    
    db = Database()
    conn = db.get_connection()
    
    try:
        name = request.form.get('name', '').strip()
        description = request.form.get('description', '').strip()
        color = request.form.get('color', '#6c757d')
        
        if not name:
            flash('Stage name is required', 'error')
            return redirect(url_for('workorder_routes.list_stages'))
        
        # Get next sequence number
        max_seq = conn.execute('SELECT MAX(sequence) as max_seq FROM work_order_stages').fetchone()
        sequence = (max_seq['max_seq'] or 0) + 1
        
        conn.execute('''
            INSERT INTO work_order_stages (name, description, color, sequence, is_active)
            VALUES (?, ?, ?, ?, 1)
        ''', (name, description, color, sequence))
        
        conn.commit()
        flash(f'Stage "{name}" created successfully', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error creating stage: {str(e)}', 'error')
    
    conn.close()
    return redirect(url_for('workorder_routes.list_stages'))


@workorder_bp.route('/workorders/stages/<int:id>/update', methods=['POST'])
@role_required('Admin')
def update_stage(id):
    """Update a work order stage"""
    db = Database()
    conn = db.get_connection()
    
    try:
        name = request.form.get('name', '').strip()
        description = request.form.get('description', '').strip()
        color = request.form.get('color', '#6c757d')
        sequence = request.form.get('sequence', 0)
        is_active = 1 if request.form.get('is_active') else 0
        
        if not name:
            flash('Stage name is required', 'error')
            return redirect(url_for('workorder_routes.list_stages'))
        
        conn.execute('''
            UPDATE work_order_stages 
            SET name = ?, description = ?, color = ?, sequence = ?, is_active = ?
            WHERE id = ?
        ''', (name, description, color, sequence, is_active, id))
        
        conn.commit()
        flash(f'Stage "{name}" updated successfully', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error updating stage: {str(e)}', 'error')
    
    conn.close()
    return redirect(url_for('workorder_routes.list_stages'))


@workorder_bp.route('/workorders/stages/<int:id>/delete', methods=['POST'])
@role_required('Admin')
def delete_stage(id):
    """Delete a work order stage"""
    db = Database()
    conn = db.get_connection()
    
    try:
        # Check if stage is in use
        usage = conn.execute('SELECT COUNT(*) as count FROM work_orders WHERE stage_id = ?', (id,)).fetchone()
        
        if usage['count'] > 0:
            flash(f'Cannot delete stage - it is used by {usage["count"]} work orders', 'error')
        else:
            stage = conn.execute('SELECT name FROM work_order_stages WHERE id = ?', (id,)).fetchone()
            conn.execute('DELETE FROM work_order_stages WHERE id = ?', (id,))
            conn.commit()
            flash(f'Stage "{stage["name"]}" deleted successfully', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting stage: {str(e)}', 'error')
    
    conn.close()
    return redirect(url_for('workorder_routes.list_stages'))


@workorder_bp.route('/api/workorder-stages')
@workorder_bp.route('/api/workorders/stages')
@login_required
def api_list_stages():
    """API endpoint to get all active stages"""
    from flask import jsonify
    
    db = Database()
    conn = db.get_connection()
    
    stages = conn.execute('''
        SELECT id, name, description, color, sequence
        FROM work_order_stages
        WHERE is_active = 1
        ORDER BY sequence, name
    ''').fetchall()
    
    conn.close()
    
    return jsonify({
        'success': True,
        'stages': [dict(s) for s in stages]
    })


@workorder_bp.route('/workorders/<int:id>/release-to-shipping', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def release_wo_to_shipping(id):
    """Release completed Work Order to Pending Shipments"""
    db = Database()
    conn = db.get_connection()
    
    try:
        # Get work order details with customer and product info
        wo = conn.execute('''
            SELECT wo.*, p.code as product_code, p.name as product_name,
                   c.name as customer_name, c.customer_number, c.shipping_address, c.billing_address
            FROM work_orders wo
            JOIN products p ON wo.product_id = p.id
            LEFT JOIN customers c ON wo.customer_id = c.id
            WHERE wo.id = ?
        ''', (id,)).fetchone()
        
        if not wo:
            flash('Work Order not found', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.list_workorders'))
        
        # Only Completed work orders can be released to shipping
        if wo['status'] != 'Completed':
            flash('Only Completed work orders can be released to shipping.', 'warning')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=id))
        
        # Check if already released
        existing = conn.execute('''
            SELECT id FROM shipments 
            WHERE reference_type = 'Work Order' AND reference_id = ? AND status IN ('Pending', 'Shipped')
        ''', (id,)).fetchone()
        
        if existing:
            flash('This work order has already been released to shipping.', 'warning')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=id))
        
        # Generate shipment number
        last_shipment = conn.execute(
            'SELECT shipment_number FROM shipments ORDER BY id DESC LIMIT 1'
        ).fetchone()
        
        if last_shipment and last_shipment['shipment_number']:
            try:
                last_num = int(last_shipment['shipment_number'].split('-')[1])
                shipment_number = f'SHIP-{last_num + 1:05d}'
            except:
                shipment_number = 'SHIP-00001'
        else:
            shipment_number = 'SHIP-00001'
        
        # Get ship-to info from customer or work order
        ship_to_name = wo['customer_name'] or ''
        ship_to_address = wo['shipping_address'] or wo['billing_address'] or ''
        
        # Create pending shipment record
        conn.execute('''
            INSERT INTO shipments (
                shipment_number, shipment_type, reference_type, reference_id,
                status, shipment_stage, ship_to_name, ship_to_address,
                released_by, released_at, created_by, created_at
            ) VALUES (?, 'Outbound', 'Work Order', ?, 'Pending', 'Pending',
                      ?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP)
        ''', (
            shipment_number, id,
            ship_to_name, ship_to_address,
            session.get('user_id'), session.get('user_id')
        ))
        
        # Update work order disposition to indicate released to shipping
        conn.execute('''
            UPDATE work_orders 
            SET disposition = 'Released to Shipping'
            WHERE id = ?
        ''', (id,))
        
        # Log activity
        from models import AuditLogger
        AuditLogger.log_change(
            conn=conn,
            record_type='work_orders',
            record_id=id,
            action_type='UPDATE',
            modified_by=session.get('user_id'),
            changed_fields={'disposition': {'old': wo['disposition'], 'new': 'Released to Shipping'}},
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        flash(f'Work Order {wo["wo_number"]} released to shipping! Shipment {shipment_number} created.', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error releasing to shipping: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('workorder_routes.view_workorder', id=id))


@workorder_bp.route('/workorders/<int:id>/turn-into-stock', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def turn_into_stock(id):
    """Explicitly turn completed Work Order into stock (updates disposition)"""
    db = Database()
    conn = db.get_connection()
    
    try:
        # Get work order details
        wo = conn.execute('''
            SELECT wo.*, p.code as product_code, p.name as product_name
            FROM work_orders wo
            JOIN products p ON wo.product_id = p.id
            WHERE wo.id = ?
        ''', (id,)).fetchone()
        
        if not wo:
            flash('Work Order not found', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.list_workorders'))
        
        # Only Completed work orders can be turned into stock
        if wo['status'] != 'Completed':
            flash('Only Completed work orders can be turned into stock.', 'warning')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=id))
        
        # Check if already in stock
        if wo['disposition'] == 'Turned into Stock':
            flash('This work order has already been turned into stock.', 'info')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=id))
        
        # Update disposition
        old_disposition = wo['disposition']
        conn.execute('''
            UPDATE work_orders 
            SET disposition = 'Turned into Stock'
            WHERE id = ?
        ''', (id,))
        
        # Log activity
        from models import AuditLogger
        AuditLogger.log_change(
            conn=conn,
            record_type='work_orders',
            record_id=id,
            action_type='UPDATE',
            modified_by=session.get('user_id'),
            changed_fields={'disposition': {'old': old_disposition, 'new': 'Turned into Stock'}},
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        flash(f'Work Order {wo["wo_number"]} marked as turned into stock. Product {wo["product_code"]} added to finished goods inventory.', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error updating work order: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('workorder_routes.view_workorder', id=id))


@workorder_bp.route('/workorders/<int:id>/generate-8130', methods=['GET', 'POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def generate_8130(id):
    """Generate FAA Form 8130-3 for completed Work Order"""
    db = Database()
    conn = db.get_connection()
    
    # Get work order details
    wo = conn.execute('''
        SELECT wo.*, p.code as product_code, p.name as product_name,
               c.name as customer_name,
               cs.company_name, cs.address_line1 as company_address, 
               cs.city as company_city, cs.state as company_state, cs.postal_code as company_zip
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN customers c ON wo.customer_id = c.id
        LEFT JOIN company_settings cs ON cs.id = 1
        WHERE wo.id = ?
    ''', (id,)).fetchone()
    
    if not wo:
        flash('Work Order not found', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    if wo['status'] != 'Completed':
        flash('Only completed work orders can have 8130 certificates generated.', 'warning')
        conn.close()
        return redirect(url_for('workorder_routes.view_workorder', id=id))
    
    # Check for existing certificate
    existing_cert = conn.execute('''
        SELECT * FROM faa_8130_certificates 
        WHERE work_order_id = ? AND status = 'Issued'
    ''', (id,)).fetchone()
    
    if request.method == 'POST':
        if existing_cert:
            flash(f'Certificate {existing_cert["certificate_number"]} already exists for this work order.', 'warning')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=id))
        
        try:
            from services.faa8130_service import FAA8130Service
            
            form_data = {
                'issuing_authority': request.form.get('issuing_authority', 'FAA / United States'),
                'organization_name': request.form.get('organization_name', ''),
                'organization_address': request.form.get('organization_address', ''),
                'serial_number': request.form.get('serial_number', ''),
                'batch_number': request.form.get('batch_number', ''),
                'status_work': request.form.get('status_work', wo['disposition'] or 'Overhauled'),
                'approval_number': request.form.get('approval_number', ''),
                'remarks': request.form.get('remarks', ''),
                'certifier_name': request.form.get('certifier_name', ''),
                'certifier_certificate_number': request.form.get('certifier_certificate_number', ''),
                'certifier_signature_date': request.form.get('certifier_signature_date', datetime.now().strftime('%Y-%m-%d')),
                'authorized_signature_name': request.form.get('authorized_signature_name', ''),
                'authorized_signature_date': request.form.get('authorized_signature_date', datetime.now().strftime('%Y-%m-%d')),
            }
            
            result = FAA8130Service.create_certificate(conn, id, form_data, session.get('user_id'))
            conn.commit()
            
            flash(f'FAA Form 8130-3 Certificate {result["certificate_number"]} generated successfully!', 'success')
            conn.close()
            return redirect(url_for('workorder_routes.view_8130', id=id))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error generating certificate: {str(e)}', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.generate_8130', id=id))
    
    # Build organization address from company settings
    company = conn.execute('SELECT * FROM company_settings LIMIT 1').fetchone()
    org_address = ''
    if company and company['address_line1']:
        org_address = f"{company['address_line1']}, {company['city'] or ''}, {company['state'] or ''} {company['postal_code'] or ''}"
    
    conn.close()
    return render_template('workorders/generate_8130.html', 
                          workorder=wo,
                          existing_cert=existing_cert,
                          org_address=org_address)


@workorder_bp.route('/workorders/<int:id>/view-8130')
@login_required
def view_8130(id):
    """View existing FAA Form 8130-3 certificate for Work Order"""
    db = Database()
    conn = db.get_connection()
    
    # Get work order details
    wo = conn.execute('''
        SELECT wo.*, p.code as product_code, p.name as product_name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.id = ?
    ''', (id,)).fetchone()
    
    if not wo:
        flash('Work Order not found', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    # Get certificate
    certificate = conn.execute('''
        SELECT c.*, u.username as created_by_name
        FROM faa_8130_certificates c
        LEFT JOIN users u ON c.created_by = u.id
        WHERE c.work_order_id = ? AND c.status = 'Issued'
        ORDER BY c.created_at DESC
        LIMIT 1
    ''', (id,)).fetchone()
    
    conn.close()
    
    if not certificate:
        flash('No 8130 certificate found for this work order.', 'warning')
        return redirect(url_for('workorder_routes.generate_8130', id=id))
    
    return render_template('workorders/view_8130.html', 
                          workorder=wo,
                          certificate=certificate)


@workorder_bp.route('/workorders/<int:id>/download-8130')
@login_required
def download_8130(id):
    """Download the 8130 PDF file"""
    from flask import send_file
    
    db = Database()
    conn = db.get_connection()
    
    certificate = conn.execute('''
        SELECT * FROM faa_8130_certificates 
        WHERE work_order_id = ? AND status = 'Issued'
        ORDER BY created_at DESC LIMIT 1
    ''', (id,)).fetchone()
    
    conn.close()
    
    if not certificate or not certificate['pdf_file_path']:
        flash('Certificate PDF not found.', 'danger')
        return redirect(url_for('workorder_routes.view_workorder', id=id))
    
    import os
    if os.path.exists(certificate['pdf_file_path']):
        return send_file(
            certificate['pdf_file_path'],
            as_attachment=True,
            download_name=f"{certificate['certificate_number']}.pdf"
        )
    else:
        flash('Certificate PDF file not found on server.', 'danger')
        return redirect(url_for('workorder_routes.view_workorder', id=id))
