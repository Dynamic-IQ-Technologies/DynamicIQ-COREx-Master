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
    workorders = conn.execute('''
        SELECT wo.*, p.code, p.name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        ORDER BY wo.planned_start_date DESC
    ''').fetchall()
    conn.close()
    return render_template('workorders/list.html', workorders=workorders)

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
                
                conn.execute('''
                    INSERT INTO work_orders 
                    (wo_number, product_id, quantity, disposition, status, priority, planned_start_date, planned_end_date, labor_cost, overhead_cost, customer_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    request.form.get('customer_name', '')
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
    
    return render_template('workorders/create.html', products=products, next_wo_number=next_wo_number)

@workorder_bp.route('/workorders/<int:id>')
@login_required
def view_workorder(id):
    db = Database()
    conn = db.get_connection()
    mrp = MRPEngine()
    
    workorder = conn.execute('''
        SELECT wo.*, p.code, p.name, p.unit_of_measure
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
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
    
    cost_info = mrp.calculate_work_order_cost(id)
    
    conn.close()
    
    return render_template('workorders/view.html', 
                         workorder=workorder, 
                         requirements=requirements,
                         cost_info=cost_info,
                         all_products=all_products,
                         task_summary=task_summary,
                         tasks=tasks)

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
            
            # Update work order
            conn.execute('''
                UPDATE work_orders 
                SET product_id = ?,
                    quantity = ?,
                    disposition = ?,
                    status = ?,
                    priority = ?,
                    planned_start_date = ?,
                    planned_end_date = ?,
                    labor_cost = ?,
                    overhead_cost = ?
                WHERE id = ?
            ''', (
                int(request.form['product_id']),
                float(request.form['quantity']),
                request.form.get('disposition', 'Manufacture'),
                request.form['status'],
                request.form.get('priority', 'Medium'),
                request.form.get('planned_start_date') or None,
                request.form.get('planned_end_date') or None,
                float(request.form.get('labor_cost', 0)),
                float(request.form.get('overhead_cost', 0)),
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
    
    conn.close()
    
    return render_template('workorders/edit.html', workorder=workorder, products=products)

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
                
                if inventory:
                    # Update existing inventory
                    new_quantity = inventory['quantity'] + wo['quantity']
                    conn.execute('''
                        UPDATE inventory 
                        SET quantity = ?,
                            last_updated = CURRENT_TIMESTAMP
                        WHERE product_id = ?
                    ''', (new_quantity, wo['product_id']))
                else:
                    # Create new inventory record
                    product = conn.execute('''
                        SELECT unit_of_measure FROM products WHERE id = ?
                    ''', (wo['product_id'],)).fetchone()
                    
                    conn.execute('''
                        INSERT INTO inventory (product_id, quantity, unit_of_measure, location)
                        VALUES (?, ?, ?, ?)
                    ''', (wo['product_id'], wo['quantity'], 
                          product['unit_of_measure'], 'Finished Goods'))
                
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
