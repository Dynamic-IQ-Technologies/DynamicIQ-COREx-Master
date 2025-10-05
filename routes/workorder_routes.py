from flask import Blueprint, render_template, request, redirect, url_for, flash
from models import Database
from mrp_logic import MRPEngine
from auth import login_required, role_required

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
                    (wo_number, product_id, quantity, status, priority, planned_start_date, planned_end_date, labor_cost, overhead_cost)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    wo_number,
                    int(request.form['product_id']),
                    float(request.form['quantity']),
                    request.form['status'],
                    request.form.get('priority', 'Medium'),
                    request.form.get('planned_start_date'),
                    request.form.get('planned_end_date'),
                    float(request.form.get('labor_cost', 0)),
                    float(request.form.get('overhead_cost', 0))
                ))
                
                wo_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
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

@workorder_bp.route('/workorders/<int:id>/update-status', methods=['POST'])
@role_required('Admin', 'Production Staff')
def update_workorder_status(id):
    db = Database()
    conn = db.get_connection()
    
    new_status = request.form['status']
    conn.execute('UPDATE work_orders SET status=? WHERE id=?', (new_status, id))
    
    if new_status == 'Completed':
        conn.execute('UPDATE work_orders SET actual_end_date=CURRENT_DATE WHERE id=?', (id,))
    elif new_status == 'In Progress':
        conn.execute('UPDATE work_orders SET actual_start_date=CURRENT_DATE WHERE id=?', (id,))
    
    conn.commit()
    conn.close()
    
    flash(f'Work Order status updated to {new_status}!', 'success')
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
