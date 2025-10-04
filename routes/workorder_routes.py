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

@workorder_bp.route('/workorders/create', methods=['GET', 'POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def create_workorder():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        conn.execute('''
            INSERT INTO work_orders 
            (wo_number, product_id, quantity, status, priority, planned_start_date, planned_end_date, labor_cost, overhead_cost)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            request.form['wo_number'],
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
        conn.close()
        
        mrp = MRPEngine()
        mrp.calculate_requirements(wo_id)
        
        flash('Work Order created successfully! Material requirements calculated.', 'success')
        return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
    
    products = conn.execute('SELECT * FROM products WHERE product_type="Finished Good" ORDER BY code').fetchall()
    conn.close()
    
    return render_template('workorders/create.html', products=products)

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
        SELECT mr.*, p.code, p.name, p.unit_of_measure
        FROM material_requirements mr
        JOIN products p ON mr.product_id = p.id
        WHERE mr.work_order_id=?
    ''', (id,)).fetchall()
    
    cost_info = mrp.calculate_work_order_cost(id)
    
    conn.close()
    
    return render_template('workorders/view.html', 
                         workorder=workorder, 
                         requirements=requirements,
                         cost_info=cost_info)

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
