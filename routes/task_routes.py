from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from models import Database
from auth import login_required, role_required
from datetime import datetime
import math

task_bp = Blueprint('task_routes', __name__)

MATERIAL_STATUSES = ['Pending', 'Partially Issued', 'Issued', 'Received']
SKILL_LEVELS = ['Apprentice', 'Intermediate', 'Advanced', 'Expert']

def generate_task_number(conn):
    last_task = conn.execute('SELECT task_number FROM work_order_tasks ORDER BY id DESC LIMIT 1').fetchone()
    if last_task:
        last_number = int(last_task['task_number'].split('-')[1])
        new_number = last_number + 1
    else:
        new_number = 1
    return f'TASK-{new_number:06d}'

@task_bp.route('/work-orders/<int:wo_id>/tasks')
@login_required
def list_tasks(wo_id):
    db = Database()
    conn = db.get_connection()
    
    work_order = conn.execute('''
        SELECT wo.*, p.code, p.name 
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.id = ?
    ''', (wo_id,)).fetchone()
    
    if not work_order:
        conn.close()
        flash('Work order not found', 'danger')
        return redirect(url_for('workorder_routes.list_workorders'))
    
    tasks = conn.execute('''
        SELECT t.*, lr.first_name, lr.last_name, lr.employee_code
        FROM work_order_tasks t
        LEFT JOIN labor_resources lr ON t.assigned_resource_id = lr.id
        WHERE t.work_order_id = ?
        ORDER BY t.sequence_number, t.created_at
    ''', (wo_id,)).fetchall()
    
    conn.close()
    return render_template('tasks/list.html', work_order=work_order, tasks=tasks)

@task_bp.route('/work-orders/<int:wo_id>/tasks/create', methods=['GET', 'POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def create_task(wo_id):
    db = Database()
    conn = db.get_connection()
    
    work_order = conn.execute('SELECT * FROM work_orders WHERE id = ?', (wo_id,)).fetchone()
    
    if not work_order:
        conn.close()
        flash('Work order not found', 'danger')
        return redirect(url_for('workorder_routes.list_workorders'))
    
    if request.method == 'POST':
        try:
            task_name = request.form.get('task_name', '').strip()
            description = request.form.get('description', '').strip()
            category = request.form.get('category', 'General')
            sequence_number = int(request.form.get('sequence_number', 0))
            priority = request.form.get('priority', 'Medium')
            
            planned_start_date = request.form.get('planned_start_date')
            planned_end_date = request.form.get('planned_end_date')
            planned_hours = float(request.form.get('planned_hours', 0))
            assigned_resource_id = request.form.get('assigned_resource_id')
            remarks = request.form.get('remarks', '').strip()
            
            if not task_name:
                flash('Task name is required', 'danger')
                labor_resources = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY first_name').fetchall()
                conn.close()
                return render_template('tasks/create.html', work_order=work_order, labor_resources=labor_resources)
            
            if not math.isfinite(planned_hours) or planned_hours < 0:
                flash('Planned hours must be a valid non-negative number', 'danger')
                labor_resources = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY first_name').fetchall()
                conn.close()
                return render_template('tasks/create.html', work_order=work_order, labor_resources=labor_resources)
            
            if assigned_resource_id:
                assigned_resource_id = int(assigned_resource_id)
            else:
                assigned_resource_id = None
            
            task_number = generate_task_number(conn)
            
            planned_labor_cost = 0
            if assigned_resource_id and planned_hours > 0:
                resource = conn.execute('SELECT hourly_rate FROM labor_resources WHERE id = ?', (assigned_resource_id,)).fetchone()
                if resource:
                    planned_labor_cost = planned_hours * resource['hourly_rate']
            
            conn.execute('''
                INSERT INTO work_order_tasks 
                (task_number, work_order_id, task_name, description, category, sequence_number, 
                 priority, planned_start_date, planned_end_date, planned_hours, planned_labor_cost,
                 assigned_resource_id, status, remarks)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (task_number, wo_id, task_name, description, category, sequence_number,
                  priority, planned_start_date, planned_end_date, planned_hours, planned_labor_cost,
                  assigned_resource_id, 'Not Started', remarks))
            
            conn.commit()
            conn.close()
            flash(f'Task {task_number} created successfully', 'success')
            return redirect(url_for('task_routes.list_tasks', wo_id=wo_id))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error creating task: {str(e)}', 'danger')
            return redirect(url_for('task_routes.create_task', wo_id=wo_id))
    
    labor_resources = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY first_name').fetchall()
    conn.close()
    return render_template('tasks/create.html', work_order=work_order, labor_resources=labor_resources)

@task_bp.route('/tasks/<int:task_id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def edit_task(task_id):
    db = Database()
    conn = db.get_connection()
    
    task = conn.execute('''
        SELECT t.*, wo.wo_number, wo.product_id, p.code, p.name
        FROM work_order_tasks t
        JOIN work_orders wo ON t.work_order_id = wo.id
        JOIN products p ON wo.product_id = p.id
        WHERE t.id = ?
    ''', (task_id,)).fetchone()
    
    if not task:
        conn.close()
        flash('Task not found', 'danger')
        return redirect(url_for('workorder_routes.list_workorders'))
    
    if request.method == 'POST':
        try:
            task_name = request.form.get('task_name', '').strip()
            description = request.form.get('description', '').strip()
            category = request.form.get('category', 'General')
            sequence_number = int(request.form.get('sequence_number', 0))
            priority = request.form.get('priority', 'Medium')
            
            planned_start_date = request.form.get('planned_start_date')
            planned_end_date = request.form.get('planned_end_date')
            planned_hours = float(request.form.get('planned_hours', 0))
            assigned_resource_id = request.form.get('assigned_resource_id')
            status = request.form.get('status', 'Not Started')
            remarks = request.form.get('remarks', '').strip()
            
            if not task_name:
                flash('Task name is required', 'danger')
                labor_resources = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY first_name').fetchall()
                conn.close()
                return render_template('tasks/edit.html', task=task, labor_resources=labor_resources)
            
            if not math.isfinite(planned_hours) or planned_hours < 0:
                flash('Planned hours must be a valid non-negative number', 'danger')
                labor_resources = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY first_name').fetchall()
                conn.close()
                return render_template('tasks/edit.html', task=task, labor_resources=labor_resources)
            
            if assigned_resource_id:
                assigned_resource_id = int(assigned_resource_id)
            else:
                assigned_resource_id = None
            
            planned_labor_cost = 0
            if assigned_resource_id and planned_hours > 0:
                resource = conn.execute('SELECT hourly_rate FROM labor_resources WHERE id = ?', (assigned_resource_id,)).fetchone()
                if resource:
                    planned_labor_cost = planned_hours * resource['hourly_rate']
            
            conn.execute('''
                UPDATE work_order_tasks 
                SET task_name = ?, description = ?, category = ?, sequence_number = ?,
                    priority = ?, planned_start_date = ?, planned_end_date = ?, planned_hours = ?,
                    planned_labor_cost = ?, assigned_resource_id = ?, status = ?, remarks = ?
                WHERE id = ?
            ''', (task_name, description, category, sequence_number, priority, 
                  planned_start_date, planned_end_date, planned_hours, planned_labor_cost,
                  assigned_resource_id, status, remarks, task_id))
            
            conn.commit()
            conn.close()
            flash('Task updated successfully', 'success')
            return redirect(url_for('task_routes.list_tasks', wo_id=task['work_order_id']))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error updating task: {str(e)}', 'danger')
            return redirect(url_for('task_routes.edit_task', task_id=task_id))
    
    labor_resources = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY first_name').fetchall()
    conn.close()
    return render_template('tasks/edit.html', task=task, labor_resources=labor_resources)

@task_bp.route('/tasks/<int:task_id>/delete', methods=['POST'])
@role_required('Admin', 'Planner')
def delete_task(task_id):
    db = Database()
    conn = db.get_connection()
    
    task = conn.execute('SELECT work_order_id FROM work_order_tasks WHERE id = ?', (task_id,)).fetchone()
    
    if not task:
        conn.close()
        flash('Task not found', 'danger')
        return redirect(url_for('workorder_routes.list_workorders'))
    
    try:
        labor_count = conn.execute('SELECT COUNT(*) as cnt FROM labor_issuance WHERE task_id = ?', (task_id,)).fetchone()
        
        if labor_count and labor_count['cnt'] > 0:
            flash('Cannot delete task with existing labor entries', 'danger')
            conn.close()
            return redirect(url_for('task_routes.list_tasks', wo_id=task['work_order_id']))
        
        conn.execute('DELETE FROM work_order_tasks WHERE id = ?', (task_id,))
        conn.commit()
        conn.close()
        flash('Task deleted successfully', 'success')
        return redirect(url_for('task_routes.list_tasks', wo_id=task['work_order_id']))
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error deleting task: {str(e)}', 'danger')
        return redirect(url_for('task_routes.list_tasks', wo_id=task['work_order_id']))

@task_bp.route('/tasks/<int:task_id>')
@login_required
def view_task(task_id):
    db = Database()
    conn = db.get_connection()
    
    task = conn.execute('''
        SELECT t.*, wo.wo_number, wo.product_id, p.code, p.name,
               lr.first_name, lr.last_name, lr.employee_code, lr.hourly_rate
        FROM work_order_tasks t
        JOIN work_orders wo ON t.work_order_id = wo.id
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN labor_resources lr ON t.assigned_resource_id = lr.id
        WHERE t.id = ?
    ''', (task_id,)).fetchone()
    
    if not task:
        conn.close()
        flash('Task not found', 'danger')
        return redirect(url_for('workorder_routes.list_workorders'))
    
    labor_entries = conn.execute('''
        SELECT li.*, lr.first_name, lr.last_name, lr.employee_code,
               u.username as created_by_name
        FROM labor_issuance li
        JOIN labor_resources lr ON li.resource_id = lr.id
        LEFT JOIN users u ON li.created_by = u.id
        WHERE li.task_id = ?
        ORDER BY li.work_date DESC, li.start_time DESC
    ''', (task_id,)).fetchall()
    
    materials = conn.execute('''
        SELECT tmr.*, p.code as material_code, p.name as material_name,
               u.username as issued_by_name
        FROM task_material_requirements tmr
        JOIN products p ON tmr.material_id = p.id
        LEFT JOIN users u ON tmr.issued_by = u.id
        WHERE tmr.task_id = ?
        ORDER BY tmr.created_at
    ''', (task_id,)).fetchall()
    
    required_skills = conn.execute('''
        SELECT trs.*, s.skillset_name, s.category
        FROM task_required_skills trs
        JOIN skillsets s ON trs.skillset_id = s.id
        WHERE trs.task_id = ?
        ORDER BY s.skillset_name
    ''', (task_id,)).fetchall()
    
    conn.close()
    return render_template('tasks/view.html', task=task, labor_entries=labor_entries, 
                         materials=materials, required_skills=required_skills,
                         material_statuses=MATERIAL_STATUSES, skill_levels=SKILL_LEVELS)

@task_bp.route('/tasks/<int:task_id>/materials', methods=['GET'])
@login_required
def get_task_materials(task_id):
    db = Database()
    conn = db.get_connection()
    
    task = conn.execute('SELECT id FROM work_order_tasks WHERE id = ?', (task_id,)).fetchone()
    if not task:
        conn.close()
        return jsonify({'success': False, 'error': 'Task not found'}), 404
    
    materials = conn.execute('''
        SELECT tmr.*, p.code as material_code, p.name as material_name
        FROM task_material_requirements tmr
        JOIN products p ON tmr.material_id = p.id
        WHERE tmr.task_id = ?
        ORDER BY tmr.created_at
    ''', (task_id,)).fetchall()
    
    conn.close()
    return jsonify({
        'success': True,
        'materials': [dict(m) for m in materials]
    })

@task_bp.route('/tasks/<int:task_id>/materials/add', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def add_task_material(task_id):
    db = Database()
    conn = db.get_connection()
    
    try:
        task = conn.execute('SELECT id FROM work_order_tasks WHERE id = ?', (task_id,)).fetchone()
        if not task:
            conn.close()
            return jsonify({'success': False, 'error': 'Task not found'}), 404
        
        material_id = request.form.get('material_id')
        quantity_required = float(request.form.get('quantity_required', 0))
        is_optional = int(request.form.get('is_optional', 0))
        
        if not material_id or quantity_required <= 0:
            conn.close()
            return jsonify({'success': False, 'error': 'Material and quantity are required'}), 400
        
        product = conn.execute('SELECT code, name, uom FROM products WHERE id = ?', (material_id,)).fetchone()
        if not product:
            conn.close()
            return jsonify({'success': False, 'error': 'Product not found'}), 404
        
        conn.execute('''
            INSERT INTO task_material_requirements 
            (task_id, material_id, description, quantity_required, unit_of_measure, is_optional, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (task_id, material_id, product['name'], quantity_required, product['uom'], is_optional, 
              request.user.get('id')))
        
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'Material added successfully'})
        
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500

@task_bp.route('/tasks/<int:task_id>/materials/<int:material_req_id>/edit', methods=['PUT', 'POST'])
@role_required('Admin', 'Planner')
def edit_task_material(task_id, material_req_id):
    db = Database()
    conn = db.get_connection()
    
    try:
        material_req = conn.execute('''
            SELECT * FROM task_material_requirements 
            WHERE id = ? AND task_id = ?
        ''', (material_req_id, task_id)).fetchone()
        
        if not material_req:
            conn.close()
            return jsonify({'success': False, 'error': 'Material requirement not found'}), 404
        
        data = request.get_json() if request.is_json else request.form
        quantity_required = float(data.get('quantity_required', material_req['quantity_required']))
        status = data.get('status', material_req['status'])
        is_optional = int(data.get('is_optional', material_req['is_optional']))
        
        if status not in MATERIAL_STATUSES:
            conn.close()
            return jsonify({'success': False, 'error': 'Invalid status'}), 400
        
        conn.execute('''
            UPDATE task_material_requirements 
            SET quantity_required = ?, status = ?, is_optional = ?, 
                updated_by = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (quantity_required, status, is_optional, request.user.get('id'), material_req_id))
        
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'Material updated successfully'})
        
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500

@task_bp.route('/tasks/<int:task_id>/materials/<int:material_req_id>/delete', methods=['DELETE', 'POST'])
@role_required('Admin', 'Planner')
def delete_task_material(task_id, material_req_id):
    db = Database()
    conn = db.get_connection()
    
    try:
        material_req = conn.execute('''
            SELECT * FROM task_material_requirements 
            WHERE id = ? AND task_id = ?
        ''', (material_req_id, task_id)).fetchone()
        
        if not material_req:
            conn.close()
            return jsonify({'success': False, 'error': 'Material requirement not found'}), 404
        
        conn.execute('DELETE FROM task_material_requirements WHERE id = ?', (material_req_id,))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'Material removed successfully'})
        
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500

@task_bp.route('/tasks/<int:task_id>/skills', methods=['GET'])
@login_required
def get_task_skills(task_id):
    db = Database()
    conn = db.get_connection()
    
    task = conn.execute('SELECT id FROM work_order_tasks WHERE id = ?', (task_id,)).fetchone()
    if not task:
        conn.close()
        return jsonify({'success': False, 'error': 'Task not found'}), 404
    
    skills = conn.execute('''
        SELECT trs.*, s.skillset_name, s.category
        FROM task_required_skills trs
        JOIN skillsets s ON trs.skillset_id = s.id
        WHERE trs.task_id = ?
        ORDER BY s.skillset_name
    ''', (task_id,)).fetchall()
    
    conn.close()
    return jsonify({
        'success': True,
        'skills': [dict(s) for s in skills]
    })

@task_bp.route('/tasks/<int:task_id>/skills/add', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def add_task_skill(task_id):
    db = Database()
    conn = db.get_connection()
    
    try:
        task = conn.execute('SELECT id FROM work_order_tasks WHERE id = ?', (task_id,)).fetchone()
        if not task:
            conn.close()
            return jsonify({'success': False, 'error': 'Task not found'}), 404
        
        skillset_id = request.form.get('skillset_id')
        skill_level = request.form.get('skill_level')
        
        if not skillset_id or not skill_level:
            conn.close()
            return jsonify({'success': False, 'error': 'Skillset and level are required'}), 400
        
        if skill_level not in SKILL_LEVELS:
            conn.close()
            return jsonify({'success': False, 'error': 'Invalid skill level'}), 400
        
        existing = conn.execute('''
            SELECT id FROM task_required_skills 
            WHERE task_id = ? AND skillset_id = ?
        ''', (task_id, skillset_id)).fetchone()
        
        if existing:
            conn.close()
            return jsonify({'success': False, 'error': 'This skillset is already required for this task'}), 400
        
        conn.execute('''
            INSERT INTO task_required_skills (task_id, skillset_id, skill_level, created_by)
            VALUES (?, ?, ?, ?)
        ''', (task_id, skillset_id, skill_level, request.user.get('id')))
        
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'Skillset requirement added successfully'})
        
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500

@task_bp.route('/tasks/<int:task_id>/skills/<int:skill_req_id>/delete', methods=['DELETE', 'POST'])
@role_required('Admin', 'Planner')
def delete_task_skill(task_id, skill_req_id):
    db = Database()
    conn = db.get_connection()
    
    try:
        skill_req = conn.execute('''
            SELECT * FROM task_required_skills 
            WHERE id = ? AND task_id = ?
        ''', (skill_req_id, task_id)).fetchone()
        
        if not skill_req:
            conn.close()
            return jsonify({'success': False, 'error': 'Skill requirement not found'}), 404
        
        conn.execute('DELETE FROM task_required_skills WHERE id = ?', (skill_req_id,))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'Skillset requirement removed successfully'})
        
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500

@task_bp.route('/api/products/<int:product_id>/details', methods=['GET'])
@login_required
def get_product_details(product_id):
    db = Database()
    conn = db.get_connection()
    
    product = conn.execute('SELECT id, code, name, uom FROM products WHERE id = ?', (product_id,)).fetchone()
    
    if not product:
        conn.close()
        return jsonify({'success': False, 'error': 'Product not found'}), 404
    
    inventory = conn.execute('''
        SELECT SUM(quantity_on_hand) as total_qty 
        FROM inventory 
        WHERE product_id = ?
    ''', (product_id,)).fetchone()
    
    conn.close()
    return jsonify({
        'success': True,
        'product': dict(product),
        'available_qty': inventory['total_qty'] if inventory and inventory['total_qty'] else 0
    })
