from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session
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
            
            planned_start_date = request.form.get('planned_start_date') or None
            planned_end_date = request.form.get('planned_end_date') or None
            planned_hours = float(request.form.get('planned_hours', 0))
            assigned_resource_id = request.form.get('assigned_resource_id')
            remarks = request.form.get('remarks', '').strip()
            task_instructions = request.form.get('task_instructions', '').strip()
            
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
                 assigned_resource_id, status, remarks, task_instructions)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (task_number, wo_id, task_name, description, category, sequence_number,
                  priority, planned_start_date, planned_end_date, planned_hours, planned_labor_cost,
                  assigned_resource_id, 'Not Started', remarks, task_instructions))
            
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
            
            planned_start_date = request.form.get('planned_start_date') or None
            planned_end_date = request.form.get('planned_end_date') or None
            planned_hours = float(request.form.get('planned_hours', 0))
            assigned_resource_id = request.form.get('assigned_resource_id')
            work_center_id = request.form.get('work_center_id')
            status = request.form.get('status', 'Not Started')
            remarks = request.form.get('remarks', '').strip()
            task_instructions = request.form.get('task_instructions', '').strip()
            discrepancies = request.form.get('discrepancies', '').strip()
            corrective_actions = request.form.get('corrective_actions', '').strip()
            
            if not task_name:
                flash('Task name is required', 'danger')
                labor_resources = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY first_name').fetchall()
                work_centers = conn.execute('SELECT id, code, name FROM work_centers WHERE status = "Active" ORDER BY code').fetchall()
                conn.close()
                return render_template('tasks/edit.html', task=task, labor_resources=labor_resources, work_centers=work_centers)
            
            if not math.isfinite(planned_hours) or planned_hours < 0:
                flash('Planned hours must be a valid non-negative number', 'danger')
                labor_resources = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY first_name').fetchall()
                work_centers = conn.execute('SELECT id, code, name FROM work_centers WHERE status = "Active" ORDER BY code').fetchall()
                conn.close()
                return render_template('tasks/edit.html', task=task, labor_resources=labor_resources, work_centers=work_centers)
            
            if assigned_resource_id:
                assigned_resource_id = int(assigned_resource_id)
            else:
                assigned_resource_id = None
            
            if work_center_id:
                work_center_id = int(work_center_id)
            else:
                work_center_id = None
            
            if status not in ['Not Started', 'Cancelled'] and task['status'] == 'Not Started':
                required_skills_count = conn.execute('''
                    SELECT COUNT(*) as cnt FROM task_required_skills WHERE task_id = ?
                ''', (task_id,)).fetchone()
                
                if not required_skills_count or required_skills_count['cnt'] == 0:
                    flash('Cannot advance task status: At least one required skillset must be assigned before starting this task', 'danger')
                    labor_resources = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY first_name').fetchall()
                    work_centers = conn.execute('SELECT id, code, name FROM work_centers WHERE status = "Active" ORDER BY code').fetchall()
                    conn.close()
                    return render_template('tasks/edit.html', task=task, labor_resources=labor_resources, work_centers=work_centers)
            
            planned_labor_cost = 0
            if assigned_resource_id and planned_hours > 0:
                resource = conn.execute('SELECT hourly_rate FROM labor_resources WHERE id = ?', (assigned_resource_id,)).fetchone()
                if resource:
                    planned_labor_cost = planned_hours * resource['hourly_rate']
            
            conn.execute('''
                UPDATE work_order_tasks 
                SET task_name = ?, description = ?, category = ?, sequence_number = ?,
                    priority = ?, planned_start_date = ?, planned_end_date = ?, planned_hours = ?,
                    planned_labor_cost = ?, assigned_resource_id = ?, work_center_id = ?, status = ?, remarks = ?,
                    task_instructions = ?, discrepancies = ?, corrective_actions = ?
                WHERE id = ?
            ''', (task_name, description, category, sequence_number, priority, 
                  planned_start_date, planned_end_date, planned_hours, planned_labor_cost,
                  assigned_resource_id, work_center_id, status, remarks,
                  task_instructions, discrepancies, corrective_actions, task_id))
            
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
    work_centers = conn.execute('SELECT id, code, name FROM work_centers WHERE status = "Active" ORDER BY code').fetchall()
    conn.close()
    return render_template('tasks/edit.html', task=task, labor_resources=labor_resources, work_centers=work_centers)

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
    
    materials_with_availability = []
    for material in materials:
        material_dict = dict(material)
        inventory = conn.execute('''
            SELECT SUM(quantity) as available_qty
            FROM inventory
            WHERE product_id = ?
        ''', (material['material_id'],)).fetchone()
        
        available_qty = inventory['available_qty'] if inventory and inventory['available_qty'] else 0
        material_dict['available_qty'] = available_qty
        material_dict['shortage'] = max(0, material['quantity_required'] - available_qty)
        material_dict['has_shortage'] = available_qty < material['quantity_required']
        materials_with_availability.append(material_dict)
    
    required_skills = conn.execute('''
        SELECT trs.*, s.skillset_name, s.category
        FROM task_required_skills trs
        JOIN skillsets s ON trs.skillset_id = s.id
        WHERE trs.task_id = ?
        ORDER BY s.skillset_name
    ''', (task_id,)).fetchall()
    
    skill_match_warnings = []
    if task['assigned_resource_id']:
        assigned_skills = conn.execute('''
            SELECT lrs.skillset_id, lrs.skill_level, s.skillset_name
            FROM labor_resource_skills lrs
            JOIN skillsets s ON lrs.skillset_id = s.id
            WHERE lrs.labor_resource_id = ?
        ''', (task['assigned_resource_id'],)).fetchall()
        
        assigned_skill_map = {s['skillset_id']: s['skill_level'] for s in assigned_skills}
        
        for req_skill in required_skills:
            if req_skill['skillset_id'] not in assigned_skill_map:
                skill_match_warnings.append({
                    'skillset_name': req_skill['skillset_name'],
                    'required_level': req_skill['skill_level'],
                    'assigned_level': None,
                    'message': f"Missing required skill: {req_skill['skillset_name']}"
                })
            else:
                assigned_level = assigned_skill_map[req_skill['skillset_id']]
                level_order = {lv: i for i, lv in enumerate(SKILL_LEVELS)}
                
                if level_order.get(assigned_level, 0) < level_order.get(req_skill['skill_level'], 0):
                    skill_match_warnings.append({
                        'skillset_name': req_skill['skillset_name'],
                        'required_level': req_skill['skill_level'],
                        'assigned_level': assigned_level,
                        'message': f"{req_skill['skillset_name']}: Required {req_skill['skill_level']}, but assigned resource only has {assigned_level}"
                    })
    
    conn.close()
    return render_template('tasks/view.html', task=task, labor_entries=labor_entries, 
                         materials=materials_with_availability, required_skills=required_skills,
                         material_statuses=MATERIAL_STATUSES, skill_levels=SKILL_LEVELS,
                         skill_match_warnings=skill_match_warnings)

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
        
        product = conn.execute('SELECT code, name, unit_of_measure FROM products WHERE id = ?', (material_id,)).fetchone()
        if not product:
            conn.close()
            return jsonify({'success': False, 'error': 'Product not found'}), 404
        
        conn.execute('''
            INSERT INTO task_material_requirements 
            (task_id, material_id, description, quantity_required, unit_of_measure, is_optional, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (task_id, material_id, product['name'], quantity_required, product['unit_of_measure'], is_optional, 
              session.get('user_id')))
        
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
        ''', (quantity_required, status, is_optional, session.get('user_id'), material_req_id))
        
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
        ''', (task_id, skillset_id, skill_level, session.get('user_id')))
        
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
    
    product = conn.execute('SELECT id, code, name, unit_of_measure as uom FROM products WHERE id = ?', (product_id,)).fetchone()
    
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

@task_bp.route('/api/products', methods=['GET'])
@login_required
def get_all_products():
    db = Database()
    conn = db.get_connection()
    
    products = conn.execute('''
        SELECT id, code, name, unit_of_measure as uom 
        FROM products 
        ORDER BY code
    ''').fetchall()
    
    conn.close()
    return jsonify([dict(p) for p in products])

@task_bp.route('/api/skillsets', methods=['GET'])
@login_required
def get_all_skillsets():
    db = Database()
    conn = db.get_connection()
    
    skillsets = conn.execute('''
        SELECT id, skillset_name, category 
        FROM skillsets 
        WHERE status = 'Active'
        ORDER BY skillset_name
    ''').fetchall()
    
    conn.close()
    return jsonify([dict(s) for s in skillsets])


@task_bp.route('/work-orders/<int:wo_id>/apply-template', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def apply_template(wo_id):
    db = Database()
    conn = db.get_connection()
    
    work_order = conn.execute('SELECT * FROM work_orders WHERE id = ?', (wo_id,)).fetchone()
    
    if not work_order:
        conn.close()
        flash('Work order not found', 'danger')
        return redirect(url_for('workorder_routes.list_workorders'))
    
    template_id = request.form.get('template_id')
    
    if not template_id:
        conn.close()
        flash('Please select a template', 'warning')
        return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
    
    try:
        template = conn.execute('SELECT * FROM task_templates WHERE id = ?', (template_id,)).fetchone()
        
        if not template:
            conn.close()
            flash('Template not found', 'danger')
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        template_items = conn.execute('''
            SELECT * FROM task_template_items 
            WHERE template_id = ? 
            ORDER BY sequence_number, id
        ''', (template_id,)).fetchall()
        
        if not template_items:
            conn.close()
            flash('Template has no tasks to apply', 'warning')
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        tasks_created = 0
        base_sequence = conn.execute('''
            SELECT COALESCE(MAX(sequence_number), 0) FROM work_order_tasks WHERE work_order_id = ?
        ''', (wo_id,)).fetchone()[0] or 0
        
        for idx, item in enumerate(template_items):
            task_number = generate_task_number(conn)
            seq_num = item['sequence_number'] if item['sequence_number'] else (base_sequence + (idx + 1) * 10)
            
            conn.execute('''
                INSERT INTO work_order_tasks 
                (task_number, work_order_id, task_name, description, category, 
                 sequence_number, priority, planned_hours, status, remarks)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (task_number, wo_id, item['task_name'], item['description'], 
                  item['category'], seq_num, item['priority'],
                  item['planned_hours'] or 0, 'Not Started', item['remarks']))
            tasks_created += 1
        
        conn.commit()
        conn.close()
        flash(f'Template "{template["template_name"]}" applied successfully - {tasks_created} tasks created', 'success')
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error applying template: {str(e)}', 'danger')
    
    return redirect(url_for('workorder_routes.view_workorder', id=wo_id))


@task_bp.route('/work-orders/<int:wo_id>/tasks/mass-edit', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def mass_edit_tasks(wo_id):
    """Mass edit multiple work order tasks at once"""
    db = Database()
    conn = db.get_connection()
    
    work_order = conn.execute('SELECT * FROM work_orders WHERE id = ?', (wo_id,)).fetchone()
    if not work_order:
        conn.close()
        return jsonify({'success': False, 'error': 'Work order not found'}), 404
    
    data = request.get_json()
    task_ids = data.get('task_ids', [])
    updates = data.get('updates', {})
    
    if not task_ids:
        conn.close()
        return jsonify({'success': False, 'error': 'No tasks selected'}), 400
    
    if not updates:
        conn.close()
        return jsonify({'success': False, 'error': 'No updates specified'}), 400
    
    try:
        updated_count = 0
        
        for task_id in task_ids:
            task = conn.execute('''
                SELECT * FROM work_order_tasks WHERE id = ? AND work_order_id = ?
            ''', (task_id, wo_id)).fetchone()
            
            if not task:
                continue
            
            update_fields = []
            update_values = []
            
            if 'status' in updates and updates['status']:
                if updates['status'] not in ['Not Started', 'Cancelled'] and task['status'] == 'Not Started':
                    required_skills = conn.execute('''
                        SELECT COUNT(*) as cnt FROM task_required_skills WHERE task_id = ?
                    ''', (task_id,)).fetchone()
                    if required_skills and required_skills['cnt'] > 0:
                        update_fields.append('status = ?')
                        update_values.append(updates['status'])
                else:
                    update_fields.append('status = ?')
                    update_values.append(updates['status'])
            
            if 'priority' in updates and updates['priority']:
                update_fields.append('priority = ?')
                update_values.append(updates['priority'])
            
            if 'category' in updates and updates['category']:
                update_fields.append('category = ?')
                update_values.append(updates['category'])
            
            if 'assigned_resource_id' in updates:
                resource_id = updates['assigned_resource_id']
                if resource_id == '' or resource_id is None:
                    update_fields.append('assigned_resource_id = ?')
                    update_values.append(None)
                else:
                    update_fields.append('assigned_resource_id = ?')
                    update_values.append(int(resource_id))
            
            if 'work_center_id' in updates:
                wc_id = updates['work_center_id']
                if wc_id == '' or wc_id is None:
                    update_fields.append('work_center_id = ?')
                    update_values.append(None)
                else:
                    update_fields.append('work_center_id = ?')
                    update_values.append(int(wc_id))
            
            if 'planned_start_date' in updates and updates['planned_start_date']:
                update_fields.append('planned_start_date = ?')
                update_values.append(updates['planned_start_date'])
            
            if 'planned_end_date' in updates and updates['planned_end_date']:
                update_fields.append('planned_end_date = ?')
                update_values.append(updates['planned_end_date'])
            
            if update_fields:
                update_values.append(task_id)
                conn.execute(f'''
                    UPDATE work_order_tasks 
                    SET {', '.join(update_fields)}
                    WHERE id = ?
                ''', update_values)
                updated_count += 1
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Successfully updated {updated_count} task(s)',
            'updated_count': updated_count
        })
        
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500


@task_bp.route('/work-orders/<int:wo_id>/tasks/data', methods=['GET'])
@login_required
def get_tasks_data(wo_id):
    """Get labor resources and work centers for mass edit modal"""
    db = Database()
    conn = db.get_connection()
    
    labor_resources = conn.execute('''
        SELECT id, employee_code, first_name, last_name 
        FROM labor_resources WHERE status = 'Active' ORDER BY first_name
    ''').fetchall()
    
    work_centers = conn.execute('''
        SELECT id, code, name FROM work_centers WHERE status = 'Active' ORDER BY code
    ''').fetchall()
    
    conn.close()
    
    return jsonify({
        'labor_resources': [dict(r) for r in labor_resources],
        'work_centers': [dict(w) for w in work_centers]
    })
