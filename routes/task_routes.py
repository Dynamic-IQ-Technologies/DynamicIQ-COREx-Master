from flask import Blueprint, render_template, request, redirect, url_for, flash
from models import Database
from auth import login_required, role_required
from datetime import datetime
import math

task_bp = Blueprint('task_routes', __name__)

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
    
    conn.close()
    return render_template('tasks/view.html', task=task, labor_entries=labor_entries)
