from flask import Blueprint, render_template, request, redirect, url_for, flash
from models import Database
from auth import login_required, role_required
from datetime import datetime
import math

labor_issuance_bp = Blueprint('labor_issuance_routes', __name__)

def generate_labor_issuance_number(conn):
    last_issuance = conn.execute('SELECT issuance_number FROM labor_issuance ORDER BY id DESC LIMIT 1').fetchone()
    if last_issuance:
        last_number = int(last_issuance['issuance_number'].split('-')[1])
        new_number = last_number + 1
    else:
        new_number = 1
    return f'LBR-{new_number:06d}'

@labor_issuance_bp.route('/tasks/<int:task_id>/labor/record', methods=['GET', 'POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def record_labor(task_id):
    db = Database()
    conn = db.get_connection()
    
    task = conn.execute('''
        SELECT t.*, wo.wo_number, wo.id as work_order_id, wo.product_id, p.code, p.name,
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
    
    if request.method == 'POST':
        try:
            resource_id_str = request.form.get('resource_id')
            if not resource_id_str:
                flash('Labor resource is required', 'danger')
                labor_resources = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY first_name').fetchall()
                conn.close()
                return render_template('labor_issuance/record.html', task=task, labor_resources=labor_resources)
            
            resource_id = int(resource_id_str)
            work_date = request.form.get('work_date')
            start_time = request.form.get('start_time')
            end_time = request.form.get('end_time')
            hours_worked = float(request.form.get('hours_worked', 0))
            remarks = request.form.get('remarks', '').strip()
            
            if not work_date:
                flash('Work date is required', 'danger')
                labor_resources = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY first_name').fetchall()
                conn.close()
                return render_template('labor_issuance/record.html', task=task, labor_resources=labor_resources)
            
            if not math.isfinite(hours_worked) or hours_worked <= 0:
                flash('Hours worked must be a valid positive number', 'danger')
                labor_resources = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY first_name').fetchall()
                conn.close()
                return render_template('labor_issuance/record.html', task=task, labor_resources=labor_resources)
            
            resource = conn.execute('SELECT * FROM labor_resources WHERE id = ?', (resource_id,)).fetchone()
            
            if not resource:
                flash('Labor resource not found', 'danger')
                conn.close()
                return redirect(url_for('task_routes.view_task', task_id=task_id))
            
            hourly_rate = resource['hourly_rate']
            labor_cost = hours_worked * hourly_rate
            
            issuance_number = generate_labor_issuance_number(conn)
            
            start_datetime_str = None
            end_datetime_str = None
            
            if start_time:
                start_datetime_str = f"{work_date} {start_time}:00"
            
            if end_time:
                end_datetime_str = f"{work_date} {end_time}:00"
            
            user_id = None
            from flask import session
            if 'user_id' in session:
                user_id = session['user_id']
            
            conn.execute('''
                INSERT INTO labor_issuance 
                (issuance_number, task_id, work_order_id, resource_id, work_date, 
                 start_time, end_time, hours_worked, hourly_rate, labor_cost, 
                 remarks, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (issuance_number, task_id, task['work_order_id'], resource_id, work_date,
                  start_datetime_str, end_datetime_str, hours_worked, hourly_rate, labor_cost,
                  remarks, user_id))
            
            current_actual_hours = task['actual_hours'] or 0
            current_actual_cost = task['actual_labor_cost'] or 0
            new_actual_hours = current_actual_hours + hours_worked
            new_actual_cost = current_actual_cost + labor_cost
            
            conn.execute('''
                UPDATE work_order_tasks 
                SET actual_hours = ?, actual_labor_cost = ?
                WHERE id = ?
            ''', (new_actual_hours, new_actual_cost, task_id))
            
            if not task['actual_start_date']:
                current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                conn.execute('UPDATE work_order_tasks SET actual_start_date = ? WHERE id = ?', 
                           (current_datetime, task_id))
            
            if task['status'] == 'Not Started':
                conn.execute('UPDATE work_order_tasks SET status = ? WHERE id = ?', 
                           ('In Progress', task_id))
            
            wo_current_labor_cost = conn.execute(
                'SELECT labor_cost FROM work_orders WHERE id = ?', 
                (task['work_order_id'],)
            ).fetchone()['labor_cost'] or 0
            
            conn.execute('''
                UPDATE work_orders 
                SET labor_cost = ?
                WHERE id = ?
            ''', (wo_current_labor_cost + labor_cost, task['work_order_id']))
            
            conn.commit()
            conn.close()
            flash(f'Labor {issuance_number} recorded successfully', 'success')
            return redirect(url_for('task_routes.view_task', task_id=task_id))
            
        except ValueError as e:
            conn.rollback()
            labor_resources = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY first_name').fetchall()
            conn.close()
            flash(f'Invalid input: {str(e)}', 'danger')
            return render_template('labor_issuance/record.html', task=task, labor_resources=labor_resources)
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error recording labor: {str(e)}', 'danger')
            return redirect(url_for('labor_issuance_routes.record_labor', task_id=task_id))
    
    labor_resources = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY first_name').fetchall()
    conn.close()
    return render_template('labor_issuance/record.html', task=task, labor_resources=labor_resources)

@labor_issuance_bp.route('/labor-issuance/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def edit_labor_issuance(id):
    db = Database()
    conn = db.get_connection()
    
    labor_issuance = conn.execute('''
        SELECT li.*, t.task_name, t.id as task_id, wo.wo_number,
               lr.first_name, lr.last_name, lr.employee_code
        FROM labor_issuance li
        JOIN work_order_tasks t ON li.task_id = t.id
        JOIN work_orders wo ON li.work_order_id = wo.id
        JOIN labor_resources lr ON li.resource_id = lr.id
        WHERE li.id = ?
    ''', (id,)).fetchone()
    
    if not labor_issuance:
        conn.close()
        flash('Labor entry not found', 'danger')
        return redirect(url_for('workorder_routes.list_workorders'))
    
    if request.method == 'POST':
        try:
            old_hours = labor_issuance['hours_worked']
            old_cost = labor_issuance['labor_cost']
            
            resource_id_str = request.form.get('resource_id')
            if not resource_id_str:
                flash('Labor resource is required', 'danger')
                labor_resources = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY first_name').fetchall()
                conn.close()
                return render_template('labor_issuance/edit.html', labor_issuance=labor_issuance, labor_resources=labor_resources)
            
            resource_id = int(resource_id_str)
            work_date = request.form.get('work_date')
            start_time = request.form.get('start_time')
            end_time = request.form.get('end_time')
            hours_worked = float(request.form.get('hours_worked', 0))
            remarks = request.form.get('remarks', '').strip()
            
            if not work_date:
                flash('Work date is required', 'danger')
                labor_resources = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY first_name').fetchall()
                conn.close()
                return render_template('labor_issuance/edit.html', labor_issuance=labor_issuance, labor_resources=labor_resources)
            
            if not math.isfinite(hours_worked) or hours_worked <= 0:
                flash('Hours worked must be a valid positive number', 'danger')
                labor_resources = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY first_name').fetchall()
                conn.close()
                return render_template('labor_issuance/edit.html', labor_issuance=labor_issuance, labor_resources=labor_resources)
            
            resource = conn.execute('SELECT * FROM labor_resources WHERE id = ?', (resource_id,)).fetchone()
            
            if not resource:
                flash('Labor resource not found', 'danger')
                conn.close()
                return redirect(url_for('task_routes.view_task', task_id=labor_issuance['task_id']))
            
            hourly_rate = resource['hourly_rate']
            labor_cost = hours_worked * hourly_rate
            
            start_datetime_str = None
            end_datetime_str = None
            
            if start_time:
                start_datetime_str = f"{work_date} {start_time}:00"
            
            if end_time:
                end_datetime_str = f"{work_date} {end_time}:00"
            
            conn.execute('''
                UPDATE labor_issuance 
                SET resource_id = ?, work_date = ?, start_time = ?, end_time = ?,
                    hours_worked = ?, hourly_rate = ?, labor_cost = ?, remarks = ?
                WHERE id = ?
            ''', (resource_id, work_date, start_datetime_str, end_datetime_str, 
                  hours_worked, hourly_rate, labor_cost, remarks, id))
            
            hours_delta = hours_worked - old_hours
            cost_delta = labor_cost - old_cost
            
            task = conn.execute('SELECT * FROM work_order_tasks WHERE id = ?', 
                              (labor_issuance['task_id'],)).fetchone()
            
            new_actual_hours = (task['actual_hours'] or 0) + hours_delta
            new_actual_cost = (task['actual_labor_cost'] or 0) + cost_delta
            
            conn.execute('''
                UPDATE work_order_tasks 
                SET actual_hours = ?, actual_labor_cost = ?
                WHERE id = ?
            ''', (new_actual_hours, new_actual_cost, labor_issuance['task_id']))
            
            wo = conn.execute('SELECT labor_cost FROM work_orders WHERE id = ?', 
                            (labor_issuance['work_order_id'],)).fetchone()
            
            new_wo_labor_cost = (wo['labor_cost'] or 0) + cost_delta
            
            conn.execute('''
                UPDATE work_orders 
                SET labor_cost = ?
                WHERE id = ?
            ''', (new_wo_labor_cost, labor_issuance['work_order_id']))
            
            conn.commit()
            conn.close()
            flash('Labor entry updated successfully', 'success')
            return redirect(url_for('task_routes.view_task', task_id=labor_issuance['task_id']))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error updating labor entry: {str(e)}', 'danger')
            return redirect(url_for('labor_issuance_routes.edit_labor_issuance', id=id))
    
    labor_resources = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY first_name').fetchall()
    conn.close()
    return render_template('labor_issuance/edit.html', labor_issuance=labor_issuance, labor_resources=labor_resources)

@labor_issuance_bp.route('/labor-issuance/<int:id>/delete', methods=['POST'])
@role_required('Admin')
def delete_labor_issuance(id):
    db = Database()
    conn = db.get_connection()
    
    labor_issuance = conn.execute('SELECT * FROM labor_issuance WHERE id = ?', (id,)).fetchone()
    
    if not labor_issuance:
        conn.close()
        flash('Labor entry not found', 'danger')
        return redirect(url_for('workorder_routes.list_workorders'))
    
    task_id = labor_issuance['task_id']
    
    try:
        hours_worked = labor_issuance['hours_worked']
        labor_cost = labor_issuance['labor_cost']
        
        task = conn.execute('SELECT * FROM work_order_tasks WHERE id = ?', (task_id,)).fetchone()
        new_actual_hours = (task['actual_hours'] or 0) - hours_worked
        new_actual_cost = (task['actual_labor_cost'] or 0) - labor_cost
        
        conn.execute('''
            UPDATE work_order_tasks 
            SET actual_hours = ?, actual_labor_cost = ?
            WHERE id = ?
        ''', (max(0, new_actual_hours), max(0, new_actual_cost), task_id))
        
        wo = conn.execute('SELECT labor_cost FROM work_orders WHERE id = ?', 
                        (labor_issuance['work_order_id'],)).fetchone()
        new_wo_labor_cost = (wo['labor_cost'] or 0) - labor_cost
        
        conn.execute('''
            UPDATE work_orders 
            SET labor_cost = ?
            WHERE id = ?
        ''', (max(0, new_wo_labor_cost), labor_issuance['work_order_id']))
        
        conn.execute('DELETE FROM labor_issuance WHERE id = ?', (id,))
        
        conn.commit()
        conn.close()
        flash('Labor entry deleted successfully', 'success')
        return redirect(url_for('task_routes.view_task', task_id=task_id))
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error deleting labor entry: {str(e)}', 'danger')
        return redirect(url_for('task_routes.view_task', task_id=task_id))
