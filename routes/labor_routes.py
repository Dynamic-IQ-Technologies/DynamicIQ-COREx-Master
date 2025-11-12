from flask import Blueprint, render_template, request, redirect, url_for, flash, Response
from models import Database
from auth import login_required, role_required
import csv
import io
import math

labor_bp = Blueprint('labor_routes', __name__)

SKILL_LEVELS = ['Apprentice', 'Intermediate', 'Advanced', 'Expert']

def generate_employee_code(conn):
    last_employee = conn.execute('SELECT employee_code FROM labor_resources ORDER BY id DESC LIMIT 1').fetchone()
    if last_employee:
        last_number = int(last_employee['employee_code'].split('-')[1])
        new_number = last_number + 1
    else:
        new_number = 1
    return f'EMP-{new_number:06d}'

@labor_bp.route('/labor-resources')
@login_required
def list_labor_resources():
    db = Database()
    conn = db.get_connection()
    labor_resources = conn.execute('SELECT * FROM labor_resources ORDER BY last_name, first_name').fetchall()
    conn.close()
    return render_template('labor/list.html', labor_resources=labor_resources)

@labor_bp.route('/labor-resources/create', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def create_labor_resource():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            first_name = request.form.get('first_name', '').strip()
            last_name = request.form.get('last_name', '').strip()
            role = request.form.get('role', '').strip()
            hourly_rate = float(request.form.get('hourly_rate', 0))
            cost_center = request.form.get('cost_center', '').strip()
            email = request.form.get('email', '').strip()
            phone = request.form.get('phone', '').strip()
            status = request.form.get('status', 'Active')
            
            if not first_name:
                flash('First name is required', 'danger')
                skillsets = conn.execute('SELECT * FROM skillsets WHERE status = "Active" ORDER BY skillset_name').fetchall()
                conn.close()
                return render_template('labor/create.html', skillsets=skillsets, skill_levels=SKILL_LEVELS)
            
            if not last_name:
                flash('Last name is required', 'danger')
                skillsets = conn.execute('SELECT * FROM skillsets WHERE status = "Active" ORDER BY skillset_name').fetchall()
                conn.close()
                return render_template('labor/create.html', skillsets=skillsets, skill_levels=SKILL_LEVELS)
            
            if not role:
                flash('Role is required', 'danger')
                skillsets = conn.execute('SELECT * FROM skillsets WHERE status = "Active" ORDER BY skillset_name').fetchall()
                conn.close()
                return render_template('labor/create.html', skillsets=skillsets, skill_levels=SKILL_LEVELS)
            
            if not math.isfinite(hourly_rate) or hourly_rate < 0:
                flash('Hourly rate must be a valid non-negative number', 'danger')
                skillsets = conn.execute('SELECT * FROM skillsets WHERE status = "Active" ORDER BY skillset_name').fetchall()
                conn.close()
                return render_template('labor/create.html', skillsets=skillsets, skill_levels=SKILL_LEVELS)
            
            skillset_ids = request.form.getlist('skillset_id[]')
            skill_levels = request.form.getlist('skill_level[]')
            
            if len(skillset_ids) != len(skill_levels):
                flash('Invalid skillset data submitted', 'danger')
                skillsets = conn.execute('SELECT * FROM skillsets WHERE status = "Active" ORDER BY skillset_name').fetchall()
                conn.close()
                return render_template('labor/create.html', skillsets=skillsets, skill_levels=SKILL_LEVELS)
            
            for i in range(len(skillset_ids)):
                if skillset_ids[i] and not skill_levels[i]:
                    flash('Each selected skillset must have a skill level', 'danger')
                    skillsets = conn.execute('SELECT * FROM skillsets WHERE status = "Active" ORDER BY skillset_name').fetchall()
                    conn.close()
                    return render_template('labor/create.html', skillsets=skillsets, skill_levels=SKILL_LEVELS)
                
                if skill_levels[i] and skill_levels[i] not in SKILL_LEVELS:
                    flash(f'Invalid skill level: {skill_levels[i]}', 'danger')
                    skillsets = conn.execute('SELECT * FROM skillsets WHERE status = "Active" ORDER BY skillset_name').fetchall()
                    conn.close()
                    return render_template('labor/create.html', skillsets=skillsets, skill_levels=SKILL_LEVELS)
            
            employee_code = generate_employee_code(conn)
            
            conn.execute('''
                INSERT INTO labor_resources 
                (employee_code, first_name, last_name, role, hourly_rate, 
                 cost_center, email, phone, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (employee_code, first_name, last_name, role, hourly_rate,
                  cost_center, email, phone, status))
            
            labor_resource_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
            
            for i in range(len(skillset_ids)):
                if skillset_ids[i] and skill_levels[i]:
                    skillset_id = int(skillset_ids[i])
                    skill_level = skill_levels[i]
                    
                    existing = conn.execute('''
                        SELECT id FROM labor_resource_skills 
                        WHERE labor_resource_id = ? AND skillset_id = ?
                    ''', (labor_resource_id, skillset_id)).fetchone()
                    
                    if not existing:
                        conn.execute('''
                            INSERT INTO labor_resource_skills (labor_resource_id, skillset_id, skill_level)
                            VALUES (?, ?, ?)
                        ''', (labor_resource_id, skillset_id, skill_level))
            
            conn.commit()
            conn.close()
            flash(f'Employee {employee_code} created successfully', 'success')
            return redirect(url_for('labor_routes.list_labor_resources'))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error creating employee: {str(e)}', 'danger')
            return redirect(url_for('labor_routes.create_labor_resource'))
    
    skillsets = conn.execute('SELECT * FROM skillsets WHERE status = "Active" ORDER BY skillset_name').fetchall()
    conn.close()
    return render_template('labor/create.html', skillsets=skillsets, skill_levels=SKILL_LEVELS)

@labor_bp.route('/labor-resources/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def edit_labor_resource(id):
    db = Database()
    conn = db.get_connection()
    
    labor_resource = conn.execute('SELECT * FROM labor_resources WHERE id = ?', (id,)).fetchone()
    
    if not labor_resource:
        conn.close()
        flash('Employee not found', 'danger')
        return redirect(url_for('labor_routes.list_labor_resources'))
    
    if request.method == 'POST':
        try:
            first_name = request.form.get('first_name', '').strip()
            last_name = request.form.get('last_name', '').strip()
            role = request.form.get('role', '').strip()
            hourly_rate = float(request.form.get('hourly_rate', 0))
            cost_center = request.form.get('cost_center', '').strip()
            email = request.form.get('email', '').strip()
            phone = request.form.get('phone', '').strip()
            status = request.form.get('status', 'Active')
            
            if not first_name:
                flash('First name is required', 'danger')
                skillsets = conn.execute('SELECT * FROM skillsets WHERE status = "Active" ORDER BY skillset_name').fetchall()
                existing_skills = conn.execute('''
                    SELECT lrs.*, s.skillset_name 
                    FROM labor_resource_skills lrs
                    JOIN skillsets s ON lrs.skillset_id = s.id
                    WHERE lrs.labor_resource_id = ?
                ''', (id,)).fetchall()
                conn.close()
                return render_template('labor/edit.html', labor_resource=labor_resource, skillsets=skillsets, 
                                     existing_skills=existing_skills, skill_levels=SKILL_LEVELS)
            
            if not last_name:
                flash('Last name is required', 'danger')
                skillsets = conn.execute('SELECT * FROM skillsets WHERE status = "Active" ORDER BY skillset_name').fetchall()
                existing_skills = conn.execute('''
                    SELECT lrs.*, s.skillset_name 
                    FROM labor_resource_skills lrs
                    JOIN skillsets s ON lrs.skillset_id = s.id
                    WHERE lrs.labor_resource_id = ?
                ''', (id,)).fetchall()
                conn.close()
                return render_template('labor/edit.html', labor_resource=labor_resource, skillsets=skillsets,
                                     existing_skills=existing_skills, skill_levels=SKILL_LEVELS)
            
            if not role:
                flash('Role is required', 'danger')
                skillsets = conn.execute('SELECT * FROM skillsets WHERE status = "Active" ORDER BY skillset_name').fetchall()
                existing_skills = conn.execute('''
                    SELECT lrs.*, s.skillset_name 
                    FROM labor_resource_skills lrs
                    JOIN skillsets s ON lrs.skillset_id = s.id
                    WHERE lrs.labor_resource_id = ?
                ''', (id,)).fetchall()
                conn.close()
                return render_template('labor/edit.html', labor_resource=labor_resource, skillsets=skillsets,
                                     existing_skills=existing_skills, skill_levels=SKILL_LEVELS)
            
            if not math.isfinite(hourly_rate) or hourly_rate < 0:
                flash('Hourly rate must be a valid non-negative number', 'danger')
                skillsets = conn.execute('SELECT * FROM skillsets WHERE status = "Active" ORDER BY skillset_name').fetchall()
                existing_skills = conn.execute('''
                    SELECT lrs.*, s.skillset_name 
                    FROM labor_resource_skills lrs
                    JOIN skillsets s ON lrs.skillset_id = s.id
                    WHERE lrs.labor_resource_id = ?
                ''', (id,)).fetchall()
                conn.close()
                return render_template('labor/edit.html', labor_resource=labor_resource, skillsets=skillsets,
                                     existing_skills=existing_skills, skill_levels=SKILL_LEVELS)
            
            skillset_ids = request.form.getlist('skillset_id[]')
            skill_levels = request.form.getlist('skill_level[]')
            
            if len(skillset_ids) != len(skill_levels):
                flash('Invalid skillset data submitted', 'danger')
                skillsets = conn.execute('SELECT * FROM skillsets WHERE status = "Active" ORDER BY skillset_name').fetchall()
                existing_skills = conn.execute('''
                    SELECT lrs.*, s.skillset_name 
                    FROM labor_resource_skills lrs
                    JOIN skillsets s ON lrs.skillset_id = s.id
                    WHERE lrs.labor_resource_id = ?
                ''', (id,)).fetchall()
                conn.close()
                return render_template('labor/edit.html', labor_resource=labor_resource, skillsets=skillsets,
                                     existing_skills=existing_skills, skill_levels=SKILL_LEVELS)
            
            for i in range(len(skillset_ids)):
                if skillset_ids[i] and not skill_levels[i]:
                    flash('Each selected skillset must have a skill level', 'danger')
                    skillsets = conn.execute('SELECT * FROM skillsets WHERE status = "Active" ORDER BY skillset_name').fetchall()
                    existing_skills = conn.execute('''
                        SELECT lrs.*, s.skillset_name 
                        FROM labor_resource_skills lrs
                        JOIN skillsets s ON lrs.skillset_id = s.id
                        WHERE lrs.labor_resource_id = ?
                    ''', (id,)).fetchall()
                    conn.close()
                    return render_template('labor/edit.html', labor_resource=labor_resource, skillsets=skillsets,
                                         existing_skills=existing_skills, skill_levels=SKILL_LEVELS)
                
                if skill_levels[i] and skill_levels[i] not in SKILL_LEVELS:
                    flash(f'Invalid skill level: {skill_levels[i]}', 'danger')
                    skillsets = conn.execute('SELECT * FROM skillsets WHERE status = "Active" ORDER BY skillset_name').fetchall()
                    existing_skills = conn.execute('''
                        SELECT lrs.*, s.skillset_name 
                        FROM labor_resource_skills lrs
                        JOIN skillsets s ON lrs.skillset_id = s.id
                        WHERE lrs.labor_resource_id = ?
                    ''', (id,)).fetchall()
                    conn.close()
                    return render_template('labor/edit.html', labor_resource=labor_resource, skillsets=skillsets,
                                         existing_skills=existing_skills, skill_levels=SKILL_LEVELS)
            
            conn.execute('''
                UPDATE labor_resources 
                SET first_name = ?, last_name = ?, role = ?, hourly_rate = ?,
                    cost_center = ?, email = ?, phone = ?, status = ?
                WHERE id = ?
            ''', (first_name, last_name, role, hourly_rate, cost_center,
                  email, phone, status, id))
            
            conn.execute('DELETE FROM labor_resource_skills WHERE labor_resource_id = ?', (id,))
            
            for i in range(len(skillset_ids)):
                if skillset_ids[i] and skill_levels[i]:
                    skillset_id = int(skillset_ids[i])
                    skill_level = skill_levels[i]
                    
                    conn.execute('''
                        INSERT INTO labor_resource_skills (labor_resource_id, skillset_id, skill_level)
                        VALUES (?, ?, ?)
                    ''', (id, skillset_id, skill_level))
            
            conn.commit()
            conn.close()
            flash('Employee updated successfully', 'success')
            return redirect(url_for('labor_routes.list_labor_resources'))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error updating employee: {str(e)}', 'danger')
            return redirect(url_for('labor_routes.edit_labor_resource', id=id))
    
    skillsets = conn.execute('SELECT * FROM skillsets WHERE status = "Active" ORDER BY skillset_name').fetchall()
    existing_skills = conn.execute('''
        SELECT lrs.*, s.skillset_name 
        FROM labor_resource_skills lrs
        JOIN skillsets s ON lrs.skillset_id = s.id
        WHERE lrs.labor_resource_id = ?
    ''', (id,)).fetchall()
    conn.close()
    return render_template('labor/edit.html', labor_resource=labor_resource, skillsets=skillsets, 
                         existing_skills=existing_skills, skill_levels=SKILL_LEVELS)

@labor_bp.route('/labor-resources/<int:id>/delete', methods=['POST'])
@role_required('Admin')
def delete_labor_resource(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        task_count = conn.execute('SELECT COUNT(*) as cnt FROM work_order_tasks WHERE assigned_resource_id = ?', (id,)).fetchone()
        labor_count = conn.execute('SELECT COUNT(*) as cnt FROM labor_issuance WHERE resource_id = ?', (id,)).fetchone()
        
        if (task_count and task_count['cnt'] > 0) or (labor_count and labor_count['cnt'] > 0):
            flash('Cannot delete employee with existing task assignments or labor entries', 'danger')
            conn.close()
            return redirect(url_for('labor_routes.list_labor_resources'))
        
        conn.execute('DELETE FROM labor_resources WHERE id = ?', (id,))
        conn.commit()
        conn.close()
        flash('Employee deleted successfully', 'success')
        return redirect(url_for('labor_routes.list_labor_resources'))
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error deleting employee: {str(e)}', 'danger')
        return redirect(url_for('labor_routes.list_labor_resources'))

@labor_bp.route('/labor-resources/<int:id>')
@login_required
def view_labor_resource(id):
    db = Database()
    conn = db.get_connection()
    
    labor_resource = conn.execute('SELECT * FROM labor_resources WHERE id = ?', (id,)).fetchone()
    
    if not labor_resource:
        conn.close()
        flash('Employee not found', 'danger')
        return redirect(url_for('labor_routes.list_labor_resources'))
    
    assigned_skills = conn.execute('''
        SELECT lrs.*, s.skillset_name, s.category
        FROM labor_resource_skills lrs
        JOIN skillsets s ON lrs.skillset_id = s.id
        WHERE lrs.labor_resource_id = ?
        ORDER BY s.skillset_name
    ''', (id,)).fetchall()
    
    tasks = conn.execute('''
        SELECT t.*, wo.wo_number, p.code, p.name
        FROM work_order_tasks t
        JOIN work_orders wo ON t.work_order_id = wo.id
        JOIN products p ON wo.product_id = p.id
        WHERE t.assigned_resource_id = ?
        ORDER BY t.planned_start_date DESC
    ''', (id,)).fetchall()
    
    labor_entries = conn.execute('''
        SELECT li.*, wo.wo_number, t.task_name
        FROM labor_issuance li
        JOIN work_orders wo ON li.work_order_id = wo.id
        JOIN work_order_tasks t ON li.task_id = t.id
        WHERE li.resource_id = ?
        ORDER BY li.work_date DESC
        LIMIT 50
    ''', (id,)).fetchall()
    
    conn.close()
    return render_template('labor/view.html', labor_resource=labor_resource, assigned_skills=assigned_skills,
                         tasks=tasks, labor_entries=labor_entries)

@labor_bp.route('/labor-resources/export')
@role_required('Admin', 'Planner')
def export_labor_resources():
    db = Database()
    conn = db.get_connection()
    labor_resources = conn.execute('SELECT * FROM labor_resources ORDER BY last_name, first_name').fetchall()
    conn.close()
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Employee Code', 'First Name', 'Last Name', 'Role', 'Skillset', 
                     'Hourly Rate', 'Cost Center', 'Email', 'Phone', 'Status'])
    
    for resource in labor_resources:
        writer.writerow([
            resource['employee_code'],
            resource['first_name'],
            resource['last_name'],
            resource['role'],
            resource['skillset'] or '',
            resource['hourly_rate'],
            resource['cost_center'] or '',
            resource['email'] or '',
            resource['phone'] or '',
            resource['status']
        ])
    
    output.seek(0)
    return Response(
        output,
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=labor_resources.csv'}
    )
