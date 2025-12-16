from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session
from models import Database
from auth import login_required, role_required
from datetime import datetime

task_template_bp = Blueprint('task_template_routes', __name__)

TASK_CATEGORIES = ['General', 'Inspection', 'Assembly', 'Testing', 'Quality Control', 'Documentation', 'Packaging', 'Shipping']
PRIORITY_LEVELS = ['Low', 'Medium', 'High', 'Critical']

def generate_template_code(conn):
    last_template = conn.execute('SELECT template_code FROM task_templates ORDER BY id DESC LIMIT 1').fetchone()
    if last_template:
        last_number = int(last_template['template_code'].split('-')[1])
        new_number = last_number + 1
    else:
        new_number = 1
    return f'TPL-{new_number:06d}'


@task_template_bp.route('/task-templates')
@login_required
def list_templates():
    db = Database()
    conn = db.get_connection()
    
    templates = conn.execute('''
        SELECT tt.*, 
               u.username as created_by_name,
               (SELECT COUNT(*) FROM task_template_items WHERE template_id = tt.id) as item_count
        FROM task_templates tt
        LEFT JOIN users u ON tt.created_by = u.id
        ORDER BY tt.template_name
    ''').fetchall()
    
    conn.close()
    return render_template('task_templates/list.html', templates=templates, categories=TASK_CATEGORIES)


@task_template_bp.route('/task-templates/create', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def create_template():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            template_name = request.form.get('template_name', '').strip()
            description = request.form.get('description', '').strip()
            category = request.form.get('category', 'General')
            
            if not template_name:
                flash('Template name is required', 'danger')
                conn.close()
                return render_template('task_templates/create.html', categories=TASK_CATEGORIES)
            
            template_code = generate_template_code(conn)
            user_id = session.get('user_id')
            
            conn.execute('''
                INSERT INTO task_templates (template_code, template_name, description, category, created_by)
                VALUES (?, ?, ?, ?, ?)
            ''', (template_code, template_name, description, category, user_id))
            
            conn.commit()
            template_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
            conn.close()
            
            flash(f'Template {template_code} created successfully', 'success')
            return redirect(url_for('task_template_routes.edit_template', template_id=template_id))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error creating template: {str(e)}', 'danger')
            return redirect(url_for('task_template_routes.create_template'))
    
    conn.close()
    return render_template('task_templates/create.html', categories=TASK_CATEGORIES)


@task_template_bp.route('/task-templates/<int:template_id>')
@login_required
def view_template(template_id):
    db = Database()
    conn = db.get_connection()
    
    template = conn.execute('''
        SELECT tt.*, u.username as created_by_name
        FROM task_templates tt
        LEFT JOIN users u ON tt.created_by = u.id
        WHERE tt.id = ?
    ''', (template_id,)).fetchone()
    
    if not template:
        conn.close()
        flash('Template not found', 'danger')
        return redirect(url_for('task_template_routes.list_templates'))
    
    items = conn.execute('''
        SELECT * FROM task_template_items
        WHERE template_id = ?
        ORDER BY sequence_number, id
    ''', (template_id,)).fetchall()
    
    conn.close()
    return render_template('task_templates/view.html', template=template, items=items)


@task_template_bp.route('/task-templates/<int:template_id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def edit_template(template_id):
    db = Database()
    conn = db.get_connection()
    
    template = conn.execute('SELECT * FROM task_templates WHERE id = ?', (template_id,)).fetchone()
    
    if not template:
        conn.close()
        flash('Template not found', 'danger')
        return redirect(url_for('task_template_routes.list_templates'))
    
    if request.method == 'POST':
        try:
            template_name = request.form.get('template_name', '').strip()
            description = request.form.get('description', '').strip()
            category = request.form.get('category', 'General')
            status = request.form.get('status', 'Active')
            
            if not template_name:
                flash('Template name is required', 'danger')
                items = conn.execute('SELECT * FROM task_template_items WHERE template_id = ? ORDER BY sequence_number', (template_id,)).fetchall()
                conn.close()
                return render_template('task_templates/edit.html', template=template, items=items, 
                                       categories=TASK_CATEGORIES, priorities=PRIORITY_LEVELS)
            
            conn.execute('''
                UPDATE task_templates 
                SET template_name = ?, description = ?, category = ?, status = ?, modified_at = ?
                WHERE id = ?
            ''', (template_name, description, category, status, datetime.now(), template_id))
            
            conn.commit()
            flash('Template updated successfully', 'success')
            
        except Exception as e:
            conn.rollback()
            flash(f'Error updating template: {str(e)}', 'danger')
    
    template = conn.execute('SELECT * FROM task_templates WHERE id = ?', (template_id,)).fetchone()
    items = conn.execute('SELECT * FROM task_template_items WHERE template_id = ? ORDER BY sequence_number, id', (template_id,)).fetchall()
    conn.close()
    
    return render_template('task_templates/edit.html', template=template, items=items, 
                           categories=TASK_CATEGORIES, priorities=PRIORITY_LEVELS)


@task_template_bp.route('/task-templates/<int:template_id>/delete', methods=['POST'])
@role_required('Admin')
def delete_template(template_id):
    db = Database()
    conn = db.get_connection()
    
    template = conn.execute('SELECT * FROM task_templates WHERE id = ?', (template_id,)).fetchone()
    
    if not template:
        conn.close()
        flash('Template not found', 'danger')
        return redirect(url_for('task_template_routes.list_templates'))
    
    try:
        conn.execute('DELETE FROM task_template_items WHERE template_id = ?', (template_id,))
        conn.execute('DELETE FROM task_templates WHERE id = ?', (template_id,))
        conn.commit()
        conn.close()
        flash(f'Template {template["template_code"]} deleted successfully', 'success')
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error deleting template: {str(e)}', 'danger')
    
    return redirect(url_for('task_template_routes.list_templates'))


@task_template_bp.route('/task-templates/<int:template_id>/items/add', methods=['POST'])
@role_required('Admin', 'Planner')
def add_template_item(template_id):
    db = Database()
    conn = db.get_connection()
    
    template = conn.execute('SELECT * FROM task_templates WHERE id = ?', (template_id,)).fetchone()
    
    if not template:
        conn.close()
        return jsonify({'success': False, 'message': 'Template not found'}), 404
    
    try:
        task_name = request.form.get('task_name', '').strip()
        description = request.form.get('description', '').strip()
        category = request.form.get('category', 'General')
        sequence_number = int(request.form.get('sequence_number', 0))
        priority = request.form.get('priority', 'Medium')
        planned_hours = float(request.form.get('planned_hours', 0))
        remarks = request.form.get('remarks', '').strip()
        
        if not task_name:
            conn.close()
            flash('Task name is required', 'danger')
            return redirect(url_for('task_template_routes.edit_template', template_id=template_id))
        
        conn.execute('''
            INSERT INTO task_template_items 
            (template_id, task_name, description, category, sequence_number, priority, planned_hours, remarks)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (template_id, task_name, description, category, sequence_number, priority, planned_hours, remarks))
        
        conn.commit()
        conn.close()
        flash('Task item added successfully', 'success')
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error adding task item: {str(e)}', 'danger')
    
    return redirect(url_for('task_template_routes.edit_template', template_id=template_id))


@task_template_bp.route('/task-templates/items/<int:item_id>/edit', methods=['POST'])
@role_required('Admin', 'Planner')
def edit_template_item(item_id):
    db = Database()
    conn = db.get_connection()
    
    item = conn.execute('SELECT * FROM task_template_items WHERE id = ?', (item_id,)).fetchone()
    
    if not item:
        conn.close()
        flash('Task item not found', 'danger')
        return redirect(url_for('task_template_routes.list_templates'))
    
    try:
        task_name = request.form.get('task_name', '').strip()
        description = request.form.get('description', '').strip()
        category = request.form.get('category', 'General')
        sequence_number = int(request.form.get('sequence_number', 0))
        priority = request.form.get('priority', 'Medium')
        planned_hours = float(request.form.get('planned_hours', 0))
        remarks = request.form.get('remarks', '').strip()
        
        if not task_name:
            conn.close()
            flash('Task name is required', 'danger')
            return redirect(url_for('task_template_routes.edit_template', template_id=item['template_id']))
        
        conn.execute('''
            UPDATE task_template_items 
            SET task_name = ?, description = ?, category = ?, sequence_number = ?, 
                priority = ?, planned_hours = ?, remarks = ?
            WHERE id = ?
        ''', (task_name, description, category, sequence_number, priority, planned_hours, remarks, item_id))
        
        conn.commit()
        conn.close()
        flash('Task item updated successfully', 'success')
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error updating task item: {str(e)}', 'danger')
    
    return redirect(url_for('task_template_routes.edit_template', template_id=item['template_id']))


@task_template_bp.route('/task-templates/items/<int:item_id>/delete', methods=['POST'])
@role_required('Admin', 'Planner')
def delete_template_item(item_id):
    db = Database()
    conn = db.get_connection()
    
    item = conn.execute('SELECT * FROM task_template_items WHERE id = ?', (item_id,)).fetchone()
    
    if not item:
        conn.close()
        flash('Task item not found', 'danger')
        return redirect(url_for('task_template_routes.list_templates'))
    
    template_id = item['template_id']
    
    try:
        conn.execute('DELETE FROM task_template_items WHERE id = ?', (item_id,))
        conn.commit()
        conn.close()
        flash('Task item deleted successfully', 'success')
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error deleting task item: {str(e)}', 'danger')
    
    return redirect(url_for('task_template_routes.edit_template', template_id=template_id))


@task_template_bp.route('/task-templates/api/list')
@login_required
def api_list_templates():
    db = Database()
    conn = db.get_connection()
    
    templates = conn.execute('''
        SELECT tt.id, tt.template_code, tt.template_name, tt.category,
               (SELECT COUNT(*) FROM task_template_items WHERE template_id = tt.id) as item_count
        FROM task_templates tt
        WHERE tt.status = 'Active'
        ORDER BY tt.template_name
    ''').fetchall()
    
    conn.close()
    
    return jsonify([dict(t) for t in templates])
