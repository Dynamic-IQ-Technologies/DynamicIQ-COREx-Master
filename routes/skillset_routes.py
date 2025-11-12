from flask import Blueprint, render_template, request, redirect, url_for, flash, Response
from models import Database
from auth import login_required, role_required
import csv
import io

skillset_bp = Blueprint('skillset_routes', __name__)

@skillset_bp.route('/skillsets')
@login_required
def list_skillsets():
    db = Database()
    conn = db.get_connection()
    skillsets = conn.execute('SELECT * FROM skillsets ORDER BY category, skillset_name').fetchall()
    conn.close()
    return render_template('skillsets/list.html', skillsets=skillsets)

@skillset_bp.route('/skillsets/create', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def create_skillset():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            skillset_name = request.form.get('skillset_name', '').strip()
            description = request.form.get('description', '').strip()
            category = request.form.get('category', '').strip()
            status = request.form.get('status', 'Active')
            
            if not skillset_name:
                flash('Skillset name is required', 'danger')
                conn.close()
                return render_template('skillsets/create.html')
            
            existing = conn.execute('SELECT id FROM skillsets WHERE skillset_name = ?', (skillset_name,)).fetchone()
            if existing:
                flash('A skillset with this name already exists', 'danger')
                conn.close()
                return render_template('skillsets/create.html')
            
            conn.execute('''
                INSERT INTO skillsets (skillset_name, description, category, status)
                VALUES (?, ?, ?, ?)
            ''', (skillset_name, description, category, status))
            
            conn.commit()
            conn.close()
            flash(f'Skillset "{skillset_name}" created successfully', 'success')
            return redirect(url_for('skillset_routes.list_skillsets'))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error creating skillset: {str(e)}', 'danger')
            return redirect(url_for('skillset_routes.create_skillset'))
    
    conn.close()
    return render_template('skillsets/create.html')

@skillset_bp.route('/skillsets/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def edit_skillset(id):
    db = Database()
    conn = db.get_connection()
    
    skillset = conn.execute('SELECT * FROM skillsets WHERE id = ?', (id,)).fetchone()
    
    if not skillset:
        conn.close()
        flash('Skillset not found', 'danger')
        return redirect(url_for('skillset_routes.list_skillsets'))
    
    if request.method == 'POST':
        try:
            skillset_name = request.form.get('skillset_name', '').strip()
            description = request.form.get('description', '').strip()
            category = request.form.get('category', '').strip()
            status = request.form.get('status', 'Active')
            
            if not skillset_name:
                flash('Skillset name is required', 'danger')
                conn.close()
                return render_template('skillsets/edit.html', skillset=skillset)
            
            existing = conn.execute('SELECT id FROM skillsets WHERE skillset_name = ? AND id != ?', (skillset_name, id)).fetchone()
            if existing:
                flash('A skillset with this name already exists', 'danger')
                conn.close()
                return render_template('skillsets/edit.html', skillset=skillset)
            
            conn.execute('''
                UPDATE skillsets 
                SET skillset_name = ?, description = ?, category = ?, status = ?
                WHERE id = ?
            ''', (skillset_name, description, category, status, id))
            
            conn.commit()
            conn.close()
            flash('Skillset updated successfully', 'success')
            return redirect(url_for('skillset_routes.list_skillsets'))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error updating skillset: {str(e)}', 'danger')
            return redirect(url_for('skillset_routes.edit_skillset', id=id))
    
    conn.close()
    return render_template('skillsets/edit.html', skillset=skillset)

@skillset_bp.route('/skillsets/<int:id>/delete', methods=['POST'])
@role_required('Admin')
def delete_skillset(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        usage_count = conn.execute('SELECT COUNT(*) as cnt FROM labor_resource_skills WHERE skillset_id = ?', (id,)).fetchone()
        
        if usage_count and usage_count['cnt'] > 0:
            flash('Cannot delete skillset that is assigned to labor resources. Please remove assignments first.', 'danger')
            conn.close()
            return redirect(url_for('skillset_routes.list_skillsets'))
        
        conn.execute('DELETE FROM skillsets WHERE id = ?', (id,))
        conn.commit()
        conn.close()
        flash('Skillset deleted successfully', 'success')
        return redirect(url_for('skillset_routes.list_skillsets'))
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error deleting skillset: {str(e)}', 'danger')
        return redirect(url_for('skillset_routes.list_skillsets'))

@skillset_bp.route('/skillsets/export')
@role_required('Admin', 'Planner')
def export_skillsets():
    db = Database()
    conn = db.get_connection()
    skillsets = conn.execute('SELECT * FROM skillsets ORDER BY category, skillset_name').fetchall()
    conn.close()
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Skillset Name', 'Description', 'Category', 'Status'])
    
    for skillset in skillsets:
        writer.writerow([
            skillset['skillset_name'],
            skillset['description'] or '',
            skillset['category'] or '',
            skillset['status']
        ])
    
    output.seek(0)
    return Response(
        output,
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=skillsets.csv'}
    )
