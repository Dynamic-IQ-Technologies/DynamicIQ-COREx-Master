from flask import Blueprint, render_template, request, redirect, url_for, flash, Response
from models import Database
from auth import login_required, role_required
import csv
import io

skillset_bp = Blueprint('skillset_routes', __name__)

SKILL_LEVEL_ORDER = {'Apprentice': 1, 'Intermediate': 2, 'Advanced': 3, 'Expert': 4}

@skillset_bp.route('/skillsets')
@login_required
def list_skillsets():
    db = Database()
    conn = db.get_connection()
    skillsets_raw = conn.execute('SELECT * FROM skillsets ORDER BY category, skillset_name').fetchall()
    
    skillsets = []
    fully_staffed = 0
    understaffed = 0
    critical_gaps = 0
    
    for s in skillsets_raw:
        skillset = dict(s)
        
        required_level = skillset.get('required_level') or 'Intermediate'
        required_order = SKILL_LEVEL_ORDER.get(required_level, 2)
        
        current_count = conn.execute('''
            SELECT COUNT(*) as cnt FROM labor_resource_skills 
            WHERE skillset_id = ?
        ''', (skillset['id'],)).fetchone()['cnt']
        
        qualified_count = conn.execute('''
            SELECT COUNT(*) as cnt FROM labor_resource_skills lrs
            JOIN labor_resources lr ON lrs.labor_resource_id = lr.id
            WHERE lrs.skillset_id = ? AND lr.status = 'Active'
            AND CASE lrs.skill_level
                WHEN 'Apprentice' THEN 1
                WHEN 'Intermediate' THEN 2
                WHEN 'Advanced' THEN 3
                WHEN 'Expert' THEN 4
                ELSE 2
            END >= ?
        ''', (skillset['id'], required_order)).fetchone()['cnt']
        
        skillset['current_count'] = current_count
        skillset['qualified_count'] = qualified_count
        
        target = skillset.get('target_headcount') or 0
        criticality = skillset.get('criticality') or 'Medium'
        
        if target > 0:
            if qualified_count >= target:
                fully_staffed += 1
            else:
                understaffed += 1
                if criticality in ['High', 'Critical']:
                    critical_gaps += 1
        
        skillsets.append(skillset)
    
    capacity_summary = {
        'total_skillsets': len(skillsets),
        'fully_staffed': fully_staffed,
        'understaffed': understaffed,
        'critical_gaps': critical_gaps
    }
    
    conn.close()
    return render_template('skillsets/list.html', skillsets=skillsets, capacity_summary=capacity_summary)

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
            required_level = request.form.get('required_level', 'Intermediate')
            target_headcount = int(request.form.get('target_headcount', 0) or 0)
            criticality = request.form.get('criticality', 'Medium')
            
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
                INSERT INTO skillsets (skillset_name, description, category, status, required_level, target_headcount, criticality)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (skillset_name, description, category, status, required_level, target_headcount, criticality))
            
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
            required_level = request.form.get('required_level', 'Intermediate')
            target_headcount = int(request.form.get('target_headcount', 0) or 0)
            criticality = request.form.get('criticality', 'Medium')
            
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
                SET skillset_name = ?, description = ?, category = ?, status = ?, required_level = ?, target_headcount = ?, criticality = ?
                WHERE id = ?
            ''', (skillset_name, description, category, status, required_level, target_headcount, criticality, id))
            
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
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Skillset Name', 'Description', 'Category', 'Status', 'Required Level', 'Target Headcount', 'Criticality', 'Current Count', 'Qualified Count'])
    
    for skillset in skillsets:
        required_level = skillset['required_level'] or 'Intermediate'
        required_order = SKILL_LEVEL_ORDER.get(required_level, 2)
        
        current_count = conn.execute('''
            SELECT COUNT(*) as cnt FROM labor_resource_skills 
            WHERE skillset_id = ?
        ''', (skillset['id'],)).fetchone()['cnt']
        
        qualified_count = conn.execute('''
            SELECT COUNT(*) as cnt FROM labor_resource_skills lrs
            JOIN labor_resources lr ON lrs.labor_resource_id = lr.id
            WHERE lrs.skillset_id = ? AND lr.status = 'Active'
            AND CASE lrs.skill_level
                WHEN 'Apprentice' THEN 1
                WHEN 'Intermediate' THEN 2
                WHEN 'Advanced' THEN 3
                WHEN 'Expert' THEN 4
                ELSE 2
            END >= ?
        ''', (skillset['id'], required_order)).fetchone()['cnt']
        
        writer.writerow([
            skillset['skillset_name'],
            skillset['description'] or '',
            skillset['category'] or '',
            skillset['status'],
            required_level,
            skillset['target_headcount'] or 0,
            skillset['criticality'] or 'Medium',
            current_count,
            qualified_count
        ])
    
    conn.close()
    output.seek(0)
    return Response(
        output,
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=skillsets.csv'}
    )
