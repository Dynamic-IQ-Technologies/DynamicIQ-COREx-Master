from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import Database
from auth import login_required, role_required

uom_bp = Blueprint('uom_routes', __name__)

@uom_bp.route('/uom')
@login_required
@role_required('Admin')
def list_uoms():
    db = Database()
    conn = db.get_connection()
    
    status_filter = request.args.get('status', 'all')
    
    if status_filter == 'active':
        uoms = conn.execute('''
            SELECT u.*, b.uom_code as base_uom_code, b.uom_name as base_uom_name
            FROM uom_master u
            LEFT JOIN uom_master b ON u.base_uom_id = b.id
            WHERE u.is_active = 1
            ORDER BY u.uom_type, u.uom_code
        ''').fetchall()
    elif status_filter == 'inactive':
        uoms = conn.execute('''
            SELECT u.*, b.uom_code as base_uom_code, b.uom_name as base_uom_name
            FROM uom_master u
            LEFT JOIN uom_master b ON u.base_uom_id = b.id
            WHERE u.is_active = 0
            ORDER BY u.uom_type, u.uom_code
        ''').fetchall()
    else:
        uoms = conn.execute('''
            SELECT u.*, b.uom_code as base_uom_code, b.uom_name as base_uom_name
            FROM uom_master u
            LEFT JOIN uom_master b ON u.base_uom_id = b.id
            ORDER BY u.uom_type, u.uom_code
        ''').fetchall()
    
    conn.close()
    return render_template('uom/list.html', uoms=uoms, status_filter=status_filter)

@uom_bp.route('/uom/create', methods=['GET', 'POST'])
@login_required
@role_required('Admin')
def create_uom():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            uom_code = request.form['uom_code'].strip().upper()
            uom_name = request.form['uom_name'].strip()
            uom_type = request.form.get('uom_type', '').strip()
            conversion_factor_str = request.form.get('conversion_factor', '').strip()
            conversion_factor = float(conversion_factor_str) if conversion_factor_str else 1.0
            base_uom_id = request.form.get('base_uom_id') or None
            rounding_precision_str = request.form.get('rounding_precision', '').strip()
            rounding_precision = int(rounding_precision_str) if rounding_precision_str else 2
            description = request.form.get('description', '').strip()
            
            if conversion_factor <= 0:
                flash('Conversion factor must be greater than zero.', 'danger')
                conn.close()
                return redirect(url_for('uom_routes.create_uom'))
            
            existing = conn.execute('SELECT id FROM uom_master WHERE uom_code = ?', (uom_code,)).fetchone()
            if existing:
                flash(f'UOM code "{uom_code}" already exists.', 'danger')
                conn.close()
                return redirect(url_for('uom_routes.create_uom'))
            
            conn.execute('''
                INSERT INTO uom_master 
                (uom_code, uom_name, uom_type, conversion_factor, base_uom_id, rounding_precision, is_active, description, created_by)
                VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
            ''', (uom_code, uom_name, uom_type, conversion_factor, base_uom_id, rounding_precision, description, session.get('user_id')))
            
            conn.commit()
            conn.close()
            flash(f'UOM "{uom_code}" created successfully!', 'success')
            return redirect(url_for('uom_routes.list_uoms'))
            
        except Exception as e:
            conn.close()
            flash(f'Error creating UOM: {str(e)}', 'danger')
            return redirect(url_for('uom_routes.create_uom'))
    
    base_uoms = conn.execute('''
        SELECT id, uom_code, uom_name, uom_type 
        FROM uom_master 
        WHERE is_active = 1
        ORDER BY uom_type, uom_code
    ''').fetchall()
    
    uom_types = ['Count', 'Weight', 'Volume', 'Length', 'Time', 'Area', 'Other']
    
    conn.close()
    return render_template('uom/create.html', base_uoms=base_uoms, uom_types=uom_types)

@uom_bp.route('/uom/<int:id>')
@login_required
@role_required('Admin')
def view_uom(id):
    db = Database()
    conn = db.get_connection()
    
    uom = conn.execute('''
        SELECT u.*, b.uom_code as base_uom_code, b.uom_name as base_uom_name,
               creator.username as created_by_name, modifier.username as modified_by_name
        FROM uom_master u
        LEFT JOIN uom_master b ON u.base_uom_id = b.id
        LEFT JOIN users creator ON u.created_by = creator.id
        LEFT JOIN users modifier ON u.modified_by = modifier.id
        WHERE u.id = ?
    ''', (id,)).fetchone()
    
    if not uom:
        flash('UOM not found.', 'danger')
        conn.close()
        return redirect(url_for('uom_routes.list_uoms'))
    
    products = conn.execute('''
        SELECT p.code, p.name, puc.conversion_factor, puc.is_base_uom, puc.is_purchase_uom, puc.is_issue_uom
        FROM product_uom_conversions puc
        JOIN products p ON puc.product_id = p.id
        WHERE puc.uom_id = ?
        ORDER BY p.code
    ''', (id,)).fetchall()
    
    conn.close()
    return render_template('uom/view.html', uom=uom, products=products)

@uom_bp.route('/uom/<int:id>/edit', methods=['GET', 'POST'])
@login_required
@role_required('Admin')
def edit_uom(id):
    db = Database()
    conn = db.get_connection()
    
    uom = conn.execute('SELECT * FROM uom_master WHERE id = ?', (id,)).fetchone()
    if not uom:
        flash('UOM not found.', 'danger')
        conn.close()
        return redirect(url_for('uom_routes.list_uoms'))
    
    if request.method == 'POST':
        try:
            uom_name = request.form['uom_name'].strip()
            uom_type = request.form.get('uom_type', '').strip()
            rounding_precision_str = request.form.get('rounding_precision', '').strip()
            rounding_precision = int(rounding_precision_str) if rounding_precision_str else 2
            conversion_factor_str = request.form.get('conversion_factor', '').strip()
            conversion_factor = float(conversion_factor_str) if conversion_factor_str else 1.0
            base_uom_id = request.form.get('base_uom_id') or None
            description = request.form.get('description', '').strip()
            
            if conversion_factor <= 0:
                flash('Conversion factor must be greater than zero.', 'danger')
                conn.close()
                return redirect(url_for('uom_routes.edit_uom', id=id))
            
            conn.execute('''
                UPDATE uom_master 
                SET uom_name = ?, uom_type = ?, rounding_precision = ?, conversion_factor = ?, 
                    base_uom_id = ?, description = ?, modified_by = ?, modified_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (uom_name, uom_type, rounding_precision, conversion_factor, base_uom_id, description, session.get('user_id'), id))
            
            conn.commit()
            conn.close()
            flash(f'UOM "{uom["uom_code"]}" updated successfully!', 'success')
            return redirect(url_for('uom_routes.view_uom', id=id))
            
        except Exception as e:
            conn.close()
            flash(f'Error updating UOM: {str(e)}', 'danger')
            return redirect(url_for('uom_routes.edit_uom', id=id))
    
    base_uoms = conn.execute('''
        SELECT id, uom_code, uom_name, uom_type 
        FROM uom_master 
        WHERE is_active = 1 AND id != ?
        ORDER BY uom_type, uom_code
    ''', (id,)).fetchall()
    
    uom_types = ['Count', 'Weight', 'Volume', 'Length', 'Time', 'Area', 'Other']
    
    conn.close()
    return render_template('uom/edit.html', uom=uom, base_uoms=base_uoms, uom_types=uom_types)

@uom_bp.route('/uom/<int:id>/deactivate', methods=['POST'])
@login_required
@role_required('Admin')
def deactivate_uom(id):
    db = Database()
    conn = db.get_connection()
    
    usage = conn.execute('SELECT COUNT(*) as count FROM product_uom_conversions WHERE uom_id = ?', (id,)).fetchone()
    if usage['count'] > 0:
        flash('Cannot deactivate UOM. It is used in product conversions.', 'danger')
        conn.close()
        return redirect(url_for('uom_routes.view_uom', id=id))
    
    conn.execute('''
        UPDATE uom_master 
        SET is_active = 0, modified_by = ?, modified_at = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (session.get('user_id'), id))
    
    conn.commit()
    conn.close()
    flash('UOM deactivated successfully.', 'success')
    return redirect(url_for('uom_routes.list_uoms'))

@uom_bp.route('/uom/<int:id>/activate', methods=['POST'])
@login_required
@role_required('Admin')
def activate_uom(id):
    db = Database()
    conn = db.get_connection()
    
    conn.execute('''
        UPDATE uom_master 
        SET is_active = 1, modified_by = ?, modified_at = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (session.get('user_id'), id))
    
    conn.commit()
    conn.close()
    flash('UOM activated successfully.', 'success')
    return redirect(url_for('uom_routes.view_uom', id=id))
