from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify, Response
from models import Database
from functools import wraps
from datetime import datetime
import csv
import io

capability_bp = Blueprint('capability_routes', __name__)

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.', 'danger')
            return redirect(url_for('auth_routes.login'))
        return f(*args, **kwargs)
    return decorated_function

@capability_bp.route('/capabilities')
@login_required
def capability_list():
    """Display list of MRO capabilities with search and filter"""
    db = Database()
    conn = db.get_connection()
    
    search_query = request.args.get('search', '').strip()
    filter_category = request.args.get('category', '').strip()
    filter_status = request.args.get('status', '').strip()
    filter_manufacturer = request.args.get('manufacturer', '').strip()
    sort_by = request.args.get('sort_by', 'capability_code')
    sort_order = request.args.get('sort_order', 'ASC')
    
    allowed_sort_columns = ['capability_code', 'part_number', 'capability_name', 'category', 
                           'manufacturer', 'status', 'created_at']
    if sort_by not in allowed_sort_columns:
        sort_by = 'capability_code'
    
    if sort_order not in ['ASC', 'DESC']:
        sort_order = 'ASC'
    
    query = '''
        SELECT 
            mc.id, mc.capability_code, mc.part_number, mc.capability_name,
            mc.applicability, mc.part_class, mc.description, mc.category, 
            mc.manufacturer, mc.tolerance, mc.compliance, mc.certification_required, 
            mc.status, mc.notes, mc.created_at, mc.updated_at,
            p.name as product_name,
            u.username as created_by_username,
            (SELECT COUNT(*) FROM capability_specifications WHERE capability_id = mc.id) as spec_count
        FROM mro_capabilities mc
        LEFT JOIN products p ON mc.product_id = p.id
        LEFT JOIN users u ON mc.created_by = u.id
        WHERE 1=1
    '''
    
    params = []
    
    if search_query:
        query += ''' AND (
            mc.capability_code LIKE ? OR 
            mc.part_number LIKE ? OR 
            mc.capability_name LIKE ? OR 
            mc.description LIKE ?
        )'''
        search_param = f'%{search_query}%'
        params.extend([search_param, search_param, search_param, search_param])
    
    if filter_category:
        query += ' AND mc.category = ?'
        params.append(filter_category)
    
    if filter_status:
        query += ' AND mc.status = ?'
        params.append(filter_status)
    
    if filter_manufacturer:
        query += ' AND mc.manufacturer LIKE ?'
        params.append(f'%{filter_manufacturer}%')
    
    query += f' ORDER BY mc.{sort_by} {sort_order}'
    
    capabilities = conn.execute(query, params).fetchall()
    
    categories = conn.execute('''
        SELECT DISTINCT category FROM mro_capabilities 
        WHERE category IS NOT NULL AND category != ''
        ORDER BY category
    ''').fetchall()
    
    manufacturers = conn.execute('''
        SELECT DISTINCT manufacturer FROM mro_capabilities 
        WHERE manufacturer IS NOT NULL AND manufacturer != ''
        ORDER BY manufacturer
    ''').fetchall()
    
    conn.close()
    
    return render_template('capabilities/list.html',
                         capabilities=capabilities,
                         categories=categories,
                         manufacturers=manufacturers,
                         search_query=search_query,
                         filter_category=filter_category,
                         filter_status=filter_status,
                         filter_manufacturer=filter_manufacturer,
                         sort_by=sort_by,
                         sort_order=sort_order)

@capability_bp.route('/capabilities/new', methods=['GET', 'POST'])
@login_required
def capability_new():
    """Create a new capability"""
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        # Auto-generate capability code
        last_cap = conn.execute('''
            SELECT capability_code FROM mro_capabilities 
            WHERE capability_code LIKE 'CAP-%'
            ORDER BY CAST(SUBSTR(capability_code, 5) AS INTEGER) DESC 
            LIMIT 1
        ''').fetchone()
        
        if last_cap:
            try:
                last_number = int(last_cap['capability_code'].split('-')[1])
                next_number = last_number + 1
            except (ValueError, IndexError):
                next_number = 1
        else:
            next_number = 1
        
        capability_code = f'CAP-{next_number:04d}'
        
        part_number = request.form.get('part_number', '').strip()
        product_id = request.form.get('product_id', '').strip()
        capability_names = request.form.getlist('capability_name[]')
        capability_name = ', '.join(capability_names) if capability_names else ''
        applicability = request.form.get('applicability', '').strip()
        part_class = request.form.get('part_class', '').strip()
        description = request.form.get('description', '').strip()
        category = request.form.get('category', '').strip()
        manufacturer = request.form.get('manufacturer', '').strip()
        compliance = request.form.get('compliance', '').strip()
        certification_required = 1 if request.form.get('certification_required') else 0
        status = request.form.get('status', 'Active')
        notes = request.form.get('notes', '').strip()
        
        if not part_number or not capability_name:
            flash('Part Number and at least one Capability Name are required.', 'danger')
            conn.close()
            return redirect(url_for('capability_routes.capability_new'))
        
        product_id = int(product_id) if product_id else None
        
        try:
            conn.execute('''
                INSERT INTO mro_capabilities (
                    capability_code, part_number, product_id, capability_name,
                    applicability, part_class, description, category, manufacturer, 
                    compliance, certification_required, status, notes, 
                    created_by, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ''', (
                capability_code, part_number, product_id, capability_name,
                applicability, part_class, description, category, manufacturer, 
                compliance, certification_required, status, notes, session['user_id']
            ))
            
            capability_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
            
            spec_names = request.form.getlist('spec_name[]')
            spec_values = request.form.getlist('spec_value[]')
            spec_types = request.form.getlist('spec_type[]')
            spec_units = request.form.getlist('spec_unit[]')
            spec_min_values = request.form.getlist('spec_min_value[]')
            spec_max_values = request.form.getlist('spec_max_value[]')
            spec_criticals = request.form.getlist('spec_critical[]')
            
            for i, spec_name in enumerate(spec_names):
                if spec_name.strip():
                    spec_value = spec_values[i] if i < len(spec_values) else ''
                    spec_type = spec_types[i] if i < len(spec_types) else 'Text'
                    spec_unit = spec_units[i] if i < len(spec_units) else ''
                    spec_min = spec_min_values[i] if i < len(spec_min_values) and spec_min_values[i].strip() else None
                    spec_max = spec_max_values[i] if i < len(spec_max_values) and spec_max_values[i].strip() else None
                    is_critical = 1 if str(i) in spec_criticals else 0
                    
                    spec_min = float(spec_min) if spec_min else None
                    spec_max = float(spec_max) if spec_max else None
                    
                    conn.execute('''
                        INSERT INTO capability_specifications (
                            capability_id, spec_name, spec_value, spec_type, unit_of_measure,
                            min_value, max_value, is_critical, display_order,
                            modified_at, modified_by
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?)
                    ''', (
                        capability_id, spec_name.strip(), spec_value.strip(), spec_type,
                        spec_unit.strip(), spec_min, spec_max, is_critical, i, session['user_id']
                    ))
            
            conn.commit()
            flash(f'Capability {capability_code} created successfully!', 'success')
            conn.close()
            return redirect(url_for('capability_routes.capability_detail', capability_id=capability_id))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error creating capability: {str(e)}', 'danger')
            conn.close()
            return redirect(url_for('capability_routes.capability_new'))
    
    # GET request - generate next capability code for display
    last_cap = conn.execute('''
        SELECT capability_code FROM mro_capabilities 
        WHERE capability_code LIKE 'CAP-%'
        ORDER BY CAST(SUBSTR(capability_code, 5) AS INTEGER) DESC 
        LIMIT 1
    ''').fetchone()
    
    if last_cap:
        try:
            last_number = int(last_cap['capability_code'].split('-')[1])
            next_number = last_number + 1
        except (ValueError, IndexError):
            next_number = 1
    else:
        next_number = 1
    
    next_capability_code = f'CAP-{next_number:04d}'
    
    products = conn.execute('SELECT id, code, name FROM products ORDER BY code').fetchall()
    conn.close()
    
    return render_template('capabilities/form.html', 
                         capability=None, 
                         specifications=[], 
                         products=products,
                         next_capability_code=next_capability_code,
                         mode='new')

@capability_bp.route('/capabilities/<int:capability_id>')
@login_required
def capability_detail(capability_id):
    """View capability details"""
    db = Database()
    conn = db.get_connection()
    
    capability = conn.execute('''
        SELECT 
            mc.*,
            p.name as product_name,
            u.username as created_by_username,
            mu.username as modified_by_username
        FROM mro_capabilities mc
        LEFT JOIN products p ON mc.product_id = p.id
        LEFT JOIN users u ON mc.created_by = u.id
        LEFT JOIN users mu ON mc.modified_by = mu.id
        WHERE mc.id = ?
    ''', (capability_id,)).fetchone()
    
    if not capability:
        flash('Capability not found.', 'danger')
        conn.close()
        return redirect(url_for('capability_routes.capability_list'))
    
    specifications = conn.execute('''
        SELECT *
        FROM capability_specifications
        WHERE capability_id = ?
        ORDER BY display_order, spec_name
    ''', (capability_id,)).fetchall()
    
    conn.close()
    
    return render_template('capabilities/detail.html',
                         capability=capability,
                         specifications=specifications)

@capability_bp.route('/capabilities/<int:capability_id>/edit', methods=['GET', 'POST'])
@login_required
def capability_edit(capability_id):
    """Edit an existing capability"""
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        part_number = request.form.get('part_number', '').strip()
        product_id = request.form.get('product_id', '').strip()
        capability_names = request.form.getlist('capability_name[]')
        capability_name = ', '.join(capability_names) if capability_names else ''
        applicability = request.form.get('applicability', '').strip()
        part_class = request.form.get('part_class', '').strip()
        description = request.form.get('description', '').strip()
        category = request.form.get('category', '').strip()
        manufacturer = request.form.get('manufacturer', '').strip()
        compliance = request.form.get('compliance', '').strip()
        certification_required = 1 if request.form.get('certification_required') else 0
        status = request.form.get('status', 'Active')
        notes = request.form.get('notes', '').strip()
        
        if not part_number or not capability_name:
            flash('Part Number and at least one Capability Name are required.', 'danger')
            conn.close()
            return redirect(url_for('capability_routes.capability_edit', capability_id=capability_id))
        
        product_id = int(product_id) if product_id else None
        
        try:
            conn.execute('''
                UPDATE mro_capabilities
                SET part_number = ?, product_id = ?, capability_name = ?,
                    applicability = ?, part_class = ?, description = ?, category = ?, 
                    manufacturer = ?, compliance = ?, certification_required = ?, 
                    status = ?, notes = ?, updated_at = datetime('now'), modified_by = ?
                WHERE id = ?
            ''', (
                part_number, product_id, capability_name, applicability, part_class,
                description, category, manufacturer, compliance, 
                certification_required, status, notes, session['user_id'], capability_id
            ))
            
            conn.execute('DELETE FROM capability_specifications WHERE capability_id = ?', (capability_id,))
            
            spec_names = request.form.getlist('spec_name[]')
            spec_values = request.form.getlist('spec_value[]')
            spec_types = request.form.getlist('spec_type[]')
            spec_units = request.form.getlist('spec_unit[]')
            spec_min_values = request.form.getlist('spec_min_value[]')
            spec_max_values = request.form.getlist('spec_max_value[]')
            spec_criticals = request.form.getlist('spec_critical[]')
            
            for i, spec_name in enumerate(spec_names):
                if spec_name.strip():
                    spec_value = spec_values[i] if i < len(spec_values) else ''
                    spec_type = spec_types[i] if i < len(spec_types) else 'Text'
                    spec_unit = spec_units[i] if i < len(spec_units) else ''
                    spec_min = spec_min_values[i] if i < len(spec_min_values) and spec_min_values[i].strip() else None
                    spec_max = spec_max_values[i] if i < len(spec_max_values) and spec_max_values[i].strip() else None
                    is_critical = 1 if str(i) in spec_criticals else 0
                    
                    spec_min = float(spec_min) if spec_min else None
                    spec_max = float(spec_max) if spec_max else None
                    
                    conn.execute('''
                        INSERT INTO capability_specifications (
                            capability_id, spec_name, spec_value, spec_type, unit_of_measure,
                            min_value, max_value, is_critical, display_order,
                            modified_at, modified_by
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?)
                    ''', (
                        capability_id, spec_name.strip(), spec_value.strip(), spec_type,
                        spec_unit.strip(), spec_min, spec_max, is_critical, i, session['user_id']
                    ))
            
            conn.commit()
            flash('Capability updated successfully!', 'success')
            conn.close()
            return redirect(url_for('capability_routes.capability_detail', capability_id=capability_id))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error updating capability: {str(e)}', 'danger')
            conn.close()
            return redirect(url_for('capability_routes.capability_edit', capability_id=capability_id))
    
    capability = conn.execute('SELECT * FROM mro_capabilities WHERE id = ?', (capability_id,)).fetchone()
    
    if not capability:
        flash('Capability not found.', 'danger')
        conn.close()
        return redirect(url_for('capability_routes.capability_list'))
    
    specifications = conn.execute('''
        SELECT * FROM capability_specifications 
        WHERE capability_id = ? 
        ORDER BY display_order, spec_name
    ''', (capability_id,)).fetchall()
    
    products = conn.execute('SELECT id, code, name FROM products ORDER BY code').fetchall()
    
    conn.close()
    
    return render_template('capabilities/form.html',
                         capability=capability,
                         specifications=specifications,
                         products=products,
                         mode='edit')

@capability_bp.route('/capabilities/<int:capability_id>/delete', methods=['POST'])
@login_required
def capability_delete(capability_id):
    """Delete a capability"""
    db = Database()
    conn = db.get_connection()
    
    capability = conn.execute(
        'SELECT capability_code FROM mro_capabilities WHERE id = ?', 
        (capability_id,)
    ).fetchone()
    
    if not capability:
        flash('Capability not found.', 'danger')
        conn.close()
        return redirect(url_for('capability_routes.capability_list'))
    
    try:
        conn.execute('DELETE FROM mro_capabilities WHERE id = ?', (capability_id,))
        conn.commit()
        flash(f'Capability {capability["capability_code"]} deleted successfully.', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting capability: {str(e)}', 'danger')
    
    conn.close()
    return redirect(url_for('capability_routes.capability_list'))

@capability_bp.route('/capabilities/export')
@login_required
def capability_export():
    """Export capabilities list to CSV"""
    db = Database()
    conn = db.get_connection()
    
    search_query = request.args.get('search', '').strip()
    filter_category = request.args.get('category', '').strip()
    filter_status = request.args.get('status', '').strip()
    filter_manufacturer = request.args.get('manufacturer', '').strip()
    
    query = '''
        SELECT 
            mc.capability_code, mc.part_number, mc.capability_name,
            mc.applicability, mc.part_class, mc.description, mc.category, 
            mc.manufacturer, mc.tolerance, mc.compliance, 
            CASE WHEN mc.certification_required = 1 THEN 'Yes' ELSE 'No' END as certification_required,
            mc.status, mc.notes, mc.created_at, mc.updated_at,
            p.name as product_name
        FROM mro_capabilities mc
        LEFT JOIN products p ON mc.product_id = p.id
        WHERE 1=1
    '''
    
    params = []
    
    if search_query:
        query += ''' AND (
            mc.capability_code LIKE ? OR 
            mc.part_number LIKE ? OR 
            mc.capability_name LIKE ? OR 
            mc.description LIKE ?
        )'''
        search_param = f'%{search_query}%'
        params.extend([search_param, search_param, search_param, search_param])
    
    if filter_category:
        query += ' AND mc.category = ?'
        params.append(filter_category)
    
    if filter_status:
        query += ' AND mc.status = ?'
        params.append(filter_status)
    
    if filter_manufacturer:
        query += ' AND mc.manufacturer LIKE ?'
        params.append(f'%{filter_manufacturer}%')
    
    query += ' ORDER BY mc.capability_code'
    
    capabilities = conn.execute(query, params).fetchall()
    conn.close()
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    writer.writerow([
        'Capability Code', 'Part Number', 'Capability Name', 'Description',
        'Category', 'Manufacturer', 'Tolerance', 'Compliance', 
        'Certification Required', 'Status', 'Product Name', 'Notes',
        'Created At', 'Updated At'
    ])
    
    for cap in capabilities:
        writer.writerow([
            cap['capability_code'], cap['part_number'], cap['capability_name'],
            cap['description'] or '', cap['category'] or '', cap['manufacturer'] or '',
            cap['tolerance'] or '', cap['compliance'] or '', cap['certification_required'],
            cap['status'], cap['product_name'] or '', cap['notes'] or '',
            cap['created_at'], cap['updated_at']
        ])
    
    output.seek(0)
    
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename=mro_capabilities_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'}
    )
