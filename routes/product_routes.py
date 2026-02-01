from flask import Blueprint, render_template, request, redirect, url_for, flash, Response, jsonify, session
from models import Database, AuditLogger
from auth import login_required, role_required
import csv
import io
import os
import json

product_bp = Blueprint('product_routes', __name__)

@product_bp.route('/products')
@login_required
def list_products():
    db = Database()
    conn = db.get_connection()
    
    # Get filter parameters
    search = request.args.get('search', '').strip()
    product_type = request.args.get('product_type', '').strip()
    uom = request.args.get('uom', '').strip()
    min_cost = request.args.get('min_cost', '').strip()
    max_cost = request.args.get('max_cost', '').strip()
    
    # Build query with filters
    query = 'SELECT * FROM products WHERE 1=1'
    params = []
    
    if search:
        query += ' AND (code LIKE ? OR name LIKE ? OR description LIKE ?)'
        search_pattern = f'%{search}%'
        params.extend([search_pattern, search_pattern, search_pattern])
    
    if product_type:
        query += ' AND product_type = ?'
        params.append(product_type)
    
    if uom:
        query += ' AND unit_of_measure = ?'
        params.append(uom)
    
    if min_cost:
        try:
            query += ' AND cost >= ?'
            params.append(float(min_cost))
        except ValueError:
            pass
    
    if max_cost:
        try:
            query += ' AND cost <= ?'
            params.append(float(max_cost))
        except ValueError:
            pass
    
    query += ' ORDER BY code'
    
    products = conn.execute(query, params).fetchall()
    
    # Get distinct values for filter dropdowns
    product_types = conn.execute('SELECT DISTINCT product_type FROM products ORDER BY product_type').fetchall()
    uoms = conn.execute('SELECT DISTINCT unit_of_measure FROM products ORDER BY unit_of_measure').fetchall()
    
    conn.close()
    
    return render_template('products/list.html', 
                         products=products,
                         product_types=product_types,
                         uoms=uoms,
                         filters={
                             'search': search,
                             'product_type': product_type,
                             'uom': uom,
                             'min_cost': min_cost,
                             'max_cost': max_cost
                         })

@product_bp.route('/products/list-json')
@login_required
def list_products_json():
    db = Database()
    conn = db.get_connection()
    products = conn.execute('SELECT id, code, name, product_type FROM products ORDER BY code').fetchall()
    conn.close()
    return jsonify([dict(p) for p in products])

@product_bp.route('/api/products/quick-create', methods=['POST'])
@login_required
def quick_create_product():
    try:
        data = request.get_json()
        code = data.get('code', '').strip()
        name = data.get('name', '').strip()
        description = data.get('description', '').strip()
        category = data.get('category', 'Component')
        cost = float(data.get('cost', 0))
        
        if not code or not name:
            return jsonify({'success': False, 'error': 'Part number and name are required'})
        
        db = Database()
        conn = db.get_connection()
        
        existing = conn.execute('SELECT id FROM products WHERE code = ?', (code,)).fetchone()
        if existing:
            conn.close()
            return jsonify({'success': False, 'error': f'Product with code {code} already exists'})
        
        cursor = conn.execute('''
            INSERT INTO products (code, name, description, part_category, cost, product_type, unit_of_measure)
            VALUES (?, ?, ?, ?, ?, 'Standard', 'EA')
        ''', (code, name, description, category, cost))
        
        product_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'product': {
                'id': product_id,
                'code': code,
                'name': name
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@product_bp.route('/products/create', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def create_product():
    if request.method == 'POST':
        db = Database()
        conn = db.get_connection()
        
        part_code = request.form['code']
        
        # Check if part code already exists
        existing = conn.execute('SELECT id FROM products WHERE code = ?', (part_code,)).fetchone()
        if existing:
            conn.close()
            flash(f'Part code "{part_code}" already exists. Please use a different code.', 'warning')
            return redirect(url_for('product_routes.create_product'))
        
        try:
            conn.execute('''
                INSERT INTO products (code, name, description, unit_of_measure, product_type, part_category, lead_time, product_category, manufacturer, cost,
                                      applicability, shelf_life_cycle, eccn, part_notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                part_code,
                request.form['name'],
                request.form['description'],
                request.form['unit_of_measure'],
                request.form['product_type'],
                request.form.get('part_category', 'Other'),
                int(request.form.get('lead_time', 0) or 0),
                request.form.get('product_category', ''),
                request.form.get('manufacturer', ''),
                0.0,
                request.form.get('applicability', ''),
                request.form.get('shelf_life_cycle', ''),
                request.form.get('eccn', ''),
                request.form.get('part_notes', '')
            ))
            
            product_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
            
            conn.execute('''
                INSERT INTO inventory (product_id, quantity, reorder_point, safety_stock)
                VALUES (?, 0, ?, ?)
            ''', (product_id, float(request.form.get('reorder_point', 0)), float(request.form.get('safety_stock', 0))))
            
            # Log audit trail
            AuditLogger.log_change(
                conn=conn,
                record_type='product',
                record_id=product_id,
                action_type='Created',
                modified_by=session.get('user_id'),
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
            
            conn.commit()
            conn.close()
            
            flash('Product created successfully!', 'success')
            return redirect(url_for('product_routes.list_products'))
        except Exception as e:
            conn.rollback()
            conn.close()
            error_msg = str(e)
            if 'UNIQUE constraint failed' in error_msg or 'unique constraint' in error_msg.lower():
                flash(f'Part code "{part_code}" already exists. Please use a different code.', 'warning')
            else:
                flash(f'Error creating product: {error_msg}', 'danger')
            return redirect(url_for('product_routes.create_product'))
    
    return render_template('products/create.html')

@product_bp.route('/products/<int:id>')
@login_required
def view_product(id):
    db = Database()
    conn = db.get_connection()
    
    product = conn.execute('SELECT * FROM products WHERE id = ?', (id,)).fetchone()
    if not product:
        conn.close()
        flash('Product not found', 'danger')
        return redirect(url_for('product_routes.list_products'))
    
    inventory = conn.execute('SELECT * FROM inventory WHERE product_id = ?', (id,)).fetchone()
    
    bom_usage = conn.execute('''
        SELECT b.*, p.code as parent_code, p.name as parent_name
        FROM boms b
        JOIN products p ON b.parent_product_id = p.id
        WHERE b.child_product_id = ?
        ORDER BY p.code
    ''', (id,)).fetchall()
    
    bom_components = conn.execute('''
        SELECT b.*, p.code as component_code, p.name as component_name, p.unit_of_measure
        FROM boms b
        JOIN products p ON b.child_product_id = p.id
        WHERE b.parent_product_id = ?
        ORDER BY b.find_number
    ''', (id,)).fetchall()
    
    recent_work_orders = conn.execute('''
        SELECT wo.*, p.code as product_code
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.product_id = ?
        ORDER BY wo.created_at DESC
        LIMIT 10
    ''', (id,)).fetchall()
    
    recent_po_lines = conn.execute('''
        SELECT pol.*, po.po_number, s.name as supplier_name
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        LEFT JOIN suppliers s ON po.supplier_id = s.id
        WHERE pol.product_id = ?
        ORDER BY po.order_date DESC
        LIMIT 10
    ''', (id,)).fetchall()
    
    uom_conversions = conn.execute('''
        SELECT puc.*, um.uom_code, um.uom_name 
        FROM product_uom_conversions puc
        JOIN uom_master um ON puc.uom_id = um.id
        WHERE puc.product_id = ? AND puc.is_active = 1
        ORDER BY um.uom_code
    ''', (id,)).fetchall()
    
    alternates = conn.execute('''
        SELECT pa.*, p.code as alternate_code, p.name as alternate_name, 
               p.product_type as alternate_type, p.unit_of_measure as alternate_uom,
               i.quantity as alternate_qty_on_hand
        FROM product_alternates pa
        JOIN products p ON pa.alternate_product_id = p.id
        LEFT JOIN inventory i ON p.id = i.product_id
        WHERE pa.product_id = ? AND pa.is_active = 1
        ORDER BY pa.priority, p.code
    ''', (id,)).fetchall()
    
    reverse_alternates = conn.execute('''
        SELECT pa.*, p.code as primary_code, p.name as primary_name,
               p.product_type as primary_type
        FROM product_alternates pa
        JOIN products p ON pa.product_id = p.id
        WHERE pa.alternate_product_id = ? AND pa.is_active = 1
        ORDER BY p.code
    ''', (id,)).fetchall()
    
    product_files = conn.execute('''
        SELECT * FROM product_files 
        WHERE product_id = ? AND is_active = 1
        ORDER BY uploaded_at DESC
    ''', (id,)).fetchall()
    
    audit_trail = conn.execute('''
        SELECT at.*, u.username 
        FROM audit_trail at
        LEFT JOIN users u ON at.modified_by = u.id
        WHERE at.record_type = 'product' AND at.record_id = ?
        ORDER BY at.modified_at DESC
        LIMIT 50
    ''', (str(id),)).fetchall()
    
    conn.close()
    
    return render_template('products/view.html',
                          product=product,
                          inventory=inventory,
                          bom_usage=bom_usage,
                          bom_components=bom_components,
                          recent_work_orders=recent_work_orders,
                          recent_po_lines=recent_po_lines,
                          uom_conversions=uom_conversions,
                          alternates=alternates,
                          reverse_alternates=reverse_alternates,
                          product_files=product_files,
                          audit_trail=audit_trail)

@product_bp.route('/products/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def edit_product(id):
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        # Get old records for audit (product and inventory)
        old_product = conn.execute('SELECT * FROM products WHERE id=?', (id,)).fetchone()
        old_inventory = conn.execute('SELECT * FROM inventory WHERE product_id=?', (id,)).fetchone()
        
        conn.execute('''
            UPDATE products 
            SET code=?, name=?, description=?, unit_of_measure=?, product_type=?, part_category=?, lead_time=?, product_category=?, manufacturer=?,
                applicability=?, shelf_life_cycle=?, eccn=?, part_notes=?, is_serialized=?, calibration_required=?, master_plan_part=?, non_inventory=?
            WHERE id=?
        ''', (
            request.form['code'],
            request.form['name'],
            request.form['description'],
            request.form['unit_of_measure'],
            request.form['product_type'],
            request.form.get('part_category', 'Other'),
            int(request.form.get('lead_time', 0) or 0),
            request.form.get('product_category', ''),
            request.form.get('manufacturer', ''),
            request.form.get('applicability', ''),
            request.form.get('shelf_life_cycle', ''),
            request.form.get('eccn', ''),
            request.form.get('part_notes', ''),
            1 if request.form.get('is_serialized') else 0,
            1 if request.form.get('calibration_required') else 0,
            1 if request.form.get('master_plan_part') else 0,
            1 if request.form.get('non_inventory') else 0,
            id
        ))
        
        conn.execute('''
            UPDATE inventory 
            SET reorder_point=?, safety_stock=?
            WHERE product_id=?
        ''', (float(request.form.get('reorder_point', 0)), float(request.form.get('safety_stock', 0)), id))
        
        # Get new records for audit
        new_product = conn.execute('SELECT * FROM products WHERE id=?', (id,)).fetchone()
        new_inventory = conn.execute('SELECT * FROM inventory WHERE product_id=?', (id,)).fetchone()
        
        # Build combined changes dictionary
        all_changes = {}
        
        # Product changes
        product_changes = AuditLogger.compare_records(dict(old_product), dict(new_product))
        if product_changes:
            all_changes.update(product_changes)
        
        # Inventory changes (with prefixed field names for clarity)
        if old_inventory and new_inventory:
            inventory_changes = AuditLogger.compare_records(dict(old_inventory), dict(new_inventory))
            if inventory_changes:
                for key, value in inventory_changes.items():
                    if key in ['reorder_point', 'safety_stock']:
                        all_changes[key] = value
        
        # Log audit trail with all changes
        if all_changes:
            AuditLogger.log_change(
                conn=conn,
                record_type='product',
                record_id=id,
                action_type='Updated',
                modified_by=session.get('user_id'),
                changed_fields=all_changes,
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
        
        conn.commit()
        conn.close()
        
        flash('Product updated successfully!', 'success')
        return redirect(url_for('product_routes.list_products'))
    
    product = conn.execute('SELECT * FROM products WHERE id=?', (id,)).fetchone()
    inventory = conn.execute('SELECT * FROM inventory WHERE product_id=?', (id,)).fetchone()
    
    # Get all active UOMs for dropdown
    uoms = conn.execute('SELECT * FROM uom_master WHERE is_active = 1 ORDER BY uom_type, uom_code').fetchall()
    
    conn.close()
    
    return render_template('products/edit.html', product=product, inventory=inventory, uoms=uoms)

@product_bp.route('/products/<int:id>/delete', methods=['POST'])
@role_required('Admin')
def delete_product(id):
    db = Database()
    conn = db.get_connection()
    
    # Get product details for audit before deleting
    product = conn.execute('SELECT * FROM products WHERE id=?', (id,)).fetchone()
    
    # Log audit trail before deletion
    if product:
        AuditLogger.log_change(
            conn=conn,
            record_type='product',
            record_id=id,
            action_type='Deleted',
            modified_by=session.get('user_id'),
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
    
    conn.execute('DELETE FROM products WHERE id=?', (id,))
    conn.commit()
    conn.close()
    
    flash('Product deleted successfully!', 'success')
    return redirect(url_for('product_routes.list_products'))

@product_bp.route('/products/mass-update', methods=['POST'])
@role_required('Admin', 'Planner')
def mass_update_products():
    product_ids_str = request.form.get('product_ids', '')
    if not product_ids_str:
        flash('No products selected for update.', 'warning')
        return redirect(url_for('product_routes.list_products'))
    
    try:
        product_ids = [int(pid.strip()) for pid in product_ids_str.split(',') if pid.strip()]
    except ValueError:
        flash('Invalid product selection.', 'danger')
        return redirect(url_for('product_routes.list_products'))
    
    if not product_ids:
        flash('No products selected for update.', 'warning')
        return redirect(url_for('product_routes.list_products'))
    
    updates = {}
    update_fields = ['product_type', 'unit_of_measure', 'part_category', 'lead_time', 
                     'manufacturer', 'cost', 'product_category']
    
    for field in update_fields:
        value = request.form.get(field, '').strip()
        if value:
            if field == 'lead_time':
                try:
                    updates[field] = int(value)
                except ValueError:
                    continue
            elif field == 'cost':
                try:
                    updates[field] = float(value)
                except ValueError:
                    continue
            else:
                updates[field] = value
    
    if not updates:
        flash('No fields to update. Please select at least one field to change.', 'warning')
        return redirect(url_for('product_routes.list_products'))
    
    db = Database()
    conn = db.get_connection()
    
    set_clause = ', '.join([f'{k} = ?' for k in updates.keys()])
    placeholders = ','.join(['?' for _ in product_ids])
    
    conn.execute(
        f'UPDATE products SET {set_clause} WHERE id IN ({placeholders})',
        list(updates.values()) + product_ids
    )
    
    for pid in product_ids:
        AuditLogger.log_change(
            conn=conn,
            record_type='product',
            record_id=pid,
            action_type='Mass Updated',
            modified_by=session.get('user_id'),
            changed_fields=updates,
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
    
    conn.commit()
    conn.close()
    
    flash(f'Successfully updated {len(product_ids)} product(s).', 'success')
    return redirect(url_for('product_routes.list_products'))

@product_bp.route('/products/export')
@login_required
def export_products():
    db = Database()
    conn = db.get_connection()
    products = conn.execute('SELECT * FROM products ORDER BY code').fetchall()
    conn.close()
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Code', 'Name', 'Description', 'Unit of Measure', 'Product Type', 'Cost',
                     'Part Category', 'Lead Time', 'Product Category', 'Manufacturer', 
                     'Applicability', 'Shelf Life Cycle', 'ECCN', 'Part Notes',
                     'Is Serialized', 'Calibration Required', 'Master Plan Part'])
    
    for product in products:
        writer.writerow([
            product['code'], 
            product['name'], 
            product['description'], 
            product['unit_of_measure'], 
            product['product_type'], 
            product['cost'],
            product.get('part_category') or '',
            product.get('lead_time') or '',
            product.get('product_category') or '',
            product.get('manufacturer') or '',
            product.get('applicability') or '',
            product.get('shelf_life_cycle') or '',
            product.get('eccn') or '',
            product.get('part_notes') or '',
            1 if product.get('is_serialized') else 0,
            1 if product.get('calibration_required') else 0,
            1 if product.get('master_plan_part') else 0
        ])
    
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=products_export.csv'}
    )

@product_bp.route('/products/template')
@login_required
def download_template():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Code', 'Name', 'Description', 'Unit of Measure', 'Product Type', 'Cost',
                     'Part Category', 'Lead Time', 'Product Category', 'Manufacturer', 
                     'Applicability', 'Shelf Life Cycle', 'ECCN', 'Part Notes',
                     'Is Serialized', 'Calibration Required', 'Master Plan Part'])
    
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=product_import_template.csv'}
    )

@product_bp.route('/products/import', methods=['POST'])
@role_required('Admin', 'Planner')
def import_products():
    if 'file' not in request.files:
        flash('No file uploaded', 'danger')
        return redirect(url_for('product_routes.list_products'))
    
    file = request.files['file']
    if not file or not file.filename:
        flash('No file selected', 'danger')
        return redirect(url_for('product_routes.list_products'))
    
    if not file.filename.lower().endswith('.csv'):
        flash('Please upload a CSV file', 'danger')
        return redirect(url_for('product_routes.list_products'))
    
    db = Database()
    conn = None
    
    try:
        stream = io.StringIO(file.stream.read().decode('UTF8'), newline=None)
        csv_reader = csv.DictReader(stream)
        
        conn = db.get_connection()
        
        imported_count = 0
        skipped_count = 0
        errors = []
        
        for row_num, row in enumerate(csv_reader, start=2):
            try:
                code = row.get('Code', '').strip()
                name = row.get('Name', '').strip()
                description = row.get('Description', '').strip()
                unit_of_measure = row.get('Unit of Measure', '').strip()
                product_type = row.get('Product Type', '').strip()
                cost_str = row.get('Cost', '').strip()
                
                part_category = row.get('Part Category', '').strip() or 'Other'
                lead_time_str = row.get('Lead Time', '').strip()
                product_category = row.get('Product Category', '').strip()
                manufacturer = row.get('Manufacturer', '').strip()
                applicability = row.get('Applicability', '').strip()
                shelf_life_cycle = row.get('Shelf Life Cycle', '').strip()
                eccn = row.get('ECCN', '').strip()
                part_notes = row.get('Part Notes', '').strip()
                is_serialized_str = row.get('Is Serialized', '').strip()
                calibration_required_str = row.get('Calibration Required', '').strip()
                master_plan_part_str = row.get('Master Plan Part', '').strip()
                
                if not code or not name or not unit_of_measure or not product_type:
                    skipped_count += 1
                    errors.append(f"Row {row_num}: Missing required fields")
                    continue
                
                try:
                    cost = float(cost_str) if cost_str else 0.0
                    lead_time = int(lead_time_str) if lead_time_str else 0
                    is_serialized = 1 if is_serialized_str.lower() in ['1', 'yes', 'true'] else 0
                    calibration_required = 1 if calibration_required_str.lower() in ['1', 'yes', 'true'] else 0
                    master_plan_part = 1 if master_plan_part_str.lower() in ['1', 'yes', 'true'] else 0
                except ValueError:
                    skipped_count += 1
                    errors.append(f"Row {row_num}: Invalid number format")
                    continue
                
                existing = conn.execute('SELECT id FROM products WHERE code = ?', (code,)).fetchone()
                
                if existing:
                    conn.execute('''
                        UPDATE products 
                        SET name=?, description=?, unit_of_measure=?, product_type=?, cost=?,
                            part_category=?, lead_time=?, product_category=?, manufacturer=?,
                            applicability=?, shelf_life_cycle=?, eccn=?, part_notes=?,
                            is_serialized=?, calibration_required=?, master_plan_part=?
                        WHERE code=?
                    ''', (name, description, unit_of_measure, product_type, cost,
                          part_category, lead_time, product_category, manufacturer,
                          applicability, shelf_life_cycle, eccn, part_notes,
                          is_serialized, calibration_required, master_plan_part, code))
                else:
                    conn.execute('''
                        INSERT INTO products (code, name, description, unit_of_measure, product_type, cost,
                                             part_category, lead_time, product_category, manufacturer,
                                             applicability, shelf_life_cycle, eccn, part_notes,
                                             is_serialized, calibration_required, master_plan_part)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (code, name, description, unit_of_measure, product_type, cost,
                          part_category, lead_time, product_category, manufacturer,
                          applicability, shelf_life_cycle, eccn, part_notes,
                          is_serialized, calibration_required, master_plan_part))
                    
                    product_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
                    
                    conn.execute('''
                        INSERT INTO inventory (product_id, quantity, reorder_point, safety_stock)
                        VALUES (?, 0, 0, 0)
                    ''', (product_id,))
                
                imported_count += 1
            except Exception as row_error:
                skipped_count += 1
                errors.append(f"Row {row_num}: {str(row_error)}")
        
        conn.commit()
        
        if imported_count > 0:
            flash(f'Successfully imported {imported_count} products. Skipped {skipped_count} rows.', 'success')
        else:
            flash(f'No products imported. Skipped {skipped_count} rows.', 'warning')
        
        if errors and len(errors) <= 10:
            for error in errors:
                flash(error, 'warning')
        elif errors:
            flash(f'First 10 errors: {"; ".join(errors[:10])}', 'warning')
            
    except Exception as e:
        flash(f'Error importing products: {str(e)}', 'danger')
    finally:
        if conn:
            conn.close()
    
    return redirect(url_for('product_routes.list_products'))

# UOM Conversion Management Routes
@product_bp.route('/products/<int:product_id>/uom-conversions')
@login_required
def get_product_uom_conversions(product_id):
    """Get active UOM conversions for a product"""
    db = Database()
    conn = db.get_connection()
    
    show_all = request.args.get('show_all', 'false').lower() == 'true'
    
    if show_all:
        # Get all versions
        conversions = conn.execute('''
            SELECT puc.*, u.uom_code, u.uom_name, u.uom_type
            FROM product_uom_conversions puc
            JOIN uom_master u ON puc.uom_id = u.id
            WHERE puc.product_id = ?
            ORDER BY u.uom_code, puc.version_number DESC
        ''', (product_id,)).fetchall()
    else:
        # Get only active versions
        conversions = conn.execute('''
            SELECT puc.*, u.uom_code, u.uom_name, u.uom_type
            FROM product_uom_conversions puc
            JOIN uom_master u ON puc.uom_id = u.id
            WHERE puc.product_id = ? AND puc.is_active = 1
            ORDER BY puc.is_base_uom DESC, u.uom_code
        ''', (product_id,)).fetchall()
    
    conn.close()
    return jsonify([dict(row) for row in conversions])

@product_bp.route('/products/<int:product_id>/uom-conversions/add', methods=['POST'])
@role_required('Admin', 'Planner')
def add_product_uom_conversion(product_id):
    """Add a new UOM conversion for a product or create a new version"""
    db = Database()
    conn = db.get_connection()
    
    try:
        from datetime import datetime
        data = request.get_json()
        uom_id = int(data.get('uom_id'))
        conversion_factor = float(data.get('conversion_factor', 1.0))
        is_base_uom = int(data.get('is_base_uom', 0))
        is_purchase_uom = int(data.get('is_purchase_uom', 0))
        is_issue_uom = int(data.get('is_issue_uom', 0))
        create_version = data.get('create_version', False)
        effective_date = data.get('effective_date', datetime.now().strftime('%Y-%m-%d'))
        version_notes = data.get('version_notes', '')
        
        # Validate UOM exists
        uom = conn.execute('SELECT * FROM uom_master WHERE id = ?', (uom_id,)).fetchone()
        if not uom:
            conn.close()
            return jsonify({'error': 'Invalid UOM'}), 400
        
        # If this is being set as base UOM, unset any existing base UOM
        if is_base_uom:
            conn.execute('''
                UPDATE product_uom_conversions 
                SET is_base_uom = 0 
                WHERE product_id = ? AND is_base_uom = 1 AND is_active = 1
            ''', (product_id,))
            # Base UOM should always have conversion factor of 1.0
            conversion_factor = 1.0
        
        # Check if active conversion already exists for this product-UOM combination
        existing_active = conn.execute('''
            SELECT * FROM product_uom_conversions 
            WHERE product_id = ? AND uom_id = ? AND is_active = 1
        ''', (product_id, uom_id)).fetchone()
        
        if existing_active and create_version:
            # Create a new version
            # Get the latest version number
            max_version = conn.execute('''
                SELECT COALESCE(MAX(version_number), 0) 
                FROM product_uom_conversions 
                WHERE product_id = ? AND uom_id = ?
            ''', (product_id, uom_id)).fetchone()[0]
            
            new_version = max_version + 1
            
            # Deactivate all existing versions
            conn.execute('''
                UPDATE product_uom_conversions
                SET is_active = 0
                WHERE product_id = ? AND uom_id = ?
            ''', (product_id, uom_id))
            
            # Insert new version
            conn.execute('''
                INSERT INTO product_uom_conversions 
                (product_id, uom_id, conversion_factor, is_base_uom, is_purchase_uom, is_issue_uom, 
                 version_number, effective_date, is_active, version_notes, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            ''', (product_id, uom_id, conversion_factor, is_base_uom, is_purchase_uom, is_issue_uom, 
                  new_version, effective_date, version_notes, session.get('user_id')))
            
            action_type = 'Created Version ' + str(new_version)
            
        elif existing_active:
            # Update existing active version
            conn.execute('''
                UPDATE product_uom_conversions
                SET conversion_factor = ?, is_base_uom = ?, is_purchase_uom = ?, is_issue_uom = ?
                WHERE product_id = ? AND uom_id = ? AND is_active = 1
            ''', (conversion_factor, is_base_uom, is_purchase_uom, is_issue_uom, product_id, uom_id))
            
            action_type = 'Updated'
        else:
            # Insert new (first version)
            conn.execute('''
                INSERT INTO product_uom_conversions 
                (product_id, uom_id, conversion_factor, is_base_uom, is_purchase_uom, is_issue_uom, 
                 version_number, effective_date, is_active, version_notes, created_by)
                VALUES (?, ?, ?, ?, ?, ?, 1, ?, 1, ?, ?)
            ''', (product_id, uom_id, conversion_factor, is_base_uom, is_purchase_uom, is_issue_uom, 
                  effective_date, version_notes, session.get('user_id')))
            
            action_type = 'Created'
        
        # Log audit trail
        AuditLogger.log_change(
            conn=conn,
            record_type='product_uom_conversion',
            record_id=product_id,
            action_type=action_type,
            modified_by=session.get('user_id'),
            changed_fields={'uom_id': uom_id, 'conversion_factor': conversion_factor, 'is_base_uom': is_base_uom, 'version_notes': version_notes},
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'UOM conversion saved successfully'})
        
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500

@product_bp.route('/products/<int:product_id>/uom-conversions/<int:uom_id>/versions')
@login_required
def get_uom_conversion_versions(product_id, uom_id):
    """Get version history for a specific product-UOM conversion"""
    db = Database()
    conn = db.get_connection()
    
    versions = conn.execute('''
        SELECT puc.*, u.uom_code, u.uom_name, u.uom_type,
               usr.username as created_by_name
        FROM product_uom_conversions puc
        JOIN uom_master u ON puc.uom_id = u.id
        LEFT JOIN users usr ON puc.created_by = usr.id
        WHERE puc.product_id = ? AND puc.uom_id = ?
        ORDER BY puc.version_number DESC
    ''', (product_id, uom_id)).fetchall()
    
    conn.close()
    return jsonify([dict(row) for row in versions])

@product_bp.route('/products/<int:product_id>/uom-conversions/<int:uom_id>/activate/<int:version_id>', methods=['POST'])
@role_required('Admin', 'Planner')
def activate_uom_conversion_version(product_id, uom_id, version_id):
    """Activate a specific version of a UOM conversion"""
    db = Database()
    conn = db.get_connection()
    
    try:
        # Deactivate all versions for this product-UOM combination
        conn.execute('''
            UPDATE product_uom_conversions
            SET is_active = 0
            WHERE product_id = ? AND uom_id = ?
        ''', (product_id, uom_id))
        
        # Activate the selected version
        conn.execute('''
            UPDATE product_uom_conversions
            SET is_active = 1
            WHERE id = ? AND product_id = ? AND uom_id = ?
        ''', (version_id, product_id, uom_id))
        
        # Log audit trail
        AuditLogger.log_change(
            conn=conn,
            record_type='product_uom_conversion',
            record_id=product_id,
            action_type='Version Activated',
            modified_by=session.get('user_id'),
            changed_fields={'version_id': version_id, 'uom_id': uom_id},
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Version activated successfully'})
    
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500

@product_bp.route('/products/<int:product_id>/uom-conversions/<int:uom_id>/delete', methods=['POST'])
@role_required('Admin', 'Planner')
def delete_product_uom_conversion(product_id, uom_id):
    """Delete a UOM conversion for a product"""
    db = Database()
    conn = db.get_connection()
    
    try:
        # Check if this is the base UOM
        conversion = conn.execute('''
            SELECT * FROM product_uom_conversions 
            WHERE product_id = ? AND uom_id = ?
        ''', (product_id, uom_id)).fetchone()
        
        if not conversion:
            conn.close()
            return jsonify({'error': 'Conversion not found'}), 404
        
        if conversion['is_base_uom']:
            conn.close()
            return jsonify({'error': 'Cannot delete base UOM. Set another UOM as base first.'}), 400
        
        # Delete the conversion
        conn.execute('''
            DELETE FROM product_uom_conversions 
            WHERE product_id = ? AND uom_id = ?
        ''', (product_id, uom_id))
        
        # Log audit trail
        AuditLogger.log_change(
            conn=conn,
            record_type='product_uom_conversion',
            record_id=product_id,
            action_type='Deleted',
            modified_by=session.get('user_id'),
            changed_fields={'uom_id': uom_id},
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'UOM conversion deleted successfully'})
        
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500


# ============== Part Analyzer (AI-Powered Cross-Module Intelligence) ==============

@product_bp.route('/part-analyzer')
@login_required
def part_analyzer():
    """Part Analyzer - AI-powered cross-module historical intelligence"""
    db = Database()
    conn = db.get_connection()
    
    products = conn.execute('''
        SELECT id, code, name, product_type FROM products ORDER BY code
    ''').fetchall()
    
    conn.close()
    
    return render_template('products/part_analyzer.html',
                          products=[dict(p) for p in products])


@product_bp.route('/part-analyzer/analyze', methods=['POST'])
@login_required
def part_analyzer_analyze():
    """Analyze a part across all modules"""
    db = Database()
    conn = db.get_connection()
    
    product_id = request.form.get('product_id')
    analysis_scope = request.form.getlist('scope')
    
    if not product_id:
        return jsonify({'success': False, 'error': 'Please select a part to analyze'})
    
    try:
        product = conn.execute('SELECT * FROM products WHERE id = ?', (product_id,)).fetchone()
        if not product:
            conn.close()
            return jsonify({'success': False, 'error': 'Product not found'})
        
        product_data = dict(product)
        
        cross_module_data = {}
        
        if not analysis_scope or 'inventory' in analysis_scope:
            inventory = conn.execute('''
                SELECT * FROM inventory WHERE product_id = ?
            ''', (product_id,)).fetchall()
            inventory_dicts = [dict(i) for i in inventory]
            cross_module_data['inventory'] = {
                'records': inventory_dicts,
                'total_qty': sum(i['quantity'] for i in inventory_dicts) if inventory_dicts else 0,
                'locations': len(set(i.get('location') for i in inventory_dicts if i.get('location')))
            }
        
        if not analysis_scope or 'bom' in analysis_scope:
            bom_usage = conn.execute('''
                SELECT b.*, p.code as parent_code, p.name as parent_name
                FROM boms b
                JOIN products p ON b.parent_product_id = p.id
                WHERE b.child_product_id = ?
            ''', (product_id,)).fetchall()
            bom_children = conn.execute('''
                SELECT b.*, p.code as child_code, p.name as child_name
                FROM boms b
                JOIN products p ON b.child_product_id = p.id
                WHERE b.parent_product_id = ?
            ''', (product_id,)).fetchall()
            cross_module_data['bom'] = {
                'used_in': [dict(b) for b in bom_usage],
                'contains': [dict(b) for b in bom_children]
            }
        
        if not analysis_scope or 'work_orders' in analysis_scope:
            work_orders = conn.execute('''
                SELECT wo.*, c.name as customer_name
                FROM work_orders wo
                LEFT JOIN customers c ON wo.customer_id = c.id
                WHERE wo.product_id = ?
                ORDER BY wo.created_at DESC LIMIT 20
            ''', (product_id,)).fetchall()
            wo_stats = conn.execute('''
                SELECT status, COUNT(*) as count
                FROM work_orders WHERE product_id = ? GROUP BY status
            ''', (product_id,)).fetchall()
            cross_module_data['work_orders'] = {
                'recent': [dict(w) for w in work_orders],
                'stats': {s['status']: s['count'] for s in wo_stats}
            }
        
        if not analysis_scope or 'purchase_orders' in analysis_scope:
            po_lines = conn.execute('''
                SELECT pol.*, po.po_number, po.status as po_status, 
                       po.order_date, s.name as supplier_name
                FROM purchase_order_lines pol
                JOIN purchase_orders po ON pol.po_id = po.id
                LEFT JOIN suppliers s ON po.supplier_id = s.id
                WHERE pol.product_id = ?
                ORDER BY po.order_date DESC LIMIT 20
            ''', (product_id,)).fetchall()
            po_stats = conn.execute('''
                SELECT SUM(pol.quantity) as total_ordered,
                       AVG(pol.unit_price) as avg_price,
                       COUNT(DISTINCT po.supplier_id) as supplier_count
                FROM purchase_order_lines pol
                JOIN purchase_orders po ON pol.po_id = po.id
                WHERE pol.product_id = ?
            ''', (product_id,)).fetchone()
            cross_module_data['purchase_orders'] = {
                'recent': [dict(p) for p in po_lines],
                'stats': dict(po_stats) if po_stats else {}
            }
        
        if not analysis_scope or 'sales_orders' in analysis_scope:
            so_lines = conn.execute('''
                SELECT sol.*, so.so_number, so.status as order_status,
                       so.order_date, c.name as customer_name
                FROM sales_order_lines sol
                JOIN sales_orders so ON sol.so_id = so.id
                LEFT JOIN customers c ON so.customer_id = c.id
                WHERE sol.product_id = ?
                ORDER BY so.order_date DESC LIMIT 20
            ''', (product_id,)).fetchall()
            so_stats = conn.execute('''
                SELECT SUM(sol.quantity) as total_sold,
                       AVG(sol.unit_price) as avg_sell_price,
                       COUNT(DISTINCT so.customer_id) as customer_count
                FROM sales_order_lines sol
                JOIN sales_orders so ON sol.so_id = so.id
                WHERE sol.product_id = ?
            ''', (product_id,)).fetchone()
            cross_module_data['sales_orders'] = {
                'recent': [dict(s) for s in so_lines],
                'stats': dict(so_stats) if so_stats else {}
            }
        
        if not analysis_scope or 'exchanges' in analysis_scope:
            # Get exchange sales orders (sales_type = 'Exchange')
            exchange_orders = conn.execute('''
                SELECT so.*, c.name as customer_name,
                       sol.product_id, sol.quantity, sol.unit_price, sol.core_charge,
                       sol.core_disposition, sol.expected_core_condition
                FROM sales_orders so
                JOIN sales_order_lines sol ON so.id = sol.so_id
                LEFT JOIN customers c ON so.customer_id = c.id
                WHERE sol.product_id = ? AND so.sales_type = 'Exchange'
                ORDER BY so.order_date DESC LIMIT 20
            ''', (product_id,)).fetchall()
            
            exchange_stats = conn.execute('''
                SELECT 
                    COUNT(DISTINCT so.id) as total_exchanges,
                    SUM(CASE WHEN sol.core_disposition = 'Received' THEN 1 ELSE 0 END) as cores_received,
                    SUM(CASE WHEN sol.core_disposition IS NULL OR sol.core_disposition = 'Pending' THEN 1 ELSE 0 END) as cores_pending,
                    SUM(CASE WHEN sol.core_disposition = 'Overdue' THEN 1 ELSE 0 END) as cores_overdue,
                    AVG(sol.core_charge) as avg_core_charge
                FROM sales_orders so
                JOIN sales_order_lines sol ON so.id = sol.so_id
                WHERE sol.product_id = ? AND so.sales_type = 'Exchange'
            ''', (product_id,)).fetchone()
            
            cross_module_data['exchanges'] = {
                'recent': [dict(e) for e in exchange_orders],
                'stats': dict(exchange_stats) if exchange_stats else {}
            }
        
        if not analysis_scope or 'quality' in analysis_scope:
            deviations = conn.execute('''
                SELECT * FROM qms_deviations 
                WHERE description LIKE ? OR root_cause LIKE ?
                ORDER BY reported_date DESC LIMIT 10
            ''', (f'%{product_data["code"]}%', f'%{product_data["code"]}%')).fetchall()
            cross_module_data['quality'] = {
                'deviations': [dict(d) for d in deviations]
            }
        
        conn.close()
        
        from openai import OpenAI
        
        api_key = os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY')
        if not api_key:
            return jsonify({
                'success': False,
                'error': 'OpenAI API key not configured. Please set up the AI integration.'
            })
        
        client = OpenAI(
            api_key=api_key,
            base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
        )
        
        prompt = f"""Analyze this part/product across all ERP modules and provide strategic intelligence.

PART INFORMATION:
Code: {product_data.get('code')}
Name: {product_data.get('name')}
Type: {product_data.get('product_type')}
Cost: ${product_data.get('cost', 0)}
Description: {product_data.get('description', 'N/A')}

CROSS-MODULE DATA:
{json.dumps(cross_module_data, indent=2, default=str)}

Provide a comprehensive analysis including:
1. Inventory Health - Current stock status, turnover assessment, reorder recommendations
2. Supply Chain Analysis - Supplier diversity, pricing trends, lead time patterns
3. Demand Analysis - Sales patterns, customer concentration, growth trends
4. Production Insights - Work order patterns, quality issues, production efficiency
5. BOM Impact - Where this part is used, criticality assessment
6. Exchange Analysis - Core return patterns, exchange volume, core charge trends, overdue cores
7. Quality Assessment - Deviation patterns, root causes, improvement opportunities
8. Strategic Recommendations - Top 3-5 actionable recommendations for this part
9. Risk Indicators - Any concerns or risks identified across modules
10. Cost Optimization - Opportunities to reduce costs or improve margins

Format the response with clear sections using plain text only. Use dashes for lists. Do not use any special characters, asterisks, markdown formatting, or symbols."""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an expert ERP analyst specializing in aerospace MRO operations. Provide actionable, data-driven insights for part management and optimization. Use plain text only without any special characters, asterisks, markdown, or formatting symbols."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=2500
        )
        
        analysis = response.choices[0].message.content
        
        return jsonify({
            'success': True,
            'product': product_data,
            'cross_module_data': cross_module_data,
            'analysis': analysis
        })
        
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)})

@product_bp.route('/products/<int:id>/alternates', methods=['GET'])
@login_required
def get_alternates(id):
    db = Database()
    conn = db.get_connection()
    
    alternates = conn.execute('''
        SELECT pa.*, p.code as alternate_code, p.name as alternate_name, 
               p.product_type as alternate_type, p.unit_of_measure as alternate_uom,
               i.quantity as alternate_qty_on_hand
        FROM product_alternates pa
        JOIN products p ON pa.alternate_product_id = p.id
        LEFT JOIN inventory i ON p.id = i.product_id
        WHERE pa.product_id = ? AND pa.is_active = 1
        ORDER BY pa.priority, p.code
    ''', (id,)).fetchall()
    
    conn.close()
    return jsonify([dict(a) for a in alternates])

@product_bp.route('/products/<int:id>/alternates', methods=['POST'])
@role_required('Admin', 'Planner')
def add_alternate(id):
    db = Database()
    conn = db.get_connection()
    
    data = request.get_json() if request.is_json else request.form
    alternate_product_id = int(data.get('alternate_product_id'))
    relationship_type = data.get('relationship_type', 'Interchangeable')
    priority = int(data.get('priority', 1))
    notes = data.get('notes', '')
    bidirectional = data.get('bidirectional', False)
    
    if alternate_product_id == id:
        conn.close()
        return jsonify({'success': False, 'error': 'Cannot add product as its own alternate'})
    
    try:
        conn.execute('''
            INSERT INTO product_alternates (product_id, alternate_product_id, relationship_type, priority, notes, approved_by, approved_date, is_active)
            VALUES (?, ?, ?, ?, ?, ?, DATE('now'), 1)
        ''', (id, alternate_product_id, relationship_type, priority, notes, session.get('username', '')))
        
        if bidirectional:
            conn.execute('''
                INSERT OR IGNORE INTO product_alternates (product_id, alternate_product_id, relationship_type, priority, notes, approved_by, approved_date, is_active)
                VALUES (?, ?, ?, ?, ?, ?, DATE('now'), 1)
            ''', (alternate_product_id, id, relationship_type, priority, notes, session.get('username', '')))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Alternate added successfully'})
    except Exception as e:
        conn.close()
        if 'UNIQUE constraint' in str(e):
            return jsonify({'success': False, 'error': 'This alternate relationship already exists'})
        return jsonify({'success': False, 'error': str(e)})

@product_bp.route('/products/alternates/<int:alternate_id>', methods=['DELETE'])
@role_required('Admin', 'Planner')
def remove_alternate(alternate_id):
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('UPDATE product_alternates SET is_active = 0 WHERE id = ?', (alternate_id,))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'Alternate removed successfully'})
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)})

@product_bp.route('/products/alternates/<int:alternate_id>', methods=['PUT'])
@role_required('Admin', 'Planner')
def update_alternate(alternate_id):
    db = Database()
    conn = db.get_connection()
    
    data = request.get_json()
    
    try:
        conn.execute('''
            UPDATE product_alternates 
            SET relationship_type = ?, priority = ?, notes = ?
            WHERE id = ?
        ''', (data.get('relationship_type'), int(data.get('priority', 1)), data.get('notes', ''), alternate_id))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'Alternate updated successfully'})
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)})

UPLOAD_FOLDER = 'uploads/product_files'
ALLOWED_EXTENSIONS = {'pdf', 'doc', 'docx', 'xls', 'xlsx', 'png', 'jpg', 'jpeg', 'gif', 'txt', 'csv', 'dwg', 'dxf', 'stp', 'step', 'igs', 'iges'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@product_bp.route('/products/<int:id>/files', methods=['POST'])
@role_required('Admin', 'Planner')
def upload_product_file(id):
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file provided'})
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'})
    
    if not allowed_file(file.filename):
        return jsonify({'success': False, 'error': 'File type not allowed'})
    
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    
    import uuid
    from datetime import datetime
    
    original_name = file.filename
    ext = original_name.rsplit('.', 1)[1].lower() if '.' in original_name else ''
    unique_name = f"{id}_{uuid.uuid4().hex[:8]}_{datetime.now().strftime('%Y%m%d%H%M%S')}.{ext}"
    
    file_path = os.path.join(UPLOAD_FOLDER, unique_name)
    file.save(file_path)
    file_size = os.path.getsize(file_path)
    
    file_category = request.form.get('category', 'General')
    description = request.form.get('description', '')
    
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('''
            INSERT INTO product_files (product_id, file_name, original_name, file_type, file_size, file_category, description, uploaded_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (id, unique_name, original_name, ext, file_size, file_category, description, session.get('username', '')))
        
        AuditLogger.log_change(
            conn=conn,
            record_type='product',
            record_id=id,
            action_type='File Uploaded',
            modified_by=session.get('user_id'),
            changes={'file_uploaded': original_name, 'category': file_category},
            ip_address=request.remote_addr
        )
        
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'File uploaded successfully'})
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)})

@product_bp.route('/products/files/<int:file_id>/download')
@login_required
def download_product_file(file_id):
    db = Database()
    conn = db.get_connection()
    
    file_record = conn.execute('SELECT * FROM product_files WHERE id = ?', (file_id,)).fetchone()
    conn.close()
    
    if not file_record:
        flash('File not found', 'danger')
        return redirect(url_for('product_routes.list_products'))
    
    file_path = os.path.join(UPLOAD_FOLDER, file_record['file_name'])
    
    if not os.path.exists(file_path):
        flash('File not found on server', 'danger')
        return redirect(url_for('product_routes.view_product', id=file_record['product_id']))
    
    from flask import send_file
    return send_file(file_path, as_attachment=True, download_name=file_record['original_name'])

@product_bp.route('/products/files/<int:file_id>', methods=['DELETE'])
@role_required('Admin', 'Planner')
def delete_product_file(file_id):
    db = Database()
    conn = db.get_connection()
    
    file_record = conn.execute('SELECT * FROM product_files WHERE id = ?', (file_id,)).fetchone()
    
    if not file_record:
        conn.close()
        return jsonify({'success': False, 'error': 'File not found'})
    
    try:
        conn.execute('UPDATE product_files SET is_active = 0 WHERE id = ?', (file_id,))
        
        AuditLogger.log_change(
            conn=conn,
            record_type='product',
            record_id=file_record['product_id'],
            action_type='File Deleted',
            modified_by=session.get('user_id'),
            changes={'file_deleted': file_record['original_name']},
            ip_address=request.remote_addr
        )
        
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'File deleted successfully'})
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)})
