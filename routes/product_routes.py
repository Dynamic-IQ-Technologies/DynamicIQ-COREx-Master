from flask import Blueprint, render_template, request, redirect, url_for, flash, Response, jsonify, session
from models import Database, AuditLogger
from auth import login_required, role_required
import csv
import io

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

@product_bp.route('/products/create', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def create_product():
    if request.method == 'POST':
        db = Database()
        conn = db.get_connection()
        
        conn.execute('''
            INSERT INTO products (code, name, description, unit_of_measure, product_type, part_category, cost)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            request.form['code'],
            request.form['name'],
            request.form['description'],
            request.form['unit_of_measure'],
            request.form['product_type'],
            request.form.get('part_category', 'Other'),
            0.0
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
        SELECT * FROM uom_conversions 
        WHERE product_id = ? AND is_active = 1
        ORDER BY target_uom
    ''', (id,)).fetchall()
    
    conn.close()
    
    return render_template('products/view.html',
                          product=product,
                          inventory=inventory,
                          bom_usage=bom_usage,
                          bom_components=bom_components,
                          recent_work_orders=recent_work_orders,
                          recent_po_lines=recent_po_lines,
                          uom_conversions=uom_conversions)

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
            SET code=?, name=?, description=?, unit_of_measure=?, product_type=?, part_category=?
            WHERE id=?
        ''', (
            request.form['code'],
            request.form['name'],
            request.form['description'],
            request.form['unit_of_measure'],
            request.form['product_type'],
            request.form.get('part_category', 'Other'),
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

@product_bp.route('/products/export')
@login_required
def export_products():
    db = Database()
    conn = db.get_connection()
    products = conn.execute('SELECT * FROM products ORDER BY code').fetchall()
    conn.close()
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Code', 'Name', 'Description', 'Unit of Measure', 'Product Type', 'Cost'])
    
    for product in products:
        writer.writerow([product['code'], product['name'], product['description'], 
                        product['unit_of_measure'], product['product_type'], product['cost']])
    
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
    writer.writerow(['Code', 'Name', 'Description', 'Unit of Measure', 'Product Type', 'Cost'])
    
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
                
                if not code or not name or not unit_of_measure or not product_type:
                    skipped_count += 1
                    errors.append(f"Row {row_num}: Missing required fields")
                    continue
                
                try:
                    cost = float(cost_str) if cost_str else 0.0
                except ValueError:
                    skipped_count += 1
                    errors.append(f"Row {row_num}: Invalid cost format")
                    continue
                
                existing = conn.execute('SELECT id FROM products WHERE code = ?', (code,)).fetchone()
                
                if existing:
                    conn.execute('''
                        UPDATE products 
                        SET name=?, description=?, unit_of_measure=?, product_type=?, cost=?
                        WHERE code=?
                    ''', (name, description, unit_of_measure, product_type, cost, code))
                else:
                    conn.execute('''
                        INSERT INTO products (code, name, description, unit_of_measure, product_type, cost)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (code, name, description, unit_of_measure, product_type, cost))
                    
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
