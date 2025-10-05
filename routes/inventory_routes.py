from flask import Blueprint, render_template, request, redirect, url_for, flash, Response
from models import Database
from auth import login_required, role_required
import csv
import io
import math

inventory_bp = Blueprint('inventory_routes', __name__)

@inventory_bp.route('/inventory')
@login_required
def list_inventory():
    db = Database()
    conn = db.get_connection()
    inventory = conn.execute('''
        SELECT i.*, p.code, p.name, p.unit_of_measure
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        ORDER BY p.code
    ''').fetchall()
    conn.close()
    return render_template('inventory/list.html', inventory=inventory)

@inventory_bp.route('/inventory/create', methods=['GET', 'POST'])
@role_required('Admin', 'Production Staff')
def create_inventory():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        product_id = int(request.form['product_id'])
        
        existing = conn.execute('SELECT id FROM inventory WHERE product_id=?', (product_id,)).fetchone()
        
        if existing:
            conn.close()
            flash('Inventory already exists for this product. Use adjust instead.', 'warning')
            return redirect(url_for('inventory_routes.list_inventory'))
        
        conn.execute('''
            INSERT INTO inventory (product_id, quantity, reorder_point, safety_stock, 
                                   warehouse_location, bin_location, condition, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'Available')
        ''', (
            product_id,
            float(request.form.get('quantity', 0)),
            float(request.form.get('reorder_point', 0)),
            float(request.form.get('safety_stock', 0)),
            request.form.get('warehouse_location', 'Main'),
            request.form.get('bin_location', ''),
            request.form.get('condition', 'Serviceable')
        ))
        
        inventory_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        conn.commit()
        conn.close()
        
        flash(f'Inventory created successfully! Inventory ID: INV-{inventory_id:06d}', 'success')
        return redirect(url_for('inventory_routes.list_inventory'))
    
    products = conn.execute('''
        SELECT p.* FROM products p
        WHERE p.id NOT IN (SELECT product_id FROM inventory)
        ORDER BY p.code
    ''').fetchall()
    conn.close()
    
    return render_template('inventory/create.html', products=products)

@inventory_bp.route('/inventory/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Production Staff')
def edit_inventory(id):
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            # Parse numeric fields with error handling
            try:
                quantity = float(request.form.get('quantity', 0))
                reorder_point = float(request.form.get('reorder_point', 0))
                safety_stock = float(request.form.get('safety_stock', 0))
            except (ValueError, TypeError):
                flash('Invalid numeric value provided', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            warehouse_location = request.form.get('warehouse_location', '').strip()
            bin_location = request.form.get('bin_location', '').strip()
            condition = request.form.get('condition', 'Serviceable')
            status = request.form.get('status', 'Available')
            
            # Validate numeric fields are finite
            if not math.isfinite(quantity):
                flash('Quantity must be a valid number', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            if not math.isfinite(reorder_point):
                flash('Reorder point must be a valid number', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            if not math.isfinite(safety_stock):
                flash('Safety stock must be a valid number', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            # Validate numeric fields are non-negative
            if quantity < 0:
                flash('Quantity cannot be negative', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            if reorder_point < 0:
                flash('Reorder point cannot be negative', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            if safety_stock < 0:
                flash('Safety stock cannot be negative', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            # Validate required fields
            if not warehouse_location:
                flash('Warehouse location is required', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            if not bin_location:
                flash('Bin location is required', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            # Validate condition and status values
            valid_conditions = ['New', 'Serviceable', 'Overhauled', 'Repaired']
            if condition not in valid_conditions:
                flash('Invalid condition value', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            valid_statuses = ['Available', 'Reserved', 'Out of Stock']
            if status not in valid_statuses:
                flash('Invalid status value', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            # Update inventory
            conn.execute('''
                UPDATE inventory 
                SET quantity=?, 
                    reorder_point=?, 
                    safety_stock=?, 
                    warehouse_location=?,
                    bin_location=?,
                    condition=?,
                    status=?,
                    last_updated=CURRENT_TIMESTAMP 
                WHERE id=?
            ''', (quantity, reorder_point, safety_stock, warehouse_location, bin_location, 
                  condition, status, id))
            
            conn.commit()
            flash('Inventory updated successfully!', 'success')
            
        except Exception as e:
            conn.rollback()
            flash(f'Error updating inventory: {str(e)}', 'danger')
        finally:
            conn.close()
        
        return redirect(url_for('inventory_routes.list_inventory'))
    
    # GET request - show edit form
    inventory = conn.execute('''
        SELECT i.*, p.code, p.name, p.unit_of_measure, p.description
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.id = ?
    ''', (id,)).fetchone()
    
    conn.close()
    
    if not inventory:
        flash('Inventory record not found', 'danger')
        return redirect(url_for('inventory_routes.list_inventory'))
    
    return render_template('inventory/edit.html', inventory=inventory)

@inventory_bp.route('/inventory/<int:id>/adjust', methods=['GET', 'POST'])
@role_required('Admin', 'Production Staff')
def adjust_inventory(id):
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        adjustment_type = request.form['adjustment_type']
        quantity = float(request.form['quantity'])
        
        current = conn.execute('SELECT quantity FROM inventory WHERE id=?', (id,)).fetchone()
        
        if adjustment_type == 'add':
            new_quantity = current['quantity'] + quantity
        elif adjustment_type == 'subtract':
            new_quantity = max(0, current['quantity'] - quantity)
        else:
            new_quantity = quantity
        
        conn.execute('UPDATE inventory SET quantity=?, last_updated=CURRENT_TIMESTAMP WHERE id=?', 
                    (new_quantity, id))
        conn.commit()
        conn.close()
        
        flash('Inventory adjusted successfully!', 'success')
        return redirect(url_for('inventory_routes.list_inventory'))
    
    inventory_item = conn.execute('''
        SELECT i.*, p.code, p.name, p.unit_of_measure
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.id=?
    ''', (id,)).fetchone()
    conn.close()
    
    return render_template('inventory/adjust.html', item=inventory_item)

@inventory_bp.route('/inventory/export')
@login_required
def export_inventory():
    db = Database()
    conn = db.get_connection()
    inventory = conn.execute('''
        SELECT p.code, p.name, i.quantity, i.reorder_point, i.safety_stock
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        ORDER BY p.code
    ''').fetchall()
    conn.close()
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Product Code', 'Product Name', 'Quantity', 'Reorder Point', 'Safety Stock'])
    
    for item in inventory:
        writer.writerow([item['code'], item['name'], item['quantity'], 
                        item['reorder_point'], item['safety_stock']])
    
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=inventory_export.csv'}
    )

@inventory_bp.route('/inventory/template')
@login_required
def download_template():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Product Code', 'Product Name', 'Quantity', 'Reorder Point', 'Safety Stock'])
    
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=inventory_import_template.csv'}
    )

@inventory_bp.route('/inventory/mass-update', methods=['GET', 'POST'])
@role_required('Admin', 'Production Staff')
def mass_update_inventory():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            selected_ids = request.form.getlist('selected_ids')
            
            if not selected_ids:
                flash('No items selected for update.', 'warning')
                return redirect(url_for('inventory_routes.mass_update_inventory'))
            
            updated_count = 0
            
            for item_id in selected_ids:
                try:
                    item_id_int = int(item_id)
                    
                    quantity = float(request.form.get(f'quantity_{item_id}', 0))
                    reorder_point = float(request.form.get(f'reorder_point_{item_id}', 0))
                    safety_stock = float(request.form.get(f'safety_stock_{item_id}', 0))
                    warehouse_location = request.form.get(f'warehouse_location_{item_id}', 'Main')
                    bin_location = request.form.get(f'bin_location_{item_id}', '')
                    condition = request.form.get(f'condition_{item_id}', 'Serviceable')
                    
                    conn.execute('''
                        UPDATE inventory
                        SET quantity = ?,
                            reorder_point = ?,
                            safety_stock = ?,
                            warehouse_location = ?,
                            bin_location = ?,
                            condition = ?,
                            status = CASE WHEN ? <= 0 THEN 'Out of Stock' ELSE status END,
                            last_updated = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (quantity, reorder_point, safety_stock, warehouse_location, 
                          bin_location, condition, quantity, item_id_int))
                    
                    updated_count += 1
                except Exception as e:
                    flash(f'Error updating inventory ID {item_id}: {str(e)}', 'warning')
            
            conn.commit()
            flash(f'Successfully updated {updated_count} inventory item(s)!', 'success')
            return redirect(url_for('inventory_routes.list_inventory'))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error during mass update: {str(e)}', 'danger')
        finally:
            conn.close()
        
        return redirect(url_for('inventory_routes.mass_update_inventory'))
    
    # GET request - show form
    inventory = conn.execute('''
        SELECT i.*, p.code, p.name, p.unit_of_measure
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        ORDER BY p.code
    ''').fetchall()
    conn.close()
    
    return render_template('inventory/mass_update.html', inventory=inventory)

@inventory_bp.route('/inventory/import', methods=['POST'])
@role_required('Admin', 'Production Staff')
def import_inventory():
    if 'file' not in request.files:
        flash('No file uploaded', 'danger')
        return redirect(url_for('inventory_routes.list_inventory'))
    
    file = request.files['file']
    if not file or not file.filename:
        flash('No file selected', 'danger')
        return redirect(url_for('inventory_routes.list_inventory'))
    
    if not file.filename.lower().endswith('.csv'):
        flash('Please upload a CSV file', 'danger')
        return redirect(url_for('inventory_routes.list_inventory'))
    
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
                code = row.get('Product Code', '').strip()
                quantity_str = row.get('Quantity', '').strip()
                reorder_str = row.get('Reorder Point', '').strip()
                safety_str = row.get('Safety Stock', '').strip()
                
                if not code:
                    skipped_count += 1
                    errors.append(f"Row {row_num}: Missing product code")
                    continue
                
                try:
                    quantity = float(quantity_str) if quantity_str else 0.0
                    reorder_point = float(reorder_str) if reorder_str else 0.0
                    safety_stock = float(safety_str) if safety_str else 0.0
                except ValueError:
                    skipped_count += 1
                    errors.append(f"Row {row_num}: Invalid number format")
                    continue
                
                product = conn.execute('SELECT id FROM products WHERE code = ?', (code,)).fetchone()
                
                if not product:
                    skipped_count += 1
                    errors.append(f"Row {row_num}: Product '{code}' not found")
                    continue
                
                conn.execute('''
                    UPDATE inventory 
                    SET quantity=?, reorder_point=?, safety_stock=?, last_updated=CURRENT_TIMESTAMP
                    WHERE product_id=?
                ''', (quantity, reorder_point, safety_stock, product['id']))
                
                imported_count += 1
            except Exception as row_error:
                skipped_count += 1
                errors.append(f"Row {row_num}: {str(row_error)}")
        
        conn.commit()
        
        if imported_count > 0:
            flash(f'Successfully imported {imported_count} inventory items. Skipped {skipped_count} rows.', 'success')
        else:
            flash(f'No inventory items imported. Skipped {skipped_count} rows.', 'warning')
        
        if errors and len(errors) <= 10:
            for error in errors:
                flash(error, 'warning')
        elif errors:
            flash(f'First 10 errors: {"; ".join(errors[:10])}', 'warning')
            
    except Exception as e:
        flash(f'Error importing inventory: {str(e)}', 'danger')
    finally:
        if conn:
            conn.close()
    
    return redirect(url_for('inventory_routes.list_inventory'))
