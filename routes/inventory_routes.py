from flask import Blueprint, render_template, request, redirect, url_for, flash, Response
from models import Database
from auth import login_required, role_required
import csv
import io

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
