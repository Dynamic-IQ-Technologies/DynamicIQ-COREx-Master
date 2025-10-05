from flask import Blueprint, render_template, request, redirect, url_for, flash, Response
from models import Database
from auth import login_required, role_required
import csv
import io

bom_bp = Blueprint('bom_routes', __name__)

@bom_bp.route('/boms')
@login_required
def list_boms():
    db = Database()
    conn = db.get_connection()
    boms = conn.execute('''
        SELECT b.*, 
               p1.code as parent_code, p1.name as parent_name,
               p2.code as child_code, p2.name as child_name
        FROM boms b
        JOIN products p1 ON b.parent_product_id = p1.id
        JOIN products p2 ON b.child_product_id = p2.id
        ORDER BY p1.code, b.id
    ''').fetchall()
    conn.close()
    return render_template('boms/list.html', boms=boms)

@bom_bp.route('/boms/create', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def create_bom():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        conn.execute('''
            INSERT INTO boms (parent_product_id, child_product_id, quantity, scrap_percentage)
            VALUES (?, ?, ?, ?)
        ''', (
            int(request.form['parent_product_id']),
            int(request.form['child_product_id']),
            float(request.form['quantity']),
            float(request.form.get('scrap_percentage', 0))
        ))
        
        conn.commit()
        conn.close()
        
        flash('BOM created successfully!', 'success')
        return redirect(url_for('bom_routes.list_boms'))
    
    products = conn.execute('SELECT * FROM products ORDER BY code').fetchall()
    conn.close()
    
    return render_template('boms/create.html', products=products)

@bom_bp.route('/boms/<int:id>/delete', methods=['POST'])
@role_required('Admin', 'Planner')
def delete_bom(id):
    db = Database()
    conn = db.get_connection()
    conn.execute('DELETE FROM boms WHERE id=?', (id,))
    conn.commit()
    conn.close()
    
    flash('BOM deleted successfully!', 'success')
    return redirect(url_for('bom_routes.list_boms'))

@bom_bp.route('/boms/export')
@login_required
def export_boms():
    db = Database()
    conn = db.get_connection()
    boms = conn.execute('''
        SELECT p1.code as parent_code, p1.name as parent_name,
               p2.code as child_code, p2.name as child_name,
               b.quantity, b.scrap_percentage
        FROM boms b
        JOIN products p1 ON b.parent_product_id = p1.id
        JOIN products p2 ON b.child_product_id = p2.id
        ORDER BY p1.code, b.id
    ''').fetchall()
    conn.close()
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Parent Code', 'Parent Name', 'Child Code', 'Child Name', 'Quantity', 'Scrap Percentage'])
    
    for bom in boms:
        writer.writerow([bom['parent_code'], bom['parent_name'], bom['child_code'], 
                        bom['child_name'], bom['quantity'], bom['scrap_percentage']])
    
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=boms_export.csv'}
    )

@bom_bp.route('/boms/template')
@login_required
def download_template():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Parent Code', 'Parent Name', 'Child Code', 'Child Name', 'Quantity', 'Scrap Percentage'])
    
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=bom_import_template.csv'}
    )

@bom_bp.route('/boms/import', methods=['POST'])
@role_required('Admin', 'Planner')
def import_boms():
    if 'file' not in request.files:
        flash('No file uploaded', 'danger')
        return redirect(url_for('bom_routes.list_boms'))
    
    file = request.files['file']
    if not file or not file.filename:
        flash('No file selected', 'danger')
        return redirect(url_for('bom_routes.list_boms'))
    
    if not file.filename.lower().endswith('.csv'):
        flash('Please upload a CSV file', 'danger')
        return redirect(url_for('bom_routes.list_boms'))
    
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
                parent_code = row.get('Parent Code', '').strip()
                child_code = row.get('Child Code', '').strip()
                quantity_str = row.get('Quantity', '').strip()
                scrap_str = row.get('Scrap Percentage', '').strip()
                
                if not parent_code or not child_code or not quantity_str:
                    skipped_count += 1
                    errors.append(f"Row {row_num}: Missing required fields")
                    continue
                
                try:
                    quantity = float(quantity_str)
                    scrap_percentage = float(scrap_str) if scrap_str else 0.0
                except ValueError:
                    skipped_count += 1
                    errors.append(f"Row {row_num}: Invalid number format")
                    continue
                
                parent = conn.execute('SELECT id FROM products WHERE code = ?', (parent_code,)).fetchone()
                child = conn.execute('SELECT id FROM products WHERE code = ?', (child_code,)).fetchone()
                
                if not parent:
                    skipped_count += 1
                    errors.append(f"Row {row_num}: Parent product '{parent_code}' not found")
                    continue
                
                if not child:
                    skipped_count += 1
                    errors.append(f"Row {row_num}: Child product '{child_code}' not found")
                    continue
                
                existing = conn.execute(
                    'SELECT id FROM boms WHERE parent_product_id = ? AND child_product_id = ?',
                    (parent['id'], child['id'])
                ).fetchone()
                
                if existing:
                    conn.execute(
                        'UPDATE boms SET quantity = ?, scrap_percentage = ? WHERE id = ?',
                        (quantity, scrap_percentage, existing['id'])
                    )
                else:
                    conn.execute(
                        'INSERT INTO boms (parent_product_id, child_product_id, quantity, scrap_percentage) VALUES (?, ?, ?, ?)',
                        (parent['id'], child['id'], quantity, scrap_percentage)
                    )
                
                imported_count += 1
            except Exception as row_error:
                skipped_count += 1
                errors.append(f"Row {row_num}: {str(row_error)}")
        
        conn.commit()
        
        if imported_count > 0:
            flash(f'Successfully imported {imported_count} BOMs. Skipped {skipped_count} rows.', 'success')
        else:
            flash(f'No BOMs imported. Skipped {skipped_count} rows.', 'warning')
        
        if errors and len(errors) <= 10:
            for error in errors:
                flash(error, 'warning')
        elif errors:
            flash(f'First 10 errors: {"; ".join(errors[:10])}', 'warning')
            
    except Exception as e:
        flash(f'Error importing BOMs: {str(e)}', 'danger')
    finally:
        if conn:
            conn.close()
    
    return redirect(url_for('bom_routes.list_boms'))
