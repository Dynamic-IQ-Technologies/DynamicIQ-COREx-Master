from flask import Blueprint, render_template, request, redirect, url_for, flash, Response, jsonify, session
from models import Database, AuditLogger
from auth import login_required, role_required
from bom_utils import BOMHierarchy
import csv
import io
from datetime import datetime

bom_bp = Blueprint('bom_routes', __name__)

@bom_bp.route('/boms')
@login_required
def list_boms():
    db = Database()
    conn = db.get_connection()
    
    # Get all BOM lines with parent and child product details
    bom_lines = conn.execute('''
        SELECT b.*, 
               p1.id as parent_id, p1.code as parent_code, p1.name as parent_name,
               p2.code as child_code, p2.name as child_name
        FROM boms b
        JOIN products p1 ON b.parent_product_id = p1.id
        JOIN products p2 ON b.child_product_id = p2.id
        ORDER BY p1.code, b.find_number, b.id
    ''').fetchall()
    
    # Group BOM lines by parent product
    grouped_boms = {}
    for line in bom_lines:
        parent_id = line['parent_id']
        if parent_id not in grouped_boms:
            grouped_boms[parent_id] = {
                'parent_id': parent_id,
                'parent_code': line['parent_code'],
                'parent_name': line['parent_name'],
                'children': []
            }
        grouped_boms[parent_id]['children'].append(line)
    
    # Convert to list and sort by parent code
    boms_grouped = sorted(grouped_boms.values(), key=lambda x: x['parent_code'])
    
    conn.close()
    return render_template('boms/list.html', boms_grouped=boms_grouped)

@bom_bp.route('/boms/create', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def create_bom():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        parent_id = int(request.form['parent_product_id'])
        child_id = int(request.form['child_product_id'])
        
        existing = conn.execute(
            'SELECT id FROM boms WHERE parent_product_id = ? AND child_product_id = ?',
            (parent_id, child_id)
        ).fetchone()
        
        if existing:
            flash('This BOM relationship already exists!', 'danger')
            products = conn.execute('SELECT * FROM products ORDER BY code').fetchall()
            next_find_number = BOMHierarchy.get_next_find_number(parent_id)
            return render_template('boms/create.html', products=products, next_find_number=next_find_number)
        
        find_number = request.form.get('find_number') or BOMHierarchy.get_next_find_number(parent_id)
        quantity = float(request.form['quantity'])
        child_cost = conn.execute('SELECT cost FROM products WHERE id = ?', (child_id,)).fetchone()['cost']
        extended_cost = quantity * (child_cost if child_cost else 0)
        
        conn.execute('''
            INSERT INTO boms (parent_product_id, child_product_id, quantity, scrap_percentage,
                            find_number, category, revision, effectivity_date, status,
                            reference_designator, document_link, notes, unit_cost, extended_cost)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            parent_id,
            child_id,
            quantity,
            float(request.form.get('scrap_percentage', 0)),
            find_number,
            request.form.get('category', 'Other'),
            request.form.get('revision', 'A'),
            request.form.get('effectivity_date') if request.form.get('effectivity_date') else None,
            request.form.get('status', 'Active'),
            request.form.get('reference_designator', ''),
            request.form.get('document_link', ''),
            request.form.get('notes', ''),
            child_cost if child_cost else 0,
            extended_cost
        ))
        
        bom_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        parent_product = conn.execute('SELECT code FROM products WHERE id = ?', (parent_id,)).fetchone()
        child_product = conn.execute('SELECT code FROM products WHERE id = ?', (child_id,)).fetchone()
        AuditLogger.log_change(conn, 'bom', bom_id, 'CREATE', session.get('user_id'),
                              {'parent_code': parent_product['code'], 'child_code': child_product['code'],
                               'quantity': quantity, 'find_number': find_number})
        conn.commit()
        conn.close()
        
        flash('BOM created successfully!', 'success')
        return redirect(url_for('bom_routes.list_boms'))
    
    products = conn.execute('SELECT * FROM products ORDER BY code').fetchall()
    parent_id = request.args.get('parent_id')
    next_find_number = BOMHierarchy.get_next_find_number(int(parent_id)) if parent_id else '1'
    
    conn.close()
    
    return render_template('boms/create.html', products=products, next_find_number=next_find_number)

@bom_bp.route('/boms/<int:id>/delete', methods=['POST'])
@role_required('Admin', 'Planner')
def delete_bom(id):
    db = Database()
    conn = db.get_connection()
    
    bom = conn.execute('''
        SELECT b.*, p1.code as parent_code, p2.code as child_code
        FROM boms b
        JOIN products p1 ON b.parent_product_id = p1.id
        JOIN products p2 ON b.child_product_id = p2.id
        WHERE b.id = ?
    ''', (id,)).fetchone()
    
    if bom:
        AuditLogger.log_change(conn, 'bom', id, 'DELETE', session.get('user_id'),
                              {'parent_code': bom['parent_code'], 'child_code': bom['child_code'],
                               'quantity': bom['quantity']})
    
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

@bom_bp.route('/boms/view/<int:product_id>')
@login_required
def view_bom_hierarchy(product_id):
    db = Database()
    conn = db.get_connection()
    
    product = conn.execute('SELECT * FROM products WHERE id = ?', (product_id,)).fetchone()
    if not product:
        flash('Product not found', 'danger')
        return redirect(url_for('bom_routes.list_boms'))
    
    hierarchy = BOMHierarchy.build_hierarchy_tree(product_id)
    summary = BOMHierarchy.get_bom_summary(product_id)
    
    conn.close()
    
    return render_template('boms/view_hierarchy.html', 
                         product=product, 
                         hierarchy=hierarchy,
                         summary=summary)

@bom_bp.route('/boms/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def edit_bom(id):
    db = Database()
    conn = db.get_connection()
    
    bom = conn.execute('SELECT * FROM boms WHERE id = ?', (id,)).fetchone()
    if not bom:
        flash('BOM not found', 'danger')
        return redirect(url_for('bom_routes.list_boms'))
    
    if request.method == 'POST':
        parent_id = int(request.form['parent_product_id'])
        child_id = int(request.form['child_product_id'])
        
        parent_product = conn.execute('SELECT * FROM products WHERE id = ?', (parent_id,)).fetchone()
        child_product = conn.execute('SELECT * FROM products WHERE id = ?', (child_id,)).fetchone()
        
        if not parent_product:
            flash('Selected parent product does not exist.', 'danger')
            products = conn.execute('SELECT * FROM products ORDER BY code').fetchall()
            return render_template('boms/edit.html', bom=bom, products=products)
        
        if not child_product:
            flash('Selected child product does not exist.', 'danger')
            products = conn.execute('SELECT * FROM products ORDER BY code').fetchall()
            return render_template('boms/edit.html', bom=bom, products=products)
        
        existing = conn.execute(
            'SELECT id FROM boms WHERE parent_product_id = ? AND child_product_id = ? AND id != ?',
            (parent_id, child_id, id)
        ).fetchone()
        
        if existing:
            flash('This BOM relationship already exists! Cannot update to duplicate parent-child combination.', 'danger')
            products = conn.execute('SELECT * FROM products ORDER BY code').fetchall()
            return render_template('boms/edit.html', bom=bom, products=products)
        
        quantity = float(request.form['quantity'])
        child_cost = child_product['cost'] if child_product['cost'] else 0
        extended_cost = quantity * child_cost
        
        conn.execute('''
            UPDATE boms SET 
                parent_product_id = ?, child_product_id = ?,
                quantity = ?, scrap_percentage = ?, find_number = ?, category = ?,
                revision = ?, effectivity_date = ?, status = ?, reference_designator = ?,
                document_link = ?, notes = ?, unit_cost = ?, extended_cost = ?
            WHERE id = ?
        ''', (
            parent_id,
            child_id,
            quantity,
            float(request.form.get('scrap_percentage', 0)),
            request.form.get('find_number'),
            request.form.get('category', 'Other'),
            request.form.get('revision', 'A'),
            request.form.get('effectivity_date') if request.form.get('effectivity_date') else None,
            request.form.get('status', 'Active'),
            request.form.get('reference_designator', ''),
            request.form.get('document_link', ''),
            request.form.get('notes', ''),
            child_cost,
            extended_cost,
            id
        ))
        
        AuditLogger.log_change(conn, 'bom', id, 'UPDATE', session.get('user_id'),
                              {'parent_code': parent_product['code'], 'child_code': child_product['code'],
                               'quantity': quantity, 'old_quantity': bom['quantity'],
                               'revision': request.form.get('revision', 'A')})
        conn.commit()
        conn.close()
        
        flash('BOM updated successfully!', 'success')
        return redirect(url_for('bom_routes.list_boms'))
    
    products = conn.execute('SELECT * FROM products ORDER BY code').fetchall()
    conn.close()
    
    return render_template('boms/edit.html', bom=bom, products=products)

@bom_bp.route('/boms/clone', methods=['POST'])
@role_required('Admin', 'Planner')
def clone_bom():
    source_id = int(request.form['source_product_id'])
    target_id = int(request.form['target_product_id'])
    
    if source_id == target_id:
        flash('Source and target products must be different', 'danger')
        return redirect(url_for('bom_routes.list_boms'))
    
    count = BOMHierarchy.clone_bom(source_id, target_id)
    flash(f'Successfully cloned {count} BOM items', 'success')
    
    return redirect(url_for('bom_routes.view_bom_hierarchy', product_id=target_id))

@bom_bp.route('/boms/mass-update', methods=['POST'])
@role_required('Admin', 'Planner')
def mass_update():
    db = Database()
    conn = db.get_connection()
    
    update_type = request.form.get('update_type')
    parent_id = request.form.get('parent_product_id')
    
    if not parent_id:
        flash('Please select a parent product', 'danger')
        return redirect(url_for('bom_routes.list_boms'))
    
    if update_type == 'status':
        new_status = request.form.get('new_status')
        conn.execute(
            'UPDATE boms SET status = ? WHERE parent_product_id = ?',
            (new_status, parent_id)
        )
        flash(f'Updated all BOM items to status: {new_status}', 'success')
    
    elif update_type == 'revision':
        new_revision = request.form.get('new_revision')
        conn.execute(
            'UPDATE boms SET revision = ? WHERE parent_product_id = ?',
            (new_revision, parent_id)
        )
        flash(f'Updated all BOM items to revision: {new_revision}', 'success')
    
    elif update_type == 'category':
        old_category = request.form.get('old_category')
        new_category = request.form.get('new_category')
        conn.execute(
            'UPDATE boms SET category = ? WHERE parent_product_id = ? AND category = ?',
            (new_category, parent_id, old_category)
        )
        flash(f'Updated {old_category} items to {new_category}', 'success')
    
    conn.commit()
    conn.close()
    
    return redirect(url_for('bom_routes.view_bom_hierarchy', product_id=parent_id))
