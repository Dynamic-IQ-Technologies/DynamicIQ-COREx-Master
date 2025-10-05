from flask import Blueprint, render_template, request, redirect, url_for, flash, Response
from models import Database
from auth import login_required, role_required
import csv
import io

supplier_bp = Blueprint('supplier_routes', __name__)

@supplier_bp.route('/suppliers')
@login_required
def list_suppliers():
    db = Database()
    conn = db.get_connection()
    suppliers = conn.execute('SELECT * FROM suppliers ORDER BY code').fetchall()
    conn.close()
    return render_template('suppliers/list.html', suppliers=suppliers)

@supplier_bp.route('/suppliers/create', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement')
def create_supplier():
    if request.method == 'POST':
        db = Database()
        conn = db.get_connection()
        
        conn.execute('''
            INSERT INTO suppliers (code, name, contact_person, email, phone, address)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            request.form['code'],
            request.form['name'],
            request.form.get('contact_person', ''),
            request.form.get('email', ''),
            request.form.get('phone', ''),
            request.form.get('address', '')
        ))
        
        conn.commit()
        conn.close()
        
        flash('Supplier created successfully!', 'success')
        return redirect(url_for('supplier_routes.list_suppliers'))
    
    return render_template('suppliers/create.html')

@supplier_bp.route('/suppliers/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement')
def edit_supplier(id):
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        conn.execute('''
            UPDATE suppliers 
            SET code=?, name=?, contact_person=?, email=?, phone=?, address=?
            WHERE id=?
        ''', (
            request.form['code'],
            request.form['name'],
            request.form.get('contact_person', ''),
            request.form.get('email', ''),
            request.form.get('phone', ''),
            request.form.get('address', ''),
            id
        ))
        
        conn.commit()
        conn.close()
        
        flash('Supplier updated successfully!', 'success')
        return redirect(url_for('supplier_routes.list_suppliers'))
    
    supplier = conn.execute('SELECT * FROM suppliers WHERE id=?', (id,)).fetchone()
    conn.close()
    
    return render_template('suppliers/edit.html', supplier=supplier)

@supplier_bp.route('/suppliers/<int:id>/delete', methods=['POST'])
@role_required('Admin')
def delete_supplier(id):
    db = Database()
    conn = db.get_connection()
    conn.execute('DELETE FROM suppliers WHERE id=?', (id,))
    conn.commit()
    conn.close()
    
    flash('Supplier deleted successfully!', 'success')
    return redirect(url_for('supplier_routes.list_suppliers'))

@supplier_bp.route('/suppliers/export')
@login_required
def export_suppliers():
    db = Database()
    conn = db.get_connection()
    suppliers = conn.execute('SELECT * FROM suppliers ORDER BY code').fetchall()
    conn.close()
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Code', 'Name', 'Contact Person', 'Email', 'Phone', 'Address'])
    
    for supplier in suppliers:
        writer.writerow([supplier['code'], supplier['name'], supplier['contact_person'], 
                        supplier['email'], supplier['phone'], supplier['address']])
    
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=suppliers_export.csv'}
    )

@supplier_bp.route('/suppliers/template')
@login_required
def download_template():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Code', 'Name', 'Contact Person', 'Email', 'Phone', 'Address'])
    
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=supplier_import_template.csv'}
    )

@supplier_bp.route('/suppliers/import', methods=['POST'])
@role_required('Admin', 'Procurement')
def import_suppliers():
    if 'file' not in request.files:
        flash('No file uploaded', 'danger')
        return redirect(url_for('supplier_routes.list_suppliers'))
    
    file = request.files['file']
    if not file or not file.filename:
        flash('No file selected', 'danger')
        return redirect(url_for('supplier_routes.list_suppliers'))
    
    if not file.filename.lower().endswith('.csv'):
        flash('Please upload a CSV file', 'danger')
        return redirect(url_for('supplier_routes.list_suppliers'))
    
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
                contact_person = row.get('Contact Person', '').strip()
                email = row.get('Email', '').strip()
                phone = row.get('Phone', '').strip()
                address = row.get('Address', '').strip()
                
                if not code or not name:
                    skipped_count += 1
                    errors.append(f"Row {row_num}: Missing required fields (Code, Name)")
                    continue
                
                existing = conn.execute('SELECT id FROM suppliers WHERE code = ?', (code,)).fetchone()
                
                if existing:
                    conn.execute('''
                        UPDATE suppliers 
                        SET name=?, contact_person=?, email=?, phone=?, address=?
                        WHERE code=?
                    ''', (name, contact_person, email, phone, address, code))
                else:
                    conn.execute('''
                        INSERT INTO suppliers (code, name, contact_person, email, phone, address)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (code, name, contact_person, email, phone, address))
                
                imported_count += 1
            except Exception as row_error:
                skipped_count += 1
                errors.append(f"Row {row_num}: {str(row_error)}")
        
        conn.commit()
        
        if imported_count > 0:
            flash(f'Successfully imported {imported_count} suppliers. Skipped {skipped_count} rows.', 'success')
        else:
            flash(f'No suppliers imported. Skipped {skipped_count} rows.', 'warning')
        
        if errors and len(errors) <= 10:
            for error in errors:
                flash(error, 'warning')
        elif errors:
            flash(f'First 10 errors: {"; ".join(errors[:10])}', 'warning')
            
    except Exception as e:
        flash(f'Error importing suppliers: {str(e)}', 'danger')
    finally:
        if conn:
            conn.close()
    
    return redirect(url_for('supplier_routes.list_suppliers'))
