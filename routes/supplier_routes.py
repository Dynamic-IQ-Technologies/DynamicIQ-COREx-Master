from flask import Blueprint, render_template, request, redirect, url_for, flash, Response, jsonify, session
from models import Database, AuditLogger
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

@supplier_bp.route('/suppliers/list-json')
@login_required
def list_suppliers_json():
    db = Database()
    conn = db.get_connection()
    suppliers = conn.execute('SELECT id, code, name, email, phone FROM suppliers ORDER BY code').fetchall()
    conn.close()
    return jsonify([dict(s) for s in suppliers])

@supplier_bp.route('/suppliers/create', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement')
def create_supplier():
    if request.method == 'POST':
        db = Database()
        conn = db.get_connection()
        
        # Auto-generate supplier code
        last_supplier = conn.execute('''
            SELECT code FROM suppliers 
            WHERE code LIKE 'SUP-%'
            ORDER BY CAST(SUBSTR(code, 5) AS INTEGER) DESC 
            LIMIT 1
        ''').fetchone()
        
        if last_supplier:
            try:
                last_number = int(last_supplier['code'].split('-')[1])
                next_number = last_number + 1
            except (ValueError, IndexError):
                next_number = 1
        else:
            next_number = 1
        
        supplier_code = f'SUP-{next_number:06d}'
        
        conn.execute('''
            INSERT INTO suppliers (code, name, contact_person, email, phone, address)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            supplier_code,
            request.form['name'],
            request.form.get('contact_person', ''),
            request.form.get('email', ''),
            request.form.get('phone', ''),
            request.form.get('address', '')
        ))
        
        supplier_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        AuditLogger.log_change(conn, 'suppliers', supplier_id, 'CREATE', session.get('user_id'),
                              {'code': supplier_code, 'name': request.form['name']})
        conn.commit()
        conn.close()
        
        flash(f'Supplier created successfully! Code: {supplier_code}', 'success')
        return redirect(url_for('supplier_routes.list_suppliers'))
    
    return render_template('suppliers/create.html')

@supplier_bp.route('/suppliers/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement')
def edit_supplier(id):
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        old_supplier = conn.execute('SELECT * FROM suppliers WHERE id = ?', (id,)).fetchone()
        
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
        
        AuditLogger.log_change(conn, 'suppliers', id, 'UPDATE', session.get('user_id'),
                              {'code': request.form['code'], 'name': request.form['name'],
                               'old_name': old_supplier['name']})
        conn.commit()
        conn.close()
        
        flash('Supplier updated successfully!', 'success')
        return redirect(url_for('supplier_routes.list_suppliers'))
    
    supplier = conn.execute('SELECT * FROM suppliers WHERE id=?', (id,)).fetchone()
    contacts = conn.execute('''
        SELECT * FROM supplier_contacts WHERE supplier_id = ? ORDER BY is_primary DESC, contact_name
    ''', (id,)).fetchall()
    conn.close()
    
    return render_template('suppliers/edit.html', supplier=supplier, contacts=contacts)

@supplier_bp.route('/suppliers/<int:id>/delete', methods=['POST'])
@role_required('Admin')
def delete_supplier(id):
    db = Database()
    conn = db.get_connection()
    
    supplier = conn.execute('SELECT * FROM suppliers WHERE id = ?', (id,)).fetchone()
    if supplier:
        AuditLogger.log_change(conn, 'suppliers', id, 'DELETE', session.get('user_id'),
                              {'code': supplier['code'], 'name': supplier['name']})
    
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

# Supplier Contacts Management
@supplier_bp.route('/suppliers/<int:supplier_id>/contacts')
@role_required('Admin', 'Procurement')
def list_contacts(supplier_id):
    """Get contacts for a supplier as JSON"""
    db = Database()
    conn = db.get_connection()
    
    supplier = conn.execute('SELECT id FROM suppliers WHERE id = ?', (supplier_id,)).fetchone()
    if not supplier:
        conn.close()
        return jsonify({'error': 'Supplier not found'}), 404
    
    contacts = conn.execute('''
        SELECT id, contact_name, title, email, phone, mobile, department, is_primary
        FROM supplier_contacts WHERE supplier_id = ? ORDER BY is_primary DESC, contact_name
    ''', (supplier_id,)).fetchall()
    conn.close()
    return jsonify([dict(c) for c in contacts])

@supplier_bp.route('/suppliers/<int:supplier_id>/contacts/add', methods=['POST'])
@role_required('Admin', 'Procurement')
def add_contact(supplier_id):
    """Add a new contact to a supplier"""
    db = Database()
    conn = db.get_connection()
    
    supplier = conn.execute('SELECT id FROM suppliers WHERE id = ?', (supplier_id,)).fetchone()
    if not supplier:
        conn.close()
        flash('Supplier not found', 'danger')
        return redirect(url_for('supplier_routes.list_suppliers'))
    
    is_primary = 1 if request.form.get('is_primary') else 0
    
    if is_primary:
        conn.execute('UPDATE supplier_contacts SET is_primary = 0 WHERE supplier_id = ?', (supplier_id,))
    
    conn.execute('''
        INSERT INTO supplier_contacts (supplier_id, contact_name, title, email, phone, mobile, department, is_primary, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        supplier_id,
        request.form['contact_name'],
        request.form.get('title', ''),
        request.form.get('email', ''),
        request.form.get('phone', ''),
        request.form.get('mobile', ''),
        request.form.get('department', ''),
        is_primary,
        request.form.get('notes', '')
    ))
    
    conn.commit()
    conn.close()
    
    flash('Contact added successfully!', 'success')
    return redirect(url_for('supplier_routes.edit_supplier', id=supplier_id))

@supplier_bp.route('/suppliers/<int:supplier_id>/contacts/<int:contact_id>/edit', methods=['POST'])
@role_required('Admin', 'Procurement')
def edit_contact(supplier_id, contact_id):
    """Edit an existing contact"""
    db = Database()
    conn = db.get_connection()
    
    contact = conn.execute('SELECT id FROM supplier_contacts WHERE id = ? AND supplier_id = ?', (contact_id, supplier_id)).fetchone()
    if not contact:
        conn.close()
        flash('Contact not found', 'danger')
        return redirect(url_for('supplier_routes.list_suppliers'))
    
    is_primary = 1 if request.form.get('is_primary') else 0
    
    if is_primary:
        conn.execute('UPDATE supplier_contacts SET is_primary = 0 WHERE supplier_id = ?', (supplier_id,))
    
    conn.execute('''
        UPDATE supplier_contacts 
        SET contact_name=?, title=?, email=?, phone=?, mobile=?, department=?, is_primary=?, notes=?
        WHERE id=? AND supplier_id=?
    ''', (
        request.form['contact_name'],
        request.form.get('title', ''),
        request.form.get('email', ''),
        request.form.get('phone', ''),
        request.form.get('mobile', ''),
        request.form.get('department', ''),
        is_primary,
        request.form.get('notes', ''),
        contact_id,
        supplier_id
    ))
    
    conn.commit()
    conn.close()
    
    flash('Contact updated successfully!', 'success')
    return redirect(url_for('supplier_routes.edit_supplier', id=supplier_id))

@supplier_bp.route('/suppliers/<int:supplier_id>/contacts/<int:contact_id>/delete', methods=['POST'])
@role_required('Admin', 'Procurement')
def delete_contact(supplier_id, contact_id):
    """Delete a contact"""
    db = Database()
    conn = db.get_connection()
    
    contact = conn.execute('SELECT id FROM supplier_contacts WHERE id = ? AND supplier_id = ?', (contact_id, supplier_id)).fetchone()
    if not contact:
        conn.close()
        flash('Contact not found', 'danger')
        return redirect(url_for('supplier_routes.list_suppliers'))
    
    conn.execute('DELETE FROM supplier_contacts WHERE id = ? AND supplier_id = ?', (contact_id, supplier_id))
    conn.commit()
    conn.close()
    
    flash('Contact deleted successfully!', 'success')
    return redirect(url_for('supplier_routes.edit_supplier', id=supplier_id))

@supplier_bp.route('/suppliers/<int:supplier_id>/contacts/<int:contact_id>/set-primary', methods=['POST'])
@role_required('Admin', 'Procurement')
def set_primary_contact(supplier_id, contact_id):
    """Set a contact as primary"""
    db = Database()
    conn = db.get_connection()
    
    contact = conn.execute('SELECT id FROM supplier_contacts WHERE id = ? AND supplier_id = ?', (contact_id, supplier_id)).fetchone()
    if not contact:
        conn.close()
        flash('Contact not found', 'danger')
        return redirect(url_for('supplier_routes.list_suppliers'))
    
    conn.execute('UPDATE supplier_contacts SET is_primary = 0 WHERE supplier_id = ?', (supplier_id,))
    conn.execute('UPDATE supplier_contacts SET is_primary = 1 WHERE id = ? AND supplier_id = ?', (contact_id, supplier_id))
    conn.commit()
    conn.close()
    
    flash('Primary contact updated!', 'success')
    return redirect(url_for('supplier_routes.edit_supplier', id=supplier_id))
