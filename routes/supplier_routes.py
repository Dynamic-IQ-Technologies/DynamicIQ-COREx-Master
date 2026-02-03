from flask import Blueprint, render_template, request, redirect, url_for, flash, Response, jsonify, session
from models import Database, AuditLogger
from auth import login_required, role_required
from datetime import datetime, timedelta
import secrets
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
        
        supplier_name = request.form['name'].strip()
        
        existing = conn.execute('''
            SELECT id, code, name FROM suppliers 
            WHERE LOWER(name) = LOWER(?)
        ''', (supplier_name,)).fetchone()
        
        if existing:
            conn.close()
            flash(f'A supplier with this name already exists: {existing["name"]} ({existing["code"]})', 'danger')
            return redirect(url_for('supplier_routes.create_supplier'))
        
        conn.execute('''
            INSERT INTO suppliers (code, name, contact_person, email, phone, address)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            supplier_code,
            supplier_name,
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

@supplier_bp.route('/suppliers/<int:id>')
@login_required
def view_supplier(id):
    db = Database()
    conn = db.get_connection()
    
    supplier = conn.execute('SELECT * FROM suppliers WHERE id = ?', (id,)).fetchone()
    
    if not supplier:
        flash('Supplier not found', 'danger')
        conn.close()
        return redirect(url_for('supplier_routes.list_suppliers'))
    
    # Get contacts
    contacts = conn.execute('''
        SELECT * FROM supplier_contacts WHERE supplier_id = ? ORDER BY is_primary DESC, contact_name
    ''', (id,)).fetchall()
    
    # Get purchase orders
    purchase_orders = conn.execute('''
        SELECT po.*, 
               COALESCE(SUM(pol.quantity * pol.unit_price), 0) as total_amount,
               COUNT(pol.id) as line_count
        FROM purchase_orders po
        LEFT JOIN purchase_order_lines pol ON po.id = pol.po_id
        WHERE po.supplier_id = ?
        GROUP BY po.id
        ORDER BY po.order_date DESC
    ''', (id,)).fetchall()
    
    # Get financial metrics
    financials = {}
    
    # Total purchases (all time) - count of POs
    total_po_count = conn.execute('''
        SELECT COUNT(*) as count FROM purchase_orders 
        WHERE supplier_id = ? AND status NOT IN ('Cancelled', 'Draft')
    ''', (id,)).fetchone()
    financials['total_po_count'] = total_po_count['count'] if total_po_count else 0
    
    # Total purchase amount (all time)
    total_purchases = conn.execute('''
        SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) as total
        FROM purchase_orders po
        JOIN purchase_order_lines pol ON po.id = pol.po_id
        WHERE po.supplier_id = ? AND po.status NOT IN ('Cancelled', 'Draft')
    ''', (id,)).fetchone()
    financials['total_purchase_amount'] = total_purchases['total'] if total_purchases else 0
    
    # YTD purchases
    ytd_purchases = conn.execute('''
        SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) as total
        FROM purchase_orders po
        JOIN purchase_order_lines pol ON po.id = pol.po_id
        WHERE po.supplier_id = ? AND po.status NOT IN ('Cancelled', 'Draft')
        AND strftime('%Y', po.order_date) = strftime('%Y', 'now')
    ''', (id,)).fetchone()
    financials['ytd_purchases'] = ytd_purchases['total'] if ytd_purchases else 0
    
    # Last year purchases
    last_year_purchases = conn.execute('''
        SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) as total
        FROM purchase_orders po
        JOIN purchase_order_lines pol ON po.id = pol.po_id
        WHERE po.supplier_id = ? AND po.status NOT IN ('Cancelled', 'Draft')
        AND strftime('%Y', po.order_date) = strftime('%Y', 'now', '-1 year')
    ''', (id,)).fetchone()
    financials['last_year_purchases'] = last_year_purchases['total'] if last_year_purchases else 0
    
    # Open POs (not completed or cancelled)
    open_pos = conn.execute('''
        SELECT COUNT(*) as count,
               COALESCE(SUM(pol.quantity * pol.unit_price), 0) as total
        FROM purchase_orders po
        LEFT JOIN purchase_order_lines pol ON po.id = pol.po_id
        WHERE po.supplier_id = ? AND po.status IN ('Open', 'Pending', 'Partial')
        GROUP BY po.supplier_id
    ''', (id,)).fetchone()
    financials['open_po_count'] = open_pos['count'] if open_pos else 0
    financials['open_po_amount'] = open_pos['total'] if open_pos else 0
    
    # Average PO value
    financials['avg_po_value'] = financials['total_purchase_amount'] / financials['total_po_count'] if financials['total_po_count'] > 0 else 0
    
    # Last PO date
    last_po = conn.execute('''
        SELECT order_date FROM purchase_orders 
        WHERE supplier_id = ? AND status NOT IN ('Cancelled', 'Draft')
        ORDER BY order_date DESC LIMIT 1
    ''', (id,)).fetchone()
    financials['last_po_date'] = last_po['order_date'] if last_po else None
    
    # First PO date (supplier since)
    first_po = conn.execute('''
        SELECT order_date FROM purchase_orders 
        WHERE supplier_id = ? AND status NOT IN ('Cancelled', 'Draft')
        ORDER BY order_date ASC LIMIT 1
    ''', (id,)).fetchone()
    financials['first_po_date'] = first_po['order_date'] if first_po else None
    
    # Pending receiving amount (quantity ordered - received)
    pending_receiving = conn.execute('''
        SELECT COALESCE(SUM((pol.quantity - pol.received_quantity) * pol.unit_price), 0) as total
        FROM purchase_orders po
        JOIN purchase_order_lines pol ON po.id = pol.po_id
        WHERE po.supplier_id = ? AND po.status IN ('Open', 'Pending', 'Partial')
        AND pol.quantity > pol.received_quantity
    ''', (id,)).fetchone()
    financials['pending_receiving_amount'] = pending_receiving['total'] if pending_receiving else 0
    
    # Get audit trail
    audit_trail = conn.execute('''
        SELECT at.*, u.username 
        FROM audit_trail at
        LEFT JOIN users u ON at.modified_by = u.id
        WHERE at.record_type = 'suppliers' AND at.record_id = ?
        ORDER BY at.modified_at DESC
        LIMIT 50
    ''', (str(id),)).fetchall()
    
    portal_token = conn.execute('''
        SELECT spt.*, u.username as created_by_name
        FROM supplier_portal_tokens spt
        LEFT JOIN users u ON spt.created_by = u.id
        WHERE spt.supplier_id = ? AND spt.is_active = 1
        ORDER BY spt.created_at DESC LIMIT 1
    ''', (id,)).fetchone()
    
    conn.close()
    return render_template('suppliers/view.html', supplier=supplier, contacts=contacts,
                          purchase_orders=purchase_orders, financials=financials, 
                          audit_trail=audit_trail, portal_token=portal_token)

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


@supplier_bp.route('/suppliers/<int:id>/generate-portal-link', methods=['POST'])
@role_required('Admin', 'Procurement')
def generate_portal_link(id):
    """Generate a supplier portal access link"""
    db = Database()
    conn = db.get_connection()
    
    supplier = conn.execute('SELECT * FROM suppliers WHERE id = ?', (id,)).fetchone()
    if not supplier:
        conn.close()
        flash('Supplier not found', 'danger')
        return redirect(url_for('supplier_routes.list_suppliers'))
    
    expiry_days = int(request.form.get('expiry_days', 90))
    
    token = secrets.token_urlsafe(32)
    expires_at = (datetime.now() + timedelta(days=expiry_days)).isoformat()
    
    conn.execute('''
        UPDATE supplier_portal_tokens SET is_active = 0 WHERE supplier_id = ?
    ''', (id,))
    
    conn.execute('''
        INSERT INTO supplier_portal_tokens 
        (supplier_id, token, expires_at, is_active, created_by)
        VALUES (?, ?, ?, 1, ?)
    ''', (id, token, expires_at, session.get('user_id')))
    
    AuditLogger.log_change(
        conn=conn,
        record_type='supplier_portal_tokens',
        record_id=id,
        action_type='CREATE',
        modified_by=session.get('user_id'),
        changed_fields={
            'supplier_id': id,
            'expires_at': expires_at,
            'expiry_days': expiry_days
        }
    )
    
    conn.commit()
    conn.close()
    
    flash(f'Supplier portal link generated successfully! Valid for {expiry_days} days.', 'success')
    return redirect(url_for('supplier_routes.view_supplier', id=id))


@supplier_bp.route('/suppliers/<int:id>/revoke-portal-link', methods=['POST'])
@role_required('Admin', 'Procurement')
def revoke_portal_link(id):
    """Revoke all supplier portal access links"""
    db = Database()
    conn = db.get_connection()
    
    conn.execute('''
        UPDATE supplier_portal_tokens SET is_active = 0 WHERE supplier_id = ?
    ''', (id,))
    
    AuditLogger.log_change(
        conn=conn,
        record_type='supplier_portal_tokens',
        record_id=id,
        action_type='REVOKE',
        modified_by=session.get('user_id'),
        changed_fields={'supplier_id': id, 'action': 'Revoked all portal links'}
    )
    
    conn.commit()
    conn.close()
    
    flash('Supplier portal links revoked successfully.', 'success')
    return redirect(url_for('supplier_routes.view_supplier', id=id))


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
