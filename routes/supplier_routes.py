from flask import Blueprint, render_template, request, redirect, url_for, flash
from models import Database
from auth import login_required, role_required

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
