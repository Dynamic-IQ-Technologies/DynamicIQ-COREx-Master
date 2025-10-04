from flask import Blueprint, render_template, request, redirect, url_for, flash
from models import Database
from auth import login_required, role_required

product_bp = Blueprint('product_routes', __name__)

@product_bp.route('/products')
@login_required
def list_products():
    db = Database()
    conn = db.get_connection()
    products = conn.execute('SELECT * FROM products ORDER BY code').fetchall()
    conn.close()
    return render_template('products/list.html', products=products)

@product_bp.route('/products/create', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def create_product():
    if request.method == 'POST':
        db = Database()
        conn = db.get_connection()
        
        conn.execute('''
            INSERT INTO products (code, name, description, unit_of_measure, product_type, cost)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            request.form['code'],
            request.form['name'],
            request.form['description'],
            request.form['unit_of_measure'],
            request.form['product_type'],
            float(request.form['cost'])
        ))
        
        product_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        
        conn.execute('''
            INSERT INTO inventory (product_id, quantity, reorder_point, safety_stock)
            VALUES (?, 0, ?, ?)
        ''', (product_id, float(request.form.get('reorder_point', 0)), float(request.form.get('safety_stock', 0))))
        
        conn.commit()
        conn.close()
        
        flash('Product created successfully!', 'success')
        return redirect(url_for('product_routes.list_products'))
    
    return render_template('products/create.html')

@product_bp.route('/products/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def edit_product(id):
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        conn.execute('''
            UPDATE products 
            SET code=?, name=?, description=?, unit_of_measure=?, product_type=?, cost=?
            WHERE id=?
        ''', (
            request.form['code'],
            request.form['name'],
            request.form['description'],
            request.form['unit_of_measure'],
            request.form['product_type'],
            float(request.form['cost']),
            id
        ))
        
        conn.execute('''
            UPDATE inventory 
            SET reorder_point=?, safety_stock=?
            WHERE product_id=?
        ''', (float(request.form.get('reorder_point', 0)), float(request.form.get('safety_stock', 0)), id))
        
        conn.commit()
        conn.close()
        
        flash('Product updated successfully!', 'success')
        return redirect(url_for('product_routes.list_products'))
    
    product = conn.execute('SELECT * FROM products WHERE id=?', (id,)).fetchone()
    inventory = conn.execute('SELECT * FROM inventory WHERE product_id=?', (id,)).fetchone()
    conn.close()
    
    return render_template('products/edit.html', product=product, inventory=inventory)

@product_bp.route('/products/<int:id>/delete', methods=['POST'])
@role_required('Admin')
def delete_product(id):
    db = Database()
    conn = db.get_connection()
    conn.execute('DELETE FROM products WHERE id=?', (id,))
    conn.commit()
    conn.close()
    
    flash('Product deleted successfully!', 'success')
    return redirect(url_for('product_routes.list_products'))
