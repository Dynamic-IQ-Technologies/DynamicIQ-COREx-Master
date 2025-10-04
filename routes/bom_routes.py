from flask import Blueprint, render_template, request, redirect, url_for, flash
from models import Database
from auth import login_required, role_required

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
