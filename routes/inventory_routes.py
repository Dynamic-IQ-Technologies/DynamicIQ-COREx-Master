from flask import Blueprint, render_template, request, redirect, url_for, flash
from models import Database
from auth import login_required, role_required

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
