from flask import Blueprint, render_template, request, redirect, url_for, flash
from models import Database
from mrp_logic import MRPEngine
from auth import login_required, role_required

po_bp = Blueprint('po_routes', __name__)

@po_bp.route('/purchaseorders')
@login_required
def list_purchaseorders():
    db = Database()
    conn = db.get_connection()
    purchase_orders = conn.execute('''
        SELECT po.*, s.name as supplier_name, p.code, p.name as product_name
        FROM purchase_orders po
        JOIN suppliers s ON po.supplier_id = s.id
        JOIN products p ON po.product_id = p.id
        ORDER BY po.order_date DESC
    ''').fetchall()
    conn.close()
    return render_template('purchaseorders/list.html', purchase_orders=purchase_orders)

@po_bp.route('/purchaseorders/create', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement')
def create_purchaseorder():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        max_attempts = 5
        po_number = None
        po_id = None
        
        for attempt in range(max_attempts):
            try:
                last_po = conn.execute('''
                    SELECT po_number FROM purchase_orders 
                    WHERE po_number LIKE 'PO-%'
                    ORDER BY CAST(SUBSTR(po_number, 4) AS INTEGER) DESC 
                    LIMIT 1
                ''').fetchone()
                
                if last_po:
                    try:
                        last_number = int(last_po['po_number'].split('-')[1])
                        next_number = last_number + 1
                    except (ValueError, IndexError):
                        next_number = 1
                else:
                    next_number = 1
                
                po_number = f'PO-{next_number:06d}'
                
                conn.execute('''
                    INSERT INTO purchase_orders 
                    (po_number, supplier_id, product_id, quantity, unit_price, status, order_date, expected_delivery_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    po_number,
                    int(request.form['supplier_id']),
                    int(request.form['product_id']),
                    float(request.form['quantity']),
                    float(request.form['unit_price']),
                    request.form['status'],
                    request.form.get('order_date'),
                    request.form.get('expected_delivery_date')
                ))
                
                po_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
                conn.commit()
                break
                
            except Exception as e:
                if 'UNIQUE constraint failed' in str(e) and attempt < max_attempts - 1:
                    conn.rollback()
                    continue
                else:
                    conn.close()
                    flash(f'Error creating purchase order: {str(e)}', 'danger')
                    return redirect(url_for('po_routes.list_purchaseorders'))
        
        conn.close()
        
        if po_id:
            flash(f'Purchase Order {po_number} created successfully!', 'success')
            return redirect(url_for('po_routes.list_purchaseorders'))
        else:
            flash('Failed to create purchase order after multiple attempts', 'danger')
            return redirect(url_for('po_routes.list_purchaseorders'))
    
    suppliers = conn.execute('SELECT * FROM suppliers ORDER BY code').fetchall()
    products = conn.execute('SELECT * FROM products ORDER BY code').fetchall()
    
    last_po = conn.execute('''
        SELECT po_number FROM purchase_orders 
        WHERE po_number LIKE 'PO-%'
        ORDER BY CAST(SUBSTR(po_number, 4) AS INTEGER) DESC 
        LIMIT 1
    ''').fetchone()
    
    if last_po:
        try:
            last_number = int(last_po['po_number'].split('-')[1])
            next_number = last_number + 1
        except (ValueError, IndexError):
            next_number = 1
    else:
        next_number = 1
    
    next_po_number = f'PO-{next_number:06d}'
    
    conn.close()
    
    return render_template('purchaseorders/create.html', suppliers=suppliers, products=products, next_po_number=next_po_number)

@po_bp.route('/purchaseorders/<int:id>/receive', methods=['POST'])
@role_required('Admin', 'Procurement')
def receive_purchaseorder(id):
    db = Database()
    conn = db.get_connection()
    
    po = conn.execute('SELECT * FROM purchase_orders WHERE id=?', (id,)).fetchone()
    
    conn.execute('UPDATE purchase_orders SET status="Received", actual_delivery_date=CURRENT_DATE WHERE id=?', (id,))
    
    inventory = conn.execute('SELECT * FROM inventory WHERE product_id=?', (po['product_id'],)).fetchone()
    
    if inventory:
        new_qty = inventory['quantity'] + po['quantity']
        conn.execute('UPDATE inventory SET quantity=?, last_updated=CURRENT_TIMESTAMP WHERE product_id=?', 
                    (new_qty, po['product_id']))
    else:
        conn.execute('INSERT INTO inventory (product_id, quantity) VALUES (?, ?)', 
                    (po['product_id'], po['quantity']))
    
    conn.commit()
    conn.close()
    
    flash('Purchase Order received and inventory updated!', 'success')
    return redirect(url_for('po_routes.list_purchaseorders'))

@po_bp.route('/purchaseorders/suggestions')
@role_required('Admin', 'Procurement', 'Planner')
def purchase_suggestions():
    mrp = MRPEngine()
    suggestions = mrp.suggest_purchase_orders()
    
    db = Database()
    conn = db.get_connection()
    suppliers = conn.execute('SELECT * FROM suppliers ORDER BY code').fetchall()
    conn.close()
    
    return render_template('purchaseorders/suggestions.html', 
                         suggestions=suggestions,
                         suppliers=suppliers)
