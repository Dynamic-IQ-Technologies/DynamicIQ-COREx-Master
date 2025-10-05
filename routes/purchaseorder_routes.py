from flask import Blueprint, render_template, request, redirect, url_for, flash, make_response, session
from models import Database, CompanySettings
from mrp_logic import MRPEngine
from auth import login_required, role_required
from datetime import datetime

po_bp = Blueprint('po_routes', __name__)

@po_bp.route('/purchaseorders')
@login_required
def list_purchaseorders():
    db = Database()
    conn = db.get_connection()
    purchase_orders = conn.execute('''
        SELECT po.*, s.name as supplier_name, p.code, p.name as product_name, p.unit_of_measure,
               uom.uom_code, uom.uom_name
        FROM purchase_orders po
        JOIN suppliers s ON po.supplier_id = s.id
        JOIN products p ON po.product_id = p.id
        LEFT JOIN uom_master uom ON po.uom_id = uom.id
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
                    (po_number, supplier_id, product_id, quantity, unit_price, status, order_date, expected_delivery_date, uom_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    po_number,
                    int(request.form['supplier_id']),
                    int(request.form['product_id']),
                    float(request.form['quantity']),
                    float(request.form['unit_price']),
                    request.form['status'],
                    request.form.get('order_date'),
                    request.form.get('expected_delivery_date'),
                    int(request.form['uom_id']) if request.form.get('uom_id') else None
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
    uoms = conn.execute('SELECT * FROM uom_master WHERE is_active = 1 ORDER BY uom_code').fetchall()
    
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
    
    return render_template('purchaseorders/create.html', suppliers=suppliers, products=products, uoms=uoms, next_po_number=next_po_number)

@po_bp.route('/purchaseorders/<int:id>')
@login_required
def view_purchaseorder(id):
    db = Database()
    conn = db.get_connection()
    
    po = conn.execute('''
        SELECT po.*, s.name as supplier_name, s.contact_person, s.email, s.phone,
               p.code as product_code, p.name as product_name, p.unit_of_measure,
               i.quantity as inventory_quantity,
               uom.uom_code, uom.uom_name
        FROM purchase_orders po
        JOIN suppliers s ON po.supplier_id = s.id
        JOIN products p ON po.product_id = p.id
        LEFT JOIN inventory i ON i.product_id = p.id
        LEFT JOIN uom_master uom ON po.uom_id = uom.id
        WHERE po.id = ?
    ''', (id,)).fetchone()
    
    conn.close()
    
    if not po:
        flash('Purchase order not found', 'danger')
        return redirect(url_for('po_routes.list_purchaseorders'))
    
    return render_template('purchaseorders/view.html', po=po)

@po_bp.route('/purchaseorders/<int:id>/print')
@login_required
def print_purchaseorder(id):
    db = Database()
    conn = db.get_connection()
    
    po = conn.execute('''
        SELECT po.*, s.name as supplier_name, s.contact_person, s.email, s.phone, s.address,
               p.code as product_code, p.name as product_name, p.unit_of_measure, p.description,
               uom.uom_code, uom.uom_name
        FROM purchase_orders po
        JOIN suppliers s ON po.supplier_id = s.id
        JOIN products p ON po.product_id = p.id
        LEFT JOIN uom_master uom ON po.uom_id = uom.id
        WHERE po.id = ?
    ''', (id,)).fetchone()
    
    conn.close()
    
    if not po:
        flash('Purchase order not found', 'danger')
        return redirect(url_for('po_routes.list_purchaseorders'))
    
    company_settings = CompanySettings.get_or_create_default()
    
    return render_template('purchaseorders/print.html', po=po, company_settings=company_settings, current_date=datetime.now().strftime('%B %d, %Y'))

@po_bp.route('/purchaseorders/<int:id>/download')
@login_required
def download_purchaseorder(id):
    db = Database()
    conn = db.get_connection()
    
    po = conn.execute('''
        SELECT po.*, s.name as supplier_name, s.contact_person, s.email, s.phone, s.address,
               p.code as product_code, p.name as product_name, p.unit_of_measure, p.description,
               uom.uom_code, uom.uom_name
        FROM purchase_orders po
        JOIN suppliers s ON po.supplier_id = s.id
        JOIN products p ON po.product_id = p.id
        LEFT JOIN uom_master uom ON po.uom_id = uom.id
        WHERE po.id = ?
    ''', (id,)).fetchone()
    
    conn.close()
    
    if not po:
        flash('Purchase order not found', 'danger')
        return redirect(url_for('po_routes.list_purchaseorders'))
    
    company_settings = CompanySettings.get_or_create_default()
    
    html_content = render_template('purchaseorders/print.html', po=po, company_settings=company_settings, current_date=datetime.now().strftime('%B %d, %Y'))
    
    response = make_response(html_content)
    response.headers['Content-Type'] = 'text/html'
    response.headers['Content-Disposition'] = f'attachment; filename=PO_{po["po_number"]}.html'
    
    return response

@po_bp.route('/purchaseorders/<int:id>/receive', methods=['POST'])
@role_required('Admin', 'Procurement')
def receive_purchaseorder(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        po = conn.execute('SELECT * FROM purchase_orders WHERE id=?', (id,)).fetchone()
        
        if not po:
            flash('Purchase Order not found', 'danger')
            return redirect(url_for('po_routes.list_purchaseorders'))
        
        # Get form data
        receipt_date = request.form.get('receipt_date')
        quantity_received = float(request.form.get('quantity_received', 0))
        condition = request.form.get('condition', 'New')
        warehouse_location = request.form.get('warehouse_location', '').strip()
        bin_location = request.form.get('bin_location', '').strip()
        remarks = request.form.get('remarks', '')
        product_id_str = request.form.get('product_id')
        
        if not product_id_str:
            flash('Product ID is required', 'danger')
            conn.close()
            return redirect(url_for('po_routes.list_purchaseorders'))
        
        product_id = int(product_id_str)
        
        # Validate required fields
        if not warehouse_location:
            flash('Warehouse location is required', 'danger')
            conn.close()
            return redirect(url_for('po_routes.list_purchaseorders'))
        
        if not bin_location:
            flash('Bin location is required', 'danger')
            conn.close()
            return redirect(url_for('po_routes.list_purchaseorders'))
        
        if quantity_received <= 0:
            flash('Quantity received must be greater than 0', 'danger')
            conn.close()
            return redirect(url_for('po_routes.list_purchaseorders'))
        
        # Validate quantity not exceeding ordered quantity
        remaining = po['quantity'] - (po['received_quantity'] or 0)
        if quantity_received > remaining:
            flash(f'Quantity exceeds remaining amount ({remaining})', 'danger')
            conn.close()
            return redirect(url_for('po_routes.list_purchaseorders'))
        
        # Generate receipt number
        last_receipt = conn.execute('''
            SELECT receipt_number FROM receiving_transactions 
            WHERE receipt_number LIKE 'RCV-%'
            ORDER BY CAST(SUBSTR(receipt_number, 5) AS INTEGER) DESC 
            LIMIT 1
        ''').fetchone()
        
        if last_receipt:
            try:
                last_number = int(last_receipt['receipt_number'].split('-')[1])
                next_number = last_number + 1
            except (ValueError, IndexError):
                next_number = 1
        else:
            next_number = 1
        
        receipt_number = f'RCV-{next_number:06d}'
        
        # Create receiving transaction
        conn.execute('''
            INSERT INTO receiving_transactions 
            (receipt_number, po_id, product_id, quantity_received, receipt_date, condition, 
             warehouse_location, bin_location, remarks, received_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (receipt_number, id, product_id, quantity_received, receipt_date, condition,
              warehouse_location, bin_location, remarks, session['user_id']))
        
        # Update inventory with combined warehouse + bin location
        combined_location = f"{warehouse_location}/{bin_location}"
        inventory = conn.execute('SELECT * FROM inventory WHERE product_id=?', (product_id,)).fetchone()
        
        inventory_id = None
        if inventory:
            new_qty = inventory['quantity'] + quantity_received
            conn.execute('''
                UPDATE inventory 
                SET quantity=?, 
                    warehouse_location=?,
                    bin_location=?,
                    condition=?,
                    status = CASE 
                        WHEN ? > (reorder_point + safety_stock) THEN 'Available'
                        ELSE status 
                    END,
                    last_updated=CURRENT_TIMESTAMP 
                WHERE product_id=?
            ''', (new_qty, warehouse_location, bin_location, condition, new_qty, product_id))
            inventory_id = inventory['id']
        else:
            # Create new inventory record
            conn.execute('''
                INSERT INTO inventory 
                (product_id, quantity, warehouse_location, bin_location, condition, 
                 status, reorder_point, safety_stock)
                VALUES (?, ?, ?, ?, ?, 'Available', 0, 0)
            ''', (product_id, quantity_received, warehouse_location, bin_location, condition))
            inventory_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        
        # Update purchase order
        new_received = (po['received_quantity'] or 0) + quantity_received
        new_status = 'Received' if new_received >= po['quantity'] else 'Partial'
        
        conn.execute('''
            UPDATE purchase_orders 
            SET received_quantity=?, 
                status=?,
                actual_delivery_date=? 
            WHERE id=?
        ''', (new_received, new_status, receipt_date, id))
        
        conn.commit()
        
        flash(f'Material received successfully! Receipt: {receipt_number}, Inventory: INV-{inventory_id:06d}', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error receiving material: {str(e)}', 'danger')
    finally:
        conn.close()
    
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
