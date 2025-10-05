from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import Database
from auth import login_required, role_required
from datetime import datetime

receiving_bp = Blueprint('receiving_routes', __name__)

@receiving_bp.route('/receiving')
@login_required
def list_receiving():
    db = Database()
    conn = db.get_connection()
    
    receipts = conn.execute('''
        SELECT 
            rt.*,
            po.po_number,
            p.code as product_code,
            p.name as product_name,
            p.unit_of_measure,
            s.name as supplier_name,
            u.username as received_by_name
        FROM receiving_transactions rt
        JOIN purchase_orders po ON rt.po_id = po.id
        JOIN products p ON rt.product_id = p.id
        JOIN suppliers s ON po.supplier_id = s.id
        LEFT JOIN users u ON rt.received_by = u.id
        ORDER BY rt.receipt_date DESC, rt.created_at DESC
    ''').fetchall()
    
    conn.close()
    return render_template('receiving/list.html', receipts=receipts)

@receiving_bp.route('/receiving/create', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement', 'Production Staff')
def create_receiving():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            po_id = int(request.form['po_id'])
            product_id = int(request.form['product_id'])
            quantity_received = float(request.form['quantity_received'])
            receipt_date = request.form['receipt_date']
            packing_slip = request.form.get('packing_slip_number', '')
            tracking = request.form.get('shipment_tracking', '')
            warehouse = request.form.get('warehouse_location', '').strip()
            bin_location = request.form.get('bin_location', '').strip()
            receiver = request.form.get('receiver_name', '')
            condition = request.form.get('condition', 'New')
            remarks = request.form.get('remarks', '')
            
            # Validate required location fields
            if not warehouse:
                flash('Warehouse location is required.', 'danger')
                conn.close()
                return redirect(url_for('receiving_routes.create_receiving'))
            
            if not bin_location:
                flash('Bin location is required.', 'danger')
                conn.close()
                return redirect(url_for('receiving_routes.create_receiving'))
            
            # Get PO details
            po = conn.execute('''
                SELECT po.*, p.name as product_name
                FROM purchase_orders po
                JOIN products p ON po.product_id = p.id
                WHERE po.id = ?
            ''', (po_id,)).fetchone()
            
            if not po:
                flash('Purchase Order not found.', 'danger')
                conn.close()
                return redirect(url_for('receiving_routes.create_receiving'))
            
            # Validate product_id matches the PO
            if po['product_id'] != product_id:
                flash('Product does not match the selected Purchase Order.', 'danger')
                conn.close()
                return redirect(url_for('receiving_routes.create_receiving'))
            
            # Validate quantity
            received_so_far = po['received_quantity'] if po['received_quantity'] else 0
            remaining = po['quantity'] - received_so_far
            
            if quantity_received > remaining:
                flash(f'Cannot receive {quantity_received} units. Only {remaining} units remaining on PO.', 'danger')
                conn.close()
                return redirect(url_for('receiving_routes.create_receiving'))
            
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
                (receipt_number, po_id, product_id, quantity_received, receipt_date, 
                 packing_slip_number, shipment_tracking, warehouse_location, bin_location, 
                 receiver_name, condition, remarks, received_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (receipt_number, po_id, product_id, quantity_received, receipt_date,
                  packing_slip, tracking, warehouse, bin_location, receiver, condition, remarks, session['user_id']))
            
            # Update PO received quantity
            new_received = received_so_far + quantity_received
            conn.execute('''
                UPDATE purchase_orders 
                SET received_quantity = ?,
                    actual_delivery_date = CASE WHEN actual_delivery_date IS NULL THEN ? ELSE actual_delivery_date END,
                    status = CASE WHEN ? >= quantity THEN 'Received' ELSE status END
                WHERE id = ?
            ''', (new_received, receipt_date, new_received, po_id))
            
            # Update inventory
            inventory = conn.execute('''
                SELECT * FROM inventory WHERE product_id = ?
            ''', (product_id,)).fetchone()
            
            if inventory:
                new_qty = inventory['quantity'] + quantity_received
                conn.execute('''
                    UPDATE inventory 
                    SET quantity = ?,
                        last_received_date = ?,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE product_id = ?
                ''', (new_qty, receipt_date, product_id))
            else:
                # Create inventory record with location info
                conn.execute('''
                    INSERT INTO inventory 
                    (product_id, quantity, condition, warehouse_location, bin_location, last_received_date, status)
                    VALUES (?, ?, ?, ?, ?, ?, 'Available')
                ''', (product_id, quantity_received, condition, warehouse, bin_location, receipt_date))
            
            conn.commit()
            flash(f'Material received successfully! Receipt Number: {receipt_number}', 'success')
            return redirect(url_for('receiving_routes.view_receiving', receipt_number=receipt_number))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error receiving material: {str(e)}', 'danger')
        finally:
            conn.close()
    
    # GET request - show form
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Get pending/ordered POs
    pos = conn.execute('''
        SELECT 
            po.*,
            s.name as supplier_name,
            p.code as product_code,
            p.name as product_name,
            p.unit_of_measure,
            COALESCE(po.received_quantity, 0) as received_so_far,
            (po.quantity - COALESCE(po.received_quantity, 0)) as remaining_quantity
        FROM purchase_orders po
        JOIN suppliers s ON po.supplier_id = s.id
        JOIN products p ON po.product_id = p.id
        WHERE po.status IN ('Ordered', 'Partially Received')
            AND (po.received_quantity IS NULL OR po.received_quantity < po.quantity)
        ORDER BY po.expected_delivery_date, po.order_date DESC
    ''').fetchall()
    
    conn.close()
    return render_template('receiving/create.html', pos=pos, today=today)

@receiving_bp.route('/receiving/<receipt_number>')
@login_required
def view_receiving(receipt_number):
    db = Database()
    conn = db.get_connection()
    
    receipt = conn.execute('''
        SELECT 
            rt.*,
            po.po_number,
            po.order_date,
            po.unit_price,
            po.quantity as po_quantity,
            po.received_quantity as po_received_quantity,
            p.code as product_code,
            p.name as product_name,
            p.unit_of_measure,
            s.name as supplier_name,
            s.contact_person,
            s.phone,
            s.email,
            u.username as received_by_name
        FROM receiving_transactions rt
        JOIN purchase_orders po ON rt.po_id = po.id
        JOIN products p ON rt.product_id = p.id
        JOIN suppliers s ON po.supplier_id = s.id
        LEFT JOIN users u ON rt.received_by = u.id
        WHERE rt.receipt_number = ?
    ''', (receipt_number,)).fetchone()
    
    if not receipt:
        flash('Receipt not found.', 'danger')
        conn.close()
        return redirect(url_for('receiving_routes.list_receiving'))
    
    conn.close()
    return render_template('receiving/view.html', receipt=receipt)
