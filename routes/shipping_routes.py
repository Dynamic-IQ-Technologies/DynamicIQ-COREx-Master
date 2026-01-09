from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify, send_file
from models import Database
from auth import login_required, role_required
from datetime import datetime
from utils.shipping_documents import ShippingDocumentGenerator
import os

shipping_bp = Blueprint('shipping_routes', __name__)

@shipping_bp.route('/shipments')
@login_required
def list_shipments():
    """List all shipments"""
    db = Database()
    conn = db.get_connection()
    
    status_filter = request.args.get('status', 'all')
    
    # Parameterized query to prevent SQL injection
    if status_filter == 'all':
        shipments = conn.execute('''
            SELECT 
                s.*,
                u.username as created_by_name,
                u2.username as shipped_by_name,
                COUNT(sl.id) as line_count,
                so.so_number,
                wo.wo_number
            FROM shipments s
            LEFT JOIN users u ON s.created_by = u.id
            LEFT JOIN users u2 ON s.shipped_by = u2.id
            LEFT JOIN shipment_lines sl ON s.id = sl.shipment_id
            LEFT JOIN sales_orders so ON s.reference_type = 'Sales Order' AND s.reference_id = so.id
            LEFT JOIN work_orders wo ON s.reference_type = 'Work Order' AND s.reference_id = wo.id
            GROUP BY s.id
            ORDER BY s.created_at DESC
        ''').fetchall()
    else:
        shipments = conn.execute('''
            SELECT 
                s.*,
                u.username as created_by_name,
                u2.username as shipped_by_name,
                COUNT(sl.id) as line_count,
                so.so_number,
                wo.wo_number
            FROM shipments s
            LEFT JOIN users u ON s.created_by = u.id
            LEFT JOIN users u2 ON s.shipped_by = u2.id
            LEFT JOIN shipment_lines sl ON s.id = sl.shipment_id
            LEFT JOIN sales_orders so ON s.reference_type = 'Sales Order' AND s.reference_id = so.id
            LEFT JOIN work_orders wo ON s.reference_type = 'Work Order' AND s.reference_id = wo.id
            WHERE s.status = ?
            GROUP BY s.id
            ORDER BY s.created_at DESC
        ''', (status_filter,)).fetchall()
    
    # Get status counts
    status_counts = conn.execute('''
        SELECT status, COUNT(*) as count
        FROM shipments
        GROUP BY status
    ''').fetchall()
    
    counts = {row['status']: row['count'] for row in status_counts}
    counts['all'] = sum(counts.values())
    
    conn.close()
    return render_template('shipping/list.html', 
                         shipments=shipments, 
                         status_filter=status_filter,
                         status_counts=counts)

@shipping_bp.route('/shipments/create', methods=['GET', 'POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def create_shipment():
    """Create a new shipment"""
    if request.method == 'POST':
        db = Database()
        conn = db.get_connection()
        
        try:
            # Get form data
            shipment_type = request.form.get('shipment_type')  # Sales Order / Work Order
            reference_type = request.form.get('reference_type')
            reference_id = int(request.form.get('reference_id') or 0)
            carrier = request.form.get('carrier', '').strip()
            tracking_number = request.form.get('tracking_number', '').strip()
            shipping_method = request.form.get('shipping_method', '').strip()
            ship_date = request.form.get('ship_date')
            expected_delivery = request.form.get('expected_delivery_date')
            ship_from = request.form.get('ship_from_location', '').strip()
            
            # Shipping address
            ship_to_name = request.form.get('ship_to_name', '').strip()
            ship_to_address = request.form.get('ship_to_address', '').strip()
            ship_to_city = request.form.get('ship_to_city', '').strip()
            ship_to_state = request.form.get('ship_to_state', '').strip()
            ship_to_postal = request.form.get('ship_to_postal_code', '').strip()
            ship_to_country = request.form.get('ship_to_country', 'USA').strip()
            
            # Package details
            weight = float(request.form.get('weight', 0) or 0)
            weight_unit = request.form.get('weight_unit', 'lbs')
            dimensions = request.form.get('dimensions', '').strip()
            freight_cost = float(request.form.get('freight_cost', 0) or 0)
            insurance_value = float(request.form.get('insurance_value', 0) or 0)
            special_instructions = request.form.get('special_instructions', '').strip()
            
            # Generate shipment number
            count = conn.execute('SELECT COUNT(*) as count FROM shipments').fetchone()['count']
            shipment_number = f"SHIP-{count + 1:07d}"
            
            # Insert shipment
            cursor = conn.execute('''
                INSERT INTO shipments (
                    shipment_number, shipment_type, reference_type, reference_id,
                    status, carrier, tracking_number, shipping_method,
                    ship_date, expected_delivery_date,
                    ship_from_location, ship_to_name, ship_to_address,
                    ship_to_city, ship_to_state, ship_to_postal_code, ship_to_country,
                    weight, weight_unit, dimensions, freight_cost, insurance_value,
                    special_instructions, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                shipment_number, shipment_type, reference_type, reference_id,
                'Pending', carrier, tracking_number, shipping_method,
                ship_date, expected_delivery,
                ship_from, ship_to_name, ship_to_address,
                ship_to_city, ship_to_state, ship_to_postal, ship_to_country,
                weight, weight_unit, dimensions, freight_cost, insurance_value,
                special_instructions, session['user_id']
            ))
            
            shipment_id = cursor.lastrowid
            
            # Auto-populate line items from source record
            line_number = 0
            
            if reference_type == 'Sales Order' and reference_id:
                so_lines = conn.execute('''
                    SELECT sol.*, p.code as product_code, p.name as product_name
                    FROM sales_order_lines sol
                    JOIN products p ON sol.product_id = p.id
                    WHERE sol.so_id = ?
                    ORDER BY sol.id
                ''', (reference_id,)).fetchall()
                
                for sol in so_lines:
                    line_number += 1
                    conn.execute('''
                        INSERT INTO shipment_lines (
                            shipment_id, line_number, product_id, quantity_shipped,
                            serial_number, lot_number, condition, notes
                        ) VALUES (?, ?, ?, ?, ?, ?, 'New', ?)
                    ''', (
                        shipment_id, line_number, sol['product_id'], sol['quantity'],
                        sol.get('serial_number', '') or '', sol.get('lot_number', '') or '',
                        f"From SO Line: {sol['product_code']} - {sol['product_name']}"
                    ))
            
            elif reference_type == 'Work Order' and reference_id:
                wo = conn.execute('''
                    SELECT wo.*, p.id as prod_id, p.code as product_code, p.name as product_name
                    FROM work_orders wo
                    JOIN products p ON wo.product_id = p.id
                    WHERE wo.id = ?
                ''', (reference_id,)).fetchone()
                
                if wo:
                    conn.execute('''
                        INSERT INTO shipment_lines (
                            shipment_id, line_number, product_id, quantity_shipped,
                            serial_number, lot_number, condition, notes
                        ) VALUES (?, 1, ?, ?, ?, ?, 'New', ?)
                    ''', (
                        shipment_id, wo['prod_id'], wo['quantity'] or 1,
                        wo.get('serial_number', '') or '', wo.get('lot_number', '') or '',
                        f"From WO {wo['wo_number']}: {wo['product_code']} - {wo['product_name']}"
                    ))
            
            conn.commit()
            conn.close()
            
            flash(f'Shipment {shipment_number} created successfully!', 'success')
            return redirect(url_for('shipping_routes.edit_shipment', id=shipment_id))
            
        except Exception as e:
            conn.close()
            flash(f'Error creating shipment: {str(e)}', 'danger')
            return redirect(url_for('shipping_routes.list_shipments'))
    
    # GET request - show form
    db = Database()
    conn = db.get_connection()
    
    # Get pending sales orders
    sales_orders = conn.execute('''
        SELECT so.id, so.so_number, so.order_date, c.name as customer_name
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        WHERE so.status IN ('Confirmed', 'Pending')
        ORDER BY so.created_at DESC
    ''').fetchall()
    
    # Get work orders ready to ship
    work_orders = conn.execute('''
        SELECT wo.id, wo.wo_number, wo.planned_end_date, p.name as product_name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.status = 'Completed'
        ORDER BY wo.created_at DESC
    ''').fetchall()
    
    conn.close()
    
    return render_template('shipping/create.html',
                         sales_orders=sales_orders,
                         work_orders=work_orders)

@shipping_bp.route('/shipments/<int:id>')
@login_required
def view_shipment(id):
    """View shipment details"""
    db = Database()
    conn = db.get_connection()
    
    shipment = conn.execute('''
        SELECT s.*, 
               u.username as created_by_name,
               u2.username as shipped_by_name,
               u3.username as packed_by_name
        FROM shipments s
        LEFT JOIN users u ON s.created_by = u.id
        LEFT JOIN users u2 ON s.shipped_by = u2.id
        LEFT JOIN users u3 ON s.packed_by = u3.id
        WHERE s.id = ?
    ''', (id,)).fetchone()
    
    if not shipment:
        flash('Shipment not found', 'danger')
        conn.close()
        return redirect(url_for('shipping_routes.list_shipments'))
    
    # Get line items
    lines = conn.execute('''
        SELECT sl.*, p.code, p.name as product_name, p.unit_of_measure
        FROM shipment_lines sl
        JOIN products p ON sl.product_id = p.id
        WHERE sl.shipment_id = ?
        ORDER BY sl.line_number
    ''', (id,)).fetchall()
    
    conn.close()
    
    return render_template('shipping/view.html', 
                         shipment=shipment, 
                         lines=lines)

@shipping_bp.route('/shipments/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def edit_shipment(id):
    """Edit shipment"""
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            # Update shipment
            conn.execute('''
                UPDATE shipments SET
                    carrier = ?, tracking_number = ?, shipping_method = ?,
                    ship_date = ?, expected_delivery_date = ?,
                    ship_from_location = ?, ship_to_name = ?, ship_to_address = ?,
                    ship_to_city = ?, ship_to_state = ?, ship_to_postal_code = ?,
                    ship_to_country = ?, weight = ?, weight_unit = ?,
                    dimensions = ?, freight_cost = ?, insurance_value = ?,
                    special_instructions = ?
                WHERE id = ?
            ''', (
                request.form.get('carrier', '').strip(),
                request.form.get('tracking_number', '').strip(),
                request.form.get('shipping_method', '').strip(),
                request.form.get('ship_date'),
                request.form.get('expected_delivery_date'),
                request.form.get('ship_from_location', '').strip(),
                request.form.get('ship_to_name', '').strip(),
                request.form.get('ship_to_address', '').strip(),
                request.form.get('ship_to_city', '').strip(),
                request.form.get('ship_to_state', '').strip(),
                request.form.get('ship_to_postal_code', '').strip(),
                request.form.get('ship_to_country', 'USA').strip(),
                float(request.form.get('weight', 0) or 0),
                request.form.get('weight_unit', 'lbs'),
                request.form.get('dimensions', '').strip(),
                float(request.form.get('freight_cost', 0) or 0),
                float(request.form.get('insurance_value', 0) or 0),
                request.form.get('special_instructions', '').strip(),
                id
            ))
            
            conn.commit()
            flash('Shipment updated successfully!', 'success')
            return redirect(url_for('shipping_routes.view_shipment', id=id))
            
        except Exception as e:
            flash(f'Error updating shipment: {str(e)}', 'danger')
        finally:
            conn.close()
            
        return redirect(url_for('shipping_routes.edit_shipment', id=id))
    
    # GET - show form
    shipment = conn.execute('SELECT * FROM shipments WHERE id = ?', (id,)).fetchone()
    
    if not shipment:
        flash('Shipment not found', 'danger')
        conn.close()
        return redirect(url_for('shipping_routes.list_shipments'))
    
    # Get line items
    lines = conn.execute('''
        SELECT sl.*, p.code, p.name as product_name, p.unit_of_measure
        FROM shipment_lines sl
        JOIN products p ON sl.product_id = p.id
        WHERE sl.shipment_id = ?
        ORDER BY sl.line_number
    ''', (id,)).fetchall()
    
    # Get available products for adding lines
    products = conn.execute('''
        SELECT id, code, name, unit_of_measure
        FROM products
        WHERE product_type IN ('Component', 'Finished Good')
        ORDER BY code
    ''').fetchall()
    
    conn.close()
    
    return render_template('shipping/edit.html',
                         shipment=shipment,
                         lines=lines,
                         products=products)

@shipping_bp.route('/shipments/<int:id>/add-line', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def add_shipment_line(id):
    """Add line item to shipment"""
    db = Database()
    conn = db.get_connection()
    
    try:
        product_id = int(request.form['product_id'])
        quantity = float(request.form['quantity_shipped'])
        serial_number = request.form.get('serial_number', '').strip()
        lot_number = request.form.get('lot_number', '').strip()
        condition = request.form.get('condition', 'New')
        package_number = request.form.get('package_number', '').strip()
        notes = request.form.get('notes', '').strip()
        
        # Get next line number
        max_line = conn.execute('''
            SELECT COALESCE(MAX(line_number), 0) as max_line
            FROM shipment_lines
            WHERE shipment_id = ?
        ''', (id,)).fetchone()['max_line']
        
        line_number = max_line + 1
        
        # Insert line
        conn.execute('''
            INSERT INTO shipment_lines (
                shipment_id, line_number, product_id, quantity_shipped,
                serial_number, lot_number, condition, package_number, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (id, line_number, product_id, quantity, serial_number,
              lot_number, condition, package_number, notes))
        
        conn.commit()
        flash('Line item added successfully!', 'success')
        
    except Exception as e:
        flash(f'Error adding line: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('shipping_routes.edit_shipment', id=id))

@shipping_bp.route('/shipments/<int:shipment_id>/delete-line/<int:line_id>', methods=['POST'])
@role_required('Admin', 'Planner')
def delete_shipment_line(shipment_id, line_id):
    """Delete shipment line"""
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('DELETE FROM shipment_lines WHERE id = ?', (line_id,))
        conn.commit()
        flash('Line item deleted successfully!', 'success')
    except Exception as e:
        flash(f'Error deleting line: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('shipping_routes.edit_shipment', id=shipment_id))

@shipping_bp.route('/shipments/<int:id>/ship', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def ship_shipment(id):
    """Mark shipment as shipped and update inventory"""
    db = Database()
    conn = db.get_connection()
    
    try:
        # Get shipment
        shipment = conn.execute('SELECT * FROM shipments WHERE id = ?', (id,)).fetchone()
        
        if shipment['status'] == 'Shipped':
            flash('Shipment already marked as shipped', 'warning')
            conn.close()
            return redirect(url_for('shipping_routes.view_shipment', id=id))
        
        # Get lines
        lines = conn.execute('''
            SELECT * FROM shipment_lines WHERE shipment_id = ?
        ''', (id,)).fetchall()
        
        if not lines:
            flash('Cannot ship: No line items added', 'danger')
            conn.close()
            return redirect(url_for('shipping_routes.edit_shipment', id=id))
        
        # Deduct inventory for each line
        for line in lines:
            conn.execute('''
                UPDATE inventory
                SET quantity = quantity - ?
                WHERE product_id = ?
            ''', (line['quantity_shipped'], line['product_id']))
        
        # Update shipment status
        conn.execute('''
            UPDATE shipments
            SET status = 'Shipped',
                ship_date = ?,
                shipped_by = ?
            WHERE id = ?
        ''', (datetime.now().strftime('%Y-%m-%d'), session['user_id'], id))
        
        # Update reference order status if needed
        if shipment['reference_type'] == 'SalesOrder':
            conn.execute('''
                UPDATE sales_orders
                SET status = 'Shipped',
                    actual_ship_date = ?,
                    tracking_number = ?
                WHERE id = ?
            ''', (datetime.now().strftime('%Y-%m-%d'), 
                  shipment['tracking_number'], 
                  shipment['reference_id']))
        
        conn.commit()
        flash('Shipment marked as shipped successfully!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error shipping: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('shipping_routes.view_shipment', id=id))

@shipping_bp.route('/shipments/<int:id>/deliver', methods=['POST'])
@role_required('Admin', 'Planner')
def deliver_shipment(id):
    """Mark shipment as delivered"""
    db = Database()
    conn = db.get_connection()
    
    try:
        actual_delivery = request.form.get('actual_delivery_date', 
                                          datetime.now().strftime('%Y-%m-%d'))
        
        conn.execute('''
            UPDATE shipments
            SET status = 'Delivered',
                actual_delivery_date = ?
            WHERE id = ?
        ''', (actual_delivery, id))
        
        conn.commit()
        flash('Shipment marked as delivered!', 'success')
        
    except Exception as e:
        flash(f'Error: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('shipping_routes.view_shipment', id=id))

@shipping_bp.route('/shipments/<int:id>/cancel', methods=['POST'])
@role_required('Admin', 'Planner')
def cancel_shipment(id):
    """Cancel a pending shipment"""
    from models import AuditLogger
    db = Database()
    conn = db.get_connection()
    
    try:
        shipment = conn.execute('SELECT * FROM shipments WHERE id = ?', (id,)).fetchone()
        
        if not shipment:
            flash('Shipment not found.', 'danger')
            conn.close()
            return redirect(url_for('shipping_routes.list_shipments'))
        
        if shipment['status'] != 'Pending':
            flash('Only pending shipments can be cancelled.', 'warning')
            conn.close()
            return redirect(url_for('shipping_routes.view_shipment', id=id))
        
        # Delete shipment lines first
        conn.execute('DELETE FROM shipment_lines WHERE shipment_id = ?', (id,))
        
        # Delete the shipment
        conn.execute('DELETE FROM shipments WHERE id = ?', (id,))
        
        # Log audit trail
        AuditLogger.log_change(
            conn=conn,
            record_type='shipments',
            record_id=id,
            action_type='Deleted',
            modified_by=session.get('user_id'),
            changed_fields={'shipment_number': shipment['shipment_number'], 'status': 'Cancelled/Deleted'},
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        flash(f'Shipment {shipment["shipment_number"]} has been cancelled and deleted.', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error cancelling shipment: {str(e)}', 'danger')
        conn.close()
        return redirect(url_for('shipping_routes.view_shipment', id=id))
    finally:
        conn.close()
    
    return redirect(url_for('shipping_routes.list_shipments'))


@shipping_bp.route('/shipments/dashboard')
@login_required
def dashboard():
    """Shipping & Receiving Dashboard"""
    db = Database()
    conn = db.get_connection()
    
    # Released lines ready to ship (from sales orders)
    ready_to_ship = conn.execute('''
        SELECT 
            sol.id as line_id,
            sol.line_number,
            sol.quantity,
            sol.uom as unit_of_measure,
            sol.serial_number,
            sol.released_to_shipping_at,
            sol.allocation_status,
            p.code as product_code,
            p.name as product_name,
            so.id as so_id,
            so.so_number,
            so.order_type,
            c.name as customer_name,
            c.customer_number,
            u.username as released_by_name
        FROM sales_order_lines sol
        JOIN sales_orders so ON sol.so_id = so.id
        JOIN products p ON sol.product_id = p.id
        LEFT JOIN customers c ON so.customer_id = c.id
        LEFT JOIN users u ON sol.released_by = u.id
        WHERE sol.released_to_shipping_at IS NOT NULL
            AND sol.shipped_at IS NULL
        ORDER BY sol.released_to_shipping_at DESC
        LIMIT 20
    ''').fetchall()
    
    # Pending shipments
    pending_shipments = conn.execute('''
        SELECT s.*, COUNT(sl.id) as item_count,
            so.so_number, wo.wo_number
        FROM shipments s
        LEFT JOIN shipment_lines sl ON s.id = sl.shipment_id
        LEFT JOIN sales_orders so ON s.reference_type = 'Sales Order' AND s.reference_id = so.id
        LEFT JOIN work_orders wo ON s.reference_type = 'Work Order' AND s.reference_id = wo.id
        WHERE s.status = 'Pending'
        GROUP BY s.id
        ORDER BY s.created_at DESC
        LIMIT 10
    ''').fetchall()
    
    # In-transit shipments
    intransit_shipments = conn.execute('''
        SELECT s.*, COUNT(sl.id) as item_count,
            so.so_number, wo.wo_number
        FROM shipments s
        LEFT JOIN shipment_lines sl ON s.id = sl.shipment_id
        LEFT JOIN sales_orders so ON s.reference_type = 'Sales Order' AND s.reference_id = so.id
        LEFT JOIN work_orders wo ON s.reference_type = 'Work Order' AND s.reference_id = wo.id
        WHERE s.status = 'Shipped'
        GROUP BY s.id
        ORDER BY s.ship_date DESC
        LIMIT 10
    ''').fetchall()
    
    # Recent receipts
    recent_receipts = conn.execute('''
        SELECT rt.*, p.code, p.name as product_name, po.po_number
        FROM receiving_transactions rt
        JOIN products p ON rt.product_id = p.id
        JOIN purchase_orders po ON rt.po_id = po.id
        ORDER BY rt.receipt_date DESC
        LIMIT 10
    ''').fetchall()
    
    # Stats - add ready to ship count
    ready_to_ship_count = conn.execute('''
        SELECT COUNT(*) as count FROM sales_order_lines 
        WHERE released_to_shipping_at IS NOT NULL AND shipped_at IS NULL
    ''').fetchone()['count']
    
    stats = {
        'ready_to_ship': ready_to_ship_count,
        'pending_shipments': conn.execute("SELECT COUNT(*) as count FROM shipments WHERE status = 'Pending'").fetchone()['count'],
        'intransit': conn.execute("SELECT COUNT(*) as count FROM shipments WHERE status = 'Shipped'").fetchone()['count'],
        'delivered_today': conn.execute("SELECT COUNT(*) as count FROM shipments WHERE status = 'Delivered' AND DATE(actual_delivery_date) = DATE('now')").fetchone()['count'],
        'receipts_today': conn.execute("SELECT COUNT(*) as count FROM receiving_transactions WHERE DATE(receipt_date) = DATE('now')").fetchone()['count'],
    }
    
    conn.close()
    
    return render_template('shipping/dashboard.html',
                         ready_to_ship=ready_to_ship,
                         pending_shipments=pending_shipments,
                         intransit_shipments=intransit_shipments,
                         recent_receipts=recent_receipts,
                         stats=stats)

@shipping_bp.route('/pending-shipments')
@login_required
def list_pending_shipments():
    """List all pending shipments awaiting confirmation"""
    db = Database()
    conn = db.get_connection()
    
    stage_filter = request.args.get('stage', 'Pending')
    
    # Get pending shipments with related data
    query = '''
        SELECT 
            s.*,
            u.username as released_by_name,
            u2.username as confirmed_by_name,
            so.so_number,
            wo.wo_number,
            c.name as customer_name,
            c.customer_number,
            COUNT(sol.id) as item_count
        FROM shipments s
        LEFT JOIN users u ON s.released_by = u.id
        LEFT JOIN users u2 ON s.confirmed_by = u2.id
        LEFT JOIN sales_orders so ON s.reference_type = 'Sales Order' AND s.reference_id = so.id
        LEFT JOIN work_orders wo ON s.reference_type = 'Work Order' AND s.reference_id = wo.id
        LEFT JOIN customers c ON (so.customer_id = c.id OR wo.customer_id = c.id)
        LEFT JOIN sales_order_lines sol ON so.id = sol.so_id
        WHERE s.shipment_stage = ?
        GROUP BY s.id
        ORDER BY s.released_at DESC
    '''
    
    shipments = conn.execute(query, (stage_filter,)).fetchall()
    
    # Get stage counts
    stage_counts = conn.execute('''
        SELECT shipment_stage, COUNT(*) as count
        FROM shipments
        WHERE shipment_stage IS NOT NULL
        GROUP BY shipment_stage
    ''').fetchall()
    
    counts = {row['shipment_stage']: row['count'] for row in stage_counts}
    counts['Pending'] = counts.get('Pending', 0)
    counts['Confirmed'] = counts.get('Confirmed', 0)
    
    conn.close()
    return render_template('shipping/pending_shipments.html', 
                         shipments=shipments, 
                         stage_filter=stage_filter,
                         stage_counts=counts)

@shipping_bp.route('/pending-shipments/<int:id>')
@login_required
def view_pending_shipment(id):
    """View pending shipment details"""
    db = Database()
    conn = db.get_connection()
    
    shipment = conn.execute('''
        SELECT 
            s.*,
            u.username as released_by_name,
            u2.username as confirmed_by_name,
            so.so_number, so.customer_id, so.status as so_status,
            wo.wo_number, wo.status as wo_status,
            c.name as customer_name,
            c.customer_number,
            c.email, c.phone
        FROM shipments s
        LEFT JOIN users u ON s.released_by = u.id
        LEFT JOIN users u2 ON s.confirmed_by = u2.id
        LEFT JOIN sales_orders so ON s.reference_type = 'Sales Order' AND s.reference_id = so.id
        LEFT JOIN work_orders wo ON s.reference_type = 'Work Order' AND s.reference_id = wo.id
        LEFT JOIN customers c ON (so.customer_id = c.id OR wo.customer_id = c.id)
        WHERE s.id = ?
    ''', (id,)).fetchone()
    
    if not shipment:
        flash('Pending shipment not found', 'danger')
        conn.close()
        return redirect(url_for('shipping_routes.list_pending_shipments'))
    
    if shipment['reference_type'] == 'Sales Order':
        lines = conn.execute('''
            SELECT sol.*, p.code, p.name
            FROM sales_order_lines sol
            JOIN products p ON sol.product_id = p.id
            WHERE sol.so_id = ?
        ''', (shipment['reference_id'],)).fetchall()
    elif shipment['reference_type'] == 'Work Order':
        lines = conn.execute('''
            SELECT p.id as product_id, p.code, p.name, 1 as quantity, 0 as unit_price
            FROM work_orders wo
            JOIN products p ON wo.product_id = p.id
            WHERE wo.id = ?
        ''', (shipment['reference_id'],)).fetchall()
    else:
        lines = []
    
    documents = conn.execute('''
        SELECT * FROM shipment_documents 
        WHERE shipment_id = ? 
        ORDER BY created_at DESC
    ''', (id,)).fetchall() if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='shipment_documents'").fetchone() else []
    
    conn.close()
    return render_template('shipping/view_pending.html', 
                         shipment=shipment, 
                         lines=lines,
                         documents=documents)

@shipping_bp.route('/pending-shipments/<int:id>/confirm', methods=['POST'])
@role_required('Admin', 'Production Staff', 'Planner')
def confirm_shipment(id):
    """Confirm and finalize a pending shipment"""
    db = Database()
    conn = db.get_connection()
    
    try:
        # Get shipment details
        shipment = conn.execute('''
            SELECT s.*, so.id as so_id, so.status as so_status
            FROM shipments s
            LEFT JOIN sales_orders so ON s.reference_type = 'Sales Order' AND s.reference_id = so.id
            WHERE s.id = ?
        ''', (id,)).fetchone()
        
        if not shipment:
            flash('Shipment not found', 'danger')
            conn.close()
            return redirect(url_for('shipping_routes.list_pending_shipments'))
        
        if shipment['shipment_stage'] != 'Pending':
            flash('This shipment has already been processed.', 'warning')
            conn.close()
            return redirect(url_for('shipping_routes.view_pending_shipment', id=id))
        
        # Get form data
        carrier = request.form.get('carrier', '').strip()
        tracking_number = request.form.get('tracking_number', '').strip()
        shipping_method = request.form.get('shipping_method', '').strip()
        
        # For Sales Orders, deduct inventory
        if shipment['reference_type'] == 'Sales Order' and shipment['so_id']:
            lines = conn.execute('''
                SELECT * FROM sales_order_lines WHERE so_id = ?
            ''', (shipment['so_id'],)).fetchall()
            
            # Check inventory availability
            for line in lines:
                if not line['is_core']:
                    inventory = conn.execute('''
                        SELECT quantity FROM inventory WHERE product_id = ?
                    ''', (line['product_id'],)).fetchone()
                    
                    available = inventory['quantity'] if inventory else 0
                    if available < line['quantity']:
                        product = conn.execute(
                            'SELECT code FROM products WHERE id = ?', (line['product_id'],)
                        ).fetchone()
                        flash(f'Insufficient inventory for product {product["code"]}. Available: {available}, Required: {line["quantity"]}', 'danger')
                        conn.close()
                        return redirect(url_for('shipping_routes.view_pending_shipment', id=id))
            
            # Deduct inventory
            for line in lines:
                if not line['is_core']:
                    conn.execute('''
                        UPDATE inventory 
                        SET quantity = quantity - ?
                        WHERE product_id = ?
                    ''', (line['quantity'], line['product_id']))
            
            # Update Sales Order status to Shipped
            conn.execute('''
                UPDATE sales_orders 
                SET status = 'Shipped', 
                    actual_ship_date = CURRENT_DATE,
                    tracking_number = ?,
                    shipping_method = ?
                WHERE id = ?
            ''', (tracking_number, shipping_method, shipment['so_id']))
        
        # Update shipment record
        conn.execute('''
            UPDATE shipments 
            SET shipment_stage = 'Confirmed',
                status = 'Shipped',
                ship_date = CURRENT_DATE,
                carrier = ?,
                tracking_number = ?,
                shipping_method = ?,
                confirmed_by = ?,
                confirmed_at = CURRENT_TIMESTAMP,
                shipped_by = ?
            WHERE id = ?
        ''', (carrier, tracking_number, shipping_method, 
              session.get('user_id'), session.get('user_id'), id))
        
        # Log activity
        from models import AuditTrail
        AuditTrail.log_change(
            conn=conn,
            record_type='shipments',
            record_id=id,
            action_type='UPDATE',
            modified_by=session.get('user_id'),
            changed_fields={'shipment_stage': 'Confirmed', 'status': 'Shipped'}
        )
        
        conn.commit()
        flash(f'Shipment {shipment["shipment_number"]} confirmed successfully! Inventory deducted and order updated to Shipped status.', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'An error occurred while confirming shipment: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('shipping_routes.list_pending_shipments'))


@shipping_bp.route('/shipments/<int:id>/documents')
@login_required
def get_shipment_documents(id):
    """Get all documents for a shipment"""
    db = Database()
    conn = db.get_connection()
    
    documents = conn.execute('''
        SELECT sd.*, u.username as generated_by_name, u2.username as finalized_by_name
        FROM shipment_documents sd
        LEFT JOIN users u ON sd.generated_by = u.id
        LEFT JOIN users u2 ON sd.finalized_by = u2.id
        WHERE sd.shipment_id = ?
        ORDER BY sd.document_type, sd.version DESC
    ''', (id,)).fetchall()
    
    conn.close()
    return jsonify({'documents': [dict(d) for d in documents]})


@shipping_bp.route('/shipments/<int:id>/generate-packing-slip', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def generate_packing_slip(id):
    """Generate a packing slip for the shipment"""
    db = Database()
    conn = db.get_connection()
    
    try:
        shipment = conn.execute('SELECT * FROM shipments WHERE id = ?', (id,)).fetchone()
        if not shipment:
            flash('Shipment not found.', 'danger')
            return redirect(url_for('shipping_routes.list_shipments'))
        
        lines = conn.execute('''
            SELECT sl.*, p.code, p.name as product_name, p.unit_of_measure
            FROM shipment_lines sl
            JOIN products p ON sl.product_id = p.id
            WHERE sl.shipment_id = ?
            ORDER BY sl.line_number
        ''', (id,)).fetchall()
        
        sales_order = None
        if shipment['reference_type'] == 'Sales Order' and shipment['reference_id']:
            sales_order = conn.execute('SELECT * FROM sales_orders WHERE id = ?', 
                                       (shipment['reference_id'],)).fetchone()
            if not lines:
                lines = conn.execute('''
                    SELECT sol.id, sol.product_id, sol.quantity, sol.unit_price, 
                           p.code, p.name as product_name, p.unit_of_measure,
                           sol.serial_number
                    FROM sales_order_lines sol
                    JOIN products p ON sol.product_id = p.id
                    WHERE sol.so_id = ?
                ''', (shipment['reference_id'],)).fetchall()
        elif shipment['reference_type'] == 'Work Order' and shipment['reference_id']:
            if not lines:
                lines = conn.execute('''
                    SELECT wo.product_id, 1 as quantity, 0 as unit_price,
                           p.code, p.name as product_name, p.unit_of_measure
                    FROM work_orders wo
                    JOIN products p ON wo.product_id = p.id
                    WHERE wo.id = ?
                ''', (shipment['reference_id'],)).fetchall()
        
        if not lines:
            flash('Cannot generate packing slip: No line items on this shipment.', 'warning')
            return redirect(url_for('shipping_routes.view_shipment', id=id))
        
        existing = conn.execute('''
            SELECT MAX(version) as max_ver FROM shipment_documents 
            WHERE shipment_id = ? AND document_type = 'Packing Slip'
        ''', (id,)).fetchone()
        
        new_version = (existing['max_ver'] or 0) + 1
        doc_number = f"PS-{shipment['shipment_number']}-V{new_version}"
        
        shipment_dict = dict(shipment)
        shipment_dict['document_number'] = doc_number
        
        generator = ShippingDocumentGenerator()
        pdf_buffer = generator.generate_packing_slip(
            shipment_dict, 
            [dict(l) for l in lines],
            dict(sales_order) if sales_order else None
        )
        
        file_path = f"static/documents/{doc_number}.pdf"
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'wb') as f:
            f.write(pdf_buffer.getvalue())
        
        cursor = conn.execute('''
            INSERT INTO shipment_documents (shipment_id, document_type, document_number, version, 
                                           status, file_path, generated_by, generated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (id, 'Packing Slip', doc_number, new_version, 'Draft', file_path, session.get('user_id')))
        doc_id = cursor.lastrowid
        
        from models import AuditTrail
        AuditTrail.log_change(
            conn=conn,
            record_type='shipment_documents',
            record_id=doc_id,
            action_type='CREATE',
            modified_by=session.get('user_id'),
            changed_fields={'document_type': 'Packing Slip', 'version': new_version}
        )
        
        conn.commit()
        conn.close()
        
        pdf_buffer.seek(0)
        return send_file(
            pdf_buffer,
            mimetype='application/pdf',
            as_attachment=False,
            download_name=f'{doc_number}.pdf'
        )
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error generating packing slip: {str(e)}', 'danger')
        return redirect(url_for('shipping_routes.view_shipment', id=id))


@shipping_bp.route('/shipments/<int:id>/generate-coc', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def generate_certificate_of_conformance(id):
    """Generate a Certificate of Conformance for the shipment"""
    db = Database()
    conn = db.get_connection()
    
    try:
        shipment = conn.execute('SELECT * FROM shipments WHERE id = ?', (id,)).fetchone()
        if not shipment:
            flash('Shipment not found.', 'danger')
            return redirect(url_for('shipping_routes.list_shipments'))
        
        lines = conn.execute('''
            SELECT sl.*, p.code, p.name as product_name, p.unit_of_measure
            FROM shipment_lines sl
            JOIN products p ON sl.product_id = p.id
            WHERE sl.shipment_id = ?
            ORDER BY sl.line_number
        ''', (id,)).fetchall()
        
        sales_order = None
        customer = None
        if shipment['reference_type'] == 'Sales Order' and shipment['reference_id']:
            sales_order = conn.execute('SELECT * FROM sales_orders WHERE id = ?', 
                                       (shipment['reference_id'],)).fetchone()
            if sales_order and sales_order['customer_id']:
                customer = conn.execute('SELECT * FROM customers WHERE id = ?',
                                        (sales_order['customer_id'],)).fetchone()
            if not lines:
                lines = conn.execute('''
                    SELECT sol.id, sol.product_id, sol.quantity, sol.unit_price, 
                           p.code, p.name as product_name, p.unit_of_measure,
                           sol.serial_number
                    FROM sales_order_lines sol
                    JOIN products p ON sol.product_id = p.id
                    WHERE sol.so_id = ?
                ''', (shipment['reference_id'],)).fetchall()
        elif shipment['reference_type'] == 'Work Order' and shipment['reference_id']:
            if not lines:
                lines = conn.execute('''
                    SELECT wo.product_id, 1 as quantity, 0 as unit_price,
                           p.code, p.name as product_name, p.unit_of_measure
                    FROM work_orders wo
                    JOIN products p ON wo.product_id = p.id
                    WHERE wo.id = ?
                ''', (shipment['reference_id'],)).fetchall()
        
        if not lines:
            flash('Cannot generate C of C: No line items on this shipment.', 'warning')
            return redirect(url_for('shipping_routes.view_shipment', id=id))
        
        existing = conn.execute('''
            SELECT MAX(version) as max_ver FROM shipment_documents 
            WHERE shipment_id = ? AND document_type = 'Certificate of Conformance'
        ''', (id,)).fetchone()
        
        new_version = (existing['max_ver'] or 0) + 1
        doc_number = f"COC-{shipment['shipment_number']}-V{new_version}"
        
        shipment_dict = dict(shipment)
        shipment_dict['document_number'] = doc_number
        
        signatory = request.form.get('signatory', 'Quality Assurance')
        
        generator = ShippingDocumentGenerator()
        pdf_buffer = generator.generate_certificate_of_conformance(
            shipment_dict, 
            [dict(l) for l in lines],
            dict(sales_order) if sales_order else None,
            dict(customer) if customer else None,
            signatory
        )
        
        file_path = f"static/documents/{doc_number}.pdf"
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'wb') as f:
            f.write(pdf_buffer.getvalue())
        
        cursor = conn.execute('''
            INSERT INTO shipment_documents (shipment_id, document_type, document_number, version, 
                                           status, file_path, signed_by, generated_by, generated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (id, 'Certificate of Conformance', doc_number, new_version, 'Unsigned', 
              file_path, signatory, session.get('user_id')))
        doc_id = cursor.lastrowid
        
        from models import AuditTrail
        AuditTrail.log_change(
            conn=conn,
            record_type='shipment_documents',
            record_id=doc_id,
            action_type='CREATE',
            modified_by=session.get('user_id'),
            changed_fields={'document_type': 'Certificate of Conformance', 'version': new_version}
        )
        
        conn.commit()
        conn.close()
        
        pdf_buffer.seek(0)
        return send_file(
            pdf_buffer,
            mimetype='application/pdf',
            as_attachment=False,
            download_name=f'{doc_number}.pdf'
        )
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error generating Certificate of Conformance: {str(e)}', 'danger')
        return redirect(url_for('shipping_routes.view_shipment', id=id))


@shipping_bp.route('/shipments/<int:id>/generate-commercial-invoice', methods=['POST'])
@role_required('Admin', 'Planner', 'Accountant')
def generate_commercial_invoice(id):
    """Generate a Commercial Invoice for the shipment"""
    db = Database()
    conn = db.get_connection()
    
    try:
        shipment = conn.execute('SELECT * FROM shipments WHERE id = ?', (id,)).fetchone()
        if not shipment:
            flash('Shipment not found.', 'danger')
            return redirect(url_for('shipping_routes.list_shipments'))
        
        lines = conn.execute('''
            SELECT sl.*, p.code, p.name as product_name, p.unit_of_measure, p.cost as unit_price,
                   p.hs_code, p.country_of_origin
            FROM shipment_lines sl
            JOIN products p ON sl.product_id = p.id
            WHERE sl.shipment_id = ?
            ORDER BY sl.line_number
        ''', (id,)).fetchall()
        
        sales_order = None
        customer = None
        if shipment['reference_type'] == 'Sales Order' and shipment['reference_id']:
            sales_order = conn.execute('SELECT * FROM sales_orders WHERE id = ?', 
                                       (shipment['reference_id'],)).fetchone()
            if sales_order and sales_order['customer_id']:
                customer = conn.execute('SELECT * FROM customers WHERE id = ?',
                                        (sales_order['customer_id'],)).fetchone()
            if not lines:
                lines = conn.execute('''
                    SELECT sol.id, sol.product_id, sol.quantity, sol.unit_price, 
                           p.code, p.name as product_name, p.unit_of_measure,
                           p.hs_code, p.country_of_origin
                    FROM sales_order_lines sol
                    JOIN products p ON sol.product_id = p.id
                    WHERE sol.so_id = ?
                ''', (shipment['reference_id'],)).fetchall()
        elif shipment['reference_type'] == 'Work Order' and shipment['reference_id']:
            if not lines:
                lines = conn.execute('''
                    SELECT wo.product_id, 1 as quantity, 0 as unit_price,
                           p.code, p.name as product_name, p.unit_of_measure,
                           p.hs_code, p.country_of_origin
                    FROM work_orders wo
                    JOIN products p ON wo.product_id = p.id
                    WHERE wo.id = ?
                ''', (shipment['reference_id'],)).fetchall()
        
        if not lines:
            flash('Cannot generate commercial invoice: No line items on this shipment.', 'warning')
            return redirect(url_for('shipping_routes.view_shipment', id=id))
        
        existing = conn.execute('''
            SELECT MAX(version) as max_ver FROM shipment_documents 
            WHERE shipment_id = ? AND document_type = 'Commercial Invoice'
        ''', (id,)).fetchone()
        
        new_version = (existing['max_ver'] or 0) + 1
        doc_number = f"CI-{shipment['shipment_number']}-V{new_version}"
        
        shipment_dict = dict(shipment)
        shipment_dict['document_number'] = doc_number
        
        generator = ShippingDocumentGenerator()
        pdf_buffer = generator.generate_commercial_invoice(
            shipment_dict, 
            [dict(l) for l in lines],
            dict(sales_order) if sales_order else None,
            dict(customer) if customer else None
        )
        
        file_path = f"static/documents/{doc_number}.pdf"
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'wb') as f:
            f.write(pdf_buffer.getvalue())
        
        cursor = conn.execute('''
            INSERT INTO shipment_documents (shipment_id, document_type, document_number, version, 
                                           status, file_path, generated_by, generated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (id, 'Commercial Invoice', doc_number, new_version, 'Draft', file_path, session.get('user_id')))
        doc_id = cursor.lastrowid
        
        from models import AuditTrail
        AuditTrail.log_change(
            conn=conn,
            record_type='shipment_documents',
            record_id=doc_id,
            action_type='CREATE',
            modified_by=session.get('user_id'),
            changed_fields={'document_type': 'Commercial Invoice', 'version': new_version}
        )
        
        conn.commit()
        conn.close()
        
        pdf_buffer.seek(0)
        return send_file(
            pdf_buffer,
            mimetype='application/pdf',
            as_attachment=False,
            download_name=f'{doc_number}.pdf'
        )
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error generating commercial invoice: {str(e)}', 'danger')
        return redirect(url_for('shipping_routes.view_shipment', id=id))


@shipping_bp.route('/shipments/<int:id>/finalize-document/<int:doc_id>', methods=['POST'])
@role_required('Admin', 'Planner')
def finalize_document(id, doc_id):
    """Finalize a document (lock from further edits)"""
    db = Database()
    conn = db.get_connection()
    
    try:
        shipment = conn.execute('SELECT * FROM shipments WHERE id = ?', (id,)).fetchone()
        if not shipment:
            return jsonify({'success': False, 'error': 'Shipment not found'}), 404
        
        document = conn.execute('SELECT * FROM shipment_documents WHERE id = ? AND shipment_id = ?', 
                                (doc_id, id)).fetchone()
        if not document:
            return jsonify({'success': False, 'error': 'Document not found'}), 404
        
        if document['status'] == 'Final':
            return jsonify({'success': False, 'error': 'Document is already finalized'}), 400
        
        conn.execute('''
            UPDATE shipment_documents 
            SET status = 'Final', finalized_by = ?, finalized_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (session.get('user_id'), doc_id))
        
        from models import AuditTrail
        AuditTrail.log_change(
            conn=conn,
            record_type='shipment_documents',
            record_id=doc_id,
            action_type='UPDATE',
            modified_by=session.get('user_id'),
            changed_fields={'status': 'Final'}
        )
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Document finalized successfully'})
        
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500


@shipping_bp.route('/shipments/<int:id>/sign-coc/<int:doc_id>', methods=['POST'])
@role_required('Admin', 'Planner')
def sign_certificate(id, doc_id):
    """Electronically sign a Certificate of Conformance"""
    db = Database()
    conn = db.get_connection()
    
    try:
        document = conn.execute('''
            SELECT * FROM shipment_documents 
            WHERE id = ? AND shipment_id = ? AND document_type = 'Certificate of Conformance'
        ''', (doc_id, id)).fetchone()
        
        if not document:
            return jsonify({'success': False, 'error': 'Document not found'}), 404
        
        signature = request.form.get('signature', '')
        if not signature:
            return jsonify({'success': False, 'error': 'Signature is required'}), 400
        
        conn.execute('''
            UPDATE shipment_documents 
            SET status = 'Signed', electronic_signature = ?, signed_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (signature, doc_id))
        
        from models import AuditTrail
        AuditTrail.log_change(
            conn=conn,
            record_type='shipment_documents',
            record_id=doc_id,
            action_type='UPDATE',
            modified_by=session.get('user_id'),
            changed_fields={'status': 'Signed', 'signed_by': signature}
        )
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Certificate signed successfully'})
        
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500


@shipping_bp.route('/shipments/<int:id>/documents/<int:doc_id>/download')
@login_required
def download_document(id, doc_id):
    """Download an existing document"""
    db = Database()
    conn = db.get_connection()
    
    try:
        document = conn.execute('''
            SELECT * FROM shipment_documents 
            WHERE id = ? AND shipment_id = ?
        ''', (doc_id, id)).fetchone()
        
        conn.close()
        
        if not document:
            flash('Document not found.', 'danger')
            return redirect(url_for('shipping_routes.view_shipment', id=id))
        
        if not document['file_path'] or not os.path.exists(document['file_path']):
            flash('Document file not available.', 'warning')
            return redirect(url_for('shipping_routes.view_shipment', id=id))
        
        return send_file(
            document['file_path'],
            mimetype='application/pdf',
            as_attachment=False,
            download_name=f"{document['document_number']}.pdf"
        )
        
    except Exception as e:
        flash(f'Error downloading document: {str(e)}', 'danger')
        return redirect(url_for('shipping_routes.view_shipment', id=id))
