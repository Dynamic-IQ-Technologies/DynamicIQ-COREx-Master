from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database
from auth import login_required, role_required
from datetime import datetime, timedelta
import json

salesorder_bp = Blueprint('salesorder_routes', __name__)

@salesorder_bp.route('/sales-orders')
@login_required
def list_sales_orders():
    db = Database()
    conn = db.get_connection()
    
    status_filter = request.args.get('status', '')
    type_filter = request.args.get('type', '')
    
    query = '''
        SELECT so.*, c.name as customer_name, c.customer_number,
               COUNT(sol.id) as line_count
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        LEFT JOIN sales_order_lines sol ON so.id = sol.so_id
        WHERE 1=1
    '''
    
    params = []
    if status_filter:
        query += ' AND so.status = ?'
        params.append(status_filter)
    if type_filter:
        query += ' AND so.sales_type = ?'
        params.append(type_filter)
    
    query += ' GROUP BY so.id ORDER BY so.order_date DESC, so.id DESC'
    
    sales_orders = conn.execute(query, params).fetchall()
    conn.close()
    
    return render_template('salesorders/list.html', sales_orders=sales_orders, 
                         status_filter=status_filter, type_filter=type_filter)

@salesorder_bp.route('/sales-orders/create', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def create_sales_order():
    if request.method == 'POST':
        db = Database()
        conn = db.get_connection()
        try:
            customer_id = int(request.form['customer_id'])
            sales_type = request.form['sales_type']
            
            # Generate SO number
            last_so = conn.execute(
                'SELECT so_number FROM sales_orders ORDER BY id DESC LIMIT 1'
            ).fetchone()
            
            if last_so:
                last_num = int(last_so['so_number'].split('-')[1])
                so_number = f'SO-{last_num + 1:06d}'
            else:
                so_number = 'SO-000001'
            
            # Parse dates
            order_date = request.form.get('order_date') or datetime.now().strftime('%Y-%m-%d')
            expected_ship_date = request.form.get('expected_ship_date') or None
            expected_return_date = request.form.get('expected_return_date') or None
            
            # Parse amounts
            core_charge_str = request.form.get('core_charge', '0').strip()
            core_charge = float(core_charge_str) if core_charge_str else 0.0
            
            repair_charge_str = request.form.get('repair_charge', '0').strip()
            repair_charge = float(repair_charge_str) if repair_charge_str else 0.0
            
            # Insert sales order
            cursor = conn.execute('''
                INSERT INTO sales_orders (
                    so_number, customer_id, sales_type, order_date, expected_ship_date,
                    status, core_charge, repair_charge, expected_return_date, 
                    service_notes, notes, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                so_number, customer_id, sales_type, order_date, expected_ship_date,
                'Draft', core_charge, repair_charge, expected_return_date,
                request.form.get('service_notes', ''),
                request.form.get('notes', ''),
                session.get('user_id')
            ))
            
            so_id = cursor.lastrowid
            conn.commit()
            
            flash(f'Sales Order created successfully! SO #: {so_number}', 'success')
            return redirect(url_for('salesorder_routes.edit_sales_order', id=so_id))
            
        except ValueError:
            conn.rollback()
            flash('Please enter valid numeric values.', 'danger')
        except Exception as e:
            conn.rollback()
            flash('An error occurred while creating the sales order. Please try again.', 'danger')
        finally:
            conn.close()
            
    # GET request - load customers
    db = Database()
    conn = db.get_connection()
    customers = conn.execute(
        'SELECT * FROM customers WHERE status = ? ORDER BY name', ('Active',)
    ).fetchall()
    conn.close()
    
    return render_template('salesorders/create.html', customers=customers)

@salesorder_bp.route('/sales-orders/<int:id>')
@login_required
def view_sales_order(id):
    db = Database()
    conn = db.get_connection()
    
    sales_order = conn.execute('''
        SELECT so.*, c.name as customer_name, c.customer_number, c.billing_address, c.shipping_address,
               u.username as created_by_name
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        LEFT JOIN users u ON so.created_by = u.id
        WHERE so.id = ?
    ''', (id,)).fetchone()
    
    if not sales_order:
        flash('Sales Order not found', 'danger')
        conn.close()
        return redirect(url_for('salesorder_routes.list_sales_orders'))
    
    # Get line items
    lines = conn.execute('''
        SELECT sol.*, p.code, p.name as product_name, p.unit_of_measure
        FROM sales_order_lines sol
        JOIN products p ON sol.product_id = p.id
        WHERE sol.so_id = ?
        ORDER BY sol.line_number
    ''', (id,)).fetchall()
    
    # Get payments
    payments = conn.execute('''
        SELECT * FROM payments
        WHERE reference_type = ? AND reference_id = ?
        ORDER BY payment_date DESC
    ''', ('SalesOrder', id)).fetchall()
    
    conn.close()
    
    return render_template('salesorders/view.html', 
                         sales_order=sales_order, lines=lines, payments=payments)

@salesorder_bp.route('/sales-orders/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def edit_sales_order(id):
    if request.method == 'POST':
        db = Database()
        conn = db.get_connection()
        try:
            # Update header
            expected_ship_date = request.form.get('expected_ship_date') or None
            expected_return_date = request.form.get('expected_return_date') or None
            
            core_charge_str = request.form.get('core_charge', '0').strip()
            core_charge = float(core_charge_str) if core_charge_str else 0.0
            
            repair_charge_str = request.form.get('repair_charge', '0').strip()
            repair_charge = float(repair_charge_str) if repair_charge_str else 0.0
            
            tax_rate_str = request.form.get('tax_rate', '0').strip()
            tax_rate = float(tax_rate_str) if tax_rate_str else 0.0
            
            conn.execute('''
                UPDATE sales_orders SET
                    expected_ship_date = ?, core_charge = ?, repair_charge = ?,
                    expected_return_date = ?, service_notes = ?, notes = ?, tax_rate = ?
                WHERE id = ?
            ''', (
                expected_ship_date, core_charge, repair_charge, expected_return_date,
                request.form.get('service_notes', ''), request.form.get('notes', ''), tax_rate, id
            ))
            
            # Recalculate totals with tax rate
            recalculate_totals(conn, id, tax_rate)
            
            conn.commit()
            flash('Sales Order updated successfully!', 'success')
            return redirect(url_for('salesorder_routes.view_sales_order', id=id))
            
        except ValueError:
            conn.rollback()
            flash('Please enter valid numeric values.', 'danger')
        except Exception as e:
            conn.rollback()
            flash('An error occurred while updating the sales order. Please try again.', 'danger')
        finally:
            conn.close()
        
        return redirect(url_for('salesorder_routes.edit_sales_order', id=id))
    
    # GET request
    db = Database()
    conn = db.get_connection()
    
    sales_order = conn.execute('''
        SELECT so.*, c.name as customer_name, c.customer_number
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        WHERE so.id = ?
    ''', (id,)).fetchone()
    
    if not sales_order:
        flash('Sales Order not found', 'danger')
        conn.close()
        return redirect(url_for('salesorder_routes.list_sales_orders'))
    
    # Get line items
    lines = conn.execute('''
        SELECT sol.*, p.code, p.name as product_name, p.unit_of_measure
        FROM sales_order_lines sol
        JOIN products p ON sol.product_id = p.id
        WHERE sol.so_id = ?
        ORDER BY sol.line_number
    ''', (id,)).fetchall()
    
    # Get products for adding lines
    products = conn.execute('''
        SELECT p.*, COALESCE(i.quantity, 0) as available_qty
        FROM products p
        LEFT JOIN inventory i ON p.id = i.product_id
        ORDER BY p.code
    ''').fetchall()
    
    conn.close()
    
    return render_template('salesorders/edit.html', 
                         sales_order=sales_order, lines=lines, products=products)

@salesorder_bp.route('/sales-orders/<int:id>/add-line', methods=['POST'])
@role_required('Admin', 'Planner')
def add_line(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        # Basic fields
        line_type = request.form.get('line_type', 'Outright')
        line_status = request.form.get('line_status', 'Draft')
        product_id = int(request.form['product_id'])
        
        quantity_str = request.form.get('quantity', '0').strip()
        quantity = float(quantity_str) if quantity_str else 0.0
        
        unit_price_str = request.form.get('unit_price', '0').strip()
        unit_price = float(unit_price_str) if unit_price_str else 0.0
        
        discount_str = request.form.get('discount_percent', '0').strip()
        discount_percent = float(discount_str) if discount_str else 0.0
        
        # Calculate line total
        line_total = (quantity * unit_price) * (1 - discount_percent / 100)
        
        # Exchange-specific fields
        core_charge_str = request.form.get('core_charge', '0').strip()
        core_charge = float(core_charge_str) if core_charge_str else 0.0
        core_due_days = request.form.get('core_due_days')
        expected_core_condition = request.form.get('expected_core_condition')
        core_disposition = request.form.get('core_disposition')
        stock_disposition = request.form.get('stock_disposition')
        
        # Managed Repair-specific fields
        quoted_tat = request.form.get('quoted_tat')
        repair_nte_str = request.form.get('repair_nte', '0').strip()
        repair_nte = float(repair_nte_str) if repair_nte_str else 0.0
        vendor_repair_source = request.form.get('vendor_repair_source')
        repair_status = request.form.get('repair_status', 'Pending')
        return_to_address = request.form.get('return_to_address')
        
        # Get next line number
        last_line = conn.execute(
            'SELECT MAX(line_number) as max_line FROM sales_order_lines WHERE so_id = ?', (id,)
        ).fetchone()
        line_number = (last_line['max_line'] or 0) + 1
        
        # Insert line with all new fields
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO sales_order_lines (
                so_id, line_number, product_id, description, quantity, unit_price,
                discount_percent, line_total, serial_number, line_notes,
                line_type, line_status,
                core_charge, core_due_days, expected_core_condition, core_disposition, stock_disposition,
                quoted_tat, repair_nte, vendor_repair_source, repair_status, return_to_address,
                created_by, modified_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            id, line_number, product_id, request.form.get('description', ''),
            quantity, unit_price, discount_percent, line_total,
            request.form.get('serial_number', ''), request.form.get('line_notes', ''),
            line_type, line_status,
            core_charge if line_type == 'Exchange' else None,
            int(core_due_days) if core_due_days else None,
            expected_core_condition if line_type == 'Exchange' else None,
            core_disposition if line_type == 'Exchange' else None,
            stock_disposition if line_type == 'Exchange' else None,
            int(quoted_tat) if quoted_tat else None,
            repair_nte if line_type == 'Managed Repair' else None,
            vendor_repair_source if line_type == 'Managed Repair' else None,
            repair_status if line_type == 'Managed Repair' else None,
            return_to_address if line_type == 'Managed Repair' else None,
            session['user_id'], session['user_id']
        ))
        
        line_id = cursor.lastrowid
        
        # For Exchange transactions, create core_due_tracking record
        if line_type == 'Exchange' and core_charge > 0:
            from datetime import datetime, timedelta
            
            # Calculate core due date
            core_due_date = None
            if core_due_days:
                core_due_date = (datetime.now() + timedelta(days=int(core_due_days))).strftime('%Y-%m-%d')
            
            conn.execute('''
                INSERT INTO core_due_tracking (
                    so_line_id, so_id, product_id, core_charge, expected_condition,
                    core_due_date, core_disposition, stock_disposition
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                line_id, id, product_id, core_charge, expected_core_condition,
                core_due_date, core_disposition, stock_disposition
            ))
        
        # Recalculate totals
        recalculate_totals(conn, id)
        
        conn.commit()
        flash(f'{line_type} line item added successfully!', 'success')
        
    except ValueError as e:
        conn.rollback()
        flash('Please enter valid numeric values.', 'danger')
    except Exception as e:
        conn.rollback()
        flash(f'An error occurred while adding the line item: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('salesorder_routes.edit_sales_order', id=id))

@salesorder_bp.route('/sales-orders/<int:id>/delete-line/<int:line_id>', methods=['POST'])
@role_required('Admin', 'Planner')
def delete_line(id, line_id):
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('DELETE FROM sales_order_lines WHERE id = ? AND so_id = ?', (line_id, id))
        recalculate_totals(conn, id)
        conn.commit()
        flash('Line item deleted successfully!', 'success')
    except Exception as e:
        conn.rollback()
        flash('An error occurred while deleting the line item.', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('salesorder_routes.edit_sales_order', id=id))

@salesorder_bp.route('/sales-orders/<int:id>/mark-pending', methods=['POST'])
@role_required('Admin', 'Planner')
def mark_pending(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('''
            UPDATE sales_orders 
            SET status = 'Pending'
            WHERE id = ? AND status = 'Draft'
        ''', (id,))
        
        conn.commit()
        flash('Sales Order marked as Pending!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash('An error occurred while updating the order status.', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('salesorder_routes.view_sales_order', id=id))

@salesorder_bp.route('/sales-orders/<int:id>/fulfill', methods=['POST'])
@role_required('Admin', 'Planner')
def fulfill_order(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        # Get sales order details
        so = conn.execute('SELECT * FROM sales_orders WHERE id = ?', (id,)).fetchone()
        
        if not so:
            flash('Sales Order not found', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.list_sales_orders'))
        
        if so['status'] != 'Pending':
            flash('Only Pending orders can be fulfilled. Please mark as Pending first.', 'warning')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=id))
        
        # Get line items
        lines = conn.execute('''
            SELECT * FROM sales_order_lines WHERE so_id = ?
        ''', (id,)).fetchall()
        
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
                    return redirect(url_for('salesorder_routes.view_sales_order', id=id))
        
        # Deduct inventory
        for line in lines:
            if not line['is_core']:
                conn.execute('''
                    UPDATE inventory 
                    SET quantity = quantity - ?
                    WHERE product_id = ?
                ''', (line['quantity'], line['product_id']))
        
        # Update sales order status
        conn.execute('''
            UPDATE sales_orders 
            SET status = 'Shipped', actual_ship_date = CURRENT_DATE
            WHERE id = ?
        ''', (id,))
        
        conn.commit()
        flash('Sales Order fulfilled successfully! Inventory has been deducted.', 'success')
        
    except Exception as e:
        conn.rollback()
        flash('An error occurred while fulfilling the order. Please try again.', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('salesorder_routes.view_sales_order', id=id))

@salesorder_bp.route('/sales-orders/<int:id>/complete', methods=['POST'])
@role_required('Admin', 'Planner')
def complete_order(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('''
            UPDATE sales_orders 
            SET status = 'Completed'
            WHERE id = ?
        ''', (id,))
        
        conn.commit()
        flash('Sales Order marked as completed!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash('An error occurred while completing the order.', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('salesorder_routes.view_sales_order', id=id))

def recalculate_totals(conn, so_id, tax_rate=None):
    # Calculate totals from line items including core charges
    totals = conn.execute('''
        SELECT 
            COALESCE(SUM(line_total), 0) as subtotal,
            COALESCE(SUM(CASE WHEN line_type = 'Exchange' THEN core_charge ELSE 0 END), 0) as total_core_charges
        FROM sales_order_lines
        WHERE so_id = ?
    ''', (so_id,)).fetchone()
    
    # Get current SO for core/repair charges and tax rate
    so = conn.execute(
        'SELECT core_charge, repair_charge, tax_rate FROM sales_orders WHERE id = ?', (so_id,)
    ).fetchone()
    
    # Use provided tax_rate or fall back to saved tax_rate
    if tax_rate is None:
        tax_rate = so['tax_rate'] or 0.0
    
    # Calculate subtotal including legacy core/repair charges from SO header AND line-level core charges
    total_core_charges = totals['total_core_charges']
    subtotal = totals['subtotal'] + total_core_charges + (so['core_charge'] or 0) + (so['repair_charge'] or 0)
    
    # Calculate tax based on rate
    tax_amount = subtotal * (tax_rate / 100) if tax_rate else 0
    total_amount = subtotal + tax_amount
    
    # Get amount paid
    amount_paid = conn.execute('''
        SELECT COALESCE(SUM(amount), 0) as paid
        FROM payments
        WHERE reference_type = ? AND reference_id = ?
    ''', ('SalesOrder', so_id)).fetchone()['paid']
    
    balance_due = total_amount - amount_paid
    
    # Update sales order with calculated values
    conn.execute('''
        UPDATE sales_orders SET
            subtotal = ?, tax_amount = ?, total_amount = ?, balance_due = ?
        WHERE id = ?
    ''', (subtotal, tax_amount, total_amount, balance_due, so_id))
