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
            
            # Insert sales order
            cursor = conn.execute('''
                INSERT INTO sales_orders (
                    so_number, customer_id, sales_type, order_date, expected_ship_date,
                    status, core_charge, repair_charge, expected_return_date, 
                    service_notes, notes, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                so_number, customer_id, sales_type, order_date, expected_ship_date,
                'Draft', 0, 0, expected_return_date,
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
            customer_id = request.form.get('customer_id')
            expected_ship_date = request.form.get('expected_ship_date') or None
            expected_return_date = request.form.get('expected_return_date') or None
            
            tax_rate_str = request.form.get('tax_rate', '0').strip()
            tax_rate = float(tax_rate_str) if tax_rate_str else 0.0
            
            conn.execute('''
                UPDATE sales_orders SET
                    customer_id = ?,
                    expected_ship_date = ?,
                    expected_return_date = ?, service_notes = ?, notes = ?, tax_rate = ?
                WHERE id = ?
            ''', (
                customer_id, expected_ship_date, expected_return_date,
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
    
    # Get customers for customer dropdown
    customers = conn.execute(
        'SELECT * FROM customers WHERE status = ? ORDER BY name', ('Active',)
    ).fetchall()
    
    conn.close()
    
    return render_template('salesorders/edit.html', 
                         sales_order=sales_order, lines=lines, products=products, customers=customers)

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
        
        # VALIDATION 1: Pricing validation
        if unit_price < 0:
            flash('Unit price cannot be negative.', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.edit_sales_order', id=id))
        
        if quantity <= 0:
            flash('Quantity must be greater than zero.', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.edit_sales_order', id=id))
        
        # VALIDATION 2: Discount limits (max 50% unless admin override)
        MAX_DISCOUNT = 50.0
        if discount_percent > MAX_DISCOUNT and session.get('role') not in ['Admin']:
            flash(f'Discount cannot exceed {MAX_DISCOUNT}%. Please contact administrator for approval.', 'warning')
            conn.close()
            return redirect(url_for('salesorder_routes.edit_sales_order', id=id))
        
        if discount_percent > 100:
            flash('Discount cannot exceed 100%.', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.edit_sales_order', id=id))
        
        # VALIDATION 3: Stock availability check for non-core items (aggregate quantities)
        if not request.form.get('is_core'):
            # Get existing quantity for this product on this order
            existing_qty = conn.execute('''
                SELECT COALESCE(SUM(quantity), 0) as total_qty
                FROM sales_order_lines
                WHERE so_id = ? AND product_id = ? AND (is_core IS NULL OR is_core = 0)
            ''', (id, product_id)).fetchone()['total_qty']
            
            # Total quantity will be existing + new line
            total_qty_needed = existing_qty + quantity
            
            # Get available inventory
            inventory = conn.execute('''
                SELECT 
                    COALESCE(quantity, 0) as available,
                    COALESCE(reserved_quantity, 0) as reserved
                FROM inventory 
                WHERE product_id = ?
            ''', (product_id,)).fetchone()
            
            available_qty = 0
            if inventory:
                available_qty = inventory['available'] - inventory['reserved']
            
            if available_qty < total_qty_needed:
                product = conn.execute(
                    'SELECT code, name FROM products WHERE id = ?', (product_id,)
                ).fetchone()
                flash(f'Insufficient stock for {product["code"]} - {product["name"]}. Available: {available_qty}, Already on order: {existing_qty}, Requested: {quantity}, Total needed: {total_qty_needed}', 'warning')
                conn.close()
                return redirect(url_for('salesorder_routes.edit_sales_order', id=id))
        
        # Calculate line total
        line_total = (quantity * unit_price) * (1 - discount_percent / 100)
        
        # VALIDATION 4: Credit limit check before adding line
        so = conn.execute('SELECT customer_id, total_amount FROM sales_orders WHERE id = ?', (id,)).fetchone()
        customer = conn.execute('''
            SELECT credit_limit, customer_number, name FROM customers WHERE id = ?
        ''', (so['customer_id'],)).fetchone()
        
        # Calculate customer's current outstanding balance
        outstanding = conn.execute('''
            SELECT COALESCE(SUM(balance_due), 0) as total_outstanding
            FROM sales_orders
            WHERE customer_id = ? AND status NOT IN ('Closed', 'Completed') AND id != ?
        ''', (so['customer_id'], id)).fetchone()['total_outstanding']
        
        # Project new total after adding this line
        projected_total = outstanding + so['total_amount'] + line_total
        
        if customer['credit_limit'] > 0 and projected_total > customer['credit_limit']:
            flash(f'Credit limit exceeded for {customer["customer_number"]} - {customer["name"]}. Credit Limit: ${customer["credit_limit"]:,.2f}, Current Outstanding: ${outstanding:,.2f}, Projected: ${projected_total:,.2f}', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.edit_sales_order', id=id))
        
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

@salesorder_bp.route('/sales-orders/<int:id>/edit-line/<int:line_id>', methods=['POST'])
@role_required('Admin', 'Planner')
def edit_line(id, line_id):
    db = Database()
    conn = db.get_connection()
    
    try:
        # Get current line to verify it exists and belongs to this SO
        current_line = conn.execute('''
            SELECT * FROM sales_order_lines WHERE id = ? AND so_id = ?
        ''', (line_id, id)).fetchone()
        
        if not current_line:
            flash('Line item not found.', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.edit_sales_order', id=id))
        
        # Parse form values
        quantity_str = request.form.get('quantity', '0').strip()
        quantity = float(quantity_str) if quantity_str else current_line['quantity']
        
        unit_price_str = request.form.get('unit_price', '0').strip()
        unit_price = float(unit_price_str) if unit_price_str else current_line['unit_price']
        
        discount_str = request.form.get('discount_percent', '0').strip()
        discount_percent = float(discount_str) if discount_str else 0.0
        
        line_status = request.form.get('line_status', current_line['line_status'])
        description = request.form.get('description', current_line['description'])
        serial_number = request.form.get('serial_number', current_line['serial_number'])
        line_notes = request.form.get('line_notes', current_line['line_notes'])
        
        # Validations
        if unit_price < 0:
            flash('Unit price cannot be negative.', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.edit_sales_order', id=id))
        
        if quantity <= 0:
            flash('Quantity must be greater than zero.', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.edit_sales_order', id=id))
        
        if discount_percent > 100:
            flash('Discount cannot exceed 100%.', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.edit_sales_order', id=id))
        
        # Calculate line total
        line_total = (quantity * unit_price) * (1 - discount_percent / 100)
        
        # Exchange-specific fields
        core_charge = 0.0
        if current_line['line_type'] == 'Exchange':
            core_charge_str = request.form.get('core_charge', '0').strip()
            core_charge = float(core_charge_str) if core_charge_str else 0.0
        
        # Managed Repair-specific fields
        repair_nte = 0.0
        quoted_tat = None
        if current_line['line_type'] == 'Managed Repair':
            repair_nte_str = request.form.get('repair_nte', '0').strip()
            repair_nte = float(repair_nte_str) if repair_nte_str else 0.0
            quoted_tat = request.form.get('quoted_tat') or current_line['quoted_tat']
        
        # Update the line item
        conn.execute('''
            UPDATE sales_order_lines SET
                quantity = ?,
                unit_price = ?,
                discount_percent = ?,
                line_total = ?,
                line_status = ?,
                description = ?,
                serial_number = ?,
                line_notes = ?,
                core_charge = ?,
                repair_nte = ?,
                quoted_tat = ?,
                modified_by = ?,
                modified_at = CURRENT_TIMESTAMP
            WHERE id = ? AND so_id = ?
        ''', (
            quantity, unit_price, discount_percent, line_total,
            line_status, description, serial_number, line_notes,
            core_charge, repair_nte, quoted_tat,
            session.get('user_id'), line_id, id
        ))
        
        # Recalculate order totals
        recalculate_totals(conn, id)
        
        conn.commit()
        flash('Line item updated successfully!', 'success')
        
    except ValueError as e:
        conn.rollback()
        flash('Please enter valid numeric values.', 'danger')
    except Exception as e:
        conn.rollback()
        flash(f'An error occurred while updating the line item.', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('salesorder_routes.edit_sales_order', id=id))

@salesorder_bp.route('/sales-orders/<int:id>/get-line/<int:line_id>')
@login_required
def get_line(id, line_id):
    """API endpoint to get line item details for editing"""
    db = Database()
    conn = db.get_connection()
    
    line = conn.execute('''
        SELECT sol.*, p.code, p.name as product_name
        FROM sales_order_lines sol
        JOIN products p ON sol.product_id = p.id
        WHERE sol.id = ? AND sol.so_id = ?
    ''', (line_id, id)).fetchone()
    
    conn.close()
    
    if not line:
        return jsonify({'error': 'Line not found'}), 404
    
    return jsonify({
        'id': line['id'],
        'line_number': line['line_number'],
        'product_id': line['product_id'],
        'product_code': line['code'],
        'product_name': line['product_name'],
        'quantity': line['quantity'],
        'unit_price': line['unit_price'],
        'discount_percent': line['discount_percent'] or 0,
        'line_total': line['line_total'],
        'line_type': line['line_type'],
        'line_status': line['line_status'],
        'description': line['description'] or '',
        'serial_number': line['serial_number'] or '',
        'line_notes': line['line_notes'] or '',
        'core_charge': line['core_charge'] or 0,
        'repair_nte': line['repair_nte'] or 0,
        'quoted_tat': line['quoted_tat'] or ''
    })

@salesorder_bp.route('/sales-orders/<int:id>/confirm', methods=['POST'])
@role_required('Admin', 'Planner')
def confirm_order(id):
    """Transition from Draft/Pending to Confirmed (supports legacy Pending orders)"""
    db = Database()
    conn = db.get_connection()
    
    try:
        # Check if order has line items
        line_count = conn.execute('''
            SELECT COUNT(*) as count FROM sales_order_lines WHERE so_id = ?
        ''', (id,)).fetchone()['count']
        
        if line_count == 0:
            flash('Cannot confirm order without line items. Please add at least one line item.', 'warning')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=id))
        
        # VALIDATION: Check stock availability for all non-core line items (aggregated per product)
        product_totals = conn.execute('''
            SELECT 
                sol.product_id,
                p.code,
                p.name,
                SUM(sol.quantity) as total_qty
            FROM sales_order_lines sol
            JOIN products p ON sol.product_id = p.id
            WHERE sol.so_id = ? AND (sol.is_core IS NULL OR sol.is_core = 0)
            GROUP BY sol.product_id, p.code, p.name
        ''', (id,)).fetchall()
        
        stock_issues = []
        for product in product_totals:
            inventory = conn.execute('''
                SELECT 
                    COALESCE(quantity, 0) as available,
                    COALESCE(reserved_quantity, 0) as reserved
                FROM inventory 
                WHERE product_id = ?
            ''', (product['product_id'],)).fetchone()
            
            available_qty = 0
            if inventory:
                available_qty = inventory['available'] - inventory['reserved']
            
            if available_qty < product['total_qty']:
                stock_issues.append(f"{product['code']}: Need {product['total_qty']}, Available {available_qty}")
        
        if stock_issues:
            flash('Cannot confirm order - Insufficient stock: ' + '; '.join(stock_issues), 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=id))
        
        # VALIDATION: Check credit limit
        so = conn.execute('SELECT customer_id, total_amount FROM sales_orders WHERE id = ?', (id,)).fetchone()
        customer = conn.execute('''
            SELECT credit_limit, customer_number, name FROM customers WHERE id = ?
        ''', (so['customer_id'],)).fetchone()
        
        # Calculate customer's current outstanding balance (excluding this order)
        outstanding = conn.execute('''
            SELECT COALESCE(SUM(balance_due), 0) as total_outstanding
            FROM sales_orders
            WHERE customer_id = ? AND status NOT IN ('Closed', 'Completed') AND id != ?
        ''', (so['customer_id'], id)).fetchone()['total_outstanding']
        
        # Check if confirming this order would exceed credit limit
        if customer['credit_limit'] > 0 and (outstanding + so['total_amount']) > customer['credit_limit']:
            flash(f'Cannot confirm - Credit limit exceeded for {customer["customer_number"]}. Limit: ${customer["credit_limit"]:,.2f}, Outstanding: ${outstanding:,.2f}, This Order: ${so["total_amount"]:,.2f}', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=id))
        
        # Support both Draft (new) and Pending (legacy migration)
        conn.execute('''
            UPDATE sales_orders 
            SET status = 'Confirmed'
            WHERE id = ? AND status IN ('Draft', 'Pending')
        ''', (id,))
        
        conn.commit()
        flash('Sales Order confirmed successfully!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash('An error occurred while confirming the order.', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('salesorder_routes.view_sales_order', id=id))

@salesorder_bp.route('/sales-orders/<int:id>/release-to-shipping', methods=['POST'])
@role_required('Admin', 'Planner')
def release_to_shipping(id):
    """Release Sales Order to Pending Shipments"""
    db = Database()
    conn = db.get_connection()
    
    try:
        # Get sales order details with customer info
        so = conn.execute('''
            SELECT so.*, c.name as customer_name, c.customer_number,
                   c.email, c.phone, c.address, c.city, c.state, c.postal_code, c.country
            FROM sales_orders so
            JOIN customers c ON so.customer_id = c.id
            WHERE so.id = ?
        ''', (id,)).fetchone()
        
        if not so:
            flash('Sales Order not found', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.list_sales_orders'))
        
        # Only Confirmed orders can be released
        if so['status'] != 'Confirmed':
            flash('Only Confirmed orders can be released to shipping.', 'warning')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=id))
        
        # Check if already released
        existing = conn.execute('''
            SELECT id FROM shipments 
            WHERE reference_type = 'Sales Order' AND reference_id = ? AND shipment_stage = 'Pending'
        ''', (id,)).fetchone()
        
        if existing:
            flash('This order has already been released to shipping.', 'warning')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=id))
        
        # Get line items count
        line_count = conn.execute('''
            SELECT COUNT(*) as count FROM sales_order_lines WHERE so_id = ?
        ''', (id,)).fetchone()['count']
        
        # Generate shipment number
        last_shipment = conn.execute(
            'SELECT shipment_number FROM shipments ORDER BY id DESC LIMIT 1'
        ).fetchone()
        
        if last_shipment and last_shipment['shipment_number']:
            last_num = int(last_shipment['shipment_number'].split('-')[1])
            shipment_number = f'SHIP-{last_num + 1:05d}'
        else:
            shipment_number = 'SHIP-00001'
        
        # Create pending shipment record
        conn.execute('''
            INSERT INTO shipments (
                shipment_number, shipment_type, reference_type, reference_id,
                status, shipment_stage, ship_to_name, ship_to_address, ship_to_city,
                ship_to_state, ship_to_postal_code, ship_to_country,
                released_by, released_at, created_by, created_at
            ) VALUES (?, 'Outbound', 'Sales Order', ?, 'Pending', 'Pending',
                      ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP)
        ''', (
            shipment_number, id,
            so['customer_name'], so['address'], so['city'],
            so['state'], so['postal_code'], so['country'],
            session.get('user_id'), session.get('user_id')
        ))
        
        # Update sales order status to Released to Shipping
        conn.execute('''
            UPDATE sales_orders 
            SET status = 'Released to Shipping'
            WHERE id = ?
        ''', (id,))
        
        # Log activity
        from models import AuditTrail
        AuditTrail.log_change(
            conn=conn,
            record_type='sales_orders',
            record_id=id,
            action_type='UPDATE',
            modified_by=session.get('user_id'),
            changed_fields={'status': 'Released to Shipping', 'shipment_number': shipment_number}
        )
        
        conn.commit()
        flash(f'Sales Order released to Pending Shipments ({shipment_number}). Shipping personnel can now process this shipment.', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'An error occurred while releasing to shipping: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('salesorder_routes.view_sales_order', id=id))

@salesorder_bp.route('/sales-orders/<int:id>/invoice', methods=['POST'])
@role_required('Admin', 'Planner', 'Accountant')
def invoice_order(id):
    """Transition from Shipped to Invoiced"""
    db = Database()
    conn = db.get_connection()
    
    try:
        so = conn.execute('SELECT * FROM sales_orders WHERE id = ?', (id,)).fetchone()
        
        if not so:
            flash('Sales Order not found', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.list_sales_orders'))
        
        if so['status'] != 'Shipped':
            flash('Only Shipped orders can be invoiced.', 'warning')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=id))
        
        # Update status to Invoiced
        conn.execute('''
            UPDATE sales_orders 
            SET status = 'Invoiced'
            WHERE id = ?
        ''', (id,))
        
        conn.commit()
        flash('Sales Order invoiced successfully!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash('An error occurred while invoicing the order.', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('salesorder_routes.view_sales_order', id=id))

@salesorder_bp.route('/sales-orders/<int:id>/close', methods=['POST'])
@role_required('Admin', 'Planner', 'Accountant')
def close_order(id):
    """Transition from Invoiced/Completed to Closed (supports legacy Completed orders)"""
    db = Database()
    conn = db.get_connection()
    
    try:
        so = conn.execute('SELECT * FROM sales_orders WHERE id = ?', (id,)).fetchone()
        
        if not so:
            flash('Sales Order not found', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.list_sales_orders'))
        
        # Support both Invoiced (new) and Completed (legacy migration)
        if so['status'] not in ['Invoiced', 'Completed']:
            flash('Only Invoiced or Completed orders can be closed.', 'warning')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=id))
        
        # Check if payment is complete
        if so['balance_due'] > 0:
            flash('Cannot close order with outstanding balance. Please process payment first.', 'warning')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=id))
        
        # Update status to Closed
        conn.execute('''
            UPDATE sales_orders 
            SET status = 'Closed'
            WHERE id = ?
        ''', (id,))
        
        conn.commit()
        flash('Sales Order closed successfully!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash('An error occurred while closing the order.', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('salesorder_routes.view_sales_order', id=id))

@salesorder_bp.route('/sales-orders/lines/<int:line_id>/allocate', methods=['POST'])
@role_required('Admin', 'Planner')
def allocate_line(line_id):
    """Allocate inventory to a sales order line"""
    db = Database()
    conn = db.get_connection()
    so_id = None
    
    try:
        # Get line details
        line = conn.execute('''
            SELECT sol.*, p.code, p.name, so.so_number
            FROM sales_order_lines sol
            JOIN products p ON sol.product_id = p.id
            JOIN sales_orders so ON sol.so_id = so.id
            WHERE sol.id = ?
        ''', (line_id,)).fetchone()
        
        if not line:
            flash('Sales order line not found', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.list_sales_orders'))
        
        so_id = line['so_id']
        
        # Check if line is a core (don't allocate cores)
        if line['is_core']:
            flash('Core items do not require inventory allocation.', 'info')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
        
        # Get available inventory with serialization info
        inventory = conn.execute('''
            SELECT i.* 
            FROM inventory i
            WHERE i.product_id = ?
        ''', (line['product_id'],)).fetchone()
        
        available_qty = inventory['quantity'] if inventory else 0
        requested_qty = line['quantity']
        serial_number = None
        
        # For serialized products, get the serial number
        if inventory and inventory['is_serialized']:
            serial_number = inventory['serial_number']
            
            # Check if this serial number is already allocated to another sales order line
            existing_allocation = conn.execute('''
                SELECT sol.id, so.so_number, sol.line_number
                FROM sales_order_lines sol
                JOIN sales_orders so ON sol.so_id = so.id
                WHERE sol.serial_number = ? 
                AND sol.id != ?
                AND sol.allocation_status IN ('Allocated', 'Partially Allocated')
            ''', (serial_number, line_id)).fetchone()
            
            if existing_allocation:
                flash(f'Serial number {serial_number} is already allocated to SO {existing_allocation["so_number"]}, Line {existing_allocation["line_number"]}. Please deallocate it first.', 'danger')
                conn.close()
                return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
        
        # Determine allocation status and quantity
        if available_qty >= requested_qty:
            allocated_qty = requested_qty
            allocation_status = 'Allocated'
            serial_msg = f' (S/N: {serial_number})' if serial_number else ''
            flash(f'Successfully allocated {allocated_qty} units of {line["code"]} to line {line["line_number"]}{serial_msg}', 'success')
        elif available_qty > 0:
            allocated_qty = available_qty
            allocation_status = 'Partially Allocated'
            flash(f'Partially allocated {allocated_qty} of {requested_qty} units for {line["code"]}. {requested_qty - allocated_qty} units backordered.', 'warning')
        else:
            allocated_qty = 0
            allocation_status = 'Backordered'
            flash(f'No inventory available for {line["code"]}. Line is backordered.', 'warning')
        
        # Update line with allocation including serial number
        conn.execute('''
            UPDATE sales_order_lines
            SET allocated_quantity = ?,
                allocation_status = ?,
                allocation_notes = ?,
                serial_number = ?,
                modified_by = ?,
                modified_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (
            allocated_qty,
            allocation_status,
            f'Allocated from available inventory: {available_qty} units',
            serial_number,
            session.get('user_id'),
            line_id
        ))
        
        # Update line status
        conn.execute('''
            UPDATE sales_order_lines
            SET line_status = ?
            WHERE id = ?
        ''', (allocation_status, line_id))
        
        # Log activity
        from models import AuditTrail
        changed_fields = {
            'allocated_quantity': allocated_qty, 
            'allocation_status': allocation_status
        }
        if serial_number:
            changed_fields['serial_number'] = serial_number
        
        AuditTrail.log_change(
            conn=conn,
            record_type='sales_order_lines',
            record_id=line_id,
            action_type='UPDATE',
            modified_by=session.get('user_id'),
            changed_fields=changed_fields
        )
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        flash(f'An error occurred during allocation: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))

@salesorder_bp.route('/sales-orders/lines/<int:line_id>/release-to-shipping', methods=['POST'])
@role_required('Admin', 'Planner')
def release_line_to_shipping(line_id):
    """Release an individual line to shipping"""
    db = Database()
    conn = db.get_connection()
    so_id = None
    
    try:
        # Get line details
        line = conn.execute('''
            SELECT sol.*, p.code, p.name, so.so_number, so.id as so_id
            FROM sales_order_lines sol
            JOIN products p ON sol.product_id = p.id
            JOIN sales_orders so ON sol.so_id = so.id
            WHERE sol.id = ?
        ''', (line_id,)).fetchone()
        
        if not line:
            flash('Sales order line not found', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.list_sales_orders'))
        
        so_id = line['so_id']
        
        # Validation: Check if line is allocated
        if line['allocation_status'] not in ['Allocated', 'Partially Allocated'] and not line['is_core']:
            flash(f'Cannot release line {line["line_number"]} - inventory not allocated. Please allocate inventory first.', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
        
        # Validation: Check if already released
        if line['released_to_shipping_at']:
            flash(f'Line {line["line_number"]} has already been released to shipping.', 'warning')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
        
        # Release the line
        conn.execute('''
            UPDATE sales_order_lines
            SET released_to_shipping_at = CURRENT_TIMESTAMP,
                released_by = ?,
                line_status = 'Released to Shipping',
                modified_by = ?,
                modified_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (session.get('user_id'), session.get('user_id'), line_id))
        
        # Log activity
        from models import AuditTrail
        AuditTrail.log_change(
            conn=conn,
            record_type='sales_order_lines',
            record_id=line_id,
            action_type='UPDATE',
            modified_by=session.get('user_id'),
            changed_fields={'line_status': 'Released to Shipping', 'released_to_shipping_at': 'CURRENT_TIMESTAMP'}
        )
        
        # Check if all lines are released - if so, update SO status
        remaining_lines = conn.execute('''
            SELECT COUNT(*) as count
            FROM sales_order_lines
            WHERE so_id = ? AND released_to_shipping_at IS NULL
        ''', (line['so_id'],)).fetchone()
        
        if remaining_lines['count'] == 0:
            # All lines released - update SO status
            conn.execute('''
                UPDATE sales_orders
                SET status = 'Released to Shipping'
                WHERE id = ?
            ''', (line['so_id'],))
            flash(f'Line {line["line_number"]} released to shipping. All lines now released - Sales Order status updated.', 'success')
        else:
            flash(f'Line {line["line_number"]} released to shipping. {remaining_lines["count"]} lines remaining.', 'success')
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        flash(f'An error occurred while releasing line: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))

@salesorder_bp.route('/sales-orders/<int:id>/allocate-all', methods=['POST'])
@role_required('Admin', 'Planner')
def allocate_all_lines(id):
    """Allocate inventory to all lines in a sales order"""
    db = Database()
    conn = db.get_connection()
    
    try:
        # Get all non-core lines
        lines = conn.execute('''
            SELECT sol.*, p.code
            FROM sales_order_lines sol
            JOIN products p ON sol.product_id = p.id
            WHERE sol.so_id = ? AND sol.is_core = 0
        ''', (id,)).fetchall()
        
        allocated_count = 0
        partial_count = 0
        backorder_count = 0
        
        for line in lines:
            # Get available inventory
            inventory = conn.execute('''
                SELECT quantity FROM inventory WHERE product_id = ?
            ''', (line['product_id'],)).fetchone()
            
            available_qty = inventory['quantity'] if inventory else 0
            requested_qty = line['quantity']
            
            # Determine allocation
            if available_qty >= requested_qty:
                allocated_qty = requested_qty
                allocation_status = 'Allocated'
                allocated_count += 1
            elif available_qty > 0:
                allocated_qty = available_qty
                allocation_status = 'Partially Allocated'
                partial_count += 1
            else:
                allocated_qty = 0
                allocation_status = 'Backordered'
                backorder_count += 1
            
            # Update line
            conn.execute('''
                UPDATE sales_order_lines
                SET allocated_quantity = ?,
                    allocation_status = ?,
                    line_status = ?,
                    modified_by = ?,
                    modified_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (allocated_qty, allocation_status, allocation_status, session.get('user_id'), line['id']))
        
        # Log activity
        from models import AuditTrail
        AuditTrail.log_change(
            conn=conn,
            record_type='sales_orders',
            record_id=id,
            action_type='UPDATE',
            modified_by=session.get('user_id'),
            changed_fields={'bulk_allocation': f'{allocated_count} allocated, {partial_count} partial, {backorder_count} backordered'}
        )
        
        conn.commit()
        
        # Build feedback message
        messages = []
        if allocated_count > 0:
            messages.append(f'{allocated_count} lines fully allocated')
        if partial_count > 0:
            messages.append(f'{partial_count} lines partially allocated')
        if backorder_count > 0:
            messages.append(f'{backorder_count} lines backordered')
        
        flash(f'Allocation complete: {", ".join(messages)}', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'An error occurred during bulk allocation: {str(e)}', 'danger')
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
