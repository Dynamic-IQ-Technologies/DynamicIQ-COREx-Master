from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify, send_file
from models import Database
from auth import login_required, role_required
from datetime import datetime, timedelta
import json
import io
import os
import requests


def get_brevo_credentials():
    """Get Brevo API key and from email from environment"""
    api_key = os.environ.get('BREVO_API_KEY')
    from_email = os.environ.get('BREVO_FROM_EMAIL')
    return api_key, from_email


def send_email_via_brevo(to_email, to_name, subject, html_content, from_email, from_name, api_key, cc_email=None):
    """Send email using Brevo (Sendinblue) API"""
    import sib_api_v3_sdk
    from sib_api_v3_sdk.rest import ApiException
    
    try:
        configuration = sib_api_v3_sdk.Configuration()
        configuration.api_key['api-key'] = api_key
        
        api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))
        
        email_params = {
            "to": [{"email": to_email, "name": to_name or to_email}],
            "sender": {"email": from_email, "name": from_name or "Dynamic.IQ-COREx"},
            "subject": subject,
            "html_content": html_content
        }
        
        if cc_email:
            email_params["cc"] = [{"email": cc_email}]
        
        send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(**email_params)
        
        api_instance.send_transac_email(send_smtp_email)
        return True, None
    except ApiException as e:
        return False, f"Brevo API error: {e.reason}"
    except Exception as e:
        return False, str(e)
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT

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
               COALESCE(lc.line_count, 0) as line_count
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        LEFT JOIN (
            SELECT so_id, COUNT(*) as line_count FROM sales_order_lines GROUP BY so_id
        ) lc ON lc.so_id = so.id
        WHERE 1=1
    '''
    
    params = []
    if status_filter:
        query += ' AND so.status = ?'
        params.append(status_filter)
    if type_filter:
        query += ' AND so.sales_type = ?'
        params.append(type_filter)
    
    query += ' ORDER BY so.order_date DESC, so.id DESC'
    
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
            
            # Handle Core Due Days and auto-calculate Expected Return Date for Exchange orders
            core_due_days = None
            expected_return_date = None
            if sales_type == 'Exchange':
                core_due_days_str = request.form.get('core_due_days', '').strip()
                if not core_due_days_str:
                    flash('Core Due Days is required for Exchange orders.', 'danger')
                    conn.close()
                    return redirect(url_for('salesorder_routes.create_sales_order'))
                core_due_days = int(core_due_days_str)
                if core_due_days < 0 or core_due_days > 365:
                    flash('Core Due Days must be between 0 and 365.', 'danger')
                    conn.close()
                    return redirect(url_for('salesorder_routes.create_sales_order'))
                # Auto-calculate expected return date
                order_dt = datetime.strptime(order_date, '%Y-%m-%d')
                expected_return_dt = order_dt + timedelta(days=core_due_days)
                expected_return_date = expected_return_dt.strftime('%Y-%m-%d')
            elif sales_type == 'Managed Repair':
                # Managed Repair orders use manual expected_return_date
                expected_return_date = request.form.get('expected_return_date') or None
            
            # Get exchange_type if Exchange type order
            exchange_type = request.form.get('exchange_type') if sales_type == 'Exchange' else None
            
            # Insert sales order
            cursor = conn.execute('''
                INSERT INTO sales_orders (
                    so_number, customer_id, sales_type, order_date, expected_ship_date,
                    status, core_charge, repair_charge, expected_return_date, 
                    service_notes, notes, created_by, exchange_type, core_due_days
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                so_number, customer_id, sales_type, order_date, expected_ship_date,
                'Draft', 0, 0, expected_return_date,
                request.form.get('service_notes', ''),
                request.form.get('notes', ''),
                session.get('user_id'),
                exchange_type,
                core_due_days
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
        SELECT sol.*, p.code, p.name as product_name, p.unit_of_measure, p.is_serialized,
               wo.wo_number
        FROM sales_order_lines sol
        JOIN products p ON sol.product_id = p.id
        LEFT JOIN work_orders wo ON sol.work_order_id = wo.id
        WHERE sol.so_id = ?
        ORDER BY sol.line_number
    ''', (id,)).fetchall()
    
    # Get payments
    payments = conn.execute('''
        SELECT * FROM payments
        WHERE reference_type = ? AND reference_id = ?
        ORDER BY payment_date DESC
    ''', ('SalesOrder', id)).fetchall()
    
    # Get linked Exchange POs (for Dual Exchange orders)
    exchange_pos = []
    if sales_order['sales_type'] == 'Exchange' and sales_order['exchange_type'] == 'Dual Exchange':
        exchange_pos = conn.execute('''
            SELECT po.*, s.name as supplier_name,
                   CASE 
                       WHEN po.exchange_owner_type = 'Customer' THEN c.name
                       WHEN po.exchange_owner_type = 'Supplier' THEN sup.name
                   END as owner_name,
                   (SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) 
                    FROM purchase_order_lines pol WHERE pol.po_id = po.id) as total_amount
            FROM purchase_orders po
            JOIN suppliers s ON po.supplier_id = s.id
            LEFT JOIN customers c ON po.exchange_owner_type = 'Customer' AND po.exchange_owner_id = c.id
            LEFT JOIN suppliers sup ON po.exchange_owner_type = 'Supplier' AND po.exchange_owner_id = sup.id
            WHERE po.source_sales_order_id = ? AND po.is_exchange = 1
            ORDER BY po.created_at DESC
        ''', (id,)).fetchall()
    
    # Get related work orders
    related_work_orders = conn.execute('''
        SELECT wo.id, wo.wo_number, wo.workorder_type, wo.status, wo.created_at,
               wos.name as stage_name, wos.color as stage_color,
               p.code as product_code, p.name as product_name
        FROM work_orders wo
        LEFT JOIN work_order_stages wos ON wo.stage_id = wos.id
        LEFT JOIN products p ON wo.product_id = p.id
        WHERE wo.so_id = ?
        ORDER BY wo.created_at DESC
    ''', (id,)).fetchall()
    
    # Get related invoices
    related_invoices = conn.execute('''
        SELECT i.id, i.invoice_number, i.invoice_type, i.invoice_date, i.status,
               i.total_amount, i.balance_due
        FROM invoices i
        WHERE i.so_id = ?
        ORDER BY i.created_at DESC
    ''', (id,)).fetchall()
    
    # Get related purchase orders (linked through work orders or directly)
    related_pos = conn.execute('''
        SELECT DISTINCT po.id, po.po_number, po.po_type, po.status, po.order_date,
               s.name as supplier_name,
               (SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) 
                FROM purchase_order_lines pol WHERE pol.po_id = po.id) as total_amount
        FROM purchase_orders po
        JOIN suppliers s ON po.supplier_id = s.id
        WHERE po.source_sales_order_id = ? OR po.id IN (
            SELECT DISTINCT wo.po_id FROM work_orders wo WHERE wo.so_id = ? AND wo.po_id IS NOT NULL
        )
        ORDER BY po.order_date DESC
    ''', (id, id)).fetchall()
    
    # Get related shipments
    related_shipments = conn.execute('''
        SELECT sh.id, sh.shipment_number, sh.shipment_type, sh.ship_date, sh.status,
               sh.carrier, sh.tracking_number,
               c.name as customer_name
        FROM shipments sh
        LEFT JOIN customers c ON sh.customer_id = c.id
        WHERE sh.so_id = ?
        ORDER BY sh.ship_date DESC
    ''', (id,)).fetchall()
    
    conn.close()
    
    return render_template('salesorders/view.html', 
                         sales_order=sales_order, lines=lines, payments=payments, exchange_pos=exchange_pos,
                         related_work_orders=related_work_orders, related_invoices=related_invoices,
                         related_pos=related_pos, related_shipments=related_shipments)

@salesorder_bp.route('/sales-orders/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def edit_sales_order(id):
    if request.method == 'POST':
        db = Database()
        conn = db.get_connection()
        try:
            # Get current sales order to check type
            current_so = conn.execute('SELECT sales_type, order_date, core_due_days, expected_return_date FROM sales_orders WHERE id = ?', (id,)).fetchone()
            
            # Update header
            customer_id = request.form.get('customer_id')
            expected_ship_date = request.form.get('expected_ship_date') or None
            
            tax_rate_str = request.form.get('tax_rate', '0').strip()
            tax_rate = float(tax_rate_str) if tax_rate_str else 0.0
            
            exchange_type = request.form.get('exchange_type')
            
            # Handle Core Due Days and auto-calculate Expected Return Date for Exchange orders
            core_due_days = current_so['core_due_days']
            expected_return_date = current_so['expected_return_date']
            old_core_due_days = core_due_days
            old_expected_return_date = expected_return_date
            
            if current_so['sales_type'] == 'Exchange':
                core_due_days_str = request.form.get('core_due_days', '').strip()
                if core_due_days_str:
                    core_due_days = int(core_due_days_str)
                    if core_due_days < 0 or core_due_days > 365:
                        flash('Core Due Days must be between 0 and 365.', 'danger')
                        conn.close()
                        return redirect(url_for('salesorder_routes.edit_sales_order', id=id))
                    # Auto-calculate expected return date using order_date
                    order_date = current_so['order_date']
                    order_dt = datetime.strptime(order_date, '%Y-%m-%d')
                    expected_return_dt = order_dt + timedelta(days=core_due_days)
                    expected_return_date = expected_return_dt.strftime('%Y-%m-%d')
            elif current_so['sales_type'] == 'Managed Repair':
                # Managed Repair orders use manual expected_return_date
                expected_return_date = request.form.get('expected_return_date') or None
            
            conn.execute('''
                UPDATE sales_orders SET
                    customer_id = ?,
                    expected_ship_date = ?,
                    expected_return_date = ?, service_notes = ?, notes = ?, tax_rate = ?,
                    exchange_type = ?, core_due_days = ?
                WHERE id = ?
            ''', (
                customer_id, expected_ship_date, expected_return_date,
                request.form.get('service_notes', ''), request.form.get('notes', ''), tax_rate,
                exchange_type, core_due_days, id
            ))
            
            # Audit logging for Core Due Days changes
            if current_so['sales_type'] == 'Exchange' and (old_core_due_days != core_due_days or old_expected_return_date != expected_return_date):
                from audit import AuditLogger
                AuditLogger.log(
                    conn=conn,
                    record_type='sales_order',
                    record_id=str(id),
                    action_type='Core Due Days Updated',
                    modified_by=session.get('user_id'),
                    changed_fields={
                        'core_due_days': {'old': old_core_due_days, 'new': core_due_days},
                        'expected_return_date': {'old': old_expected_return_date, 'new': expected_return_date}
                    }
                )
            
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
    
    # Get products for adding lines (show actual available = quantity - reserved)
    products = conn.execute('''
        SELECT p.*, COALESCE(i.quantity, 0) - COALESCE(i.reserved_quantity, 0) as available_qty
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
        # Get the line details for validation
        line = conn.execute('''
            SELECT sol.*, p.code as product_code
            FROM sales_order_lines sol
            LEFT JOIN products p ON sol.product_id = p.id
            WHERE sol.id = ? AND sol.so_id = ?
        ''', (line_id, id)).fetchone()
        
        if not line:
            flash('Line item not found.', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.edit_sales_order', id=id))
        
        # VALIDATION 1: Check if line has been shipped (released_to_shipping_at is set)
        if line['released_to_shipping_at']:
            flash(f'Cannot delete line {line["line_number"]} ({line["product_code"]}) - it has been released to shipping. Shipped lines cannot be deleted.', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.edit_sales_order', id=id))
        
        # VALIDATION 2: Check if line has been invoiced
        invoice_check = conn.execute('''
            SELECT il.id, i.invoice_number
            FROM invoice_lines il
            JOIN invoices i ON il.invoice_id = i.id
            WHERE il.reference_type = 'sales_order_line' AND il.reference_id = ?
        ''', (line_id,)).fetchone()
        
        if invoice_check:
            flash(f'Cannot delete line {line["line_number"]} ({line["product_code"]}) - it has been invoiced (Invoice #{invoice_check["invoice_number"]}). Invoiced lines cannot be deleted.', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.edit_sales_order', id=id))
        
        # VALIDATION 3: Check if line is allocated (has inventory reserved)
        if line['allocation_status'] in ('Allocated', 'Partially Allocated'):
            allocated_qty = line['allocated_quantity'] or 0
            flash(f'Cannot delete line {line["line_number"]} ({line["product_code"]}) - {allocated_qty} units are allocated. Please deallocate inventory first before deleting.', 'warning')
            conn.close()
            return redirect(url_for('salesorder_routes.edit_sales_order', id=id))
        
        # All validations passed - delete the line
        conn.execute('DELETE FROM sales_order_lines WHERE id = ? AND so_id = ?', (line_id, id))
        
        # Also delete any associated core tracking records
        conn.execute('DELETE FROM core_due_tracking WHERE so_line_id = ?', (line_id,))
        
        recalculate_totals(conn, id)
        conn.commit()
        flash(f'Line {line["line_number"]} ({line["product_code"]}) deleted successfully!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'An error occurred while deleting the line item: {str(e)}', 'danger')
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

@salesorder_bp.route('/sales-orders/<int:id>/email-preview')
@login_required
def email_preview(id):
    """Preview email acknowledgement before confirming order"""
    db = Database()
    conn = db.get_connection()
    
    sales_order = conn.execute('''
        SELECT so.*, c.name as customer_name, c.customer_number, c.email as customer_email
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        WHERE so.id = ?
    ''', (id,)).fetchone()
    
    if not sales_order:
        flash('Sales Order not found.', 'danger')
        conn.close()
        return redirect(url_for('salesorder_routes.list_sales_orders'))
    
    if sales_order['status'] not in ['Draft', 'Pending']:
        flash('Order has already been confirmed.', 'warning')
        conn.close()
        return redirect(url_for('salesorder_routes.view_sales_order', id=id))
    
    customer = conn.execute('SELECT * FROM customers WHERE id = ?', 
                           (sales_order['customer_id'],)).fetchone()
    
    lines = conn.execute('''
        SELECT sol.*, p.code, p.name
        FROM sales_order_lines sol
        JOIN products p ON sol.product_id = p.id
        WHERE sol.so_id = ?
        ORDER BY sol.line_number
    ''', (id,)).fetchall()
    
    company = conn.execute('SELECT * FROM company_settings LIMIT 1').fetchone()
    
    api_key, from_email = get_brevo_credentials()
    email_configured = bool(api_key and from_email)
    
    conn.close()
    
    return render_template('salesorders/email_preview.html',
                         sales_order=dict(sales_order),
                         customer=dict(customer),
                         lines=[dict(l) for l in lines],
                         company=dict(company) if company else {},
                         email_configured=email_configured)


@salesorder_bp.route('/sales-orders/<int:id>/send-acknowledgement', methods=['POST'])
@login_required
def send_order_acknowledgement(id):
    """Send order acknowledgement email and optionally confirm order"""
    db = Database()
    conn = db.get_connection()
    
    recipient_email = request.form.get('recipient_email')
    cc_email = request.form.get('cc_email')
    subject = request.form.get('subject')
    additional_message = request.form.get('additional_message')
    confirm_order_flag = request.form.get('confirm_order') == 'on'
    
    api_key, from_email = get_brevo_credentials()
    email_configured = bool(api_key and from_email)
    
    if email_configured and recipient_email:
        sales_order = conn.execute('''
            SELECT so.*, c.name as customer_name, c.customer_number, c.shipping_address
            FROM sales_orders so
            JOIN customers c ON so.customer_id = c.id
            WHERE so.id = ?
        ''', (id,)).fetchone()
        
        lines = conn.execute('''
            SELECT sol.*, p.code, p.name, p.unit_of_measure
            FROM sales_order_lines sol
            JOIN products p ON sol.product_id = p.id
            WHERE sol.so_id = ?
            ORDER BY sol.line_number
        ''', (id,)).fetchall()
        
        company = conn.execute('SELECT * FROM company_settings LIMIT 1').fetchone()
        company = dict(company) if company else {}
        
        lines_html = ""
        for line in lines:
            line_total = (line['quantity'] or 0) * (line['unit_price'] or 0)
            lines_html += f"""
            <tr>
                <td style="padding:12px 15px;border-bottom:1px solid #e2e8f0;">
                    <div style="font-weight:600;color:#1e3a5f;">{line['code']}</div>
                    <div style="font-size:10px;color:#64748b;">{line['name']}</div>
                </td>
                <td style="padding:12px 15px;border-bottom:1px solid #e2e8f0;">{line['unit_of_measure'] or 'EA'}</td>
                <td style="padding:12px 15px;border-bottom:1px solid #e2e8f0;text-align:right;">{line['quantity']}</td>
                <td style="padding:12px 15px;border-bottom:1px solid #e2e8f0;text-align:right;">${line['unit_price'] or 0:,.2f}</td>
                <td style="padding:12px 15px;border-bottom:1px solid #e2e8f0;text-align:right;">${line_total:,.2f}</td>
            </tr>
            """
        
        additional_msg_html = ""
        if additional_message:
            additional_msg_html = f"""
            <div style="background:#e0f2fe;border:1px solid #7dd3fc;border-radius:8px;padding:15px;margin-bottom:20px;">
                <div style="font-weight:600;color:#0369a1;margin-bottom:8px;">Message from Seller</div>
                <div style="color:#0c4a6e;">{additional_message}</div>
            </div>
            """
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head><meta charset="UTF-8"></head>
        <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:14px;color:#1e293b;margin:0;padding:0;">
            <div style="max-width:650px;margin:0 auto;background:#fff;">
                <div style="background:linear-gradient(135deg,#1e3a5f 0%,#0f172a 100%);color:white;padding:30px 40px;">
                    <table width="100%"><tr>
                        <td><h1 style="margin:0;font-size:24px;">{company.get('company_name', 'Dynamic.IQ-COREx')}</h1>
                        <p style="margin:5px 0 0 0;font-size:11px;opacity:0.9;">
                            {company.get('address_line1', '')}<br>
                            {company.get('city', '')}{ ', ' + company.get('state', '') if company.get('state') else ''} {company.get('postal_code', '')}
                        </p></td>
                        <td style="text-align:right;">
                            <h2 style="margin:0;font-size:20px;letter-spacing:2px;">ORDER ACKNOWLEDGEMENT</h2>
                            <div style="background:rgba(255,255,255,0.15);padding:6px 16px;border-radius:4px;display:inline-block;margin-top:8px;">{sales_order['so_number']}</div>
                        </td>
                    </tr></table>
                </div>
                <div style="padding:30px 40px;">
                    <p>Dear {sales_order['customer_name']},</p>
                    <p>Thank you for your order. We are pleased to confirm receipt and provide the following acknowledgement.</p>
                    {additional_msg_html}
                    <table style="width:100%;border-collapse:collapse;margin-bottom:25px;">
                        <thead><tr style="background:#1e3a5f;color:white;">
                            <th style="padding:12px 15px;text-align:left;font-size:10px;text-transform:uppercase;">Product</th>
                            <th style="padding:12px 15px;text-align:left;font-size:10px;text-transform:uppercase;">UOM</th>
                            <th style="padding:12px 15px;text-align:right;font-size:10px;text-transform:uppercase;">Qty</th>
                            <th style="padding:12px 15px;text-align:right;font-size:10px;text-transform:uppercase;">Unit Price</th>
                            <th style="padding:12px 15px;text-align:right;font-size:10px;text-transform:uppercase;">Total</th>
                        </tr></thead>
                        <tbody>{lines_html}</tbody>
                    </table>
                    <div style="text-align:right;margin-bottom:25px;">
                        <div style="display:inline-block;width:280px;border:2px solid #1e3a5f;border-radius:8px;overflow:hidden;">
                            <div style="display:flex;justify-content:space-between;padding:10px 15px;border-bottom:1px solid #e2e8f0;">
                                <span>Subtotal:</span><span>${sales_order['subtotal'] or 0:,.2f}</span>
                            </div>
                            <div style="display:flex;justify-content:space-between;padding:10px 15px;background:#1e3a5f;color:white;font-size:14px;font-weight:700;">
                                <span>Total:</span><span>${sales_order['total_amount'] or 0:,.2f}</span>
                            </div>
                        </div>
                    </div>
                    <p>If you have any questions regarding this order, please don't hesitate to contact us.</p>
                    <p style="margin-top:20px;">Thank you for your business!<br><br>Best regards,<br><strong>{company.get('company_name', 'Dynamic.IQ-COREx')}</strong></p>
                </div>
                <div style="background:#1e3a5f;color:rgba(255,255,255,0.8);padding:20px 40px;text-align:center;font-size:11px;">
                    {company.get('address_line1', '')} | {company.get('city', '')}{ ', ' + company.get('state', '') if company.get('state') else ''} {company.get('postal_code', '')}
                    {' | Phone: ' + company.get('phone', '') if company.get('phone') else ''} {' | ' + company.get('email', '') if company.get('email') else ''}
                </div>
            </div>
        </body>
        </html>
        """
        
        success, error = send_email_via_brevo(recipient_email, sales_order['customer_name'], subject, html_content, from_email, company.get('company_name', 'Dynamic.IQ-COREx'), api_key, cc_email or None)
        
        if success:
            flash(f'Email acknowledgement sent to {recipient_email}!', 'success')
        else:
            flash(f'Failed to send email: {error}', 'danger')
    else:
        flash('Order acknowledgement email skipped - no email service configured.', 'warning')
    
    if confirm_order_flag:
        try:
            line_count = conn.execute('''
                SELECT COUNT(*) as count FROM sales_order_lines WHERE so_id = ?
            ''', (id,)).fetchone()['count']
            
            if line_count == 0:
                flash('Cannot confirm order without line items.', 'warning')
                conn.close()
                return redirect(url_for('salesorder_routes.view_sales_order', id=id))
            
            # Check stock for non-core lines that are NOT already allocated
            product_totals = conn.execute('''
                SELECT sol.product_id, p.code, p.name, SUM(sol.quantity) as total_qty
                FROM sales_order_lines sol
                JOIN products p ON sol.product_id = p.id
                WHERE sol.so_id = ? 
                    AND (sol.is_core IS NULL OR sol.is_core = 0)
                    AND sol.inventory_id IS NULL
                    AND sol.work_order_id IS NULL
                GROUP BY sol.product_id, p.code, p.name
            ''', (id,)).fetchall()
            
            stock_issues = []
            for product in product_totals:
                inventory = conn.execute('''
                    SELECT COALESCE(quantity, 0) as available, COALESCE(reserved_quantity, 0) as reserved
                    FROM inventory WHERE product_id = ?
                ''', (product['product_id'],)).fetchone()
                
                available_qty = 0
                if inventory:
                    available_qty = inventory['available'] - inventory['reserved']
                
                if available_qty < product['total_qty']:
                    stock_issues.append(f"{product['code']}: Need {product['total_qty']}, Available {available_qty}")
            
            if stock_issues:
                flash('Cannot confirm - Insufficient stock: ' + '; '.join(stock_issues), 'danger')
                conn.close()
                return redirect(url_for('salesorder_routes.view_sales_order', id=id))
            
            so = conn.execute('SELECT customer_id, total_amount FROM sales_orders WHERE id = ?', (id,)).fetchone()
            customer = conn.execute('SELECT credit_limit, customer_number, name FROM customers WHERE id = ?', 
                                   (so['customer_id'],)).fetchone()
            
            outstanding = conn.execute('''
                SELECT COALESCE(SUM(balance_due), 0) as total_outstanding
                FROM sales_orders
                WHERE customer_id = ? AND status NOT IN ('Closed', 'Completed') AND id != ?
            ''', (so['customer_id'], id)).fetchone()['total_outstanding']
            
            if customer['credit_limit'] > 0 and (outstanding + so['total_amount']) > customer['credit_limit']:
                flash(f'Cannot confirm - Credit limit exceeded for {customer["customer_number"]}.', 'danger')
                conn.close()
                return redirect(url_for('salesorder_routes.view_sales_order', id=id))
            
            conn.execute('''
                UPDATE sales_orders SET status = 'Confirmed' WHERE id = ? AND status IN ('Draft', 'Pending')
            ''', (id,))
            conn.commit()
            flash('Sales Order confirmed successfully!', 'success')
            
        except Exception as e:
            conn.rollback()
            flash('An error occurred while confirming the order.', 'danger')
    
    conn.close()
    return redirect(url_for('salesorder_routes.view_sales_order', id=id))


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
        
        # VALIDATION: Check stock availability for non-core lines that are NOT already allocated
        # Skip lines with inventory_id or work_order_id (already sourced)
        product_totals = conn.execute('''
            SELECT 
                sol.product_id,
                p.code,
                p.name,
                SUM(sol.quantity) as total_qty
            FROM sales_order_lines sol
            JOIN products p ON sol.product_id = p.id
            WHERE sol.so_id = ? 
                AND (sol.is_core IS NULL OR sol.is_core = 0)
                AND sol.inventory_id IS NULL
                AND sol.work_order_id IS NULL
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
                   c.email, c.phone, c.billing_address, c.shipping_address
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
        cursor = conn.execute('''
            INSERT INTO shipments (
                shipment_number, shipment_type, reference_type, reference_id,
                status, shipment_stage, ship_to_name, ship_to_address,
                released_by, released_at, created_by, created_at
            ) VALUES (?, 'Outbound', 'Sales Order', ?, 'Pending', 'Pending',
                      ?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP)
        ''', (
            shipment_number, id,
            so['customer_name'], so['shipping_address'] or so['billing_address'],
            session.get('user_id'), session.get('user_id')
        ))
        
        shipment_id = cursor.lastrowid
        
        # Auto-populate shipment lines from sales order lines
        so_lines = conn.execute('''
            SELECT sol.*, p.code as product_code, p.name as product_name
            FROM sales_order_lines sol
            JOIN products p ON sol.product_id = p.id
            WHERE sol.so_id = ?
            ORDER BY sol.id
        ''', (id,)).fetchall()
        
        line_number = 0
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
                f"From SO Line {sol['id']}: {sol['product_code']} - {sol['product_name']}"
            ))
        
        # Update sales order status to Released to Shipping
        conn.execute('''
            UPDATE sales_orders 
            SET status = 'Released to Shipping'
            WHERE id = ?
        ''', (id,))
        
        # Log activity
        from models import AuditLogger
        AuditLogger.log_change(
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

@salesorder_bp.route('/sales-orders/<int:id>/delete', methods=['POST'])
@role_required('Admin', 'Planner')
def delete_sales_order(id):
    """Delete a Draft sales order"""
    db = Database()
    conn = db.get_connection()
    
    try:
        so = conn.execute('SELECT * FROM sales_orders WHERE id = ?', (id,)).fetchone()
        
        if not so:
            flash('Sales Order not found', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.list_sales_orders'))
        
        if so['status'] != 'Draft':
            flash('Only Draft orders can be deleted. Please cancel the order instead.', 'warning')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=id))
        
        so_number = so['so_number']
        
        conn.execute('DELETE FROM sales_order_lines WHERE so_id = ?', (id,))
        
        conn.execute('DELETE FROM sales_orders WHERE id = ?', (id,))
        
        from models import AuditLogger
        AuditLogger.log(
            conn=conn,
            record_type='sales_order',
            record_id=str(id),
            action_type='DELETE',
            modified_by=session.get('user_id'),
            changed_fields={'so_number': so_number, 'deleted': True}
        )
        
        conn.commit()
        flash(f'Sales Order {so_number} deleted successfully!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash('An error occurred while deleting the order.', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('salesorder_routes.list_sales_orders'))

@salesorder_bp.route('/sales-orders/lines/<int:line_id>/available-inventory', methods=['GET'])
@login_required
def get_available_inventory(line_id):
    """API endpoint to get available inventory for a sales order line"""
    db = Database()
    conn = db.get_connection()
    
    try:
        # Get line details
        line = conn.execute('''
            SELECT sol.*, p.code, p.name
            FROM sales_order_lines sol
            JOIN products p ON sol.product_id = p.id
            WHERE sol.id = ?
        ''', (line_id,)).fetchone()
        
        if not line:
            conn.close()
            return jsonify({'error': 'Line not found'}), 404
        
        # Get all available inventory for this product
        # Include items with Available/Serviceable status or null status that have available quantity
        # Exclude only Reserved and Out of Stock items
        inventory_items = conn.execute('''
            SELECT 
                i.id,
                i.quantity,
                i.warehouse_location,
                i.bin_location,
                i.condition,
                i.status,
                i.is_serialized,
                i.serial_number,
                COALESCE(i.reserved_quantity, 0) as reserved_quantity,
                (i.quantity - COALESCE(i.reserved_quantity, 0)) as available_qty
            FROM inventory i
            WHERE i.product_id = ?
            AND i.quantity > COALESCE(i.reserved_quantity, 0)
            AND (i.status IN ('Available', 'Serviceable') OR i.status IS NULL OR i.status = '')
            ORDER BY i.warehouse_location, i.bin_location
        ''', (line['product_id'],)).fetchall()
        
        # Check which serial numbers are already allocated
        allocated_serials = conn.execute('''
            SELECT serial_number 
            FROM sales_order_lines 
            WHERE serial_number IS NOT NULL 
            AND allocation_status IN ('Allocated', 'Partially Allocated')
            AND id != ?
        ''', (line_id,)).fetchall()
        allocated_serial_set = {row['serial_number'] for row in allocated_serials}
        
        result = []
        for inv in inventory_items:
            # Skip if serialized item's serial number is already allocated elsewhere
            # Only check for truly serialized items (is_serialized = 1)
            if inv['is_serialized'] and inv['serial_number'] and inv['serial_number'] in allocated_serial_set:
                continue
                
            result.append({
                'id': inv['id'],
                'quantity': inv['quantity'],
                'available_qty': inv['available_qty'],
                'warehouse_location': inv['warehouse_location'] or 'Main',
                'bin_location': inv['bin_location'] or '-',
                'condition': inv['condition'] or 'Serviceable',
                'is_serialized': bool(inv['is_serialized']),
                'serial_number': inv['serial_number']
            })
        
        conn.close()
        return jsonify({
            'line_id': line_id,
            'product_code': line['code'],
            'product_name': line['name'],
            'requested_qty': line['quantity'],
            'inventory': result
        })
        
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500

@salesorder_bp.route('/sales-orders/lines/<int:line_id>/allocate', methods=['POST'])
@role_required('Admin', 'Planner')
def allocate_line(line_id):
    """Allocate user-selected inventory to a sales order line"""
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
        
        # Get user-selected inventory ID and quantity
        inventory_id = request.form.get('inventory_id')
        allocate_qty_str = request.form.get('allocate_qty', '')
        serial_number = request.form.get('serial_number', '').strip() or None
        
        if not inventory_id:
            flash('Please select inventory to allocate.', 'warning')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
        
        # Get selected inventory
        inventory = conn.execute('''
            SELECT i.* 
            FROM inventory i
            WHERE i.id = ? AND i.product_id = ?
        ''', (inventory_id, line['product_id'])).fetchone()
        
        if not inventory:
            flash('Selected inventory not found or does not match the product.', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
        
        available_qty = inventory['quantity'] - (inventory['reserved_quantity'] or 0)
        requested_qty = line['quantity']
        
        # Determine allocation quantity
        if allocate_qty_str:
            try:
                allocate_qty = float(allocate_qty_str)
                if allocate_qty <= 0:
                    flash('Allocation quantity must be greater than 0.', 'danger')
                    conn.close()
                    return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
                if allocate_qty > available_qty:
                    flash(f'Cannot allocate {allocate_qty} units. Only {available_qty} available.', 'danger')
                    conn.close()
                    return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
            except ValueError:
                flash('Invalid allocation quantity.', 'danger')
                conn.close()
                return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
        else:
            # Default to min of available and requested
            allocate_qty = min(available_qty, requested_qty)
        
        # For serialized items, use the serial number from inventory
        if inventory['is_serialized']:
            serial_number = inventory['serial_number']
            
            # Check if this serial number is already allocated elsewhere
            existing_allocation = conn.execute('''
                SELECT sol.id, so.so_number, sol.line_number
                FROM sales_order_lines sol
                JOIN sales_orders so ON sol.so_id = so.id
                WHERE sol.serial_number = ? 
                AND sol.id != ?
                AND sol.allocation_status IN ('Allocated', 'Partially Allocated')
            ''', (serial_number, line_id)).fetchone()
            
            if existing_allocation:
                flash(f'Serial number {serial_number} is already allocated to SO {existing_allocation["so_number"]}, Line {existing_allocation["line_number"]}.', 'danger')
                conn.close()
                return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
        
        # Determine allocation status
        if allocate_qty >= requested_qty:
            allocation_status = 'Allocated'
            serial_msg = f' (S/N: {serial_number})' if serial_number else ''
            flash(f'Successfully allocated {allocate_qty} units of {line["code"]} from {inventory["warehouse_location"]}/{inventory["bin_location"] or "N/A"}{serial_msg}', 'success')
        else:
            allocation_status = 'Partially Allocated'
            flash(f'Partially allocated {allocate_qty} of {requested_qty} units for {line["code"]}. {requested_qty - allocate_qty} units still needed.', 'warning')
        
        # Update line with allocation
        conn.execute('''
            UPDATE sales_order_lines
            SET allocated_quantity = ?,
                allocation_status = ?,
                allocation_notes = ?,
                serial_number = ?,
                inventory_id = ?,
                modified_by = ?,
                modified_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (
            allocate_qty,
            allocation_status,
            f'Allocated from {inventory["warehouse_location"]}/{inventory["bin_location"] or "N/A"} (Inv ID: {inventory_id})',
            serial_number,
            inventory_id,
            session.get('user_id'),
            line_id
        ))
        
        # Update line status
        conn.execute('''
            UPDATE sales_order_lines
            SET line_status = ?
            WHERE id = ?
        ''', (allocation_status, line_id))
        
        # Update inventory status to reflect allocation
        # If fully allocating the available quantity, mark as Allocated
        new_reserved = (inventory['reserved_quantity'] or 0) + allocate_qty
        if new_reserved >= inventory['quantity']:
            conn.execute('''
                UPDATE inventory
                SET status = 'Allocated',
                    reserved_quantity = ?,
                    last_updated = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (new_reserved, inventory_id))
        else:
            # Partially reserved - update reserved_quantity but keep status
            conn.execute('''
                UPDATE inventory
                SET reserved_quantity = ?,
                    last_updated = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (new_reserved, inventory_id))
        
        # Log activity
        from models import AuditLogger
        changed_fields = {
            'allocated_quantity': allocate_qty, 
            'allocation_status': allocation_status,
            'inventory_id': inventory_id,
            'warehouse_location': inventory['warehouse_location']
        }
        if serial_number:
            changed_fields['serial_number'] = serial_number
        
        AuditLogger.log_change(
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

@salesorder_bp.route('/sales-orders/lines/<int:line_id>/deallocate', methods=['POST'])
@role_required('Admin', 'Planner')
def deallocate_line(line_id):
    """Deallocate inventory from a sales order line, returning it to available stock"""
    db = Database()
    conn = db.get_connection()
    so_id = None
    
    try:
        # Get line details
        line = conn.execute('''
            SELECT sol.*, p.code, p.name, so.so_number, so.id as so_id
            FROM sales_order_lines sol
            LEFT JOIN products p ON sol.product_id = p.id
            JOIN sales_orders so ON sol.so_id = so.id
            WHERE sol.id = ?
        ''', (line_id,)).fetchone()
        
        if not line:
            flash('Sales order line not found', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.list_sales_orders'))
        
        so_id = line['so_id']
        
        # Validation: Check if line has been released to shipping
        if line['released_to_shipping_at']:
            flash(f'Cannot deallocate line {line["line_number"]} - it has been released to shipping.', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
        
        # Validation: Check if line is allocated
        if line['allocation_status'] not in ['Allocated', 'Partially Allocated']:
            flash(f'Line {line["line_number"]} is not currently allocated.', 'warning')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
        
        allocated_qty = line['allocated_quantity'] or 0
        serial_number = line['serial_number']
        inventory_id = line['inventory_id']
        
        # Clear the allocation
        conn.execute('''
            UPDATE sales_order_lines
            SET allocated_quantity = 0,
                allocation_status = 'Pending',
                allocation_notes = 'Deallocated by user',
                serial_number = NULL,
                inventory_id = NULL,
                line_status = 'Pending',
                modified_by = ?,
                modified_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (session.get('user_id'), line_id))
        
        # Update inventory status back to Available
        if inventory_id:
            # Get current inventory reserved quantity
            inv = conn.execute('SELECT quantity, reserved_quantity FROM inventory WHERE id = ?', (inventory_id,)).fetchone()
            if inv:
                new_reserved = max(0, (inv['reserved_quantity'] or 0) - allocated_qty)
                # If no more reserved quantity, set status back to Available
                new_status = 'Available' if new_reserved == 0 else None
                if new_status:
                    conn.execute('''
                        UPDATE inventory
                        SET status = 'Available',
                            reserved_quantity = ?,
                            last_updated = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (new_reserved, inventory_id))
                else:
                    conn.execute('''
                        UPDATE inventory
                        SET reserved_quantity = ?,
                            last_updated = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (new_reserved, inventory_id))
        
        # Log activity
        from models import AuditLogger
        changed_fields = {
            'allocated_quantity': f'{allocated_qty} -> 0', 
            'allocation_status': f'{line["allocation_status"]} -> Pending',
            'action': 'Deallocated'
        }
        if serial_number:
            changed_fields['serial_number'] = f'{serial_number} -> NULL'
        
        AuditLogger.log_change(
            conn=conn,
            record_type='sales_order_lines',
            record_id=line_id,
            action_type='UPDATE',
            modified_by=session.get('user_id'),
            changed_fields=changed_fields
        )
        
        # Check for linked Exchange PO and cancel it
        cancelled_po_number = None
        linked_exchange_po = conn.execute('''
            SELECT po.id, po.po_number, po.status
            FROM purchase_orders po
            JOIN purchase_order_lines pol ON pol.po_id = po.id
            WHERE pol.source_so_line_id = ?
            AND po.is_exchange = 1
            AND po.status NOT IN ('Cancelled', 'Received', 'Closed')
        ''', (line_id,)).fetchone()
        
        if linked_exchange_po:
            # Cancel the Exchange PO
            conn.execute('''
                UPDATE purchase_orders
                SET status = 'Cancelled',
                    exchange_status = 'Cancelled - Deallocated',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (linked_exchange_po['id'],))
            
            cancelled_po_number = linked_exchange_po['po_number']
            
            # Log the Exchange PO cancellation
            AuditLogger.log_change(
                conn=conn,
                record_type='purchase_orders',
                record_id=linked_exchange_po['id'],
                action_type='UPDATE',
                modified_by=session.get('user_id'),
                changed_fields={
                    'status': f"{linked_exchange_po['status']} -> Cancelled",
                    'exchange_status': 'Cancelled - Deallocated',
                    'action': 'Auto-cancelled due to SO line deallocation',
                    'source_so_line_id': line_id
                }
            )
        
        conn.commit()
        
        serial_msg = f' (S/N: {serial_number})' if serial_number else ''
        po_msg = f' Linked Exchange PO {cancelled_po_number} was automatically cancelled.' if cancelled_po_number else ''
        flash(f'Successfully deallocated {allocated_qty} units of {line["code"]} from line {line["line_number"]}{serial_msg}.{po_msg} A new Exchange PO can now be created.', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'An error occurred during deallocation: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))

@salesorder_bp.route('/sales-orders/lines/<int:line_id>/available-work-orders')
@role_required('Admin', 'Planner')
def get_available_work_orders_for_line(line_id):
    """Get available work orders that can be allocated to a sales order line"""
    db = Database()
    conn = db.get_connection()
    
    try:
        line = conn.execute('''
            SELECT sol.*, p.code, p.name
            FROM sales_order_lines sol
            JOIN products p ON sol.product_id = p.id
            WHERE sol.id = ?
        ''', (line_id,)).fetchone()
        
        if not line:
            conn.close()
            return jsonify({'error': 'Line not found'}), 404
        
        work_orders = conn.execute('''
            SELECT wo.id, wo.wo_number, wo.quantity, wo.status, wo.priority,
                   wo.planned_start_date, wo.planned_end_date, wo.disposition,
                   p.code as product_code, p.name as product_name,
                   COALESCE(wo.material_cost, 0) + COALESCE(wo.labor_cost, 0) as total_cost
            FROM work_orders wo
            JOIN products p ON wo.product_id = p.id
            WHERE wo.product_id = ?
            AND wo.status IN ('Open', 'In Progress', 'Completed')
            AND wo.so_id IS NULL
            AND NOT EXISTS (
                SELECT 1 FROM sales_order_lines sol2 
                WHERE sol2.work_order_id = wo.id
            )
            ORDER BY wo.status DESC, wo.planned_end_date ASC
        ''', (line['product_id'],)).fetchall()
        
        result = []
        for wo in work_orders:
            result.append({
                'id': wo['id'],
                'wo_number': wo['wo_number'],
                'quantity': wo['quantity'],
                'status': wo['status'],
                'priority': wo['priority'],
                'planned_start_date': wo['planned_start_date'],
                'planned_end_date': wo['planned_end_date'],
                'disposition': wo['disposition'],
                'product_code': wo['product_code'],
                'total_cost': wo['total_cost']
            })
        
        conn.close()
        return jsonify({
            'line_id': line_id,
            'product_code': line['code'],
            'product_name': line['name'],
            'requested_qty': line['quantity'],
            'work_orders': result
        })
        
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500

@salesorder_bp.route('/sales-orders/lines/<int:line_id>/allocate-work-order', methods=['POST'])
@role_required('Admin', 'Planner')
def allocate_work_order_to_line(line_id):
    """Allocate a work order to a sales order line"""
    db = Database()
    conn = db.get_connection()
    so_id = None
    
    try:
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
        
        if line['is_core']:
            flash('Core items cannot have work orders allocated.', 'info')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
        
        work_order_id = request.form.get('work_order_id')
        
        if not work_order_id:
            flash('Please select a work order to allocate.', 'warning')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
        
        wo = conn.execute('''
            SELECT wo.*, p.code as product_code
            FROM work_orders wo
            JOIN products p ON wo.product_id = p.id
            WHERE wo.id = ? AND wo.product_id = ?
        ''', (work_order_id, line['product_id'])).fetchone()
        
        if not wo:
            flash('Selected work order not found or does not match the product.', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
        
        if wo['so_id'] is not None:
            flash('This work order is already linked to a different sales order.', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
        
        existing = conn.execute('''
            SELECT sol.id, so.so_number, sol.line_number
            FROM sales_order_lines sol
            JOIN sales_orders so ON sol.so_id = so.id
            WHERE sol.work_order_id = ? AND sol.id != ?
        ''', (work_order_id, line_id)).fetchone()
        
        if existing:
            flash(f'Work order {wo["wo_number"]} is already allocated to SO {existing["so_number"]}, Line {existing["line_number"]}.', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
        
        allocate_qty = min(wo['quantity'], line['quantity'])
        allocation_status = 'Allocated' if allocate_qty >= line['quantity'] else 'Partially Allocated'
        
        conn.execute('''
            UPDATE sales_order_lines
            SET work_order_id = ?,
                allocated_quantity = ?,
                allocation_status = ?,
                allocation_notes = ?,
                line_status = ?,
                modified_by = ?,
                modified_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (
            work_order_id,
            allocate_qty,
            allocation_status,
            f'Allocated from WO {wo["wo_number"]} (Status: {wo["status"]})',
            allocation_status,
            session.get('user_id'),
            line_id
        ))
        
        conn.execute('''
            UPDATE work_orders
            SET so_id = ?
            WHERE id = ?
        ''', (so_id, work_order_id))
        
        from models import AuditLogger
        AuditLogger.log_change(
            conn=conn,
            record_type='sales_order_lines',
            record_id=line_id,
            action_type='UPDATE',
            modified_by=session.get('user_id'),
            changed_fields={
                'work_order_id': work_order_id,
                'wo_number': wo['wo_number'],
                'allocated_quantity': allocate_qty,
                'allocation_status': allocation_status
            }
        )
        
        conn.commit()
        flash(f'Successfully allocated work order {wo["wo_number"]} ({allocate_qty} units) to line {line["line_number"]}.', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'An error occurred during work order allocation: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))

@salesorder_bp.route('/sales-orders/lines/<int:line_id>/deallocate-work-order', methods=['POST'])
@role_required('Admin', 'Planner')
def deallocate_work_order_from_line(line_id):
    """Deallocate a work order from a sales order line"""
    db = Database()
    conn = db.get_connection()
    so_id = None
    
    try:
        line = conn.execute('''
            SELECT sol.*, p.code, p.name, so.so_number, so.id as so_id,
                   wo.wo_number
            FROM sales_order_lines sol
            JOIN products p ON sol.product_id = p.id
            JOIN sales_orders so ON sol.so_id = so.id
            LEFT JOIN work_orders wo ON sol.work_order_id = wo.id
            WHERE sol.id = ?
        ''', (line_id,)).fetchone()
        
        if not line:
            flash('Sales order line not found', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.list_sales_orders'))
        
        so_id = line['so_id']
        
        if line['released_to_shipping_at']:
            flash(f'Cannot deallocate line {line["line_number"]} - it has been released to shipping.', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
        
        if not line['work_order_id']:
            flash(f'Line {line["line_number"]} does not have a work order allocated.', 'warning')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
        
        wo_number = line['wo_number']
        work_order_id = line['work_order_id']
        
        conn.execute('''
            UPDATE sales_order_lines
            SET work_order_id = NULL,
                allocated_quantity = 0,
                allocation_status = 'Pending',
                allocation_notes = 'Work order deallocated by user',
                line_status = 'Pending',
                modified_by = ?,
                modified_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (session.get('user_id'), line_id))
        
        conn.execute('''
            UPDATE work_orders
            SET so_id = NULL
            WHERE id = ?
        ''', (work_order_id,))
        
        from models import AuditLogger
        AuditLogger.log_change(
            conn=conn,
            record_type='sales_order_lines',
            record_id=line_id,
            action_type='UPDATE',
            modified_by=session.get('user_id'),
            changed_fields={
                'work_order_id': f'{work_order_id} -> NULL',
                'wo_number': wo_number,
                'allocation_status': 'Pending',
                'action': 'Work Order Deallocated'
            }
        )
        
        conn.commit()
        flash(f'Successfully deallocated work order {wo_number} from line {line["line_number"]}.', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'An error occurred during work order deallocation: {str(e)}', 'danger')
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
        from models import AuditLogger
        AuditLogger.log_change(
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


@salesorder_bp.route('/sales-orders/<int:id>/create-exchange-po', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def create_exchange_po(id):
    """Create Exchange Fee Purchase Order from Dual Exchange Sales Order"""
    db = Database()
    conn = db.get_connection()
    
    try:
        # Get sales order with exchange type validation
        sales_order = conn.execute('''
            SELECT so.*, c.name as customer_name, c.customer_number
            FROM sales_orders so
            JOIN customers c ON so.customer_id = c.id
            WHERE so.id = ?
        ''', (id,)).fetchone()
        
        if not sales_order:
            flash('Sales order not found', 'danger')
            conn.close()
            return redirect(url_for('salesorder_routes.list_sales_orders'))
        
        # Validate exchange type is Dual Exchange
        if sales_order['sales_type'] != 'Exchange' or sales_order['exchange_type'] != 'Dual Exchange':
            flash('Exchange Purchase Orders can only be created for Dual Exchange sales orders', 'warning')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=id))
        
        # Check if active Exchange PO already exists for this SO (exclude cancelled ones)
        existing_po = conn.execute('''
            SELECT po_number FROM purchase_orders 
            WHERE source_sales_order_id = ? AND is_exchange = 1
            AND status NOT IN ('Cancelled')
        ''', (id,)).fetchone()
        
        if existing_po:
            flash(f'An active Exchange PO ({existing_po["po_number"]}) already exists for this Sales Order', 'warning')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=id))
        
        # Get sales order lines for the exchange
        lines = conn.execute('''
            SELECT sol.*, p.code as product_code, p.name as product_name, p.description as product_description
            FROM sales_order_lines sol
            JOIN products p ON sol.product_id = p.id
            WHERE sol.so_id = ?
            ORDER BY sol.line_number
        ''', (id,)).fetchall()
        
        if not lines:
            flash('No line items found for this sales order', 'warning')
            conn.close()
            return redirect(url_for('salesorder_routes.view_sales_order', id=id))
        
        # Get suppliers and customers for owner selection
        suppliers = conn.execute('SELECT id, code, name FROM suppliers ORDER BY name').fetchall()
        customers = conn.execute('SELECT id, customer_number, name FROM customers ORDER BY name').fetchall()
        
        if request.method == 'POST':
            # Validate required fields
            owner_type = request.form.get('exchange_owner_type')
            owner_id = request.form.get('exchange_owner_id')
            supplier_id = request.form.get('supplier_id')
            
            if not owner_type or not owner_id:
                flash('Please select the owner of the exchanged unit', 'danger')
                return render_template('salesorders/create_exchange_po.html',
                    sales_order=sales_order, lines=lines, suppliers=suppliers, customers=customers)
            
            if not supplier_id:
                flash('Please select a supplier for the Purchase Order', 'danger')
                return render_template('salesorders/create_exchange_po.html',
                    sales_order=sales_order, lines=lines, suppliers=suppliers, customers=customers)
            
            # Validate owner exists based on owner_type
            if owner_type == 'Customer':
                owner_exists = conn.execute('SELECT id FROM customers WHERE id = ?', (owner_id,)).fetchone()
            elif owner_type == 'Supplier':
                owner_exists = conn.execute('SELECT id FROM suppliers WHERE id = ?', (owner_id,)).fetchone()
            else:
                owner_exists = None
            
            if not owner_exists:
                flash('Invalid exchange owner selection. Please select a valid customer or supplier.', 'danger')
                return render_template('salesorders/create_exchange_po.html',
                    sales_order=sales_order, lines=lines, suppliers=suppliers, customers=customers)
            
            # Re-check for existing active Exchange PO within transaction to prevent race conditions
            existing_po_recheck = conn.execute('''
                SELECT po_number FROM purchase_orders 
                WHERE source_sales_order_id = ? AND is_exchange = 1
                AND status NOT IN ('Cancelled')
            ''', (id,)).fetchone()
            
            if existing_po_recheck:
                flash(f'An active Exchange PO ({existing_po_recheck["po_number"]}) already exists for this Sales Order', 'warning')
                conn.close()
                return redirect(url_for('salesorder_routes.view_sales_order', id=id))
            
            # Generate PO number
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
            
            # Generate unique exchange reference ID
            import uuid
            exchange_reference_id = f'EXC-{uuid.uuid4().hex[:8].upper()}'
            
            # Create Exchange Fee Purchase Order
            cursor = conn.execute('''
                INSERT INTO purchase_orders (
                    po_number, supplier_id, status, order_date, notes,
                    po_type, is_exchange, exchange_owner_type, exchange_owner_id,
                    exchange_reference_id, source_sales_order_id, exchange_status,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                po_number, supplier_id, 'Draft', datetime.now().strftime('%Y-%m-%d'),
                f'Exchange Fee PO for Sales Order {sales_order["so_number"]}',
                'Exchange Fee', 1, owner_type, int(owner_id),
                exchange_reference_id, id, 'Open'
            ))
            po_id = cursor.lastrowid
            
            # Get or create non-inventory Exchange Fee product
            exchange_fee_product = conn.execute('''
                SELECT id FROM products WHERE code = 'EXCHANGE-FEE' AND product_type = 'Non-Inventory'
            ''').fetchone()
            
            if not exchange_fee_product:
                # Create the Exchange Fee non-inventory product
                cursor2 = conn.execute('''
                    INSERT INTO products (code, name, description, unit_of_measure, product_type, cost)
                    VALUES ('EXCHANGE-FEE', 'Exchange Fee', 'Non-inventory item for exchange fee charges', 'EA', 'Non-Inventory', 0)
                ''')
                exchange_fee_product_id = cursor2.lastrowid
            else:
                exchange_fee_product_id = exchange_fee_product['id']
            
            # Create Exchange Fee line items with Part Number and Serial Number references
            for idx, line in enumerate(lines, 1):
                # Get exchange fee from form
                exchange_fee = float(request.form.get(f'exchange_fee_{line["id"]}', line['unit_price'] or 0))
                
                # Get Part Number and Serial Number from sales order line
                part_number = line['product_code'] or 'N/A'
                serial_number = line['serial_number'] if line['serial_number'] else ''
                
                # Create description with Exchange Part Number and Serial Number
                line_description = f"Exchange Fee for P/N: {part_number}"
                if serial_number:
                    line_description += f", S/N: {serial_number}"
                
                # Use non-inventory Exchange Fee product (not the actual exchanged part)
                conn.execute('''
                    INSERT INTO purchase_order_lines (
                        po_id, line_number, product_id, quantity, unit_price,
                        description, exchange_fee_flag, source_so_line_id,
                        reference_part_number, reference_serial_number,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    po_id, idx, exchange_fee_product_id, 1, exchange_fee,
                    line_description, 1, line['id'],
                    part_number, serial_number
                ))
            
            # Log to audit trail
            from models import AuditLogger
            AuditLogger.log_change(
                conn=conn,
                record_type='purchase_orders',
                record_id=po_id,
                action_type='CREATE',
                modified_by=session.get('user_id'),
                changed_fields={
                    'action': 'Exchange PO Created',
                    'source_sales_order': sales_order['so_number'],
                    'exchange_reference_id': exchange_reference_id,
                    'exchange_owner_type': owner_type,
                    'po_type': 'Exchange Fee'
                }
            )
            
            conn.commit()
            flash(f'Exchange Purchase Order {po_number} created successfully', 'success')
            conn.close()
            return redirect(url_for('po_routes.view_purchaseorder', id=po_id))
        
        conn.close()
        return render_template('salesorders/create_exchange_po.html',
            sales_order=sales_order, lines=lines, suppliers=suppliers, customers=customers)
    
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error creating Exchange PO: {str(e)}', 'danger')
        return redirect(url_for('salesorder_routes.view_sales_order', id=id))


@salesorder_bp.route('/api/exchange-owner-details/<owner_type>/<int:owner_id>')
@login_required
def get_exchange_owner_details(owner_type, owner_id):
    """API to get owner details for exchange selection"""
    db = Database()
    conn = db.get_connection()
    
    try:
        if owner_type == 'Customer':
            owner = conn.execute('''
                SELECT id, customer_number as code, name, email, phone
                FROM customers WHERE id = ?
            ''', (owner_id,)).fetchone()
        elif owner_type == 'Supplier':
            owner = conn.execute('''
                SELECT id, code, name, email, phone
                FROM suppliers WHERE id = ?
            ''', (owner_id,)).fetchone()
        else:
            return jsonify({'error': 'Invalid owner type'}), 400
        
        if owner:
            return jsonify(dict(owner))
        return jsonify({'error': 'Owner not found'}), 404
    finally:
        conn.close()


@salesorder_bp.route('/exchanges-report')
@login_required
def exchanges_report():
    """Comprehensive exchanges report grouped by part number and owner obligations"""
    db = Database()
    conn = db.get_connection()
    
    try:
        exchanges_by_part = conn.execute('''
            SELECT 
                p.code as part_number,
                p.name as part_name,
                COUNT(DISTINCT so.id) as exchange_count,
                SUM(sol.quantity) as total_quantity,
                SUM(sol.line_total) as total_value,
                GROUP_CONCAT(DISTINCT so.so_number) as so_numbers
            FROM sales_orders so
            JOIN sales_order_lines sol ON so.id = sol.so_id
            JOIN products p ON sol.product_id = p.id
            WHERE so.sales_type = 'Exchange'
            GROUP BY p.id
            ORDER BY exchange_count DESC, p.code
        ''').fetchall()
        
        customer_owed = conn.execute('''
            SELECT 
                po.po_number,
                po.exchange_reference_id,
                po.exchange_status,
                po.order_date,
                po.expected_date,
                po.total_amount,
                so.so_number as source_so,
                c.name as owner_name,
                c.customer_number as owner_code,
                p.code as part_number,
                p.name as part_name,
                sol.quantity,
                CASE 
                    WHEN po.expected_date < date('now') AND po.exchange_status != 'Received' 
                    THEN julianday('now') - julianday(po.expected_date)
                    ELSE 0 
                END as days_overdue
            FROM purchase_orders po
            JOIN sales_orders so ON po.source_sales_order_id = so.id
            JOIN customers c ON po.exchange_owner_id = c.id
            LEFT JOIN sales_order_lines sol ON so.id = sol.so_id
            LEFT JOIN products p ON sol.product_id = p.id
            WHERE po.is_exchange = 1 
              AND po.exchange_owner_type = 'Customer'
            ORDER BY days_overdue DESC, po.order_date DESC
        ''').fetchall()
        
        supplier_owed = conn.execute('''
            SELECT 
                po.po_number,
                po.exchange_reference_id,
                po.exchange_status,
                po.order_date,
                po.expected_date,
                po.total_amount,
                so.so_number as source_so,
                s.name as owner_name,
                s.code as owner_code,
                p.code as part_number,
                p.name as part_name,
                sol.quantity,
                CASE 
                    WHEN po.expected_date < date('now') AND po.exchange_status != 'Received' 
                    THEN julianday('now') - julianday(po.expected_date)
                    ELSE 0 
                END as days_overdue
            FROM purchase_orders po
            JOIN sales_orders so ON po.source_sales_order_id = so.id
            JOIN suppliers s ON po.exchange_owner_id = s.id
            LEFT JOIN sales_order_lines sol ON so.id = sol.so_id
            LEFT JOIN products p ON sol.product_id = p.id
            WHERE po.is_exchange = 1 
              AND po.exchange_owner_type = 'Supplier'
            ORDER BY days_overdue DESC, po.order_date DESC
        ''').fetchall()
        
        all_exchanges = conn.execute('''
            SELECT 
                so.id,
                so.so_number,
                so.order_date,
                so.status,
                so.exchange_type,
                so.total_amount,
                c.name as customer_name,
                c.customer_number,
                p.code as part_number,
                p.name as part_name,
                sol.quantity,
                sol.unit_price,
                sol.line_total
            FROM sales_orders so
            JOIN customers c ON so.customer_id = c.id
            LEFT JOIN sales_order_lines sol ON so.id = sol.so_id
            LEFT JOIN products p ON sol.product_id = p.id
            WHERE so.sales_type = 'Exchange'
            ORDER BY so.order_date DESC, so.id DESC
        ''').fetchall()
        
        stats = {
            'total_exchanges': len(set(e['so_number'] for e in all_exchanges)) if all_exchanges else 0,
            'customer_owed_count': len(customer_owed),
            'supplier_owed_count': len(supplier_owed),
            'overdue_customer': sum(1 for e in customer_owed if e['days_overdue'] and e['days_overdue'] > 0),
            'overdue_supplier': sum(1 for e in supplier_owed if e['days_overdue'] and e['days_overdue'] > 0),
            'total_parts': len(exchanges_by_part)
        }
        
        conn.close()
        return render_template('salesorders/exchanges_report.html',
                             exchanges_by_part=exchanges_by_part,
                             customer_owed=customer_owed,
                             supplier_owed=supplier_owed,
                             all_exchanges=all_exchanges,
                             stats=stats)
    except Exception as e:
        conn.close()
        flash(f'Error generating exchanges report: {str(e)}', 'danger')
        return redirect(url_for('salesorder_routes.list_sales_orders'))


@salesorder_bp.route('/sales-orders/<int:id>/print')
@login_required
def print_sales_order(id):
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
    
    lines = conn.execute('''
        SELECT sol.*, p.code, p.name as product_name, p.unit_of_measure, p.is_serialized
        FROM sales_order_lines sol
        JOIN products p ON sol.product_id = p.id
        WHERE sol.so_id = ?
        ORDER BY sol.line_number
    ''', (id,)).fetchall()
    
    company_settings = conn.execute('SELECT * FROM company_settings WHERE id = 1').fetchone()
    
    conn.close()
    
    return render_template('salesorders/print.html', 
                         sales_order=sales_order, 
                         lines=lines, 
                         company_settings=company_settings or {},
                         now=datetime.now())


@salesorder_bp.route('/sales-orders/<int:id>/pdf')
@login_required
def download_pdf(id):
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
    
    lines = conn.execute('''
        SELECT sol.*, p.code, p.name as product_name, p.unit_of_measure
        FROM sales_order_lines sol
        JOIN products p ON sol.product_id = p.id
        WHERE sol.so_id = ?
        ORDER BY sol.line_number
    ''', (id,)).fetchall()
    
    company_settings = conn.execute('SELECT * FROM company_settings WHERE id = 1').fetchone()
    
    conn.close()
    
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch, leftMargin=0.5*inch, rightMargin=0.5*inch)
    story = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle(
        'SOTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#1e3a5f'),
        spaceAfter=5,
        alignment=TA_CENTER
    )
    
    company_style = ParagraphStyle(
        'Company',
        parent=styles['Normal'],
        fontSize=12,
        textColor=colors.HexColor('#1e3a5f'),
        alignment=TA_CENTER,
        spaceAfter=5
    )
    
    header_style = ParagraphStyle(
        'Header',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#475569'),
        alignment=TA_CENTER
    )
    
    if company_settings and company_settings['company_name']:
        story.append(Paragraph(company_settings['company_name'], company_style))
        addr_parts = []
        if company_settings['address_line1']:
            addr_parts.append(company_settings['address_line1'])
        if company_settings['city']:
            city_line = company_settings['city']
            if company_settings['state']:
                city_line += f", {company_settings['state']}"
            if company_settings['postal_code']:
                city_line += f" {company_settings['postal_code']}"
            addr_parts.append(city_line)
        if addr_parts:
            story.append(Paragraph(' | '.join(addr_parts), header_style))
        story.append(Spacer(1, 0.2*inch))
    
    story.append(Paragraph("SALES ORDER", title_style))
    story.append(Spacer(1, 0.1*inch))
    
    so_num_style = ParagraphStyle(
        'SONum',
        parent=styles['Normal'],
        fontSize=14,
        textColor=colors.HexColor('#f97316'),
        alignment=TA_CENTER,
        spaceAfter=20
    )
    story.append(Paragraph(sales_order['so_number'], so_num_style))
    story.append(Spacer(1, 0.2*inch))
    
    info_data = [
        ['Order Date:', sales_order['order_date'], 'Customer:', sales_order['customer_name']],
        ['Sales Type:', sales_order['sales_type'], 'Account #:', sales_order['customer_number'] or '-'],
        ['Status:', sales_order['status'], 'Expected Ship:', sales_order['expected_ship_date'] or 'TBD'],
    ]
    
    if sales_order['exchange_type']:
        info_data.append(['Exchange Type:', sales_order['exchange_type'], '', ''])
    
    info_table = Table(info_data, colWidths=[1.3*inch, 2*inch, 1.3*inch, 2.5*inch])
    info_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#64748b')),
        ('TEXTCOLOR', (2, 0), (2, -1), colors.HexColor('#64748b')),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
    ]))
    story.append(info_table)
    story.append(Spacer(1, 0.3*inch))
    
    if sales_order['billing_address'] or sales_order['shipping_address']:
        addr_data = [['Bill To:', 'Ship To:']]
        addr_data.append([
            sales_order['billing_address'] or '-',
            sales_order['shipping_address'] or '-'
        ])
        addr_table = Table(addr_data, colWidths=[3.5*inch, 3.5*inch])
        addr_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#64748b')),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ]))
        story.append(addr_table)
        story.append(Spacer(1, 0.2*inch))
    
    line_data = [['Line', 'Product Code', 'Description', 'UOM', 'Qty', 'Unit Price', 'Total']]
    for line in lines:
        line_dict = dict(line)
        line_total = (line_dict.get('quantity') or 0) * (line_dict.get('unit_price') or 0)
        desc = line_dict.get('product_name', '')
        serial_num = line_dict.get('serial_number')
        lot_num = line_dict.get('lot_number')
        if serial_num:
            desc += f"\nS/N: {serial_num}"
        if lot_num:
            desc += f"\nLot: {lot_num}"
        line_data.append([
            str(line_dict.get('line_number', '')),
            line_dict.get('code', ''),
            Paragraph(desc, styles['Normal']),
            line_dict.get('unit_of_measure') or 'EA',
            str(line_dict.get('quantity', '')),
            f"${line_dict.get('unit_price', 0):,.2f}" if line_dict.get('unit_price') else '$0.00',
            f"${line_total:,.2f}"
        ])
    
    lines_table = Table(line_data, colWidths=[0.5*inch, 1.2*inch, 2.3*inch, 0.6*inch, 0.5*inch, 1*inch, 1*inch])
    lines_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1e3a5f')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('ALIGN', (0, 1), (0, -1), 'CENTER'),
        ('ALIGN', (4, 1), (-1, -1), 'RIGHT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('FONTSIZE', (0, 1), (-1, -1), 9),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
        ('TOPPADDING', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
        ('TOPPADDING', (0, 1), (-1, -1), 6),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e2e8f0')),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))
    story.append(lines_table)
    story.append(Spacer(1, 0.2*inch))
    
    totals_data = [
        ['', '', '', '', '', 'Subtotal:', f"${sales_order['subtotal']:,.2f}" if sales_order['subtotal'] else '$0.00'],
    ]
    
    if sales_order['tax_rate'] and sales_order['tax_rate'] > 0:
        totals_data.append(['', '', '', '', '', f"Tax ({sales_order['tax_rate']}%):", f"${sales_order['tax_amount']:,.2f}" if sales_order['tax_amount'] else '$0.00'])
    
    totals_data.append(['', '', '', '', '', 'TOTAL:', f"${sales_order['total_amount']:,.2f}" if sales_order['total_amount'] else '$0.00'])
    
    totals_table = Table(totals_data, colWidths=[0.5*inch, 1.2*inch, 2.3*inch, 0.6*inch, 0.5*inch, 1*inch, 1*inch])
    totals_table.setStyle(TableStyle([
        ('ALIGN', (5, 0), (-1, -1), 'RIGHT'),
        ('FONTNAME', (5, 0), (5, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('FONTNAME', (5, -1), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (5, -1), (-1, -1), 11),
        ('TEXTCOLOR', (5, -1), (-1, -1), colors.HexColor('#1e3a5f')),
        ('LINEABOVE', (5, -1), (-1, -1), 2, colors.HexColor('#1e3a5f')),
    ]))
    story.append(totals_table)
    
    if sales_order['service_notes']:
        story.append(Spacer(1, 0.3*inch))
        notes_style = ParagraphStyle('Notes', parent=styles['Normal'], fontSize=9)
        story.append(Paragraph(f"<b>Service Notes:</b> {sales_order['service_notes']}", notes_style))
    
    if sales_order['notes']:
        story.append(Spacer(1, 0.2*inch))
        notes_style = ParagraphStyle('Notes', parent=styles['Normal'], fontSize=9)
        story.append(Paragraph(f"<b>Order Notes:</b> {sales_order['notes']}", notes_style))
    
    story.append(Spacer(1, 0.4*inch))
    footer_style = ParagraphStyle('Footer', parent=styles['Normal'], fontSize=8, textColor=colors.HexColor('#94a3b8'))
    story.append(Paragraph(f"Generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}", footer_style))
    
    doc.build(story)
    buffer.seek(0)
    
    return send_file(
        buffer,
        mimetype='application/pdf',
        as_attachment=True,
        download_name=f"SalesOrder_{sales_order['so_number']}.pdf"
    )
