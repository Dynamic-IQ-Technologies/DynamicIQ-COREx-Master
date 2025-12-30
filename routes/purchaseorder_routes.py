from flask import Blueprint, render_template, request, redirect, url_for, flash, make_response, session, jsonify
from models import Database, CompanySettings, AuditLogger
from mrp_logic import MRPEngine
from auth import login_required, role_required
from datetime import datetime, timedelta
from utils.uom_conversion import (
    validate_po_line_conversion, check_conversion_defined, 
    VarianceType, ConversionResult, get_product_uom_options
)
import json
import secrets
import os


def get_brevo_credentials():
    """Get Brevo API key and from email from environment"""
    api_key = os.environ.get('BREVO_API_KEY')
    from_email = os.environ.get('BREVO_FROM_EMAIL')
    return api_key, from_email


def send_po_email_via_brevo(to_email, to_name, subject, html_content, from_email, from_name, api_key):
    """Send PO email using Brevo (Sendinblue) API"""
    import sib_api_v3_sdk
    from sib_api_v3_sdk.rest import ApiException
    
    configuration = sib_api_v3_sdk.Configuration()
    configuration.api_key['api-key'] = api_key
    
    api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))
    
    send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(
        to=[{"email": to_email, "name": to_name}],
        sender={"email": from_email, "name": from_name},
        subject=subject,
        html_content=html_content
    )
    
    try:
        api_response = api_instance.send_transac_email(send_smtp_email)
        if api_response and hasattr(api_response, 'message_id') and api_response.message_id:
            return True, str(api_response.message_id)
        return True, 'Email sent successfully'
    except ApiException as e:
        return False, str(e)
    except Exception as e:
        return False, str(e)

po_bp = Blueprint('po_routes', __name__)

# UOM Conversion Helper Functions
def get_product_uom_conversion(conn, product_id, uom_id):
    """
    Get the conversion factor for a specific product-UOM combination.
    Returns: (conversion_factor, base_uom_id, base_uom_code) or (None, None, None) if not found
    """
    # First check if there's a product-specific conversion
    product_conversion = conn.execute('''
        SELECT puc.conversion_factor, puc.is_base_uom, u.id as uom_id, u.uom_code,
               bu.id as base_uom_id, bu.uom_code as base_uom_code
        FROM product_uom_conversions puc
        JOIN uom_master u ON puc.uom_id = u.id
        LEFT JOIN uom_master bu ON u.base_uom_id = bu.id
        WHERE puc.product_id = ? AND puc.uom_id = ?
    ''', (product_id, uom_id)).fetchone()
    
    if product_conversion:
        if product_conversion['is_base_uom']:
            # This is the base UOM for the product
            return (1.0, product_conversion['uom_id'], product_conversion['uom_code'])
        else:
            # Use product-specific conversion factor
            base_uom_id = product_conversion['base_uom_id'] if product_conversion['base_uom_id'] else product_conversion['uom_id']
            base_uom_code = product_conversion['base_uom_code'] if product_conversion['base_uom_code'] else product_conversion['uom_code']
            return (product_conversion['conversion_factor'], base_uom_id, base_uom_code)
    
    # Fall back to standard UOM conversion if no product-specific conversion exists
    uom_info = conn.execute('''
        SELECT u.id, u.uom_code, u.conversion_factor, u.base_uom_id,
               bu.id as base_id, bu.uom_code as base_code
        FROM uom_master u
        LEFT JOIN uom_master bu ON u.base_uom_id = bu.id
        WHERE u.id = ?
    ''', (uom_id,)).fetchone()
    
    if uom_info:
        if uom_info['base_uom_id']:
            return (uom_info['conversion_factor'], uom_info['base_id'], uom_info['base_code'])
        else:
            # This UOM is itself a base unit
            return (1.0, uom_info['id'], uom_info['uom_code'])
    
    return (None, None, None)

def calculate_base_quantity(order_quantity, conversion_factor):
    """
    Calculate base quantity from order quantity and conversion factor.
    Formula: base_quantity = order_quantity * conversion_factor
    """
    if conversion_factor is None or conversion_factor == 0:
        return order_quantity
    return order_quantity * conversion_factor

@po_bp.route('/purchaseorders')
@login_required
def list_purchaseorders():
    db = Database()
    conn = db.get_connection()
    
    # Get filter parameters
    status_filter = request.args.get('status', '')
    supplier_filter = request.args.get('supplier', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    po_type_filter = request.args.get('po_type', '')
    
    # Get sort parameters
    sort_by = request.args.get('sort', 'order_date')
    sort_dir = request.args.get('dir', 'desc')
    
    # Validate sort column
    valid_sort_columns = {
        'po_number': 'po.po_number',
        'supplier_name': 's.name',
        'total_amount': 'total_amount',
        'status': 'po.status',
        'order_date': 'po.order_date',
        'expected_delivery_date': 'po.expected_delivery_date'
    }
    sort_column = valid_sort_columns.get(sort_by, 'po.order_date')
    sort_direction = 'ASC' if sort_dir.lower() == 'asc' else 'DESC'
    
    # Build query with filters - include both regular lines and service lines
    query = '''
        SELECT po.*, s.name as supplier_name,
               (SELECT COUNT(*) FROM purchase_order_lines WHERE po_id = po.id) + 
               (SELECT COUNT(*) FROM purchase_order_service_lines WHERE po_id = po.id) as line_count,
               COALESCE(
                   (SELECT SUM(quantity * unit_price) FROM purchase_order_lines WHERE po_id = po.id), 0
               ) + COALESCE(
                   (SELECT SUM(total_cost) FROM purchase_order_service_lines WHERE po_id = po.id), 0
               ) as total_amount
        FROM purchase_orders po
        JOIN suppliers s ON po.supplier_id = s.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND po.status = ?'
        params.append(status_filter)
    
    if supplier_filter:
        query += ' AND po.supplier_id = ?'
        params.append(int(supplier_filter))
    
    if date_from:
        query += ' AND po.order_date >= ?'
        params.append(date_from)
    
    if date_to:
        query += ' AND po.order_date <= ?'
        params.append(date_to)
    
    if po_type_filter == 'exchange':
        query += ' AND po.is_exchange = 1'
    elif po_type_filter == 'standard':
        query += ' AND (po.is_exchange = 0 OR po.is_exchange IS NULL)'
    elif po_type_filter == 'service':
        query += ' AND po.po_type = ?'
        params.append('Service')
    
    query += f' ORDER BY {sort_column} {sort_direction}'
    
    purchase_orders = conn.execute(query, params).fetchall()
    
    # Get suppliers for filter dropdown
    suppliers = conn.execute('SELECT id, name FROM suppliers ORDER BY name').fetchall()
    
    # Get available statuses
    statuses = ['Draft', 'Ordered', 'Partially Received', 'Received', 'Cancelled']
    
    # Calculate summary stats
    total_count = len(purchase_orders)
    total_value = sum(po['total_amount'] or 0 for po in purchase_orders)
    open_count = sum(1 for po in purchase_orders if po['status'] not in ['Received', 'Cancelled'])
    
    conn.close()
    return render_template('purchaseorders/list.html', 
        purchase_orders=purchase_orders,
        suppliers=suppliers,
        statuses=statuses,
        status_filter=status_filter,
        supplier_filter=supplier_filter,
        date_from=date_from,
        date_to=date_to,
        po_type_filter=po_type_filter,
        sort_by=sort_by,
        sort_dir=sort_dir,
        total_count=total_count,
        total_value=total_value,
        open_count=open_count
    )

@po_bp.route('/purchaseorders/create', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement')
def create_purchaseorder():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        max_attempts = 5
        po_number = None
        po_id = None
        
        # Extract line items from form data
        lines = {}
        for key in request.form.keys():
            if key.startswith('lines['):
                # Parse lines[1][product_id] format
                parts = key.replace('lines[', '').replace(']', '').split('[')
                line_num = parts[0]
                field_name = parts[1]
                
                if line_num not in lines:
                    lines[line_num] = {}
                lines[line_num][field_name] = request.form[key]
        
        if not lines:
            flash('Please add at least one line item to the purchase order.', 'danger')
            conn.close()
            return redirect(url_for('po_routes.create_purchaseorder'))
        
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
                
                # Insert PO header
                conn.execute('''
                    INSERT INTO purchase_orders 
                    (po_number, supplier_id, status, order_date, expected_delivery_date, notes)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    po_number,
                    int(request.form['supplier_id']),
                    request.form['status'],
                    request.form.get('order_date'),
                    request.form.get('expected_delivery_date'),
                    request.form.get('notes')
                ))
                
                po_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
                
                # Insert line items with UOM conversion and cost preservation
                for line_num, line_data in sorted(lines.items()):
                    product_id = int(line_data['product_id'])
                    quantity = float(line_data['quantity'])
                    unit_price = float(line_data['unit_price'])
                    uom_id = int(line_data['uom_id']) if line_data.get('uom_id') else None
                    
                    # Calculate extended cost (invariant - never changes due to UOM conversion)
                    extended_cost = round(quantity * unit_price, 6)
                    
                    # Calculate base quantity and get conversion info
                    base_quantity = quantity
                    base_uom_id = uom_id
                    conversion_factor = 1.0
                    
                    if uom_id:
                        conv_factor, base_uom, base_code = get_product_uom_conversion(conn, product_id, uom_id)
                        if conv_factor is not None:
                            conversion_factor = conv_factor
                            base_uom_id = base_uom
                            base_quantity = calculate_base_quantity(quantity, conversion_factor)
                    
                    # Calculate base unit price from extended cost and base quantity
                    base_unit_price = round(extended_cost / base_quantity, 6) if base_quantity > 0 else 0
                    
                    conn.execute('''
                        INSERT INTO purchase_order_lines
                        (po_id, line_number, product_id, quantity, unit_price, uom_id,
                         base_quantity, base_uom_id, conversion_factor_used,
                         extended_cost, base_unit_price)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        po_id,
                        int(line_num),
                        product_id,
                        quantity,
                        unit_price,
                        uom_id,
                        base_quantity,
                        base_uom_id,
                        conversion_factor,
                        extended_cost,
                        base_unit_price
                    ))
                
                # Log audit trail
                AuditLogger.log_change(
                    conn=conn,
                    record_type='purchase_order',
                    record_id=po_id,
                    action_type='Created',
                    modified_by=session.get('user_id'),
                    ip_address=request.remote_addr,
                    user_agent=request.headers.get('User-Agent')
                )
                
                conn.commit()
                break
                
            except Exception as e:
                if 'UNIQUE constraint failed' in str(e) and attempt < max_attempts - 1:
                    conn.rollback()
                    continue
                else:
                    conn.rollback()
                    conn.close()
                    flash(f'Error creating purchase order: {str(e)}', 'danger')
                    return redirect(url_for('po_routes.list_purchaseorders'))
        
        conn.close()
        
        if po_id:
            flash(f'Purchase Order {po_number} created successfully with {len(lines)} line(s)!', 'success')
            return redirect(url_for('po_routes.list_purchaseorders'))
        else:
            flash('Failed to create purchase order after multiple attempts', 'danger')
            return redirect(url_for('po_routes.list_purchaseorders'))
    
    suppliers = conn.execute('SELECT * FROM suppliers ORDER BY code').fetchall()
    products = conn.execute('SELECT * FROM products ORDER BY code').fetchall()
    uoms = conn.execute("SELECT id, uom_code, uom_name, uom_type FROM unit_of_measure WHERE status = 'Active' ORDER BY uom_code").fetchall()
    
    # Convert Row objects to dictionaries for JSON serialization
    products_list = [dict(p) for p in products]
    uoms_list = [dict(u) for u in uoms]
    
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
    
    return render_template('purchaseorders/create.html', suppliers=suppliers, products=products_list, uoms=uoms_list, next_po_number=next_po_number)

@po_bp.route('/purchaseorders/<int:id>')
@login_required
def view_purchaseorder(id):
    db = Database()
    conn = db.get_connection()
    
    # Get PO header with supplier info
    po = conn.execute('''
        SELECT po.*, s.name as supplier_name, s.contact_person, s.email, s.phone, s.address
        FROM purchase_orders po
        JOIN suppliers s ON po.supplier_id = s.id
        WHERE po.id = ?
    ''', (id,)).fetchone()
    
    if not po:
        conn.close()
        flash('Purchase order not found', 'danger')
        return redirect(url_for('po_routes.list_purchaseorders'))
    
    # Get line items with product, UOM, and base UOM info
    lines = conn.execute('''
        SELECT pol.*, p.code as product_code, p.name as product_name, p.unit_of_measure,
               uom.uom_code, uom.uom_name,
               base_uom.uom_code as base_uom_code, base_uom.uom_name as base_uom_name,
               i.quantity as inventory_quantity
        FROM purchase_order_lines pol
        JOIN products p ON pol.product_id = p.id
        LEFT JOIN uom_master uom ON pol.uom_id = uom.id
        LEFT JOIN uom_master base_uom ON pol.base_uom_id = base_uom.id
        LEFT JOIN inventory i ON i.product_id = p.id
        WHERE pol.po_id = ?
        ORDER BY pol.line_number
    ''', (id,)).fetchall()
    
    # Get related A/P records
    ap_records = conn.execute('''
        SELECT vi.*, (vi.total_amount - vi.amount_paid) as balance_due
        FROM vendor_invoices vi
        WHERE vi.po_id = ?
        ORDER BY vi.created_at DESC
    ''', (id,)).fetchall()
    
    # Get service lines for Service POs
    service_lines = []
    if po['po_type'] == 'Service':
        service_lines = conn.execute('''
            SELECT psl.*, wo.wo_number
            FROM purchase_order_service_lines psl
            LEFT JOIN work_orders wo ON psl.work_order_id = wo.id
            WHERE psl.po_id = ?
            ORDER BY psl.line_number
        ''', (id,)).fetchall()
    
    # Get source sales order for Exchange POs
    source_sales_order = None
    exchange_owner = None
    if po['is_exchange'] and po['source_sales_order_id']:
        source_sales_order = conn.execute('''
            SELECT so.*, c.name as customer_name, c.customer_number
            FROM sales_orders so
            JOIN customers c ON so.customer_id = c.id
            WHERE so.id = ?
        ''', (po['source_sales_order_id'],)).fetchone()
        
        # Get exchange owner details
        if po['exchange_owner_type'] == 'Customer':
            exchange_owner = conn.execute('''
                SELECT id, customer_number as code, name, email, phone
                FROM customers WHERE id = ?
            ''', (po['exchange_owner_id'],)).fetchone()
        elif po['exchange_owner_type'] == 'Supplier':
            exchange_owner = conn.execute('''
                SELECT id, code, name, email, phone
                FROM suppliers WHERE id = ?
            ''', (po['exchange_owner_id'],)).fetchone()
    
    # Calculate total for the PO
    if po['po_type'] == 'Service' and service_lines:
        total = sum((line['total_cost'] or 0) for line in service_lines)
    else:
        total = sum((line['extended_cost'] or (line['quantity'] * (line['unit_price'] or 0))) for line in lines)
    
    # Validate UoM conversions for each line and build variance status
    line_validations = {}
    overall_variance_ok = True
    variance_warnings = []
    
    for line in lines:
        if line['product_id'] and line['uom_id']:
            result = validate_po_line_conversion(
                line['product_id'], 
                line['quantity'] or 0, 
                line['uom_id'], 
                line['unit_price'] or 0,
                conn,
                po_line_id=line['id']
            )
            line_validations[line['id']] = {
                'success': result.success,
                'variance_type': result.variance_type,
                'variance_blocked': result.variance_blocked,
                'error_message': result.error_message
            }
            if not result.success or result.variance_blocked:
                overall_variance_ok = False
                variance_warnings.append(f"Line {line['line_number']}: {result.error_message or result.variance_type}")
        else:
            line_validations[line['id']] = {
                'success': True,
                'variance_type': 'NONE',
                'variance_blocked': False,
                'error_message': None
            }
    
    conn.close()
    
    # Get current date for overdue badge and modal
    from datetime import date
    today = date.today().strftime('%Y-%m-%d')
    now = datetime.now()
    
    return render_template('purchaseorders/view.html', po=po, lines=lines, ap_records=ap_records, 
                          today=today, now=now, total=total, source_sales_order=source_sales_order, 
                          exchange_owner=exchange_owner, service_lines=service_lines,
                          line_validations=line_validations, overall_variance_ok=overall_variance_ok,
                          variance_warnings=variance_warnings)

@po_bp.route('/purchaseorders/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement')
def edit_purchaseorder(id):
    db = Database()
    conn = db.get_connection()
    
    # Get PO header
    po = conn.execute('SELECT * FROM purchase_orders WHERE id = ?', (id,)).fetchone()
    
    if not po:
        conn.close()
        flash('Purchase order not found', 'danger')
        return redirect(url_for('po_routes.list_purchaseorders'))
    
    # Check if PO can be edited (not fully received)
    if po['status'] == 'Received':
        conn.close()
        flash('Cannot edit a fully received purchase order', 'warning')
        return redirect(url_for('po_routes.view_purchaseorder', id=id))
    
    if request.method == 'POST':
        try:
            # Get header data
            supplier_id = int(request.form['supplier_id'])
            status = request.form['status']
            order_date = request.form.get('order_date') or None
            expected_delivery_date = request.form.get('expected_delivery_date') or None
            notes = request.form.get('notes', '').strip()
            
            # Extract line items from form data
            lines = {}
            for key in request.form.keys():
                if key.startswith('lines['):
                    parts = key.replace('lines[', '').replace(']', '').split('[')
                    line_num = parts[0]
                    field_name = parts[1]
                    
                    if line_num not in lines:
                        lines[line_num] = {}
                    lines[line_num][field_name] = request.form[key]
            
            if not lines:
                flash('Please add at least one line item to the purchase order.', 'danger')
                conn.close()
                return redirect(url_for('po_routes.edit_purchaseorder', id=id))
            
            # Get old PO data for audit trail (header + lines)
            old_po_data = {
                'supplier_id': po['supplier_id'],
                'status': po['status'],
                'order_date': po['order_date'],
                'expected_delivery_date': po['expected_delivery_date'],
                'notes': po['notes']
            }
            
            # Get existing lines from database for comparison and validation
            existing_lines = conn.execute('''
                SELECT * FROM purchase_order_lines WHERE po_id = ?
            ''', (id,)).fetchall()
            
            # Map existing lines by ID for lookup
            existing_lines_map = {line['id']: dict(line) for line in existing_lines}
            
            # Store old lines for audit
            old_lines_data = [{
                'id': line['id'],
                'line_number': line['line_number'],
                'product_id': line['product_id'],
                'quantity': line['quantity'],
                'unit_price': line['unit_price'],
                'received_quantity': line['received_quantity']
            } for line in existing_lines]
            
            # Update PO header
            conn.execute('''
                UPDATE purchase_orders
                SET supplier_id = ?, status = ?, order_date = ?, expected_delivery_date = ?, notes = ?
                WHERE id = ?
            ''', (supplier_id, status, order_date, expected_delivery_date, notes, id))
            
            # Track which line IDs are in the submission
            submitted_line_ids = set()
            new_lines_data = []
            
            # Process each submitted line
            for line_num, line_data in sorted(lines.items(), key=lambda x: int(x[0])):
                product_id = int(line_data['product_id'])
                uom_id = int(line_data['uom_id']) if line_data.get('uom_id') else None
                quantity = float(line_data['quantity'])
                unit_price = float(line_data['unit_price'])
                line_id_str = line_data.get('line_id', '').strip()
                
                if line_id_str and line_id_str.isdigit():
                    # Existing line - UPDATE it
                    line_id = int(line_id_str)
                    
                    # Validate: line_id must belong to this PO (prevent tampering)
                    if line_id not in existing_lines_map:
                        raise ValueError(f'Invalid line_id {line_id}: does not belong to this purchase order')
                    
                    submitted_line_ids.add(line_id)
                    existing_line = existing_lines_map[line_id]
                    received_qty = existing_line['received_quantity'] or 0
                    
                    # Validate: cannot change product on lines with receipts (prevents inventory corruption)
                    if received_qty > 0 and existing_line['product_id'] != product_id:
                        raise ValueError(f'Line {line_num}: Cannot change product on a line that has been partially or fully received')
                    
                    # Validate: ordered quantity must be >= received quantity
                    if quantity < received_qty:
                        raise ValueError(f'Line {line_num}: Ordered quantity ({quantity}) cannot be less than received quantity ({received_qty})')
                    
                    # Calculate base quantity and conversion info for update
                    base_quantity = quantity
                    base_uom_id = uom_id
                    conversion_factor = 1.0
                    
                    if uom_id:
                        conv_factor, base_uom, base_code = get_product_uom_conversion(conn, product_id, uom_id)
                        if conv_factor is not None:
                            conversion_factor = conv_factor
                            base_uom_id = base_uom
                            base_quantity = calculate_base_quantity(quantity, conversion_factor)
                    
                    # Update existing line with conversion info
                    conn.execute('''
                        UPDATE purchase_order_lines
                        SET line_number = ?, product_id = ?, uom_id = ?, quantity = ?, unit_price = ?,
                            base_quantity = ?, base_uom_id = ?, conversion_factor_used = ?
                        WHERE id = ?
                    ''', (int(line_num), product_id, uom_id, quantity, unit_price, 
                          base_quantity, base_uom_id, conversion_factor, line_id))
                    
                    new_lines_data.append({
                        'id': line_id,
                        'line_number': int(line_num),
                        'product_id': product_id,
                        'quantity': quantity,
                        'unit_price': unit_price,
                        'received_quantity': received_qty
                    })
                else:
                    # Calculate base quantity and conversion info for new line
                    base_quantity = quantity
                    base_uom_id = uom_id
                    conversion_factor = 1.0
                    
                    if uom_id:
                        conv_factor, base_uom, base_code = get_product_uom_conversion(conn, product_id, uom_id)
                        if conv_factor is not None:
                            conversion_factor = conv_factor
                            base_uom_id = base_uom
                            base_quantity = calculate_base_quantity(quantity, conversion_factor)
                    
                    # New line - INSERT it with conversion info
                    cursor = conn.execute('''
                        INSERT INTO purchase_order_lines 
                        (po_id, line_number, product_id, uom_id, quantity, unit_price, received_quantity,
                         base_quantity, base_uom_id, conversion_factor_used)
                        VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
                    ''', (id, int(line_num), product_id, uom_id, quantity, unit_price,
                          base_quantity, base_uom_id, conversion_factor))
                    
                    new_line_id = cursor.lastrowid
                    submitted_line_ids.add(new_line_id)
                    
                    new_lines_data.append({
                        'id': new_line_id,
                        'line_number': int(line_num),
                        'product_id': product_id,
                        'quantity': quantity,
                        'unit_price': unit_price,
                        'received_quantity': 0
                    })
            
            # Delete lines that were removed (only if not received)
            for line_id, existing_line in existing_lines_map.items():
                if line_id not in submitted_line_ids:
                    received_qty = existing_line['received_quantity'] or 0
                    if received_qty > 0:
                        raise ValueError(f'Cannot delete line {existing_line["line_number"]}: {received_qty} units have been received')
                    conn.execute('DELETE FROM purchase_order_lines WHERE id = ?', (line_id,))
            
            # Create audit log with header and line items
            new_po_data = {
                'supplier_id': supplier_id,
                'status': status,
                'order_date': order_date,
                'expected_delivery_date': expected_delivery_date,
                'notes': notes
            }
            
            # Build changed fields for header
            changed_fields = {}
            for key in old_po_data.keys():
                if old_po_data.get(key) != new_po_data.get(key):
                    changed_fields[key] = {'old': old_po_data.get(key), 'new': new_po_data.get(key)}
            
            # Log header changes
            if changed_fields:
                AuditLogger.log_change(
                    conn=conn,
                    record_type='purchase_order',
                    record_id=str(id),
                    action_type='Updated',
                    modified_by=session['user_id'],
                    changed_fields=changed_fields
                )
            
            # Log line item changes if different
            if old_lines_data != new_lines_data:
                AuditLogger.log_change(
                    conn=conn,
                    record_type='purchase_order_lines',
                    record_id=str(id),
                    action_type='Updated',
                    modified_by=session['user_id'],
                    changed_fields={'old_lines': old_lines_data, 'new_lines': new_lines_data}
                )
            
            conn.commit()
            conn.close()
            
            flash(f'Purchase Order {po["po_number"]} updated successfully!', 'success')
            return redirect(url_for('po_routes.view_purchaseorder', id=id))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error updating purchase order: {str(e)}', 'danger')
            return redirect(url_for('po_routes.edit_purchaseorder', id=id))
    
    # GET request - load edit form
    lines = conn.execute('''
        SELECT pol.*, p.code as product_code, p.name as product_name
        FROM purchase_order_lines pol
        JOIN products p ON pol.product_id = p.id
        WHERE pol.po_id = ?
        ORDER BY pol.line_number
    ''', (id,)).fetchall()
    
    suppliers = conn.execute('SELECT * FROM suppliers ORDER BY code').fetchall()
    products = conn.execute('SELECT * FROM products ORDER BY code').fetchall()
    uoms = conn.execute("SELECT id, uom_code, uom_name, uom_type FROM unit_of_measure WHERE status = 'Active' ORDER BY uom_code").fetchall()
    
    # Convert Row objects to dictionaries for JSON serialization
    products_list = [dict(p) for p in products]
    uoms_list = [dict(u) for u in uoms]
    lines_list = [dict(l) for l in lines]
    
    conn.close()
    
    return render_template('purchaseorders/edit.html', 
                         po=po, 
                         lines=lines_list,
                         suppliers=suppliers, 
                         products=products_list, 
                         uoms=uoms_list)

@po_bp.route('/purchaseorders/<int:id>/print')
@login_required
def print_purchaseorder(id):
    db = Database()
    conn = db.get_connection()
    
    # Get PO header
    po = conn.execute('''
        SELECT po.*, s.name as supplier_name, s.contact_person, s.email, s.phone, s.address
        FROM purchase_orders po
        JOIN suppliers s ON po.supplier_id = s.id
        WHERE po.id = ?
    ''', (id,)).fetchone()
    
    if not po:
        conn.close()
        flash('Purchase order not found', 'danger')
        return redirect(url_for('po_routes.list_purchaseorders'))
    
    # Get line items
    lines = conn.execute('''
        SELECT pol.*, p.code as product_code, p.name as product_name, p.description, p.unit_of_measure,
               uom.uom_code, uom.uom_name
        FROM purchase_order_lines pol
        JOIN products p ON pol.product_id = p.id
        LEFT JOIN uom_master uom ON pol.uom_id = uom.id
        WHERE pol.po_id = ?
        ORDER BY pol.line_number
    ''', (id,)).fetchall()
    
    conn.close()
    
    company_settings = CompanySettings.get_or_create_default()
    
    return render_template('purchaseorders/print.html', po=po, lines=lines, company_settings=company_settings, current_date=datetime.now().strftime('%B %d, %Y'))

@po_bp.route('/purchaseorders/<int:id>/download')
@login_required
def download_purchaseorder(id):
    db = Database()
    conn = db.get_connection()
    
    # Get PO header
    po = conn.execute('''
        SELECT po.*, s.name as supplier_name, s.contact_person, s.email, s.phone, s.address
        FROM purchase_orders po
        JOIN suppliers s ON po.supplier_id = s.id
        WHERE po.id = ?
    ''', (id,)).fetchone()
    
    if not po:
        conn.close()
        flash('Purchase order not found', 'danger')
        return redirect(url_for('po_routes.list_purchaseorders'))
    
    # Get line items
    lines = conn.execute('''
        SELECT pol.*, p.code as product_code, p.name as product_name, p.description, p.unit_of_measure,
               uom.uom_code, uom.uom_name
        FROM purchase_order_lines pol
        JOIN products p ON pol.product_id = p.id
        LEFT JOIN uom_master uom ON pol.uom_id = uom.id
        WHERE pol.po_id = ?
        ORDER BY pol.line_number
    ''', (id,)).fetchall()
    
    conn.close()
    
    company_settings = CompanySettings.get_or_create_default()
    
    html_content = render_template('purchaseorders/print.html', po=po, lines=lines, company_settings=company_settings, current_date=datetime.now().strftime('%B %d, %Y'))
    
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

@po_bp.route('/api/product-uom-conversions/<int:product_id>')
@login_required
def api_product_uom_conversions(product_id):
    """API endpoint to get available UOM conversions for a specific product"""
    from flask import jsonify
    
    db = Database()
    conn = db.get_connection()
    
    # Get product-specific UOM conversions
    product_uoms = conn.execute('''
        SELECT puc.*, u.uom_code, u.uom_name, u.uom_type,
               bu.id as base_uom_id, bu.uom_code as base_uom_code, bu.uom_name as base_uom_name
        FROM product_uom_conversions puc
        JOIN uom_master u ON puc.uom_id = u.id
        LEFT JOIN uom_master bu ON u.base_uom_id = bu.id
        WHERE puc.product_id = ? AND u.is_active = 1
        ORDER BY puc.is_purchase_uom DESC, u.uom_code
    ''', (product_id,)).fetchall()
    
    # If no product-specific UOMs, get all active UOMs
    if not product_uoms:
        all_uoms = conn.execute('''
            SELECT u.id as uom_id, u.uom_code, u.uom_name, u.uom_type, u.conversion_factor,
                   u.base_uom_id, bu.uom_code as base_uom_code, bu.uom_name as base_uom_name,
                   0 as is_base_uom, 0 as is_purchase_uom, 0 as is_issue_uom
            FROM uom_master u
            LEFT JOIN uom_master bu ON u.base_uom_id = bu.id
            WHERE u.is_active = 1
            ORDER BY u.uom_type, u.uom_code
        ''').fetchall()
        
        conn.close()
        return jsonify([dict(row) for row in all_uoms])
    
    conn.close()
    return jsonify([dict(row) for row in product_uoms])

@po_bp.route('/api/calculate-conversion', methods=['POST'])
@login_required
def api_calculate_conversion():
    """API endpoint to calculate base quantity from order quantity and UOM"""
    from flask import jsonify
    
    data = request.get_json()
    product_id = data.get('product_id')
    uom_id = data.get('uom_id')
    quantity = float(data.get('quantity', 0))
    
    if not product_id or not uom_id:
        return jsonify({'error': 'Missing required parameters'}), 400
    
    db = Database()
    conn = db.get_connection()
    
    try:
        conv_factor, base_uom_id, base_uom_code = get_product_uom_conversion(conn, product_id, uom_id)
        
        if conv_factor is None:
            conn.close()
            return jsonify({'error': 'No conversion found'}), 404
        
        base_quantity = calculate_base_quantity(quantity, conv_factor)
        
        conn.close()
        
        return jsonify({
            'conversion_factor': conv_factor,
            'base_uom_id': base_uom_id,
            'base_uom_code': base_uom_code,
            'base_quantity': round(base_quantity, 4),
            'order_quantity': quantity
        })
        
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500

@po_bp.route('/api/validate-po-line-conversion', methods=['POST'])
@login_required
def api_validate_po_line_conversion():
    """
    API endpoint to validate PO line conversion with variance detection.
    Implements the UoM Conversion & Variance Control Engine.
    """
    data = request.get_json()
    product_id = data.get('product_id')
    uom_id = data.get('uom_id')
    quantity = data.get('quantity', 0)
    unit_price = data.get('unit_price', 0)
    
    if not product_id or not uom_id:
        return jsonify({'success': False, 'error': 'Missing required parameters'}), 400
    
    try:
        quantity = float(quantity)
        unit_price = float(unit_price)
    except (ValueError, TypeError):
        return jsonify({'success': False, 'error': 'Invalid quantity or unit price'}), 400
    
    # Use the variance control engine
    result = validate_po_line_conversion(product_id, quantity, uom_id, unit_price)
    
    if not result.success:
        return jsonify({
            'success': False,
            'error': result.error_message,
            'variance_blocked': result.variance_blocked,
            'variance_type': result.variance_type
        }), 400
    
    return jsonify({
        'success': True,
        'original_quantity': float(result.original_quantity) if result.original_quantity else 0,
        'original_uom_code': result.original_uom_code,
        'base_quantity': float(result.converted_quantity) if result.converted_quantity else 0,
        'base_uom_code': result.target_uom_code,
        'conversion_factor': float(result.conversion_factor) if result.conversion_factor else 1,
        'extended_cost': float(result.extended_cost) if result.extended_cost else 0,
        'normalized_unit_cost': float(result.normalized_unit_cost) if result.normalized_unit_cost else 0,
        'variance_type': result.variance_type,
        'variance_amount': float(result.variance_amount) if result.variance_amount else 0,
        'variance_blocked': result.variance_blocked,
        'rounding_adjustment': float(result.rounding_adjustment) if result.rounding_adjustment else 0
    })


@po_bp.route('/api/check-conversion-defined', methods=['POST'])
@login_required
def api_check_conversion_defined():
    """
    API endpoint to check if a valid UoM conversion is defined for a product.
    Used to block PO line creation with undefined conversions.
    """
    data = request.get_json()
    product_id = data.get('product_id')
    uom_id = data.get('uom_id')
    
    if not product_id or not uom_id:
        return jsonify({'defined': False, 'error': 'Missing required parameters'}), 400
    
    is_defined, factor, error = check_conversion_defined(product_id, uom_id)
    
    return jsonify({
        'defined': is_defined,
        'conversion_factor': float(factor) if factor else None,
        'error': error
    })


@po_bp.route('/api/quick-add-product', methods=['POST'])
@role_required('Admin', 'Procurement', 'Planner')
def api_quick_add_product():
    """API endpoint to quickly create a new product from PO line"""
    from flask import jsonify
    
    data = request.get_json()
    code = data.get('code', '').strip()
    name = data.get('name', '').strip()
    description = data.get('description', '').strip()
    unit_of_measure = data.get('unit_of_measure', 'EA')
    product_type = data.get('product_type', 'Purchased')
    part_category = data.get('part_category', 'Other')
    
    if not code or not name:
        return jsonify({'success': False, 'error': 'Part number and name are required'}), 400
    
    db = Database()
    conn = db.get_connection()
    
    try:
        # Check if product code already exists
        existing = conn.execute('SELECT id FROM products WHERE code = ?', (code,)).fetchone()
        if existing:
            conn.close()
            return jsonify({'success': False, 'error': f'Product with code "{code}" already exists'}), 400
        
        # Create the product
        cursor = conn.execute('''
            INSERT INTO products (code, name, description, unit_of_measure, product_type, part_category)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (code, name, description, unit_of_measure, product_type, part_category))
        
        product_id = cursor.lastrowid
        
        # Create inventory record for the new product
        conn.execute('''
            INSERT INTO inventory (product_id, quantity, warehouse_location, reorder_point, safety_stock)
            VALUES (?, 0, 'Main', 0, 0)
        ''', (product_id,))
        
        conn.commit()
        
        # Log the audit
        AuditLogger.log_change(
            'products', 
            product_id, 
            'CREATE', 
            None, 
            {'code': code, 'name': name, 'product_type': product_type},
            f'Quick-created from Purchase Order'
        )
        
        conn.close()
        
        return jsonify({
            'success': True,
            'product': {
                'id': product_id,
                'code': code,
                'name': name,
                'unit_of_measure': unit_of_measure,
                'product_type': product_type
            }
        })
        
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500

@po_bp.route('/api/purchaseorders/mass-update', methods=['POST'])
@role_required('Admin', 'Procurement')
def api_mass_update_purchaseorders():
    """API endpoint to mass update multiple purchase orders"""
    from flask import jsonify
    
    data = request.get_json()
    po_ids = data.get('po_ids', [])
    updates = data.get('updates', {})
    
    if not po_ids:
        return jsonify({'success': False, 'error': 'No purchase orders selected'}), 400
    
    if not updates:
        return jsonify({'success': False, 'error': 'No updates specified'}), 400
    
    db = Database()
    conn = db.get_connection()
    
    try:
        updated_count = 0
        
        for po_id in po_ids:
            # Get current PO for audit
            old_po = conn.execute('SELECT * FROM purchase_orders WHERE id = ?', (po_id,)).fetchone()
            if not old_po:
                continue
            
            # Build update query dynamically
            update_fields = []
            update_values = []
            
            if 'status' in updates:
                update_fields.append('status = ?')
                update_values.append(updates['status'])
            
            if 'expected_delivery_date' in updates:
                update_fields.append('expected_delivery_date = ?')
                update_values.append(updates['expected_delivery_date'] or None)
            
            if 'order_date' in updates:
                update_fields.append('order_date = ?')
                update_values.append(updates['order_date'] or None)
            
            if 'notes_append' in updates and updates['notes_append']:
                # Append to existing notes
                current_notes = old_po['notes'] or ''
                new_notes = current_notes + ('\n' if current_notes else '') + updates['notes_append']
                update_fields.append('notes = ?')
                update_values.append(new_notes)
            
            if update_fields:
                update_values.append(po_id)
                conn.execute(f'''
                    UPDATE purchase_orders 
                    SET {', '.join(update_fields)}
                    WHERE id = ?
                ''', update_values)
                
                # Get new PO for audit
                new_po = conn.execute('SELECT * FROM purchase_orders WHERE id = ?', (po_id,)).fetchone()
                
                # Log the audit
                AuditLogger.log_change(
                    'purchase_orders',
                    po_id,
                    'UPDATE',
                    dict(old_po),
                    dict(new_po),
                    'Mass update'
                )
                
                updated_count += 1
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'updated_count': updated_count,
            'message': f'Successfully updated {updated_count} purchase order(s)'
        })
        
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500


@po_bp.route('/purchaseorders/<int:id>/receive-exchange', methods=['POST'])
@role_required('Admin', 'Procurement')
def receive_exchange_po(id):
    """Receive an Exchange Fee Purchase Order (no inventory creation)"""
    db = Database()
    conn = db.get_connection()
    
    try:
        po = conn.execute('SELECT * FROM purchase_orders WHERE id = ?', (id,)).fetchone()
        
        if not po:
            flash('Purchase order not found', 'danger')
            conn.close()
            return redirect(url_for('po_routes.list_purchaseorders'))
        
        if not po['is_exchange']:
            flash('This is not an Exchange Purchase Order. Use standard receiving.', 'warning')
            conn.close()
            return redirect(url_for('po_routes.view_purchaseorder', id=id))
        
        if po['status'] == 'Received':
            flash('This Exchange PO has already been received', 'warning')
            conn.close()
            return redirect(url_for('po_routes.view_purchaseorder', id=id))
        
        if po['status'] == 'Cancelled':
            flash('Cannot receive a cancelled purchase order', 'danger')
            conn.close()
            return redirect(url_for('po_routes.view_purchaseorder', id=id))
        
        receipt_date = request.form.get('receipt_date') or datetime.now().strftime('%Y-%m-%d')
        remarks = request.form.get('remarks', '')
        
        # Update PO status to Received
        conn.execute('''
            UPDATE purchase_orders
            SET status = 'Received',
                exchange_status = 'Completed',
                actual_delivery_date = ?
            WHERE id = ?
        ''', (receipt_date, id))
        
        # Update all PO lines as fully received
        conn.execute('''
            UPDATE purchase_order_lines
            SET received_quantity = quantity
            WHERE po_id = ?
        ''', (id,))
        
        # Generate receipt number for tracking
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
        
        # Get the first line for the receiving transaction record
        first_line = conn.execute('''
            SELECT pol.*, p.id as product_id
            FROM purchase_order_lines pol
            JOIN products p ON pol.product_id = p.id
            WHERE pol.po_id = ?
            ORDER BY pol.line_number
            LIMIT 1
        ''', (id,)).fetchone()
        
        if first_line:
            # Create receiving transaction record (for audit trail, no inventory impact)
            conn.execute('''
                INSERT INTO receiving_transactions 
                (receipt_number, po_id, product_id, quantity_received, receipt_date, 
                 condition, warehouse_location, bin_location, remarks, received_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (receipt_number, id, first_line['product_id'], first_line['quantity'], 
                  receipt_date, 'N/A - Exchange Fee', 'N/A', 'N/A', 
                  f'Exchange Fee Receipt. {remarks}'.strip(), session.get('user_id')))
        
        # Calculate total value for A/P record
        po_lines = conn.execute('''
            SELECT SUM(quantity * unit_price) as total_value
            FROM purchase_order_lines
            WHERE po_id = ?
        ''', (id,)).fetchone()
        
        total_value = po_lines['total_value'] or 0
        
        # Generate A/P number
        last_ap = conn.execute('''
            SELECT invoice_number FROM vendor_invoices 
            WHERE invoice_number LIKE 'AP-%'
            ORDER BY CAST(SUBSTR(invoice_number, 4) AS INTEGER) DESC 
            LIMIT 1
        ''').fetchone()
        
        if last_ap:
            try:
                last_ap_number = int(last_ap['invoice_number'].split('-')[1])
                next_ap_number = last_ap_number + 1
            except (ValueError, IndexError):
                next_ap_number = 1
        else:
            next_ap_number = 1
        
        ap_number = f'AP-{next_ap_number:07d}'
        
        # Calculate due date (30 days payment terms)
        from datetime import timedelta
        receipt_dt = datetime.strptime(receipt_date, '%Y-%m-%d')
        due_date = (receipt_dt + timedelta(days=30)).strftime('%Y-%m-%d')
        
        # Create A/P record for the Exchange Fee
        conn.execute('''
            INSERT INTO vendor_invoices 
            (invoice_number, vendor_id, po_id, invoice_date, due_date, 
             amount, tax_amount, total_amount, amount_paid, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            ap_number,
            po['supplier_id'],
            id,
            receipt_date,
            due_date,
            total_value,
            0,  # tax_amount
            total_value,
            0,  # amount_paid
            'Pending Invoice'
        ))
        
        # Log audit trail
        AuditLogger.log_change(
            conn, 'purchase_orders', id, 'RECEIVE', session.get('user_id'),
            {
                'po_number': po['po_number'],
                'receipt_date': receipt_date,
                'receipt_number': receipt_number,
                'ap_number': ap_number,
                'action': 'Exchange PO Received',
                'remarks': remarks
            }
        )
        
        conn.commit()
        flash(f'Exchange PO {po["po_number"]} received successfully (Receipt: {receipt_number}, A/P: {ap_number})', 'success')
        conn.close()
        return redirect(url_for('po_routes.view_purchaseorder', id=id))
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error receiving Exchange PO: {str(e)}', 'danger')
        return redirect(url_for('po_routes.view_purchaseorder', id=id))


@po_bp.route('/purchaseorders/<int:id>/receive-service', methods=['POST'])
@role_required('Admin', 'Procurement', 'Production Staff')
def receive_service_po(id):
    """Receive a Service/Misc Purchase Order (no inventory creation)"""
    db = Database()
    conn = db.get_connection()
    
    try:
        po = conn.execute('SELECT * FROM purchase_orders WHERE id = ?', (id,)).fetchone()
        
        if not po:
            flash('Purchase order not found', 'danger')
            conn.close()
            return redirect(url_for('po_routes.list_purchaseorders'))
        
        if po['po_type'] != 'Service':
            flash('This is not a Service Purchase Order. Use standard receiving.', 'warning')
            conn.close()
            return redirect(url_for('po_routes.view_purchaseorder', id=id))
        
        if po['status'] == 'Received':
            flash('This Service PO has already been received', 'warning')
            conn.close()
            return redirect(url_for('po_routes.view_purchaseorder', id=id))
        
        if po['status'] == 'Cancelled':
            flash('Cannot receive a cancelled purchase order', 'danger')
            conn.close()
            return redirect(url_for('po_routes.view_purchaseorder', id=id))
        
        receipt_date = request.form.get('receipt_date') or datetime.now().strftime('%Y-%m-%d')
        remarks = request.form.get('remarks', '')
        
        # Update PO status to Received
        conn.execute('''
            UPDATE purchase_orders
            SET status = 'Received',
                actual_delivery_date = ?
            WHERE id = ?
        ''', (receipt_date, id))
        
        # Update all service lines as Received
        conn.execute('''
            UPDATE purchase_order_service_lines
            SET status = 'Received',
                received_date = ?,
                received_by = ?
            WHERE po_id = ?
        ''', (receipt_date, session.get('user_id'), id))
        
        # Calculate total value for A/P record from service lines
        service_total = conn.execute('''
            SELECT COALESCE(SUM(total_cost), 0) as total_value
            FROM purchase_order_service_lines
            WHERE po_id = ?
        ''', (id,)).fetchone()
        
        total_value = service_total['total_value'] or 0
        
        # Generate A/P number
        last_ap = conn.execute('''
            SELECT invoice_number FROM vendor_invoices 
            WHERE invoice_number LIKE 'AP-%'
            ORDER BY CAST(SUBSTR(invoice_number, 4) AS INTEGER) DESC 
            LIMIT 1
        ''').fetchone()
        
        if last_ap:
            try:
                last_ap_number = int(last_ap['invoice_number'].split('-')[1])
                next_ap_number = last_ap_number + 1
            except (ValueError, IndexError):
                next_ap_number = 1
        else:
            next_ap_number = 1
        
        ap_number = f'AP-{next_ap_number:07d}'
        
        # Calculate due date (30 days payment terms)
        from datetime import timedelta
        receipt_dt = datetime.strptime(receipt_date, '%Y-%m-%d')
        due_date = (receipt_dt + timedelta(days=30)).strftime('%Y-%m-%d')
        
        # Create A/P record for the Service PO
        conn.execute('''
            INSERT INTO vendor_invoices 
            (invoice_number, vendor_id, po_id, invoice_date, due_date, 
             amount, tax_amount, total_amount, amount_paid, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            ap_number,
            po['supplier_id'],
            id,
            receipt_date,
            due_date,
            total_value,
            0,  # tax_amount
            total_value,
            0,  # amount_paid
            'Pending Invoice'
        ))
        
        # Log audit trail
        AuditLogger.log_change(
            conn, 'purchase_orders', id, 'RECEIVE', session.get('user_id'),
            {
                'po_number': str(po['po_number']),
                'receipt_date': str(receipt_date),
                'ap_number': str(ap_number),
                'action': 'Service PO Received',
                'total_value': float(total_value),
                'remarks': str(remarks) if remarks else ''
            }
        )
        
        po_number = po['po_number']
        conn.commit()
        flash(f'Service PO {po_number} received successfully (A/P: {ap_number})', 'success')
        conn.close()
        return redirect(url_for('po_routes.view_purchaseorder', id=id))
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error receiving Service PO: {str(e)}', 'danger')
        return redirect(url_for('po_routes.view_purchaseorder', id=id))


@po_bp.route('/purchaseorders/<int:id>/cancel', methods=['POST'])
@role_required('Admin', 'Procurement')
def cancel_purchaseorder(id):
    """Cancel a purchase order"""
    db = Database()
    conn = db.get_connection()
    
    try:
        po = conn.execute('SELECT * FROM purchase_orders WHERE id = ?', (id,)).fetchone()
        
        if not po:
            flash('Purchase order not found', 'danger')
            conn.close()
            return redirect(url_for('po_routes.list_purchaseorders'))
        
        if po['status'] == 'Received':
            flash('Cannot cancel a fully received purchase order', 'danger')
            conn.close()
            return redirect(url_for('po_routes.view_purchaseorder', id=id))
        
        if po['status'] == 'Cancelled':
            flash('Purchase order is already cancelled', 'warning')
            conn.close()
            return redirect(url_for('po_routes.view_purchaseorder', id=id))
        
        conn.execute('''
            UPDATE purchase_orders 
            SET status = 'Cancelled', updated_at = CURRENT_TIMESTAMP 
            WHERE id = ?
        ''', (id,))
        
        AuditLogger.log_change(
            conn, 'purchase_orders', id, 'CANCEL', session.get('user_id'),
            {'po_number': po['po_number'], 'previous_status': po['status']}
        )
        
        conn.commit()
        flash(f'Purchase Order {po["po_number"]} has been cancelled', 'success')
        conn.close()
        return redirect(url_for('po_routes.view_purchaseorder', id=id))
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error cancelling purchase order: {str(e)}', 'danger')
        return redirect(url_for('po_routes.view_purchaseorder', id=id))


@po_bp.route('/purchaseorders/<int:id>/delete', methods=['POST'])
@role_required('Admin')
def delete_purchaseorder(id):
    """Delete a purchase order (Admin only)"""
    db = Database()
    conn = db.get_connection()
    
    try:
        po = conn.execute('SELECT * FROM purchase_orders WHERE id = ?', (id,)).fetchone()
        
        if not po:
            flash('Purchase order not found', 'danger')
            conn.close()
            return redirect(url_for('po_routes.list_purchaseorders'))
        
        if po['status'] == 'Received':
            flash('Cannot delete a received purchase order. Cancel it instead.', 'danger')
            conn.close()
            return redirect(url_for('po_routes.view_purchaseorder', id=id))
        
        received_qty = conn.execute('''
            SELECT SUM(received_quantity) as total FROM purchase_order_lines WHERE po_id = ?
        ''', (id,)).fetchone()
        
        if received_qty and received_qty['total'] and received_qty['total'] > 0:
            flash('Cannot delete a purchase order with received items. Cancel it instead.', 'danger')
            conn.close()
            return redirect(url_for('po_routes.view_purchaseorder', id=id))
        
        AuditLogger.log_change(
            conn, 'purchase_orders', id, 'DELETE', session.get('user_id'),
            {'po_number': po['po_number'], 'supplier_id': po['supplier_id'], 'status': po['status']}
        )
        
        conn.execute('DELETE FROM purchase_order_lines WHERE po_id = ?', (id,))
        conn.execute('DELETE FROM purchase_orders WHERE id = ?', (id,))
        
        conn.commit()
        flash(f'Purchase Order {po["po_number"]} has been deleted', 'success')
        conn.close()
        return redirect(url_for('po_routes.list_purchaseorders'))
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error deleting purchase order: {str(e)}', 'danger')
        return redirect(url_for('po_routes.view_purchaseorder', id=id))


@po_bp.route('/purchaseorders/<int:po_id>/send-to-supplier')
@login_required
@role_required('Admin', 'Procurement', 'Finance')
def send_to_supplier(po_id):
    """Page to send PO to supplier via email"""
    db = Database()
    conn = db.get_connection()
    
    po = conn.execute('SELECT * FROM purchase_orders WHERE id = ?', (po_id,)).fetchone()
    if not po:
        conn.close()
        flash('Purchase order not found', 'danger')
        return redirect(url_for('po_routes.list_purchaseorders'))
    
    supplier = conn.execute('SELECT * FROM suppliers WHERE id = ?', (po['supplier_id'],)).fetchone()
    
    lines = conn.execute('''
        SELECT pol.*, p.part_number, p.description, u.uom_code
        FROM purchase_order_lines pol
        JOIN products p ON pol.product_id = p.id
        LEFT JOIN uom_master u ON pol.uom_id = u.id
        WHERE pol.po_id = ?
        ORDER BY pol.line_number
    ''', (po_id,)).fetchall()
    
    total_amount = sum(line['quantity'] * line['unit_price'] for line in lines)
    
    existing_token = conn.execute('''
        SELECT * FROM po_supplier_tokens
        WHERE po_id = ? AND supplier_id = ? AND expires_at > ?
        ORDER BY created_at DESC LIMIT 1
    ''', (po_id, supplier['id'], datetime.now().isoformat())).fetchone()
    
    conn.close()
    
    return render_template('purchaseorders/send_to_supplier.html',
                         po=po,
                         supplier=supplier,
                         lines=lines,
                         total_amount=total_amount,
                         existing_token=existing_token)


@po_bp.route('/purchaseorders/<int:po_id>/generate-link', methods=['POST'])
@login_required
@role_required('Admin', 'Procurement', 'Finance')
def generate_supplier_link(po_id):
    """Generate secure token link for supplier to view PO"""
    db = Database()
    conn = db.get_connection()
    
    try:
        po = conn.execute('SELECT * FROM purchase_orders WHERE id = ?', (po_id,)).fetchone()
        if not po:
            conn.close()
            flash('Purchase order not found', 'danger')
            return redirect(url_for('po_routes.list_purchaseorders'))
        
        token = secrets.token_urlsafe(32)
        expires_at = datetime.now() + timedelta(days=30)
        
        conn.execute('''
            INSERT INTO po_supplier_tokens (po_id, supplier_id, token, expires_at)
            VALUES (?, ?, ?, ?)
        ''', (po_id, po['supplier_id'], token, expires_at.isoformat()))
        
        conn.commit()
        flash('Secure link generated successfully', 'success')
        conn.close()
        return redirect(url_for('po_routes.send_to_supplier', po_id=po_id))
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error generating link: {str(e)}', 'danger')
        return redirect(url_for('po_routes.send_to_supplier', po_id=po_id))


@po_bp.route('/purchaseorders/<int:po_id>/email-link', methods=['POST'])
@login_required
@role_required('Admin', 'Procurement', 'Finance')
def email_supplier_link(po_id):
    """Email PO secure link to supplier"""
    db = Database()
    conn = db.get_connection()
    
    try:
        po = conn.execute('SELECT * FROM purchase_orders WHERE id = ?', (po_id,)).fetchone()
        if not po:
            conn.close()
            flash('Purchase order not found', 'danger')
            return redirect(url_for('po_routes.list_purchaseorders'))
        
        supplier = conn.execute('SELECT * FROM suppliers WHERE id = ?', (po['supplier_id'],)).fetchone()
        if not supplier or not supplier['email']:
            conn.close()
            flash('Supplier does not have an email address on file', 'warning')
            return redirect(url_for('po_routes.send_to_supplier', po_id=po_id))
        
        token_record = conn.execute('''
            SELECT * FROM po_supplier_tokens
            WHERE po_id = ? AND supplier_id = ? AND expires_at > ?
            ORDER BY created_at DESC LIMIT 1
        ''', (po_id, supplier['id'], datetime.now().isoformat())).fetchone()
        
        if not token_record:
            conn.close()
            flash('No valid link found. Please generate a link first.', 'warning')
            return redirect(url_for('po_routes.send_to_supplier', po_id=po_id))
        
        api_key, from_email = get_brevo_credentials()
        if not api_key or not from_email:
            conn.close()
            flash('Email service not configured. Please set BREVO_API_KEY and BREVO_FROM_EMAIL in Secrets.', 'danger')
            return redirect(url_for('po_routes.send_to_supplier', po_id=po_id))
        
        company = conn.execute('SELECT * FROM company_settings LIMIT 1').fetchone()
        company_name = company['company_name'] if company else 'Dynamic.IQ-COREx'
        
        base_url = request.url_root.rstrip('/')
        supplier_link = f"{base_url}/po/view/{token_record['token']}"
        
        lines = conn.execute('''
            SELECT pol.*, p.part_number, p.description
            FROM purchase_order_lines pol
            JOIN products p ON pol.product_id = p.id
            WHERE pol.po_id = ?
            ORDER BY pol.line_number
        ''', (po_id,)).fetchall()
        
        total_amount = sum(line['quantity'] * line['unit_price'] for line in lines)
        
        lines_html = ""
        for line in lines:
            line_total = line['quantity'] * line['unit_price']
            lines_html += f'''
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #e2e8f0;">{line['part_number']}</td>
                    <td style="padding: 10px; border-bottom: 1px solid #e2e8f0;">{line['description'][:40] if line['description'] else ''}</td>
                    <td style="padding: 10px; border-bottom: 1px solid #e2e8f0; text-align: right;">{line['quantity']}</td>
                    <td style="padding: 10px; border-bottom: 1px solid #e2e8f0; text-align: right;">${line['unit_price']:.2f}</td>
                    <td style="padding: 10px; border-bottom: 1px solid #e2e8f0; text-align: right;">${line_total:.2f}</td>
                </tr>
            '''
        
        html_content = f'''
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 0; background-color: #f8fafc; }}
        .container {{ max-width: 650px; margin: 0 auto; background-color: #ffffff; }}
        .header {{ background: linear-gradient(135deg, #1e3a5f 0%, #3b82f6 100%); color: white; padding: 30px; text-align: center; }}
        .header h1 {{ margin: 0; font-size: 24px; }}
        .content {{ padding: 30px; }}
        .po-box {{ background-color: #f1f5f9; border-radius: 8px; padding: 20px; margin: 20px 0; }}
        .po-box h3 {{ color: #1e3a5f; margin-top: 0; }}
        .detail-row {{ display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #e2e8f0; }}
        .detail-label {{ color: #64748b; }}
        .detail-value {{ color: #1e293b; font-weight: 500; }}
        .btn {{ display: inline-block; background: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%); color: white; padding: 15px 40px; 
                text-decoration: none; border-radius: 8px; font-weight: 600; margin: 20px 0; }}
        .btn:hover {{ background: linear-gradient(135deg, #2563eb 0%, #1e40af 100%); }}
        .footer {{ background-color: #f1f5f9; padding: 20px; text-align: center; font-size: 12px; color: #64748b; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th {{ background-color: #f1f5f9; padding: 10px; text-align: left; font-size: 12px; color: #64748b; text-transform: uppercase; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{company_name}</h1>
            <p style="margin: 10px 0 0; opacity: 0.9;">Purchase Order</p>
        </div>
        <div class="content">
            <p>Dear {supplier['name']},</p>
            <p>Please find below the details of our Purchase Order:</p>
            
            <div class="po-box">
                <h3>PO #{po['po_number']}</h3>
                <div class="detail-row">
                    <span class="detail-label">Order Date</span>
                    <span class="detail-value">{po['order_date'] if po['order_date'] else 'N/A'}</span>
                </div>
                <div class="detail-row">
                    <span class="detail-label">Expected Delivery</span>
                    <span class="detail-value">{po['expected_delivery_date'] if po['expected_delivery_date'] else 'TBD'}</span>
                </div>
                <div class="detail-row">
                    <span class="detail-label">Total Amount</span>
                    <span class="detail-value" style="font-size: 18px; color: #3b82f6;">${total_amount:.2f}</span>
                </div>
            </div>
            
            <h4>Order Items</h4>
            <table>
                <thead>
                    <tr>
                        <th>Part #</th>
                        <th>Description</th>
                        <th style="text-align: right;">Qty</th>
                        <th style="text-align: right;">Unit Price</th>
                        <th style="text-align: right;">Total</th>
                    </tr>
                </thead>
                <tbody>
                    {lines_html}
                </tbody>
            </table>
            
            <p style="margin-top: 25px;">Click the button below to view the complete Purchase Order online:</p>
            
            <div style="text-align: center;">
                <a href="{supplier_link}" class="btn">View Purchase Order</a>
            </div>
            
            <p>If you have any questions about this order, please contact us directly.</p>
            
            <p>Thank you for your partnership.</p>
            
            <p>Best regards,<br>
            <strong>{company_name}</strong></p>
        </div>
        <div class="footer">
            <p>This is an automated message from {company_name}.</p>
        </div>
    </div>
</body>
</html>
'''
        
        success, result = send_po_email_via_brevo(
            supplier['email'],
            supplier['name'],
            f"Purchase Order {po['po_number']} from {company_name}",
            html_content,
            from_email,
            company_name,
            api_key
        )
        
        if success:
            conn.execute('''
                UPDATE po_supplier_tokens 
                SET email_sent = 1, email_sent_at = ? 
                WHERE id = ?
            ''', (datetime.now().isoformat(), token_record['id']))
            conn.commit()
            flash(f'Purchase Order email sent successfully to {supplier["email"]}', 'success')
        else:
            flash(f'Failed to send email: {result}', 'danger')
        
        conn.close()
        return redirect(url_for('po_routes.send_to_supplier', po_id=po_id))
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error sending email: {str(e)}', 'danger')
        return redirect(url_for('po_routes.send_to_supplier', po_id=po_id))


@po_bp.route('/po/view/<token>')
def supplier_view_po(token):
    """Public portal for suppliers to view PO via token"""
    db = Database()
    conn = db.get_connection()
    
    token_record = conn.execute('''
        SELECT * FROM po_supplier_tokens WHERE token = ?
    ''', (token,)).fetchone()
    
    if not token_record:
        conn.close()
        return render_template('errors/invalid_token.html', 
                             message='This link is invalid or has expired.'), 404
    
    if datetime.fromisoformat(token_record['expires_at']) < datetime.now():
        conn.close()
        return render_template('errors/invalid_token.html',
                             message='This link has expired. Please contact the company for a new link.'), 410
    
    conn.execute('''
        UPDATE po_supplier_tokens SET last_accessed_at = ? WHERE id = ?
    ''', (datetime.now().isoformat(), token_record['id']))
    conn.commit()
    
    po = conn.execute('SELECT * FROM purchase_orders WHERE id = ?', (token_record['po_id'],)).fetchone()
    supplier = conn.execute('SELECT * FROM suppliers WHERE id = ?', (token_record['supplier_id'],)).fetchone()
    company = conn.execute('SELECT * FROM company_settings LIMIT 1').fetchone()
    
    lines = conn.execute('''
        SELECT pol.*, p.part_number, p.description, u.uom_code
        FROM purchase_order_lines pol
        JOIN products p ON pol.product_id = p.id
        LEFT JOIN uom_master u ON pol.uom_id = u.id
        WHERE pol.po_id = ?
        ORDER BY pol.line_number
    ''', (token_record['po_id'],)).fetchall()
    
    total_amount = sum(line['quantity'] * line['unit_price'] for line in lines)
    
    conn.close()
    
    return render_template('purchaseorders/supplier_portal.html',
                         po=po,
                         supplier=supplier,
                         company=company,
                         lines=lines,
                         total_amount=total_amount)
