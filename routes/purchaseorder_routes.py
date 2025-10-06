from flask import Blueprint, render_template, request, redirect, url_for, flash, make_response, session
from models import Database, CompanySettings, AuditLogger
from mrp_logic import MRPEngine
from auth import login_required, role_required
from datetime import datetime

po_bp = Blueprint('po_routes', __name__)

@po_bp.route('/purchaseorders')
@login_required
def list_purchaseorders():
    db = Database()
    conn = db.get_connection()
    
    # Get PO headers with supplier info and line counts
    purchase_orders = conn.execute('''
        SELECT po.*, s.name as supplier_name,
               COUNT(pol.id) as line_count,
               COALESCE(SUM(pol.quantity * pol.unit_price), 0) as total_amount
        FROM purchase_orders po
        JOIN suppliers s ON po.supplier_id = s.id
        LEFT JOIN purchase_order_lines pol ON po.id = pol.po_id
        GROUP BY po.id
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
                
                # Insert line items
                for line_num, line_data in sorted(lines.items()):
                    conn.execute('''
                        INSERT INTO purchase_order_lines
                        (po_id, line_number, product_id, quantity, unit_price, uom_id)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        po_id,
                        int(line_num),
                        int(line_data['product_id']),
                        float(line_data['quantity']),
                        float(line_data['unit_price']),
                        int(line_data['uom_id']) if line_data.get('uom_id') else None
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
    uoms = conn.execute('SELECT * FROM uom_master WHERE is_active = 1 ORDER BY uom_code').fetchall()
    
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
    
    # Get line items with product and UOM info
    lines = conn.execute('''
        SELECT pol.*, p.code as product_code, p.name as product_name, p.unit_of_measure,
               uom.uom_code, uom.uom_name,
               i.quantity as inventory_quantity
        FROM purchase_order_lines pol
        JOIN products p ON pol.product_id = p.id
        LEFT JOIN uom_master uom ON pol.uom_id = uom.id
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
    
    conn.close()
    
    return render_template('purchaseorders/view.html', po=po, lines=lines, ap_records=ap_records)

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
                    
                    # Update existing line
                    conn.execute('''
                        UPDATE purchase_order_lines
                        SET line_number = ?, product_id = ?, uom_id = ?, quantity = ?, unit_price = ?
                        WHERE id = ?
                    ''', (int(line_num), product_id, uom_id, quantity, unit_price, line_id))
                    
                    new_lines_data.append({
                        'id': line_id,
                        'line_number': int(line_num),
                        'product_id': product_id,
                        'quantity': quantity,
                        'unit_price': unit_price,
                        'received_quantity': received_qty
                    })
                else:
                    # New line - INSERT it
                    cursor = conn.execute('''
                        INSERT INTO purchase_order_lines 
                        (po_id, line_number, product_id, uom_id, quantity, unit_price, received_quantity)
                        VALUES (?, ?, ?, ?, ?, ?, 0)
                    ''', (id, int(line_num), product_id, uom_id, quantity, unit_price))
                    
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
            
            audit_logger = AuditLogger(conn)
            
            # Log header changes
            audit_logger.log_update(
                record_type='purchase_order',
                record_id=id,
                old_data=old_po_data,
                new_data=new_po_data,
                user_id=session['user_id']
            )
            
            # Log line item changes if different
            if old_lines_data != new_lines_data:
                audit_logger.log_update(
                    record_type='purchase_order_lines',
                    record_id=id,
                    old_data={'lines': old_lines_data},
                    new_data={'lines': new_lines_data},
                    user_id=session['user_id']
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
    uoms = conn.execute('SELECT * FROM uom_master WHERE is_active = 1 ORDER BY uom_code').fetchall()
    
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
