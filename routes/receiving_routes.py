from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import Database, GLAutoPost, AuditLogger
from auth import login_required, role_required
from datetime import datetime, timedelta

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
    if request.method == 'POST':
        db = Database()
        conn = db.get_connection()
        try:
            po_line_id = int(request.form['po_line_id'])
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
            
            # Get PO Line details with PO header, product, and UOM conversion info
            po_line = conn.execute('''
                SELECT pol.*, po.po_number, po.supplier_id, po.order_date,
                       p.name as product_name, p.code as product_code,
                       uom.uom_code as order_uom_code, uom.uom_name as order_uom_name,
                       base_uom.uom_code as base_uom_code, base_uom.uom_name as base_uom_name
                FROM purchase_order_lines pol
                JOIN purchase_orders po ON pol.po_id = po.id
                JOIN products p ON pol.product_id = p.id
                LEFT JOIN uom_master uom ON pol.uom_id = uom.id
                LEFT JOIN uom_master base_uom ON pol.base_uom_id = base_uom.id
                WHERE pol.id = ?
            ''', (po_line_id,)).fetchone()
            
            if not po_line:
                flash('Purchase Order Line not found.', 'danger')
                conn.close()
                return redirect(url_for('receiving_routes.create_receiving'))
            
            # Validate quantity
            received_so_far = po_line['received_quantity'] if po_line['received_quantity'] else 0
            remaining = po_line['quantity'] - received_so_far
            
            if quantity_received > remaining:
                flash(f'Cannot receive {quantity_received} units. Only {remaining} units remaining on this line.', 'danger')
                conn.close()
                return redirect(url_for('receiving_routes.create_receiving'))
            
            po_id = po_line['po_id']
            product_id = po_line['product_id']
            
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
            
            # Calculate UOM conversion info for audit trail
            conversion_factor = po_line['conversion_factor_used'] if po_line['conversion_factor_used'] else 1.0
            base_quantity_for_receipt = quantity_received * conversion_factor
            receiving_uom_id = po_line['uom_id']  # Receiving in PO UOM by default
            
            # Calculate unit cost at receipt from PO line (preserves cost allocation)
            base_unit_price = po_line.get('base_unit_price')
            if base_unit_price is None and po_line['unit_price']:
                # Fallback calculation if base_unit_price not set
                extended = po_line['quantity'] * po_line['unit_price']
                base_qty = po_line['base_quantity'] or po_line['quantity']
                base_unit_price = extended / base_qty if base_qty > 0 else po_line['unit_price']
            unit_cost_at_receipt = round(base_unit_price or 0, 6)
            
            # Create receiving transaction with UOM audit trail
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO receiving_transactions 
                (receipt_number, po_id, product_id, quantity_received, receipt_date, 
                 packing_slip_number, shipment_tracking, warehouse_location, bin_location, 
                 receiver_name, condition, remarks, received_by,
                 po_line_id, receiving_uom_id, conversion_factor_used, base_quantity_received, unit_cost_at_receipt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (receipt_number, po_id, product_id, quantity_received, receipt_date,
                  packing_slip, tracking, warehouse, bin_location, receiver, condition, remarks, session['user_id'],
                  po_line_id, receiving_uom_id, conversion_factor, base_quantity_for_receipt, unit_cost_at_receipt))
            
            receipt_id = cursor.lastrowid
            
            # Log audit trail for receiving transaction
            AuditLogger.log_change(
                conn=conn,
                record_type='receiving_transaction',
                record_id=receipt_id,
                action_type='Created',
                modified_by=session.get('user_id'),
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
            
            # Get old PO line record for audit before update
            old_po_line = conn.execute('SELECT * FROM purchase_order_lines WHERE id = ?', (po_line_id,)).fetchone()
            
            # Update PO line received quantity
            new_line_received = received_so_far + quantity_received
            conn.execute('''
                UPDATE purchase_order_lines 
                SET received_quantity = ?
                WHERE id = ?
            ''', (new_line_received, po_line_id))
            
            # Get new PO line record for audit and log changes
            new_po_line = conn.execute('SELECT * FROM purchase_order_lines WHERE id = ?', (po_line_id,)).fetchone()
            po_line_changes = AuditLogger.compare_records(dict(old_po_line), dict(new_po_line))
            if po_line_changes:
                AuditLogger.log_change(
                    conn=conn,
                    record_type='purchase_order_line',
                    record_id=po_line_id,
                    action_type='Updated',
                    modified_by=session.get('user_id'),
                    changed_fields=po_line_changes,
                    ip_address=request.remote_addr,
                    user_agent=request.headers.get('User-Agent')
                )
            
            # Update PO header delivery date and status based on all lines
            conn.execute('''
                UPDATE purchase_orders 
                SET actual_delivery_date = CASE WHEN actual_delivery_date IS NULL THEN ? ELSE actual_delivery_date END
                WHERE id = ?
            ''', (receipt_date, po_id))
            
            # Check if all lines are fully received to update PO status
            all_lines_received = conn.execute('''
                SELECT COUNT(*) as incomplete_count
                FROM purchase_order_lines
                WHERE po_id = ? AND (received_quantity IS NULL OR received_quantity < quantity)
            ''', (po_id,)).fetchone()['incomplete_count']
            
            if all_lines_received == 0:
                conn.execute('UPDATE purchase_orders SET status = ? WHERE id = ?', ('Received', po_id))
            
            # Use the already calculated conversion values from earlier
            # base_quantity_for_receipt and unit_cost_at_receipt already computed above
            
            # Update inventory with base quantity
            inventory = conn.execute('''
                SELECT * FROM inventory WHERE product_id = ?
            ''', (product_id,)).fetchone()
            
            if inventory:
                new_qty = inventory['quantity'] + base_quantity_for_receipt
                conn.execute('''
                    UPDATE inventory 
                    SET quantity = ?,
                        unit_cost = ?,
                        last_received_date = ?,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE product_id = ?
                ''', (new_qty, unit_cost_at_receipt, receipt_date, product_id))
            else:
                # Create inventory record with location info using base quantity
                conn.execute('''
                    INSERT INTO inventory 
                    (product_id, quantity, unit_cost, condition, warehouse_location, bin_location, last_received_date, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'Available')
                ''', (product_id, base_quantity_for_receipt, unit_cost_at_receipt, condition, warehouse, bin_location, receipt_date))
            
            # Update base received quantity on PO line
            base_received_so_far = po_line['base_received_quantity'] if po_line['base_received_quantity'] else 0
            new_base_received = base_received_so_far + base_quantity_for_receipt
            conn.execute('''
                UPDATE purchase_order_lines 
                SET base_received_quantity = ?
                WHERE id = ?
            ''', (new_base_received, po_line_id))
            
            # Auto-post GL entry for receiving
            # Debit: Inventory (increase asset)
            # Credit: Accounts Payable (increase liability)
            total_value = quantity_received * po_line['unit_price']
            gl_lines = [
                {
                    'account_code': '1130',  # Inventory
                    'debit': total_value,
                    'credit': 0,
                    'description': f'Material received - {po_line["product_name"]} ({receipt_number})'
                },
                {
                    'account_code': '2110',  # Accounts Payable
                    'debit': 0,
                    'credit': total_value,
                    'description': f'AP for material received - {po_line["product_name"]} ({receipt_number})'
                }
            ]
            
            gl_entry_id = GLAutoPost.create_auto_journal_entry(
                conn=conn,
                entry_date=receipt_date,
                description=f'Material Receiving - {receipt_number}',
                transaction_source='Material Receiving',
                reference_type='receiving_transaction',
                reference_id=receipt_id,
                lines=gl_lines,
                created_by=session['user_id']
            )
            
            # Verify GL entry was created successfully before proceeding with A/P
            if not gl_entry_id:
                raise Exception("Failed to create GL entry for receiving transaction")
            
            # Auto-create Accounts Payable (Vendor Invoice) record
            # Check if AP already exists for this receipt (by linking to receipt_id via GL entry)
            # A receipt can only have one AP record
            existing_ap = conn.execute('''
                SELECT vi.id, vi.invoice_number FROM vendor_invoices vi
                JOIN gl_entries ge ON vi.gl_entry_id = ge.id
                WHERE ge.reference_type = 'receiving_transaction' AND ge.reference_id = ?
            ''', (receipt_id,)).fetchone()
            
            if existing_ap:
                # Duplicate receipt - reuse existing AP
                conn.commit()
                flash(f'Material received successfully! Receipt: {receipt_number}, Linked to existing A/P: {existing_ap["invoice_number"]}', 'success')
                return redirect(url_for('receiving_routes.view_receiving', receipt_number=receipt_number))
            
            # Use default payment terms (30 days Net)
            payment_terms_days = 30
            
            # Calculate due date based on payment terms
            receipt_dt = datetime.strptime(receipt_date, '%Y-%m-%d')
            due_date = (receipt_dt + timedelta(days=payment_terms_days)).strftime('%Y-%m-%d')
            
            # Generate unique AP number (only after confirming no duplicate)
            last_ap = conn.execute('''
                SELECT invoice_number FROM vendor_invoices 
                WHERE invoice_number LIKE 'AP-%'
                ORDER BY CAST(SUBSTR(invoice_number, 4) AS INTEGER) DESC 
                LIMIT 1
            ''').fetchone()
            
            if last_ap:
                try:
                    last_number = int(last_ap['invoice_number'].split('-')[1])
                    next_number = last_number + 1
                except (ValueError, IndexError):
                    next_number = 1
            else:
                next_number = 1
            
            ap_number = f'AP-{next_number:07d}'
            
            # Create vendor invoice (A/P record)
            # Note: amount_paid is initialized to 0 and should be updated via payment processing workflow
            cursor.execute('''
                INSERT INTO vendor_invoices 
                (invoice_number, vendor_id, po_id, invoice_date, due_date, 
                 amount, tax_amount, total_amount, amount_paid, status, gl_entry_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                ap_number,
                po_line['supplier_id'],
                po_id,
                receipt_date,
                due_date,
                total_value,
                0,  # tax_amount - can be enhanced later
                total_value,
                0,  # amount_paid - updated by payment processing
                'Pending Invoice',
                gl_entry_id
            ))
            
            ap_id = cursor.lastrowid
            
            # Log audit trail for A/P creation
            AuditLogger.log_change(
                conn=conn,
                record_type='accounts_payable',
                record_id=ap_id,
                action_type='Created',
                modified_by=session.get('user_id'),
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
            
            conn.commit()
            flash(f'Material received successfully! Receipt: {receipt_number}, A/P: {ap_number} created', 'success')
            return redirect(url_for('receiving_routes.view_receiving', receipt_number=receipt_number))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error receiving material: {str(e)}', 'danger')
        finally:
            conn.close()
        
        return redirect(url_for('receiving_routes.create_receiving'))
    
    # GET request - show form
    db = Database()
    conn = db.get_connection()
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Get pending/ordered PO lines with remaining quantities to receive
    pos = conn.execute('''
        SELECT 
            pol.id as line_id,
            pol.po_id,
            pol.product_id,
            pol.quantity,
            pol.unit_price,
            pol.line_number,
            COALESCE(pol.received_quantity, 0) as received_so_far,
            (pol.quantity - COALESCE(pol.received_quantity, 0)) as remaining_quantity,
            po.po_number,
            po.order_date,
            po.expected_delivery_date,
            po.status,
            s.name as supplier_name,
            p.code as product_code,
            p.name as product_name,
            p.unit_of_measure
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        JOIN suppliers s ON po.supplier_id = s.id
        JOIN products p ON pol.product_id = p.id
        WHERE po.status IN ('Ordered', 'Partially Received')
            AND (pol.received_quantity IS NULL OR pol.received_quantity < pol.quantity)
        ORDER BY po.expected_delivery_date, po.order_date DESC, pol.line_number
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
            p.code as product_code,
            p.name as product_name,
            p.unit_of_measure,
            s.name as supplier_name,
            s.contact_person,
            s.phone,
            s.email,
            u.username as received_by_name,
            pol.unit_price,
            pol.quantity as po_quantity,
            COALESCE(pol.received_quantity, 0) as po_received_quantity,
            ruom.uom_code as receiving_uom_code,
            ruom.uom_name as receiving_uom_name
        FROM receiving_transactions rt
        JOIN purchase_orders po ON rt.po_id = po.id
        JOIN products p ON rt.product_id = p.id
        JOIN suppliers s ON po.supplier_id = s.id
        LEFT JOIN users u ON rt.received_by = u.id
        LEFT JOIN purchase_order_lines pol ON pol.po_id = rt.po_id AND pol.product_id = rt.product_id
        LEFT JOIN uom_master ruom ON rt.receiving_uom_id = ruom.id
        WHERE rt.receipt_number = ?
    ''', (receipt_number,)).fetchone()
    
    if not receipt:
        flash('Receipt not found.', 'danger')
        conn.close()
        return redirect(url_for('receiving_routes.list_receiving'))
    
    conn.close()
    return render_template('receiving/view.html', receipt=receipt)
