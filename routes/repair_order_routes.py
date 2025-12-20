from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from models import Database
from auth import role_required
from datetime import datetime, date
import json

repair_order_bp = Blueprint('repair_order_routes', __name__)

REPAIR_TYPES = [
    'Inspection', 'Repair', 'Overhaul', 'Calibration', 'Welding', 
    'NDT', 'Plating/Coating', 'Heat Treatment', 'Testing', 'Certification'
]

PRIORITIES = ['Routine', 'AOG', 'Critical']

RO_STATUSES = [
    'Draft', 'Approved', 'Awaiting Shipment', 'Shipped', 
    'In Repair', 'Completed', 'Received', 'Closed'
]

LINE_STATUSES = ['Pending', 'Shipped', 'In Repair', 'Repaired', 'Received']

CONDITIONS = ['Serviceable', 'Unserviceable', 'Repairable', 'Beyond Repair', 'Quarantine']

def generate_ro_number(conn):
    result = conn.execute('''
        SELECT ro_number FROM repair_orders 
        WHERE ro_number LIKE 'RO-%' 
        ORDER BY id DESC LIMIT 1
    ''').fetchone()
    
    if result:
        try:
            last_num = int(result['ro_number'].split('-')[1])
            return f"RO-{last_num + 1:06d}"
        except:
            pass
    return "RO-000001"

def log_ro_audit(conn, ro_id, action_type, description, old_status=None, new_status=None, changed_fields=None):
    try:
        conn.execute('''
            INSERT INTO repair_order_audit (ro_id, action_type, action_description, old_status, new_status, changed_fields, performed_by, ip_address)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (ro_id, action_type, description, old_status, new_status, 
              json.dumps(changed_fields) if changed_fields else None,
              current_user.id if current_user.is_authenticated else None,
              request.remote_addr))
    except Exception as e:
        print(f"RO Audit log error: {e}")

@repair_order_bp.route('/repair-orders')
@login_required
def list_repair_orders():
    db = Database()
    conn = db.get_connection()
    
    status_filter = request.args.get('status', '')
    vendor_filter = request.args.get('vendor', '')
    priority_filter = request.args.get('priority', '')
    repair_type_filter = request.args.get('repair_type', '')
    
    query = '''
        SELECT ro.*, s.name as vendor_name, s.code as vendor_code,
               u.username as created_by_name,
               (SELECT COUNT(*) FROM repair_order_lines WHERE ro_id = ro.id) as line_count,
               wo.wo_number as related_wo_number
        FROM repair_orders ro
        LEFT JOIN suppliers s ON ro.vendor_id = s.id
        LEFT JOIN users u ON ro.created_by = u.id
        LEFT JOIN work_orders wo ON ro.related_work_order_id = wo.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND ro.status = ?'
        params.append(status_filter)
    if vendor_filter:
        query += ' AND ro.vendor_id = ?'
        params.append(int(vendor_filter))
    if priority_filter:
        query += ' AND ro.priority = ?'
        params.append(priority_filter)
    if repair_type_filter:
        query += ' AND ro.repair_type = ?'
        params.append(repair_type_filter)
    
    query += ' ORDER BY ro.created_at DESC'
    
    repair_orders = conn.execute(query, params).fetchall()
    vendors = conn.execute('SELECT id, code, name FROM suppliers ORDER BY name').fetchall()
    
    stats = {
        'total': len(repair_orders),
        'draft': sum(1 for ro in repair_orders if ro['status'] == 'Draft'),
        'in_progress': sum(1 for ro in repair_orders if ro['status'] in ['Approved', 'Awaiting Shipment', 'Shipped', 'In Repair']),
        'completed': sum(1 for ro in repair_orders if ro['status'] in ['Completed', 'Received', 'Closed']),
        'total_estimated': sum(ro['estimated_total_cost'] or 0 for ro in repair_orders),
        'total_actual': sum(ro['actual_total_cost'] or 0 for ro in repair_orders)
    }
    
    conn.close()
    return render_template('repair_orders/list.html',
        repair_orders=repair_orders,
        vendors=vendors,
        statuses=RO_STATUSES,
        priorities=PRIORITIES,
        repair_types=REPAIR_TYPES,
        status_filter=status_filter,
        vendor_filter=vendor_filter,
        priority_filter=priority_filter,
        repair_type_filter=repair_type_filter,
        stats=stats
    )

@repair_order_bp.route('/repair-orders/create', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement', 'Production Staff')
def create_repair_order():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        ro_number = generate_ro_number(conn)
        vendor_id = request.form.get('vendor_id') or None
        repair_type = request.form['repair_type']
        priority = request.form.get('priority', 'Routine')
        expected_tat = request.form.get('expected_tat_days') or None
        currency = request.form.get('currency', 'USD')
        payment_terms = request.form.get('payment_terms', '')
        vendor_quote_ref = request.form.get('vendor_quote_ref', '')
        vendor_notes = request.form.get('vendor_notes', '')
        internal_notes = request.form.get('internal_notes', '')
        ship_to_address = request.form.get('ship_to_address', '')
        related_wo_id = request.form.get('related_work_order_id') or None
        
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO repair_orders (
                ro_number, vendor_id, repair_type, priority, status,
                related_work_order_id, expected_tat_days, currency, payment_terms,
                vendor_quote_ref, vendor_notes, internal_notes, ship_to_address, created_by
            ) VALUES (?, ?, ?, ?, 'Draft', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (ro_number, vendor_id, repair_type, priority, related_wo_id,
              expected_tat, currency, payment_terms, vendor_quote_ref,
              vendor_notes, internal_notes, ship_to_address, current_user.id))
        
        ro_id = cursor.lastrowid
        conn.commit()
        
        log_ro_audit(conn, ro_id, 'CREATE', f'Repair Order {ro_number} created')
        conn.commit()
        
        conn.close()
        flash(f'Repair Order {ro_number} created successfully.', 'success')
        return redirect(url_for('repair_order_routes.view_repair_order', id=ro_id))
    
    vendors = conn.execute('SELECT id, code, name FROM suppliers ORDER BY name').fetchall()
    work_orders = conn.execute('''
        SELECT id, wo_number, product_id FROM work_orders 
        WHERE status NOT IN ('Completed', 'Cancelled', 'Closed') 
        ORDER BY wo_number DESC LIMIT 100
    ''').fetchall()
    
    from_inventory = request.args.get('inventory_id')
    from_wo = request.args.get('work_order_id')
    from_wo_task = request.args.get('wo_task_id')
    
    prefill = {}
    if from_inventory:
        inv = conn.execute('''
            SELECT i.*, p.code, p.name, p.description 
            FROM inventory i JOIN products p ON i.product_id = p.id WHERE i.id = ?
        ''', (from_inventory,)).fetchone()
        if inv:
            prefill['inventory'] = dict(inv)
    
    if from_wo:
        wo = conn.execute('SELECT * FROM work_orders WHERE id = ?', (from_wo,)).fetchone()
        if wo:
            prefill['work_order'] = dict(wo)
    
    conn.close()
    return render_template('repair_orders/create.html',
        vendors=vendors,
        work_orders=work_orders,
        repair_types=REPAIR_TYPES,
        priorities=PRIORITIES,
        conditions=CONDITIONS,
        prefill=prefill
    )

@repair_order_bp.route('/repair-orders/<int:id>')
@login_required
def view_repair_order(id):
    db = Database()
    conn = db.get_connection()
    
    ro = conn.execute('''
        SELECT ro.*, s.name as vendor_name, s.code as vendor_code, s.address as vendor_address,
               s.email as vendor_email, s.phone as vendor_phone,
               u.username as created_by_name, a.username as approved_by_name,
               wo.wo_number as related_wo_number
        FROM repair_orders ro
        LEFT JOIN suppliers s ON ro.vendor_id = s.id
        LEFT JOIN users u ON ro.created_by = u.id
        LEFT JOIN users a ON ro.approved_by = a.id
        LEFT JOIN work_orders wo ON ro.related_work_order_id = wo.id
        WHERE ro.id = ?
    ''', (id,)).fetchone()
    
    if not ro:
        conn.close()
        flash('Repair Order not found.', 'danger')
        return redirect(url_for('repair_order_routes.list_repair_orders'))
    
    lines = conn.execute('''
        SELECT rol.*, p.code as part_code, p.name as part_name,
               inv.serial_number as inv_serial, inv.lot_number as inv_lot,
               wo.wo_number as source_wo_number, u.username as received_by_name
        FROM repair_order_lines rol
        LEFT JOIN products p ON rol.product_id = p.id
        LEFT JOIN inventory inv ON rol.inventory_id = inv.id
        LEFT JOIN work_orders wo ON rol.linked_work_order_id = wo.id
        LEFT JOIN users u ON rol.received_by = u.id
        WHERE rol.ro_id = ?
        ORDER BY rol.id
    ''', (id,)).fetchall()
    
    documents = conn.execute('''
        SELECT rod.*, u.username as uploaded_by_name
        FROM repair_order_documents rod
        LEFT JOIN users u ON rod.uploaded_by = u.id
        WHERE rod.ro_id = ? AND rod.is_active = 1
        ORDER BY rod.uploaded_at DESC
    ''', (id,)).fetchall()
    
    shipments = conn.execute('''
        SELECT ros.*, u.username as shipped_by_name
        FROM repair_order_shipments ros
        LEFT JOIN users u ON ros.shipped_by = u.id
        WHERE ros.ro_id = ?
        ORDER BY ros.created_at DESC
    ''', (id,)).fetchall()
    
    audit_trail = conn.execute('''
        SELECT roa.*, u.username as performed_by_name
        FROM repair_order_audit roa
        LEFT JOIN users u ON roa.performed_by = u.id
        WHERE roa.ro_id = ?
        ORDER BY roa.performed_at DESC
        LIMIT 50
    ''', (id,)).fetchall()
    
    conn.close()
    return render_template('repair_orders/view.html',
        ro=ro,
        lines=lines,
        documents=documents,
        shipments=shipments,
        audit_trail=audit_trail,
        statuses=RO_STATUSES,
        line_statuses=LINE_STATUSES,
        conditions=CONDITIONS
    )

@repair_order_bp.route('/repair-orders/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement', 'Production Staff')
def edit_repair_order(id):
    db = Database()
    conn = db.get_connection()
    
    ro = conn.execute('SELECT * FROM repair_orders WHERE id = ?', (id,)).fetchone()
    if not ro:
        conn.close()
        flash('Repair Order not found.', 'danger')
        return redirect(url_for('repair_order_routes.list_repair_orders'))
    
    if ro['status'] in ['Closed']:
        conn.close()
        flash('Cannot edit a closed Repair Order.', 'warning')
        return redirect(url_for('repair_order_routes.view_repair_order', id=id))
    
    if request.method == 'POST':
        vendor_id = request.form.get('vendor_id') or None
        repair_type = request.form['repair_type']
        priority = request.form.get('priority', 'Routine')
        expected_tat = request.form.get('expected_tat_days') or None
        currency = request.form.get('currency', 'USD')
        payment_terms = request.form.get('payment_terms', '')
        vendor_quote_ref = request.form.get('vendor_quote_ref', '')
        vendor_notes = request.form.get('vendor_notes', '')
        internal_notes = request.form.get('internal_notes', '')
        ship_to_address = request.form.get('ship_to_address', '')
        
        conn.execute('''
            UPDATE repair_orders SET
                vendor_id = ?, repair_type = ?, priority = ?, expected_tat_days = ?,
                currency = ?, payment_terms = ?, vendor_quote_ref = ?, vendor_notes = ?,
                internal_notes = ?, ship_to_address = ?, last_updated = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (vendor_id, repair_type, priority, expected_tat, currency, payment_terms,
              vendor_quote_ref, vendor_notes, internal_notes, ship_to_address, id))
        
        log_ro_audit(conn, id, 'UPDATE', f'Repair Order {ro["ro_number"]} updated')
        conn.commit()
        conn.close()
        
        flash('Repair Order updated successfully.', 'success')
        return redirect(url_for('repair_order_routes.view_repair_order', id=id))
    
    vendors = conn.execute('SELECT id, code, name FROM suppliers ORDER BY name').fetchall()
    conn.close()
    
    return render_template('repair_orders/edit.html',
        ro=ro,
        vendors=vendors,
        repair_types=REPAIR_TYPES,
        priorities=PRIORITIES
    )

@repair_order_bp.route('/repair-orders/<int:id>/add-line', methods=['POST'])
@role_required('Admin', 'Procurement', 'Production Staff')
def add_repair_order_line(id):
    db = Database()
    conn = db.get_connection()
    
    ro = conn.execute('SELECT * FROM repair_orders WHERE id = ?', (id,)).fetchone()
    if not ro or ro['status'] in ['Shipped', 'In Repair', 'Completed', 'Received', 'Closed']:
        conn.close()
        flash('Cannot add lines to this Repair Order.', 'warning')
        return redirect(url_for('repair_order_routes.view_repair_order', id=id))
    
    product_id = request.form.get('product_id') or None
    inventory_id = request.form.get('inventory_id') or None
    part_number = request.form.get('part_number', '')
    description = request.form.get('description', '')
    serial_number = request.form.get('serial_number', '')
    lot_number = request.form.get('lot_number', '')
    quantity = int(request.form.get('quantity', 1))
    condition_at_removal = request.form.get('condition_at_removal', '')
    reason_for_repair = request.form.get('reason_for_repair', '')
    requested_services = request.form.get('requested_services', '')
    estimated_cost = float(request.form.get('estimated_cost', 0) or 0)
    linked_wo_id = request.form.get('linked_work_order_id') or None
    
    if inventory_id:
        inv = conn.execute('''
            SELECT i.*, p.code, p.name FROM inventory i 
            JOIN products p ON i.product_id = p.id WHERE i.id = ?
        ''', (inventory_id,)).fetchone()
        if inv:
            if inv['status'] not in ['Available', 'Serviceable']:
                conn.close()
                flash(f'Cannot add item: Inventory status is "{inv["status"]}". Only available items can be sent for repair.', 'warning')
                return redirect(url_for('repair_order_routes.view_repair_order', id=id))
            
            product_id = inv['product_id']
            part_number = inv['code']
            description = inv['name']
            serial_number = inv['serial_number'] or serial_number
            lot_number = inv['lot_number'] or lot_number
            old_inv_status = inv['status']
            conn.execute('''
                UPDATE inventory SET status = 'On Repair Order', last_updated = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (inventory_id,))
            log_ro_audit(conn, id, 'INVENTORY_LOCKED', f'Inventory {part_number} (ID:{inventory_id}) locked for repair. Previous status: {old_inv_status}',
                        changed_fields={'inventory_id': inventory_id, 'old_status': old_inv_status, 'new_status': 'On Repair Order'})
    
    conn.execute('''
        INSERT INTO repair_order_lines (
            ro_id, product_id, inventory_id, part_number, description, serial_number,
            lot_number, quantity, condition_at_removal, reason_for_repair,
            requested_services, estimated_cost, linked_work_order_id, line_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Pending')
    ''', (id, product_id, inventory_id, part_number, description, serial_number,
          lot_number, quantity, condition_at_removal, reason_for_repair,
          requested_services, estimated_cost, linked_wo_id))
    
    total_estimated = conn.execute(
        'SELECT COALESCE(SUM(estimated_cost), 0) as total FROM repair_order_lines WHERE ro_id = ?', (id,)
    ).fetchone()['total']
    conn.execute('UPDATE repair_orders SET estimated_total_cost = ? WHERE id = ?', (total_estimated, id))
    
    log_ro_audit(conn, id, 'ADD_LINE', f'Added line item: {part_number or description}')
    conn.commit()
    conn.close()
    
    flash('Line item added successfully.', 'success')
    return redirect(url_for('repair_order_routes.view_repair_order', id=id))

@repair_order_bp.route('/repair-orders/<int:id>/update-status', methods=['POST'])
@role_required('Admin', 'Procurement', 'Production Staff')
def update_ro_status(id):
    db = Database()
    conn = db.get_connection()
    
    ro = conn.execute('SELECT * FROM repair_orders WHERE id = ?', (id,)).fetchone()
    if not ro:
        conn.close()
        flash('Repair Order not found.', 'danger')
        return redirect(url_for('repair_order_routes.list_repair_orders'))
    
    new_status = request.form.get('status')
    old_status = ro['status']
    
    if new_status not in RO_STATUSES:
        conn.close()
        flash('Invalid status.', 'danger')
        return redirect(url_for('repair_order_routes.view_repair_order', id=id))
    
    valid_transitions = {
        'Draft': ['Approved'],
        'Approved': ['Awaiting Shipment', 'Shipped'],
        'Awaiting Shipment': ['Shipped'],
        'Shipped': ['In Repair'],
        'In Repair': ['Completed'],
        'Completed': ['Received'],
        'Received': ['Closed'],
        'Closed': []
    }
    
    if new_status not in valid_transitions.get(old_status, []):
        conn.close()
        flash(f'Cannot transition from {old_status} to {new_status}. Invalid workflow transition.', 'danger')
        return redirect(url_for('repair_order_routes.view_repair_order', id=id))
    
    line_count = conn.execute('SELECT COUNT(*) as cnt FROM repair_order_lines WHERE ro_id = ?', (id,)).fetchone()['cnt']
    if new_status == 'Approved' and line_count == 0:
        conn.close()
        flash('Cannot approve: Add at least one line item before approving.', 'warning')
        return redirect(url_for('repair_order_routes.view_repair_order', id=id))
    
    if new_status == 'Received':
        unreceived = conn.execute('''
            SELECT COUNT(*) as cnt FROM repair_order_lines WHERE ro_id = ? AND line_status != 'Received'
        ''', (id,)).fetchone()['cnt']
        if unreceived > 0:
            conn.close()
            flash(f'Cannot mark as Received: {unreceived} line item(s) have not been received yet.', 'warning')
            return redirect(url_for('repair_order_routes.view_repair_order', id=id))
    
    update_fields = {'status': new_status}
    
    if new_status == 'Approved':
        update_fields['approved_by'] = current_user.id
        update_fields['approved_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn.execute('''
            UPDATE inventory SET status = 'Awaiting Repair Shipment', last_updated = CURRENT_TIMESTAMP
            WHERE id IN (SELECT inventory_id FROM repair_order_lines WHERE ro_id = ? AND inventory_id IS NOT NULL)
        ''', (id,))
    elif new_status == 'In Repair':
        conn.execute('UPDATE repair_order_lines SET line_status = ? WHERE ro_id = ?', ('In Repair', id))
    elif new_status == 'Completed':
        update_fields['completed_date'] = date.today().isoformat()
        conn.execute('UPDATE repair_order_lines SET line_status = ? WHERE ro_id = ? AND line_status != ?', 
                     ('Repaired', id, 'Received'))
    elif new_status == 'Received':
        update_fields['received_date'] = date.today().isoformat()
    elif new_status == 'Closed':
        update_fields['closed_date'] = date.today().isoformat()
    
    set_clause = ', '.join([f'{k} = ?' for k in update_fields.keys()])
    conn.execute(f'UPDATE repair_orders SET {set_clause}, last_updated = CURRENT_TIMESTAMP WHERE id = ?',
                 list(update_fields.values()) + [id])
    
    log_ro_audit(conn, id, 'STATUS_CHANGE', f'Status changed from {old_status} to {new_status}', 
                 old_status, new_status, {'transition': f'{old_status} -> {new_status}'})
    conn.commit()
    conn.close()
    
    flash(f'Status updated to {new_status}.', 'success')
    return redirect(url_for('repair_order_routes.view_repair_order', id=id))

@repair_order_bp.route('/repair-orders/<int:id>/ship', methods=['POST'])
@role_required('Admin', 'Procurement', 'Shipping')
def create_ro_shipment(id):
    db = Database()
    conn = db.get_connection()
    
    ro = conn.execute('SELECT * FROM repair_orders WHERE id = ?', (id,)).fetchone()
    if not ro:
        conn.close()
        flash('Repair Order not found.', 'danger')
        return redirect(url_for('repair_order_routes.list_repair_orders'))
    
    carrier = request.form.get('carrier', '')
    tracking_number = request.form.get('tracking_number', '')
    ship_date = request.form.get('ship_date', date.today().isoformat())
    estimated_arrival = request.form.get('estimated_arrival', '')
    shipping_cost = float(request.form.get('shipping_cost', 0) or 0)
    package_count = int(request.form.get('package_count', 1) or 1)
    weight = request.form.get('weight') or None
    special_instructions = request.form.get('special_instructions', '')
    
    conn.execute('''
        INSERT INTO repair_order_shipments (
            ro_id, shipment_type, carrier, tracking_number, ship_date, estimated_arrival,
            shipping_cost, package_count, weight, special_instructions, shipped_by
        ) VALUES (?, 'Outbound', ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (id, carrier, tracking_number, ship_date, estimated_arrival or None,
          shipping_cost, package_count, weight, special_instructions, current_user.id))
    
    conn.execute('''
        UPDATE repair_orders SET status = 'Shipped', shipped_date = ?, last_updated = CURRENT_TIMESTAMP
        WHERE id = ? AND status IN ('Draft', 'Approved', 'Awaiting Shipment')
    ''', (ship_date, id))
    
    conn.execute('''
        UPDATE inventory SET status = 'In External Repair' 
        WHERE id IN (SELECT inventory_id FROM repair_order_lines WHERE ro_id = ? AND inventory_id IS NOT NULL)
    ''', (id,))
    
    conn.execute('UPDATE repair_order_lines SET line_status = ? WHERE ro_id = ?', ('Shipped', id))
    
    log_ro_audit(conn, id, 'SHIPPED', f'Shipment created - Carrier: {carrier}, Tracking: {tracking_number}',
                changed_fields={'carrier': carrier, 'tracking_number': tracking_number, 'ship_date': ship_date, 
                               'shipping_cost': shipping_cost, 'package_count': package_count})
    conn.commit()
    conn.close()
    
    flash('Shipment recorded. Items marked as shipped.', 'success')
    return redirect(url_for('repair_order_routes.view_repair_order', id=id))

@repair_order_bp.route('/repair-orders/<int:id>/receive-line/<int:line_id>', methods=['POST'])
@role_required('Admin', 'Receiving', 'Production Staff')
def receive_ro_line(id, line_id):
    db = Database()
    conn = db.get_connection()
    
    line = conn.execute('SELECT * FROM repair_order_lines WHERE id = ? AND ro_id = ?', (line_id, id)).fetchone()
    if not line:
        conn.close()
        flash('Line item not found.', 'danger')
        return redirect(url_for('repair_order_routes.view_repair_order', id=id))
    
    received_condition = request.form.get('received_condition', 'Serviceable')
    actual_cost = float(request.form.get('actual_cost', 0) or 0)
    received_notes = request.form.get('received_notes', '')
    
    conn.execute('''
        UPDATE repair_order_lines SET 
            line_status = 'Received', received_condition = ?, actual_cost = ?,
            received_notes = ?, received_by = ?, received_at = CURRENT_TIMESTAMP,
            last_updated = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (received_condition, actual_cost, received_notes, current_user.id, line_id))
    
    if line['inventory_id']:
        new_status = 'Available' if received_condition == 'Serviceable' else 'Quarantine'
        conn.execute('''
            UPDATE inventory SET status = ?, condition = ?, last_updated = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (new_status, received_condition, line['inventory_id']))
        log_ro_audit(conn, id, 'INVENTORY_RETURNED', f'Inventory {line["part_number"]} (ID:{line["inventory_id"]}) returned from repair. Condition: {received_condition}, Status: {new_status}',
                    changed_fields={'inventory_id': line['inventory_id'], 'received_condition': received_condition, 'new_status': new_status, 'actual_cost': actual_cost})
    
    total_actual = conn.execute(
        'SELECT COALESCE(SUM(actual_cost), 0) as total FROM repair_order_lines WHERE ro_id = ?', (id,)
    ).fetchone()['total']
    conn.execute('UPDATE repair_orders SET actual_total_cost = ? WHERE id = ?', (total_actual, id))
    
    all_received = conn.execute('''
        SELECT COUNT(*) as pending FROM repair_order_lines 
        WHERE ro_id = ? AND line_status != 'Received'
    ''', (id,)).fetchone()['pending'] == 0
    
    if all_received:
        conn.execute('''
            UPDATE repair_orders SET status = 'Received', received_date = CURRENT_DATE, last_updated = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (id,))
        log_ro_audit(conn, id, 'AUTO_STATUS_CHANGE', 'All line items received. RO status automatically updated to Received.',
                    old_status='Completed', new_status='Received')
    
    log_ro_audit(conn, id, 'RECEIVE_LINE', f'Received line {line["part_number"]} - Condition: {received_condition}, Cost: ${actual_cost}',
                changed_fields={'line_id': line_id, 'received_condition': received_condition, 'actual_cost': actual_cost, 'received_notes': received_notes})
    conn.commit()
    conn.close()
    
    flash('Item received successfully.', 'success')
    return redirect(url_for('repair_order_routes.view_repair_order', id=id))

@repair_order_bp.route('/api/repair-orders/search-inventory')
@login_required
def search_inventory_for_ro():
    db = Database()
    conn = db.get_connection()
    
    q = request.args.get('q', '')
    
    results = conn.execute('''
        SELECT i.id, i.product_id, i.quantity, i.serial_number, i.lot_number, i.condition, i.status,
               p.code, p.name, p.description
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.status = 'Available' AND i.quantity > 0
          AND (p.code LIKE ? OR p.name LIKE ? OR i.serial_number LIKE ?)
        ORDER BY p.code
        LIMIT 20
    ''', (f'%{q}%', f'%{q}%', f'%{q}%')).fetchall()
    
    conn.close()
    return jsonify([dict(r) for r in results])
