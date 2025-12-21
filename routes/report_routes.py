from flask import Blueprint, render_template, Response, request, redirect, url_for, flash
from models import Database
from auth import login_required
import csv
import io

report_bp = Blueprint('report_routes', __name__)

@report_bp.route('/reports/inventory')
@login_required
def inventory_report():
    db = Database()
    conn = db.get_connection()
    
    inventory_data = conn.execute('''
        SELECT i.*, p.code, p.name, p.unit_of_measure, p.cost,
               (i.quantity * p.cost) as total_value
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        ORDER BY total_value DESC
    ''').fetchall()
    
    total_inventory_value = sum(item['total_value'] for item in inventory_data)
    
    conn.close()
    
    return render_template('reports/inventory.html', 
                         inventory_data=inventory_data,
                         total_value=total_inventory_value)

@report_bp.route('/reports/workorder-costs')
@login_required
def workorder_costs_report():
    db = Database()
    conn = db.get_connection()
    
    status_filter = request.args.get('status', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    sort_by = request.args.get('sort_by', 'wo_number')
    sort_order = request.args.get('sort_order', 'desc')
    
    valid_sort_cols = ['wo_number', 'code', 'quantity', 'material_cost', 'labor_cost', 
                       'overhead_cost', 'service_cost', 'total_cost', 'status', 'planned_start_date']
    if sort_by not in valid_sort_cols:
        sort_by = 'wo_number'
    if sort_order not in ['asc', 'desc']:
        sort_order = 'desc'
    
    query = '''
        SELECT wo.*, p.code, p.name,
               COALESCE((SELECT SUM(mi.quantity_issued * COALESCE(pr.cost, 0)) 
                        FROM material_issues mi 
                        JOIN products pr ON mi.product_id = pr.id
                        WHERE mi.work_order_id = wo.id), 0) +
               COALESCE((SELECT SUM(tm.issued_qty * COALESCE(tm.unit_cost, pr2.cost, 0))
                        FROM work_order_task_materials tm
                        JOIN work_order_tasks wot ON tm.task_id = wot.id
                        JOIN products pr2 ON tm.product_id = pr2.id
                        WHERE wot.work_order_id = wo.id AND tm.issued_qty > 0), 0) as actual_material_cost,
               COALESCE((SELECT SUM(total_cost) FROM purchase_order_service_lines 
                        WHERE work_order_id = wo.id AND status = 'Received'), 0) as service_cost,
               (COALESCE((SELECT SUM(mi.quantity_issued * COALESCE(pr.cost, 0)) 
                        FROM material_issues mi 
                        JOIN products pr ON mi.product_id = pr.id
                        WHERE mi.work_order_id = wo.id), 0) +
                COALESCE((SELECT SUM(tm.issued_qty * COALESCE(tm.unit_cost, pr2.cost, 0))
                        FROM work_order_task_materials tm
                        JOIN work_order_tasks wot ON tm.task_id = wot.id
                        JOIN products pr2 ON tm.product_id = pr2.id
                        WHERE wot.work_order_id = wo.id AND tm.issued_qty > 0), 0) +
                wo.labor_cost + wo.overhead_cost + 
                COALESCE((SELECT SUM(total_cost) FROM purchase_order_service_lines 
                         WHERE work_order_id = wo.id AND status = 'Received'), 0)) as total_cost
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND wo.status = ?'
        params.append(status_filter)
    
    if date_from:
        query += ' AND wo.planned_start_date >= ?'
        params.append(date_from)
    
    if date_to:
        query += ' AND wo.planned_start_date <= ?'
        params.append(date_to)
    
    query += f' ORDER BY {sort_by} {sort_order.upper()}'
    
    workorder_costs = conn.execute(query, params).fetchall()
    
    statuses = conn.execute('SELECT DISTINCT status FROM work_orders ORDER BY status').fetchall()
    status_list = [s['status'] for s in statuses]
    
    totals = {
        'material': sum(wo['actual_material_cost'] or 0 for wo in workorder_costs),
        'labor': sum(wo['labor_cost'] or 0 for wo in workorder_costs),
        'overhead': sum(wo['overhead_cost'] or 0 for wo in workorder_costs),
        'service': sum(wo['service_cost'] or 0 for wo in workorder_costs),
        'total': sum(wo['total_cost'] or 0 for wo in workorder_costs)
    }
    
    conn.close()
    
    return render_template('reports/workorder_costs.html', 
                          workorder_costs=workorder_costs,
                          status_list=status_list,
                          status_filter=status_filter,
                          date_from=date_from,
                          date_to=date_to,
                          sort_by=sort_by,
                          sort_order=sort_order,
                          totals=totals)

@report_bp.route('/reports/material-usage')
@login_required
def material_usage_report():
    db = Database()
    conn = db.get_connection()
    
    material_usage = conn.execute('''
        SELECT mr.*, p.code, p.name, wo.wo_number,
               (mr.required_quantity * p.cost) as total_cost
        FROM material_requirements mr
        JOIN products p ON mr.product_id = p.id
        JOIN work_orders wo ON mr.work_order_id = wo.id
        ORDER BY total_cost DESC
    ''').fetchall()
    
    conn.close()
    
    return render_template('reports/material_usage.html', material_usage=material_usage)

@report_bp.route('/reports/material-requirements')
@login_required
def material_requirements_report():
    db = Database()
    conn = db.get_connection()
    
    # Get PO summary for all products
    po_summary = conn.execute('''
        SELECT pol.product_id, 
               SUM(pol.quantity - COALESCE(pol.received_quantity, 0)) as qty_on_order
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        WHERE po.status IN ('Ordered', 'Partially Received')
        GROUP BY pol.product_id
    ''').fetchall()
    po_dict = {row['product_id']: row['qty_on_order'] for row in po_summary}
    
    # Get current inventory levels
    inventory = conn.execute('''
        SELECT product_id, SUM(quantity) as total_qty
        FROM inventory
        GROUP BY product_id
    ''').fetchall()
    inventory_dict = {row['product_id']: row['total_qty'] for row in inventory}
    
    # Get production work order material requirements
    production_requirements = conn.execute('''
        SELECT 
            'Production' as source_type,
            wo.wo_number as order_number,
            wo.status as order_status,
            wo.planned_start_date as order_date,
            mr.product_id,
            p.code,
            p.name,
            p.unit_of_measure,
            p.cost,
            mr.required_quantity,
            mr.available_quantity,
            mr.shortage_quantity,
            mr.status,
            (mr.required_quantity * p.cost) as total_cost
        FROM material_requirements mr
        JOIN products p ON mr.product_id = p.id
        JOIN work_orders wo ON mr.work_order_id = wo.id
        WHERE mr.status != 'Satisfied'
    ''').fetchall()
    
    # Get service work order material requirements
    service_requirements = conn.execute('''
        SELECT 
            'Service' as source_type,
            swo.swo_number as order_number,
            swo.status as order_status,
            swo.due_date as order_date,
            swm.product_id,
            p.code,
            p.name,
            p.unit_of_measure,
            p.cost,
            swm.quantity as required_quantity,
            swm.allocated_from_inventory
        FROM service_wo_materials swm
        JOIN products p ON swm.product_id = p.id
        JOIN service_work_orders swo ON swm.swo_id = swo.id
        WHERE swo.status NOT IN ('Completed', 'Cancelled', 'Invoiced')
          AND swm.allocated_from_inventory = 0
    ''').fetchall()
    
    # Track net available inventory per product as we process requirements
    net_inventory = inventory_dict.copy()
    
    # Combine and process all requirements
    all_requirements = []
    product_shortages = {}
    
    # Process production requirements first (they use material_requirements which already calculated shortages)
    for req in production_requirements:
        req_dict = dict(req)
        product_id = req['product_id']
        
        # Production requirements already have shortage calculated
        # Deduct the required quantity from net inventory
        required_qty = req['required_quantity']
        net_inventory[product_id] = net_inventory.get(product_id, 0) - required_qty
        
        all_requirements.append(req_dict)
        
        shortage = req['shortage_quantity'] if req['shortage_quantity'] > 0 else 0
        
        if shortage > 0:
            if product_id not in product_shortages:
                product_shortages[product_id] = {
                    'code': req['code'],
                    'name': req['name'],
                    'total_shortage': 0,
                    'shortage_value': 0
                }
            product_shortages[product_id]['total_shortage'] += shortage
            product_shortages[product_id]['shortage_value'] += shortage * (req['cost'] or 0)
    
    # Process service requirements - calculate shortage based on net remaining inventory
    for req in service_requirements:
        product_id = req['product_id']
        required_qty = req['required_quantity']
        
        # Use net available inventory (after production requirements)
        available_qty = max(0, net_inventory.get(product_id, 0))
        shortage_qty = max(0, required_qty - available_qty)
        
        # Deduct this requirement from net inventory
        net_inventory[product_id] = net_inventory.get(product_id, 0) - required_qty
        
        req_dict = {
            'source_type': req['source_type'],
            'order_number': req['order_number'],
            'order_status': req['order_status'],
            'order_date': req['order_date'],
            'product_id': product_id,
            'code': req['code'],
            'name': req['name'],
            'unit_of_measure': req['unit_of_measure'],
            'cost': req['cost'],
            'required_quantity': required_qty,
            'available_quantity': available_qty,
            'shortage_quantity': shortage_qty,
            'status': 'Shortage' if shortage_qty > 0 else 'Available',
            'total_cost': required_qty * (req['cost'] or 0)
        }
        all_requirements.append(req_dict)
        
        if shortage_qty > 0:
            if product_id not in product_shortages:
                product_shortages[product_id] = {
                    'code': req['code'],
                    'name': req['name'],
                    'total_shortage': 0,
                    'shortage_value': 0
                }
            product_shortages[product_id]['total_shortage'] += shortage_qty
            product_shortages[product_id]['shortage_value'] += shortage_qty * (req['cost'] or 0)
    
    # Filter out products fully covered by POs
    filtered_requirements = []
    filtered_product_shortages = {}
    
    for req in all_requirements:
        product_id = req['product_id']
        product_total_shortage = product_shortages.get(product_id, {}).get('total_shortage', 0)
        qty_on_order = po_dict.get(product_id, 0)
        
        # Only include if shortage is not fully covered by POs
        if product_total_shortage == 0 or product_total_shortage > qty_on_order:
            filtered_requirements.append(req)
            
            if product_id in product_shortages and product_total_shortage > qty_on_order:
                filtered_product_shortages[product_id] = product_shortages[product_id]
    
    # Sort requirements by date and shortage
    filtered_requirements.sort(key=lambda x: (x['order_date'] or '9999-12-31', -x['shortage_quantity']), reverse=True)
    
    # Calculate totals
    total_requirements = len(filtered_requirements)
    total_shortages = sum(1 for r in filtered_requirements if r['shortage_quantity'] > 0)
    total_cost = sum(r['total_cost'] for r in filtered_requirements)
    total_shortage_cost = sum(r['shortage_quantity'] * (r['cost'] or 0) for r in filtered_requirements if r['shortage_quantity'] > 0)
    
    # Convert product shortages dict to list
    shortages_by_product = sorted(
        [{'code': v['code'], 'name': v['name'], 'total_shortage': v['total_shortage'], 'shortage_value': v['shortage_value']}
         for v in filtered_product_shortages.values()],
        key=lambda x: x['shortage_value'],
        reverse=True
    )
    
    conn.close()
    
    return render_template('reports/material_requirements.html', 
                         requirements=filtered_requirements,
                         total_requirements=total_requirements,
                         total_shortages=total_shortages,
                         total_cost=total_cost,
                         total_shortage_cost=total_shortage_cost,
                         shortages_by_product=shortages_by_product)

@report_bp.route('/reports/material-requirements/export')
@login_required
def export_material_requirements():
    db = Database()
    conn = db.get_connection()
    
    # Get PO summary for all products
    po_summary = conn.execute('''
        SELECT pol.product_id, 
               SUM(pol.quantity - COALESCE(pol.received_quantity, 0)) as qty_on_order
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        WHERE po.status IN ('Ordered', 'Partially Received')
        GROUP BY pol.product_id
    ''').fetchall()
    po_dict = {row['product_id']: row['qty_on_order'] for row in po_summary}
    
    # Get current inventory levels
    inventory = conn.execute('''
        SELECT product_id, SUM(quantity) as total_qty
        FROM inventory
        GROUP BY product_id
    ''').fetchall()
    inventory_dict = {row['product_id']: row['total_qty'] for row in inventory}
    
    # Get production work order material requirements
    production_requirements = conn.execute('''
        SELECT 
            'Production' as source_type,
            wo.wo_number as order_number,
            wo.status as order_status,
            wo.planned_start_date as order_date,
            mr.product_id,
            p.code,
            p.name,
            p.unit_of_measure,
            p.cost,
            mr.required_quantity,
            mr.available_quantity,
            mr.shortage_quantity,
            mr.status,
            (mr.required_quantity * p.cost) as total_cost
        FROM material_requirements mr
        JOIN products p ON mr.product_id = p.id
        JOIN work_orders wo ON mr.work_order_id = wo.id
        WHERE mr.status != 'Satisfied'
    ''').fetchall()
    
    # Get service work order material requirements
    service_requirements = conn.execute('''
        SELECT 
            'Service' as source_type,
            swo.swo_number as order_number,
            swo.status as order_status,
            swo.due_date as order_date,
            swm.product_id,
            p.code,
            p.name,
            p.unit_of_measure,
            p.cost,
            swm.quantity as required_quantity,
            swm.allocated_from_inventory
        FROM service_wo_materials swm
        JOIN products p ON swm.product_id = p.id
        JOIN service_work_orders swo ON swm.swo_id = swo.id
        WHERE swo.status NOT IN ('Completed', 'Cancelled', 'Invoiced')
          AND swm.allocated_from_inventory = 0
    ''').fetchall()
    
    # Track net available inventory per product as we process requirements
    net_inventory = inventory_dict.copy()
    
    # Combine and process all requirements
    all_requirements = []
    product_shortages = {}
    
    # Process production requirements first
    for req in production_requirements:
        req_dict = dict(req)
        product_id = req['product_id']
        
        # Deduct the required quantity from net inventory
        required_qty = req['required_quantity']
        net_inventory[product_id] = net_inventory.get(product_id, 0) - required_qty
        
        all_requirements.append(req_dict)
        
        shortage = req['shortage_quantity'] if req['shortage_quantity'] > 0 else 0
        
        if shortage > 0:
            if product_id not in product_shortages:
                product_shortages[product_id] = 0
            product_shortages[product_id] += shortage
    
    # Process service requirements - calculate shortage based on net remaining inventory
    for req in service_requirements:
        product_id = req['product_id']
        required_qty = req['required_quantity']
        
        # Use net available inventory (after production requirements)
        available_qty = max(0, net_inventory.get(product_id, 0))
        shortage_qty = max(0, required_qty - available_qty)
        
        # Deduct this requirement from net inventory
        net_inventory[product_id] = net_inventory.get(product_id, 0) - required_qty
        
        req_dict = {
            'source_type': req['source_type'],
            'order_number': req['order_number'],
            'order_status': req['order_status'],
            'order_date': req['order_date'],
            'product_id': product_id,
            'code': req['code'],
            'name': req['name'],
            'unit_of_measure': req['unit_of_measure'],
            'cost': req['cost'],
            'required_quantity': required_qty,
            'available_quantity': available_qty,
            'shortage_quantity': shortage_qty,
            'status': 'Shortage' if shortage_qty > 0 else 'Available',
            'total_cost': required_qty * (req['cost'] or 0)
        }
        all_requirements.append(req_dict)
        
        if shortage_qty > 0:
            if product_id not in product_shortages:
                product_shortages[product_id] = 0
            product_shortages[product_id] += shortage_qty
    
    # Filter out products fully covered by POs
    filtered_requirements = []
    for req in all_requirements:
        product_id = req['product_id']
        product_total_shortage = product_shortages.get(product_id, 0)
        qty_on_order = po_dict.get(product_id, 0)
        
        # Only include if shortage is not fully covered by POs
        if product_total_shortage == 0 or product_total_shortage > qty_on_order:
            filtered_requirements.append(req)
    
    # Sort requirements by date
    filtered_requirements.sort(key=lambda x: (x['order_date'] or '9999-12-31', -x['shortage_quantity']), reverse=True)
    
    conn.close()
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Source Type', 'Order Number', 'Order Status', 'Order Date', 'Product Code', 'Product Name', 
                    'Unit of Measure', 'Required Qty', 'Available Qty', 'Shortage Qty', 
                    'Status', 'Unit Cost', 'Total Cost'])
    
    for req in filtered_requirements:
        writer.writerow([req['source_type'], req['order_number'], req['order_status'], req['order_date'],
                        req['code'], req['name'], req['unit_of_measure'],
                        req['required_quantity'], req['available_quantity'], req['shortage_quantity'],
                        req['status'], req['cost'], req['total_cost']])
    
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=material_requirements_report.csv'}
    )

@report_bp.route('/reports/material-requirements/procure', methods=['POST'])
@login_required
def procure_from_requirements():
    db = Database()
    conn = db.get_connection()
    
    created_pos = []
    created_po_ids = []
    errors = []
    
    try:
        form_data = request.form.to_dict(flat=False)
        
        num_items = len([k for k in form_data.keys() if k.startswith('items[') and k.endswith('][product_id]')])
        
        for i in range(num_items):
            try:
                product_id_str = request.form.get(f'items[{i}][product_id]', '')
                supplier_id_str = request.form.get(f'items[{i}][supplier_id]', '')
                quantity_str = request.form.get(f'items[{i}][quantity]', '')
                unit_price_str = request.form.get(f'items[{i}][unit_price]', '')
                
                if not all([product_id_str, supplier_id_str, quantity_str, unit_price_str]):
                    errors.append(f"Missing data for item {i}")
                    continue
                
                product_id = int(product_id_str)
                supplier_id = int(supplier_id_str)
                quantity = float(quantity_str)
                unit_price = float(unit_price_str)
                
                existing_po_count = conn.execute(
                    "SELECT COUNT(*) as count FROM purchase_orders WHERE po_number LIKE 'PO-%'"
                ).fetchone()['count']
                
                next_po_number = f"PO-{str(existing_po_count + 1).zfill(6)}"
                
                for attempt in range(5):
                    try:
                        cursor = conn.cursor()
                        # Create PO header
                        cursor.execute('''
                            INSERT INTO purchase_orders (po_number, supplier_id, status, order_date, expected_delivery_date)
                            VALUES (?, ?, ?, DATE('now'), DATE('now', '+7 days'))
                        ''', (next_po_number, supplier_id, 'Ordered'))
                        
                        po_id = cursor.lastrowid
                        
                        # Create PO line with product details
                        cursor.execute('''
                            INSERT INTO purchase_order_lines 
                            (po_id, line_number, product_id, quantity, unit_price, received_quantity)
                            VALUES (?, 1, ?, ?, ?, 0)
                        ''', (po_id, product_id, quantity, unit_price))
                        
                        conn.commit()
                        created_pos.append({'number': next_po_number, 'id': po_id})
                        created_po_ids.append(po_id)
                        break
                    except Exception as e:
                        if 'UNIQUE constraint' in str(e) and attempt < 4:
                            existing_po_count += 1
                            next_po_number = f"PO-{str(existing_po_count + 1).zfill(6)}"
                        else:
                            raise
                
            except Exception as item_error:
                errors.append(f"Error creating PO for item {i}: {str(item_error)}")
        
        if created_pos:
            po_links = ', '.join([f'<a href="{url_for("po_routes.view_purchaseorder", id=po["id"])}" class="alert-link">{po["number"]}</a>' for po in created_pos])
            flash(f'Successfully created {len(created_pos)} purchase order(s): {po_links}', 'success')
        
        if errors:
            for error in errors:
                flash(error, 'warning')
                
    except Exception as e:
        flash(f'Error processing procurement: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('report_routes.material_requirements_report'))

@report_bp.route('/reports/ojt')
@login_required
def ojt_report():
    """OJT (On-the-Job Training) Report - Detailed job report by employee"""
    db = Database()
    conn = db.get_connection()
    
    employee_filter = request.args.get('employee', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    wo_filter = request.args.get('work_order', '')
    sort_by = request.args.get('sort_by', 'clock_in_time')
    sort_order = request.args.get('sort_order', 'desc')
    
    valid_sort_cols = ['employee_name', 'wo_number', 'clock_in_time', 'clock_out_time', 'hours_worked']
    if sort_by not in valid_sort_cols:
        sort_by = 'clock_in_time'
    if sort_order not in ['asc', 'desc']:
        sort_order = 'desc'
    
    query = '''
        SELECT 
            wott.id,
            wott.entry_number,
            lr.first_name || ' ' || lr.last_name as employee_name,
            lr.employee_code,
            wo.wo_number,
            wo.description as wo_description,
            p.code as part_number,
            p.name as part_name,
            wot.task_name,
            wot.description as task_description,
            wott.clock_in_time,
            wott.clock_out_time,
            wott.hours_worked,
            wott.labor_cost,
            wott.hourly_rate,
            wott.status,
            wott.notes
        FROM work_order_time_tracking wott
        JOIN labor_resources lr ON wott.employee_id = lr.id
        JOIN work_orders wo ON wott.work_order_id = wo.id
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN work_order_tasks wot ON wott.task_id = wot.id
        WHERE 1=1
    '''
    params = []
    
    if employee_filter:
        query += ' AND wott.employee_id = ?'
        params.append(employee_filter)
    
    if date_from:
        query += ' AND DATE(wott.clock_in_time) >= ?'
        params.append(date_from)
    
    if date_to:
        query += ' AND DATE(wott.clock_in_time) <= ?'
        params.append(date_to)
    
    if wo_filter:
        query += ' AND wo.id = ?'
        params.append(wo_filter)
    
    sort_col_map = {
        'employee_name': 'lr.first_name',
        'wo_number': 'wo.wo_number',
        'clock_in_time': 'wott.clock_in_time',
        'clock_out_time': 'wott.clock_out_time',
        'hours_worked': 'wott.hours_worked'
    }
    
    query += f' ORDER BY {sort_col_map.get(sort_by, "wott.clock_in_time")} {sort_order.upper()}'
    
    time_entries = conn.execute(query, params).fetchall()
    
    employees = conn.execute('''
        SELECT id, employee_code, first_name || ' ' || last_name as name
        FROM labor_resources
        WHERE status = 'Active'
        ORDER BY first_name, last_name
    ''').fetchall()
    
    work_orders = conn.execute('''
        SELECT DISTINCT wo.id, wo.wo_number
        FROM work_orders wo
        JOIN work_order_time_tracking wott ON wo.id = wott.work_order_id
        ORDER BY wo.wo_number DESC
    ''').fetchall()
    
    grouped_data = {}
    for entry in time_entries:
        emp_name = entry['employee_name']
        if emp_name not in grouped_data:
            grouped_data[emp_name] = {
                'employee_code': entry['employee_code'],
                'entries': [],
                'total_hours': 0,
                'total_cost': 0
            }
        grouped_data[emp_name]['entries'].append(entry)
        grouped_data[emp_name]['total_hours'] += entry['hours_worked'] or 0
        grouped_data[emp_name]['total_cost'] += entry['labor_cost'] or 0
    
    grand_totals = {
        'hours': sum(g['total_hours'] for g in grouped_data.values()),
        'cost': sum(g['total_cost'] for g in grouped_data.values()),
        'entries': len(time_entries)
    }
    
    conn.close()
    
    return render_template('reports/ojt_report.html',
                          grouped_data=grouped_data,
                          employees=employees,
                          work_orders=work_orders,
                          employee_filter=employee_filter,
                          date_from=date_from,
                          date_to=date_to,
                          wo_filter=wo_filter,
                          sort_by=sort_by,
                          sort_order=sort_order,
                          grand_totals=grand_totals)

@report_bp.route('/reports/ojt/export')
@login_required
def ojt_report_export():
    """Export OJT Report to CSV"""
    db = Database()
    conn = db.get_connection()
    
    employee_filter = request.args.get('employee', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    wo_filter = request.args.get('work_order', '')
    
    query = '''
        SELECT 
            lr.first_name || ' ' || lr.last_name as employee_name,
            lr.employee_code,
            wo.wo_number,
            wo.description as wo_description,
            p.code as part_number,
            wot.task_name,
            wot.description as task_description,
            wott.clock_in_time,
            wott.clock_out_time,
            wott.hours_worked,
            wott.labor_cost,
            wott.hourly_rate
        FROM work_order_time_tracking wott
        JOIN labor_resources lr ON wott.employee_id = lr.id
        JOIN work_orders wo ON wott.work_order_id = wo.id
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN work_order_tasks wot ON wott.task_id = wot.id
        WHERE 1=1
    '''
    params = []
    
    if employee_filter:
        query += ' AND wott.employee_id = ?'
        params.append(employee_filter)
    
    if date_from:
        query += ' AND DATE(wott.clock_in_time) >= ?'
        params.append(date_from)
    
    if date_to:
        query += ' AND DATE(wott.clock_in_time) <= ?'
        params.append(date_to)
    
    if wo_filter:
        query += ' AND wo.id = ?'
        params.append(wo_filter)
    
    query += ' ORDER BY lr.first_name, wott.clock_in_time DESC'
    
    entries = conn.execute(query, params).fetchall()
    conn.close()
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    writer.writerow([
        'Employee Name', 'Employee Code', 'Work Order', 'WO Description',
        'Part Number', 'Task Name', 'Task Description', 'Start Time', 
        'End Time', 'Hours Worked', 'Labor Cost', 'Hourly Rate'
    ])
    
    for entry in entries:
        writer.writerow([
            entry['employee_name'],
            entry['employee_code'],
            entry['wo_number'],
            entry['wo_description'] or '',
            entry['part_number'],
            entry['task_name'] or 'General',
            entry['task_description'] or '',
            entry['clock_in_time'],
            entry['clock_out_time'] or 'In Progress',
            round(entry['hours_worked'] or 0, 2),
            round(entry['labor_cost'] or 0, 2),
            round(entry['hourly_rate'] or 0, 2)
        ])
    
    output.seek(0)
    
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=ojt_report.csv'}
    )
