from flask import Blueprint, render_template, Response, request, redirect, url_for, flash
from models import Database, CompanySettings
from auth import login_required
import csv
import io
from datetime import datetime
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
import os

report_bp = Blueprint('report_routes', __name__)

@report_bp.route('/reports/inventory')
@login_required
def inventory_report():
    db = Database()
    conn = db.get_connection()
    
    # Get filter parameters
    search = request.args.get('search', '').strip()
    product_type = request.args.get('product_type', '')
    location = request.args.get('location', '')
    min_qty = request.args.get('min_qty', '')
    max_qty = request.args.get('max_qty', '')
    
    # Get sort parameters
    sort_by = request.args.get('sort_by', 'total_value')
    sort_order = request.args.get('sort_order', 'desc')
    
    valid_sort_cols = ['code', 'name', 'serial_number', 'quantity', 'effective_cost', 'total_value', 'product_type', 'location']
    if sort_by not in valid_sort_cols:
        sort_by = 'total_value'
    if sort_order not in ['asc', 'desc']:
        sort_order = 'desc'
    
    # Build query with filters
    query = '''
        SELECT i.*, p.code, p.name, p.unit_of_measure, p.cost as product_cost, p.product_type,
               COALESCE(i.unit_cost, p.cost, 0) as effective_cost,
               (i.quantity * COALESCE(i.unit_cost, p.cost, 0)) as total_value
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE 1=1
    '''
    params = []
    
    if search:
        query += ' AND (p.code LIKE ? OR p.name LIKE ? OR i.serial_number LIKE ?)'
        search_pattern = f'%{search}%'
        params.extend([search_pattern, search_pattern, search_pattern])
    
    if product_type:
        query += ' AND p.product_type = ?'
        params.append(product_type)
    
    if location:
        query += ' AND i.location LIKE ?'
        params.append(f'%{location}%')
    
    if min_qty:
        try:
            query += ' AND i.quantity >= ?'
            params.append(float(min_qty))
        except ValueError:
            pass
    
    if max_qty:
        try:
            query += ' AND i.quantity <= ?'
            params.append(float(max_qty))
        except ValueError:
            pass
    
    query += f' ORDER BY {sort_by} {sort_order.upper()}'
    
    inventory_data = conn.execute(query, params).fetchall()
    
    total_inventory_value = sum(item['total_value'] or 0 for item in inventory_data)
    
    # Get product types for filter dropdown
    product_types = conn.execute('SELECT DISTINCT product_type FROM products ORDER BY product_type').fetchall()
    
    # Get GL balance for comparison
    gl_balance = conn.execute('''
        SELECT 
            COALESCE(SUM(CASE WHEN l.debit > 0 THEN l.debit ELSE 0 END) - 
            SUM(CASE WHEN l.credit > 0 THEN l.credit ELSE 0 END), 0) as balance
        FROM gl_entry_lines l
        JOIN chart_of_accounts coa ON l.account_id = coa.id
        WHERE coa.account_code = '1130'
    ''').fetchone()
    
    conn.close()
    
    return render_template('reports/inventory.html', 
                         inventory_data=inventory_data,
                         total_value=total_inventory_value,
                         gl_balance=gl_balance['balance'] if gl_balance else 0,
                         product_types=product_types,
                         filters={
                             'search': search,
                             'product_type': product_type,
                             'location': location,
                             'min_qty': min_qty,
                             'max_qty': max_qty
                         },
                         sort_by=sort_by,
                         sort_order=sort_order)

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
    
    # Get work order task material requirements (includes non-inventory items like crates)
    task_material_requirements = conn.execute('''
        SELECT 
            'Production' as source_type,
            wo.wo_number as order_number,
            wo.status as order_status,
            wo.planned_start_date as order_date,
            tm.product_id,
            p.code,
            p.name,
            p.unit_of_measure,
            COALESCE(p.cost, 0) as cost,
            tm.required_qty as required_quantity,
            COALESCE(tm.issued_qty, 0) as available_quantity,
            (tm.required_qty - COALESCE(tm.issued_qty, 0)) as shortage_quantity,
            tm.material_status as status,
            (tm.required_qty * COALESCE(p.cost, 0)) as total_cost
        FROM work_order_task_materials tm
        JOIN products p ON tm.product_id = p.id
        JOIN work_order_tasks wot ON tm.task_id = wot.id
        JOIN work_orders wo ON wot.work_order_id = wo.id
        WHERE wo.status NOT IN ('Completed', 'Closed', 'Cancelled')
          AND COALESCE(tm.issued_qty, 0) < tm.required_qty
          AND NOT EXISTS (
              SELECT 1 FROM material_requirements mr 
              WHERE mr.work_order_id = wo.id AND mr.product_id = tm.product_id
          )
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
    
    # Process task material requirements (includes items like WO-CRATE)
    for req in task_material_requirements:
        req_dict = dict(req)
        product_id = req['product_id']
        shortage_qty = req['shortage_quantity'] if req['shortage_quantity'] > 0 else 0
        
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
    
    # Process sales order requirements - items not fully allocated
    sales_order_requirements = conn.execute('''
        SELECT 
            'Sales Order' as source_type,
            so.so_number as order_number,
            so.status as order_status,
            so.order_date as order_date,
            sol.product_id,
            p.code,
            p.name,
            p.unit_of_measure,
            p.cost,
            sol.quantity as required_quantity,
            COALESCE(sol.allocated_quantity, 0) as allocated_quantity,
            (sol.quantity - COALESCE(sol.allocated_quantity, 0)) as shortage_quantity,
            sol.allocation_status
        FROM sales_order_lines sol
        JOIN products p ON sol.product_id = p.id
        JOIN sales_orders so ON sol.so_id = so.id
        WHERE so.status NOT IN ('Cancelled', 'Closed', 'Shipped', 'Invoiced')
          AND sol.is_core = 0
          AND sol.quantity > COALESCE(sol.allocated_quantity, 0)
    ''').fetchall()
    
    for req in sales_order_requirements:
        product_id = req['product_id']
        shortage_qty = req['shortage_quantity']
        
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
            'required_quantity': req['required_quantity'],
            'available_quantity': req['allocated_quantity'],
            'shortage_quantity': shortage_qty,
            'status': 'Shortage' if shortage_qty > 0 else 'Available',
            'total_cost': req['required_quantity'] * (req['cost'] or 0)
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

@report_bp.route('/reports/net-requirements')
@login_required
def net_requirements_report():
    """Consolidated net requirements including sales order demand"""
    from mrp_logic import MRPEngine
    
    mrp = MRPEngine()
    
    requirements = mrp.calculate_net_requirements()
    
    total_items = len(requirements)
    shortage_items = [r for r in requirements if r['status'] == 'Shortage']
    total_shortages = len(shortage_items)
    total_demand = sum(r['total_demand'] for r in requirements)
    total_so_demand = sum(r['sales_order_demand'] for r in requirements)
    total_wo_demand = sum(r['work_order_demand'] for r in requirements)
    
    return render_template('reports/net_requirements.html',
                         requirements=requirements,
                         total_items=total_items,
                         total_shortages=total_shortages,
                         total_demand=total_demand,
                         total_so_demand=total_so_demand,
                         total_wo_demand=total_wo_demand)

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
    
    base_query = '''
        SELECT 
            wott.id,
            wott.entry_number,
            lr.first_name || ' ' || lr.last_name as employee_name,
            lr.employee_code,
            wo.wo_number,
            p.name as wo_description,
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
            wott.notes,
            'WO' as source_type
        FROM work_order_time_tracking wott
        JOIN labor_resources lr ON wott.employee_id = lr.id
        JOIN work_orders wo ON wott.work_order_id = wo.id
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN work_order_tasks wot ON wott.task_id = wot.id
        WHERE 1=1
    '''
    params = []
    
    if employee_filter:
        base_query += ' AND wott.employee_id = ?'
        params.append(employee_filter)
    
    if date_from:
        base_query += ' AND DATE(wott.clock_in_time) >= ?'
        params.append(date_from)
    
    if date_to:
        base_query += ' AND DATE(wott.clock_in_time) <= ?'
        params.append(date_to)
    
    if wo_filter:
        base_query += ' AND wo.id = ?'
        params.append(wo_filter)
    
    ndt_query = '''
        SELECT 
            tcp_in.id,
            tcp_in.punch_number as entry_number,
            lr.first_name || ' ' || lr.last_name as employee_name,
            lr.employee_code,
            nwo.ndt_wo_number as wo_number,
            nwo.part_description as wo_description,
            COALESCE(p.code, '') as part_number,
            COALESCE(p.name, nwo.part_description) as part_name,
            nwo.ndt_methods as task_name,
            nwo.applicable_code as task_description,
            tcp_in.punch_time as clock_in_time,
            tcp_out.punch_time as clock_out_time,
            CASE 
                WHEN tcp_out.punch_time IS NOT NULL 
                THEN ROUND((julianday(tcp_out.punch_time) - julianday(tcp_in.punch_time)) * 24, 2)
                ELSE 0 
            END as hours_worked,
            CASE 
                WHEN tcp_out.punch_time IS NOT NULL 
                THEN ROUND((julianday(tcp_out.punch_time) - julianday(tcp_in.punch_time)) * 24 * COALESCE(lr.hourly_rate, 0), 2)
                ELSE 0 
            END as labor_cost,
            lr.hourly_rate,
            'Approved' as status,
            tcp_in.notes,
            'NDT' as source_type
        FROM time_clock_punches tcp_in
        JOIN labor_resources lr ON tcp_in.employee_id = lr.id
        JOIN ndt_work_orders nwo ON tcp_in.ndt_work_order_id = nwo.id
        LEFT JOIN products p ON nwo.product_id = p.id
        LEFT JOIN time_clock_punches tcp_out ON (
            tcp_out.employee_id = tcp_in.employee_id 
            AND tcp_out.ndt_work_order_id = tcp_in.ndt_work_order_id
            AND tcp_out.punch_type = 'Clock Out'
            AND tcp_out.punch_time > tcp_in.punch_time
            AND tcp_out.id = (
                SELECT MIN(t2.id) FROM time_clock_punches t2 
                WHERE t2.employee_id = tcp_in.employee_id 
                AND t2.ndt_work_order_id = tcp_in.ndt_work_order_id 
                AND t2.punch_type = 'Clock Out' 
                AND t2.punch_time > tcp_in.punch_time
            )
        )
        WHERE tcp_in.punch_type = 'Clock In'
          AND tcp_in.ndt_work_order_id IS NOT NULL
    '''
    ndt_params = []
    
    if employee_filter:
        ndt_query += ' AND tcp_in.employee_id = ?'
        ndt_params.append(employee_filter)
    
    if date_from:
        ndt_query += ' AND DATE(tcp_in.punch_time) >= ?'
        ndt_params.append(date_from)
    
    if date_to:
        ndt_query += ' AND DATE(tcp_in.punch_time) <= ?'
        ndt_params.append(date_to)
    
    if wo_filter and str(wo_filter).startswith('NDT-'):
        pass
    
    query = f'SELECT * FROM ({base_query} UNION ALL {ndt_query}) combined'
    all_params = params + ndt_params
    
    sort_col_map = {
        'employee_name': 'employee_name',
        'wo_number': 'wo_number',
        'clock_in_time': 'clock_in_time',
        'clock_out_time': 'clock_out_time',
        'hours_worked': 'hours_worked'
    }
    
    query += f' ORDER BY {sort_col_map.get(sort_by, "clock_in_time")} {sort_order.upper()}'
    
    time_entries = conn.execute(query, all_params).fetchall()
    
    employees = conn.execute('''
        SELECT id, employee_code, first_name || ' ' || last_name as name
        FROM labor_resources
        WHERE status = 'Active'
        ORDER BY first_name, last_name
    ''').fetchall()
    
    work_orders = conn.execute('''
        SELECT wo_number, wo_number as id FROM (
            SELECT DISTINCT wo.wo_number
            FROM work_orders wo
            JOIN work_order_time_tracking wott ON wo.id = wott.work_order_id
            UNION
            SELECT DISTINCT nwo.ndt_wo_number as wo_number
            FROM ndt_work_orders nwo
            JOIN time_clock_punches tcp ON nwo.id = tcp.ndt_work_order_id
        ) combined
        ORDER BY wo_number DESC
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
            p.name as wo_description,
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

@report_bp.route('/reports/ojt/pdf')
@login_required
def ojt_report_pdf():
    """Export OJT Report to PDF with company header"""
    db = Database()
    conn = db.get_connection()
    
    company = CompanySettings.get()
    
    employee_filter = request.args.get('employee', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    wo_filter = request.args.get('work_order', '')
    
    query = '''
        SELECT 
            lr.first_name || ' ' || lr.last_name as employee_name,
            lr.employee_code,
            wo.wo_number,
            p.name as wo_description,
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
    
    grouped_data = {}
    for entry in entries:
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
        'entries': len(entries)
    }
    
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(letter), topMargin=0.5*inch, bottomMargin=0.5*inch,
                           leftMargin=0.5*inch, rightMargin=0.5*inch)
    story = []
    styles = getSampleStyleSheet()
    
    company_name = company['company_name'] if company else 'Company Name'
    company_address = ''
    if company:
        parts = []
        if company['address_line1']:
            parts.append(company['address_line1'])
        if company['city'] and company['state']:
            parts.append(f"{company['city']}, {company['state']} {company['postal_code'] or ''}")
        if company['phone']:
            parts.append(f"Phone: {company['phone']}")
        if company['email']:
            parts.append(f"Email: {company['email']}")
        company_address = ' | '.join(parts)
    
    header_style = ParagraphStyle(
        'CompanyHeader',
        parent=styles['Heading1'],
        fontSize=18,
        textColor=colors.HexColor('#1e3a8a'),
        spaceAfter=5,
        alignment=TA_CENTER
    )
    
    subheader_style = ParagraphStyle(
        'CompanySubheader',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#64748b'),
        spaceAfter=15,
        alignment=TA_CENTER
    )
    
    title_style = ParagraphStyle(
        'ReportTitle',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=colors.HexColor('#1e3a8a'),
        spaceAfter=5,
        alignment=TA_CENTER
    )
    
    section_style = ParagraphStyle(
        'SectionHeader',
        parent=styles['Heading3'],
        fontSize=11,
        textColor=colors.white,
        backColor=colors.HexColor('#1e3a8a'),
        spaceBefore=15,
        spaceAfter=5,
        leftIndent=5,
        rightIndent=5
    )
    
    story.append(Paragraph(company_name, header_style))
    if company_address:
        story.append(Paragraph(company_address, subheader_style))
    
    story.append(Spacer(1, 0.1*inch))
    story.append(Paragraph("ON-THE-JOB TRAINING (OJT) REPORT", title_style))
    
    date_range = ""
    if date_from and date_to:
        date_range = f"Period: {date_from} to {date_to}"
    elif date_from:
        date_range = f"From: {date_from}"
    elif date_to:
        date_range = f"Through: {date_to}"
    else:
        date_range = f"Generated: {datetime.now().strftime('%Y-%m-%d %I:%M %p')}"
    
    date_style = ParagraphStyle(
        'DateInfo',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#64748b'),
        spaceAfter=15,
        alignment=TA_CENTER
    )
    story.append(Paragraph(date_range, date_style))
    story.append(Spacer(1, 0.2*inch))
    
    summary_data = [
        ['Total Entries', 'Total Hours Worked'],
        [str(grand_totals['entries']), f"{grand_totals['hours']:.2f}"]
    ]
    summary_table = Table(summary_data, colWidths=[3.5*inch, 3.5*inch])
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1e3a8a')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('FONTSIZE', (0, 1), (-1, -1), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
        ('TOPPADDING', (0, 1), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 10),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f1f5f9')),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#cbd5e1')),
    ]))
    story.append(summary_table)
    story.append(Spacer(1, 0.3*inch))
    
    for employee_name, data in grouped_data.items():
        story.append(Paragraph(f"  {employee_name} ({data['employee_code']}) - {len(data['entries'])} entries, {data['total_hours']:.2f} hours", section_style))
        story.append(Spacer(1, 0.1*inch))
        
        table_data = [['Work Order', 'Part #', 'Task', 'Start', 'End', 'Hours']]
        
        for entry in data['entries']:
            start_time = ''
            if entry['clock_in_time']:
                start_time = str(entry['clock_in_time']).replace('T', ' ')[:16]
            
            end_time = 'In Progress'
            if entry['clock_out_time']:
                end_time = str(entry['clock_out_time']).replace('T', ' ')[:16]
            
            task_name = entry['task_name'] or 'General'
            if len(task_name) > 20:
                task_name = task_name[:17] + '...'
            
            table_data.append([
                entry['wo_number'],
                entry['part_number'],
                task_name,
                start_time,
                end_time,
                f"{entry['hours_worked'] or 0:.2f}"
            ])
        
        table_data.append(['', '', '', '', 'Subtotal:', f"{data['total_hours']:.2f}"])
        
        col_widths = [1.3*inch, 1.2*inch, 2.0*inch, 1.8*inch, 1.8*inch, 0.9*inch]
        detail_table = Table(table_data, colWidths=col_widths)
        
        detail_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#e2e8f0')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#1e3a8a')),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
            ('TOPPADDING', (0, 0), (-1, 0), 6),
            ('ALIGN', (-1, 0), (-1, -1), 'RIGHT'),
            ('GRID', (0, 0), (-1, -2), 0.5, colors.HexColor('#cbd5e1')),
            ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#f8fafc')),
            ('FONTNAME', (-2, -1), (-1, -1), 'Helvetica-Bold'),
            ('LINEABOVE', (0, -1), (-1, -1), 1, colors.HexColor('#1e3a8a')),
        ]))
        
        story.append(detail_table)
        story.append(Spacer(1, 0.2*inch))
    
    if not grouped_data:
        no_data_style = ParagraphStyle(
            'NoData',
            parent=styles['Normal'],
            fontSize=12,
            textColor=colors.HexColor('#64748b'),
            alignment=TA_CENTER,
            spaceBefore=30
        )
        story.append(Paragraph("No time tracking entries found matching your criteria.", no_data_style))
    
    footer_style = ParagraphStyle(
        'Footer',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#94a3b8'),
        alignment=TA_CENTER,
        spaceBefore=30
    )
    story.append(Spacer(1, 0.3*inch))
    story.append(Paragraph(f"Report generated on {datetime.now().strftime('%Y-%m-%d at %I:%M %p')}", footer_style))
    
    doc.build(story)
    buffer.seek(0)
    
    filename = f"ojt_report_{datetime.now().strftime('%Y%m%d')}.pdf"
    
    return Response(
        buffer.getvalue(),
        mimetype='application/pdf',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )


@report_bp.route('/reports/master-plan')
@login_required
def master_plan_report():
    db = Database()
    conn = db.get_connection()
    
    master_parts = conn.execute('''
        SELECT p.id, p.code, p.name, p.description, p.unit_of_measure, p.cost,
               p.lead_time,
               COALESCE(inv_agg.total_qty, 0) as inventory_on_hand,
               COALESCE(inv_agg.avg_unit_cost, p.cost, 0) as unit_cost,
               COALESCE(inv_agg.total_value, 0) as inventory_value
        FROM products p
        LEFT JOIN (
            SELECT product_id, 
                   SUM(quantity) as total_qty,
                   AVG(COALESCE(unit_cost, 0)) as avg_unit_cost,
                   SUM(quantity * COALESCE(unit_cost, 0)) as total_value
            FROM inventory
            GROUP BY product_id
        ) inv_agg ON p.id = inv_agg.product_id
        WHERE p.master_plan_part = 1
        ORDER BY p.code
    ''').fetchall()
    
    report_data = []
    
    for part in master_parts:
        product_id = part['id']
        
        exchange_on_customer = conn.execute('''
            SELECT COALESCE(SUM(sol.quantity), 0) as qty
            FROM sales_order_lines sol
            JOIN sales_orders so ON sol.so_id = so.id
            WHERE sol.product_id = ?
              AND so.sales_type = 'Exchange'
              AND so.status NOT IN ('Closed', 'Cancelled', 'Invoiced', 'Completed')
        ''', (product_id,)).fetchone()
        
        customer_exchange_details = conn.execute('''
            SELECT so.id as so_id, so.so_number, so.order_date, so.status, so.exchange_type,
                   so.expected_return_date, sol.quantity,
                   c.name as customer_name, c.customer_number,
                   CASE WHEN so.expected_return_date < date('now') THEN 1 ELSE 0 END as is_overdue,
                   julianday(so.expected_return_date) - julianday('now') as days_until_due
            FROM sales_order_lines sol
            JOIN sales_orders so ON sol.so_id = so.id
            JOIN customers c ON so.customer_id = c.id
            WHERE sol.product_id = ?
              AND so.sales_type = 'Exchange'
              AND so.status NOT IN ('Closed', 'Cancelled', 'Invoiced', 'Completed')
            ORDER BY so.expected_return_date
        ''', (product_id,)).fetchall()
        
        exchange_on_supplier = conn.execute('''
            SELECT COALESCE(SUM(pol.quantity), 0) as qty
            FROM purchase_order_lines pol
            JOIN purchase_orders po ON pol.po_id = po.id
            WHERE pol.product_id = ?
              AND po.is_exchange = 1
              AND po.status NOT IN ('Closed', 'Cancelled', 'Received')
        ''', (product_id,)).fetchone()
        
        supplier_exchange_details = conn.execute('''
            SELECT po.id as po_id, po.po_number, po.order_date, po.status, po.expected_delivery_date,
                   pol.quantity,
                   s.name as supplier_name,
                   po.source_sales_order_id,
                   (SELECT so.so_number FROM sales_orders so WHERE so.id = po.source_sales_order_id) as linked_so_number,
                   CASE WHEN po.expected_delivery_date < date('now') THEN 1 ELSE 0 END as is_overdue,
                   julianday(po.expected_delivery_date) - julianday('now') as days_until_due
            FROM purchase_order_lines pol
            JOIN purchase_orders po ON pol.po_id = po.id
            JOIN suppliers s ON po.supplier_id = s.id
            WHERE pol.product_id = ?
              AND po.is_exchange = 1
              AND po.status NOT IN ('Closed', 'Cancelled', 'Received')
            ORDER BY po.expected_delivery_date
        ''', (product_id,)).fetchall()
        
        work_orders = conn.execute('''
            SELECT wo.id, wo.wo_number, wo.quantity, wo.status, 
                   wo.planned_start_date, wo.planned_end_date
            FROM work_orders wo
            WHERE wo.product_id = ?
              AND wo.status NOT IN ('Completed', 'Closed', 'Cancelled')
            ORDER BY wo.planned_start_date
        ''', (product_id,)).fetchall()
        
        wo_summary = {}
        total_wo_qty = 0
        for wo in work_orders:
            status = wo['status'] or 'Unknown'
            if status not in wo_summary:
                wo_summary[status] = {'count': 0, 'qty': 0}
            wo_summary[status]['count'] += 1
            wo_summary[status]['qty'] += wo['quantity'] or 0
            total_wo_qty += wo['quantity'] or 0
        
        consumption_90d = conn.execute('''
            SELECT COALESCE(SUM(mi.quantity_issued), 0) as consumed
            FROM material_issues mi
            WHERE mi.product_id = ?
              AND mi.issue_date >= date('now', '-90 days')
        ''', (product_id,)).fetchone()
        
        shipments_90d = conn.execute('''
            SELECT COALESCE(SUM(sol.quantity), 0) as shipped
            FROM sales_order_lines sol
            JOIN sales_orders so ON sol.so_id = so.id
            WHERE sol.product_id = ?
              AND so.status = 'Shipped'
              AND so.order_date >= date('now', '-90 days')
        ''', (product_id,)).fetchone()
        
        total_consumption_90d = (consumption_90d['consumed'] or 0) + (shipments_90d['shipped'] or 0)
        avg_weekly_consumption = total_consumption_90d / 13 if total_consumption_90d > 0 else 0
        
        available_stock = (part['inventory_on_hand'] or 0)
        weeks_of_supply = available_stock / avg_weekly_consumption if avg_weekly_consumption > 0 else float('inf')
        
        if weeks_of_supply == float('inf'):
            forecast_status = 'No Demand'
            forecast_class = 'secondary'
        elif weeks_of_supply < 2:
            forecast_status = 'Critical'
            forecast_class = 'danger'
        elif weeks_of_supply < 4:
            forecast_status = 'Low'
            forecast_class = 'warning'
        elif weeks_of_supply < 8:
            forecast_status = 'Adequate'
            forecast_class = 'info'
        else:
            forecast_status = 'Sufficient'
            forecast_class = 'success'
        
        expected_inbound = conn.execute('''
            SELECT COALESCE(SUM(pol.quantity - COALESCE(pol.received_quantity, 0)), 0) as pending
            FROM purchase_order_lines pol
            JOIN purchase_orders po ON pol.po_id = po.id
            WHERE pol.product_id = ?
              AND po.status NOT IN ('Closed', 'Cancelled', 'Received')
              AND po.is_exchange = 0
        ''', (product_id,)).fetchone()
        
        report_data.append({
            'product': dict(part),
            'inventory_on_hand': part['inventory_on_hand'] or 0,
            'inventory_value': part['inventory_value'] or 0,
            'exchange_on_customer': exchange_on_customer['qty'] or 0,
            'exchange_on_supplier': exchange_on_supplier['qty'] or 0,
            'customer_exchange_details': [dict(ex) for ex in customer_exchange_details],
            'supplier_exchange_details': [dict(ex) for ex in supplier_exchange_details],
            'work_orders': [dict(wo) for wo in work_orders],
            'wo_summary': wo_summary,
            'total_wo_qty': total_wo_qty,
            'consumption_90d': total_consumption_90d,
            'avg_weekly': round(avg_weekly_consumption, 2),
            'weeks_of_supply': round(weeks_of_supply, 1) if weeks_of_supply != float('inf') else None,
            'forecast_status': forecast_status,
            'forecast_class': forecast_class,
            'expected_inbound': expected_inbound['pending'] or 0
        })
    
    summary_stats = {
        'total_parts': len(report_data),
        'total_inventory_value': sum(d['inventory_value'] for d in report_data),
        'critical_count': sum(1 for d in report_data if d['forecast_status'] == 'Critical'),
        'low_count': sum(1 for d in report_data if d['forecast_status'] == 'Low'),
        'total_on_exchange': sum(d['exchange_on_customer'] + d['exchange_on_supplier'] for d in report_data),
        'total_in_wo': sum(d['total_wo_qty'] for d in report_data)
    }
    
    conn.close()
    
    return render_template('reports/master_plan.html',
                         report_data=report_data,
                         summary_stats=summary_stats)


@report_bp.route('/reports/executive-inventory-dashboard')
@login_required
def executive_inventory_dashboard():
    """Executive Inventory Dashboard - C-level inventory intelligence"""
    from datetime import datetime, timedelta
    
    db = Database()
    conn = db.get_connection()
    
    is_postgres = db.use_postgres
    
    if is_postgres:
        date_90_days_ago = "(CURRENT_DATE - INTERVAL '90 days')"
        issue_date_compare = "mi.issue_date::date"
        days_since_update = "EXTRACT(DAY FROM (CURRENT_TIMESTAMP - i.last_updated::timestamp))"
    else:
        date_90_days_ago = "date('now', '-90 days')"
        issue_date_compare = "mi.issue_date"
        days_since_update = "julianday('now') - julianday(i.last_updated)"
    
    # Get filter parameters
    product_category_filter = request.args.get('product_category', '')
    part_category_filter = request.args.get('part_category', '')
    warehouse_filter = request.args.get('warehouse', '')
    status_filter = request.args.get('status', '')
    
    # === KPI CALCULATIONS ===
    
    # Total Inventory Value
    total_value_query = '''
        SELECT 
            COALESCE(SUM(i.quantity * COALESCE(i.unit_cost, p.cost, 0)), 0) as total_value,
            COALESCE(SUM(i.quantity), 0) as total_quantity,
            COUNT(DISTINCT i.id) as total_items
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.quantity > 0
    '''
    totals = conn.execute(total_value_query).fetchone()
    total_inventory_value = totals['total_value'] or 0
    total_on_hand_qty = totals['total_quantity'] or 0
    total_items = totals['total_items'] or 0
    
    # Average Inventory Cost
    avg_cost = total_inventory_value / total_items if total_items > 0 else 0
    
    # Inventory by Product Category
    category_breakdown = conn.execute('''
        SELECT 
            COALESCE(p.product_category, 'Uncategorized') as category,
            SUM(i.quantity * COALESCE(i.unit_cost, p.cost, 0)) as total_value,
            SUM(i.quantity) as total_qty,
            COUNT(DISTINCT i.id) as item_count
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.quantity > 0
        GROUP BY p.product_category
        ORDER BY total_value DESC
    ''').fetchall()
    
    # Inventory by Part Category (Sub-Category)
    part_category_breakdown = conn.execute('''
        SELECT 
            COALESCE(p.part_category, 'Other') as part_category,
            COALESCE(p.product_category, 'Uncategorized') as product_category,
            SUM(i.quantity * COALESCE(i.unit_cost, p.cost, 0)) as total_value,
            SUM(i.quantity) as total_qty,
            COUNT(DISTINCT i.id) as item_count
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.quantity > 0
        GROUP BY p.part_category, p.product_category
        ORDER BY total_value DESC
    ''').fetchall()
    
    # === AGING ANALYSIS ===
    # Using last_updated as proxy for last movement
    today = datetime.now().date()
    
    aging_query = f'''
        SELECT 
            aging_bucket,
            SUM(bucket_value) as bucket_value,
            SUM(bucket_qty) as bucket_qty,
            SUM(item_count) as item_count
        FROM (
            SELECT 
                CASE 
                    WHEN {days_since_update} <= 30 THEN '0-30 Days'
                    WHEN {days_since_update} <= 60 THEN '31-60 Days'
                    WHEN {days_since_update} <= 90 THEN '61-90 Days'
                    WHEN {days_since_update} <= 180 THEN '91-180 Days'
                    ELSE '180+ Days'
                END as aging_bucket,
                CASE 
                    WHEN {days_since_update} <= 30 THEN 1
                    WHEN {days_since_update} <= 60 THEN 2
                    WHEN {days_since_update} <= 90 THEN 3
                    WHEN {days_since_update} <= 180 THEN 4
                    ELSE 5
                END as bucket_order,
                i.quantity * COALESCE(i.unit_cost, p.cost, 0) as bucket_value,
                i.quantity as bucket_qty,
                1 as item_count
            FROM inventory i
            JOIN products p ON i.product_id = p.id
            WHERE i.quantity > 0
        ) sub
        GROUP BY aging_bucket, bucket_order
        ORDER BY bucket_order
    '''
    aging_buckets = conn.execute(aging_query).fetchall()
    
    # Calculate slow-moving and non-moving inventory
    slow_moving_query = f'''
        SELECT COALESCE(SUM(i.quantity * COALESCE(i.unit_cost, p.cost, 0)), 0) as value
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.quantity > 0
          AND {days_since_update} > 90
          AND {days_since_update} <= 180
    '''
    slow_moving = conn.execute(slow_moving_query).fetchone()
    slow_moving_value = slow_moving['value'] or 0
    
    non_moving_query = f'''
        SELECT COALESCE(SUM(i.quantity * COALESCE(i.unit_cost, p.cost, 0)), 0) as value
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.quantity > 0
          AND {days_since_update} > 180
    '''
    non_moving = conn.execute(non_moving_query).fetchone()
    non_moving_value = non_moving['value'] or 0
    
    # === USAGE ANALYTICS ===
    # Get material issues from last 90 days
    usage_query = f'''
        SELECT COALESCE(SUM(mi.quantity_issued * COALESCE(p.cost, 0)), 0) as usage_value,
               COALESCE(SUM(mi.quantity_issued), 0) as usage_qty
        FROM material_issues mi
        JOIN products p ON mi.product_id = p.id
        WHERE {issue_date_compare} >= {date_90_days_ago}
    '''
    usage_90d = conn.execute(usage_query).fetchone()
    usage_value_90d = usage_90d['usage_value'] or 0
    
    # Inventory Turnover Ratio (annualized)
    annual_usage = (usage_value_90d / 90) * 365 if usage_value_90d > 0 else 0
    turnover_ratio = annual_usage / total_inventory_value if total_inventory_value > 0 else 0
    
    # Days Inventory on Hand
    dio = 365 / turnover_ratio if turnover_ratio > 0 else 999
    
    # Top 10 Most Used Parts (last 90 days)
    top_used_query = f'''
        SELECT p.code, p.name, p.product_category, p.part_category,
               SUM(mi.quantity_issued) as total_issued,
               SUM(mi.quantity_issued * COALESCE(p.cost, 0)) as total_value
        FROM material_issues mi
        JOIN products p ON mi.product_id = p.id
        WHERE {issue_date_compare} >= {date_90_days_ago}
        GROUP BY p.id, p.code, p.name, p.product_category, p.part_category
        ORDER BY total_issued DESC
        LIMIT 10
    '''
    top_used = conn.execute(top_used_query).fetchall()
    
    # Bottom 10 Least Used (with inventory)
    if is_postgres:
        subquery_issue_date = "issue_date::date"
    else:
        subquery_issue_date = "issue_date"
    
    least_used_query = f'''
        SELECT p.code, p.name, p.product_category, p.part_category,
               i.quantity as on_hand,
               i.quantity * COALESCE(i.unit_cost, p.cost, 0) as inventory_value,
               COALESCE(mi.total_issued, 0) as total_issued,
               {days_since_update} as days_since_movement
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        LEFT JOIN (
            SELECT product_id, SUM(quantity_issued) as total_issued
            FROM material_issues
            WHERE {subquery_issue_date} >= {date_90_days_ago}
            GROUP BY product_id
        ) mi ON p.id = mi.product_id
        WHERE i.quantity > 0
        ORDER BY COALESCE(mi.total_issued, 0) ASC, {days_since_update} DESC
        LIMIT 10
    '''
    least_used = conn.execute(least_used_query).fetchall()
    
    # === RISK INDICATORS ===
    # Excess inventory (more than 180 days supply based on usage)
    excess_query = f'''
        SELECT 
            p.code, p.name, p.product_category,
            i.quantity as on_hand,
            i.quantity * COALESCE(i.unit_cost, p.cost, 0) as inventory_value,
            COALESCE(mi.avg_daily_usage, 0) as avg_daily_usage,
            CASE 
                WHEN COALESCE(mi.avg_daily_usage, 0) = 0 THEN 999
                ELSE i.quantity / mi.avg_daily_usage
            END as days_of_supply
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        LEFT JOIN (
            SELECT product_id, SUM(quantity_issued) / 90.0 as avg_daily_usage
            FROM material_issues
            WHERE {subquery_issue_date} >= {date_90_days_ago}
            GROUP BY product_id
        ) mi ON p.id = mi.product_id
        WHERE i.quantity > 0
        ORDER BY days_of_supply DESC
        LIMIT 15
    '''
    excess_inventory = conn.execute(excess_query).fetchall()
    
    # Obsolescence risk (high value items with no movement > 180 days)
    obsolescence_query = f'''
        SELECT COALESCE(SUM(i.quantity * COALESCE(i.unit_cost, p.cost, 0)), 0) as at_risk_value,
               COUNT(DISTINCT i.id) as at_risk_items
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.quantity > 0
          AND {days_since_update} > 180
    '''
    obsolescence_risk = conn.execute(obsolescence_query).fetchone()
    
    # Get unique filter options
    product_categories = conn.execute('''
        SELECT DISTINCT COALESCE(product_category, 'Uncategorized') as category 
        FROM products WHERE product_category IS NOT NULL ORDER BY category
    ''').fetchall()
    
    part_categories = conn.execute('''
        SELECT DISTINCT COALESCE(part_category, 'Other') as category 
        FROM products ORDER BY category
    ''').fetchall()
    
    warehouses = conn.execute('''
        SELECT DISTINCT warehouse_location 
        FROM inventory 
        WHERE warehouse_location IS NOT NULL AND warehouse_location != ''
        ORDER BY warehouse_location
    ''').fetchall()
    
    # Detailed inventory list for drill-down
    inventory_details_query = f'''
        SELECT 
            p.code, p.name, p.product_category, p.part_category, p.unit_of_measure,
            i.quantity, i.warehouse_location, i.bin_location,
            COALESCE(i.unit_cost, p.cost, 0) as unit_cost,
            i.quantity * COALESCE(i.unit_cost, p.cost, 0) as extended_value,
            i.last_updated,
            {days_since_update} as days_since_movement
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.quantity > 0
        ORDER BY extended_value DESC
        LIMIT 100
    '''
    inventory_details = conn.execute(inventory_details_query).fetchall()
    
    conn.close()
    
    # Calculate percentages for aging buckets
    aging_with_pct = []
    risk_mapping = {
        '0-30 Days': 'Low',
        '31-60 Days': 'Low', 
        '61-90 Days': 'Medium',
        '91-180 Days': 'Medium',
        '180+ Days': 'High'
    }
    for bucket in aging_buckets:
        pct = (bucket['bucket_value'] / total_inventory_value * 100) if total_inventory_value > 0 else 0
        risk = risk_mapping.get(bucket['aging_bucket'], 'Medium')
        aging_with_pct.append({
            'bucket': bucket['aging_bucket'],
            'value': bucket['bucket_value'] or 0,
            'qty': bucket['bucket_qty'] or 0,
            'items': bucket['item_count'] or 0,
            'pct': round(pct, 1),
            'risk': risk
        })
    
    # Category with percentages
    categories_with_pct = []
    for cat in category_breakdown:
        pct = (cat['total_value'] / total_inventory_value * 100) if total_inventory_value > 0 else 0
        categories_with_pct.append({
            'category': cat['category'],
            'value': cat['total_value'] or 0,
            'qty': cat['total_qty'] or 0,
            'items': cat['item_count'] or 0,
            'pct': round(pct, 1)
        })
    
    kpis = {
        'total_inventory_value': total_inventory_value,
        'total_on_hand_qty': total_on_hand_qty,
        'total_items': total_items,
        'avg_cost': avg_cost,
        'slow_moving_value': slow_moving_value,
        'non_moving_value': non_moving_value,
        'turnover_ratio': round(turnover_ratio, 2),
        'dio': round(dio, 0) if dio < 999 else 'N/A',
        'usage_90d': usage_value_90d,
        'obsolescence_risk_value': obsolescence_risk['at_risk_value'] or 0,
        'obsolescence_risk_items': obsolescence_risk['at_risk_items'] or 0
    }
    
    return render_template('reports/executive_inventory_dashboard.html',
                         kpis=kpis,
                         categories=categories_with_pct,
                         part_categories=part_category_breakdown,
                         aging_buckets=aging_with_pct,
                         top_used=top_used,
                         least_used=least_used,
                         excess_inventory=excess_inventory,
                         inventory_details=inventory_details,
                         filter_product_categories=product_categories,
                         filter_part_categories=part_categories,
                         filter_warehouses=warehouses,
                         selected_filters={
                             'product_category': product_category_filter,
                             'part_category': part_category_filter,
                             'warehouse': warehouse_filter,
                             'status': status_filter
                         })


@report_bp.route('/reports/organizational-scorecard')
@login_required
def organizational_scorecard():
    """Organizational Scorecard - Executive view of financial, operational, and inventory health"""
    from datetime import datetime, timedelta
    
    db = Database()
    conn = db.get_connection()
    
    is_postgres = db.use_postgres
    
    if is_postgres:
        week_start = "(CURRENT_DATE - INTERVAL '7 days')"
        prior_week_start = "(CURRENT_DATE - INTERVAL '14 days')"
        prior_week_end = "(CURRENT_DATE - INTERVAL '7 days')"
        year_start = "DATE_TRUNC('year', CURRENT_DATE)"
        date_diff = "EXTRACT(DAY FROM (CURRENT_DATE - order_date::date))"
        days_until = "EXTRACT(DAY FROM (required_date::date - CURRENT_DATE))"
    else:
        week_start = "date('now', '-7 days')"
        prior_week_start = "date('now', '-14 days')"
        prior_week_end = "date('now', '-7 days')"
        year_start = "date('now', 'start of year')"
        date_diff = "julianday('now') - julianday(order_date)"
        days_until = "julianday(required_date) - julianday('now')"
    
    # === FINANCIAL PERFORMANCE ===
    
    # Weekly Sales Revenue (current week)
    current_week_sales = conn.execute(f'''
        SELECT COALESCE(SUM(total_amount), 0) as revenue,
               COUNT(*) as order_count
        FROM sales_orders 
        WHERE order_date >= {week_start}
          AND status != 'Cancelled'
    ''').fetchone()
    
    # Prior week sales for comparison
    prior_week_sales = conn.execute(f'''
        SELECT COALESCE(SUM(total_amount), 0) as revenue
        FROM sales_orders 
        WHERE order_date >= {prior_week_start}
          AND order_date < {prior_week_end}
          AND status != 'Cancelled'
    ''').fetchone()
    
    current_week_revenue = float(current_week_sales['revenue'] or 0)
    prior_week_revenue = float(prior_week_sales['revenue'] or 0)
    week_change_pct = ((current_week_revenue - prior_week_revenue) / prior_week_revenue * 100) if prior_week_revenue > 0 else 0
    
    # Year-to-Date Sales & Profit
    ytd_sales = conn.execute(f'''
        SELECT COALESCE(SUM(total_amount), 0) as revenue,
               COUNT(*) as order_count
        FROM sales_orders 
        WHERE order_date >= {year_start}
          AND status != 'Cancelled'
    ''').fetchone()
    
    ytd_revenue = float(ytd_sales['revenue'] or 0)
    
    # Estimated gross profit (using average margin assumption or actual costs if available)
    ytd_costs = conn.execute(f'''
        SELECT COALESCE(SUM(sol.quantity * COALESCE(p.cost, 0)), 0) as total_cost
        FROM sales_order_lines sol
        JOIN sales_orders so ON sol.so_id = so.id
        LEFT JOIN products p ON sol.product_id = p.id
        WHERE so.order_date >= {year_start}
          AND so.status != 'Cancelled'
    ''').fetchone()
    
    ytd_cost = float(ytd_costs['total_cost'] or 0)
    ytd_gross_profit = ytd_revenue - ytd_cost
    profit_margin = (ytd_gross_profit / ytd_revenue * 100) if ytd_revenue > 0 else 0
    
    # Accounts Receivable Aging
    ar_aging = conn.execute('''
        SELECT 
            COALESCE(SUM(balance_due), 0) as total_ar,
            COALESCE(SUM(CASE WHEN balance_due > 0 THEN balance_due ELSE 0 END), 0) as open_ar
        FROM invoices
        WHERE status IN ('Sent', 'Posted', 'Overdue')
    ''').fetchone()
    
    total_ar = float(ar_aging['total_ar'] or 0)
    
    # AR Aging buckets
    if is_postgres:
        ar_buckets = conn.execute('''
            SELECT 
                COALESCE(SUM(CASE WHEN (CURRENT_DATE - invoice_date::date) <= 30 THEN balance_due ELSE 0 END), 0) as bucket_0_30,
                COALESCE(SUM(CASE WHEN (CURRENT_DATE - invoice_date::date) > 30 AND (CURRENT_DATE - invoice_date::date) <= 60 THEN balance_due ELSE 0 END), 0) as bucket_31_60,
                COALESCE(SUM(CASE WHEN (CURRENT_DATE - invoice_date::date) > 60 THEN balance_due ELSE 0 END), 0) as bucket_60_plus
            FROM invoices
            WHERE status IN ('Sent', 'Posted', 'Overdue') AND balance_due > 0
        ''').fetchone()
    else:
        ar_buckets = conn.execute('''
            SELECT 
                COALESCE(SUM(CASE WHEN julianday('now') - julianday(invoice_date) <= 30 THEN balance_due ELSE 0 END), 0) as bucket_0_30,
                COALESCE(SUM(CASE WHEN julianday('now') - julianday(invoice_date) > 30 AND julianday('now') - julianday(invoice_date) <= 60 THEN balance_due ELSE 0 END), 0) as bucket_31_60,
                COALESCE(SUM(CASE WHEN julianday('now') - julianday(invoice_date) > 60 THEN balance_due ELSE 0 END), 0) as bucket_60_plus
            FROM invoices
            WHERE status IN ('Sent', 'Posted', 'Overdue') AND balance_due > 0
        ''').fetchone()
    
    ar_over_30 = float(ar_buckets['bucket_31_60'] or 0) + float(ar_buckets['bucket_60_plus'] or 0)
    ar_over_30_pct = (ar_over_30 / total_ar * 100) if total_ar > 0 else 0
    
    # Accounts Payable Aging
    ap_aging = conn.execute('''
        SELECT 
            COALESCE(SUM(total_amount - amount_paid), 0) as total_ap
        FROM vendor_invoices
        WHERE status IN ('Open', 'Pending', 'Overdue')
    ''').fetchone()
    
    total_ap = float(ap_aging['total_ap'] or 0)
    
    # AP Aging buckets
    if is_postgres:
        ap_buckets = conn.execute('''
            SELECT 
                COALESCE(SUM(CASE WHEN (CURRENT_DATE - invoice_date::date) <= 30 THEN (total_amount - amount_paid) ELSE 0 END), 0) as bucket_0_30,
                COALESCE(SUM(CASE WHEN (CURRENT_DATE - invoice_date::date) > 30 AND (CURRENT_DATE - invoice_date::date) <= 60 THEN (total_amount - amount_paid) ELSE 0 END), 0) as bucket_31_60,
                COALESCE(SUM(CASE WHEN (CURRENT_DATE - invoice_date::date) > 60 THEN (total_amount - amount_paid) ELSE 0 END), 0) as bucket_60_plus
            FROM vendor_invoices
            WHERE status IN ('Open', 'Pending', 'Overdue') AND (total_amount - amount_paid) > 0
        ''').fetchone()
    else:
        ap_buckets = conn.execute('''
            SELECT 
                COALESCE(SUM(CASE WHEN julianday('now') - julianday(invoice_date) <= 30 THEN (total_amount - amount_paid) ELSE 0 END), 0) as bucket_0_30,
                COALESCE(SUM(CASE WHEN julianday('now') - julianday(invoice_date) > 30 AND julianday('now') - julianday(invoice_date) <= 60 THEN (total_amount - amount_paid) ELSE 0 END), 0) as bucket_31_60,
                COALESCE(SUM(CASE WHEN julianday('now') - julianday(invoice_date) > 60 THEN (total_amount - amount_paid) ELSE 0 END), 0) as bucket_60_plus
            FROM vendor_invoices
            WHERE status IN ('Open', 'Pending', 'Overdue') AND (total_amount - amount_paid) > 0
        ''').fetchone()
    
    ap_over_30 = float(ap_buckets['bucket_31_60'] or 0) + float(ap_buckets['bucket_60_plus'] or 0)
    ap_over_30_pct = (ap_over_30 / total_ap * 100) if total_ap > 0 else 0
    
    # === OPERATIONS PERFORMANCE ===
    
    # Open Sales Orders with completion tracking
    open_sales_orders = conn.execute('''
        SELECT 
            so.id, so.so_number as order_number, so.order_date, so.expected_ship_date as required_date, so.status,
            so.total_amount as order_value,
            c.name as customer_name,
            COALESCE(wo.total_wo_value, 0) as budgeted_expense,
            COALESCE(wo.actual_cost, 0) as actual_cost,
            COALESCE(wo.completion_pct, 0) as completion_pct
        FROM sales_orders so
        LEFT JOIN customers c ON so.customer_id = c.id
        LEFT JOIN (
            SELECT 
                so_id,
                SUM(COALESCE(labor_cost, 0) + COALESCE(material_cost, 0) + COALESCE(overhead_cost, 0)) as total_wo_value,
                SUM(COALESCE(labor_cost, 0) + COALESCE(material_cost, 0) + COALESCE(overhead_cost, 0)) as actual_cost,
                AVG(CASE 
                    WHEN status = 'Completed' THEN 100
                    WHEN status = 'In Progress' THEN 50
                    WHEN status = 'Released' THEN 25
                    ELSE 0
                END) as completion_pct
            FROM work_orders
            WHERE so_id IS NOT NULL
            GROUP BY so_id
        ) wo ON so.id = wo.so_id
        WHERE so.status NOT IN ('Closed', 'Cancelled', 'Shipped')
        ORDER BY so.expected_ship_date ASC
        LIMIT 20
    ''').fetchall()
    
    # Dated Sales Orders (future delivery dates)
    if is_postgres:
        dated_orders = conn.execute('''
            SELECT 
                so.id, so.so_number as order_number, so.order_date, so.expected_ship_date as required_date, so.status,
                so.total_amount as order_value,
                c.name as customer_name,
                (so.expected_ship_date::date - CURRENT_DATE) as days_until_delivery
            FROM sales_orders so
            LEFT JOIN customers c ON so.customer_id = c.id
            WHERE so.expected_ship_date > CURRENT_DATE
              AND so.status NOT IN ('Closed', 'Cancelled', 'Shipped')
            ORDER BY so.expected_ship_date ASC
            LIMIT 15
        ''').fetchall()
    else:
        dated_orders = conn.execute('''
            SELECT 
                so.id, so.so_number as order_number, so.order_date, so.expected_ship_date as required_date, so.status,
                so.total_amount as order_value,
                c.name as customer_name,
                julianday(so.expected_ship_date) - julianday('now') as days_until_delivery
            FROM sales_orders so
            LEFT JOIN customers c ON so.customer_id = c.id
            WHERE date(so.expected_ship_date) > date('now')
              AND so.status NOT IN ('Closed', 'Cancelled', 'Shipped')
            ORDER BY so.expected_ship_date ASC
            LIMIT 15
        ''').fetchall()
    
    future_revenue = sum(float(o['order_value'] or 0) for o in dated_orders)
    
    # === INVENTORY STATUS ===
    
    # Ready-to-Sell Inventory
    ready_inventory = conn.execute('''
        SELECT 
            COALESCE(SUM(i.quantity), 0) as total_qty,
            COALESCE(SUM(i.quantity * COALESCE(i.unit_cost, p.cost, 0)), 0) as total_value,
            COUNT(DISTINCT i.id) as sku_count
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.quantity > 0
          AND i.status IN ('Available', 'Serviceable', 'Ready')
    ''').fetchone()
    
    ready_qty = float(ready_inventory['total_qty'] or 0)
    ready_value = float(ready_inventory['total_value'] or 0)
    
    # Inventory awaiting work/certification
    awaiting_work = conn.execute('''
        SELECT 
            COALESCE(SUM(i.quantity), 0) as total_qty,
            COALESCE(SUM(i.quantity * COALESCE(i.unit_cost, p.cost, 0)), 0) as total_value,
            COUNT(DISTINCT i.id) as item_count
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.quantity > 0
          AND i.status IN ('In Repair', 'Awaiting Certification', 'QC Hold', 'Inspection', 'Quarantine')
    ''').fetchone()
    
    awaiting_qty = float(awaiting_work['total_qty'] or 0)
    awaiting_value = float(awaiting_work['total_value'] or 0)
    
    # Work orders by status for inventory tie-up
    wo_status_summary = conn.execute('''
        SELECT 
            status,
            COUNT(*) as count,
            COALESCE(SUM(COALESCE(labor_cost, 0) + COALESCE(material_cost, 0) + COALESCE(overhead_cost, 0)), 0) as total_value
        FROM work_orders
        WHERE status NOT IN ('Completed', 'Closed', 'Cancelled')
        GROUP BY status
        ORDER BY count DESC
    ''').fetchall()
    
    # Inventory by category for drill-down
    inventory_by_category = conn.execute('''
        SELECT 
            COALESCE(p.product_category, 'Uncategorized') as category,
            SUM(i.quantity) as qty,
            SUM(i.quantity * COALESCE(i.unit_cost, p.cost, 0)) as value
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.quantity > 0
        GROUP BY p.product_category
        ORDER BY value DESC
        LIMIT 8
    ''').fetchall()
    
    conn.close()
    
    # === GENERATE INSIGHTS & ALERTS ===
    insights = []
    
    # AR Risk Alert
    if ar_over_30_pct > 25:
        insights.append({
            'type': 'warning',
            'icon': 'exclamation-triangle',
            'message': f'AR over 30 days exceeds {ar_over_30_pct:.0f}% (${ar_over_30:,.0f}) — potential cash flow risk'
        })
    
    # AP Alert
    if ap_over_30_pct > 30:
        insights.append({
            'type': 'danger',
            'icon': 'clock-history',
            'message': f'AP over 30 days at {ap_over_30_pct:.0f}% (${ap_over_30:,.0f}) — review payment schedule'
        })
    
    # Week-over-week performance
    if week_change_pct < -20:
        insights.append({
            'type': 'danger',
            'icon': 'graph-down-arrow',
            'message': f'Weekly revenue down {abs(week_change_pct):.0f}% vs prior week — investigate sales pipeline'
        })
    elif week_change_pct > 20:
        insights.append({
            'type': 'success',
            'icon': 'graph-up-arrow',
            'message': f'Weekly revenue up {week_change_pct:.0f}% vs prior week — strong sales momentum'
        })
    
    # Future revenue
    if future_revenue > 0:
        insights.append({
            'type': 'info',
            'icon': 'calendar-check',
            'message': f'Upcoming dated orders represent ${future_revenue:,.0f} in future revenue'
        })
    
    # Inventory awaiting work
    if awaiting_value > ready_value * 0.3:
        insights.append({
            'type': 'warning',
            'icon': 'box-seam',
            'message': f'${awaiting_value:,.0f} in inventory awaiting work/certification — review processing queue'
        })
    
    # Profit margin alert
    if profit_margin < 20 and ytd_revenue > 0:
        insights.append({
            'type': 'warning',
            'icon': 'percent',
            'message': f'YTD profit margin at {profit_margin:.1f}% — below target threshold'
        })
    
    # Build KPI dictionary
    financial = {
        'current_week_revenue': current_week_revenue,
        'prior_week_revenue': prior_week_revenue,
        'week_change_pct': round(week_change_pct, 1),
        'ytd_revenue': ytd_revenue,
        'ytd_gross_profit': ytd_gross_profit,
        'profit_margin': round(profit_margin, 1),
        'total_ar': total_ar,
        'ar_0_30': float(ar_buckets['bucket_0_30'] or 0),
        'ar_31_60': float(ar_buckets['bucket_31_60'] or 0),
        'ar_60_plus': float(ar_buckets['bucket_60_plus'] or 0),
        'ar_over_30_pct': round(ar_over_30_pct, 1),
        'total_ap': total_ap,
        'ap_0_30': float(ap_buckets['bucket_0_30'] or 0),
        'ap_31_60': float(ap_buckets['bucket_31_60'] or 0),
        'ap_60_plus': float(ap_buckets['bucket_60_plus'] or 0),
        'ap_over_30_pct': round(ap_over_30_pct, 1)
    }
    
    operations = {
        'open_orders_count': len(open_sales_orders),
        'open_orders_value': sum(float(o['order_value'] or 0) for o in open_sales_orders),
        'dated_orders_count': len(dated_orders),
        'future_revenue': future_revenue
    }
    
    inventory = {
        'ready_qty': ready_qty,
        'ready_value': ready_value,
        'awaiting_qty': awaiting_qty,
        'awaiting_value': awaiting_value
    }
    
    return render_template('reports/organizational_scorecard.html',
                         financial=financial,
                         operations=operations,
                         inventory=inventory,
                         open_sales_orders=open_sales_orders,
                         dated_orders=dated_orders,
                         wo_status_summary=wo_status_summary,
                         inventory_by_category=inventory_by_category,
                         insights=insights,
                         snapshot_time=datetime.now().strftime('%Y-%m-%d %H:%M'))
