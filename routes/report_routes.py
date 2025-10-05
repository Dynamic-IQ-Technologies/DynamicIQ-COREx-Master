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
    
    workorder_costs = conn.execute('''
        SELECT wo.*, p.code, p.name,
               (wo.material_cost + wo.labor_cost + wo.overhead_cost) as total_cost
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        ORDER BY wo.planned_start_date DESC
    ''').fetchall()
    
    conn.close()
    
    return render_template('reports/workorder_costs.html', workorder_costs=workorder_costs)

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
    
    requirements = conn.execute('''
        SELECT mr.*, p.code, p.name, p.unit_of_measure, p.cost, 
               wo.wo_number, wo.status as wo_status, wo.planned_start_date,
               (mr.required_quantity * p.cost) as total_cost
        FROM material_requirements mr
        JOIN products p ON mr.product_id = p.id
        JOIN work_orders wo ON mr.work_order_id = wo.id
        ORDER BY wo.planned_start_date DESC, mr.shortage_quantity DESC
    ''').fetchall()
    
    total_requirements = len(requirements)
    total_shortages = sum(1 for r in requirements if r['shortage_quantity'] > 0)
    total_cost = sum(r['total_cost'] for r in requirements)
    total_shortage_cost = sum(r['total_cost'] for r in requirements if r['shortage_quantity'] > 0)
    
    shortages_by_product = conn.execute('''
        SELECT p.code, p.name, SUM(mr.shortage_quantity) as total_shortage,
               SUM(mr.shortage_quantity * p.cost) as shortage_value
        FROM material_requirements mr
        JOIN products p ON mr.product_id = p.id
        WHERE mr.shortage_quantity > 0
        GROUP BY p.code, p.name
        ORDER BY shortage_value DESC
    ''').fetchall()
    
    conn.close()
    
    return render_template('reports/material_requirements.html', 
                         requirements=requirements,
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
    
    requirements = conn.execute('''
        SELECT wo.wo_number, wo.status as wo_status, wo.planned_start_date,
               p.code, p.name, p.unit_of_measure,
               mr.required_quantity, mr.available_quantity, mr.shortage_quantity,
               mr.status, p.cost, (mr.required_quantity * p.cost) as total_cost
        FROM material_requirements mr
        JOIN products p ON mr.product_id = p.id
        JOIN work_orders wo ON mr.work_order_id = wo.id
        ORDER BY wo.planned_start_date DESC
    ''').fetchall()
    
    conn.close()
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['WO Number', 'WO Status', 'Planned Start Date', 'Product Code', 'Product Name', 
                    'Unit of Measure', 'Required Qty', 'Available Qty', 'Shortage Qty', 
                    'Status', 'Unit Cost', 'Total Cost'])
    
    for req in requirements:
        writer.writerow([req['wo_number'], req['wo_status'], req['planned_start_date'],
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
    errors = []
    
    try:
        form_data = request.form.to_dict(flat=False)
        
        num_items = len([k for k in form_data.keys() if k.startswith('items[') and k.endswith('][product_id]')])
        
        for i in range(num_items):
            try:
                product_id = int(request.form.get(f'items[{i}][product_id]'))
                supplier_id = int(request.form.get(f'items[{i}][supplier_id]'))
                quantity = float(request.form.get(f'items[{i}][quantity]'))
                unit_price = float(request.form.get(f'items[{i}][unit_price]'))
                
                existing_po_count = conn.execute(
                    "SELECT COUNT(*) as count FROM purchase_orders WHERE po_number LIKE 'PO-%'"
                ).fetchone()['count']
                
                next_po_number = f"PO-{str(existing_po_count + 1).zfill(6)}"
                
                for attempt in range(5):
                    try:
                        cursor = conn.cursor()
                        cursor.execute('''
                            INSERT INTO purchase_orders (po_number, supplier_id, product_id, quantity, 
                                                        unit_price, status, order_date)
                            VALUES (?, ?, ?, ?, ?, ?, DATE('now'))
                        ''', (next_po_number, supplier_id, product_id, quantity, unit_price, 'Ordered'))
                        
                        conn.commit()
                        created_pos.append(next_po_number)
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
            flash(f'Successfully created {len(created_pos)} purchase order(s): {", ".join(created_pos)}', 'success')
        
        if errors:
            for error in errors:
                flash(error, 'warning')
                
    except Exception as e:
        flash(f'Error processing procurement: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('report_routes.material_requirements_report'))
