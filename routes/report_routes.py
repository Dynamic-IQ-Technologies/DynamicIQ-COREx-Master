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
    
    # First, calculate which products have their total shortage covered by POs
    # Only count positive shortages to avoid edge cases with zero/negative values
    products_covered = conn.execute('''
        SELECT mr.product_id
        FROM material_requirements mr
        LEFT JOIN (
            SELECT pol.product_id, 
                   SUM(pol.quantity - COALESCE(pol.received_quantity, 0)) as qty_on_order
            FROM purchase_order_lines pol
            JOIN purchase_orders po ON pol.po_id = po.id
            WHERE po.status IN ('Ordered', 'Partially Received')
            GROUP BY pol.product_id
        ) po_summary ON mr.product_id = po_summary.product_id
        WHERE mr.status != 'Satisfied'
        GROUP BY mr.product_id
        HAVING SUM(CASE WHEN mr.shortage_quantity > 0 THEN mr.shortage_quantity ELSE 0 END) <= COALESCE(MAX(po_summary.qty_on_order), 0)
    ''').fetchall()
    
    # Extract product IDs that are fully covered
    covered_product_ids = [row['product_id'] for row in products_covered]
    
    # Now get all requirements, excluding products that are fully covered
    if covered_product_ids:
        placeholders = ','.join('?' * len(covered_product_ids))
        requirements = conn.execute(f'''
            SELECT mr.*, p.code, p.name, p.unit_of_measure, p.cost, 
                   wo.wo_number, wo.status as wo_status, wo.planned_start_date,
                   (mr.required_quantity * p.cost) as total_cost
            FROM material_requirements mr
            JOIN products p ON mr.product_id = p.id
            JOIN work_orders wo ON mr.work_order_id = wo.id
            WHERE mr.status != 'Satisfied'
              AND mr.product_id NOT IN ({placeholders})
            ORDER BY wo.planned_start_date DESC, mr.shortage_quantity DESC
        ''', covered_product_ids).fetchall()
    else:
        requirements = conn.execute('''
            SELECT mr.*, p.code, p.name, p.unit_of_measure, p.cost, 
                   wo.wo_number, wo.status as wo_status, wo.planned_start_date,
                   (mr.required_quantity * p.cost) as total_cost
            FROM material_requirements mr
            JOIN products p ON mr.product_id = p.id
            JOIN work_orders wo ON mr.work_order_id = wo.id
            WHERE mr.status != 'Satisfied'
            ORDER BY wo.planned_start_date DESC, mr.shortage_quantity DESC
        ''').fetchall()
    
    total_requirements = len(requirements)
    total_shortages = sum(1 for r in requirements if r['shortage_quantity'] > 0)
    total_cost = sum(r['total_cost'] for r in requirements)
    total_shortage_cost = sum(r['total_cost'] for r in requirements if r['shortage_quantity'] > 0)
    
    # Shortages by product - only show products not fully covered by POs
    if covered_product_ids:
        placeholders = ','.join('?' * len(covered_product_ids))
        shortages_by_product = conn.execute(f'''
            SELECT p.code, p.name, 
                   SUM(mr.shortage_quantity) as total_shortage,
                   SUM(mr.shortage_quantity * p.cost) as shortage_value
            FROM material_requirements mr
            JOIN products p ON mr.product_id = p.id
            WHERE mr.shortage_quantity > 0
              AND mr.product_id NOT IN ({placeholders})
            GROUP BY p.code, p.name
            ORDER BY shortage_value DESC
        ''', covered_product_ids).fetchall()
    else:
        shortages_by_product = conn.execute('''
            SELECT p.code, p.name, 
                   SUM(mr.shortage_quantity) as total_shortage,
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
    
    # Calculate which products have their total shortage covered by POs
    # Only count positive shortages to avoid edge cases with zero/negative values
    products_covered = conn.execute('''
        SELECT mr.product_id
        FROM material_requirements mr
        LEFT JOIN (
            SELECT product_id, 
                   SUM(quantity - COALESCE(received_quantity, 0)) as qty_on_order
            FROM purchase_orders
            WHERE status IN ('Ordered', 'Partially Received')
            GROUP BY product_id
        ) po_summary ON mr.product_id = po_summary.product_id
        WHERE mr.status != 'Satisfied'
        GROUP BY mr.product_id
        HAVING SUM(CASE WHEN mr.shortage_quantity > 0 THEN mr.shortage_quantity ELSE 0 END) <= COALESCE(MAX(po_summary.qty_on_order), 0)
    ''').fetchall()
    
    covered_product_ids = [row['product_id'] for row in products_covered]
    
    # Get requirements excluding products fully covered by POs
    if covered_product_ids:
        placeholders = ','.join('?' * len(covered_product_ids))
        requirements = conn.execute(f'''
            SELECT wo.wo_number, wo.status as wo_status, wo.planned_start_date,
                   p.code, p.name, p.unit_of_measure,
                   mr.required_quantity, mr.available_quantity, mr.shortage_quantity,
                   mr.status, p.cost, (mr.required_quantity * p.cost) as total_cost
            FROM material_requirements mr
            JOIN products p ON mr.product_id = p.id
            JOIN work_orders wo ON mr.work_order_id = wo.id
            WHERE mr.status != 'Satisfied'
              AND mr.product_id NOT IN ({placeholders})
            ORDER BY wo.planned_start_date DESC
        ''', covered_product_ids).fetchall()
    else:
        requirements = conn.execute('''
            SELECT wo.wo_number, wo.status as wo_status, wo.planned_start_date,
                   p.code, p.name, p.unit_of_measure,
                   mr.required_quantity, mr.available_quantity, mr.shortage_quantity,
                   mr.status, p.cost, (mr.required_quantity * p.cost) as total_cost
            FROM material_requirements mr
            JOIN products p ON mr.product_id = p.id
            JOIN work_orders wo ON mr.work_order_id = wo.id
            WHERE mr.status != 'Satisfied'
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
                        cursor.execute('''
                            INSERT INTO purchase_orders (po_number, supplier_id, product_id, quantity, 
                                                        unit_price, status, order_date)
                            VALUES (?, ?, ?, ?, ?, ?, DATE('now'))
                        ''', (next_po_number, supplier_id, product_id, quantity, unit_price, 'Ordered'))
                        
                        po_id = cursor.lastrowid
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
