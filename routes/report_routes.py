from flask import Blueprint, render_template
from models import Database
from auth import login_required

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
