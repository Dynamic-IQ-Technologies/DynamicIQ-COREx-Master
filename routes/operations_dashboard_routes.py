from flask import Blueprint, render_template, session, redirect, url_for, flash, request, make_response
from functools import wraps
from models import Database
from datetime import datetime, timedelta
import json

operations_dashboard_routes = Blueprint('operations_dashboard_routes', __name__)

def role_required(*roles):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user_id' not in session:
                flash('Please log in to access this page.', 'warning')
                return redirect(url_for('auth_routes.login'))
            if session.get('role') not in roles:
                flash('Access denied. Insufficient permissions.', 'danger')
                return redirect(url_for('main_routes.dashboard'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

@operations_dashboard_routes.route('/operations-dashboard')
@role_required('Admin', 'Planner', 'Accountant')
def dashboard():
    db = Database()
    conn = db.get_connection()
    
    # Get filter parameters
    date_range = request.args.get('date_range', '30')  # days
    
    # Calculate date ranges
    today = datetime.now()
    if date_range == '7':
        start_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')
        period_label = "Last 7 Days"
    elif date_range == '30':
        start_date = (today - timedelta(days=30)).strftime('%Y-%m-%d')
        period_label = "Last 30 Days"
    elif date_range == '90':
        start_date = (today - timedelta(days=90)).strftime('%Y-%m-%d')
        period_label = "Last 90 Days"
    else:
        start_date = (today - timedelta(days=30)).strftime('%Y-%m-%d')
        period_label = "Last 30 Days"
    
    end_date = today.strftime('%Y-%m-%d')
    
    # KPI 1: Work Order Status Distribution
    wo_status = conn.execute('''
        SELECT status, COUNT(*) as count
        FROM work_orders
        WHERE created_at >= ?
        GROUP BY status
    ''', (start_date,)).fetchall()
    
    total_wo = sum(row['count'] for row in wo_status)
    pending_wo = sum(row['count'] for row in wo_status if row['status'] == 'Pending')
    in_progress_wo = sum(row['count'] for row in wo_status if row['status'] == 'In Progress')
    completed_wo = sum(row['count'] for row in wo_status if row['status'] == 'Completed')
    
    # KPI 2: Production Efficiency (Completed vs Planned)
    efficiency_data = conn.execute('''
        SELECT 
            COUNT(*) as total_planned,
            SUM(CASE WHEN status = 'Completed' AND actual_end_date <= planned_end_date THEN 1 ELSE 0 END) as on_time_completed,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as total_completed
        FROM work_orders
        WHERE planned_start_date >= ?
    ''', (start_date,)).fetchone()
    
    production_efficiency = (efficiency_data['total_completed'] / efficiency_data['total_planned'] * 100) if efficiency_data['total_planned'] > 0 else 0
    on_time_delivery = (efficiency_data['on_time_completed'] / efficiency_data['total_completed'] * 100) if efficiency_data['total_completed'] > 0 else 0
    
    # KPI 3: Current Backlog (Pending + In Progress Work Orders)
    backlog_count = pending_wo + in_progress_wo
    backlog_value = conn.execute('''
        SELECT COALESCE(SUM(material_cost + labor_cost + overhead_cost), 0) as backlog_value
        FROM work_orders
        WHERE status IN ('Pending', 'In Progress')
    ''').fetchone()['backlog_value']
    
    # KPI 4: Resource Utilization (Active Labor)
    active_labor = conn.execute('''
        SELECT COUNT(*) as active_count
        FROM time_tracking
        WHERE clock_out_time IS NULL
    ''').fetchone()['active_count']
    
    total_labor = conn.execute('SELECT COUNT(*) FROM labor_resources WHERE is_active = 1').fetchone()[0]
    labor_utilization = (active_labor / total_labor * 100) if total_labor > 0 else 0
    
    # KPI 5: Inventory Health
    low_stock_items = conn.execute('''
        SELECT COUNT(*) as low_stock_count
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.quantity <= p.reorder_point
    ''').fetchone()['low_stock_count']
    
    total_inventory_items = conn.execute('SELECT COUNT(*) FROM inventory WHERE quantity > 0').fetchone()[0]
    
    # KPI 6: Purchase Order Status
    pending_pos = conn.execute('''
        SELECT COUNT(*) as pending_po_count
        FROM purchase_orders
        WHERE status = 'Pending'
    ''').fetchone()['pending_po_count']
    
    # Critical Alerts
    delayed_work_orders = conn.execute('''
        SELECT wo.*, p.name as product_name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.status IN ('Pending', 'In Progress')
        AND wo.planned_end_date < DATE('now')
        ORDER BY wo.planned_end_date
        LIMIT 10
    ''').fetchall()
    
    # Work Order Trend (Last 30 days)
    wo_trend = conn.execute('''
        SELECT 
            DATE(created_at) as date,
            COUNT(*) as count,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed
        FROM work_orders
        WHERE created_at >= DATE('now', '-30 days')
        GROUP BY DATE(created_at)
        ORDER BY date
    ''').fetchall()
    
    # Material Usage Trend
    material_usage = conn.execute('''
        SELECT 
            DATE(mi.issue_date) as date,
            COALESCE(SUM(mi.quantity_issued * p.unit_cost), 0) as value
        FROM material_issues mi
        JOIN products p ON mi.product_id = p.id
        WHERE mi.issue_date >= DATE('now', '-30 days')
        GROUP BY DATE(mi.issue_date)
        ORDER BY date
    ''').fetchall()
    
    # Top Products by Work Order Volume
    top_products = conn.execute('''
        SELECT 
            p.name as product_name,
            p.code as product_code,
            COUNT(wo.id) as wo_count,
            SUM(wo.quantity) as total_qty
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.created_at >= ?
        GROUP BY p.id, p.name, p.code
        ORDER BY wo_count DESC
        LIMIT 10
    ''', (start_date,)).fetchall()
    
    # Pending Approvals / Actions
    pending_material_issues = conn.execute('''
        SELECT COUNT(*) as count
        FROM material_requirements mr
        LEFT JOIN material_issues mi ON mr.work_order_id = mi.work_order_id AND mr.product_id = mi.product_id
        WHERE mi.id IS NULL
        AND mr.work_order_id IN (SELECT id FROM work_orders WHERE status IN ('Pending', 'In Progress'))
    ''').fetchone()['count']
    
    conn.close()
    
    # Prepare chart data
    wo_trend_dates = [row['date'] for row in wo_trend]
    wo_trend_created = [row['count'] for row in wo_trend]
    wo_trend_completed = [row['completed'] for row in wo_trend]
    
    material_usage_dates = [row['date'] for row in material_usage]
    material_usage_values = [float(row['value']) for row in material_usage]
    
    wo_status_labels = [row['status'] for row in wo_status]
    wo_status_counts = [row['count'] for row in wo_status]
    
    top_product_names = [f"{row['product_code']}: {row['product_name'][:20]}" for row in top_products]
    top_product_counts = [row['wo_count'] for row in top_products]
    
    return render_template('operations/executive_dashboard.html',
                         total_wo=total_wo,
                         pending_wo=pending_wo,
                         in_progress_wo=in_progress_wo,
                         completed_wo=completed_wo,
                         production_efficiency=production_efficiency,
                         on_time_delivery=on_time_delivery,
                         backlog_count=backlog_count,
                         backlog_value=backlog_value,
                         active_labor=active_labor,
                         total_labor=total_labor,
                         labor_utilization=labor_utilization,
                         low_stock_items=low_stock_items,
                         total_inventory_items=total_inventory_items,
                         pending_pos=pending_pos,
                         pending_material_issues=pending_material_issues,
                         delayed_work_orders=delayed_work_orders,
                         wo_trend_dates=wo_trend_dates,
                         wo_trend_created=wo_trend_created,
                         wo_trend_completed=wo_trend_completed,
                         material_usage_dates=material_usage_dates,
                         material_usage_values=material_usage_values,
                         wo_status_labels=wo_status_labels,
                         wo_status_counts=wo_status_counts,
                         top_product_names=top_product_names,
                         top_product_counts=top_product_counts,
                         period_label=period_label,
                         date_range=date_range,
                         last_updated=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
