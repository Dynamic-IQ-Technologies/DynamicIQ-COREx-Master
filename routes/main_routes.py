from flask import Blueprint, render_template, session, jsonify, redirect, url_for
from models import Database
from mrp_logic import MRPEngine
from auth import login_required
from datetime import datetime, timedelta

main_bp = Blueprint('main_routes', __name__)

@main_bp.route('/health')
def health_check():
    """Lightweight health check endpoint for deployment"""
    return jsonify({"status": "healthy"}), 200

@main_bp.route('/')
def root():
    """Root endpoint - handles health checks and redirects to dashboard"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    return dashboard()

@main_bp.route('/dashboard')
@login_required
def dashboard():
    db = Database()
    conn = db.get_connection()
    mrp = MRPEngine()
    
    today = datetime.now().strftime('%Y-%m-%d')
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    # === CORE COUNTS ===
    products_count = conn.execute('SELECT COUNT(*) as count FROM products').fetchone()['count']
    suppliers_count = conn.execute('SELECT COUNT(*) as count FROM suppliers').fetchone()['count']
    customers_count = conn.execute('SELECT COUNT(*) as count FROM customers').fetchone()['count']
    
    # === WORK ORDERS (PRODUCTION) ===
    wo_stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Released' THEN 1 ELSE 0 END) as released,
            SUM(CASE WHEN status = 'In Progress' THEN 1 ELSE 0 END) as in_progress,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN status = 'On Hold' THEN 1 ELSE 0 END) as on_hold,
            SUM(CASE WHEN status NOT IN ('Completed', 'Cancelled') THEN 1 ELSE 0 END) as active
        FROM work_orders
    ''').fetchone()
    
    active_work_orders = conn.execute('''
        SELECT wo.*, p.code, p.name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.status NOT IN ('Completed', 'Cancelled')
        ORDER BY wo.planned_start_date ASC
        LIMIT 8
    ''').fetchall()
    
    # === SALES ORDERS ===
    so_stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Draft' THEN 1 ELSE 0 END) as draft,
            SUM(CASE WHEN status = 'Confirmed' THEN 1 ELSE 0 END) as confirmed,
            SUM(CASE WHEN status = 'Processing' THEN 1 ELSE 0 END) as processing,
            SUM(CASE WHEN status = 'Shipped' THEN 1 ELSE 0 END) as shipped,
            SUM(CASE WHEN status NOT IN ('Completed', 'Cancelled') THEN 1 ELSE 0 END) as active,
            COALESCE(SUM(CASE WHEN status NOT IN ('Completed', 'Cancelled') THEN total_amount ELSE 0 END), 0) as active_value
        FROM sales_orders
    ''').fetchone()
    
    recent_sales_orders = conn.execute('''
        SELECT so.*, c.name as customer_name
        FROM sales_orders so
        LEFT JOIN customers c ON so.customer_id = c.id
        WHERE so.status NOT IN ('Completed', 'Cancelled')
        ORDER BY so.order_date DESC
        LIMIT 5
    ''').fetchall()
    
    # === PURCHASE ORDERS (PROCUREMENT) ===
    po_stats = conn.execute('''
        SELECT 
            COUNT(DISTINCT po.id) as total,
            SUM(CASE WHEN po.status = 'Draft' THEN 1 ELSE 0 END) as draft,
            SUM(CASE WHEN po.status = 'Ordered' THEN 1 ELSE 0 END) as ordered,
            SUM(CASE WHEN po.status = 'Partially Received' THEN 1 ELSE 0 END) as partial,
            SUM(CASE WHEN po.status NOT IN ('Received', 'Cancelled') THEN 1 ELSE 0 END) as open_count
        FROM purchase_orders po
    ''').fetchone()
    
    po_open_value = conn.execute('''
        SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) as open_value
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        WHERE po.status NOT IN ('Received', 'Cancelled')
    ''').fetchone()['open_value']
    
    pending_pos = conn.execute('''
        SELECT po.*, s.name as supplier_name
        FROM purchase_orders po
        LEFT JOIN suppliers s ON po.supplier_id = s.id
        WHERE po.status IN ('Draft', 'Ordered', 'Partially Received')
        ORDER BY po.expected_delivery_date ASC
        LIMIT 5
    ''').fetchall()
    
    # === INVENTORY ===
    inventory_stats = conn.execute('''
        SELECT 
            COUNT(*) as total_items,
            COALESCE(SUM(quantity), 0) as total_qty,
            COALESCE(SUM(i.quantity * COALESCE(p.cost, 0)), 0) as total_value
        FROM inventory i
        LEFT JOIN products p ON i.product_id = p.id
    ''').fetchone()
    
    low_stock = conn.execute('''
        SELECT i.*, p.code, p.name 
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.quantity <= i.reorder_point AND i.reorder_point > 0
        ORDER BY (i.quantity * 1.0 / NULLIF(i.reorder_point, 0)) ASC
        LIMIT 8
    ''').fetchall()
    
    low_stock_count = conn.execute('''
        SELECT COUNT(*) as count FROM inventory WHERE quantity <= reorder_point AND reorder_point > 0
    ''').fetchone()['count']
    
    # === INVOICES / ACCOUNTS RECEIVABLE ===
    invoice_stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Draft' THEN 1 ELSE 0 END) as draft,
            SUM(CASE WHEN status = 'Sent' THEN 1 ELSE 0 END) as sent,
            SUM(CASE WHEN status = 'Overdue' THEN 1 ELSE 0 END) as overdue,
            COALESCE(SUM(CASE WHEN status IN ('Sent', 'Overdue') THEN total_amount - COALESCE(amount_paid, 0) ELSE 0 END), 0) as ar_outstanding
        FROM invoices
    ''').fetchone()
    
    # === ACCOUNTS PAYABLE ===
    # Get vendor invoices outstanding
    vi_stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Open' THEN 1 ELSE 0 END) as open_count,
            SUM(CASE WHEN status = 'Approved' THEN 1 ELSE 0 END) as approved,
            COALESCE(SUM(CASE WHEN status IN ('Open', 'Approved') THEN total_amount - COALESCE(amount_paid, 0) ELSE 0 END), 0) as ap_outstanding
        FROM vendor_invoices
    ''').fetchone()
    
    # Also get open purchase orders value (not yet invoiced)
    po_outstanding = conn.execute('''
        SELECT 
            COUNT(DISTINCT po.id) as open_count,
            COALESCE(SUM(pol.quantity * pol.unit_price), 0) as total_value
        FROM purchase_orders po
        LEFT JOIN purchase_order_lines pol ON po.id = pol.po_id
        WHERE po.status IN ('Sent', 'Approved', 'Partial')
    ''').fetchone()
    
    # Combine A/P stats
    ap_stats = {
        'total': (vi_stats['total'] or 0) + (po_outstanding['open_count'] or 0),
        'open_count': (vi_stats['open_count'] or 0) + (po_outstanding['open_count'] or 0),
        'approved': vi_stats['approved'] or 0,
        'ap_outstanding': (vi_stats['ap_outstanding'] or 0) + (po_outstanding['total_value'] or 0)
    }
    
    # === SERVICE WORK ORDERS ===
    swo_stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status NOT IN ('Completed', 'Cancelled', 'Invoiced') THEN 1 ELSE 0 END) as active
        FROM service_work_orders
    ''').fetchone()
    
    # === SHIPMENTS ===
    shipment_stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END) as pending,
            SUM(CASE WHEN status = 'In Transit' THEN 1 ELSE 0 END) as in_transit
        FROM shipments
    ''').fetchone()
    
    # === LABOR / WORKFORCE ===
    active_labor = conn.execute('''
        SELECT COUNT(*) as count FROM labor_resources WHERE status = 'Active'
    ''').fetchone()['count']
    
    clocked_in_today = conn.execute('''
        SELECT COUNT(DISTINCT t1.employee_id) as count 
        FROM time_clock_punches t1
        WHERE DATE(t1.punch_time) = DATE('now') 
        AND t1.punch_type = 'Clock In'
        AND NOT EXISTS (
            SELECT 1 FROM time_clock_punches t2 
            WHERE t2.employee_id = t1.employee_id 
            AND t2.punch_type = 'Clock Out' 
            AND t2.punch_time > t1.punch_time
            AND DATE(t2.punch_time) = DATE('now')
        )
    ''').fetchone()['count']
    
    # === SHORTAGE ITEMS ===
    shortage_items = mrp.get_shortage_items()
    
    # === WORK ORDER TRENDS (Last 30 days by week) ===
    wo_trend = conn.execute('''
        SELECT 
            strftime('%Y-%W', created_at) as week,
            COUNT(*) as count
        FROM work_orders 
        WHERE created_at >= ?
        GROUP BY week
        ORDER BY week
    ''', (thirty_days_ago,)).fetchall()
    
    # === SALES TREND (Last 30 days) ===
    sales_trend = conn.execute('''
        SELECT 
            strftime('%Y-%m-%d', order_date) as day,
            COUNT(*) as count,
            COALESCE(SUM(total_amount), 0) as value
        FROM sales_orders 
        WHERE order_date >= ?
        GROUP BY day
        ORDER BY day
    ''', (thirty_days_ago,)).fetchall()
    
    conn.close()
    
    return render_template('dashboard.html',
                         products_count=products_count,
                         suppliers_count=suppliers_count,
                         customers_count=customers_count,
                         wo_stats=wo_stats,
                         active_work_orders=active_work_orders,
                         so_stats=so_stats,
                         recent_sales_orders=recent_sales_orders,
                         po_stats=po_stats,
                         po_open_value=po_open_value,
                         pending_pos=pending_pos,
                         inventory_stats=inventory_stats,
                         low_stock=low_stock,
                         low_stock_count=low_stock_count,
                         invoice_stats=invoice_stats,
                         ap_stats=ap_stats,
                         swo_stats=swo_stats,
                         shipment_stats=shipment_stats,
                         active_labor=active_labor,
                         clocked_in_today=clocked_in_today,
                         shortage_items=shortage_items,
                         wo_trend=[dict(r) for r in wo_trend],
                         sales_trend=[dict(r) for r in sales_trend],
                         user_role=session.get('role'))
