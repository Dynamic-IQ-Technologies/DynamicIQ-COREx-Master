from flask import Blueprint, render_template, session
from models import Database
from mrp_logic import MRPEngine
from auth import login_required

main_bp = Blueprint('main_routes', __name__)

@main_bp.route('/')
@login_required
def dashboard():
    db = Database()
    conn = db.get_connection()
    mrp = MRPEngine()
    
    products_count = conn.execute('SELECT COUNT(*) as count FROM products').fetchone()['count']
    work_orders_count = conn.execute('SELECT COUNT(*) as count FROM work_orders WHERE status != "Completed"').fetchone()['count']
    suppliers_count = conn.execute('SELECT COUNT(*) as count FROM suppliers').fetchone()['count']
    
    low_stock = conn.execute('''
        SELECT i.*, p.code, p.name 
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.quantity <= i.reorder_point
        ORDER BY i.quantity ASC
        LIMIT 10
    ''').fetchall()
    
    active_work_orders = conn.execute('''
        SELECT wo.*, p.code, p.name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.status != "Completed"
        ORDER BY wo.planned_start_date ASC
        LIMIT 10
    ''').fetchall()
    
    shortage_items = mrp.get_shortage_items()
    
    conn.close()
    
    return render_template('dashboard.html',
                         products_count=products_count,
                         work_orders_count=work_orders_count,
                         suppliers_count=suppliers_count,
                         low_stock=low_stock,
                         active_work_orders=active_work_orders,
                         shortage_items=shortage_items,
                         user_role=session.get('role'))
