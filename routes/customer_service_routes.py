from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session
from auth import login_required, role_required
from models import Database
from datetime import datetime, timedelta

customer_service_bp = Blueprint('customer_service', __name__)

DEFAULT_STAGES = [
    ('Order Received', 1),
    ('Engineering Review', 2),
    ('Material Procurement', 3),
    ('Production', 4),
    ('Quality Assurance', 5),
    ('Shipping', 6)
]

def update_sales_order_status(conn, so_id):
    """Auto-update sales order status based on linked work orders and stages"""
    work_orders = conn.execute('''
        SELECT status FROM work_orders WHERE so_id = ?
    ''', (so_id,)).fetchall()
    
    stages = conn.execute('''
        SELECT stage_status FROM order_stage_tracking WHERE sales_order_id = ?
    ''', (so_id,)).fetchall()
    
    order = conn.execute('SELECT status FROM sales_orders WHERE id = ?', (so_id,)).fetchone()
    if not order:
        return
    
    current_status = order['status']
    new_status = current_status
    
    if work_orders:
        wo_statuses = [wo['status'] for wo in work_orders]
        
        if all(s in ['Completed', 'Complete'] for s in wo_statuses):
            new_status = 'Completed'
        elif any(s in ['In Progress', 'Released'] for s in wo_statuses):
            new_status = 'In Production'
        elif all(s in ['Planned', 'Pending'] for s in wo_statuses) and current_status == 'Approved':
            new_status = 'Approved'
    
    if stages:
        stage_statuses = [s['stage_status'] for s in stages]
        if all(s == 'Complete' for s in stage_statuses):
            new_status = 'Completed'
    
    if new_status != current_status and current_status not in ['Shipped', 'Closed', 'Cancelled']:
        conn.execute('''
            UPDATE sales_orders SET status = ? WHERE id = ?
        ''', (new_status, so_id))

@customer_service_bp.route('/customer-service')
@login_required
@role_required('Admin', 'Planner', 'Production Staff', 'Procurement')
def dashboard():
    db = Database()
    conn = db.get_connection()
    
    orders_by_status_rows = conn.execute('''
        SELECT status, COUNT(*) as count 
        FROM sales_orders 
        GROUP BY status
    ''').fetchall()
    orders_by_status = [{'status': row['status'], 'count': row['count']} for row in orders_by_status_rows]
    
    pending_quotes = conn.execute('''
        SELECT so.*, c.name as customer_name,
               JULIANDAY(DATE('now')) - JULIANDAY(so.order_date) as days_pending
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        WHERE so.status IN ('Draft', 'Quoted', 'Pending Approval')
        ORDER BY so.order_date ASC
        LIMIT 10
    ''').fetchall()
    
    work_orders_awaiting = conn.execute('''
        SELECT wo.*, p.name as product_name, p.code as product_code,
               so.so_number, c.name as customer_name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN sales_orders so ON wo.so_id = so.id
        LEFT JOIN customers c ON COALESCE(wo.customer_id, so.customer_id) = c.id
        WHERE wo.status IN ('Planned', 'Pending')
        ORDER BY wo.planned_start_date ASC
        LIMIT 10
    ''').fetchall()
    
    recent_confirmations = conn.execute('''
        SELECT woc.*, wo.wo_number, u.username as confirmed_by_name
        FROM work_order_confirmations woc
        JOIN work_orders wo ON woc.work_order_id = wo.id
        JOIN users u ON woc.confirmed_by = u.id
        ORDER BY woc.confirmation_date DESC
        LIMIT 5
    ''').fetchall()
    
    at_risk_orders = conn.execute('''
        SELECT so.*, c.name as customer_name,
               JULIANDAY(so.expected_ship_date) - JULIANDAY(DATE('now')) as days_until_due
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        WHERE so.status NOT IN ('Completed', 'Shipped', 'Closed', 'Cancelled')
          AND so.expected_ship_date IS NOT NULL
          AND JULIANDAY(so.expected_ship_date) - JULIANDAY(DATE('now')) <= 7
        ORDER BY so.expected_ship_date ASC
        LIMIT 10
    ''').fetchall()
    
    total_orders = conn.execute('SELECT COUNT(*) FROM sales_orders').fetchone()[0]
    active_orders = conn.execute('''
        SELECT COUNT(*) FROM sales_orders 
        WHERE status NOT IN ('Completed', 'Shipped', 'Closed', 'Cancelled')
    ''').fetchone()[0]
    pending_confirmation = conn.execute('''
        SELECT COUNT(*) FROM work_orders 
        WHERE status IN ('Planned', 'Pending')
    ''').fetchone()[0]
    overdue_count = conn.execute('''
        SELECT COUNT(*) FROM sales_orders 
        WHERE status NOT IN ('Completed', 'Shipped', 'Closed', 'Cancelled')
          AND expected_ship_date IS NOT NULL
          AND expected_ship_date < DATE('now')
    ''').fetchone()[0]
    
    conn.close()
    
    return render_template('customer_service/dashboard.html',
                         orders_by_status=orders_by_status,
                         pending_quotes=pending_quotes,
                         work_orders_awaiting=work_orders_awaiting,
                         recent_confirmations=recent_confirmations,
                         at_risk_orders=at_risk_orders,
                         total_orders=total_orders,
                         active_orders=active_orders,
                         pending_confirmation=pending_confirmation,
                         overdue_count=overdue_count)


@customer_service_bp.route('/customer-service/orders')
@login_required
@role_required('Admin', 'Planner', 'Production Staff', 'Procurement')
def orders_list():
    db = Database()
    conn = db.get_connection()
    
    status_filter = request.args.get('status', '')
    customer_filter = request.args.get('customer', '')
    
    query = '''
        SELECT so.*, c.name as customer_name, c.customer_number,
               (SELECT COUNT(*) FROM sales_order_lines WHERE so_id = so.id) as line_count
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND so.status = ?'
        params.append(status_filter)
    
    if customer_filter:
        query += ' AND so.customer_id = ?'
        params.append(customer_filter)
    
    query += ' ORDER BY so.order_date DESC'
    
    orders = conn.execute(query, params).fetchall()
    
    customers = conn.execute('SELECT id, name FROM customers ORDER BY name').fetchall()
    statuses = conn.execute('SELECT DISTINCT status FROM sales_orders ORDER BY status').fetchall()
    
    conn.close()
    
    return render_template('customer_service/orders_list.html',
                         orders=orders,
                         customers=customers,
                         statuses=statuses,
                         status_filter=status_filter,
                         customer_filter=customer_filter)


@customer_service_bp.route('/customer-service/orders/<int:order_id>')
@login_required
@role_required('Admin', 'Planner', 'Production Staff', 'Procurement')
def order_detail(order_id):
    db = Database()
    conn = db.get_connection()
    
    order = conn.execute('''
        SELECT so.*, c.name as customer_name, c.customer_number, c.email as customer_email,
               c.phone as customer_phone
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        WHERE so.id = ?
    ''', (order_id,)).fetchone()
    
    if not order:
        conn.close()
        flash('Order not found', 'danger')
        return redirect(url_for('customer_service.orders_list'))
    
    order_lines = conn.execute('''
        SELECT sol.*, p.code as product_code, p.name as product_name
        FROM sales_order_lines sol
        JOIN products p ON sol.product_id = p.id
        WHERE sol.so_id = ?
        ORDER BY sol.line_number
    ''', (order_id,)).fetchall()
    
    linked_work_orders = conn.execute('''
        SELECT wo.*, p.code as product_code, p.name as product_name,
               woc.confirmation_date, woc.confirmed_by,
               u.username as confirmed_by_name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN work_order_confirmations woc ON wo.id = woc.work_order_id
        LEFT JOIN users u ON woc.confirmed_by = u.id
        WHERE wo.so_id = ?
        ORDER BY wo.created_at DESC
    ''', (order_id,)).fetchall()
    
    stages = conn.execute('''
        SELECT * FROM order_stage_tracking 
        WHERE sales_order_id = ?
        ORDER BY stage_order
    ''', (order_id,)).fetchall()
    
    if not stages:
        for stage_name, stage_order in DEFAULT_STAGES:
            conn.execute('''
                INSERT INTO order_stage_tracking 
                (sales_order_id, stage_name, stage_order, stage_status, percent_complete)
                VALUES (?, ?, ?, 'Not Started', 0)
            ''', (order_id, stage_name, stage_order))
        conn.commit()
        stages = conn.execute('''
            SELECT * FROM order_stage_tracking 
            WHERE sales_order_id = ?
            ORDER BY stage_order
        ''', (order_id,)).fetchall()
    
    conn.close()
    
    return render_template('customer_service/order_detail.html',
                         order=order,
                         order_lines=order_lines,
                         linked_work_orders=linked_work_orders,
                         stages=stages)


@customer_service_bp.route('/customer-service/orders/<int:order_id>/update-stage', methods=['POST'])
@login_required
@role_required('Admin', 'Planner', 'Production Staff')
def update_stage(order_id):
    db = Database()
    conn = db.get_connection()
    
    stage_id = request.form.get('stage_id')
    new_status = request.form.get('status')
    percent_complete = request.form.get('percent_complete', 0)
    
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        if new_status == 'In Progress':
            conn.execute('''
                UPDATE order_stage_tracking 
                SET stage_status = ?, percent_complete = ?, started_at = COALESCE(started_at, ?), updated_at = ?
                WHERE id = ?
            ''', (new_status, percent_complete, now, now, stage_id))
        elif new_status == 'Complete':
            conn.execute('''
                UPDATE order_stage_tracking 
                SET stage_status = ?, percent_complete = 100, completed_at = ?, updated_at = ?
                WHERE id = ?
            ''', (new_status, now, now, stage_id))
        else:
            conn.execute('''
                UPDATE order_stage_tracking 
                SET stage_status = ?, percent_complete = ?, updated_at = ?
                WHERE id = ?
            ''', (new_status, percent_complete, now, stage_id))
        
        update_sales_order_status(conn, order_id)
        
        conn.commit()
        flash('Stage updated successfully', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error updating stage: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('customer_service.order_detail', order_id=order_id))


@customer_service_bp.route('/customer-service/work-orders-confirmation')
@login_required
@role_required('Admin', 'Planner')
def work_orders_confirmation():
    db = Database()
    conn = db.get_connection()
    
    work_orders = conn.execute('''
        SELECT wo.*, p.code as product_code, p.name as product_name,
               so.so_number, so.status as so_status, c.name as customer_name,
               (SELECT SUM(CASE WHEN i.quantity >= mr.required_quantity THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
                FROM material_requirements mr
                LEFT JOIN inventory i ON mr.product_id = i.product_id
                WHERE mr.work_order_id = wo.id) as material_availability
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN sales_orders so ON wo.so_id = so.id
        LEFT JOIN customers c ON COALESCE(wo.customer_id, so.customer_id) = c.id
        WHERE wo.status IN ('Planned', 'Pending')
        ORDER BY wo.priority DESC, wo.planned_start_date ASC
    ''').fetchall()
    
    conn.close()
    
    return render_template('customer_service/work_orders_confirmation.html',
                         work_orders=work_orders)


@customer_service_bp.route('/customer-service/work-orders/<int:wo_id>/confirm', methods=['POST'])
@login_required
@role_required('Admin', 'Planner')
def confirm_work_order(wo_id):
    db = Database()
    conn = db.get_connection()
    
    work_order = conn.execute('SELECT * FROM work_orders WHERE id = ?', (wo_id,)).fetchone()
    
    if not work_order:
        conn.close()
        flash('Work order not found', 'danger')
        return redirect(url_for('customer_service.work_orders_confirmation'))
    
    quote_approved = 1 if request.form.get('quote_approved') else 0
    materials_available = 1 if request.form.get('materials_available') else 0
    capacity_available = 1 if request.form.get('capacity_available') else 0
    confirmation_notes = request.form.get('confirmation_notes', '')
    
    try:
        previous_status = work_order['status']
        new_status = 'Released'
        
        conn.execute('''
            UPDATE work_orders SET status = ? WHERE id = ?
        ''', (new_status, wo_id))
        
        conn.execute('''
            INSERT INTO work_order_confirmations 
            (work_order_id, confirmed_by, quote_approved, materials_available, 
             capacity_available, confirmation_notes, previous_status, new_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (wo_id, session.get('user_id'), quote_approved, materials_available,
              capacity_available, confirmation_notes, previous_status, new_status))
        
        if work_order['so_id']:
            update_sales_order_status(conn, work_order['so_id'])
        
        conn.commit()
        flash(f'Work Order {work_order["wo_number"]} confirmed and released successfully', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error confirming work order: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('customer_service.work_orders_confirmation'))


@customer_service_bp.route('/customer-service/pending-quotes')
@login_required
@role_required('Admin', 'Planner', 'Production Staff', 'Procurement')
def pending_quotes():
    db = Database()
    conn = db.get_connection()
    
    quotes = conn.execute('''
        SELECT so.*, c.name as customer_name, c.email as customer_email,
               JULIANDAY(DATE('now')) - JULIANDAY(so.order_date) as days_pending,
               u.username as created_by_name
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        LEFT JOIN users u ON so.created_by = u.id
        WHERE so.status IN ('Draft', 'Quoted', 'Pending Approval')
        ORDER BY so.order_date ASC
    ''').fetchall()
    
    conn.close()
    
    return render_template('customer_service/pending_quotes.html', quotes=quotes)


@customer_service_bp.route('/customer-service/quotes/<int:order_id>/follow-up', methods=['POST'])
@login_required
@role_required('Admin', 'Planner')
def follow_up_quote(order_id):
    db = Database()
    conn = db.get_connection()
    
    order = conn.execute('SELECT * FROM sales_orders WHERE id = ?', (order_id,)).fetchone()
    
    if not order:
        conn.close()
        flash('Order not found', 'danger')
        return redirect(url_for('customer_service.pending_quotes'))
    
    try:
        notes = order['notes'] or ''
        follow_up_note = f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Follow-up by {session.get('username', 'User')}"
        
        conn.execute('''
            UPDATE sales_orders SET notes = ? WHERE id = ?
        ''', (notes + follow_up_note, order_id))
        
        conn.commit()
        flash(f'Follow-up recorded for order {order["so_number"]}', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error recording follow-up: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('customer_service.pending_quotes'))


@customer_service_bp.route('/customer-service/at-risk')
@login_required
@role_required('Admin', 'Planner', 'Production Staff', 'Procurement')
def at_risk_orders():
    db = Database()
    conn = db.get_connection()
    
    orders = conn.execute('''
        SELECT so.*, c.name as customer_name,
               JULIANDAY(so.expected_ship_date) - JULIANDAY(DATE('now')) as days_until_due,
               CASE 
                   WHEN so.expected_ship_date < DATE('now') THEN 'Overdue'
                   WHEN JULIANDAY(so.expected_ship_date) - JULIANDAY(DATE('now')) <= 3 THEN 'Critical'
                   WHEN JULIANDAY(so.expected_ship_date) - JULIANDAY(DATE('now')) <= 7 THEN 'Warning'
                   ELSE 'Normal'
               END as risk_level
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        WHERE so.status NOT IN ('Completed', 'Shipped', 'Closed', 'Cancelled')
          AND so.expected_ship_date IS NOT NULL
        ORDER BY so.expected_ship_date ASC
    ''').fetchall()
    
    conn.close()
    
    return render_template('customer_service/at_risk_orders.html', orders=orders)


@customer_service_bp.route('/customer-service/orders/<int:order_id>/create-work-order', methods=['POST'])
@login_required
@role_required('Admin', 'Planner')
def create_work_order_from_so(order_id):
    """Create a work order linked to a sales order"""
    db = Database()
    conn = db.get_connection()
    
    order = conn.execute('''
        SELECT so.*, c.name as customer_name
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        WHERE so.id = ?
    ''', (order_id,)).fetchone()
    
    if not order:
        conn.close()
        flash('Sales order not found', 'danger')
        return redirect(url_for('customer_service.orders_list'))
    
    product_id = request.form.get('product_id')
    quantity = request.form.get('quantity', 1)
    priority = request.form.get('priority', 'Medium')
    planned_start = request.form.get('planned_start_date')
    planned_end = request.form.get('planned_end_date')
    
    if not product_id:
        conn.close()
        flash('Please select a product for the work order', 'warning')
        return redirect(url_for('customer_service.order_detail', order_id=order_id))
    
    try:
        last_wo = conn.execute("SELECT wo_number FROM work_orders ORDER BY id DESC LIMIT 1").fetchone()
        if last_wo:
            last_num = int(last_wo['wo_number'].replace('WO-', ''))
            new_num = f"WO-{last_num + 1:06d}"
        else:
            new_num = "WO-000001"
        
        conn.execute('''
            INSERT INTO work_orders 
            (wo_number, product_id, quantity, status, priority, planned_start_date, planned_end_date, 
             so_id, customer_id)
            VALUES (?, ?, ?, 'Planned', ?, ?, ?, ?, ?)
        ''', (new_num, product_id, quantity, priority, planned_start, planned_end, 
              order_id, order['customer_id']))
        
        if order['status'] == 'Approved':
            conn.execute("UPDATE sales_orders SET status = 'In Production' WHERE id = ?", (order_id,))
        
        conn.commit()
        flash(f'Work Order {new_num} created successfully for {order["so_number"]}', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error creating work order: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('customer_service.order_detail', order_id=order_id))


@customer_service_bp.route('/customer-service/orders/<int:order_id>/approve', methods=['POST'])
@login_required
@role_required('Admin', 'Planner')
def approve_order(order_id):
    """Approve a sales order (mark quote as approved)"""
    db = Database()
    conn = db.get_connection()
    
    order = conn.execute('SELECT * FROM sales_orders WHERE id = ?', (order_id,)).fetchone()
    
    if not order:
        conn.close()
        flash('Order not found', 'danger')
        return redirect(url_for('customer_service.orders_list'))
    
    try:
        conn.execute('''
            UPDATE sales_orders SET status = 'Approved' WHERE id = ?
        ''', (order_id,))
        
        for stage_name, stage_order in DEFAULT_STAGES[:1]:
            conn.execute('''
                UPDATE order_stage_tracking 
                SET stage_status = 'In Progress', started_at = CURRENT_TIMESTAMP
                WHERE sales_order_id = ? AND stage_order = 1
            ''', (order_id,))
        
        conn.commit()
        flash(f'Order {order["so_number"]} approved successfully', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error approving order: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('customer_service.order_detail', order_id=order_id))
