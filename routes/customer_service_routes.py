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
    
    pending_followups = conn.execute('''
        SELECT COUNT(*) FROM customer_communications 
        WHERE follow_up_required = 1 AND follow_up_completed = 0
    ''').fetchone()[0]
    
    open_escalations = conn.execute('''
        SELECT COUNT(*) FROM order_escalations 
        WHERE status IN ('Open', 'In Progress')
    ''').fetchone()[0]
    
    recent_activity = conn.execute('''
        SELECT oal.*, so.so_number, u.username as created_by_name
        FROM order_activity_log oal
        JOIN sales_orders so ON oal.sales_order_id = so.id
        LEFT JOIN users u ON oal.created_by = u.id
        ORDER BY oal.created_at DESC
        LIMIT 10
    ''').fetchall()
    
    wo_quotes_draft = conn.execute('''
        SELECT q.*, wo.wo_number, wo.id as work_order_id, p.code as product_code, 
               p.name as product_name, c.name as customer_name, c.customer_number,
               (SELECT COUNT(*) FROM work_order_quote_lines WHERE quote_id = q.id) as line_count
        FROM work_order_quotes q
        JOIN work_orders wo ON q.work_order_id = wo.id
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN customers c ON wo.customer_id = c.id
        WHERE q.status = 'Draft'
        ORDER BY q.created_at DESC
    ''').fetchall()
    
    wo_quotes_submitted = conn.execute('''
        SELECT q.*, wo.wo_number, wo.id as work_order_id, p.code as product_code, 
               p.name as product_name, c.name as customer_name, c.customer_number,
               (SELECT COUNT(*) FROM work_order_quote_lines WHERE quote_id = q.id) as line_count
        FROM work_order_quotes q
        JOIN work_orders wo ON q.work_order_id = wo.id
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN customers c ON wo.customer_id = c.id
        WHERE q.status IN ('Pending Approval', 'Sent', 'Quoted', 'Submitted')
        ORDER BY q.created_at DESC
    ''').fetchall()
    
    wo_quotes_approved = conn.execute('''
        SELECT q.*, wo.wo_number, wo.id as work_order_id, p.code as product_code, 
               p.name as product_name, c.name as customer_name, c.customer_number,
               (SELECT COUNT(*) FROM work_order_quote_lines WHERE quote_id = q.id) as line_count
        FROM work_order_quotes q
        JOIN work_orders wo ON q.work_order_id = wo.id
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN customers c ON wo.customer_id = c.id
        WHERE q.status = 'Approved' AND COALESCE(q.acknowledged, 0) = 0
        ORDER BY q.created_at DESC
    ''').fetchall()
    
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
                         overdue_count=overdue_count,
                         pending_followups=pending_followups,
                         open_escalations=open_escalations,
                         recent_activity=recent_activity,
                         wo_quotes_draft=wo_quotes_draft,
                         wo_quotes_submitted=wo_quotes_submitted,
                         wo_quotes_approved=wo_quotes_approved)


@customer_service_bp.route('/customer-service/quotes/<int:quote_id>/acknowledge', methods=['POST', 'GET'])
@login_required
@role_required('Admin', 'Planner', 'Production Staff', 'Procurement')
def acknowledge_quote(quote_id):
    """Acknowledge an approved work order quote"""
    db = Database()
    conn = db.get_connection()
    
    # Get redirect destination (default to customer service dashboard)
    next_url = request.args.get('next', 'customer_service.dashboard')
    
    quote = conn.execute('SELECT * FROM work_order_quotes WHERE id = ?', (quote_id,)).fetchone()
    
    if not quote:
        conn.close()
        flash('Quote not found', 'danger')
        return redirect(url_for(next_url))
    
    if quote['status'] != 'Approved':
        conn.close()
        flash('Only approved quotes can be acknowledged', 'warning')
        return redirect(url_for(next_url))
    
    try:
        conn.execute('''
            UPDATE work_order_quotes 
            SET acknowledged = 1, acknowledged_by = ?, acknowledged_at = ?
            WHERE id = ?
        ''', (session.get('user_id'), datetime.now().strftime('%Y-%m-%d %H:%M:%S'), quote_id))
        conn.commit()
        flash('Quote acknowledged successfully', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error acknowledging quote: {str(e)}', 'danger')
    
    conn.close()
    return redirect(url_for(next_url))


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
    
    notes = conn.execute('''
        SELECT on_t.*, u.username as created_by_name
        FROM order_notes on_t
        JOIN users u ON on_t.created_by = u.id
        WHERE on_t.sales_order_id = ?
        ORDER BY on_t.is_pinned DESC, on_t.created_at DESC
    ''', (order_id,)).fetchall()
    
    activity_log = conn.execute('''
        SELECT oal.*, u.username as created_by_name
        FROM order_activity_log oal
        LEFT JOIN users u ON oal.created_by = u.id
        WHERE oal.sales_order_id = ?
        ORDER BY oal.created_at DESC
        LIMIT 20
    ''', (order_id,)).fetchall()
    
    escalations = conn.execute('''
        SELECT e.*, u1.username as escalated_by_name, u2.username as assigned_to_name
        FROM order_escalations e
        JOIN users u1 ON e.escalated_by = u1.id
        LEFT JOIN users u2 ON e.assigned_to = u2.id
        WHERE e.sales_order_id = ?
        ORDER BY e.escalated_at DESC
    ''', (order_id,)).fetchall()
    
    conn.close()
    
    return render_template('customer_service/order_detail.html',
                         order=order,
                         order_lines=order_lines,
                         linked_work_orders=linked_work_orders,
                         stages=stages,
                         notes=notes,
                         activity_log=activity_log,
                         escalations=escalations)


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


def log_order_activity(conn, so_id, activity_type, description, old_value=None, new_value=None, user_id=None):
    """Log an activity for an order"""
    conn.execute('''
        INSERT INTO order_activity_log (sales_order_id, activity_type, activity_description, old_value, new_value, created_by)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (so_id, activity_type, description, old_value, new_value, user_id or session.get('user_id')))


@customer_service_bp.route('/customer-service/communications')
@login_required
@role_required('Admin', 'Planner', 'Production Staff', 'Procurement')
def communications_list():
    """List all customer communications"""
    db = Database()
    conn = db.get_connection()
    
    customer_filter = request.args.get('customer', '')
    type_filter = request.args.get('type', '')
    pending_only = request.args.get('pending', '')
    
    query = '''
        SELECT cc.*, c.name as customer_name, so.so_number, u.username as created_by_name
        FROM customer_communications cc
        JOIN customers c ON cc.customer_id = c.id
        LEFT JOIN sales_orders so ON cc.sales_order_id = so.id
        JOIN users u ON cc.created_by = u.id
        WHERE 1=1
    '''
    params = []
    
    if customer_filter:
        query += ' AND cc.customer_id = ?'
        params.append(customer_filter)
    if type_filter:
        query += ' AND cc.communication_type = ?'
        params.append(type_filter)
    if pending_only == '1':
        query += ' AND cc.follow_up_required = 1 AND cc.follow_up_completed = 0'
    
    query += ' ORDER BY cc.communication_date DESC LIMIT 100'
    
    communications = conn.execute(query, params).fetchall()
    
    customers = conn.execute('SELECT id, name FROM customers ORDER BY name').fetchall()
    
    pending_follow_ups = conn.execute('''
        SELECT COUNT(*) FROM customer_communications 
        WHERE follow_up_required = 1 AND follow_up_completed = 0
    ''').fetchone()[0]
    
    conn.close()
    
    return render_template('customer_service/communications.html',
                         communications=communications,
                         customers=customers,
                         pending_follow_ups=pending_follow_ups,
                         filters={'customer': customer_filter, 'type': type_filter, 'pending': pending_only})


@customer_service_bp.route('/customer-service/communications/add', methods=['GET', 'POST'])
@login_required
@role_required('Admin', 'Planner', 'Production Staff', 'Procurement')
def add_communication():
    """Add a new customer communication"""
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        customer_id = request.form.get('customer_id')
        sales_order_id = request.form.get('sales_order_id') or None
        comm_type = request.form.get('communication_type')
        subject = request.form.get('subject')
        description = request.form.get('description')
        follow_up = 1 if request.form.get('follow_up_required') else 0
        follow_up_date = request.form.get('follow_up_date') or None
        outcome = request.form.get('outcome')
        
        try:
            conn.execute('''
                INSERT INTO customer_communications 
                (customer_id, sales_order_id, communication_type, subject, description, 
                 follow_up_required, follow_up_date, outcome, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (customer_id, sales_order_id, comm_type, subject, description,
                  follow_up, follow_up_date, outcome, session.get('user_id')))
            
            if sales_order_id:
                log_order_activity(conn, sales_order_id, 'Communication', 
                                 f'{comm_type}: {subject}', user_id=session.get('user_id'))
            
            conn.commit()
            flash('Communication logged successfully', 'success')
            return redirect(url_for('customer_service.communications_list'))
        except Exception as e:
            conn.rollback()
            flash(f'Error logging communication: {str(e)}', 'danger')
    
    customers = conn.execute('SELECT id, name FROM customers ORDER BY name').fetchall()
    sales_orders = conn.execute('''
        SELECT so.id, so.so_number, c.name as customer_name 
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        WHERE so.status NOT IN ('Completed', 'Shipped', 'Closed', 'Cancelled')
        ORDER BY so.so_number DESC
    ''').fetchall()
    
    conn.close()
    
    return render_template('customer_service/add_communication.html',
                         customers=customers, sales_orders=sales_orders)


@customer_service_bp.route('/customer-service/communications/<int:comm_id>/complete-followup', methods=['POST'])
@login_required
@role_required('Admin', 'Planner', 'Production Staff', 'Procurement')
def complete_followup(comm_id):
    """Mark a follow-up as completed"""
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('''
            UPDATE customer_communications 
            SET follow_up_completed = 1, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (comm_id,))
        conn.commit()
        flash('Follow-up marked as completed', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('customer_service.communications_list', pending='1'))


@customer_service_bp.route('/customer-service/orders/<int:order_id>/notes', methods=['POST'])
@login_required
@role_required('Admin', 'Planner', 'Production Staff', 'Procurement')
def add_order_note(order_id):
    """Add a quick note to an order"""
    db = Database()
    conn = db.get_connection()
    
    note_text = request.form.get('note_text')
    note_type = request.form.get('note_type', 'General')
    is_pinned = 1 if request.form.get('is_pinned') else 0
    
    try:
        conn.execute('''
            INSERT INTO order_notes (sales_order_id, note_type, note_text, is_pinned, created_by)
            VALUES (?, ?, ?, ?, ?)
        ''', (order_id, note_type, note_text, is_pinned, session.get('user_id')))
        
        log_order_activity(conn, order_id, 'Note Added', f'{note_type} note added', user_id=session.get('user_id'))
        
        conn.commit()
        flash('Note added successfully', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error adding note: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('customer_service.order_detail', order_id=order_id))


@customer_service_bp.route('/customer-service/orders/<int:order_id>/notes/<int:note_id>/delete', methods=['POST'])
@login_required
@role_required('Admin', 'Planner')
def delete_order_note(order_id, note_id):
    """Delete an order note"""
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('DELETE FROM order_notes WHERE id = ? AND sales_order_id = ?', (note_id, order_id))
        conn.commit()
        flash('Note deleted', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting note: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('customer_service.order_detail', order_id=order_id))


@customer_service_bp.route('/customer-service/analytics')
@login_required
@role_required('Admin', 'Planner')
def analytics():
    """Customer service analytics and reporting"""
    db = Database()
    conn = db.get_connection()
    
    total_communications = conn.execute('SELECT COUNT(*) FROM customer_communications').fetchone()[0]
    this_month_comms = conn.execute('''
        SELECT COUNT(*) FROM customer_communications 
        WHERE communication_date >= DATE('now', 'start of month')
    ''').fetchone()[0]
    
    pending_followups = conn.execute('''
        SELECT COUNT(*) FROM customer_communications 
        WHERE follow_up_required = 1 AND follow_up_completed = 0
    ''').fetchone()[0]
    
    overdue_followups = conn.execute('''
        SELECT COUNT(*) FROM customer_communications 
        WHERE follow_up_required = 1 AND follow_up_completed = 0 
          AND follow_up_date < DATE('now')
    ''').fetchone()[0]
    
    comms_by_type_rows = conn.execute('''
        SELECT communication_type, COUNT(*) as count
        FROM customer_communications
        GROUP BY communication_type
        ORDER BY count DESC
    ''').fetchall()
    comms_by_type = [{'type': r['communication_type'], 'count': r['count']} for r in comms_by_type_rows]
    
    comms_by_month_rows = conn.execute('''
        SELECT strftime('%Y-%m', communication_date) as month, COUNT(*) as count
        FROM customer_communications
        WHERE communication_date >= DATE('now', '-6 months')
        GROUP BY month
        ORDER BY month
    ''').fetchall()
    comms_by_month = [{'month': r['month'], 'count': r['count']} for r in comms_by_month_rows]
    
    top_customers_rows = conn.execute('''
        SELECT c.name, COUNT(cc.id) as comm_count
        FROM customer_communications cc
        JOIN customers c ON cc.customer_id = c.id
        GROUP BY cc.customer_id
        ORDER BY comm_count DESC
        LIMIT 10
    ''').fetchall()
    top_customers = [{'name': r['name'], 'count': r['comm_count']} for r in top_customers_rows]
    
    avg_orders_per_stage_rows = conn.execute('''
        SELECT stage_name, 
               AVG(CASE WHEN completed_at IS NOT NULL AND started_at IS NOT NULL 
                   THEN JULIANDAY(completed_at) - JULIANDAY(started_at) 
                   ELSE NULL END) as avg_days
        FROM order_stage_tracking
        WHERE stage_status = 'Complete'
        GROUP BY stage_name
        ORDER BY stage_order
    ''').fetchall()
    stage_durations = [{'stage': r['stage_name'], 'avg_days': round(r['avg_days'] or 0, 1)} for r in avg_orders_per_stage_rows]
    
    recent_notes = conn.execute('''
        SELECT on_t.*, so.so_number, u.username as created_by_name
        FROM order_notes on_t
        JOIN sales_orders so ON on_t.sales_order_id = so.id
        JOIN users u ON on_t.created_by = u.id
        ORDER BY on_t.created_at DESC
        LIMIT 10
    ''').fetchall()
    
    conn.close()
    
    return render_template('customer_service/analytics.html',
                         total_communications=total_communications,
                         this_month_comms=this_month_comms,
                         pending_followups=pending_followups,
                         overdue_followups=overdue_followups,
                         comms_by_type=comms_by_type,
                         comms_by_month=comms_by_month,
                         top_customers=top_customers,
                         stage_durations=stage_durations,
                         recent_notes=recent_notes)


# ==================== PHASE 4: ESCALATION MANAGEMENT ====================

@customer_service_bp.route('/customer-service/escalations')
@login_required
@role_required('Admin', 'Planner', 'Production Staff', 'Procurement')
def escalations_list():
    """List all escalations with filtering"""
    db = Database()
    conn = db.get_connection()
    
    status_filter = request.args.get('status', '')
    priority_filter = request.args.get('priority', '')
    
    query = '''
        SELECT e.*, so.so_number, c.name as customer_name,
               u1.username as escalated_by_name, u2.username as assigned_to_name,
               u3.username as resolved_by_name
        FROM order_escalations e
        JOIN sales_orders so ON e.sales_order_id = so.id
        JOIN customers c ON so.customer_id = c.id
        JOIN users u1 ON e.escalated_by = u1.id
        LEFT JOIN users u2 ON e.assigned_to = u2.id
        LEFT JOIN users u3 ON e.resolved_by = u3.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND e.status = ?'
        params.append(status_filter)
    if priority_filter:
        query += ' AND e.priority = ?'
        params.append(priority_filter)
    
    query += ' ORDER BY e.escalated_at DESC'
    
    escalations = conn.execute(query, params).fetchall()
    
    open_count = conn.execute("SELECT COUNT(*) FROM order_escalations WHERE status = 'Open'").fetchone()[0]
    in_progress_count = conn.execute("SELECT COUNT(*) FROM order_escalations WHERE status = 'In Progress'").fetchone()[0]
    resolved_today = conn.execute('''
        SELECT COUNT(*) FROM order_escalations 
        WHERE status = 'Resolved' AND DATE(resolved_at) = DATE('now')
    ''').fetchone()[0]
    
    users = conn.execute('SELECT id, username FROM users ORDER BY username').fetchall()
    
    conn.close()
    
    return render_template('customer_service/escalations.html',
                         escalations=escalations,
                         users=users,
                         open_count=open_count,
                         in_progress_count=in_progress_count,
                         resolved_today=resolved_today,
                         filters={'status': status_filter, 'priority': priority_filter})


@customer_service_bp.route('/customer-service/orders/<int:order_id>/escalate', methods=['GET', 'POST'])
@login_required
@role_required('Admin', 'Planner')
def escalate_order(order_id):
    """Create an escalation for an order"""
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
        flash('Order not found', 'danger')
        return redirect(url_for('customer_service.orders_list'))
    
    if request.method == 'POST':
        escalation_reason = request.form.get('escalation_reason')
        priority = request.form.get('priority', 'High')
        assigned_to = request.form.get('assigned_to') or None
        target_date = request.form.get('target_resolution_date') or None
        escalation_level = int(request.form.get('escalation_level', 1))
        
        try:
            conn.execute('''
                INSERT INTO order_escalations 
                (sales_order_id, escalation_level, escalation_reason, priority, 
                 assigned_to, escalated_by, target_resolution_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (order_id, escalation_level, escalation_reason, priority,
                  assigned_to, session.get('user_id'), target_date))
            
            log_order_activity(conn, order_id, 'Escalation', 
                             f'Order escalated: {priority} priority - {escalation_reason[:50]}...',
                             user_id=session.get('user_id'))
            
            conn.commit()
            flash(f'Escalation created for order {order["so_number"]}', 'success')
            conn.close()
            return redirect(url_for('customer_service.order_detail', order_id=order_id))
        except Exception as e:
            conn.rollback()
            flash(f'Error creating escalation: {str(e)}', 'danger')
    
    users = conn.execute('SELECT id, username FROM users ORDER BY username').fetchall()
    conn.close()
    
    return render_template('customer_service/escalate_order.html',
                         order=order,
                         users=users)


@customer_service_bp.route('/customer-service/escalations/<int:escalation_id>/update', methods=['POST'])
@login_required
@role_required('Admin', 'Planner')
def update_escalation(escalation_id):
    """Update escalation status or assignment"""
    db = Database()
    conn = db.get_connection()
    
    escalation = conn.execute('SELECT * FROM order_escalations WHERE id = ?', (escalation_id,)).fetchone()
    
    if not escalation:
        conn.close()
        flash('Escalation not found', 'danger')
        return redirect(url_for('customer_service.escalations_list'))
    
    new_status = request.form.get('status')
    assigned_to = request.form.get('assigned_to') or None
    resolution_notes = request.form.get('resolution_notes', '')
    
    try:
        if new_status == 'Resolved':
            conn.execute('''
                UPDATE order_escalations 
                SET status = ?, assigned_to = ?, resolution_notes = ?,
                    resolved_at = CURRENT_TIMESTAMP, resolved_by = ?
                WHERE id = ?
            ''', (new_status, assigned_to, resolution_notes, session.get('user_id'), escalation_id))
            
            log_order_activity(conn, escalation['sales_order_id'], 'Escalation Resolved', 
                             f'Escalation resolved: {resolution_notes[:50]}...' if resolution_notes else 'Escalation resolved',
                             user_id=session.get('user_id'))
        else:
            conn.execute('''
                UPDATE order_escalations 
                SET status = ?, assigned_to = ?
                WHERE id = ?
            ''', (new_status, assigned_to, escalation_id))
        
        conn.commit()
        flash('Escalation updated successfully', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error updating escalation: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('customer_service.escalations_list'))


# ==================== PHASE 4: SLA CONFIGURATION ====================

@customer_service_bp.route('/customer-service/sla')
@login_required
@role_required('Admin')
def sla_list():
    """List all SLA configurations"""
    db = Database()
    conn = db.get_connection()
    
    slas = conn.execute('''
        SELECT s.*, u.username as created_by_name
        FROM sla_configurations s
        LEFT JOIN users u ON s.created_by = u.id
        ORDER BY s.sla_name
    ''').fetchall()
    
    conn.close()
    
    return render_template('customer_service/sla_list.html', slas=slas)


@customer_service_bp.route('/customer-service/sla/add', methods=['GET', 'POST'])
@login_required
@role_required('Admin')
def add_sla():
    """Add a new SLA configuration"""
    if request.method == 'POST':
        db = Database()
        conn = db.get_connection()
        
        sla_name = request.form.get('sla_name')
        order_type = request.form.get('order_type') or None
        customer_tier = request.form.get('customer_tier') or None
        response_time = int(request.form.get('response_time_hours', 24))
        resolution_time = int(request.form.get('resolution_time_hours', 72))
        escalation_time = int(request.form.get('escalation_time_hours', 48))
        
        try:
            conn.execute('''
                INSERT INTO sla_configurations 
                (sla_name, order_type, customer_tier, response_time_hours, 
                 resolution_time_hours, escalation_time_hours, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (sla_name, order_type, customer_tier, response_time,
                  resolution_time, escalation_time, session.get('user_id')))
            
            conn.commit()
            flash(f'SLA "{sla_name}" created successfully', 'success')
            conn.close()
            return redirect(url_for('customer_service.sla_list'))
        except Exception as e:
            conn.rollback()
            flash(f'Error creating SLA: {str(e)}', 'danger')
            conn.close()
    
    return render_template('customer_service/add_sla.html')


@customer_service_bp.route('/customer-service/sla/<int:sla_id>/toggle', methods=['POST'])
@login_required
@role_required('Admin')
def toggle_sla(sla_id):
    """Toggle SLA active status"""
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('''
            UPDATE sla_configurations 
            SET is_active = CASE WHEN is_active = 1 THEN 0 ELSE 1 END
            WHERE id = ?
        ''', (sla_id,))
        conn.commit()
        flash('SLA status updated', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error updating SLA: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('customer_service.sla_list'))


@customer_service_bp.route('/customer-service/sla-breaches')
@login_required
@role_required('Admin', 'Planner', 'Production Staff', 'Procurement')
def sla_breaches():
    """View orders at risk of SLA breach"""
    db = Database()
    conn = db.get_connection()
    
    at_risk_orders = conn.execute('''
        SELECT so.*, c.name as customer_name,
               JULIANDAY(DATE('now')) - JULIANDAY(so.order_date) as hours_since_order,
               (JULIANDAY(DATE('now')) - JULIANDAY(so.order_date)) * 24 as total_hours,
               CASE 
                   WHEN (JULIANDAY(DATE('now')) - JULIANDAY(so.order_date)) * 24 > 72 THEN 'Critical'
                   WHEN (JULIANDAY(DATE('now')) - JULIANDAY(so.order_date)) * 24 > 48 THEN 'Warning'
                   ELSE 'Normal'
               END as sla_status
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        WHERE so.status NOT IN ('Completed', 'Shipped', 'Closed', 'Cancelled')
          AND (JULIANDAY(DATE('now')) - JULIANDAY(so.order_date)) * 24 > 24
        ORDER BY so.order_date ASC
    ''').fetchall()
    
    critical_count = len([o for o in at_risk_orders if o['sla_status'] == 'Critical'])
    warning_count = len([o for o in at_risk_orders if o['sla_status'] == 'Warning'])
    
    conn.close()
    
    return render_template('customer_service/sla_breaches.html',
                         at_risk_orders=at_risk_orders,
                         critical_count=critical_count,
                         warning_count=warning_count)


# ==================== PHASE 4: CUSTOMER FEEDBACK ====================

@customer_service_bp.route('/customer-service/feedback')
@login_required
@role_required('Admin', 'Planner', 'Production Staff', 'Procurement')
def feedback_list():
    """List all customer feedback"""
    db = Database()
    conn = db.get_connection()
    
    rating_filter = request.args.get('rating', '')
    
    query = '''
        SELECT f.*, c.name as customer_name, so.so_number, wo.wo_number
        FROM customer_feedback f
        JOIN customers c ON f.customer_id = c.id
        LEFT JOIN sales_orders so ON f.sales_order_id = so.id
        LEFT JOIN work_orders wo ON f.work_order_id = wo.id
        WHERE 1=1
    '''
    params = []
    
    if rating_filter:
        query += ' AND f.rating = ?'
        params.append(int(rating_filter))
    
    query += ' ORDER BY f.submitted_at DESC'
    
    feedback = conn.execute(query, params).fetchall()
    
    avg_rating = conn.execute('SELECT AVG(rating) FROM customer_feedback').fetchone()[0] or 0
    total_feedback = conn.execute('SELECT COUNT(*) FROM customer_feedback').fetchone()[0]
    recommend_rate = conn.execute('''
        SELECT COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM customer_feedback), 0) 
        FROM customer_feedback WHERE would_recommend = 1
    ''').fetchone()[0] or 0
    
    rating_distribution_rows = conn.execute('''
        SELECT rating, COUNT(*) as count
        FROM customer_feedback
        GROUP BY rating
        ORDER BY rating DESC
    ''').fetchall()
    rating_distribution = [{'rating': r['rating'], 'count': r['count']} for r in rating_distribution_rows]
    
    conn.close()
    
    return render_template('customer_service/feedback_list.html',
                         feedback=feedback,
                         avg_rating=round(avg_rating, 1),
                         total_feedback=total_feedback,
                         recommend_rate=round(recommend_rate, 1),
                         rating_distribution=rating_distribution,
                         rating_filter=rating_filter)


@customer_service_bp.route('/customer-service/feedback/add', methods=['GET', 'POST'])
@login_required
@role_required('Admin', 'Planner')
def add_feedback():
    """Record new customer feedback"""
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        customer_id = request.form.get('customer_id')
        sales_order_id = request.form.get('sales_order_id') or None
        work_order_id = request.form.get('work_order_id') or None
        rating = int(request.form.get('rating', 5))
        feedback_type = request.form.get('feedback_type', 'Order Completion')
        comments = request.form.get('comments', '')
        would_recommend = 1 if request.form.get('would_recommend') else 0
        follow_up = 1 if request.form.get('follow_up_required') else 0
        
        try:
            conn.execute('''
                INSERT INTO customer_feedback 
                (sales_order_id, work_order_id, customer_id, rating, feedback_type,
                 comments, would_recommend, follow_up_required)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (sales_order_id, work_order_id, customer_id, rating, feedback_type,
                  comments, would_recommend, follow_up))
            
            if sales_order_id:
                log_order_activity(conn, sales_order_id, 'Feedback Received', 
                                 f'Customer feedback: {rating}/5 stars',
                                 user_id=session.get('user_id'))
            
            conn.commit()
            flash('Customer feedback recorded', 'success')
            conn.close()
            return redirect(url_for('customer_service.feedback_list'))
        except Exception as e:
            conn.rollback()
            flash(f'Error recording feedback: {str(e)}', 'danger')
    
    customers = conn.execute('SELECT id, name FROM customers ORDER BY name').fetchall()
    recent_orders = conn.execute('''
        SELECT so.id, so.so_number, c.name as customer_name
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        WHERE so.status IN ('Completed', 'Shipped', 'Closed')
        ORDER BY so.order_date DESC
        LIMIT 50
    ''').fetchall()
    
    conn.close()
    
    return render_template('customer_service/add_feedback.html',
                         customers=customers,
                         recent_orders=recent_orders)
