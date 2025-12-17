from flask import Blueprint, render_template, request, flash, redirect, url_for
from models import Database
import secrets

portal_bp = Blueprint('portal', __name__)


@portal_bp.route('/portal/<token>')
def customer_portal(token):
    """Public customer portal - no login required"""
    db = Database()
    conn = db.get_connection()
    
    customer = conn.execute('''
        SELECT * FROM customers 
        WHERE portal_token = ? AND portal_enabled = 1
    ''', (token,)).fetchone()
    
    if not customer:
        conn.close()
        return render_template('portal/invalid.html'), 404
    
    # Sales Orders
    sales_orders = conn.execute('''
        SELECT so.*, 
               (SELECT COUNT(*) FROM sales_order_lines WHERE so_id = so.id) as line_count,
               (SELECT COUNT(*) FROM work_orders WHERE so_id = so.id) as wo_count
        FROM sales_orders so
        WHERE so.customer_id = ?
        ORDER BY so.order_date DESC
    ''', (customer['id'],)).fetchall()
    
    # Work Orders linked to this customer
    work_orders = conn.execute('''
        SELECT wo.*, p.code as product_code, p.name as product_name,
               so.so_number
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN sales_orders so ON wo.so_id = so.id
        WHERE wo.customer_id = ?
        ORDER BY wo.created_at DESC
    ''', (customer['id'],)).fetchall()
    
    # Invoices for this customer
    invoices = conn.execute('''
        SELECT i.*, so.so_number
        FROM invoices i
        LEFT JOIN sales_orders so ON i.so_id = so.id
        WHERE i.customer_id = ?
        ORDER BY i.invoice_date DESC
    ''', (customer['id'],)).fetchall()
    
    # Quotes (sales orders with Quote status)
    quotes = conn.execute('''
        SELECT so.*, 
               (SELECT COUNT(*) FROM sales_order_lines WHERE so_id = so.id) as line_count
        FROM sales_orders so
        WHERE so.customer_id = ? AND so.status IN ('Draft', 'Quoted', 'Pending Approval')
        ORDER BY so.order_date DESC
    ''', (customer['id'],)).fetchall()
    
    stats = {
        'sales_orders': len(sales_orders),
        'active_orders': len([o for o in sales_orders if o['status'] not in ('Completed', 'Shipped', 'Closed', 'Cancelled')]),
        'work_orders': len(work_orders),
        'active_work_orders': len([wo for wo in work_orders if wo['status'] not in ('Completed', 'Closed', 'Cancelled')]),
        'invoices': len(invoices),
        'pending_invoices': len([i for i in invoices if i['status'] in ('Draft', 'Sent', 'Overdue')]),
        'quotes': len(quotes)
    }
    
    conn.close()
    
    return render_template('portal/dashboard.html',
                         customer=customer,
                         sales_orders=sales_orders,
                         work_orders=work_orders,
                         invoices=invoices,
                         quotes=quotes,
                         stats=stats,
                         token=token)


@portal_bp.route('/portal/<token>/order/<int:order_id>')
def portal_order_detail(token, order_id):
    """View order details in customer portal"""
    db = Database()
    conn = db.get_connection()
    
    customer = conn.execute('''
        SELECT * FROM customers 
        WHERE portal_token = ? AND portal_enabled = 1
    ''', (token,)).fetchone()
    
    if not customer:
        conn.close()
        return render_template('portal/invalid.html'), 404
    
    order = conn.execute('''
        SELECT so.*
        FROM sales_orders so
        WHERE so.id = ? AND so.customer_id = ?
    ''', (order_id, customer['id'])).fetchone()
    
    if not order:
        conn.close()
        flash('Order not found', 'danger')
        return redirect(url_for('portal.customer_portal', token=token))
    
    order_lines = conn.execute('''
        SELECT sol.*, p.code as product_code, p.name as product_name
        FROM sales_order_lines sol
        JOIN products p ON sol.product_id = p.id
        WHERE sol.so_id = ?
        ORDER BY sol.line_number
    ''', (order_id,)).fetchall()
    
    stages = conn.execute('''
        SELECT * FROM order_stage_tracking 
        WHERE sales_order_id = ?
        ORDER BY stage_order
    ''', (order_id,)).fetchall()
    
    work_orders = conn.execute('''
        SELECT wo.wo_number, wo.status, wo.planned_start_date, wo.planned_end_date,
               p.name as product_name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.so_id = ?
        ORDER BY wo.created_at DESC
    ''', (order_id,)).fetchall()
    
    conn.close()
    
    return render_template('portal/order_detail.html',
                         customer=customer,
                         order=order,
                         order_lines=order_lines,
                         stages=stages,
                         work_orders=work_orders,
                         token=token)


def generate_portal_token():
    """Generate a secure random portal token"""
    return secrets.token_urlsafe(32)
