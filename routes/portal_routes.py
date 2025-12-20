from flask import Blueprint, render_template, request, flash, redirect, url_for
from models import Database, AuditLogger
from datetime import datetime
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
               so.so_number, wos.name as stage_name, wos.color as stage_color
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN sales_orders so ON wo.so_id = so.id
        LEFT JOIN work_order_stages wos ON wo.stage_id = wos.id
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
    
    # Sales Order Quotes (sales orders with Quote status - excluding drafts)
    so_quotes = conn.execute('''
        SELECT so.*, 
               (SELECT COUNT(*) FROM sales_order_lines WHERE so_id = so.id) as line_count
        FROM sales_orders so
        WHERE so.customer_id = ? AND so.status IN ('Quoted', 'Pending Approval')
        ORDER BY so.order_date DESC
    ''', (customer['id'],)).fetchall()
    
    # Work Order Quotes - linked via work_orders.customer_id OR quote's customer_account
    wo_quotes = conn.execute('''
        SELECT q.*, wo.wo_number, wo.id as work_order_id, p.code as product_code, p.name as product_name,
               (SELECT COUNT(*) FROM work_order_quote_lines WHERE quote_id = q.id) as line_count
        FROM work_order_quotes q
        JOIN work_orders wo ON q.work_order_id = wo.id
        JOIN products p ON wo.product_id = p.id
        WHERE (wo.customer_id = ? OR q.customer_account = ?) 
          AND q.status IN ('Pending Approval', 'Sent', 'Quoted', 'Submitted')
        ORDER BY q.created_at DESC
    ''', (customer['id'], customer['customer_number'])).fetchall()
    
    stats = {
        'sales_orders': len(sales_orders),
        'active_orders': len([o for o in sales_orders if o['status'] not in ('Completed', 'Shipped', 'Closed', 'Cancelled')]),
        'work_orders': len(work_orders),
        'active_work_orders': len([wo for wo in work_orders if wo['status'] not in ('Completed', 'Closed', 'Cancelled')]),
        'invoices': len(invoices),
        'pending_invoices': len([i for i in invoices if i['status'] in ('Draft', 'Sent', 'Overdue')]),
        'quotes': len(so_quotes) + len(wo_quotes),
        'wo_quotes': len(wo_quotes)
    }
    
    conn.close()
    
    return render_template('portal/dashboard.html',
                         customer=customer,
                         sales_orders=sales_orders,
                         work_orders=work_orders,
                         invoices=invoices,
                         quotes=so_quotes,
                         wo_quotes=wo_quotes,
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


@portal_bp.route('/portal/<token>/quote/<int:quote_id>')
def portal_quote_detail(token, quote_id):
    """View work order quote details in customer portal"""
    db = Database()
    conn = db.get_connection()
    
    customer = conn.execute('''
        SELECT * FROM customers 
        WHERE portal_token = ? AND portal_enabled = 1
    ''', (token,)).fetchone()
    
    if not customer:
        conn.close()
        return render_template('portal/invalid.html'), 404
    
    quote = conn.execute('''
        SELECT q.*, wo.wo_number, wo.id as work_order_id, 
               p.code as product_code, p.name as product_name,
               wo.quantity as wo_quantity, p.description as product_description,
               prep.username as prepared_by_name,
               appr.username as approved_by_name
        FROM work_order_quotes q
        JOIN work_orders wo ON q.work_order_id = wo.id
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN users prep ON q.prepared_by = prep.id
        LEFT JOIN users appr ON q.approved_by = appr.id
        WHERE q.id = ? AND wo.customer_id = ?
    ''', (quote_id, customer['id'])).fetchone()
    
    if not quote:
        conn.close()
        flash('Quote not found', 'danger')
        return redirect(url_for('portal.customer_portal', token=token))
    
    quote_lines = conn.execute('''
        SELECT ql.*, p.code as product_code, p.name as product_name
        FROM work_order_quote_lines ql
        LEFT JOIN products p ON ql.product_id = p.id
        WHERE ql.quote_id = ?
        ORDER BY ql.sequence_number, ql.id
    ''', (quote_id,)).fetchall()
    
    conn.close()
    
    return render_template('portal/quote_detail.html',
                         customer=customer,
                         quote=quote,
                         quote_lines=quote_lines,
                         token=token)


@portal_bp.route('/portal/<token>/quote/<int:quote_id>/approve', methods=['POST'])
def portal_approve_quote(token, quote_id):
    """Approve a work order quote via customer portal"""
    db = Database()
    conn = db.get_connection()
    
    customer = conn.execute('''
        SELECT * FROM customers 
        WHERE portal_token = ? AND portal_enabled = 1
    ''', (token,)).fetchone()
    
    if not customer:
        conn.close()
        return render_template('portal/invalid.html'), 404
    
    quote = conn.execute('''
        SELECT q.*, wo.wo_number, wo.customer_id
        FROM work_order_quotes q
        JOIN work_orders wo ON q.work_order_id = wo.id
        WHERE q.id = ? AND wo.customer_id = ?
    ''', (quote_id, customer['id'])).fetchone()
    
    if not quote:
        conn.close()
        flash('Quote not found', 'danger')
        return redirect(url_for('portal.customer_portal', token=token))
    
    if quote['status'] not in ('Pending Approval', 'Sent', 'Submitted', 'Quoted'):
        conn.close()
        flash('This quote cannot be approved in its current status.', 'warning')
        return redirect(url_for('portal.portal_quote_detail', token=token, quote_id=quote_id))
    
    approver_name = request.form.get('approver_name', customer['name'])
    approver_title = request.form.get('approver_title', '')
    approval_notes = request.form.get('approval_notes', '')
    
    conn.execute('''
        UPDATE work_order_quotes 
        SET status = 'Approved', 
            customer_approved_at = CURRENT_TIMESTAMP,
            customer_approved_by = ?,
            customer_approver_title = ?,
            customer_approval_notes = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (approver_name, approver_title, approval_notes, quote_id))
    
    AuditLogger.log_change(
        conn=conn,
        record_type='work_order_quote',
        record_id=quote_id,
        action_type='Customer Approved',
        modified_by=None,
        changed_fields={
            'status': 'Approved',
            'customer_approved_by': approver_name,
            'approved_via': 'Customer Portal'
        },
        ip_address=request.remote_addr,
        user_agent=request.headers.get('User-Agent')
    )
    
    conn.commit()
    conn.close()
    
    flash('Quote approved successfully. Thank you for your approval.', 'success')
    return redirect(url_for('portal.portal_quote_detail', token=token, quote_id=quote_id))


@portal_bp.route('/portal/<token>/quote/<int:quote_id>/decline', methods=['POST'])
def portal_decline_quote(token, quote_id):
    """Decline a work order quote via customer portal"""
    db = Database()
    conn = db.get_connection()
    
    customer = conn.execute('''
        SELECT * FROM customers 
        WHERE portal_token = ? AND portal_enabled = 1
    ''', (token,)).fetchone()
    
    if not customer:
        conn.close()
        return render_template('portal/invalid.html'), 404
    
    quote = conn.execute('''
        SELECT q.*, wo.wo_number, wo.customer_id
        FROM work_order_quotes q
        JOIN work_orders wo ON q.work_order_id = wo.id
        WHERE q.id = ? AND wo.customer_id = ?
    ''', (quote_id, customer['id'])).fetchone()
    
    if not quote:
        conn.close()
        flash('Quote not found', 'danger')
        return redirect(url_for('portal.customer_portal', token=token))
    
    if quote['status'] not in ('Pending Approval', 'Sent', 'Submitted', 'Quoted'):
        conn.close()
        flash('This quote cannot be declined in its current status.', 'warning')
        return redirect(url_for('portal.portal_quote_detail', token=token, quote_id=quote_id))
    
    decline_reason = request.form.get('decline_reason', '')
    
    conn.execute('''
        UPDATE work_order_quotes 
        SET status = 'Declined', 
            customer_declined_at = CURRENT_TIMESTAMP,
            customer_decline_reason = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (decline_reason, quote_id))
    
    AuditLogger.log_change(
        conn=conn,
        record_type='work_order_quote',
        record_id=quote_id,
        action_type='Customer Declined',
        modified_by=None,
        changed_fields={
            'status': 'Declined',
            'decline_reason': decline_reason,
            'declined_via': 'Customer Portal'
        },
        ip_address=request.remote_addr,
        user_agent=request.headers.get('User-Agent')
    )
    
    conn.commit()
    conn.close()
    
    flash('Quote declined. We will follow up with you shortly.', 'info')
    return redirect(url_for('portal.portal_quote_detail', token=token, quote_id=quote_id))


def generate_portal_token():
    """Generate a secure random portal token"""
    return secrets.token_urlsafe(32)
