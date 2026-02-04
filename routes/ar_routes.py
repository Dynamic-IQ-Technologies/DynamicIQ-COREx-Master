from flask import Blueprint, render_template, request, redirect, url_for, flash, session, make_response
from models import Database, AuditLogger
from auth import login_required, role_required
from datetime import datetime, timedelta
from utils.gl_journal import create_payment_received_entry
import csv
import io

ar_bp = Blueprint('ar_routes', __name__)

@ar_bp.route('/accounts-receivable')
@login_required
@role_required('Admin', 'Accountant', 'Sales')
def list_ar():
    """List all Accounts Receivable records"""
    db = Database()
    conn = db.get_connection()
    
    status_filter = request.args.get('status', 'all')
    customer_filter = request.args.get('customer_id', '')
    
    query = '''
        SELECT 
            i.*,
            c.name as customer_name,
            c.customer_number as customer_code,
            (i.total_amount - COALESCE(i.amount_paid, 0)) as balance_due
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        WHERE 1=1
    '''
    
    params = []
    if status_filter != 'all':
        query += ' AND i.status = ?'
        params.append(status_filter)
    
    if customer_filter:
        query += ' AND i.customer_id = ?'
        params.append(customer_filter)
    
    query += ' ORDER BY i.due_date ASC, i.created_at DESC'
    
    receivables = conn.execute(query, params).fetchall()
    
    today_date = datetime.now().strftime('%Y-%m-%d')
    total_open = sum(float(r['balance_due'] or 0) for r in receivables if r['status'] not in ['Paid', 'Cancelled'])
    total_overdue = sum(
        float(r['balance_due'] or 0) for r in receivables 
        if r['status'] not in ['Paid', 'Cancelled'] and r['due_date'] and r['due_date'] < today_date
    )
    
    customers = conn.execute('SELECT id, name FROM customers ORDER BY name').fetchall()
    
    aging_query = '''
        SELECT 
            CASE 
                WHEN due_date >= date('now') THEN 'Current'
                WHEN julianday('now') - julianday(due_date) BETWEEN 1 AND 30 THEN '1-30 Days'
                WHEN julianday('now') - julianday(due_date) BETWEEN 31 AND 60 THEN '31-60 Days'
                WHEN julianday('now') - julianday(due_date) BETWEEN 61 AND 90 THEN '61-90 Days'
                ELSE '90+ Days'
            END as aging_bucket,
            COUNT(*) as invoice_count,
            COALESCE(SUM(total_amount - COALESCE(amount_paid, 0)), 0) as amount
        FROM invoices
        WHERE status NOT IN ('Paid', 'Cancelled')
        GROUP BY 1
        ORDER BY 
            CASE aging_bucket 
                WHEN 'Current' THEN 1
                WHEN '1-30 Days' THEN 2
                WHEN '31-60 Days' THEN 3
                WHEN '61-90 Days' THEN 4
                ELSE 5
            END
    '''
    aging_data = conn.execute(aging_query).fetchall()
    
    conn.close()
    
    return render_template('ar/list.html', 
                         receivables=receivables,
                         status_filter=status_filter,
                         customer_filter=customer_filter,
                         customers=customers,
                         total_open=total_open,
                         total_overdue=total_overdue,
                         aging_data=aging_data,
                         today_date=today_date)

@ar_bp.route('/accounts-receivable/<int:id>')
@login_required
@role_required('Admin', 'Accountant', 'Sales')
def view_ar(id):
    """View A/R invoice details"""
    db = Database()
    conn = db.get_connection()
    
    invoice = conn.execute('''
        SELECT 
            i.*,
            c.name as customer_name,
            c.customer_number as customer_code,
            c.email as customer_email,
            c.phone as customer_phone,
            c.billing_address as customer_address,
            (i.total_amount - COALESCE(i.amount_paid, 0)) as balance_due
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        WHERE i.id = ?
    ''', (id,)).fetchone()
    
    if not invoice:
        flash('Invoice not found.', 'danger')
        return redirect(url_for('ar_routes.list_ar'))
    
    lines = conn.execute('''
        SELECT il.*, p.name as product_name, p.code as part_number
        FROM invoice_lines il
        LEFT JOIN products p ON il.product_id = p.id
        WHERE il.invoice_id = ?
        ORDER BY il.id
    ''', (id,)).fetchall()
    
    payments = conn.execute('''
        SELECT * FROM payments
        WHERE reference_type = 'invoice' AND reference_id = ?
        ORDER BY payment_date DESC
    ''', (id,)).fetchall()
    
    conn.close()
    
    return render_template('ar/view.html',
                         invoice=invoice,
                         lines=lines,
                         payments=payments)

@ar_bp.route('/accounts-receivable/<int:id>/record-payment', methods=['GET', 'POST'])
@login_required
@role_required('Admin', 'Accountant')
def record_payment(id):
    """Record a payment against an invoice"""
    db = Database()
    conn = db.get_connection()
    
    # Ensure clean transaction state (PostgreSQL requires this after any prior error)
    try:
        conn.rollback()
    except:
        pass
    
    invoice = conn.execute('''
        SELECT 
            i.*,
            c.name as customer_name,
            (i.total_amount - COALESCE(i.amount_paid, 0)) as balance_due
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        WHERE i.id = ?
    ''', (id,)).fetchone()
    
    if not invoice:
        flash('Invoice not found.', 'danger')
        return redirect(url_for('ar_routes.list_ar'))
    
    if request.method == 'POST':
        payment_amount = float(request.form.get('amount', 0))
        payment_date = request.form.get('payment_date', datetime.now().strftime('%Y-%m-%d'))
        payment_method = request.form.get('payment_method', 'Check')
        reference_number = request.form.get('reference_number', '')
        notes = request.form.get('notes', '')
        
        if payment_amount <= 0:
            flash('Payment amount must be greater than zero.', 'danger')
            return render_template('ar/record_payment.html', invoice=invoice)
        
        if payment_amount > float(invoice['balance_due']):
            flash('Payment amount cannot exceed balance due.', 'danger')
            return render_template('ar/record_payment.html', invoice=invoice)
        
        try:
            payment_number = f"PMT-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            cursor = conn.execute('''
                INSERT INTO payments (payment_number, payment_date, payment_type, reference_type, reference_id, amount, payment_method, check_number, remarks, created_by, created_at)
                VALUES (?, ?, 'Receipt', 'invoice', ?, ?, ?, ?, ?, ?, ?)
            ''', (payment_number, payment_date, id, payment_amount, payment_method, reference_number, notes, session.get('user_id'), datetime.now()))
            
            payment_id = cursor.lastrowid
            
            gl_entry_id = create_payment_received_entry(
                conn=conn,
                payment_id=payment_id,
                payment_number=payment_number,
                payment_date=payment_date,
                amount=payment_amount,
                invoice_number=invoice['invoice_number'],
                user_id=session.get('user_id')
            )
            
            if gl_entry_id:
                conn.execute('UPDATE payments SET gl_entry_id = ? WHERE id = ?', (gl_entry_id, payment_id))
            
            new_amount_paid = float(invoice['amount_paid'] or 0) + payment_amount
            new_balance = float(invoice['total_amount']) - new_amount_paid
            new_status = 'Paid' if new_balance <= 0.01 else invoice['status']
            
            conn.execute('''
                UPDATE invoices
                SET amount_paid = ?, balance_due = ?, status = ?
                WHERE id = ?
            ''', (new_amount_paid, new_balance, new_status, id))
            
            conn.commit()
            
            AuditLogger.log(
                conn,
                'ar_invoice',
                id,
                'UPDATE',
                session.get('user_id'),
                {'payment_amount': payment_amount, 'invoice': invoice['invoice_number'], 'action': 'payment_recorded'}
            )
            
            flash(f'Payment of ${payment_amount:,.2f} recorded successfully.', 'success')
            return redirect(url_for('ar_routes.view_ar', id=id))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error recording payment: {str(e)}', 'danger')
    
    conn.close()
    return render_template('ar/record_payment.html', invoice=invoice)

@ar_bp.route('/accounts-receivable/export')
@login_required
@role_required('Admin', 'Accountant')
def export_ar():
    """Export A/R aging report to CSV"""
    db = Database()
    conn = db.get_connection()
    
    receivables = conn.execute('''
        SELECT 
            i.invoice_number,
            c.name as customer_name,
            i.invoice_date,
            i.due_date,
            i.total_amount,
            i.amount_paid,
            (i.total_amount - COALESCE(i.amount_paid, 0)) as balance_due,
            i.status,
            CASE 
                WHEN i.due_date >= date('now') THEN 'Current'
                WHEN julianday('now') - julianday(i.due_date) BETWEEN 1 AND 30 THEN '1-30 Days'
                WHEN julianday('now') - julianday(i.due_date) BETWEEN 31 AND 60 THEN '31-60 Days'
                WHEN julianday('now') - julianday(i.due_date) BETWEEN 61 AND 90 THEN '61-90 Days'
                ELSE '90+ Days'
            END as aging
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        WHERE i.status NOT IN ('Paid', 'Cancelled')
        ORDER BY i.due_date ASC
    ''').fetchall()
    
    conn.close()
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    writer.writerow(['A/R Aging Report'])
    writer.writerow(['Generated:', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
    writer.writerow([])
    writer.writerow(['Invoice #', 'Customer', 'Invoice Date', 'Due Date', 'Total Amount', 'Paid', 'Balance Due', 'Status', 'Aging'])
    
    for r in receivables:
        writer.writerow([
            r['invoice_number'],
            r['customer_name'],
            r['invoice_date'],
            r['due_date'],
            f"${r['total_amount']:,.2f}",
            f"${r['amount_paid'] or 0:,.2f}",
            f"${r['balance_due']:,.2f}",
            r['status'],
            r['aging']
        ])
    
    response = make_response(output.getvalue())
    response.headers['Content-Disposition'] = f'attachment; filename=ar_aging_{datetime.now().strftime("%Y%m%d")}.csv'
    response.headers['Content-Type'] = 'text/csv'
    
    return response
