from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import Database
from auth import login_required, role_required
from datetime import datetime, timedelta

invoice_bp = Blueprint('invoice_routes', __name__)

@invoice_bp.route('/invoices')
@login_required
@role_required('Admin', 'Accountant', 'Planner')
def list_invoices():
    """Display invoice dashboard with all invoices"""
    db = Database()
    conn = db.get_connection()
    
    # Get filter parameters
    status_filter = request.args.get('status', 'all')
    customer_filter = request.args.get('customer_id', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    
    # Build query with filters
    query = '''
        SELECT 
            i.*,
            c.name as customer_name,
            c.customer_number,
            so.so_number,
            wo.wo_number,
            u.username as created_by_name,
            COUNT(il.id) as line_count
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        LEFT JOIN sales_orders so ON i.so_id = so.id
        LEFT JOIN work_orders wo ON i.wo_id = wo.id
        LEFT JOIN users u ON i.created_by = u.id
        LEFT JOIN invoice_lines il ON i.id = il.invoice_id
        WHERE 1=1
    '''
    
    params = []
    if status_filter != 'all':
        query += ' AND i.status = ?'
        params.append(status_filter)
    if customer_filter:
        query += ' AND i.customer_id = ?'
        params.append(int(customer_filter))
    if date_from:
        query += ' AND i.invoice_date >= ?'
        params.append(date_from)
    if date_to:
        query += ' AND i.invoice_date <= ?'
        params.append(date_to)
    
    query += ' GROUP BY i.id ORDER BY i.invoice_date DESC, i.created_at DESC'
    
    invoices = conn.execute(query, params).fetchall()
    
    # Get customers for filter dropdown
    customers = conn.execute('''
        SELECT id, customer_number, name FROM customers ORDER BY customer_number
    ''').fetchall()
    
    # Calculate dashboard metrics
    total_invoiced = sum(inv['total_amount'] for inv in invoices)
    total_paid = sum(inv['amount_paid'] for inv in invoices)
    total_outstanding = sum(inv['balance_due'] for inv in invoices if inv['status'] != 'Void')
    
    # Overdue invoices
    today = datetime.now().strftime('%Y-%m-%d')
    overdue_amount = sum(
        inv['balance_due'] for inv in invoices 
        if inv['status'] in ['Posted', 'Approved'] and inv['due_date'] < today and inv['balance_due'] > 0
    )
    
    conn.close()
    
    return render_template('invoices/dashboard.html',
                         invoices=invoices,
                         customers=customers,
                         status_filter=status_filter,
                         customer_filter=customer_filter,
                         date_from=date_from,
                         date_to=date_to,
                         total_invoiced=total_invoiced,
                         total_paid=total_paid,
                         total_outstanding=total_outstanding,
                         overdue_amount=overdue_amount,
                         today=today)

@invoice_bp.route('/invoices/<int:id>')
@login_required
@role_required('Admin', 'Accountant', 'Planner')
def view_invoice(id):
    """View invoice details"""
    db = Database()
    conn = db.get_connection()
    
    invoice = conn.execute('''
        SELECT 
            i.*,
            c.name as customer_name,
            c.customer_number,
            c.email as customer_email,
            c.billing_address,
            c.shipping_address,
            so.so_number,
            wo.wo_number,
            u.username as created_by_name,
            u2.username as approved_by_name,
            u3.username as posted_by_name
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        LEFT JOIN sales_orders so ON i.so_id = so.id
        LEFT JOIN work_orders wo ON i.wo_id = wo.id
        LEFT JOIN users u ON i.created_by = u.id
        LEFT JOIN users u2 ON i.approved_by = u2.id
        LEFT JOIN users u3 ON i.posted_by = u3.id
        WHERE i.id = ?
    ''', (id,)).fetchone()
    
    if not invoice:
        flash('Invoice not found', 'danger')
        conn.close()
        return redirect(url_for('invoice_routes.list_invoices'))
    
    # Get line items
    lines = conn.execute('''
        SELECT il.*, p.code as product_code, p.name as product_name
        FROM invoice_lines il
        LEFT JOIN products p ON il.product_id = p.id
        WHERE il.invoice_id = ?
        ORDER BY il.line_number
    ''', (id,)).fetchall()
    
    # Get payments
    payments = conn.execute('''
        SELECT * FROM payments
        WHERE reference_type = 'Invoice' AND reference_id = ?
        ORDER BY payment_date DESC
    ''', (id,)).fetchall()
    
    conn.close()
    
    return render_template('invoices/view.html',
                         invoice=invoice,
                         lines=lines,
                         payments=payments)

@invoice_bp.route('/invoices/create-from-so/<int:so_id>', methods=['GET', 'POST'])
@login_required
@role_required('Admin', 'Accountant')
def create_from_sales_order(so_id):
    """Create invoice from Sales Order"""
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            # Get invoice details from form
            invoice_date = request.form.get('invoice_date') or datetime.now().strftime('%Y-%m-%d')
            payment_terms = int(request.form.get('payment_terms', 30))
            
            # Calculate due date
            inv_date = datetime.strptime(invoice_date, '%Y-%m-%d')
            due_date = (inv_date + timedelta(days=payment_terms)).strftime('%Y-%m-%d')
            
            # Get sales order
            so = conn.execute('SELECT * FROM sales_orders WHERE id = ?', (so_id,)).fetchone()
            
            if not so:
                flash('Sales Order not found', 'danger')
                conn.close()
                return redirect(url_for('invoice_routes.list_invoices'))
            
            # Generate invoice number
            last_inv = conn.execute('''
                SELECT invoice_number FROM invoices 
                ORDER BY id DESC LIMIT 1
            ''').fetchone()
            
            if last_inv:
                last_num = int(last_inv['invoice_number'].split('-')[1])
                invoice_number = f'INV-{last_num + 1:06d}'
            else:
                invoice_number = 'INV-000001'
            
            # Create invoice
            cursor = conn.execute('''
                INSERT INTO invoices (
                    invoice_number, invoice_type, customer_id, so_id,
                    invoice_date, due_date, payment_terms, status,
                    subtotal, tax_rate, tax_amount, discount_amount, total_amount,
                    balance_due, notes, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                invoice_number, 'Sales Order', so['customer_id'], so_id,
                invoice_date, due_date, payment_terms, 'Draft',
                so['subtotal'], 0, so['tax_amount'], so['discount_amount'], so['total_amount'],
                so['total_amount'], request.form.get('notes', ''), session['user_id']
            ))
            
            invoice_id = cursor.lastrowid
            
            # Copy line items from sales order
            so_lines = conn.execute('''
                SELECT * FROM sales_order_lines WHERE so_id = ?
                ORDER BY line_number
            ''', (so_id,)).fetchall()
            
            for line in so_lines:
                conn.execute('''
                    INSERT INTO invoice_lines (
                        invoice_id, line_number, product_id, description,
                        quantity, unit_price, discount_percent, line_total,
                        reference_type, reference_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    invoice_id, line['line_number'], line['product_id'], line['description'],
                    line['quantity'], line['unit_price'], line['discount_percent'], line['line_total'],
                    'sales_order_line', line['id']
                ))
            
            conn.commit()
            flash(f'Invoice {invoice_number} created successfully!', 'success')
            return redirect(url_for('invoice_routes.view_invoice', id=invoice_id))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error creating invoice: {str(e)}', 'danger')
        finally:
            conn.close()
        
        return redirect(url_for('salesorder_routes.view_sales_order', id=so_id))
    
    # GET - show form
    so = conn.execute('''
        SELECT so.*, c.name as customer_name, c.customer_number, c.payment_terms
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        WHERE so.id = ?
    ''', (so_id,)).fetchone()
    
    if not so:
        flash('Sales Order not found', 'danger')
        conn.close()
        return redirect(url_for('invoice_routes.list_invoices'))
    
    # Get line items
    lines = conn.execute('''
        SELECT sol.*, p.code, p.name as product_name
        FROM sales_order_lines sol
        JOIN products p ON sol.product_id = p.id
        WHERE sol.so_id = ?
        ORDER BY sol.line_number
    ''', (so_id,)).fetchall()
    
    conn.close()
    
    return render_template('invoices/create_from_so.html',
                         sales_order=so,
                         lines=lines,
                         today=datetime.now().strftime('%Y-%m-%d'))
