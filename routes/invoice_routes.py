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

@invoice_bp.route('/invoices/<int:id>/approve', methods=['POST'])
@login_required
@role_required('Admin', 'Accountant')
def approve_invoice(id):
    """Approve an invoice and optionally auto-post to GL based on preferences"""
    db = Database()
    conn = db.get_connection()
    
    try:
        invoice = conn.execute('SELECT * FROM invoices WHERE id = ?', (id,)).fetchone()
        
        if not invoice:
            flash('Invoice not found', 'danger')
            return redirect(url_for('invoice_routes.list_invoices'))
        
        if invoice['status'] != 'Draft':
            flash('Only Draft invoices can be approved', 'warning')
            return redirect(url_for('invoice_routes.view_invoice', id=id))
        
        # Get accounting preferences
        settings = conn.execute('SELECT auto_post_invoice_gl FROM company_settings WHERE id = 1').fetchone()
        auto_post_gl = settings['auto_post_invoice_gl'] if settings else 0
        
        # Update invoice status to Approved
        conn.execute('''
            UPDATE invoices 
            SET status = 'Approved', 
                approved_by = ?, 
                approved_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (session['user_id'], id))
        
        # If auto-posting is enabled, post to GL immediately
        if auto_post_gl:
            try:
                # Get Accounts Receivable and Sales Revenue account IDs
                ar_account = conn.execute(
                    "SELECT id FROM chart_of_accounts WHERE account_code = '1120'"
                ).fetchone()
                
                revenue_account = conn.execute(
                    "SELECT id FROM chart_of_accounts WHERE account_code = '4100'"
                ).fetchone()
                
                if ar_account and revenue_account:
                    # Generate GL entry number
                    last_entry = conn.execute(
                        'SELECT entry_number FROM gl_entries ORDER BY id DESC LIMIT 1'
                    ).fetchone()
                    
                    if last_entry:
                        last_num = int(last_entry['entry_number'].split('-')[1])
                        entry_number = f'JE-{last_num + 1:06d}'
                    else:
                        entry_number = 'JE-000001'
                    
                    # Create GL Entry
                    cursor = conn.execute('''
                        INSERT INTO gl_entries (
                            entry_number, entry_date, description, 
                            transaction_source, created_by, status
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        entry_number,
                        invoice['invoice_date'],
                        f"Revenue Recognition - Invoice {invoice['invoice_number']}",
                        'Invoice',
                        session['user_id'],
                        'Posted'
                    ))
                    
                    gl_entry_id = cursor.lastrowid
                    
                    # Debit Accounts Receivable
                    conn.execute('''
                        INSERT INTO gl_entry_lines (gl_entry_id, account_id, debit, credit)
                        VALUES (?, ?, ?, 0)
                    ''', (gl_entry_id, ar_account['id'], invoice['total_amount']))
                    
                    # Credit Sales Revenue
                    conn.execute('''
                        INSERT INTO gl_entry_lines (gl_entry_id, account_id, debit, credit)
                        VALUES (?, ?, 0, ?)
                    ''', (gl_entry_id, revenue_account['id'], invoice['total_amount']))
                    
                    # Update invoice with GL entry reference and status
                    conn.execute('''
                        UPDATE invoices 
                        SET status = 'Posted', 
                            gl_entry_id = ?,
                            posted_by = ?, 
                            posted_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (gl_entry_id, session['user_id'], id))
                    
                    # Update balance due
                    conn.execute('''
                        UPDATE invoices SET balance_due = total_amount - amount_paid WHERE id = ?
                    ''', (id,))
                    
                    conn.commit()
                    flash(f'Invoice {invoice["invoice_number"]} approved and automatically posted to GL! (Entry: {entry_number})', 'success')
                else:
                    # Accounts not found, just approve without posting
                    conn.commit()
                    flash(f'Invoice {invoice["invoice_number"]} approved successfully! (GL accounts not found - manual posting required)', 'warning')
            except Exception as gl_error:
                # If GL posting fails, invoice remains approved but not posted
                conn.commit()
                flash(f'Invoice {invoice["invoice_number"]} approved, but GL posting failed: {str(gl_error)}. Please post manually.', 'warning')
        else:
            conn.commit()
            flash(f'Invoice {invoice["invoice_number"]} approved successfully!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error approving invoice: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('invoice_routes.view_invoice', id=id))

@invoice_bp.route('/invoices/<int:id>/post', methods=['POST'])
@login_required
@role_required('Admin', 'Accountant')
def post_invoice(id):
    """Post an invoice to GL - Records Revenue"""
    db = Database()
    conn = db.get_connection()
    
    try:
        invoice = conn.execute('SELECT * FROM invoices WHERE id = ?', (id,)).fetchone()
        
        if not invoice:
            flash('Invoice not found', 'danger')
            return redirect(url_for('invoice_routes.list_invoices'))
        
        if invoice['status'] != 'Approved':
            flash('Only Approved invoices can be posted to GL', 'warning')
            return redirect(url_for('invoice_routes.view_invoice', id=id))
        
        if invoice['gl_entry_id']:
            flash('Invoice already posted to GL', 'warning')
            return redirect(url_for('invoice_routes.view_invoice', id=id))
        
        # Get Accounts Receivable and Sales Revenue account IDs
        ar_account = conn.execute(
            "SELECT id FROM chart_of_accounts WHERE account_code = '1120'"
        ).fetchone()
        
        revenue_account = conn.execute(
            "SELECT id FROM chart_of_accounts WHERE account_code = '4100'"
        ).fetchone()
        
        if not ar_account or not revenue_account:
            flash('Required GL accounts not found. Please contact administrator.', 'danger')
            return redirect(url_for('invoice_routes.view_invoice', id=id))
        
        # Generate GL entry number
        last_entry = conn.execute(
            'SELECT entry_number FROM gl_entries ORDER BY id DESC LIMIT 1'
        ).fetchone()
        
        if last_entry:
            last_num = int(last_entry['entry_number'].split('-')[1])
            entry_number = f'JE-{last_num + 1:06d}'
        else:
            entry_number = 'JE-000001'
        
        # Create GL Entry
        cursor = conn.execute('''
            INSERT INTO gl_entries (
                entry_number, entry_date, description, 
                transaction_source, created_by, status
            ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            entry_number,
            invoice['invoice_date'],
            f"Revenue Recognition - Invoice {invoice['invoice_number']}",
            'Invoice',
            session['user_id'],
            'Posted'
        ))
        
        gl_entry_id = cursor.lastrowid
        
        # Debit Accounts Receivable
        conn.execute('''
            INSERT INTO gl_entry_lines (gl_entry_id, account_id, debit, credit)
            VALUES (?, ?, ?, 0)
        ''', (gl_entry_id, ar_account['id'], invoice['total_amount']))
        
        # Credit Sales Revenue
        conn.execute('''
            INSERT INTO gl_entry_lines (gl_entry_id, account_id, debit, credit)
            VALUES (?, ?, 0, ?)
        ''', (gl_entry_id, revenue_account['id'], invoice['total_amount']))
        
        # Update invoice with GL entry reference
        conn.execute('''
            UPDATE invoices 
            SET status = 'Posted', 
                gl_entry_id = ?,
                posted_by = ?, 
                posted_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (gl_entry_id, session['user_id'], id))
        
        # Update balance due
        conn.execute('''
            UPDATE invoices SET balance_due = total_amount - amount_paid WHERE id = ?
        ''', (id,))
        
        conn.commit()
        flash(f'Invoice {invoice["invoice_number"]} posted to GL! Revenue recorded (Entry: {entry_number})', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error posting invoice: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('invoice_routes.view_invoice', id=id))

@invoice_bp.route('/invoices/<int:id>/void', methods=['POST'])
@login_required
@role_required('Admin')
def void_invoice(id):
    """Void an invoice (Admin only)"""
    db = Database()
    conn = db.get_connection()
    
    try:
        invoice = conn.execute('SELECT * FROM invoices WHERE id = ?', (id,)).fetchone()
        
        if not invoice:
            flash('Invoice not found', 'danger')
            return redirect(url_for('invoice_routes.list_invoices'))
        
        if invoice['status'] == 'Void':
            flash('Invoice is already voided', 'warning')
            return redirect(url_for('invoice_routes.view_invoice', id=id))
        
        # Update invoice status
        conn.execute('UPDATE invoices SET status = \'Void\' WHERE id = ?', (id,))
        
        conn.commit()
        flash(f'Invoice {invoice["invoice_number"]} has been voided', 'info')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error voiding invoice: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('invoice_routes.view_invoice', id=id))
