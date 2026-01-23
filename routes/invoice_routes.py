from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import Database, AuditLogger
from auth import login_required, role_required
from datetime import datetime, timedelta
import secrets
import os


def get_brevo_credentials():
    """Get Brevo API key and from email from environment"""
    api_key = os.environ.get('BREVO_API_KEY')
    from_email = os.environ.get('BREVO_FROM_EMAIL')
    return api_key, from_email


def send_invoice_email_via_brevo(to_email, to_name, subject, html_content, from_email, from_name, api_key):
    """Send invoice email using Brevo API"""
    import sib_api_v3_sdk
    from sib_api_v3_sdk.rest import ApiException
    
    configuration = sib_api_v3_sdk.Configuration()
    configuration.api_key['api-key'] = api_key
    
    api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))
    
    send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(
        to=[{"email": to_email, "name": to_name}],
        sender={"email": from_email, "name": from_name},
        subject=subject,
        html_content=html_content
    )
    
    try:
        api_response = api_instance.send_transac_email(send_smtp_email)
        if api_response and hasattr(api_response, 'message_id') and api_response.message_id:
            return True, str(api_response.message_id)
        return True, 'Email sent successfully'
    except ApiException as e:
        return False, str(e)
    except Exception as e:
        return False, str(e)

invoice_bp = Blueprint('invoice_routes', __name__)

@invoice_bp.route('/invoices')
@login_required
@role_required('Admin', 'Accountant', 'Planner')
def list_invoices():
    """Display invoice dashboard with all invoices including NDT invoices"""
    db = Database()
    conn = db.get_connection()
    
    # Get filter parameters
    status_filter = request.args.get('status', 'all')
    customer_filter = request.args.get('customer_id', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    invoice_type = request.args.get('type', 'all')
    
    # Build query for regular invoices
    query = '''
        SELECT 
            i.id, i.invoice_number, i.invoice_date, i.due_date, i.status,
            i.total_amount, i.amount_paid, i.balance_due, i.created_at,
            c.name as customer_name,
            c.customer_number,
            so.so_number,
            wo.wo_number,
            u.username as created_by_name,
            COALESCE(lc.line_count, 0) as line_count,
            'Standard' as invoice_type,
            NULL as ndt_wo_number
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        LEFT JOIN sales_orders so ON i.so_id = so.id
        LEFT JOIN work_orders wo ON i.wo_id = wo.id
        LEFT JOIN users u ON i.created_by = u.id
        LEFT JOIN (
            SELECT invoice_id, COUNT(*) as line_count FROM invoice_lines GROUP BY invoice_id
        ) lc ON lc.invoice_id = i.id
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
    
    # Get regular invoices if not filtering to NDT only
    if invoice_type != 'ndt':
        invoices = [dict(row) for row in conn.execute(query, params).fetchall()]
    else:
        invoices = []
    
    # Build query for NDT invoices
    ndt_query = '''
        SELECT 
            ni.id, ni.invoice_number, ni.invoice_date, ni.due_date, ni.status,
            ni.total_amount, ni.amount_paid, ni.balance_due, ni.created_at,
            c.name as customer_name,
            c.customer_number,
            NULL as so_number,
            NULL as wo_number,
            u.username as created_by_name,
            0 as line_count,
            'NDT' as invoice_type,
            nw.ndt_wo_number
        FROM ndt_invoices ni
        LEFT JOIN customers c ON ni.customer_id = c.id
        LEFT JOIN users u ON ni.created_by = u.id
        LEFT JOIN ndt_work_orders nw ON ni.ndt_wo_id = nw.id
        WHERE 1=1
    '''
    
    ndt_params = []
    if status_filter != 'all':
        ndt_query += ' AND ni.status = ?'
        ndt_params.append(status_filter)
    if customer_filter:
        ndt_query += ' AND ni.customer_id = ?'
        ndt_params.append(int(customer_filter))
    if date_from:
        ndt_query += ' AND ni.invoice_date >= ?'
        ndt_params.append(date_from)
    if date_to:
        ndt_query += ' AND ni.invoice_date <= ?'
        ndt_params.append(date_to)
    
    # Get NDT invoices if not filtering to standard only
    if invoice_type != 'standard':
        ndt_invoices = [dict(row) for row in conn.execute(ndt_query, ndt_params).fetchall()]
        invoices.extend(ndt_invoices)
    
    # Sort combined list by date descending
    invoices.sort(key=lambda x: (x['invoice_date'] or '', x['created_at'] or ''), reverse=True)
    
    # Get customers for filter dropdown
    customers = conn.execute('''
        SELECT id, customer_number, name FROM customers ORDER BY customer_number
    ''').fetchall()
    
    # Calculate dashboard metrics
    total_invoiced = sum(inv['total_amount'] for inv in invoices)
    total_paid = sum(inv['amount_paid'] for inv in invoices)
    total_outstanding = sum(inv['balance_due'] for inv in invoices if inv['status'] != 'Void')
    
    # Overdue invoices
    today = datetime.now().date()
    overdue_amount = sum(
        inv['balance_due'] for inv in invoices 
        if inv['status'] in ['Posted', 'Approved'] and inv['due_date'] and (inv['due_date'] if hasattr(inv['due_date'], 'date') else datetime.strptime(str(inv['due_date'])[:10], '%Y-%m-%d').date()) < today and inv['balance_due'] > 0
    )
    
    conn.close()
    
    return render_template('invoices/dashboard.html',
                         invoices=invoices,
                         customers=customers,
                         status_filter=status_filter,
                         customer_filter=customer_filter,
                         date_from=date_from,
                         date_to=date_to,
                         invoice_type=invoice_type,
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
            
            # Generate invoice number - find max INV- number
            last_inv = conn.execute('''
                SELECT invoice_number FROM invoices 
                WHERE invoice_number LIKE 'INV-%'
                ORDER BY id DESC
            ''').fetchall()
            
            max_num = 0
            for inv in last_inv:
                try:
                    parts = inv['invoice_number'].replace('INV-', '').split('-')
                    num = int(parts[-1])
                    if num > max_num:
                        max_num = num
                except (ValueError, IndexError):
                    continue
            
            invoice_number = f'INV-{max_num + 1:06d}'
            
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


@invoice_bp.route('/invoices/create-from-ndt/<int:ndt_wo_id>', methods=['GET', 'POST'])
@login_required
@role_required('Admin', 'Accountant', 'Planner')
def create_from_ndt_wo(ndt_wo_id):
    """Create invoice or quote from NDT Work Order"""
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            invoice_date = request.form.get('invoice_date') or datetime.now().strftime('%Y-%m-%d')
            payment_terms = int(request.form.get('payment_terms', 30))
            document_type = request.form.get('document_type', 'Invoice')
            
            inv_date = datetime.strptime(invoice_date, '%Y-%m-%d')
            due_date = (inv_date + timedelta(days=payment_terms)).strftime('%Y-%m-%d')
            
            ndt_wo = conn.execute('''
                SELECT nw.*, c.name as customer_name, c.payment_terms as customer_payment_terms
                FROM ndt_work_orders nw
                LEFT JOIN customers c ON nw.customer_id = c.id
                WHERE nw.id = ?
            ''', (ndt_wo_id,)).fetchone()
            
            if not ndt_wo:
                flash('NDT Work Order not found', 'danger')
                conn.close()
                return redirect(url_for('invoice_routes.list_invoices'))
            
            if not ndt_wo['customer_id']:
                flash('NDT Work Order has no customer assigned', 'danger')
                conn.close()
                return redirect(url_for('ndt_routes.wo_view', id=ndt_wo_id))
            
            prefix = 'QUO' if document_type == 'Quote' else 'INV'
            last_inv = conn.execute(f'''
                SELECT invoice_number FROM invoices 
                WHERE invoice_number LIKE '{prefix}-%'
                ORDER BY id DESC
            ''').fetchall()
            
            max_num = 0
            for inv in last_inv:
                try:
                    parts = inv['invoice_number'].replace(f'{prefix}-', '').split('-')
                    num = int(parts[-1])
                    if num > max_num:
                        max_num = num
                except (ValueError, IndexError):
                    continue
            
            invoice_number = f'{prefix}-{max_num + 1:06d}'
            
            subtotal = 0
            line_items = []
            
            for i in range(1, 11):
                desc = request.form.get(f'line_desc_{i}')
                qty = request.form.get(f'line_qty_{i}')
                price = request.form.get(f'line_price_{i}')
                
                if desc and qty and price:
                    qty = float(qty)
                    price = float(price)
                    line_total = qty * price
                    subtotal += line_total
                    line_items.append({
                        'line_number': len(line_items) + 1,
                        'description': desc,
                        'quantity': qty,
                        'unit_price': price,
                        'line_total': line_total
                    })
            
            tax_rate = float(request.form.get('tax_rate', 0))
            tax_amount = subtotal * (tax_rate / 100)
            total_amount = subtotal + tax_amount
            
            invoice_type = f'NDT {document_type}'
            
            cursor = conn.execute('''
                INSERT INTO invoices (
                    invoice_number, invoice_type, customer_id, 
                    invoice_date, due_date, payment_terms, status,
                    subtotal, tax_rate, tax_amount, discount_amount, total_amount,
                    balance_due, notes, source_type, source_id, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                invoice_number, invoice_type, ndt_wo['customer_id'],
                invoice_date, due_date, payment_terms, 'Draft',
                subtotal, tax_rate, tax_amount, 0, total_amount,
                total_amount, request.form.get('notes', ''), 
                'ndt_work_order', ndt_wo_id, session['user_id']
            ))
            
            invoice_id = cursor.lastrowid
            
            for line in line_items:
                conn.execute('''
                    INSERT INTO invoice_lines (
                        invoice_id, line_number, description,
                        quantity, unit_price, discount_percent, line_total,
                        reference_type, reference_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    invoice_id, line['line_number'], line['description'],
                    line['quantity'], line['unit_price'], 0, line['line_total'],
                    'ndt_work_order', ndt_wo_id
                ))
            
            conn.commit()
            flash(f'{document_type} {invoice_number} created successfully!', 'success')
            return redirect(url_for('invoice_routes.view_invoice', id=invoice_id))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error creating document: {str(e)}', 'danger')
        finally:
            conn.close()
        
        return redirect(url_for('ndt_routes.wo_view', id=ndt_wo_id))
    
    ndt_wo = conn.execute('''
        SELECT nw.*, c.name as customer_name, c.customer_number, c.payment_terms
        FROM ndt_work_orders nw
        LEFT JOIN customers c ON nw.customer_id = c.id
        WHERE nw.id = ?
    ''', (ndt_wo_id,)).fetchone()
    
    if not ndt_wo:
        flash('NDT Work Order not found', 'danger')
        conn.close()
        return redirect(url_for('ndt_routes.wo_list'))
    
    if not ndt_wo['customer_id']:
        flash('NDT Work Order has no customer assigned. Please assign a customer first.', 'warning')
        conn.close()
        return redirect(url_for('ndt_routes.wo_edit', id=ndt_wo_id))
    
    inspection_results = conn.execute('''
        SELECT ir.*, t.first_name || ' ' || t.last_name as technician_name
        FROM ndt_inspection_results ir
        LEFT JOIN ndt_technicians t ON ir.technician_id = t.id
        WHERE ir.ndt_wo_id = ?
        ORDER BY ir.method
    ''', (ndt_wo_id,)).fetchall()
    
    conn.close()
    
    return render_template('invoices/create_from_ndt.html',
                         ndt_wo=ndt_wo,
                         inspection_results=inspection_results,
                         today=datetime.now().strftime('%Y-%m-%d'))


@invoice_bp.route('/invoices/<int:invoice_id>/send-to-customer')
@login_required
def send_to_customer(invoice_id):
    """Page to send invoice to customer via email"""
    db = Database()
    conn = db.get_connection()
    
    invoice = conn.execute('''
        SELECT i.*, c.name as customer_name, c.email as customer_email, c.customer_number
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        WHERE i.id = ?
    ''', (invoice_id,)).fetchone()
    
    if not invoice:
        conn.close()
        flash('Invoice not found', 'danger')
        return redirect(url_for('invoice_routes.list_invoices'))
    
    lines = conn.execute('''
        SELECT il.*, p.code as part_number, p.name as product_name
        FROM invoice_lines il
        LEFT JOIN products p ON il.product_id = p.id
        WHERE il.invoice_id = ?
        ORDER BY il.line_number
    ''', (invoice_id,)).fetchall()
    
    existing_token = conn.execute('''
        SELECT * FROM invoice_customer_tokens
        WHERE invoice_id = ? AND customer_id = ? AND expires_at > ?
        ORDER BY created_at DESC LIMIT 1
    ''', (invoice_id, invoice['customer_id'], datetime.now().isoformat())).fetchone()
    
    conn.close()
    
    return render_template('invoices/send_to_customer.html',
                         invoice=invoice,
                         lines=lines,
                         existing_token=existing_token)


@invoice_bp.route('/invoices/<int:invoice_id>/generate-link', methods=['POST'])
@login_required
def generate_customer_link(invoice_id):
    """Generate secure token link for customer to view invoice"""
    db = Database()
    conn = db.get_connection()
    
    try:
        invoice = conn.execute('''
            SELECT i.*, c.id as customer_id FROM invoices i
            JOIN customers c ON i.customer_id = c.id
            WHERE i.id = ?
        ''', (invoice_id,)).fetchone()
        
        if not invoice:
            conn.close()
            flash('Invoice not found', 'danger')
            return redirect(url_for('invoice_routes.list_invoices'))
        
        token = secrets.token_urlsafe(32)
        expires_at = datetime.now() + timedelta(days=60)
        
        conn.execute('''
            INSERT INTO invoice_customer_tokens (invoice_id, customer_id, token, expires_at)
            VALUES (?, ?, ?, ?)
        ''', (invoice_id, invoice['customer_id'], token, expires_at.isoformat()))
        
        conn.commit()
        flash('Secure link generated successfully', 'success')
        conn.close()
        return redirect(url_for('invoice_routes.send_to_customer', invoice_id=invoice_id))
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error generating link: {str(e)}', 'danger')
        return redirect(url_for('invoice_routes.send_to_customer', invoice_id=invoice_id))


@invoice_bp.route('/invoices/<int:invoice_id>/email-link', methods=['POST'])
@login_required
def email_customer_link(invoice_id):
    """Email invoice secure link to customer"""
    db = Database()
    conn = db.get_connection()
    
    try:
        invoice = conn.execute('''
            SELECT i.*, c.name as customer_name, c.email as customer_email
            FROM invoices i
            JOIN customers c ON i.customer_id = c.id
            WHERE i.id = ?
        ''', (invoice_id,)).fetchone()
        
        if not invoice:
            conn.close()
            flash('Invoice not found', 'danger')
            return redirect(url_for('invoice_routes.list_invoices'))
        
        if not invoice['customer_email']:
            conn.close()
            flash('Customer does not have an email address on file', 'warning')
            return redirect(url_for('invoice_routes.send_to_customer', invoice_id=invoice_id))
        
        token_record = conn.execute('''
            SELECT * FROM invoice_customer_tokens
            WHERE invoice_id = ? AND expires_at > ?
            ORDER BY created_at DESC LIMIT 1
        ''', (invoice_id, datetime.now().isoformat())).fetchone()
        
        if not token_record:
            conn.close()
            flash('No valid link found. Please generate a link first.', 'warning')
            return redirect(url_for('invoice_routes.send_to_customer', invoice_id=invoice_id))
        
        api_key, from_email = get_brevo_credentials()
        if not api_key or not from_email:
            conn.close()
            flash('Email service not configured. Please set BREVO_API_KEY and BREVO_FROM_EMAIL in Secrets.', 'danger')
            return redirect(url_for('invoice_routes.send_to_customer', invoice_id=invoice_id))
        
        company = conn.execute('SELECT * FROM company_settings LIMIT 1').fetchone()
        company_name = company['company_name'] if company else 'Dynamic.IQ-COREx'
        
        base_url = request.url_root.rstrip('/')
        customer_link = f"{base_url}/invoice/view/{token_record['token']}"
        
        lines = conn.execute('''
            SELECT il.*, p.code as part_number, p.name as product_name
            FROM invoice_lines il
            LEFT JOIN products p ON il.product_id = p.id
            WHERE il.invoice_id = ?
            ORDER BY il.line_number
        ''', (invoice_id,)).fetchall()
        
        lines_html = ""
        for line in lines:
            lines_html += f'''
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #e2e8f0;">{line['part_number'] or line['description'][:30]}</td>
                    <td style="padding: 10px; border-bottom: 1px solid #e2e8f0;">{line['description'][:40] if line['description'] else ''}</td>
                    <td style="padding: 10px; border-bottom: 1px solid #e2e8f0; text-align: right;">{line['quantity']}</td>
                    <td style="padding: 10px; border-bottom: 1px solid #e2e8f0; text-align: right;">${line['unit_price']:.2f}</td>
                    <td style="padding: 10px; border-bottom: 1px solid #e2e8f0; text-align: right;">${line['line_total']:.2f}</td>
                </tr>
            '''
        
        html_content = f'''
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 0; background-color: #f8fafc; }}
        .container {{ max-width: 650px; margin: 0 auto; background-color: #ffffff; }}
        .header {{ background: linear-gradient(135deg, #1e3a5f 0%, #3b82f6 100%); color: white; padding: 30px; text-align: center; }}
        .header h1 {{ margin: 0; font-size: 24px; }}
        .content {{ padding: 30px; }}
        .invoice-box {{ background-color: #f1f5f9; border-radius: 8px; padding: 20px; margin: 20px 0; }}
        .invoice-box h3 {{ color: #1e3a5f; margin-top: 0; }}
        .detail-row {{ display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #e2e8f0; }}
        .detail-label {{ color: #64748b; }}
        .detail-value {{ color: #1e293b; font-weight: 500; }}
        .btn {{ display: inline-block; background: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%); color: white; padding: 15px 40px; 
                text-decoration: none; border-radius: 8px; font-weight: 600; margin: 20px 0; }}
        .footer {{ background-color: #f1f5f9; padding: 20px; text-align: center; font-size: 12px; color: #64748b; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th {{ background-color: #f1f5f9; padding: 10px; text-align: left; font-size: 12px; color: #64748b; text-transform: uppercase; }}
        .amount-due {{ font-size: 24px; color: #dc2626; font-weight: bold; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{company_name}</h1>
            <p style="margin: 10px 0 0; opacity: 0.9;">Invoice</p>
        </div>
        <div class="content">
            <p>Dear {invoice['customer_name']},</p>
            <p>Please find below your invoice details:</p>
            
            <div class="invoice-box">
                <h3>Invoice #{invoice['invoice_number']}</h3>
                <div class="detail-row">
                    <span class="detail-label">Invoice Date</span>
                    <span class="detail-value">{invoice['invoice_date']}</span>
                </div>
                <div class="detail-row">
                    <span class="detail-label">Due Date</span>
                    <span class="detail-value">{invoice['due_date']}</span>
                </div>
                <div class="detail-row">
                    <span class="detail-label">Amount Due</span>
                    <span class="amount-due">${invoice['balance_due']:.2f}</span>
                </div>
            </div>
            
            <h4>Invoice Items</h4>
            <table>
                <thead>
                    <tr>
                        <th>Item</th>
                        <th>Description</th>
                        <th style="text-align: right;">Qty</th>
                        <th style="text-align: right;">Unit Price</th>
                        <th style="text-align: right;">Total</th>
                    </tr>
                </thead>
                <tbody>
                    {lines_html}
                </tbody>
            </table>
            
            <div style="text-align: right; margin-top: 20px; padding: 15px; background-color: #f8fafc; border-radius: 8px;">
                <div style="margin-bottom: 8px;">Subtotal: <strong>${invoice['subtotal']:.2f}</strong></div>
                <div style="margin-bottom: 8px;">Tax: <strong>${invoice['tax_amount']:.2f}</strong></div>
                <div style="font-size: 18px; color: #1e3a5f;">Total: <strong>${invoice['total_amount']:.2f}</strong></div>
            </div>
            
            <p style="margin-top: 25px;">Click the button below to view your complete invoice online:</p>
            
            <div style="text-align: center;">
                <a href="{customer_link}" class="btn">View Invoice</a>
            </div>
            
            <p>If you have any questions about this invoice, please contact us.</p>
            
            <p>Thank you for your business.</p>
            
            <p>Best regards,<br>
            <strong>{company_name}</strong></p>
        </div>
        <div class="footer">
            <p>This is an automated message from {company_name}.</p>
        </div>
    </div>
</body>
</html>
'''
        
        success, result = send_invoice_email_via_brevo(
            invoice['customer_email'],
            invoice['customer_name'],
            f"Invoice {invoice['invoice_number']} from {company_name}",
            html_content,
            from_email,
            company_name,
            api_key
        )
        
        if success:
            conn.execute('''
                UPDATE invoice_customer_tokens 
                SET email_sent = 1, email_sent_at = ? 
                WHERE id = ?
            ''', (datetime.now().isoformat(), token_record['id']))
            conn.commit()
            flash(f'Invoice email sent successfully to {invoice["customer_email"]}', 'success')
        else:
            flash(f'Failed to send email: {result}', 'danger')
        
        conn.close()
        return redirect(url_for('invoice_routes.send_to_customer', invoice_id=invoice_id))
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error sending email: {str(e)}', 'danger')
        return redirect(url_for('invoice_routes.send_to_customer', invoice_id=invoice_id))


@invoice_bp.route('/invoice/view/<token>')
def customer_view_invoice(token):
    """Public portal for customers to view invoice via token"""
    db = Database()
    conn = db.get_connection()
    
    token_record = conn.execute('''
        SELECT * FROM invoice_customer_tokens WHERE token = ?
    ''', (token,)).fetchone()
    
    if not token_record:
        conn.close()
        return render_template('errors/invalid_token.html', 
                             message='This link is invalid or has expired.'), 404
    
    if datetime.fromisoformat(token_record['expires_at']) < datetime.now():
        conn.close()
        return render_template('errors/invalid_token.html',
                             message='This link has expired. Please contact us for a new link.'), 410
    
    conn.execute('''
        UPDATE invoice_customer_tokens SET last_accessed_at = ? WHERE id = ?
    ''', (datetime.now().isoformat(), token_record['id']))
    conn.commit()
    
    invoice = conn.execute('''
        SELECT i.*, c.name as customer_name, c.email as customer_email,
               c.billing_address, c.customer_number
        FROM invoices i
        JOIN customers c ON i.customer_id = c.id
        WHERE i.id = ?
    ''', (token_record['invoice_id'],)).fetchone()
    
    company = conn.execute('SELECT * FROM company_settings LIMIT 1').fetchone()
    
    lines = conn.execute('''
        SELECT il.*, p.code as part_number, p.name as product_name
        FROM invoice_lines il
        LEFT JOIN products p ON il.product_id = p.id
        WHERE il.invoice_id = ?
        ORDER BY il.line_number
    ''', (token_record['invoice_id'],)).fetchall()
    
    conn.close()
    
    return render_template('invoices/customer_portal.html',
                         invoice=invoice,
                         company=company,
                         lines=lines)
