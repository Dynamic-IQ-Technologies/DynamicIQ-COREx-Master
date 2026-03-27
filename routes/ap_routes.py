from flask import Blueprint, render_template, request, redirect, url_for, flash, session, make_response
from models import Database, AuditLogger
from auth import login_required, role_required
from datetime import datetime, date, timedelta
import csv
import io

ap_bp = Blueprint('ap_routes', __name__)

@ap_bp.route('/accounts-payable')
@login_required
@role_required('Admin', 'Accountant', 'Procurement')
def list_ap():
    """List all Accounts Payable records"""
    db = Database()
    conn = db.get_connection()
    
    # Get filter parameters
    status_filter = request.args.get('status', 'all')
    
    query = '''
        SELECT 
            vi.*,
            s.name as vendor_name,
            s.code as vendor_code,
            po.po_number,
            (vi.total_amount - vi.amount_paid) as balance_due
        FROM vendor_invoices vi
        JOIN suppliers s ON vi.vendor_id = s.id
        LEFT JOIN purchase_orders po ON vi.po_id = po.id
        WHERE 1=1
    '''
    
    params = []
    if status_filter != 'all':
        query += ' AND vi.status = ?'
        params.append(status_filter)
    
    query += ' ORDER BY vi.due_date ASC, vi.created_at DESC'
    
    payables = conn.execute(query, params).fetchall()
    
    # Calculate summary statistics
    today_date = datetime.now().date()
    total_open = sum((p['balance_due'] or 0) for p in payables if p['status'] in ['Open', 'Pending Invoice'])
    total_overdue = sum(
        (p['balance_due'] or 0) for p in payables 
        if p['status'] in ['Open', 'Pending Invoice'] and p.get('due_date') and (p['due_date'] if isinstance(p['due_date'], date) else datetime.strptime(str(p['due_date']), '%Y-%m-%d').date()) < today_date
    )
    
    conn.close()
    
    return render_template('ap/list.html', 
                         payables=payables,
                         status_filter=status_filter,
                         total_open=total_open,
                         total_overdue=total_overdue,
                         today_date=today_date)

@ap_bp.route('/accounts-payable/<int:id>')
@login_required
@role_required('Admin', 'Accountant', 'Procurement')
def view_ap(id):
    """View A/P record details"""
    db = Database()
    conn = db.get_connection()
    
    ap = conn.execute('''
        SELECT 
            vi.*,
            s.name as vendor_name,
            s.code as vendor_code,
            s.contact_person,
            s.email,
            s.phone,
            po.po_number,
            ge.entry_number as gl_entry_number
        FROM vendor_invoices vi
        JOIN suppliers s ON vi.vendor_id = s.id
        LEFT JOIN purchase_orders po ON vi.po_id = po.id
        LEFT JOIN gl_entries ge ON vi.gl_entry_id = ge.id
        WHERE vi.id = ?
    ''', (id,)).fetchone()
    
    if not ap:
        flash('A/P record not found.', 'danger')
        conn.close()
        return redirect(url_for('ap_routes.list_ap'))
    
    # Get related receiving transactions
    receipts = conn.execute('''
        SELECT 
            rt.*,
            p.code as product_code,
            p.name as product_name
        FROM receiving_transactions rt
        JOIN products p ON rt.product_id = p.id
        WHERE rt.po_id = ?
        ORDER BY rt.receipt_date DESC
    ''', (ap['po_id'],)).fetchall() if ap['po_id'] else []
    
    conn.close()
    
    return render_template('ap/view.html', ap=ap, receipts=receipts, today_date=datetime.now().strftime('%Y-%m-%d'))

@ap_bp.route('/accounts-payable/<int:id>/edit', methods=['GET', 'POST'])
@login_required
@role_required('Admin', 'Accountant')
def edit_ap(id):
    """Edit A/P record"""
    db = Database()
    conn = db.get_connection()
    
    # Get the A/P record
    ap = conn.execute('''
        SELECT vi.*, s.name as vendor_name
        FROM vendor_invoices vi
        JOIN suppliers s ON vi.vendor_id = s.id
        WHERE vi.id = ?
    ''', (id,)).fetchone()
    
    if not ap:
        flash('A/P record not found.', 'danger')
        conn.close()
        return redirect(url_for('ap_routes.list_ap'))
    
    # Don't allow editing if already paid
    if ap['status'] == 'Paid':
        flash('Cannot edit paid invoices.', 'warning')
        conn.close()
        return redirect(url_for('ap_routes.view_ap', id=id))
    
    if request.method == 'POST':
        try:
            # Get old record for audit
            old_ap = dict(ap)
            
            # Get form data
            invoice_number = request.form['invoice_number']
            vendor_invoice_number = request.form.get('vendor_invoice_number', '').strip()
            invoice_date = request.form['invoice_date']
            due_date = request.form['due_date']
            total_amount = float(request.form['total_amount'])
            description = request.form.get('description', '')
            
            # Validate (before any DB writes)
            if total_amount <= 0:
                flash('Total amount must be greater than zero', 'danger')
                return render_template('ap/edit.html', ap=ap)
            
            if invoice_date > due_date:
                flash('Due date cannot be before invoice date', 'danger')
                return render_template('ap/edit.html', ap=ap)
            
            # Critical: Ensure total amount is not less than amount already paid
            amount_paid = ap['amount_paid'] or 0
            if total_amount < amount_paid:
                flash(f'Total amount cannot be less than amount already paid (${amount_paid:.2f})', 'danger')
                return render_template('ap/edit.html', ap=ap)
            
            # Update the record
            conn.execute('''
                UPDATE vendor_invoices 
                SET invoice_number = ?,
                    vendor_invoice_number = ?,
                    invoice_date = ?,
                    due_date = ?,
                    total_amount = ?,
                    description = ?
                WHERE id = ?
            ''', (invoice_number, vendor_invoice_number or None, invoice_date, due_date, total_amount, description, id))
            
            # Get new record for audit
            new_ap = conn.execute('SELECT * FROM vendor_invoices WHERE id = ?', (id,)).fetchone()
            
            # Log audit trail
            changes = AuditLogger.compare_records(old_ap, dict(new_ap))
            if changes:
                AuditLogger.log_change(
                    conn=conn,
                    record_type='accounts_payable',
                    record_id=id,
                    action_type='Updated',
                    modified_by=session.get('user_id'),
                    changed_fields=changes,
                    ip_address=request.remote_addr,
                    user_agent=request.headers.get('User-Agent')
                )
            
            conn.commit()
            flash('A/P record updated successfully!', 'success')
            return redirect(url_for('ap_routes.view_ap', id=id))
            
        except Exception as e:
            try:
                conn.rollback()
            except:
                pass  # No transaction to rollback
            flash(f'Error updating A/P record: {str(e)}', 'danger')
            return render_template('ap/edit.html', ap=ap)
        finally:
            conn.close()
    
    conn.close()
    return render_template('ap/edit.html', ap=ap)

@ap_bp.route('/accounts-payable/<int:id>/set-vendor-invoice-number', methods=['POST'])
@login_required
@role_required('Admin', 'Accountant', 'Procurement')
def set_vendor_invoice_number(id):
    """Set or update the vendor's own invoice number on an A/P record."""
    db = Database()
    conn = db.get_connection()
    try:
        conn.rollback()
    except Exception:
        pass
    try:
        ap = conn.execute('SELECT * FROM vendor_invoices WHERE id = ?', (id,)).fetchone()
        if not ap:
            flash('A/P record not found.', 'danger')
            return redirect(url_for('ap_routes.list_ap'))

        vendor_invoice_number = request.form.get('vendor_invoice_number', '').strip()
        if not vendor_invoice_number:
            flash('Vendor invoice number cannot be blank.', 'warning')
            return redirect(url_for('ap_routes.view_ap', id=id))

        old_ap = dict(ap)
        conn.execute(
            'UPDATE vendor_invoices SET vendor_invoice_number = ? WHERE id = ?',
            (vendor_invoice_number, id)
        )

        changes = AuditLogger.compare_records(old_ap, dict(conn.execute('SELECT * FROM vendor_invoices WHERE id = ?', (id,)).fetchone()))
        if changes:
            AuditLogger.log_change(
                conn=conn,
                record_type='accounts_payable',
                record_id=id,
                action_type='Updated',
                modified_by=session.get('user_id'),
                changed_fields=changes,
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )

        conn.commit()
        flash(f'Vendor invoice number set to <strong>{vendor_invoice_number}</strong>.', 'success')
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        flash(f'Error updating vendor invoice number: {str(e)}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('ap_routes.view_ap', id=id))


@ap_bp.route('/accounts-payable/<int:id>/record-payment', methods=['POST'])
@login_required
@role_required('Admin', 'Accountant')
def record_payment(id):
    """Record payment for A/P"""
    db = Database()
    conn = db.get_connection()
    
    # Ensure clean transaction state (PostgreSQL requires this after any prior error)
    try:
        conn.rollback()
    except:
        pass
    
    try:
        # Get the A/P record
        ap = conn.execute('SELECT * FROM vendor_invoices WHERE id = ?', (id,)).fetchone()
        
        if not ap:
            flash('A/P record not found.', 'danger')
            conn.close()
            return redirect(url_for('ap_routes.list_ap'))
        
        if ap['status'] == 'Paid':
            flash('This invoice is already fully paid.', 'warning')
            conn.close()
            return redirect(url_for('ap_routes.view_ap', id=id))
        
        # Get old record for audit
        old_ap = dict(ap)
        
        # Get payment details
        payment_amount = float(request.form['payment_amount'])
        payment_date = request.form['payment_date']
        payment_method = request.form.get('payment_method', 'Check')
        payment_reference = request.form.get('payment_reference', '')
        
        # Calculate current balance
        current_paid = ap['amount_paid'] or 0
        total_amount = ap['total_amount']
        balance_due = total_amount - current_paid
        
        # Validate payment amount
        if payment_amount <= 0:
            flash('Payment amount must be greater than zero', 'danger')
            conn.close()
            return redirect(url_for('ap_routes.view_ap', id=id))
        
        if payment_amount > balance_due:
            flash(f'Payment amount (${payment_amount:.2f}) cannot exceed balance due (${balance_due:.2f})', 'danger')
            conn.close()
            return redirect(url_for('ap_routes.view_ap', id=id))
        
        # Calculate new total paid
        new_amount_paid = current_paid + payment_amount
        
        # Determine new status
        new_status = 'Paid' if new_amount_paid >= total_amount else 'Open'
        
        # Generate GL entry number for payment
        last_payment_entry = conn.execute('''
            SELECT entry_number FROM gl_entries 
            WHERE entry_number LIKE 'AP-PAY-%'
            ORDER BY CAST(SUBSTR(entry_number, 8) AS INTEGER) DESC 
            LIMIT 1
        ''').fetchone()
        
        if last_payment_entry:
            try:
                last_number = int(last_payment_entry['entry_number'].split('-')[2])
                next_number = last_number + 1
            except (ValueError, IndexError):
                next_number = 1
        else:
            next_number = 1
        
        payment_entry_number = f'AP-PAY-{next_number:06d}'
        
        # Create GL entry for payment (DR: A/P, CR: Cash)
        gl_description = f'Payment for {ap["invoice_number"]} - {payment_method}'
        if payment_reference:
            gl_description += f' ({payment_reference})'
        
        gl_pay_cur = conn.execute('''
            INSERT INTO gl_entries (
                entry_number, entry_date, description, 
                transaction_source, reference_type, reference_id, 
                status, created_by, posted_by, posted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            payment_entry_number, payment_date, gl_description,
            'AP Payment', 'vendor_invoice', id,
            'Posted', session.get('user_id'), session.get('user_id'), datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ))
        gl_entry_id = gl_pay_cur.lastrowid
        
        # Get account IDs
        ap_account = conn.execute("SELECT id FROM chart_of_accounts WHERE account_code = '2110'").fetchone()
        cash_account = conn.execute("SELECT id FROM chart_of_accounts WHERE account_code = '1110'").fetchone()
        
        if not ap_account or not cash_account:
            raise ValueError('Required GL accounts not found (A/P: 2110, Cash: 1110)')
        
        # DR: Accounts Payable (reduces liability)
        conn.execute('''
            INSERT INTO gl_entry_lines (gl_entry_id, account_id, debit, credit, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (gl_entry_id, ap_account['id'], payment_amount, 0, f'Payment for {ap["invoice_number"]}'))
        
        # CR: Cash (reduces asset)
        conn.execute('''
            INSERT INTO gl_entry_lines (gl_entry_id, account_id, debit, credit, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (gl_entry_id, cash_account['id'], 0, payment_amount, f'Payment for {ap["invoice_number"]}'))
        
        # Update the vendor invoice record (payment method/reference are stored on the GL entry)
        conn.execute('''
            UPDATE vendor_invoices 
            SET amount_paid = ?,
                status = ?,
                payment_date = ?
            WHERE id = ?
        ''', (new_amount_paid, new_status, payment_date, id))
        
        # Get new record for audit
        new_ap = conn.execute('SELECT * FROM vendor_invoices WHERE id = ?', (id,)).fetchone()
        
        # Log audit trail
        changes = AuditLogger.compare_records(old_ap, dict(new_ap))
        if changes:
            AuditLogger.log_change(
                conn=conn,
                record_type='accounts_payable',
                record_id=id,
                action_type='Payment Recorded',
                modified_by=session.get('user_id'),
                changed_fields=changes,
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
        
        conn.commit()
        
        if new_status == 'Paid':
            flash(f'Payment of ${payment_amount:.2f} recorded. Invoice is now fully paid!', 'success')
        else:
            remaining = total_amount - new_amount_paid
            flash(f'Payment of ${payment_amount:.2f} recorded. Remaining balance: ${remaining:.2f}', 'success')
        
        return redirect(url_for('ap_routes.view_ap', id=id))
        
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        flash(f'Error recording payment: {str(e)}', 'danger')
        return redirect(url_for('ap_routes.view_ap', id=id))
    finally:
        conn.close()

@ap_bp.route('/accounts-payable/<int:id>/update-status', methods=['POST'])
@login_required
@role_required('Admin', 'Accountant')
def update_ap_status(id):
    """Update A/P status"""
    db = Database()
    conn = db.get_connection()
    
    try:
        # Get old record for audit
        old_ap = conn.execute('SELECT * FROM vendor_invoices WHERE id = ?', (id,)).fetchone()
        
        new_status = request.form['status']
        
        conn.execute('UPDATE vendor_invoices SET status = ? WHERE id = ?', (new_status, id))
        
        # Get new record for audit
        new_ap = conn.execute('SELECT * FROM vendor_invoices WHERE id = ?', (id,)).fetchone()
        
        # Log audit trail
        changes = AuditLogger.compare_records(dict(old_ap), dict(new_ap))
        if changes:
            AuditLogger.log_change(
                conn=conn,
                record_type='accounts_payable',
                record_id=id,
                action_type='Updated',
                modified_by=session.get('user_id'),
                changed_fields=changes,
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
        
        conn.commit()
        flash(f'A/P status updated to {new_status}!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error updating A/P status: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('ap_routes.view_ap', id=id))

@ap_bp.route('/accounts-payable/dashboard')
@login_required
@role_required('Admin', 'Accountant', 'Procurement')
def ap_dashboard():
    """A/P Dashboard with aging and summary"""
    db = Database()
    conn = db.get_connection()
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Get aging buckets
    aging_data = {
        'current': [],
        '1_30': [],
        '31_60': [],
        '61_90': [],
        'over_90': []
    }
    
    open_payables = conn.execute('''
        SELECT 
            vi.*,
            s.name as vendor_name,
            (vi.total_amount - vi.amount_paid) as balance_due,
            julianday(?) - julianday(vi.due_date) as days_overdue
        FROM vendor_invoices vi
        JOIN suppliers s ON vi.vendor_id = s.id
        WHERE vi.status IN ('Open', 'Pending Invoice')
    ''', (today,)).fetchall()
    
    for ap in open_payables:
        days = ap['days_overdue']
        if days < 0:
            aging_data['current'].append(ap)
        elif days <= 30:
            aging_data['1_30'].append(ap)
        elif days <= 60:
            aging_data['31_60'].append(ap)
        elif days <= 90:
            aging_data['61_90'].append(ap)
        else:
            aging_data['over_90'].append(ap)
    
    # Calculate totals
    aging_totals = {
        'current': sum(ap['balance_due'] for ap in aging_data['current']),
        '1_30': sum(ap['balance_due'] for ap in aging_data['1_30']),
        '31_60': sum(ap['balance_due'] for ap in aging_data['31_60']),
        '61_90': sum(ap['balance_due'] for ap in aging_data['61_90']),
        'over_90': sum(ap['balance_due'] for ap in aging_data['over_90'])
    }
    
    # Top vendors by payable amount
    top_vendors = conn.execute('''
        SELECT 
            s.name as vendor_name,
            COUNT(vi.id) as invoice_count,
            SUM(vi.total_amount - vi.amount_paid) as total_due
        FROM vendor_invoices vi
        JOIN suppliers s ON vi.vendor_id = s.id
        WHERE vi.status IN ('Open', 'Pending Invoice')
        GROUP BY s.id, s.name
        ORDER BY total_due DESC
        LIMIT 10
    ''').fetchall()
    
    conn.close()
    
    return render_template('ap/dashboard.html', 
                         aging_data=aging_data,
                         aging_totals=aging_totals,
                         top_vendors=top_vendors)

@ap_bp.route('/accounts-payable/export')
@login_required
@role_required('Admin', 'Accountant', 'Procurement')
def export_ap():
    """Export A/P records to CSV"""
    db = Database()
    conn = db.get_connection()
    
    payables = conn.execute('''
        SELECT 
            vi.invoice_number,
            s.name as vendor_name,
            po.po_number,
            vi.invoice_date,
            vi.due_date,
            vi.total_amount,
            vi.amount_paid,
            (vi.total_amount - vi.amount_paid) as balance_due,
            vi.status
        FROM vendor_invoices vi
        JOIN suppliers s ON vi.vendor_id = s.id
        LEFT JOIN purchase_orders po ON vi.po_id = po.id
        ORDER BY vi.due_date ASC
    ''').fetchall()
    
    # Create CSV
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Header
    writer.writerow(['AP Number', 'Vendor', 'PO Number', 'Invoice Date', 'Due Date', 
                    'Total Amount', 'Amount Paid', 'Balance Due', 'Status'])
    
    # Data
    for ap in payables:
        writer.writerow([
            ap['invoice_number'],
            ap['vendor_name'],
            ap['po_number'] or '',
            ap['invoice_date'],
            ap['due_date'],
            f"${ap['total_amount']:.2f}",
            f"${ap['amount_paid']:.2f}",
            f"${ap['balance_due']:.2f}",
            ap['status']
        ])
    
    # Create response
    output.seek(0)
    response = make_response(output.getvalue())
    response.headers['Content-Disposition'] = f'attachment; filename=accounts_payable_{datetime.now().strftime("%Y%m%d")}.csv'
    response.headers['Content-Type'] = 'text/csv'
    
    conn.close()
    return response
