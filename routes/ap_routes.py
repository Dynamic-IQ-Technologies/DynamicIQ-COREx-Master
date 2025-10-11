from flask import Blueprint, render_template, request, redirect, url_for, flash, session, make_response
from models import Database, AuditLogger
from auth import login_required, role_required
from datetime import datetime, timedelta
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
    today_date = datetime.now().strftime('%Y-%m-%d')
    total_open = sum(p['balance_due'] for p in payables if p['status'] in ['Open', 'Pending Invoice'])
    total_overdue = sum(
        p['balance_due'] for p in payables 
        if p['status'] in ['Open', 'Pending Invoice'] and p['due_date'] < today_date
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
