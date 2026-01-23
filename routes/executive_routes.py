from flask import Blueprint, render_template, session, redirect, url_for, flash, request, jsonify
from functools import wraps
from models import Database, safe_float
from datetime import datetime, timedelta
import csv
from io import StringIO
from flask import make_response
import os

executive_routes = Blueprint('executive_routes', __name__)

USE_POSTGRES = os.environ.get('REPLIT_DEPLOYMENT') == '1' and os.environ.get('DATABASE_URL') is not None

def role_required(*roles):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user_id' not in session:
                return redirect(url_for('auth_routes.login'))
            if session.get('role') not in roles:
                flash('Access denied. Insufficient permissions.', 'danger')
                return redirect(url_for('main_routes.dashboard'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

@executive_routes.route('/executive-dashboard')
@role_required('Admin', 'Accountant')
def dashboard():
    db = Database()
    conn = db.get_connection()
    
    # Get filter parameters
    date_filter = request.args.get('date_range', 'ytd')
    vendor_filter = request.args.get('vendor_id', '')
    
    # Calculate date ranges
    today = datetime.now()
    current_year = today.year
    current_month = today.month
    
    if date_filter == 'wtd':
        days_since_monday = today.weekday()
        start_date = (today - timedelta(days=days_since_monday)).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
        period_label = "Week to Date"
    elif date_filter == 'mtd':
        start_date = datetime(current_year, current_month, 1).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
        period_label = "Month to Date"
    elif date_filter == 'qtd':
        quarter_start_month = ((current_month - 1) // 3) * 3 + 1
        start_date = datetime(current_year, quarter_start_month, 1).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
        period_label = "Quarter to Date"
    else:  # ytd
        start_date = datetime(current_year, 1, 1).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
        period_label = "Year to Date"
    
    # KPI 1: Total Revenue (from sales revenue in GL)
    # Revenue = Credits to Sales Revenue accounts (4100, 4200, 4300)
    revenue_query = '''
        SELECT COALESCE(SUM(gll.credit - gll.debit), 0) as total_revenue
        FROM gl_entry_lines gll
        JOIN gl_entries ge ON gll.gl_entry_id = ge.id
        JOIN chart_of_accounts coa ON gll.account_id = coa.id
        WHERE coa.account_type = 'Revenue'
        AND ge.entry_date BETWEEN ? AND ?
        AND ge.status = 'Posted'
    '''
    revenue = conn.execute(revenue_query, (start_date, end_date)).fetchone()['total_revenue']
    
    # KPI 2: Total Expenses (from GL expense accounts)
    expense_query = '''
        SELECT COALESCE(SUM(gll.debit - gll.credit), 0) as total_expenses
        FROM gl_entry_lines gll
        JOIN gl_entries ge ON gll.gl_entry_id = ge.id
        JOIN chart_of_accounts coa ON gll.account_id = coa.id
        WHERE coa.account_type = 'Expense'
        AND ge.entry_date BETWEEN ? AND ?
        AND ge.status = 'Posted'
    '''
    expenses = conn.execute(expense_query, (start_date, end_date)).fetchone()['total_expenses']
    
    # KPI 3: Gross Profit Margin
    gross_profit = revenue - expenses
    profit_margin = (gross_profit / revenue * 100) if revenue > 0 else 0
    
    # KPI 4: Total Accounts Payable (Open & Due)
    ap_open_params = []
    ap_open_query = '''
        SELECT COALESCE(SUM(total_amount - COALESCE(amount_paid, 0)), 0) as ap_open
        FROM vendor_invoices
        WHERE status NOT IN ('Paid', 'Cancelled')
    '''
    if vendor_filter:
        ap_open_query += ' AND vendor_id = ?'
        ap_open_params.append(vendor_filter)
    
    ap_open = conn.execute(ap_open_query, ap_open_params).fetchone()['ap_open']
    
    ap_due_params = [today.strftime('%Y-%m-%d')]
    ap_due_query = '''
        SELECT COALESCE(SUM(total_amount - COALESCE(amount_paid, 0)), 0) as ap_due
        FROM vendor_invoices
        WHERE status NOT IN ('Paid', 'Cancelled')
        AND due_date <= ?
    '''
    if vendor_filter:
        ap_due_query += ' AND vendor_id = ?'
        ap_due_params.append(vendor_filter)
    
    ap_due = conn.execute(ap_due_query, ap_due_params).fetchone()['ap_due']
    
    # KPI 5: Cash on Hand (from GL cash accounts)
    cash_query = '''
        SELECT COALESCE(SUM(gll.debit - gll.credit), 0) as cash_balance
        FROM gl_entry_lines gll
        JOIN chart_of_accounts coa ON gll.account_id = coa.id
        JOIN gl_entries ge ON gll.gl_entry_id = ge.id
        WHERE coa.account_type = 'Asset'
        AND coa.account_name LIKE '%Cash%'
        AND ge.status = 'Posted'
    '''
    cash_balance = conn.execute(cash_query).fetchone()['cash_balance']
    
    # KPI 6: Net Income
    net_income = revenue - expenses
    
    # KPI 7: Inventory Value (uses inventory unit_cost if set, otherwise product cost)
    inventory_value_query = '''
        SELECT COALESCE(SUM(i.quantity * COALESCE(i.unit_cost, p.cost, 0)), 0) as inventory_value
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.quantity > 0
    '''
    inventory_value = conn.execute(inventory_value_query).fetchone()['inventory_value']
    
    # Period-based KPIs
    # KPI 8: A/P Payments Made During Period (from vendor_invoices paid during period)
    ap_payments_params = [start_date, end_date]
    ap_payments_query = '''
        SELECT COALESCE(SUM(vi.amount_paid), 0) as payments_made
        FROM vendor_invoices vi
        WHERE vi.payment_date BETWEEN ? AND ?
        AND vi.status = 'Paid'
    '''
    if vendor_filter:
        ap_payments_query += ' AND vi.vendor_id = ?'
        ap_payments_params.append(vendor_filter)
    ap_payments = conn.execute(ap_payments_query, ap_payments_params).fetchone()['payments_made']
    
    # KPI 9: Invoices Billed During Period (Customer Revenue)
    ar_billed_query = '''
        SELECT COALESCE(SUM(total_amount), 0) as total_billed,
               COUNT(*) as invoice_count
        FROM invoices
        WHERE invoice_date BETWEEN ? AND ?
        AND status != 'Cancelled'
    '''
    ar_billed_result = conn.execute(ar_billed_query, (start_date, end_date)).fetchone()
    ar_billed = ar_billed_result['total_billed']
    ar_invoice_count = ar_billed_result['invoice_count']
    
    # KPI 10: A/R Collections During Period (from payments table)
    ar_collections_query = '''
        SELECT COALESCE(SUM(p.amount), 0) as collections
        FROM payments p
        WHERE p.payment_date BETWEEN ? AND ?
        AND p.reference_type = 'invoice'
    '''
    ar_collections = conn.execute(ar_collections_query, (start_date, end_date)).fetchone()['collections']
    
    # KPI 11: Purchase Orders Created During Period
    po_created_params = [start_date, end_date]
    po_created_query = '''
        SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) as total_ordered,
               COUNT(DISTINCT po.id) as po_count
        FROM purchase_orders po
        LEFT JOIN purchase_order_lines pol ON po.id = pol.po_id
        WHERE po.order_date BETWEEN ? AND ?
        AND po.status != 'Cancelled'
    '''
    if vendor_filter:
        po_created_query += ' AND po.supplier_id = ?'
        po_created_params.append(vendor_filter)
    po_result = conn.execute(po_created_query, po_created_params).fetchone()
    po_ordered = po_result['total_ordered'] or 0
    po_count = po_result['po_count'] or 0
    
    # KPI 12: Work Orders Completed During Period
    wo_completed_query = '''
        SELECT COUNT(*) as wo_count,
               COALESCE(SUM(material_cost + labor_cost + overhead_cost), 0) as total_cost
        FROM work_orders
        WHERE actual_end_date BETWEEN ? AND ?
        AND status = 'Completed'
    '''
    wo_result = conn.execute(wo_completed_query, (start_date, end_date)).fetchone()
    wo_completed_count = wo_result['wo_count']
    wo_completed_cost = wo_result['total_cost']
    
    # Chart Data: Revenue vs Expense Trend (Last 12 months)
    twelve_months_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    
    if USE_POSTGRES:
        trend_query = '''
            WITH months AS (
                SELECT DISTINCT TO_CHAR(ge.entry_date, 'YYYY-MM') as month
                FROM gl_entries ge
                WHERE ge.entry_date >= ?
            )
            SELECT 
                m.month,
                COALESCE((
                    SELECT SUM(gll.credit - gll.debit)
                    FROM gl_entry_lines gll
                    JOIN gl_entries ge ON gll.gl_entry_id = ge.id
                    JOIN chart_of_accounts coa ON gll.account_id = coa.id
                    WHERE coa.account_type = 'Revenue'
                    AND TO_CHAR(ge.entry_date, 'YYYY-MM') = m.month
                    AND ge.status = 'Posted'
                ), 0) as revenue,
                COALESCE((
                    SELECT SUM(gll.debit - gll.credit)
                    FROM gl_entry_lines gll
                    JOIN gl_entries ge ON gll.gl_entry_id = ge.id
                    JOIN chart_of_accounts coa ON gll.account_id = coa.id
                    WHERE coa.account_type = 'Expense'
                    AND TO_CHAR(ge.entry_date, 'YYYY-MM') = m.month
                    AND ge.status = 'Posted'
                ), 0) as expenses
            FROM months m
            GROUP BY m.month
            ORDER BY m.month
        '''
        trend_data = conn.execute(trend_query, (twelve_months_ago,)).fetchall()
    else:
        trend_query = '''
            WITH months AS (
                SELECT DISTINCT strftime('%Y-%m', ge.entry_date) as month
                FROM gl_entries ge
                WHERE ge.entry_date >= date('now', '-12 months')
            )
            SELECT 
                m.month,
                COALESCE((
                    SELECT SUM(gll.credit - gll.debit)
                    FROM gl_entry_lines gll
                    JOIN gl_entries ge ON gll.gl_entry_id = ge.id
                    JOIN chart_of_accounts coa ON gll.account_id = coa.id
                    WHERE coa.account_type = 'Revenue'
                    AND strftime('%Y-%m', ge.entry_date) = m.month
                    AND ge.status = 'Posted'
                ), 0) as revenue,
                COALESCE((
                    SELECT SUM(gll.debit - gll.credit)
                    FROM gl_entry_lines gll
                    JOIN gl_entries ge ON gll.gl_entry_id = ge.id
                    JOIN chart_of_accounts coa ON gll.account_id = coa.id
                    WHERE coa.account_type = 'Expense'
                    AND strftime('%Y-%m', ge.entry_date) = m.month
                    AND ge.status = 'Posted'
                ), 0) as expenses
            FROM months m
            GROUP BY m.month
            ORDER BY m.month
        '''
        trend_data = conn.execute(trend_query).fetchall()
    
    # Chart Data: A/P Aging
    ap_aging_params = []
    if USE_POSTGRES:
        ap_aging_query = '''
            SELECT 
                CASE 
                    WHEN CURRENT_DATE - due_date <= 0 THEN 'Current'
                    WHEN CURRENT_DATE - due_date BETWEEN 1 AND 30 THEN '1-30 Days'
                    WHEN CURRENT_DATE - due_date BETWEEN 31 AND 60 THEN '31-60 Days'
                    WHEN CURRENT_DATE - due_date BETWEEN 61 AND 90 THEN '61-90 Days'
                    ELSE '90+ Days'
                END as aging_bucket,
                COALESCE(SUM(total_amount - COALESCE(amount_paid, 0)), 0) as amount
            FROM vendor_invoices
            WHERE status NOT IN ('Paid', 'Cancelled')
        '''
    else:
        ap_aging_query = '''
            SELECT 
                CASE 
                    WHEN julianday('now') - julianday(due_date) <= 0 THEN 'Current'
                    WHEN julianday('now') - julianday(due_date) BETWEEN 1 AND 30 THEN '1-30 Days'
                    WHEN julianday('now') - julianday(due_date) BETWEEN 31 AND 60 THEN '31-60 Days'
                    WHEN julianday('now') - julianday(due_date) BETWEEN 61 AND 90 THEN '61-90 Days'
                    ELSE '90+ Days'
                END as aging_bucket,
                COALESCE(SUM(total_amount - COALESCE(amount_paid, 0)), 0) as amount
            FROM vendor_invoices
            WHERE status NOT IN ('Paid', 'Cancelled')
        '''
    if vendor_filter:
        ap_aging_query += ' AND vendor_id = ?'
        ap_aging_params.append(vendor_filter)
    
    ap_aging_query += '''
        GROUP BY 1
        ORDER BY 
            CASE 
                WHEN (CASE WHEN CURRENT_DATE - due_date <= 0 THEN 'Current'
                          WHEN CURRENT_DATE - due_date BETWEEN 1 AND 30 THEN '1-30 Days'
                          WHEN CURRENT_DATE - due_date BETWEEN 31 AND 60 THEN '31-60 Days'
                          WHEN CURRENT_DATE - due_date BETWEEN 61 AND 90 THEN '61-90 Days'
                          ELSE '90+ Days' END) = 'Current' THEN 1
                WHEN (CASE WHEN CURRENT_DATE - due_date <= 0 THEN 'Current'
                          WHEN CURRENT_DATE - due_date BETWEEN 1 AND 30 THEN '1-30 Days'
                          WHEN CURRENT_DATE - due_date BETWEEN 31 AND 60 THEN '31-60 Days'
                          WHEN CURRENT_DATE - due_date BETWEEN 61 AND 90 THEN '61-90 Days'
                          ELSE '90+ Days' END) = '1-30 Days' THEN 2
                ELSE 3
            END
    '''
    ap_aging = conn.execute(ap_aging_query, ap_aging_params).fetchall()
    
    # Chart Data: Top 10 Vendors by Spend
    top_vendors_params = [start_date, end_date]
    top_vendors_query = '''
        SELECT 
            s.name as vendor_name,
            COALESCE(SUM(vi.total_amount), 0) as total_spend
        FROM suppliers s
        LEFT JOIN vendor_invoices vi ON s.id = vi.vendor_id
        WHERE vi.invoice_date BETWEEN ? AND ?
    '''
    if vendor_filter:
        top_vendors_query += ' AND s.id = ?'
        top_vendors_params.append(vendor_filter)
    
    top_vendors_query += '''
        GROUP BY s.id, s.name
        ORDER BY total_spend DESC
        LIMIT 10
    '''
    top_vendors = conn.execute(top_vendors_query, top_vendors_params).fetchall()
    
    # Chart Data: Work Order Cost Analysis (Top 10 by cost)
    top_workorders_query = '''
        SELECT 
            wo.wo_number,
            p.name as product_name,
            (wo.material_cost + wo.labor_cost + wo.overhead_cost) as total_cost
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.status = 'Completed'
        AND wo.actual_end_date BETWEEN ? AND ?
        ORDER BY total_cost DESC
        LIMIT 10
    '''
    top_workorders = conn.execute(top_workorders_query, (start_date, end_date)).fetchall()
    
    # Get vendor list for filter dropdown
    vendors = conn.execute('SELECT id, name FROM suppliers ORDER BY name').fetchall()
    
    conn.close()
    
    # Prepare data for charts
    trend_labels = [row['month'] for row in trend_data]
    trend_revenue = [float(row['revenue']) for row in trend_data]
    trend_expenses = [float(row['expenses']) for row in trend_data]
    
    ap_aging_labels = [row['aging_bucket'] for row in ap_aging]
    ap_aging_amounts = [float(row['amount']) for row in ap_aging]
    
    vendor_labels = [row['vendor_name'] for row in top_vendors]
    vendor_amounts = [float(row['total_spend']) for row in top_vendors]
    
    wo_labels = [f"{row['wo_number']}: {row['product_name'][:20]}" for row in top_workorders]
    wo_amounts = [float(row['total_cost']) for row in top_workorders]
    
    return render_template('executive/dashboard.html',
                         revenue=revenue,
                         expenses=expenses,
                         gross_profit=gross_profit,
                         profit_margin=profit_margin,
                         ap_open=ap_open,
                         ap_due=ap_due,
                         cash_balance=cash_balance,
                         net_income=net_income,
                         inventory_value=inventory_value,
                         ap_payments=ap_payments,
                         ar_billed=ar_billed,
                         ar_invoice_count=ar_invoice_count,
                         ar_collections=ar_collections,
                         po_ordered=po_ordered,
                         po_count=po_count,
                         wo_completed_count=wo_completed_count,
                         wo_completed_cost=wo_completed_cost,
                         period_label=period_label,
                         date_filter=date_filter,
                         vendor_filter=vendor_filter,
                         trend_labels=trend_labels,
                         trend_revenue=trend_revenue,
                         trend_expenses=trend_expenses,
                         ap_aging_labels=ap_aging_labels,
                         ap_aging_amounts=ap_aging_amounts,
                         vendor_labels=vendor_labels,
                         vendor_amounts=vendor_amounts,
                         wo_labels=wo_labels,
                         wo_amounts=wo_amounts,
                         vendors=vendors,
                         last_updated=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

@executive_routes.route('/executive-dashboard/export')
@role_required('Admin', 'Accountant')
def export_dashboard():
    db = Database()
    conn = db.get_connection()
    
    date_filter = request.args.get('date_range', 'ytd')
    vendor_filter = request.args.get('vendor_id', '')
    today = datetime.now()
    current_year = today.year
    current_month = today.month
    
    if date_filter == 'wtd':
        days_since_monday = today.weekday()
        start_date = (today - timedelta(days=days_since_monday)).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
    elif date_filter == 'mtd':
        start_date = datetime(current_year, current_month, 1).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
    elif date_filter == 'qtd':
        quarter_start_month = ((current_month - 1) // 3) * 3 + 1
        start_date = datetime(current_year, quarter_start_month, 1).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
    else:  # ytd
        start_date = datetime(current_year, 1, 1).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
    
    # Get summary data
    revenue = conn.execute('''
        SELECT COALESCE(SUM(gll.credit - gll.debit), 0) as total_revenue
        FROM gl_entry_lines gll
        JOIN gl_entries ge ON gll.gl_entry_id = ge.id
        JOIN chart_of_accounts coa ON gll.account_id = coa.id
        WHERE coa.account_type = 'Revenue'
        AND ge.entry_date BETWEEN ? AND ?
        AND ge.status = 'Posted'
    ''', (start_date, end_date)).fetchone()['total_revenue']
    
    expenses = conn.execute('''
        SELECT COALESCE(SUM(gll.debit - gll.credit), 0) as total_expenses
        FROM gl_entry_lines gll
        JOIN gl_entries ge ON gll.gl_entry_id = ge.id
        JOIN chart_of_accounts coa ON gll.account_id = coa.id
        WHERE coa.account_type = 'Expense'
        AND ge.entry_date BETWEEN ? AND ?
        AND ge.status = 'Posted'
    ''', (start_date, end_date)).fetchone()['total_expenses']
    
    ap_params = []
    ap_query = '''
        SELECT COALESCE(SUM(total_amount - COALESCE(amount_paid, 0)), 0) as ap_open
        FROM vendor_invoices
        WHERE status NOT IN ('Paid', 'Cancelled')
    '''
    if vendor_filter:
        ap_query += ' AND vendor_id = ?'
        ap_params.append(vendor_filter)
    
    ap_open = conn.execute(ap_query, ap_params).fetchone()['ap_open']
    
    # Get vendor name if filtered
    vendor_name = 'All Vendors'
    if vendor_filter:
        vendor_row = conn.execute('SELECT name FROM suppliers WHERE id = ?', (vendor_filter,)).fetchone()
        if vendor_row:
            vendor_name = vendor_row['name']
    
    conn.close()
    
    # Create CSV
    output = StringIO()
    writer = csv.writer(output)
    
    writer.writerow(['Executive Dashboard Summary'])
    writer.writerow(['Generated:', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
    writer.writerow(['Period:', date_filter.upper()])
    writer.writerow(['Vendor Filter:', vendor_name])
    writer.writerow([])
    writer.writerow(['Metric', 'Value'])
    writer.writerow(['Total Revenue', f'${revenue:,.2f}'])
    writer.writerow(['Total Expenses', f'${expenses:,.2f}'])
    writer.writerow(['Gross Profit', f'${revenue - expenses:,.2f}'])
    writer.writerow(['Profit Margin', f'{(revenue - expenses) / revenue * 100:.2f}%' if revenue > 0 else '0.00%'])
    writer.writerow(['Accounts Payable (Open)', f'${ap_open:,.2f}'])
    
    response = make_response(output.getvalue())
    filename_suffix = f'_{vendor_name.replace(" ", "_")}' if vendor_filter else ''
    response.headers['Content-Disposition'] = f'attachment; filename=executive_dashboard_{date_filter}{filename_suffix}_{today.strftime("%Y%m%d")}.csv'
    response.headers['Content-Type'] = 'text/csv'
    
    return response
