from flask import Blueprint, render_template, session, redirect, url_for, flash, request, jsonify
from functools import wraps
from models import Database
from datetime import datetime, timedelta
import csv
from io import StringIO
from flask import make_response

executive_routes = Blueprint('executive_routes', __name__)

def role_required(*roles):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user_id' not in session:
                flash('Please log in to access this page.', 'warning')
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
    
    if date_filter == 'mtd':
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
    
    # KPI 1: Total Revenue (from sales/production completions)
    # Revenue = Finished goods value from completed work orders
    revenue_query = '''
        SELECT COALESCE(SUM(wo.material_cost + wo.labor_cost + wo.overhead_cost), 0) as total_revenue
        FROM work_orders wo
        WHERE wo.status = 'Completed'
        AND wo.actual_end_date BETWEEN ? AND ?
    '''
    revenue = conn.execute(revenue_query, (start_date, end_date)).fetchone()['total_revenue']
    
    # KPI 2: Total Expenses (from GL expense accounts)
    expense_query = '''
        SELECT COALESCE(SUM(ABS(gll.debit_amount - gll.credit_amount)), 0) as total_expenses
        FROM gl_lines gll
        JOIN gl_entries ge ON gll.gl_entry_id = ge.id
        JOIN chart_of_accounts coa ON gll.account_id = coa.id
        WHERE coa.account_type IN ('Operating Expense', 'Cost of Goods Sold')
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
        SELECT COALESCE(SUM(gll.debit_amount - gll.credit_amount), 0) as cash_balance
        FROM gl_lines gll
        JOIN chart_of_accounts coa ON gll.account_id = coa.id
        JOIN gl_entries ge ON gll.gl_entry_id = ge.id
        WHERE coa.account_type = 'Asset'
        AND coa.account_name LIKE '%Cash%'
        AND ge.status = 'Posted'
    '''
    cash_balance = conn.execute(cash_query).fetchone()['cash_balance']
    
    # KPI 6: Net Income
    net_income = revenue - expenses
    
    # KPI 7: Inventory Value
    inventory_value_query = '''
        SELECT COALESCE(SUM(i.quantity * p.unit_cost), 0) as inventory_value
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.quantity > 0
    '''
    inventory_value = conn.execute(inventory_value_query).fetchone()['inventory_value']
    
    # Chart Data: Revenue vs Expense Trend (Last 12 months)
    trend_query = '''
        WITH months AS (
            SELECT DISTINCT strftime('%Y-%m', wo.actual_end_date) as month
            FROM work_orders wo
            WHERE wo.actual_end_date >= date('now', '-12 months')
            UNION
            SELECT DISTINCT strftime('%Y-%m', ge.entry_date) as month
            FROM gl_entries ge
            WHERE ge.entry_date >= date('now', '-12 months')
        )
        SELECT 
            m.month,
            COALESCE(SUM(wo.material_cost + wo.labor_cost + wo.overhead_cost), 0) as revenue,
            COALESCE((
                SELECT SUM(ABS(gll.debit_amount - gll.credit_amount))
                FROM gl_lines gll
                JOIN gl_entries ge ON gll.gl_entry_id = ge.id
                JOIN chart_of_accounts coa ON gll.account_id = coa.id
                WHERE coa.account_type IN ('Operating Expense', 'Cost of Goods Sold')
                AND strftime('%Y-%m', ge.entry_date) = m.month
                AND ge.status = 'Posted'
            ), 0) as expenses
        FROM months m
        LEFT JOIN work_orders wo ON strftime('%Y-%m', wo.actual_end_date) = m.month
            AND wo.status = 'Completed'
        GROUP BY m.month
        ORDER BY m.month
    '''
    trend_data = conn.execute(trend_query).fetchall()
    
    # Chart Data: A/P Aging
    ap_aging_params = []
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
        GROUP BY aging_bucket
        ORDER BY 
            CASE aging_bucket
                WHEN 'Current' THEN 1
                WHEN '1-30 Days' THEN 2
                WHEN '31-60 Days' THEN 3
                WHEN '61-90 Days' THEN 4
                ELSE 5
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
    
    if date_filter == 'mtd':
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
        SELECT COALESCE(SUM(wo.material_cost + wo.labor_cost + wo.overhead_cost), 0) as total_revenue
        FROM work_orders wo
        WHERE wo.status = 'Completed'
        AND wo.actual_end_date BETWEEN ? AND ?
    ''', (start_date, end_date)).fetchone()['total_revenue']
    
    expenses = conn.execute('''
        SELECT COALESCE(SUM(ABS(gll.debit_amount - gll.credit_amount)), 0) as total_expenses
        FROM gl_lines gll
        JOIN gl_entries ge ON gll.gl_entry_id = ge.id
        JOIN chart_of_accounts coa ON gll.account_id = coa.id
        WHERE coa.account_type IN ('Operating Expense', 'Cost of Goods Sold')
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
