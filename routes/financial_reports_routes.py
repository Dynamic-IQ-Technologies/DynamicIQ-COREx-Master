from flask import Blueprint, render_template, request
from models import Database
from auth import login_required, role_required
from datetime import datetime

financial_reports_bp = Blueprint('financial_reports_routes', __name__)

@financial_reports_bp.route('/reports/trial-balance')
@login_required
@role_required('Admin', 'Accountant')
def trial_balance():
    db = Database()
    conn = db.get_connection()
    
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    
    # Get all posted GL entries up to the end date
    accounts = conn.execute('''
        SELECT 
            coa.id,
            coa.account_code,
            coa.account_name,
            coa.account_type,
            COALESCE(SUM(gl_lines.debit), 0) as total_debit,
            COALESCE(SUM(gl_lines.credit), 0) as total_credit
        FROM chart_of_accounts coa
        LEFT JOIN gl_entry_lines gl_lines ON coa.id = gl_lines.account_id
        LEFT JOIN gl_entries gl ON gl_lines.gl_entry_id = gl.id
        WHERE coa.is_active = 1 
        AND (gl.status = 'Posted' OR gl.id IS NULL)
        AND (gl.entry_date <= ? OR gl.id IS NULL)
        GROUP BY coa.id, coa.account_code, coa.account_name, coa.account_type
        HAVING (COALESCE(SUM(gl_lines.debit), 0) + COALESCE(SUM(gl_lines.credit), 0)) > 0
        ORDER BY coa.account_code
    ''', (end_date,)).fetchall()
    
    total_debits = sum(acc['total_debit'] for acc in accounts)
    total_credits = sum(acc['total_credit'] for acc in accounts)
    
    conn.close()
    
    return render_template('accounting/trial_balance.html',
                         accounts=accounts,
                         total_debits=total_debits,
                         total_credits=total_credits,
                         end_date=end_date)

@financial_reports_bp.route('/reports/balance-sheet')
@login_required
@role_required('Admin', 'Accountant')
def balance_sheet():
    db = Database()
    conn = db.get_connection()
    
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    
    # Get balances for all accounts
    balances = conn.execute('''
        SELECT 
            coa.account_code,
            coa.account_name,
            coa.account_type,
            COALESCE(SUM(gl_lines.debit), 0) as total_debit,
            COALESCE(SUM(gl_lines.credit), 0) as total_credit
        FROM chart_of_accounts coa
        LEFT JOIN gl_entry_lines gl_lines ON coa.id = gl_lines.account_id
        LEFT JOIN gl_entries gl ON gl_lines.gl_entry_id = gl.id
        WHERE coa.is_active = 1 
        AND (gl.status = 'Posted' OR gl.id IS NULL)
        AND (gl.entry_date <= ? OR gl.id IS NULL)
        GROUP BY coa.id, coa.account_code, coa.account_name, coa.account_type
        ORDER BY coa.account_code
    ''', (end_date,)).fetchall()
    
    # Calculate account balances based on normal balance
    assets = []
    liabilities = []
    equity = []
    
    total_assets = 0
    total_liabilities = 0
    total_equity = 0
    
    for balance in balances:
        if balance['account_type'] == 'Asset':
            net_balance = balance['total_debit'] - balance['total_credit']
            if net_balance != 0:
                assets.append({
                    'code': balance['account_code'],
                    'name': balance['account_name'],
                    'balance': net_balance
                })
                total_assets += net_balance
        elif balance['account_type'] == 'Liability':
            net_balance = balance['total_credit'] - balance['total_debit']
            if net_balance != 0:
                liabilities.append({
                    'code': balance['account_code'],
                    'name': balance['account_name'],
                    'balance': net_balance
                })
                total_liabilities += net_balance
        elif balance['account_type'] == 'Equity':
            net_balance = balance['total_credit'] - balance['total_debit']
            if net_balance != 0:
                equity.append({
                    'code': balance['account_code'],
                    'name': balance['account_name'],
                    'balance': net_balance
                })
                total_equity += net_balance
    
    conn.close()
    
    return render_template('accounting/balance_sheet.html',
                         assets=assets,
                         liabilities=liabilities,
                         equity=equity,
                         total_assets=total_assets,
                         total_liabilities=total_liabilities,
                         total_equity=total_equity,
                         end_date=end_date)

@financial_reports_bp.route('/reports/income-statement')
@login_required
@role_required('Admin', 'Accountant')
def income_statement():
    db = Database()
    conn = db.get_connection()
    
    start_date = request.args.get('start_date', f'{datetime.now().year}-01-01')
    end_date = request.args.get('end_date', datetime.now().strftime('%Y-%m-%d'))
    
    # Get balances for revenue and expense accounts
    balances = conn.execute('''
        SELECT 
            coa.account_code,
            coa.account_name,
            coa.account_type,
            COALESCE(SUM(gl_lines.debit), 0) as total_debit,
            COALESCE(SUM(gl_lines.credit), 0) as total_credit
        FROM chart_of_accounts coa
        LEFT JOIN gl_entry_lines gl_lines ON coa.id = gl_lines.account_id
        LEFT JOIN gl_entries gl ON gl_lines.gl_entry_id = gl.id
        WHERE coa.is_active = 1 
        AND (gl.status = 'Posted' OR gl.id IS NULL)
        AND (gl.entry_date BETWEEN ? AND ? OR gl.id IS NULL)
        AND coa.account_type IN ('Revenue', 'Expense')
        GROUP BY coa.id, coa.account_code, coa.account_name, coa.account_type
        ORDER BY coa.account_code
    ''', (start_date, end_date)).fetchall()
    
    revenues = []
    expenses = []
    
    total_revenue = 0
    total_expense = 0
    
    for balance in balances:
        net_balance = balance['total_credit'] - balance['total_debit']
        
        if balance['account_type'] == 'Revenue':
            if net_balance != 0:
                revenues.append({
                    'code': balance['account_code'],
                    'name': balance['account_name'],
                    'balance': net_balance
                })
                total_revenue += net_balance
        elif balance['account_type'] == 'Expense':
            expense_balance = balance['total_debit'] - balance['total_credit']
            if expense_balance != 0:
                expenses.append({
                    'code': balance['account_code'],
                    'name': balance['account_name'],
                    'balance': expense_balance
                })
                total_expense += expense_balance
    
    net_income = total_revenue - total_expense
    
    conn.close()
    
    return render_template('accounting/income_statement.html',
                         revenues=revenues,
                         expenses=expenses,
                         total_revenue=total_revenue,
                         total_expense=total_expense,
                         net_income=net_income,
                         start_date=start_date,
                         end_date=end_date)
