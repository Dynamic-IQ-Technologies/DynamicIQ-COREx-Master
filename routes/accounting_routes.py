from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database
from auth import login_required, role_required
import logging

accounting_bp = Blueprint('accounting_routes', __name__)
logger = logging.getLogger(__name__)

@accounting_bp.route('/chart-of-accounts')
@login_required
@role_required('Admin', 'Accountant')
def list_accounts():
    db = Database()
    conn = db.get_connection()
    
    try:
        accounts = conn.execute('''
            SELECT * FROM chart_of_accounts 
            ORDER BY account_code
        ''').fetchall()
        
        logger.info(f"[Chart of Accounts] Fetched {len(accounts)} accounts")
        if len(accounts) == 0:
            logger.warning("[Chart of Accounts] No accounts found - checking database directly")
            count_result = conn.execute('SELECT COUNT(*) as cnt FROM chart_of_accounts').fetchone()
            logger.warning(f"[Chart of Accounts] Direct count: {count_result}")
    except Exception as e:
        logger.error(f"[Chart of Accounts] Error fetching accounts: {str(e)}")
        accounts = []
    finally:
        conn.close()
    
    return render_template('accounting/chart_of_accounts.html', accounts=accounts)

@accounting_bp.route('/chart-of-accounts/create', methods=['GET', 'POST'])
@role_required('Admin')
def create_account():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            account_code = request.form['account_code']
            account_name = request.form['account_name']
            account_type = request.form['account_type']
            description = request.form.get('description', '')
            parent_id = request.form.get('parent_account_id', '')
            
            parent_account_id = int(parent_id) if parent_id else None
            
            conn.execute('''
                INSERT INTO chart_of_accounts (account_code, account_name, account_type, parent_account_id, description, is_active)
                VALUES (?, ?, ?, ?, ?, 1)
            ''', (account_code, account_name, account_type, parent_account_id, description))
            
            conn.commit()
            flash('Account created successfully!', 'success')
            return redirect(url_for('accounting_routes.list_accounts'))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error creating account: {str(e)}', 'danger')
        finally:
            conn.close()
    
    accounts = conn.execute('SELECT * FROM chart_of_accounts WHERE is_active = 1 ORDER BY account_code').fetchall()
    conn.close()
    
    return render_template('accounting/create_account.html', accounts=accounts)

@accounting_bp.route('/chart-of-accounts/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin')
def edit_account(id):
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            account_name = request.form['account_name']
            description = request.form.get('description', '')
            is_active = 1 if request.form.get('is_active') == 'on' else 0
            parent_id = request.form.get('parent_account_id', '')
            
            parent_account_id = int(parent_id) if parent_id else None
            
            conn.execute('''
                UPDATE chart_of_accounts 
                SET account_name = ?, parent_account_id = ?, description = ?, is_active = ?
                WHERE id = ?
            ''', (account_name, parent_account_id, description, is_active, id))
            
            conn.commit()
            flash('Account updated successfully!', 'success')
            return redirect(url_for('accounting_routes.list_accounts'))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error updating account: {str(e)}', 'danger')
        finally:
            conn.close()
    
    account = conn.execute('SELECT * FROM chart_of_accounts WHERE id = ?', (id,)).fetchone()
    accounts = conn.execute('SELECT * FROM chart_of_accounts WHERE is_active = 1 AND id != ? ORDER BY account_code', (id,)).fetchall()
    conn.close()
    
    return render_template('accounting/edit_account.html', account=account, accounts=accounts)

@accounting_bp.route('/general-ledger')
@login_required
@role_required('Admin', 'Accountant')
def general_ledger():
    db = Database()
    conn = db.get_connection()
    
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    account_id = request.args.get('account_id', '')
    
    query = '''
        SELECT 
            gl.entry_number,
            gl.entry_date,
            gl.description,
            gl.transaction_source,
            coa.account_code,
            coa.account_name,
            gl_lines.debit,
            gl_lines.credit,
            gl.status
        FROM gl_entry_lines gl_lines
        JOIN gl_entries gl ON gl_lines.gl_entry_id = gl.id
        JOIN chart_of_accounts coa ON gl_lines.account_id = coa.id
        WHERE 1=1
    '''
    
    params = []
    
    if start_date:
        query += ' AND gl.entry_date >= ?'
        params.append(start_date)
    
    if end_date:
        query += ' AND gl.entry_date <= ?'
        params.append(end_date)
    
    if account_id:
        query += ' AND coa.id = ?'
        params.append(account_id)
    
    query += ' ORDER BY gl.entry_date DESC, gl.entry_number DESC'
    
    entries = conn.execute(query, params).fetchall()
    accounts = conn.execute('SELECT * FROM chart_of_accounts WHERE is_active = 1 ORDER BY account_code').fetchall()
    
    conn.close()
    
    return render_template('accounting/general_ledger.html', 
                         entries=entries, 
                         accounts=accounts,
                         start_date=start_date,
                         end_date=end_date,
                         account_id=account_id)

@accounting_bp.route('/revenue-tracker/department/<dept>')
@login_required
@role_required('Admin', 'Accountant')
def department_transactions(dept):
    """View detailed transactions for a specific department"""
    db = Database()
    conn = db.get_connection()
    
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    
    transactions = []
    dept_name = ''
    dept_color = ''
    total_revenue = 0
    total_cost = 0
    
    date_filter = ""
    params = []
    if start_date:
        date_filter += " AND {date_col} >= ?"
        params.append(start_date)
    if end_date:
        date_filter += " AND {date_col} <= ?"
        params.append(end_date)
    
    if dept == 'sales':
        dept_name = 'Sales'
        dept_color = '#28a745'
        
        rows = conn.execute(f'''
            SELECT 
                so.id,
                so.so_number as reference,
                so.order_date as trans_date,
                c.name as customer_name,
                so.total_amount as revenue,
                COALESCE((
                    SELECT SUM(sol.quantity * COALESCE(p.cost, 0))
                    FROM sales_order_lines sol
                    LEFT JOIN products p ON sol.product_id = p.id
                    WHERE sol.so_id = so.id
                ), 0) as cost,
                so.status
            FROM sales_orders so
            LEFT JOIN customers c ON so.customer_id = c.id
            WHERE so.status NOT IN ('Cancelled', 'Draft') {date_filter.format(date_col='so.order_date')}
            ORDER BY so.order_date DESC
        ''', params).fetchall()
        
        for r in rows:
            rev = float(r['revenue'] or 0)
            cost = float(r['cost'] or 0)
            transactions.append({
                'id': r['id'],
                'type': 'Sales Order',
                'reference': r['reference'],
                'date': r['trans_date'],
                'description': r['customer_name'] or 'N/A',
                'revenue': rev,
                'cost': cost,
                'profit': rev - cost,
                'status': r['status'],
                'link': f"/sales-orders/{r['id']}"
            })
            total_revenue += rev
            total_cost += cost
            
    elif dept == 'operations':
        dept_name = 'Operations'
        dept_color = '#007bff'
        
        rows = conn.execute(f'''
            SELECT 
                wo.id,
                wo.wo_number as reference,
                wo.created_at as trans_date,
                p.name as product_name,
                COALESCE(i.total_amount, 0) as revenue,
                COALESCE(wo.material_cost, 0) + COALESCE(wo.labor_cost, 0) + COALESCE(wo.overhead_cost, 0) as cost,
                wo.status
            FROM work_orders wo
            LEFT JOIN products p ON wo.product_id = p.id
            LEFT JOIN invoices i ON i.wo_id = wo.id AND i.status NOT IN ('Cancelled', 'Draft')
            WHERE wo.status NOT IN ('Cancelled') {date_filter.format(date_col='wo.created_at')}
            ORDER BY wo.created_at DESC
        ''', params).fetchall()
        
        for r in rows:
            rev = float(r['revenue'] or 0)
            cost = float(r['cost'] or 0)
            transactions.append({
                'id': r['id'],
                'type': 'Work Order',
                'reference': r['reference'],
                'date': str(r['trans_date'])[:10] if r['trans_date'] else '',
                'description': r['product_name'] or 'N/A',
                'revenue': rev,
                'cost': cost,
                'profit': rev - cost,
                'status': r['status'],
                'link': f"/work-orders/{r['id']}"
            })
            total_revenue += rev
            total_cost += cost
            
    elif dept == 'ndt':
        dept_name = 'NDT'
        dept_color = '#6f42c1'
        
        rows = conn.execute(f'''
            SELECT 
                nwo.id,
                nwo.ndt_wo_number as reference,
                nwo.created_at as trans_date,
                nwo.part_description as part_number,
                COALESCE(i.total_amount, 0) + COALESCE(ni.total_amount, 0) as revenue,
                COALESCE(c.total_cost, 0) as cost,
                nwo.status
            FROM ndt_work_orders nwo
            LEFT JOIN invoices i ON i.source_type = 'ndt_work_order' AND i.source_id = nwo.id AND i.status NOT IN ('Cancelled', 'Draft')
            LEFT JOIN ndt_invoices ni ON ni.ndt_wo_id = nwo.id AND ni.status NOT IN ('Cancelled', 'Void')
            LEFT JOIN (
                SELECT ndt_wo_id, SUM(total_cost) as total_cost FROM ndt_wo_costs GROUP BY ndt_wo_id
            ) c ON c.ndt_wo_id = nwo.id
            WHERE 1=1 {date_filter.format(date_col='nwo.created_at')}
            ORDER BY nwo.created_at DESC
        ''', params).fetchall()
        
        for r in rows:
            rev = float(r['revenue'] or 0)
            cost = float(r['cost'] or 0)
            transactions.append({
                'id': r['id'],
                'type': 'NDT Work Order',
                'reference': r['reference'],
                'date': str(r['trans_date'])[:10] if r['trans_date'] else '',
                'description': r['part_number'] or 'N/A',
                'revenue': rev,
                'cost': cost,
                'profit': rev - cost,
                'status': r['status'],
                'link': f"/ndt/work-orders/{r['id']}"
            })
            total_revenue += rev
            total_cost += cost
            
    elif dept == 'consulting':
        dept_name = 'Consulting'
        dept_color = '#fd7e14'
        
        rows = conn.execute(f'''
            SELECT 
                swo.id,
                swo.swo_number as reference,
                swo.created_at as trans_date,
                c.name as customer_name,
                swo.service_type,
                COALESCE(swo.total_cost, 0) as revenue,
                COALESCE(swo.labor_subtotal, 0) * 0.4 as cost,
                swo.status
            FROM service_work_orders swo
            LEFT JOIN customers c ON swo.customer_id = c.id
            WHERE swo.status NOT IN ('Cancelled') {date_filter.format(date_col='swo.created_at')}
            ORDER BY swo.created_at DESC
        ''', params).fetchall()
        
        for r in rows:
            rev = float(r['revenue'] or 0)
            cost = float(r['cost'] or 0)
            transactions.append({
                'id': r['id'],
                'type': 'Service Work Order',
                'reference': r['reference'],
                'date': str(r['trans_date'])[:10] if r['trans_date'] else '',
                'description': f"{r['customer_name'] or 'N/A'} - {r['service_type'] or ''}",
                'revenue': rev,
                'cost': cost,
                'profit': rev - cost,
                'status': r['status'],
                'link': f"/service-work-orders/{r['id']}"
            })
            total_revenue += rev
            total_cost += cost
    else:
        conn.close()
        flash('Unknown department.', 'danger')
        return redirect(url_for('accounting_routes.revenue_tracker'))
    
    conn.close()
    
    total_profit = total_revenue - total_cost
    margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0
    
    return render_template('accounting/department_transactions.html',
                         dept=dept,
                         dept_name=dept_name,
                         dept_color=dept_color,
                         transactions=transactions,
                         total_revenue=total_revenue,
                         total_cost=total_cost,
                         total_profit=total_profit,
                         margin=margin,
                         start_date=start_date,
                         end_date=end_date)

@accounting_bp.route('/revenue-tracker')
@login_required
@role_required('Admin', 'Accountant')
def revenue_tracker():
    """Revenue and Profitability Tracker by Department"""
    db = Database()
    conn = db.get_connection()
    
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    
    date_filter_sales = ""
    date_filter_wo = ""
    date_filter_ndt = ""
    date_filter_swo = ""
    params_sales = []
    params_wo = []
    params_ndt = []
    params_swo = []
    
    if start_date:
        date_filter_sales += " AND order_date >= ?"
        date_filter_wo += " AND created_at >= ?"
        date_filter_ndt += " AND created_at >= ?"
        date_filter_swo += " AND created_at >= ?"
        params_sales.append(start_date)
        params_wo.append(start_date)
        params_ndt.append(start_date)
        params_swo.append(start_date)
    
    if end_date:
        date_filter_sales += " AND order_date <= ?"
        date_filter_wo += " AND created_at <= ?"
        date_filter_ndt += " AND created_at <= ?"
        date_filter_swo += " AND created_at <= ?"
        params_sales.append(end_date)
        params_wo.append(end_date)
        params_ndt.append(end_date)
        params_swo.append(end_date)
    
    sales_data = conn.execute(f'''
        SELECT 
            COUNT(*) as order_count,
            COALESCE(SUM(total_amount), 0) as total_revenue,
            COALESCE(SUM(amount_paid), 0) as collected,
            COALESCE(SUM(balance_due), 0) as outstanding,
            COALESCE(SUM(discount_amount), 0) as discounts,
            COALESCE(SUM(tax_amount), 0) as taxes,
            COALESCE(SUM(subtotal), 0) as subtotal
        FROM sales_orders
        WHERE status NOT IN ('Cancelled', 'Draft') {date_filter_sales}
    ''', params_sales).fetchone()
    
    # Calculate COGS from product cost * quantity
    sales_cogs = conn.execute(f'''
        SELECT COALESCE(SUM(
            sol.quantity * COALESCE(p.cost, 0)
        ), 0) as cogs
        FROM sales_order_lines sol
        JOIN sales_orders so ON sol.so_id = so.id
        LEFT JOIN products p ON sol.product_id = p.id
        WHERE so.status NOT IN ('Cancelled', 'Draft') {date_filter_sales.replace('order_date', 'so.order_date')}
    ''', params_sales).fetchone()
    
    sales_by_type = conn.execute(f'''
        SELECT 
            sales_type,
            COUNT(*) as count,
            COALESCE(SUM(total_amount), 0) as revenue
        FROM sales_orders
        WHERE status NOT IN ('Cancelled', 'Draft') {date_filter_sales}
        GROUP BY sales_type
        ORDER BY revenue DESC
    ''', params_sales).fetchall()
    
    operations_data = conn.execute(f'''
        SELECT 
            COUNT(*) as order_count,
            COALESCE(SUM(material_cost), 0) as material_cost,
            COALESCE(SUM(labor_cost), 0) as labor_cost,
            COALESCE(SUM(overhead_cost), 0) as overhead_cost,
            COALESCE(SUM(material_cost + labor_cost + overhead_cost), 0) as total_cost
        FROM work_orders
        WHERE status NOT IN ('Cancelled') {date_filter_wo}
    ''', params_wo).fetchone()
    
    wo_revenue = conn.execute(f'''
        SELECT COALESCE(SUM(i.total_amount), 0) as invoiced_revenue
        FROM invoices i
        WHERE (i.source_type IN ('work_order', 'Work Order', 'Service Work Order') OR i.wo_id IS NOT NULL)
        AND i.status NOT IN ('Cancelled', 'Draft') {date_filter_wo.replace('created_at', 'i.created_at')}
    ''', params_wo).fetchone()
    
    wo_by_status = conn.execute(f'''
        SELECT 
            status,
            COUNT(*) as count,
            COALESCE(SUM(material_cost + labor_cost + overhead_cost), 0) as cost
        FROM work_orders
        WHERE status NOT IN ('Cancelled') {date_filter_wo}
        GROUP BY status
        ORDER BY count DESC
    ''', params_wo).fetchall()
    
    ndt_data = conn.execute(f'''
        SELECT 
            COUNT(*) as order_count,
            SUM(CASE WHEN status = 'Approved' THEN 1 ELSE 0 END) as approved_count,
            SUM(CASE WHEN status = 'Rejected' THEN 1 ELSE 0 END) as rejected_count,
            SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END) as closed_count
        FROM ndt_work_orders
        WHERE 1=1 {date_filter_ndt}
    ''', params_ndt).fetchone()
    
    ndt_revenue = conn.execute(f'''
        SELECT COALESCE(SUM(invoiced_revenue), 0) as invoiced_revenue FROM (
            SELECT SUM(i.total_amount) as invoiced_revenue
            FROM invoices i
            WHERE i.source_type = 'ndt_work_order' AND i.status != 'Cancelled' {date_filter_ndt.replace('created_at', 'i.created_at')}
            UNION ALL
            SELECT SUM(ni.total_amount) as invoiced_revenue
            FROM ndt_invoices ni
            WHERE ni.status NOT IN ('Cancelled', 'Void') {date_filter_ndt.replace('created_at', 'ni.invoice_date')}
        )
    ''', params_ndt + params_ndt).fetchone()
    
    ndt_costs = conn.execute(f'''
        SELECT 
            COALESCE(SUM(c.total_cost), 0) as tracked_cost
        FROM ndt_wo_costs c
        JOIN ndt_work_orders nwo ON c.ndt_wo_id = nwo.id
        WHERE 1=1 {date_filter_ndt.replace('created_at', 'nwo.created_at')}
    ''', params_ndt).fetchone()
    
    ndt_by_method = conn.execute(f'''
        SELECT 
            ndt_methods,
            COUNT(*) as count
        FROM ndt_work_orders
        WHERE 1=1 {date_filter_ndt}
        GROUP BY ndt_methods
        ORDER BY count DESC
        LIMIT 5
    ''', params_ndt).fetchall()
    
    consulting_data = conn.execute(f'''
        SELECT 
            COUNT(*) as order_count,
            COALESCE(SUM(labor_subtotal), 0) as labor_revenue,
            COALESCE(SUM(materials_subtotal), 0) as materials_revenue,
            COALESCE(SUM(expenses_subtotal), 0) as expenses_revenue,
            COALESCE(SUM(total_cost), 0) as total_revenue,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed_count,
            SUM(CASE WHEN invoiced = 1 THEN 1 ELSE 0 END) as invoiced_count
        FROM service_work_orders
        WHERE status NOT IN ('Cancelled') {date_filter_swo}
    ''', params_swo).fetchone()
    
    consulting_by_type = conn.execute(f'''
        SELECT 
            service_type,
            COUNT(*) as count,
            COALESCE(SUM(total_cost), 0) as revenue
        FROM service_work_orders
        WHERE status NOT IN ('Cancelled') {date_filter_swo}
        GROUP BY service_type
        ORDER BY revenue DESC
    ''', params_swo).fetchall()
    
    trend_date_filter_sales = ""
    trend_date_filter_wo = ""
    trend_date_filter_ndt = ""
    trend_date_filter_swo = ""
    trend_params = []
    
    if start_date:
        trend_date_filter_sales += " AND order_date >= ?"
        trend_date_filter_wo += " AND created_at >= ?"
        trend_date_filter_ndt += " AND created_at >= ?"
        trend_date_filter_swo += " AND created_at >= ?"
        trend_params.extend([start_date, start_date, start_date, start_date, start_date])
    
    if end_date:
        trend_date_filter_sales += " AND order_date <= ?"
        trend_date_filter_wo += " AND created_at <= ?"
        trend_date_filter_ndt += " AND created_at <= ?"
        trend_date_filter_swo += " AND created_at <= ?"
        trend_params.extend([end_date, end_date, end_date, end_date, end_date])
    
    monthly_trends = conn.execute(f'''
        SELECT 
            strftime('%Y-%m', order_date) as month,
            'Sales' as department,
            COALESCE(SUM(total_amount), 0) as revenue
        FROM sales_orders
        WHERE status NOT IN ('Cancelled', 'Draft') {trend_date_filter_sales}
        GROUP BY strftime('%Y-%m', order_date)
        UNION ALL
        SELECT 
            strftime('%Y-%m', created_at) as month,
            'Operations' as department,
            COALESCE(SUM(material_cost + labor_cost + overhead_cost), 0) as revenue
        FROM work_orders
        WHERE status NOT IN ('Cancelled') {trend_date_filter_wo}
        GROUP BY strftime('%Y-%m', created_at)
        UNION ALL
        SELECT 
            strftime('%Y-%m', i.created_at) as month,
            'NDT' as department,
            COALESCE(SUM(i.total_amount), 0) as revenue
        FROM invoices i
        WHERE i.source_type = 'ndt_work_order' AND i.status != 'Cancelled' {trend_date_filter_ndt.replace('created_at', 'i.created_at')}
        GROUP BY strftime('%Y-%m', i.created_at)
        UNION ALL
        SELECT 
            strftime('%Y-%m', ni.invoice_date) as month,
            'NDT' as department,
            COALESCE(SUM(ni.total_amount), 0) as revenue
        FROM ndt_invoices ni
        WHERE ni.status NOT IN ('Cancelled', 'Void') {trend_date_filter_ndt.replace('created_at', 'ni.invoice_date')}
        GROUP BY strftime('%Y-%m', ni.invoice_date)
        UNION ALL
        SELECT 
            strftime('%Y-%m', created_at) as month,
            'Consulting' as department,
            COALESCE(SUM(total_cost), 0) as revenue
        FROM service_work_orders
        WHERE status NOT IN ('Cancelled') {trend_date_filter_swo}
        GROUP BY strftime('%Y-%m', created_at)
        ORDER BY month DESC
        LIMIT 48
    ''', trend_params).fetchall()
    
    months = sorted(list(set([r['month'] for r in monthly_trends if r['month']])))[-12:]
    trend_data = {
        'labels': months,
        'sales': [0] * len(months),
        'operations': [0] * len(months),
        'ndt': [0] * len(months),
        'consulting': [0] * len(months)
    }
    
    for row in monthly_trends:
        if row['month'] in months:
            idx = months.index(row['month'])
            dept = row['department'].lower()
            if dept in trend_data:
                trend_data[dept][idx] = row['revenue']
    
    conn.close()
    
    sales_dict = dict(sales_data) if sales_data else {}
    ops_dict = dict(operations_data) if operations_data else {}
    ndt_dict = dict(ndt_data) if ndt_data else {}
    consulting_dict = dict(consulting_data) if consulting_data else {}
    
    sales_rev = sales_dict.get('total_revenue', 0) or 0
    sales_cost = sales_cogs['cogs'] if sales_cogs else 0
    sales_profit = sales_rev - sales_cost
    sales_margin = (sales_profit / sales_rev * 100) if sales_rev > 0 else 0
    
    ops_rev = wo_revenue['invoiced_revenue'] if wo_revenue else 0
    ops_cost = ops_dict.get('total_cost', 0) or 0
    ops_profit = ops_rev - ops_cost
    ops_margin = (ops_profit / ops_rev * 100) if ops_rev > 0 else 0
    
    ndt_rev = ndt_revenue['invoiced_revenue'] if ndt_revenue else 0
    ndt_cost = ndt_costs['tracked_cost'] if ndt_costs else 0
    ndt_profit = ndt_rev - ndt_cost
    ndt_margin = (ndt_profit / ndt_rev * 100) if ndt_rev > 0 else 0
    
    consulting_rev = consulting_dict.get('total_revenue', 0) or 0
    consulting_labor = consulting_dict.get('labor_revenue', 0) or 0
    consulting_cost = consulting_labor * 0.4
    consulting_profit = consulting_rev - consulting_cost
    consulting_margin = (consulting_profit / consulting_rev * 100) if consulting_rev > 0 else 0
    
    departments = {
        'sales': {
            'name': 'Sales',
            'icon': 'bi-cart-check',
            'color': '#28a745',
            'data': sales_dict,
            'breakdown': [dict(r) for r in sales_by_type],
            'revenue': sales_rev,
            'cost': sales_cost,
            'profit': sales_profit,
            'margin': sales_margin,
            'orders': sales_dict.get('order_count', 0) or 0
        },
        'operations': {
            'name': 'Operations',
            'icon': 'bi-gear-wide-connected',
            'color': '#007bff',
            'data': ops_dict,
            'breakdown': [dict(r) for r in wo_by_status],
            'revenue': ops_rev,
            'cost': ops_cost,
            'profit': ops_profit,
            'margin': ops_margin,
            'orders': ops_dict.get('order_count', 0) or 0
        },
        'ndt': {
            'name': 'NDT',
            'icon': 'bi-search',
            'color': '#6f42c1',
            'data': ndt_dict,
            'breakdown': [dict(r) for r in ndt_by_method],
            'revenue': ndt_rev,
            'cost': ndt_cost,
            'profit': ndt_profit,
            'margin': ndt_margin,
            'orders': ndt_dict.get('order_count', 0) or 0
        },
        'consulting': {
            'name': 'Consulting',
            'icon': 'bi-people',
            'color': '#fd7e14',
            'data': consulting_dict,
            'breakdown': [dict(r) for r in consulting_by_type],
            'revenue': consulting_rev,
            'cost': consulting_cost,
            'profit': consulting_profit,
            'margin': consulting_margin,
            'orders': consulting_dict.get('order_count', 0) or 0
        }
    }
    
    total_revenue = sum([d['revenue'] for d in departments.values()])
    total_cost = sum([d['cost'] for d in departments.values()])
    total_profit = sum([d['profit'] for d in departments.values()])
    overall_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0
    
    return render_template('accounting/revenue_tracker.html',
                         departments=departments,
                         total_revenue=total_revenue,
                         total_cost=total_cost,
                         total_profit=total_profit,
                         overall_margin=overall_margin,
                         trend_data=trend_data,
                         start_date=start_date,
                         end_date=end_date)


# ---------------------------------------------------------------------------
# GL Integrity & Backfill
# ---------------------------------------------------------------------------

@accounting_bp.route('/accounting/gl-integrity')
@login_required
@role_required('Admin', 'Accountant')
def gl_integrity():
    """GL Integrity dashboard — shows orphan transactions and unbalanced JEs."""
    from utils.accounting_engine import integrity_check
    db = Database()
    conn = db.get_connection()
    try:
        report = integrity_check(conn)
    except Exception as e:
        logger.error(f'GL integrity check failed: {e}')
        report = {'summary': {'total_violations': 0, 'ar_missing': 0, 'ap_missing': 0,
                               'unbalanced': 0, 'empty_headers': 0},
                  'ar_invoices_missing_je': [], 'ap_invoices_missing_je': [],
                  'unbalanced_entries': [], 'empty_journal_entries': []}
    finally:
        conn.close()
    return render_template('accounting/gl_integrity.html', report=report)


@accounting_bp.route('/api/accounting/backfill-je', methods=['POST'])
@login_required
@role_required('Admin', 'Accountant')
def api_backfill_je():
    """Create missing journal entries for historical transactions."""
    from utils.accounting_engine import backfill_missing_je
    db = Database()
    conn = db.get_connection()
    try:
        conn.rollback()
    except Exception:
        pass
    try:
        result = backfill_missing_je(conn, session['user_id'])
        conn.commit()
        return jsonify({'success': True, 'created': result['created'], 'errors': result['errors'],
                        'created_count': len(result['created']), 'error_count': len(result['errors'])})
    except Exception as e:
        conn.rollback()
        logger.error(f'Backfill failed: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        conn.close()


@accounting_bp.route('/api/accounting/integrity-check')
@login_required
@role_required('Admin', 'Accountant')
def api_integrity_check():
    """Return integrity check results as JSON."""
    from utils.accounting_engine import integrity_check
    db = Database()
    conn = db.get_connection()
    try:
        report = integrity_check(conn)
        return jsonify({'success': True, 'report': report})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        conn.close()
