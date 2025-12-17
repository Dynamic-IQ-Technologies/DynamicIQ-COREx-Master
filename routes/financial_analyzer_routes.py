from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database
from datetime import datetime, timedelta
import json
import os
from openai import OpenAI

financial_analyzer_bp = Blueprint('financial_analyzer_routes', __name__)

def get_openai_client():
    """Get OpenAI client configured with Replit AI Integrations"""
    return OpenAI(
        api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
        base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
    )

def get_gl_account_balance(conn, account_code):
    """Get the balance from General Ledger for a specific account code"""
    result = conn.execute('''
        SELECT COALESCE(SUM(gl_lines.debit - gl_lines.credit), 0) as balance
        FROM gl_entry_lines gl_lines
        JOIN gl_entries gl ON gl_lines.gl_entry_id = gl.id
        JOIN chart_of_accounts coa ON gl_lines.account_id = coa.id
        WHERE coa.account_code = ? AND gl.status = 'Posted'
    ''', (account_code,)).fetchone()
    return result['balance'] if result else 0

def calculate_cash_metrics(conn):
    """Calculate cash flow and liquidity metrics using GL data"""
    metrics = {}
    
    today = datetime.now()
    month_start = today.replace(day=1).strftime('%Y-%m-%d')
    last_month_start = (today.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')
    
    cash_balance = get_gl_account_balance(conn, '1110')
    ar_gl_balance = get_gl_account_balance(conn, '1120')
    ap_gl_balance = abs(get_gl_account_balance(conn, '2110'))
    
    cash_in_mtd = conn.execute('''
        SELECT COALESCE(SUM(amount_paid), 0) as total FROM invoices 
        WHERE status IN ('Paid', 'Partial') AND invoice_date >= ?
    ''', (month_start,)).fetchone()['total']
    
    cash_in_last_month = conn.execute('''
        SELECT COALESCE(SUM(amount_paid), 0) as total FROM invoices 
        WHERE status IN ('Paid', 'Partial') AND invoice_date >= ? AND invoice_date < ?
    ''', (last_month_start, month_start)).fetchone()['total']
    
    cash_out_mtd = conn.execute('''
        SELECT COALESCE(SUM(amount_paid), 0) as total FROM vendor_invoices 
        WHERE status IN ('Paid', 'Partial') AND invoice_date >= ?
    ''', (month_start,)).fetchone()['total']
    
    cash_out_last_month = conn.execute('''
        SELECT COALESCE(SUM(amount_paid), 0) as total FROM vendor_invoices 
        WHERE status IN ('Paid', 'Partial') AND invoice_date >= ? AND invoice_date < ?
    ''', (last_month_start, month_start)).fetchone()['total']
    
    ar_outstanding = conn.execute('''
        SELECT COALESCE(SUM(total_amount - COALESCE(amount_paid, 0)), 0) as total 
        FROM invoices WHERE status NOT IN ('Paid', 'Cancelled', 'Voided')
    ''').fetchone()['total']
    
    ap_outstanding = conn.execute('''
        SELECT COALESCE(SUM(total_amount - COALESCE(amount_paid, 0)), 0) as total 
        FROM vendor_invoices WHERE status NOT IN ('Paid', 'Cancelled')
    ''').fetchone()['total']
    
    inventory_value = conn.execute('''
        SELECT COALESCE(SUM(i.quantity * p.cost), 0) as total 
        FROM inventory i
        JOIN products p ON i.product_id = p.id
    ''').fetchone()['total']
    
    open_po_value = conn.execute('''
        SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) as total 
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        WHERE po.status IN ('Draft', 'Submitted', 'Approved', 'Partial')
    ''').fetchone()['total']
    
    net_cash_flow_mtd = cash_in_mtd - cash_out_mtd
    net_cash_flow_last_month = cash_in_last_month - cash_out_last_month
    
    monthly_burn_rate = cash_out_last_month if cash_out_last_month > 0 else cash_out_mtd
    
    current_assets = cash_balance + ar_outstanding + inventory_value
    current_liabilities = ap_outstanding + open_po_value
    net_working_capital = current_assets - current_liabilities
    
    runway_months = 0
    if monthly_burn_rate > 0:
        available_cash = max(0, cash_balance + (ar_outstanding * 0.85) - ap_outstanding - open_po_value)
        runway_months = available_cash / monthly_burn_rate
    
    metrics['cash_balance'] = cash_balance
    metrics['cash_in_mtd'] = cash_in_mtd
    metrics['cash_in_last_month'] = cash_in_last_month
    metrics['cash_out_mtd'] = cash_out_mtd
    metrics['cash_out_last_month'] = cash_out_last_month
    metrics['net_cash_flow_mtd'] = net_cash_flow_mtd
    metrics['net_cash_flow_last_month'] = net_cash_flow_last_month
    metrics['ar_outstanding'] = ar_outstanding
    metrics['ap_outstanding'] = ap_outstanding
    metrics['inventory_value'] = inventory_value
    metrics['open_po_value'] = open_po_value
    metrics['monthly_burn_rate'] = monthly_burn_rate
    metrics['current_assets'] = current_assets
    metrics['current_liabilities'] = current_liabilities
    metrics['net_working_capital'] = net_working_capital
    metrics['runway_months'] = round(runway_months, 1)
    
    return metrics

def calculate_revenue_metrics(conn):
    """Calculate revenue and margin metrics"""
    metrics = {}
    
    today = datetime.now()
    month_start = today.replace(day=1).strftime('%Y-%m-%d')
    last_month_start = (today.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')
    last_month_end = (today.replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')
    year_start = today.replace(month=1, day=1).strftime('%Y-%m-%d')
    
    total_revenue = conn.execute('''
        SELECT COALESCE(SUM(total_amount), 0) as total FROM sales_orders 
        WHERE status NOT IN ('Draft', 'Cancelled')
    ''').fetchone()['total']
    
    revenue_mtd = conn.execute('''
        SELECT COALESCE(SUM(total_amount), 0) as total FROM sales_orders 
        WHERE order_date >= ? AND status NOT IN ('Draft', 'Cancelled')
    ''', (month_start,)).fetchone()['total']
    
    revenue_last_month = conn.execute('''
        SELECT COALESCE(SUM(total_amount), 0) as total FROM sales_orders 
        WHERE order_date >= ? AND order_date < ? AND status NOT IN ('Draft', 'Cancelled')
    ''', (last_month_start, month_start)).fetchone()['total']
    
    revenue_ytd = conn.execute('''
        SELECT COALESCE(SUM(total_amount), 0) as total FROM sales_orders 
        WHERE order_date >= ? AND status NOT IN ('Draft', 'Cancelled')
    ''', (year_start,)).fetchone()['total']
    
    revenue_growth = 0
    if revenue_last_month > 0:
        revenue_growth = ((revenue_mtd - revenue_last_month) / revenue_last_month) * 100
    
    total_cogs = conn.execute('''
        SELECT COALESCE(SUM(material_cost + labor_cost + overhead_cost), 0) as total 
        FROM work_orders WHERE status IN ('Completed', 'Closed')
    ''').fetchone()['total']
    
    total_material_cost = conn.execute('''
        SELECT COALESCE(SUM(material_cost), 0) as total FROM work_orders
    ''').fetchone()['total']
    
    total_labor_cost = conn.execute('''
        SELECT COALESCE(SUM(labor_cost), 0) as total FROM work_orders
    ''').fetchone()['total']
    
    total_overhead = conn.execute('''
        SELECT COALESCE(SUM(overhead_cost), 0) as total FROM work_orders
    ''').fetchone()['total']
    
    gross_margin = 0
    gross_profit = 0
    if total_revenue > 0:
        gross_profit = total_revenue - total_cogs
        gross_margin = (gross_profit / total_revenue) * 100
    
    avg_order_value = conn.execute('''
        SELECT COALESCE(AVG(total_amount), 0) as avg FROM sales_orders 
        WHERE status NOT IN ('Draft', 'Cancelled')
    ''').fetchone()['avg']
    
    order_count = conn.execute('''
        SELECT COUNT(*) as count FROM sales_orders 
        WHERE status NOT IN ('Draft', 'Cancelled')
    ''').fetchone()['count']
    
    customer_count = conn.execute('''
        SELECT COUNT(DISTINCT customer_id) as count FROM sales_orders 
        WHERE status NOT IN ('Draft', 'Cancelled')
    ''').fetchone()['count']
    
    revenue_per_customer = 0
    if customer_count > 0:
        revenue_per_customer = total_revenue / customer_count
    
    metrics['total_revenue'] = total_revenue
    metrics['revenue_mtd'] = revenue_mtd
    metrics['revenue_last_month'] = revenue_last_month
    metrics['revenue_ytd'] = revenue_ytd
    metrics['revenue_growth'] = revenue_growth
    metrics['total_cogs'] = total_cogs
    metrics['total_material_cost'] = total_material_cost
    metrics['total_labor_cost'] = total_labor_cost
    metrics['total_overhead'] = total_overhead
    metrics['gross_profit'] = gross_profit
    metrics['gross_margin'] = gross_margin
    metrics['avg_order_value'] = avg_order_value
    metrics['order_count'] = order_count
    metrics['customer_count'] = customer_count
    metrics['revenue_per_customer'] = revenue_per_customer
    
    return metrics

def calculate_efficiency_metrics(conn):
    """Calculate operational efficiency and capital metrics"""
    metrics = {}
    
    avg_wo_material_cost = conn.execute('''
        SELECT COALESCE(AVG(material_cost), 0) as avg FROM work_orders 
        WHERE status IN ('Completed', 'Closed')
    ''').fetchone()['avg']
    
    avg_wo_labor_cost = conn.execute('''
        SELECT COALESCE(AVG(labor_cost), 0) as avg FROM work_orders 
        WHERE status IN ('Completed', 'Closed')
    ''').fetchone()['avg']
    
    avg_wo_total_cost = avg_wo_material_cost + avg_wo_labor_cost
    
    completed_wo = conn.execute('''
        SELECT COUNT(*) as count FROM work_orders WHERE status IN ('Completed', 'Closed')
    ''').fetchone()['count']
    
    total_wo = conn.execute('SELECT COUNT(*) as count FROM work_orders').fetchone()['count']
    
    wo_completion_rate = 0
    if total_wo > 0:
        wo_completion_rate = (completed_wo / total_wo) * 100
    
    avg_employee_cost = conn.execute('''
        SELECT COALESCE(AVG(hourly_rate), 0) * 160 as monthly_cost FROM labor_resources 
        WHERE status = 'Active'
    ''').fetchone()['monthly_cost']
    
    employee_count = conn.execute('''
        SELECT COUNT(*) as count FROM labor_resources WHERE status = 'Active'
    ''').fetchone()['count']
    
    total_monthly_labor_expense = avg_employee_cost * employee_count if employee_count > 0 else 0
    
    revenue_data = calculate_revenue_metrics(conn)
    revenue_per_employee = 0
    if employee_count > 0:
        revenue_per_employee = revenue_data['total_revenue'] / employee_count
    
    inventory_value = conn.execute('''
        SELECT COALESCE(SUM(i.quantity * p.cost), 0) as total 
        FROM inventory i JOIN products p ON i.product_id = p.id
    ''').fetchone()['total']
    
    inventory_turnover = 0
    if inventory_value > 0:
        inventory_turnover = revenue_data['total_cogs'] / inventory_value
    
    days_inventory = 0
    if inventory_turnover > 0:
        days_inventory = 365 / inventory_turnover
    
    metrics['avg_wo_material_cost'] = avg_wo_material_cost
    metrics['avg_wo_labor_cost'] = avg_wo_labor_cost
    metrics['avg_wo_total_cost'] = avg_wo_total_cost
    metrics['wo_completion_rate'] = wo_completion_rate
    metrics['employee_count'] = employee_count
    metrics['total_monthly_labor_expense'] = total_monthly_labor_expense
    metrics['revenue_per_employee'] = revenue_per_employee
    metrics['inventory_turnover'] = round(inventory_turnover, 2)
    metrics['days_inventory'] = round(days_inventory, 0)
    
    return metrics

def calculate_risk_metrics(conn):
    """Calculate financial risk indicators"""
    metrics = {}
    
    ar_by_age = conn.execute('''
        SELECT 
            SUM(CASE WHEN julianday('now') - julianday(invoice_date) <= 30 THEN total_amount - COALESCE(amount_paid, 0) ELSE 0 END) as current,
            SUM(CASE WHEN julianday('now') - julianday(invoice_date) > 30 AND julianday('now') - julianday(invoice_date) <= 60 THEN total_amount - COALESCE(amount_paid, 0) ELSE 0 END) as days_31_60,
            SUM(CASE WHEN julianday('now') - julianday(invoice_date) > 60 AND julianday('now') - julianday(invoice_date) <= 90 THEN total_amount - COALESCE(amount_paid, 0) ELSE 0 END) as days_61_90,
            SUM(CASE WHEN julianday('now') - julianday(invoice_date) > 90 THEN total_amount - COALESCE(amount_paid, 0) ELSE 0 END) as over_90
        FROM invoices WHERE status NOT IN ('Paid', 'Cancelled', 'Voided')
    ''').fetchone()
    
    metrics['ar_current'] = ar_by_age['current'] or 0
    metrics['ar_31_60'] = ar_by_age['days_31_60'] or 0
    metrics['ar_61_90'] = ar_by_age['days_61_90'] or 0
    metrics['ar_over_90'] = ar_by_age['over_90'] or 0
    
    total_ar = metrics['ar_current'] + metrics['ar_31_60'] + metrics['ar_61_90'] + metrics['ar_over_90']
    metrics['ar_risk_percentage'] = 0
    if total_ar > 0:
        metrics['ar_risk_percentage'] = ((metrics['ar_61_90'] + metrics['ar_over_90']) / total_ar) * 100
    
    top_customer_revenue = conn.execute('''
        SELECT customer_id, COALESCE(SUM(total_amount), 0) as revenue
        FROM sales_orders WHERE status NOT IN ('Draft', 'Cancelled')
        GROUP BY customer_id ORDER BY revenue DESC LIMIT 1
    ''').fetchone()
    
    total_revenue = conn.execute('''
        SELECT COALESCE(SUM(total_amount), 0) as total FROM sales_orders 
        WHERE status NOT IN ('Draft', 'Cancelled')
    ''').fetchone()['total']
    
    metrics['revenue_concentration'] = 0
    if total_revenue > 0 and top_customer_revenue:
        metrics['revenue_concentration'] = (top_customer_revenue['revenue'] / total_revenue) * 100
    
    top_supplier_spend = conn.execute('''
        SELECT po.supplier_id, COALESCE(SUM(pol.quantity * pol.unit_price), 0) as spend
        FROM purchase_orders po
        JOIN purchase_order_lines pol ON pol.po_id = po.id
        WHERE po.status NOT IN ('Draft', 'Cancelled')
        GROUP BY po.supplier_id ORDER BY spend DESC LIMIT 1
    ''').fetchone()
    
    total_spend = conn.execute('''
        SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) as total 
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        WHERE po.status NOT IN ('Draft', 'Cancelled')
    ''').fetchone()['total']
    
    metrics['supplier_concentration'] = 0
    if total_spend > 0 and top_supplier_spend:
        metrics['supplier_concentration'] = (top_supplier_spend['spend'] / total_spend) * 100
    
    overdue_ar = conn.execute('''
        SELECT COUNT(*) as count FROM invoices 
        WHERE status NOT IN ('Paid', 'Cancelled', 'Voided') AND due_date < date('now')
    ''').fetchone()['count']
    
    overdue_ap = conn.execute('''
        SELECT COUNT(*) as count FROM vendor_invoices 
        WHERE status NOT IN ('Paid', 'Cancelled') AND due_date < date('now')
    ''').fetchone()['count']
    
    metrics['overdue_ar_count'] = overdue_ar
    metrics['overdue_ap_count'] = overdue_ap
    
    return metrics

@financial_analyzer_bp.route('/financial-analyzer')
def dashboard():
    """Financial Analyzer CFO Dashboard"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    cash = calculate_cash_metrics(conn)
    revenue = calculate_revenue_metrics(conn)
    efficiency = calculate_efficiency_metrics(conn)
    risk = calculate_risk_metrics(conn)
    
    conn.close()
    
    return render_template('financial_analyzer/dashboard.html',
                         cash=cash,
                         revenue=revenue,
                         efficiency=efficiency,
                         risk=risk)

@financial_analyzer_bp.route('/financial-analyzer/analyze', methods=['POST'])
def generate_analysis():
    """Generate AI-powered CFO financial analysis"""
    if 'user_id' not in session:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    
    db = Database()
    conn = db.get_connection()
    
    cash = calculate_cash_metrics(conn)
    revenue = calculate_revenue_metrics(conn)
    efficiency = calculate_efficiency_metrics(conn)
    risk = calculate_risk_metrics(conn)
    
    conn.close()
    
    financial_snapshot = f"""
FINANCIAL SNAPSHOT - CFO BRIEFING
==================================

CASH POSITION & LIQUIDITY:
- Cash Balance (GL): ${cash['cash_balance']:,.2f}
- Current Assets: ${cash['current_assets']:,.2f}
- Current Liabilities: ${cash['current_liabilities']:,.2f}
- Net Working Capital: ${cash['net_working_capital']:,.2f}
- Cash In (MTD): ${cash['cash_in_mtd']:,.2f}
- Cash Out (MTD): ${cash['cash_out_mtd']:,.2f}
- Net Cash Flow (MTD): ${cash['net_cash_flow_mtd']:,.2f}
- Monthly Burn Rate: ${cash['monthly_burn_rate']:,.2f}
- Estimated Runway: {cash['runway_months']} months
- A/R Outstanding: ${cash['ar_outstanding']:,.2f}
- A/P Outstanding: ${cash['ap_outstanding']:,.2f}
- Inventory Value: ${cash['inventory_value']:,.2f}
- Open PO Commitments: ${cash['open_po_value']:,.2f}

REVENUE & MARGINS:
- Total Revenue: ${revenue['total_revenue']:,.2f}
- Revenue MTD: ${revenue['revenue_mtd']:,.2f}
- Revenue Growth: {revenue['revenue_growth']:.1f}%
- Revenue YTD: ${revenue['revenue_ytd']:,.2f}
- Gross Profit: ${revenue['gross_profit']:,.2f}
- Gross Margin: {revenue['gross_margin']:.1f}%
- Avg Order Value: ${revenue['avg_order_value']:,.2f}
- Revenue/Customer: ${revenue['revenue_per_customer']:,.2f}
- Total COGS: ${revenue['total_cogs']:,.2f}

OPERATIONAL EFFICIENCY:
- Employees: {efficiency['employee_count']}
- Revenue/Employee: ${efficiency['revenue_per_employee']:,.2f}
- Monthly Labor Expense: ${efficiency['total_monthly_labor_expense']:,.2f}
- Avg WO Cost: ${efficiency['avg_wo_total_cost']:,.2f}
- WO Completion Rate: {efficiency['wo_completion_rate']:.1f}%
- Inventory Turnover: {efficiency['inventory_turnover']}x
- Days Inventory: {efficiency['days_inventory']} days

RISK INDICATORS:
- A/R Current: ${risk['ar_current']:,.2f}
- A/R 31-60 Days: ${risk['ar_31_60']:,.2f}
- A/R 61-90 Days: ${risk['ar_61_90']:,.2f}
- A/R Over 90 Days: ${risk['ar_over_90']:,.2f}
- A/R Risk %: {risk['ar_risk_percentage']:.1f}%
- Revenue Concentration: {risk['revenue_concentration']:.1f}%
- Supplier Concentration: {risk['supplier_concentration']:.1f}%
- Overdue A/R: {risk['overdue_ar_count']} invoices
- Overdue A/P: {risk['overdue_ap_count']} invoices
"""
    
    try:
        client = get_openai_client()
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": """You are a Super AI CFO - a Fortune-scale Chief Financial Officer providing executive-level financial analysis.

Analyze the financial data and provide:

1. EXECUTIVE SUMMARY (CFO Brief) - 2-3 sentences on overall financial health
2. FINANCIAL HEALTH SCORE (0-100) with breakdown
3. CRITICAL FINDINGS - Top 3 issues requiring immediate attention
4. CASH FLOW ANALYSIS - Cash position assessment and runway outlook
5. MARGIN ANALYSIS - Profitability assessment
6. RISK ASSESSMENT - Key financial risks with severity
7. CFO RECOMMENDATIONS - Top 5 actionable recommendations ranked by impact
8. 90-DAY OUTLOOK - Forward-looking financial expectations

Format as JSON:
{
    "executive_summary": "string",
    "health_score": number,
    "health_breakdown": {"cash": number, "revenue": number, "efficiency": number, "risk": number},
    "critical_findings": [{"issue": "string", "impact": "string", "urgency": "High|Medium|Low"}],
    "cash_analysis": {"status": "Healthy|Caution|Critical", "summary": "string", "runway_assessment": "string"},
    "margin_analysis": {"status": "string", "gross_margin_assessment": "string"},
    "risks": [{"risk": "string", "severity": "Critical|High|Medium|Low", "mitigation": "string"}],
    "recommendations": [{"action": "string", "impact": "string", "priority": "High|Medium|Low", "timeline": "string"}],
    "outlook_90_day": {"summary": "string", "key_metrics_forecast": [{"metric": "string", "direction": "Up|Stable|Down"}]}
}"""
                },
                {
                    "role": "user",
                    "content": f"Provide CFO-level financial analysis for this company:\n\n{financial_snapshot}"
                }
            ],
            temperature=0.5,
            max_tokens=2500
        )
        
        content = response.choices[0].message.content
        content = content.strip()
        if content.startswith('```json'):
            content = content[7:]
        if content.startswith('```'):
            content = content[3:]
        if content.endswith('```'):
            content = content[:-3]
        
        analysis_data = json.loads(content)
        
        return jsonify({
            'success': True,
            'data': analysis_data
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@financial_analyzer_bp.route('/financial-analyzer/scenario', methods=['POST'])
def run_scenario():
    """Run financial scenario modeling"""
    if 'user_id' not in session:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    
    scenario_type = request.json.get('scenario_type', 'base')
    adjustment = request.json.get('adjustment', 0)
    
    db = Database()
    conn = db.get_connection()
    
    cash = calculate_cash_metrics(conn)
    revenue = calculate_revenue_metrics(conn)
    
    conn.close()
    
    scenarios = {
        'revenue_growth': {
            'title': f'Revenue Growth (+{adjustment}%)',
            'new_revenue': revenue['revenue_mtd'] * (1 + adjustment/100),
            'impact_on_margin': revenue['gross_margin'],
            'impact_on_runway': cash['runway_months'] * (1 + adjustment/200)
        },
        'cost_reduction': {
            'title': f'Cost Reduction (-{adjustment}%)',
            'new_burn_rate': cash['monthly_burn_rate'] * (1 - adjustment/100),
            'new_runway': cash['runway_months'] * (1 + adjustment/100) if cash['monthly_burn_rate'] > 0 else 0,
            'savings': cash['monthly_burn_rate'] * (adjustment/100)
        },
        'revenue_decline': {
            'title': f'Revenue Decline (-{adjustment}%)',
            'new_revenue': revenue['revenue_mtd'] * (1 - adjustment/100),
            'impact_on_margin': revenue['gross_margin'] - (adjustment * 0.3),
            'impact_on_runway': cash['runway_months'] * (1 - adjustment/150)
        }
    }
    
    scenario_data = scenarios.get(scenario_type, scenarios['revenue_growth'])
    
    return jsonify({
        'success': True,
        'scenario': scenario_data,
        'baseline': {
            'revenue_mtd': revenue['revenue_mtd'],
            'burn_rate': cash['monthly_burn_rate'],
            'runway': cash['runway_months'],
            'margin': revenue['gross_margin']
        }
    })

@financial_analyzer_bp.route('/financial-analyzer/api/metrics')
def api_metrics():
    """API endpoint for real-time financial metrics"""
    if 'user_id' not in session:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    
    db = Database()
    conn = db.get_connection()
    
    cash = calculate_cash_metrics(conn)
    revenue = calculate_revenue_metrics(conn)
    efficiency = calculate_efficiency_metrics(conn)
    risk = calculate_risk_metrics(conn)
    
    conn.close()
    
    return jsonify({
        'success': True,
        'cash': cash,
        'revenue': revenue,
        'efficiency': efficiency,
        'risk': risk,
        'timestamp': datetime.now().isoformat()
    })
