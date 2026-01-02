from flask import Blueprint, render_template, request, jsonify, session
from models import Database
from auth import login_required, role_required
from datetime import datetime, timedelta
import json
import os

sales_dashboard_bp = Blueprint('sales_dashboard', __name__)

def get_date_ranges():
    """Calculate rolling date ranges for executive dashboard"""
    today = datetime.now()
    
    last_30_start = (today - timedelta(days=30)).strftime('%Y-%m-%d')
    last_90_start = (today - timedelta(days=90)).strftime('%Y-%m-%d')
    last_365_start = (today - timedelta(days=365)).strftime('%Y-%m-%d')
    
    prev_30_start = (today - timedelta(days=60)).strftime('%Y-%m-%d')
    prev_30_end = (today - timedelta(days=30)).strftime('%Y-%m-%d')
    
    today_str = today.strftime('%Y-%m-%d')
    
    return {
        'mtd_start': last_30_start,
        'qtd_start': last_90_start,
        'ytd_start': last_365_start,
        'prev_30_start': prev_30_start,
        'prev_30_end': prev_30_end,
        'today': today_str
    }

@sales_dashboard_bp.route('/executive-sales-dashboard')
@login_required
def executive_sales_dashboard():
    db = Database()
    conn = db.get_connection()
    
    dates = get_date_ranges()
    today = datetime.now()
    
    mtd_revenue = conn.execute('''
        SELECT COALESCE(SUM(total_amount), 0) as revenue
        FROM invoices
        WHERE invoice_date >= ? AND invoice_date <= ?
        AND status IN ('Posted', 'Paid', 'Partial')
    ''', (dates['mtd_start'], dates['today'])).fetchone()
    
    qtd_revenue = conn.execute('''
        SELECT COALESCE(SUM(total_amount), 0) as revenue
        FROM invoices
        WHERE invoice_date >= ? AND invoice_date <= ?
        AND status IN ('Posted', 'Paid', 'Partial')
    ''', (dates['qtd_start'], dates['today'])).fetchone()
    
    ytd_revenue = conn.execute('''
        SELECT COALESCE(SUM(total_amount), 0) as revenue
        FROM invoices
        WHERE invoice_date >= ? AND invoice_date <= ?
        AND status IN ('Posted', 'Paid', 'Partial')
    ''', (dates['ytd_start'], dates['today'])).fetchone()
    
    prev_year_ytd = conn.execute('''
        SELECT COALESCE(SUM(total_amount), 0) as revenue
        FROM invoices
        WHERE invoice_date >= ? AND invoice_date <= ?
        AND status IN ('Posted', 'Paid', 'Partial')
    ''', ((today - timedelta(days=365)).replace(month=1, day=1).strftime('%Y-%m-%d'), 
          (today - timedelta(days=365)).strftime('%Y-%m-%d'))).fetchone()
    
    ytd_growth = 0
    if prev_year_ytd['revenue'] > 0:
        ytd_growth = ((ytd_revenue['revenue'] - prev_year_ytd['revenue']) / prev_year_ytd['revenue']) * 100
    
    bookings = conn.execute('''
        SELECT COALESCE(SUM(COALESCE(so.total_amount, 0)), 0) as total
        FROM sales_orders so
        WHERE so.order_date >= ? AND so.order_date <= ?
    ''', (dates['ytd_start'], dates['today'])).fetchone()
    
    gross_margin_revenue = conn.execute('''
        SELECT COALESCE(SUM(total_amount), 0) as revenue
        FROM invoices
        WHERE invoice_date >= ? AND status IN ('Posted', 'Paid', 'Partial')
    ''', (dates['ytd_start'],)).fetchone()
    
    gross_margin_cost = conn.execute('''
        SELECT COALESCE(SUM(COALESCE(wo.material_cost, 0) + COALESCE(wo.labor_cost, 0)), 0) as cost
        FROM work_orders wo
        WHERE wo.id IN (
            SELECT DISTINCT wo_id FROM invoices 
            WHERE wo_id IS NOT NULL 
            AND invoice_date >= ? 
            AND status IN ('Posted', 'Paid', 'Partial')
        )
    ''', (dates['ytd_start'],)).fetchone()
    
    gross_margin = {
        'revenue': gross_margin_revenue['revenue'],
        'cost': gross_margin_cost['cost']
    }
    
    margin_pct = 0
    if gross_margin['revenue'] > 0:
        margin_pct = ((gross_margin['revenue'] - gross_margin['cost']) / gross_margin['revenue']) * 100
    
    pipeline_value = conn.execute('''
        SELECT COALESCE(SUM(COALESCE(total_amount, 0)), 0) as total
        FROM sales_orders
        WHERE status IN ('Draft', 'Pending', 'Confirmed')
    ''').fetchone()
    
    quote_stats = conn.execute('''
        SELECT 
            COUNT(*) as total_quotes,
            SUM(CASE WHEN status = 'Approved' THEN 1 ELSE 0 END) as approved,
            SUM(CASE WHEN status = 'Rejected' THEN 1 ELSE 0 END) as rejected
        FROM work_order_quotes
        WHERE created_at >= ?
    ''', (dates['ytd_start'],)).fetchone()
    
    win_rate = 0
    if quote_stats['total_quotes'] > 0:
        win_rate = (quote_stats['approved'] or 0) / quote_stats['total_quotes'] * 100
    
    quote_cycle = conn.execute('''
        SELECT AVG(
            JULIANDAY(COALESCE(customer_approved_at, updated_at)) - JULIANDAY(created_at)
        ) as avg_days
        FROM work_order_quotes
        WHERE status = 'Approved' AND created_at >= ?
    ''', (dates['ytd_start'],)).fetchone()
    
    revenue_at_risk = conn.execute('''
        SELECT COALESCE(SUM(sol.line_total), 0) as total
        FROM sales_order_lines sol
        JOIN sales_orders so ON sol.so_id = so.id
        WHERE so.status IN ('Confirmed', 'In Progress')
        AND (
            sol.allocation_status != 'Allocated'
            OR EXISTS (
                SELECT 1 FROM work_orders wo 
                WHERE wo.so_id = so.id 
                AND wo.status IN ('On Hold', 'Planned')
            )
        )
    ''').fetchone()
    
    capacity_stats = conn.execute('''
        SELECT 
            COUNT(*) as total_wo,
            SUM(CASE WHEN status = 'In Progress' THEN 1 ELSE 0 END) as in_progress,
            SUM(CASE WHEN status = 'Planned' THEN 1 ELSE 0 END) as planned
        FROM work_orders
        WHERE status NOT IN ('Completed', 'Cancelled')
    ''').fetchone()
    
    customer_revenue = conn.execute('''
        SELECT 
            c.id, c.name, c.customer_number,
            COALESCE(SUM(i.total_amount), 0) as revenue,
            COUNT(DISTINCT i.id) as invoice_count,
            COUNT(DISTINCT so.id) as order_count
        FROM customers c
        LEFT JOIN invoices i ON i.customer_id = c.id 
            AND i.status IN ('Posted', 'Paid', 'Partial')
            AND i.invoice_date >= ?
        LEFT JOIN sales_orders so ON so.customer_id = c.id
            AND so.order_date >= ?
        GROUP BY c.id
        ORDER BY revenue DESC
        LIMIT 20
    ''', (dates['ytd_start'], dates['ytd_start'])).fetchall()
    
    revenue_by_type = conn.execute('''
        SELECT 
            COALESCE(so.sales_type, 'Other') as sales_type,
            COALESCE(SUM(i.total_amount), 0) as revenue,
            COUNT(DISTINCT i.id) as count
        FROM invoices i
        LEFT JOIN sales_orders so ON i.so_id = so.id
        WHERE i.status IN ('Posted', 'Paid', 'Partial')
        AND i.invoice_date >= ?
        GROUP BY so.sales_type
        ORDER BY revenue DESC
    ''', (dates['ytd_start'],)).fetchall()
    
    monthly_revenue = conn.execute('''
        SELECT 
            strftime('%Y-%m', invoice_date) as month,
            COALESCE(SUM(total_amount), 0) as revenue
        FROM invoices
        WHERE invoice_date >= date('now', '-12 months')
        AND status IN ('Posted', 'Paid', 'Partial')
        GROUP BY strftime('%Y-%m', invoice_date)
        ORDER BY month
    ''').fetchall()
    
    pipeline_by_stage = conn.execute('''
        SELECT 
            status,
            COUNT(*) as count,
            COALESCE(SUM(total_amount), 0) as value
        FROM sales_orders
        WHERE status NOT IN ('Completed', 'Cancelled', 'Closed')
        GROUP BY status
        ORDER BY 
            CASE status 
                WHEN 'Draft' THEN 1
                WHEN 'Pending' THEN 2
                WHEN 'Confirmed' THEN 3
                WHEN 'In Progress' THEN 4
                ELSE 5
            END
    ''').fetchall()
    
    quote_aging = conn.execute('''
        SELECT 
            CASE 
                WHEN JULIANDAY('now') - JULIANDAY(created_at) <= 7 THEN '0-7 days'
                WHEN JULIANDAY('now') - JULIANDAY(created_at) <= 14 THEN '8-14 days'
                WHEN JULIANDAY('now') - JULIANDAY(created_at) <= 30 THEN '15-30 days'
                ELSE '30+ days'
            END as age_bucket,
            COUNT(*) as count,
            COALESCE(SUM(total_amount), 0) as value
        FROM work_order_quotes
        WHERE status = 'Pending'
        GROUP BY age_bucket
        ORDER BY 
            CASE age_bucket
                WHEN '0-7 days' THEN 1
                WHEN '8-14 days' THEN 2
                WHEN '15-30 days' THEN 3
                ELSE 4
            END
    ''').fetchall()
    
    upcoming_inductions = conn.execute('''
        SELECT 
            CASE 
                WHEN date(planned_start_date) <= date('now', '+30 days') THEN '0-30 days'
                WHEN date(planned_start_date) <= date('now', '+60 days') THEN '31-60 days'
                ELSE '61-90 days'
            END as period,
            COUNT(*) as count,
            COALESCE(SUM(sol.line_total), 0) as value
        FROM work_orders wo
        LEFT JOIN sales_orders so ON wo.so_id = so.id
        LEFT JOIN sales_order_lines sol ON sol.so_id = so.id AND sol.product_id = wo.product_id
        WHERE wo.status = 'Planned'
        AND wo.planned_start_date BETWEEN date('now') AND date('now', '+90 days')
        GROUP BY period
    ''').fetchall()
    
    avg_quote_turnaround = conn.execute('''
        SELECT AVG(
            JULIANDAY(COALESCE(customer_approved_at, customer_declined_at, updated_at)) - JULIANDAY(created_at)
        ) as avg_days
        FROM work_order_quotes
        WHERE status IN ('Approved', 'Rejected')
        AND created_at >= ?
    ''', (dates['ytd_start'],)).fetchone()
    
    quote_acceptance = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Approved' THEN 1 ELSE 0 END) as accepted
        FROM work_order_quotes
        WHERE status IN ('Approved', 'Rejected')
        AND created_at >= ?
    ''', (dates['ytd_start'],)).fetchone()
    
    acceptance_rate = 0
    if quote_acceptance['total'] > 0:
        acceptance_rate = (quote_acceptance['accepted'] / quote_acceptance['total']) * 100
    
    dso = conn.execute('''
        SELECT 
            AVG(
                CASE 
                    WHEN status = 'Paid' THEN JULIANDAY(due_date) - JULIANDAY(invoice_date)
                    ELSE JULIANDAY('now') - JULIANDAY(invoice_date)
                END
            ) as avg_days
        FROM invoices
        WHERE status IN ('Paid', 'Partial', 'Posted')
        AND invoice_date >= ?
    ''', (dates['ytd_start'],)).fetchone()
    
    ar_outstanding = conn.execute('''
        SELECT 
            COALESCE(SUM(total_amount - COALESCE(amount_paid, 0)), 0) as balance,
            COUNT(*) as count
        FROM invoices
        WHERE status IN ('Posted', 'Sent', 'Partial')
    ''').fetchone()
    
    ar_overdue = conn.execute('''
        SELECT 
            COALESCE(SUM(total_amount - COALESCE(amount_paid, 0)), 0) as balance,
            COUNT(*) as count
        FROM invoices
        WHERE status IN ('Posted', 'Sent', 'Partial')
        AND due_date < date('now')
    ''').fetchone()
    
    forecast_30 = conn.execute('''
        SELECT COALESCE(SUM(total_amount * 0.8), 0) as forecast
        FROM sales_orders
        WHERE status = 'Confirmed'
        AND expected_ship_date BETWEEN date('now') AND date('now', '+30 days')
    ''').fetchone()
    
    forecast_60 = conn.execute('''
        SELECT COALESCE(SUM(total_amount * 0.6), 0) as forecast
        FROM sales_orders
        WHERE status IN ('Confirmed', 'Pending')
        AND expected_ship_date BETWEEN date('now', '+31 days') AND date('now', '+60 days')
    ''').fetchone()
    
    forecast_90 = conn.execute('''
        SELECT COALESCE(SUM(total_amount * 0.4), 0) as forecast
        FROM sales_orders
        WHERE status IN ('Confirmed', 'Pending', 'Draft')
        AND expected_ship_date BETWEEN date('now', '+61 days') AND date('now', '+90 days')
    ''').fetchone()
    
    alerts = []
    
    high_value_at_risk = conn.execute('''
        SELECT so.so_number, so.total_amount, c.name as customer_name
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        WHERE so.status = 'Confirmed'
        AND so.total_amount > 50000
        AND EXISTS (
            SELECT 1 FROM sales_order_lines sol 
            WHERE sol.so_id = so.id AND sol.allocation_status != 'Allocated'
        )
        LIMIT 5
    ''').fetchall()
    
    for deal in high_value_at_risk:
        alerts.append({
            'type': 'warning',
            'icon': 'exclamation-triangle',
            'title': f'High-value deal at risk',
            'message': f'{deal["so_number"]} (${deal["total_amount"]:,.2f}) - {deal["customer_name"]} has unallocated items'
        })
    
    overdue_quotes = conn.execute('''
        SELECT COUNT(*) as count, COALESCE(SUM(total_amount), 0) as value
        FROM work_order_quotes
        WHERE status = 'Pending'
        AND JULIANDAY('now') - JULIANDAY(created_at) > 14
    ''').fetchone()
    
    if overdue_quotes['count'] > 0:
        alerts.append({
            'type': 'warning',
            'icon': 'clock-history',
            'title': 'Quote aging beyond SLA',
            'message': f'{overdue_quotes["count"]} quotes (${overdue_quotes["value"]:,.2f}) pending > 14 days'
        })
    
    capacity_overload = conn.execute('''
        SELECT COUNT(*) as count
        FROM work_orders
        WHERE status = 'In Progress'
        AND planned_end_date < date('now', '+7 days')
    ''').fetchone()
    
    if capacity_overload['count'] > 5:
        alerts.append({
            'type': 'danger',
            'icon': 'fire',
            'title': 'Capacity overload impacting revenue',
            'message': f'{capacity_overload["count"]} work orders due within 7 days'
        })
    
    if margin_pct < 25:
        alerts.append({
            'type': 'warning',
            'icon': 'graph-down-arrow',
            'title': 'Margin below target threshold',
            'message': f'YTD gross margin at {margin_pct:.1f}% (target: 25%)'
        })
    
    repeat_customers = conn.execute('''
        SELECT 
            COUNT(DISTINCT c.id) as total_customers,
            SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) as repeat_customers
        FROM (
            SELECT c.id, COUNT(so.id) as order_count
            FROM customers c
            LEFT JOIN sales_orders so ON so.customer_id = c.id
            GROUP BY c.id
        ) sub
        JOIN customers c ON c.id = sub.id
    ''').fetchone()
    
    repeat_rate = 0
    if repeat_customers['total_customers'] > 0:
        repeat_rate = (repeat_customers['repeat_customers'] or 0) / repeat_customers['total_customers'] * 100
    
    conn.close()
    
    kpis = {
        'mtd_revenue': mtd_revenue['revenue'],
        'qtd_revenue': qtd_revenue['revenue'],
        'ytd_revenue': ytd_revenue['revenue'],
        'ytd_growth': ytd_growth,
        'bookings': bookings['total'],
        'gross_margin_pct': margin_pct,
        'pipeline_value': pipeline_value['total'],
        'win_rate': win_rate,
        'avg_deal_cycle': quote_cycle['avg_days'] or 0,
        'revenue_at_risk': revenue_at_risk['total'],
        'capacity_utilization': (capacity_stats['in_progress'] or 0) / max(capacity_stats['total_wo'] or 1, 1) * 100,
        'quote_turnaround': avg_quote_turnaround['avg_days'] or 0,
        'acceptance_rate': acceptance_rate,
        'dso': dso['avg_days'] or 0,
        'ar_outstanding': ar_outstanding['balance'],
        'ar_overdue': ar_overdue['balance'],
        'repeat_rate': repeat_rate
    }
    
    forecasts = {
        '30_day': forecast_30['forecast'],
        '60_day': forecast_60['forecast'],
        '90_day': forecast_90['forecast']
    }
    
    return render_template('sales_dashboard/executive.html',
                         kpis=kpis,
                         customer_revenue=customer_revenue,
                         revenue_by_type=revenue_by_type,
                         monthly_revenue=monthly_revenue,
                         pipeline_by_stage=pipeline_by_stage,
                         quote_aging=quote_aging,
                         upcoming_inductions=upcoming_inductions,
                         forecasts=forecasts,
                         alerts=alerts,
                         dates=dates)


@sales_dashboard_bp.route('/api/sales-copilot', methods=['POST'])
@login_required
def sales_copilot():
    """AI Sales Copilot for executive insights"""
    try:
        from openai import OpenAI
        client = OpenAI()
        
        question = request.json.get('question', '')
        
        db = Database()
        conn = db.get_connection()
        dates = get_date_ranges()
        
        context_data = {}
        
        context_data['ytd_revenue'] = conn.execute('''
            SELECT COALESCE(SUM(total_amount), 0) as revenue
            FROM invoices WHERE invoice_date >= ? AND status IN ('Posted', 'Paid')
        ''', (dates['ytd_start'],)).fetchone()['revenue']
        
        context_data['mtd_revenue'] = conn.execute('''
            SELECT COALESCE(SUM(total_amount), 0) as revenue
            FROM invoices WHERE invoice_date >= ? AND status IN ('Posted', 'Paid')
        ''', (dates['mtd_start'],)).fetchone()['revenue']
        
        context_data['prev_month_revenue'] = conn.execute('''
            SELECT COALESCE(SUM(total_amount), 0) as revenue
            FROM invoices 
            WHERE invoice_date >= date('now', 'start of month', '-1 month')
            AND invoice_date < date('now', 'start of month')
            AND status IN ('Posted', 'Paid')
        ''').fetchone()['revenue']
        
        context_data['pipeline'] = conn.execute('''
            SELECT COALESCE(SUM(total_amount), 0) as value, COUNT(*) as count
            FROM sales_orders WHERE status IN ('Draft', 'Pending', 'Confirmed')
        ''').fetchone()
        
        context_data['top_customers'] = conn.execute('''
            SELECT c.name, COALESCE(SUM(i.total_amount), 0) as revenue
            FROM customers c
            JOIN invoices i ON i.customer_id = c.id
            WHERE i.invoice_date >= ? AND i.status IN ('Posted', 'Paid')
            GROUP BY c.id ORDER BY revenue DESC LIMIT 5
        ''', (dates['ytd_start'],)).fetchall()
        
        context_data['wo_capacity'] = conn.execute('''
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'In Progress' THEN 1 ELSE 0 END) as in_progress,
                SUM(CASE WHEN status = 'Planned' AND planned_start_date <= date('now', '+60 days') THEN 1 ELSE 0 END) as upcoming
            FROM work_orders WHERE status NOT IN ('Completed', 'Cancelled')
        ''').fetchone()
        
        context_data['high_value_deals'] = conn.execute('''
            SELECT so.so_number, so.total_amount, c.name, so.status
            FROM sales_orders so
            JOIN customers c ON so.customer_id = c.id
            WHERE so.status NOT IN ('Completed', 'Cancelled', 'Closed')
            ORDER BY so.total_amount DESC LIMIT 5
        ''').fetchall()
        
        conn.close()
        
        context_str = f"""
Current Business Metrics:
- YTD Revenue: ${context_data['ytd_revenue']:,.2f}
- MTD Revenue: ${context_data['mtd_revenue']:,.2f}
- Previous Month Revenue: ${context_data['prev_month_revenue']:,.2f}
- Pipeline Value: ${context_data['pipeline']['value']:,.2f} ({context_data['pipeline']['count']} orders)
- Active Work Orders: {context_data['wo_capacity']['in_progress']} in progress, {context_data['wo_capacity']['upcoming']} upcoming in 60 days

Top 5 Customers by Revenue:
{chr(10).join([f"- {c['name']}: ${c['revenue']:,.2f}" for c in context_data['top_customers']])}

High Value Deals:
{chr(10).join([f"- {d['so_number']}: ${d['total_amount']:,.2f} ({d['name']}) - {d['status']}" for d in context_data['high_value_deals']])}
"""
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": """You are an Executive Sales Copilot for an Aviation MRO company. 
Provide concise, data-backed, action-oriented responses.
Focus on strategic insights for C-suite executives.
Keep responses under 200 words.
Use bullet points for clarity.
Always reference specific numbers from the data provided."""
                },
                {
                    "role": "user",
                    "content": f"Business Context:\n{context_str}\n\nExecutive Question: {question}"
                }
            ],
            max_tokens=500,
            temperature=0.7
        )
        
        return jsonify({
            'success': True,
            'response': response.choices[0].message.content
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
