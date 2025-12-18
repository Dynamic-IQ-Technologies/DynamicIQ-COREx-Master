"""
Business Analytics AI Super Agent Routes
Autonomous intelligent manager for ERP process optimization and predictive analytics
"""
from flask import Blueprint, render_template, request, jsonify, session
from models import Database
from auth import login_required, role_required
from datetime import datetime, timedelta
import json
import os

business_analytics_bp = Blueprint('business_analytics_routes', __name__, url_prefix='/business-analytics')

def get_openai_client():
    """Get OpenAI client using Replit AI integration"""
    try:
        from openai import OpenAI
        return OpenAI(
            api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
            base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
        )
    except Exception as e:
        print(f"OpenAI client error: {e}")
        return None

@business_analytics_bp.route('/')
@login_required
@role_required('Admin', 'Accountant', 'Planner')
def dashboard():
    """Main Business Analytics Dashboard"""
    db = Database()
    conn = db.get_connection()
    
    today = datetime.now().date()
    month_start = today.replace(day=1)
    last_month_start = (month_start - timedelta(days=1)).replace(day=1)
    
    kpis = {
        'revenue': get_revenue_kpis(conn, month_start, last_month_start),
        'operations': get_operations_kpis(conn),
        'inventory': get_inventory_kpis(conn),
        'sales': get_sales_kpis(conn),
        'finance': get_finance_kpis(conn),
        'workforce': get_workforce_kpis(conn)
    }
    
    alerts = get_active_alerts(conn)
    
    recent_trends = get_trend_data(conn)
    
    process_health = get_process_health(conn)
    
    conn.close()
    
    return render_template('business_analytics/dashboard.html',
                         kpis=kpis,
                         alerts=alerts,
                         trends=recent_trends,
                         process_health=process_health)

@business_analytics_bp.route('/api/analyze', methods=['POST'])
@login_required
@role_required('Admin', 'Accountant', 'Planner')
def analyze():
    """AI-powered analysis endpoint"""
    data = request.get_json()
    query = data.get('query', '')
    analysis_type = data.get('type', 'general')
    
    if not query:
        return jsonify({'success': False, 'error': 'Query required'})
    
    db = Database()
    conn = db.get_connection()
    
    context = gather_analysis_context(conn, analysis_type)
    
    conn.close()
    
    client = get_openai_client()
    if not client:
        return jsonify({'success': False, 'error': 'AI service unavailable'})
    
    try:
        system_prompt = """You are the Business Analytics AI Super Agent for Dynamic.IQ-MRPx ERP system.
You are an autonomous, intelligent manager that analyzes, optimizes, and predicts business processes.

Your capabilities include:
- Process monitoring and bottleneck identification
- Data analysis with trend and variance analysis
- Predictive and prescriptive analytics
- Autonomous decision support with actionable recommendations
- KPI tracking across Finance, Supply Chain, MRO, HR, Sales, and Operations

Guidelines:
- Be proactive and solution-oriented
- Provide concise, clear, and actionable insights
- Prioritize high-impact recommendations
- Include specific numbers and metrics when available
- Format responses with clear sections and bullet points
- Always justify recommendations with data"""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"""
ERP Context Data:
{json.dumps(context, indent=2)}

User Query: {query}

Provide a comprehensive analysis with:
1. Current Situation Assessment
2. Key Findings
3. Root Cause Analysis (if applicable)
4. Actionable Recommendations
5. Predicted Impact/Outcomes
"""}
            ],
            max_tokens=2000,
            temperature=0.7
        )
        
        analysis = response.choices[0].message.content
        
        return jsonify({
            'success': True,
            'analysis': analysis,
            'context': analysis_type,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@business_analytics_bp.route('/api/predict', methods=['POST'])
@login_required
@role_required('Admin', 'Accountant', 'Planner')
def predict():
    """Predictive analytics endpoint"""
    data = request.get_json()
    metric = data.get('metric', 'revenue')
    horizon = data.get('horizon', 30)
    
    db = Database()
    conn = db.get_connection()
    
    historical_data = get_historical_data(conn, metric, 90)
    
    conn.close()
    
    client = get_openai_client()
    if not client:
        return jsonify({'success': False, 'error': 'AI service unavailable'})
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": """You are a predictive analytics engine. 
Analyze historical data and provide forecasts with confidence intervals.
Return your response in JSON format with keys: forecast, confidence, factors, risks, recommendations."""},
                {"role": "user", "content": f"""
Analyze this historical {metric} data and predict the next {horizon} days:

Historical Data (last 90 days):
{json.dumps(historical_data, indent=2)}

Provide forecast in JSON format."""}
            ],
            max_tokens=1500,
            temperature=0.5
        )
        
        return jsonify({
            'success': True,
            'prediction': response.choices[0].message.content,
            'metric': metric,
            'horizon': horizon
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@business_analytics_bp.route('/api/executive-summary')
@login_required
@role_required('Admin', 'Accountant', 'Planner')
def executive_summary():
    """Generate daily executive summary"""
    db = Database()
    conn = db.get_connection()
    
    summary_data = {
        'revenue': get_revenue_kpis(conn, datetime.now().date().replace(day=1), None),
        'operations': get_operations_kpis(conn),
        'inventory': get_inventory_kpis(conn),
        'sales': get_sales_kpis(conn),
        'alerts': get_active_alerts(conn),
        'process_health': get_process_health(conn)
    }
    
    conn.close()
    
    client = get_openai_client()
    if not client:
        return jsonify({'success': False, 'error': 'AI service unavailable'})
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": """You are generating a concise executive summary for a CEO.
Focus on:
- Key performance highlights
- Critical issues requiring attention
- Opportunities identified
- Recommended actions

Keep it brief, impactful, and action-oriented. Use bullet points."""},
                {"role": "user", "content": f"""
Generate today's executive summary based on this ERP data:

{json.dumps(summary_data, indent=2)}

Date: {datetime.now().strftime('%B %d, %Y')}"""}
            ],
            max_tokens=1000,
            temperature=0.6
        )
        
        return jsonify({
            'success': True,
            'summary': response.choices[0].message.content,
            'generated_at': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@business_analytics_bp.route('/api/process-optimization', methods=['POST'])
@login_required
@role_required('Admin', 'Planner')
def process_optimization():
    """Analyze and recommend process optimizations"""
    data = request.get_json()
    process_area = data.get('area', 'all')
    
    db = Database()
    conn = db.get_connection()
    
    process_data = get_process_metrics(conn, process_area)
    
    conn.close()
    
    client = get_openai_client()
    if not client:
        return jsonify({'success': False, 'error': 'AI service unavailable'})
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": """You are a process optimization expert.
Analyze ERP workflow data to identify:
1. Bottlenecks and inefficiencies
2. Cost reduction opportunities
3. Automation potential
4. Resource optimization
5. Compliance gaps

Provide specific, actionable recommendations with estimated impact."""},
                {"role": "user", "content": f"""
Analyze the {process_area} processes and provide optimization recommendations:

Process Metrics:
{json.dumps(process_data, indent=2)}"""}
            ],
            max_tokens=1500,
            temperature=0.7
        )
        
        return jsonify({
            'success': True,
            'recommendations': response.choices[0].message.content,
            'area': process_area
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@business_analytics_bp.route('/kpi-details/<category>')
@login_required
@role_required('Admin', 'Accountant', 'Planner')
def kpi_details(category):
    """Detailed KPI breakdown by category"""
    db = Database()
    conn = db.get_connection()
    
    today = datetime.now().date()
    month_start = today.replace(day=1)
    
    details = {}
    
    if category == 'revenue':
        details = get_detailed_revenue(conn, month_start)
    elif category == 'operations':
        details = get_detailed_operations(conn)
    elif category == 'inventory':
        details = get_detailed_inventory(conn)
    elif category == 'sales':
        details = get_detailed_sales(conn)
    elif category == 'finance':
        details = get_detailed_finance(conn)
    elif category == 'workforce':
        details = get_detailed_workforce(conn)
    
    conn.close()
    
    return render_template('business_analytics/kpi_details.html',
                         category=category,
                         details=details)

def get_revenue_kpis(conn, month_start, last_month_start):
    """Get revenue-related KPIs"""
    current = conn.execute('''
        SELECT 
            COALESCE(SUM(total_amount), 0) as revenue,
            COUNT(*) as orders
        FROM sales_orders
        WHERE status NOT IN ('Cancelled', 'Draft')
        AND order_date >= ?
    ''', (month_start.isoformat(),)).fetchone()
    
    previous = None
    if last_month_start:
        previous = conn.execute('''
            SELECT COALESCE(SUM(total_amount), 0) as revenue
            FROM sales_orders
            WHERE status NOT IN ('Cancelled', 'Draft')
            AND order_date >= ? AND order_date < ?
        ''', (last_month_start.isoformat(), month_start.isoformat())).fetchone()
    
    change = 0
    if previous and previous['revenue'] > 0:
        change = ((current['revenue'] - previous['revenue']) / previous['revenue']) * 100
    
    return {
        'current': current['revenue'],
        'orders': current['orders'],
        'previous': previous['revenue'] if previous else 0,
        'change': change
    }

def get_operations_kpis(conn):
    """Get operations-related KPIs"""
    wo_stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'In Progress' THEN 1 ELSE 0 END) as in_progress,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN planned_end_date < date('now') AND status NOT IN ('Completed', 'Cancelled') THEN 1 ELSE 0 END) as overdue
        FROM work_orders
        WHERE status != 'Cancelled'
    ''').fetchone()
    
    return {
        'total_wo': wo_stats['total'],
        'in_progress': wo_stats['in_progress'],
        'completed': wo_stats['completed'],
        'overdue': wo_stats['overdue'],
        'completion_rate': (wo_stats['completed'] / wo_stats['total'] * 100) if wo_stats['total'] > 0 else 0
    }

def get_inventory_kpis(conn):
    """Get inventory-related KPIs"""
    inv_stats = conn.execute('''
        SELECT 
            COUNT(*) as total_products,
            SUM(CASE WHEN COALESCE(i.quantity, 0) <= COALESCE(i.reorder_point, 0) THEN 1 ELSE 0 END) as low_stock,
            SUM(CASE WHEN COALESCE(i.quantity, 0) = 0 THEN 1 ELSE 0 END) as out_of_stock,
            COALESCE(SUM(COALESCE(i.quantity, 0) * COALESCE(p.cost, 0)), 0) as total_value
        FROM products p
        LEFT JOIN inventory i ON p.id = i.product_id
    ''').fetchone()
    
    return {
        'total_products': inv_stats['total_products'],
        'low_stock': inv_stats['low_stock'] or 0,
        'out_of_stock': inv_stats['out_of_stock'] or 0,
        'total_value': inv_stats['total_value']
    }

def get_sales_kpis(conn):
    """Get sales-related KPIs"""
    sales_stats = conn.execute('''
        SELECT 
            COUNT(*) as total_orders,
            SUM(CASE WHEN status = 'Draft' THEN 1 ELSE 0 END) as pending,
            SUM(CASE WHEN status = 'Shipped' THEN 1 ELSE 0 END) as shipped,
            COALESCE(SUM(balance_due), 0) as outstanding_ar
        FROM sales_orders
        WHERE status != 'Cancelled'
    ''').fetchone()
    
    return {
        'total_orders': sales_stats['total_orders'],
        'pending': sales_stats['pending'],
        'shipped': sales_stats['shipped'],
        'outstanding_ar': sales_stats['outstanding_ar']
    }

def get_finance_kpis(conn):
    """Get finance-related KPIs"""
    ar = conn.execute('''
        SELECT COALESCE(SUM(balance_due), 0) as ar
        FROM invoices WHERE status != 'Paid'
    ''').fetchone()
    
    ap = conn.execute('''
        SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) as ap
        FROM purchase_orders po
        JOIN purchase_order_lines pol ON po.id = pol.po_id
        WHERE po.status NOT IN ('Cancelled', 'Received')
    ''').fetchone()
    
    return {
        'accounts_receivable': ar['ar'],
        'accounts_payable': ap['ap'],
        'net_position': ar['ar'] - ap['ap']
    }

def get_workforce_kpis(conn):
    """Get workforce-related KPIs"""
    labor_stats = conn.execute('''
        SELECT 
            COUNT(*) as total_resources,
            SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active
        FROM labor_resources
    ''').fetchone()
    
    return {
        'total_resources': labor_stats['total_resources'],
        'active': labor_stats['active'] or 0,
        'utilization': 0
    }

def get_active_alerts(conn):
    """Get current system alerts"""
    alerts = []
    
    low_stock = conn.execute('''
        SELECT COUNT(*) as count FROM products p
        LEFT JOIN inventory i ON p.id = i.product_id
        WHERE COALESCE(i.quantity, 0) <= COALESCE(i.reorder_point, 0)
    ''').fetchone()
    if low_stock['count'] > 0:
        alerts.append({
            'type': 'warning',
            'category': 'Inventory',
            'message': f"{low_stock['count']} products at or below reorder point",
            'priority': 'high'
        })
    
    overdue_wo = conn.execute('''
        SELECT COUNT(*) as count FROM work_orders
        WHERE planned_end_date < date('now') 
        AND status NOT IN ('Completed', 'Cancelled')
    ''').fetchone()
    if overdue_wo['count'] > 0:
        alerts.append({
            'type': 'danger',
            'category': 'Operations',
            'message': f"{overdue_wo['count']} work orders are overdue",
            'priority': 'critical'
        })
    
    pending_po = conn.execute('''
        SELECT COUNT(*) as count FROM purchase_orders
        WHERE status = 'Pending' AND expected_date < date('now')
    ''').fetchone()
    if pending_po['count'] > 0:
        alerts.append({
            'type': 'warning',
            'category': 'Procurement',
            'message': f"{pending_po['count']} purchase orders past expected date",
            'priority': 'medium'
        })
    
    outstanding = conn.execute('''
        SELECT COUNT(*) as count, COALESCE(SUM(balance_due), 0) as total
        FROM invoices WHERE status != 'Paid' AND due_date < date('now')
    ''').fetchone()
    if outstanding['count'] > 0:
        alerts.append({
            'type': 'warning',
            'category': 'Finance',
            'message': f"${outstanding['total']:,.2f} in overdue invoices ({outstanding['count']} invoices)",
            'priority': 'high'
        })
    
    return alerts

def get_trend_data(conn):
    """Get trend data for charts"""
    trends = conn.execute('''
        SELECT 
            strftime('%Y-%m', order_date) as month,
            COALESCE(SUM(total_amount), 0) as revenue,
            COUNT(*) as orders
        FROM sales_orders
        WHERE status NOT IN ('Cancelled', 'Draft')
        GROUP BY strftime('%Y-%m', order_date)
        ORDER BY month DESC
        LIMIT 12
    ''').fetchall()
    
    return [dict(t) for t in reversed(trends)]

def get_process_health(conn):
    """Calculate overall process health score"""
    scores = []
    
    wo_health = conn.execute('''
        SELECT 
            CASE 
                WHEN COUNT(*) = 0 THEN 100
                ELSE (1.0 - (SUM(CASE WHEN planned_end_date < date('now') AND status NOT IN ('Completed', 'Cancelled') THEN 1 ELSE 0 END) * 1.0 / COUNT(*))) * 100
            END as score
        FROM work_orders WHERE status != 'Cancelled'
    ''').fetchone()
    scores.append({'name': 'Operations', 'score': wo_health['score'], 'color': '#007bff'})
    
    inv_health = conn.execute('''
        SELECT 
            CASE 
                WHEN COUNT(*) = 0 THEN 100
                ELSE (1.0 - (SUM(CASE WHEN COALESCE(i.quantity, 0) <= COALESCE(i.reorder_point, 0) THEN 1 ELSE 0 END) * 1.0 / COUNT(*))) * 100
            END as score
        FROM products p
        LEFT JOIN inventory i ON p.id = i.product_id
    ''').fetchone()
    scores.append({'name': 'Inventory', 'score': inv_health['score'], 'color': '#28a745'})
    
    sales_health = conn.execute('''
        SELECT 
            CASE 
                WHEN COUNT(*) = 0 THEN 100
                ELSE (SUM(CASE WHEN status IN ('Shipped', 'Invoiced', 'Closed') THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
            END as score
        FROM sales_orders WHERE status != 'Cancelled'
    ''').fetchone()
    scores.append({'name': 'Sales', 'score': sales_health['score'], 'color': '#fd7e14'})
    
    overall = sum(s['score'] for s in scores) / len(scores) if scores else 0
    
    return {
        'overall': overall,
        'components': scores
    }

def gather_analysis_context(conn, analysis_type):
    """Gather context data for AI analysis"""
    context = {}
    
    if analysis_type in ['general', 'sales']:
        context['sales'] = dict(conn.execute('''
            SELECT 
                COUNT(*) as total_orders,
                COALESCE(SUM(total_amount), 0) as total_revenue,
                COALESCE(AVG(total_amount), 0) as avg_order_value
            FROM sales_orders WHERE status NOT IN ('Cancelled', 'Draft')
        ''').fetchone())
    
    if analysis_type in ['general', 'operations']:
        context['operations'] = dict(conn.execute('''
            SELECT 
                COUNT(*) as total_wo,
                SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'In Progress' THEN 1 ELSE 0 END) as in_progress,
                SUM(CASE WHEN planned_end_date < date('now') AND status NOT IN ('Completed', 'Cancelled') THEN 1 ELSE 0 END) as overdue
            FROM work_orders WHERE status != 'Cancelled'
        ''').fetchone())
    
    if analysis_type in ['general', 'inventory']:
        context['inventory'] = dict(conn.execute('''
            SELECT 
                COUNT(*) as total_products,
                SUM(CASE WHEN COALESCE(i.quantity, 0) <= COALESCE(i.reorder_point, 0) THEN 1 ELSE 0 END) as low_stock,
                COALESCE(SUM(COALESCE(i.quantity, 0) * COALESCE(p.cost, 0)), 0) as total_value
            FROM products p
            LEFT JOIN inventory i ON p.id = i.product_id
        ''').fetchone())
    
    if analysis_type in ['general', 'finance']:
        ap_result = conn.execute('''
            SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) as total 
            FROM purchase_orders po
            LEFT JOIN purchase_order_lines pol ON po.id = pol.po_id
            WHERE po.status NOT IN ('Cancelled', 'Received')
        ''').fetchone()
        context['finance'] = {
            'ar': conn.execute('SELECT COALESCE(SUM(balance_due), 0) as total FROM invoices WHERE status != "Paid"').fetchone()['total'],
            'ap': ap_result['total']
        }
    
    return context

def get_historical_data(conn, metric, days):
    """Get historical data for predictions"""
    if metric == 'revenue':
        data = conn.execute('''
            SELECT 
                date(order_date) as date,
                COALESCE(SUM(total_amount), 0) as value
            FROM sales_orders
            WHERE order_date >= date('now', ?)
            AND status NOT IN ('Cancelled', 'Draft')
            GROUP BY date(order_date)
            ORDER BY date
        ''', (f'-{days} days',)).fetchall()
    elif metric == 'orders':
        data = conn.execute('''
            SELECT 
                date(order_date) as date,
                COUNT(*) as value
            FROM sales_orders
            WHERE order_date >= date('now', ?)
            AND status NOT IN ('Cancelled', 'Draft')
            GROUP BY date(order_date)
            ORDER BY date
        ''', (f'-{days} days',)).fetchall()
    else:
        data = []
    
    return [dict(d) for d in data]

def get_process_metrics(conn, area):
    """Get process metrics for optimization analysis"""
    metrics = {}
    
    if area in ['all', 'operations']:
        metrics['work_orders'] = dict(conn.execute('''
            SELECT 
                AVG(julianday(actual_end_date) - julianday(actual_start_date)) as avg_cycle_time,
                AVG(CASE WHEN actual_end_date > planned_end_date THEN julianday(actual_end_date) - julianday(planned_end_date) ELSE 0 END) as avg_delay,
                COALESCE(AVG(material_cost + labor_cost + overhead_cost), 0) as avg_cost
            FROM work_orders 
            WHERE status = 'Completed' AND actual_end_date IS NOT NULL
        ''').fetchone())
    
    if area in ['all', 'procurement']:
        metrics['purchase_orders'] = dict(conn.execute('''
            SELECT 
                COUNT(*) as total,
                AVG(julianday(received_date) - julianday(order_date)) as avg_lead_time
            FROM purchase_orders 
            WHERE status = 'Received' AND received_date IS NOT NULL
        ''').fetchone())
    
    return metrics

def get_detailed_revenue(conn, month_start):
    """Get detailed revenue breakdown"""
    by_type = conn.execute('''
        SELECT 
            sales_type,
            COUNT(*) as orders,
            COALESCE(SUM(total_amount), 0) as revenue
        FROM sales_orders
        WHERE status NOT IN ('Cancelled', 'Draft') AND order_date >= ?
        GROUP BY sales_type
        ORDER BY revenue DESC
    ''', (month_start.isoformat(),)).fetchall()
    
    by_customer = conn.execute('''
        SELECT 
            c.name,
            COUNT(*) as orders,
            COALESCE(SUM(so.total_amount), 0) as revenue
        FROM sales_orders so
        JOIN customers c ON so.customer_id = c.id
        WHERE so.status NOT IN ('Cancelled', 'Draft') AND so.order_date >= ?
        GROUP BY c.id
        ORDER BY revenue DESC
        LIMIT 10
    ''', (month_start.isoformat(),)).fetchall()
    
    return {
        'by_type': [dict(r) for r in by_type],
        'by_customer': [dict(r) for r in by_customer]
    }

def get_detailed_operations(conn):
    """Get detailed operations breakdown"""
    by_status = conn.execute('''
        SELECT status, COUNT(*) as count
        FROM work_orders WHERE status != 'Cancelled'
        GROUP BY status
    ''').fetchall()
    
    return {'by_status': [dict(r) for r in by_status]}

def get_detailed_inventory(conn):
    """Get detailed inventory breakdown"""
    low_stock = conn.execute('''
        SELECT p.name, p.code as sku, COALESCE(i.quantity, 0) as quantity_on_hand, COALESCE(i.reorder_point, 0) as reorder_point
        FROM products p
        LEFT JOIN inventory i ON p.id = i.product_id
        WHERE COALESCE(i.quantity, 0) <= COALESCE(i.reorder_point, 0)
        ORDER BY i.quantity
        LIMIT 20
    ''').fetchall()
    
    return {'low_stock_items': [dict(r) for r in low_stock]}

def get_detailed_sales(conn):
    """Get detailed sales breakdown"""
    pipeline = conn.execute('''
        SELECT status, COUNT(*) as count, COALESCE(SUM(total_amount), 0) as value
        FROM sales_orders
        WHERE status NOT IN ('Cancelled', 'Closed')
        GROUP BY status
    ''').fetchall()
    
    return {'pipeline': [dict(r) for r in pipeline]}

def get_detailed_finance(conn):
    """Get detailed finance breakdown"""
    ar_aging = conn.execute('''
        SELECT 
            CASE 
                WHEN julianday('now') - julianday(due_date) <= 0 THEN 'Current'
                WHEN julianday('now') - julianday(due_date) <= 30 THEN '1-30 Days'
                WHEN julianday('now') - julianday(due_date) <= 60 THEN '31-60 Days'
                ELSE '60+ Days'
            END as aging_bucket,
            COUNT(*) as count,
            COALESCE(SUM(balance_due), 0) as amount
        FROM invoices
        WHERE status != 'Paid'
        GROUP BY aging_bucket
    ''').fetchall()
    
    return {'ar_aging': [dict(r) for r in ar_aging]}

def get_detailed_workforce(conn):
    """Get detailed workforce breakdown"""
    by_skill = conn.execute('''
        SELECT s.name as skill, COUNT(*) as count
        FROM labor_resource_skillsets lrs
        JOIN skillsets s ON lrs.skillset_id = s.id
        GROUP BY s.id
        ORDER BY count DESC
    ''').fetchall()
    
    return {'by_skill': [dict(r) for r in by_skill]}
