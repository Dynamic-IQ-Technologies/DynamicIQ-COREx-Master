from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database, safe_float
from datetime import datetime, timedelta
import json
import os
from openai import OpenAI

org_analyzer_bp = Blueprint('org_analyzer_routes', __name__)

def get_openai_client():
    """Get OpenAI client configured with Replit AI Integrations"""
    return OpenAI(
        api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
        base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
    )

def calculate_financial_kpis(conn):
    """Calculate financial KPIs from system data"""
    kpis = {}
    
    today = datetime.now().strftime('%Y-%m-%d')
    month_start = datetime.now().replace(day=1).strftime('%Y-%m-%d')
    last_month_start = (datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')
    last_month_end = (datetime.now().replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')
    
    total_revenue = conn.execute('''
        SELECT COALESCE(SUM(total_amount), 0) as total FROM sales_orders 
        WHERE status NOT IN ('Draft', 'Cancelled')
    ''').fetchone()['total']
    
    mtd_revenue = conn.execute('''
        SELECT COALESCE(SUM(total_amount), 0) as total FROM sales_orders 
        WHERE order_date >= ? AND status NOT IN ('Draft', 'Cancelled')
    ''', (month_start,)).fetchone()['total']
    
    last_month_revenue = conn.execute('''
        SELECT COALESCE(SUM(total_amount), 0) as total FROM sales_orders 
        WHERE order_date >= ? AND order_date <= ? AND status NOT IN ('Draft', 'Cancelled')
    ''', (last_month_start, last_month_end)).fetchone()['total']
    
    revenue_growth = 0
    if last_month_revenue > 0:
        revenue_growth = ((mtd_revenue - last_month_revenue) / last_month_revenue) * 100
    
    total_material_cost = conn.execute('''
        SELECT COALESCE(SUM(material_cost), 0) as total FROM work_orders
    ''').fetchone()['total']
    
    total_labor_cost = conn.execute('''
        SELECT COALESCE(SUM(labor_cost), 0) as total FROM work_orders
    ''').fetchone()['total']
    
    gross_margin = 0
    if total_revenue > 0:
        gross_margin = ((total_revenue - total_material_cost - total_labor_cost) / total_revenue) * 100
    
    open_po_value = conn.execute('''
        SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) as total 
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        WHERE po.status IN ('Draft', 'Submitted', 'Approved', 'Partial')
    ''').fetchone()['total']
    
    inventory_value = conn.execute('''
        SELECT COALESCE(SUM(i.quantity * COALESCE(i.unit_cost, p.cost, 0)), 0) as total 
        FROM inventory i
        JOIN products p ON i.product_id = p.id
    ''').fetchone()['total']
    
    ar_outstanding = conn.execute('''
        SELECT COALESCE(SUM(total_amount - COALESCE(amount_paid, 0)), 0) as total 
        FROM invoices WHERE status NOT IN ('Paid', 'Cancelled', 'Voided')
    ''').fetchone()['total']
    
    ap_outstanding = conn.execute('''
        SELECT COALESCE(SUM(total_amount - COALESCE(amount_paid, 0)), 0) as total 
        FROM vendor_invoices WHERE status NOT IN ('Paid', 'Cancelled')
    ''').fetchone()['total']
    
    kpis['total_revenue'] = total_revenue
    kpis['mtd_revenue'] = mtd_revenue
    kpis['revenue_growth'] = revenue_growth
    kpis['gross_margin'] = gross_margin
    kpis['total_material_cost'] = total_material_cost
    kpis['total_labor_cost'] = total_labor_cost
    kpis['open_po_value'] = open_po_value
    kpis['inventory_value'] = inventory_value
    kpis['ar_outstanding'] = ar_outstanding
    kpis['ap_outstanding'] = ap_outstanding
    kpis['net_working_capital'] = ar_outstanding + inventory_value - ap_outstanding - open_po_value
    
    return kpis

def calculate_operational_kpis(conn):
    """Calculate operational KPIs from system data"""
    kpis = {}
    
    active_wo = conn.execute('''
        SELECT COUNT(*) as count FROM work_orders 
        WHERE status IN ('Open', 'In Progress', 'Pending Material')
    ''').fetchone()['count']
    
    completed_wo = conn.execute('''
        SELECT COUNT(*) as count FROM work_orders WHERE status = 'Completed'
    ''').fetchone()['count']
    
    total_wo = conn.execute('SELECT COUNT(*) as count FROM work_orders').fetchone()['count']
    
    wo_completion_rate = 0
    if total_wo > 0:
        wo_completion_rate = (completed_wo / total_wo) * 100
    
    on_time_wo = conn.execute('''
        SELECT COUNT(*) as count FROM work_orders 
        WHERE status = 'Completed' AND actual_end_date <= planned_end_date
    ''').fetchone()['count']
    
    on_time_rate = 0
    if completed_wo > 0:
        on_time_rate = (on_time_wo / completed_wo) * 100
    
    overdue_wo = conn.execute('''
        SELECT COUNT(*) as count FROM work_orders 
        WHERE status NOT IN ('Completed', 'Closed', 'Cancelled') 
        AND planned_end_date < date('now')
    ''').fetchone()['count']
    
    low_stock_items = conn.execute('''
        SELECT COUNT(*) as count FROM inventory 
        WHERE quantity <= reorder_point AND reorder_point > 0
    ''').fetchone()['count']
    
    stockout_items = conn.execute('''
        SELECT COUNT(*) as count FROM inventory WHERE quantity <= 0
    ''').fetchone()['count']
    
    open_po = conn.execute('''
        SELECT COUNT(*) as count FROM purchase_orders 
        WHERE status IN ('Draft', 'Submitted', 'Approved', 'Partial')
    ''').fetchone()['count']
    
    pending_shipments = conn.execute('''
        SELECT COUNT(*) as count FROM sales_orders 
        WHERE status IN ('Ready to Ship', 'Released')
    ''').fetchone()['count']
    
    kpis['active_work_orders'] = active_wo
    kpis['completed_work_orders'] = completed_wo
    kpis['wo_completion_rate'] = wo_completion_rate
    kpis['on_time_delivery_rate'] = on_time_rate
    kpis['overdue_work_orders'] = overdue_wo
    kpis['low_stock_items'] = low_stock_items
    kpis['stockout_items'] = stockout_items
    kpis['open_purchase_orders'] = open_po
    kpis['pending_shipments'] = pending_shipments
    
    return kpis

def calculate_workforce_kpis(conn):
    """Calculate workforce KPIs from system data"""
    kpis = {}
    
    total_employees = conn.execute('''
        SELECT COUNT(*) as count FROM labor_resources WHERE status = 'Active'
    ''').fetchone()['count']
    
    today = datetime.now().strftime('%Y-%m-%d')
    clocked_in = conn.execute('''
        SELECT COUNT(DISTINCT employee_id) as count FROM time_clock_punches 
        WHERE date(punch_time) = ? AND punch_type = 'Clock In'
        AND employee_id NOT IN (
            SELECT employee_id FROM time_clock_punches 
            WHERE date(punch_time) = ? AND punch_type = 'Clock Out'
        )
    ''', (today, today)).fetchone()['count']
    
    month_start = datetime.now().replace(day=1).strftime('%Y-%m-%d')
    # Use database-specific syntax for time calculations
    is_postgres = os.environ.get('REPLIT_DEPLOYMENT') == '1'
    if is_postgres:
        total_hours_mtd = conn.execute('''
            SELECT COALESCE(SUM(
                CASE WHEN punch_type = 'Clock Out' THEN 
                    EXTRACT(EPOCH FROM (punch_time - (
                        SELECT MAX(punch_time) FROM time_clock_punches p2 
                        WHERE p2.employee_id = time_clock_punches.employee_id 
                        AND p2.punch_type = 'Clock In' 
                        AND p2.punch_time < time_clock_punches.punch_time
                    ))) / 3600.0
                ELSE 0 END
            ), 0) as hours FROM time_clock_punches WHERE punch_time::date >= ?
        ''', (month_start,)).fetchone()['hours']
    else:
        total_hours_mtd = conn.execute('''
            SELECT COALESCE(SUM(
                CASE WHEN punch_type = 'Clock Out' THEN 
                    (julianday(punch_time) - julianday((
                        SELECT MAX(punch_time) FROM time_clock_punches p2 
                        WHERE p2.employee_id = time_clock_punches.employee_id 
                        AND p2.punch_type = 'Clock In' 
                        AND p2.punch_time < time_clock_punches.punch_time
                    ))) * 24.0
                ELSE 0 END
            ), 0) as hours FROM time_clock_punches WHERE date(punch_time) >= ?
        ''', (month_start,)).fetchone()['hours']
    
    productivity_per_employee = 0
    if total_employees > 0 and total_hours_mtd > 0:
        mtd_revenue = conn.execute('''
            SELECT COALESCE(SUM(total_amount), 0) as total FROM sales_orders 
            WHERE order_date >= ? AND status NOT IN ('Draft', 'Cancelled')
        ''', (month_start,)).fetchone()['total']
        productivity_per_employee = mtd_revenue / total_employees
    
    avg_hourly_rate = conn.execute('''
        SELECT COALESCE(AVG(hourly_rate), 0) as avg FROM labor_resources WHERE status = 'Active'
    ''').fetchone()['avg']
    
    with_skills = conn.execute('''
        SELECT COUNT(DISTINCT labor_resource_id) as count FROM labor_resource_skills
    ''').fetchone()['count']
    
    skill_coverage = 0
    if total_employees > 0:
        skill_coverage = (with_skills / total_employees) * 100
    
    kpis['total_employees'] = total_employees
    kpis['currently_clocked_in'] = clocked_in
    kpis['hours_worked_mtd'] = round(total_hours_mtd, 1)
    kpis['productivity_per_employee'] = productivity_per_employee
    kpis['avg_hourly_rate'] = avg_hourly_rate
    kpis['skill_coverage'] = skill_coverage
    
    return kpis

def calculate_customer_kpis(conn):
    """Calculate customer KPIs from system data"""
    kpis = {}
    
    total_customers = conn.execute('SELECT COUNT(*) as count FROM customers').fetchone()['count']
    
    customers_with_orders = conn.execute('''
        SELECT COUNT(DISTINCT customer_id) as count FROM sales_orders
    ''').fetchone()['count']
    
    avg_order_value = conn.execute('''
        SELECT COALESCE(AVG(total_amount), 0) as avg FROM sales_orders 
        WHERE status NOT IN ('Draft', 'Cancelled')
    ''').fetchone()['avg']
    
    open_escalations = conn.execute('''
        SELECT COUNT(*) as count FROM order_escalations 
        WHERE status NOT IN ('Resolved', 'Closed')
    ''').fetchone()['count']
    
    avg_feedback = conn.execute('''
        SELECT COALESCE(AVG(rating), 0) as avg FROM customer_feedback
    ''').fetchone()['avg']
    
    overdue_orders = conn.execute('''
        SELECT COUNT(*) as count FROM sales_orders 
        WHERE status NOT IN ('Shipped', 'Delivered', 'Cancelled', 'Closed')
        AND expected_ship_date < date('now')
    ''').fetchone()['count']
    
    kpis['total_customers'] = total_customers
    kpis['active_customers'] = customers_with_orders
    kpis['avg_order_value'] = avg_order_value
    kpis['open_escalations'] = open_escalations
    kpis['avg_satisfaction_rating'] = avg_feedback
    kpis['overdue_orders'] = overdue_orders
    
    return kpis

def calculate_health_score(financial, operational, workforce, customer):
    """Calculate overall organizational health score (0-100)"""
    scores = {}
    
    financial_score = 50
    if financial['gross_margin'] > 30:
        financial_score += 20
    elif financial['gross_margin'] > 20:
        financial_score += 10
    if financial['revenue_growth'] > 0:
        financial_score += min(financial['revenue_growth'], 20)
    if financial['net_working_capital'] > 0:
        financial_score += 10
    scores['financial'] = min(100, max(0, financial_score))
    
    operational_score = 50
    if operational['wo_completion_rate'] > 80:
        operational_score += 20
    elif operational['wo_completion_rate'] > 60:
        operational_score += 10
    if operational['on_time_delivery_rate'] > 90:
        operational_score += 20
    elif operational['on_time_delivery_rate'] > 75:
        operational_score += 10
    operational_score -= operational['overdue_work_orders'] * 2
    operational_score -= operational['stockout_items'] * 3
    scores['operational'] = min(100, max(0, operational_score))
    
    workforce_score = 50
    if workforce['skill_coverage'] > 80:
        workforce_score += 25
    elif workforce['skill_coverage'] > 50:
        workforce_score += 10
    if workforce['total_employees'] > 0:
        workforce_score += min(25, workforce['productivity_per_employee'] / 1000)
    scores['workforce'] = min(100, max(0, workforce_score))
    
    customer_score = 50
    if customer['avg_satisfaction_rating'] >= 4:
        customer_score += 30
    elif customer['avg_satisfaction_rating'] >= 3:
        customer_score += 15
    customer_score -= customer['open_escalations'] * 5
    customer_score -= customer['overdue_orders'] * 2
    scores['customer'] = min(100, max(0, customer_score))
    
    scores['overall'] = (scores['financial'] * 0.3 + scores['operational'] * 0.3 + 
                         scores['workforce'] * 0.2 + scores['customer'] * 0.2)
    
    return scores

@org_analyzer_bp.route('/org-analyzer')
def dashboard():
    """Organizational Analyzer Executive Dashboard"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    financial = calculate_financial_kpis(conn)
    operational = calculate_operational_kpis(conn)
    workforce = calculate_workforce_kpis(conn)
    customer = calculate_customer_kpis(conn)
    health_scores = calculate_health_score(financial, operational, workforce, customer)
    
    alerts = conn.execute('''
        SELECT * FROM org_alerts 
        WHERE is_acknowledged = 0 
        ORDER BY severity DESC, created_at DESC 
        LIMIT 10
    ''').fetchall()
    
    recommendations = conn.execute('''
        SELECT * FROM org_recommendations 
        WHERE status = 'Pending' 
        ORDER BY priority DESC, created_at DESC 
        LIMIT 5
    ''').fetchall()
    
    forecasts = conn.execute('''
        SELECT * FROM org_forecasts 
        WHERE forecast_date >= date('now') 
        ORDER BY forecast_date ASC 
        LIMIT 10
    ''').fetchall()
    
    conn.close()
    
    return render_template('org_analyzer/dashboard.html',
                         financial=financial,
                         operational=operational,
                         workforce=workforce,
                         customer=customer,
                         health_scores=health_scores,
                         alerts=alerts,
                         recommendations=recommendations,
                         forecasts=forecasts)

@org_analyzer_bp.route('/org-analyzer/generate-insights', methods=['POST'])
def generate_insights():
    """Generate AI-powered insights and recommendations"""
    if 'user_id' not in session:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    
    db = Database()
    conn = db.get_connection()
    
    financial = calculate_financial_kpis(conn)
    operational = calculate_operational_kpis(conn)
    workforce = calculate_workforce_kpis(conn)
    customer = calculate_customer_kpis(conn)
    health_scores = calculate_health_score(financial, operational, workforce, customer)
    
    data_summary = f"""
ORGANIZATIONAL DATA SNAPSHOT
============================

FINANCIAL METRICS:
- Total Revenue: ${financial['total_revenue']:,.2f}
- Month-to-Date Revenue: ${financial['mtd_revenue']:,.2f}
- Revenue Growth: {financial['revenue_growth']:.1f}%
- Gross Margin: {financial['gross_margin']:.1f}%
- Inventory Value: ${financial['inventory_value']:,.2f}
- A/R Outstanding: ${financial['ar_outstanding']:,.2f}
- A/P Outstanding: ${financial['ap_outstanding']:,.2f}
- Net Working Capital: ${financial['net_working_capital']:,.2f}

OPERATIONAL METRICS:
- Active Work Orders: {operational['active_work_orders']}
- Completion Rate: {operational['wo_completion_rate']:.1f}%
- On-Time Delivery: {operational['on_time_delivery_rate']:.1f}%
- Overdue Work Orders: {operational['overdue_work_orders']}
- Low Stock Items: {operational['low_stock_items']}
- Stockouts: {operational['stockout_items']}
- Open Purchase Orders: {operational['open_purchase_orders']}

WORKFORCE METRICS:
- Total Employees: {workforce['total_employees']}
- Currently Clocked In: {workforce['currently_clocked_in']}
- Hours Worked MTD: {workforce['hours_worked_mtd']}
- Productivity/Employee: ${workforce['productivity_per_employee']:,.2f}
- Skill Coverage: {workforce['skill_coverage']:.1f}%

CUSTOMER METRICS:
- Total Customers: {customer['total_customers']}
- Active Customers: {customer['active_customers']}
- Average Order Value: ${customer['avg_order_value']:,.2f}
- Open Escalations: {customer['open_escalations']}
- Satisfaction Rating: {customer['avg_satisfaction_rating']:.1f}/5
- Overdue Orders: {customer['overdue_orders']}

HEALTH SCORES:
- Overall: {health_scores['overall']:.0f}/100
- Financial: {health_scores['financial']:.0f}/100
- Operational: {health_scores['operational']:.0f}/100
- Workforce: {health_scores['workforce']:.0f}/100
- Customer: {health_scores['customer']:.0f}/100
"""
    
    try:
        client = get_openai_client()
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": """You are an AI CEO / Organizational Intelligence Engine. Analyze the provided organizational data and generate:

1. EXECUTIVE SUMMARY (2-3 sentences on overall state)
2. KEY INSIGHTS (3-5 critical observations)
3. RISK ALERTS (any concerning metrics that need immediate attention)
4. STRATEGIC RECOMMENDATIONS (3-5 actionable recommendations with priority, impact, and confidence score 0-100)
5. 30-DAY FORECAST (brief prediction of key metrics)

Format as JSON with this structure:
{
    "executive_summary": "string",
    "insights": [{"title": "string", "description": "string", "category": "string"}],
    "alerts": [{"severity": "Critical|Warning|Info", "title": "string", "message": "string"}],
    "recommendations": [{"title": "string", "description": "string", "priority": "High|Medium|Low", "impact": "string", "risk_level": "Low|Medium|High", "time_to_value": "string", "confidence_score": number}],
    "forecast": {"summary": "string", "revenue_trend": "Up|Stable|Down", "risk_outlook": "Low|Medium|High"}
}"""
                },
                {
                    "role": "user",
                    "content": f"Analyze this organizational data and provide strategic insights:\n\n{data_summary}"
                }
            ],
            temperature=0.7,
            max_tokens=2000
        )
        
        content = response.choices[0].message.content
        content = content.strip()
        if content.startswith('```json'):
            content = content[7:]
        if content.startswith('```'):
            content = content[3:]
        if content.endswith('```'):
            content = content[:-3]
        
        insights_data = json.loads(content)
        
        for alert in insights_data.get('alerts', []):
            conn.execute('''
                INSERT INTO org_alerts (alert_type, severity, title, message, created_at)
                VALUES ('AI Generated', ?, ?, ?, datetime('now'))
            ''', (alert['severity'], alert['title'], alert['message']))
        
        for rec in insights_data.get('recommendations', []):
            conn.execute('''
                INSERT INTO org_recommendations 
                (recommendation_type, priority, title, description, business_impact, risk_level, time_to_value, confidence_score, generated_by, created_at)
                VALUES ('AI Generated', ?, ?, ?, ?, ?, ?, ?, 'GPT-4o', datetime('now'))
            ''', (rec['priority'], rec['title'], rec['description'], rec.get('impact', ''), 
                  rec.get('risk_level', 'Medium'), rec.get('time_to_value', ''), rec.get('confidence_score', 70)))
        
        today = datetime.now().strftime('%Y-%m-%d')
        conn.execute('''
            INSERT INTO org_health_scores 
            (score_date, overall_score, financial_score, operational_score, workforce_score, customer_score, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (today, health_scores['overall'], health_scores['financial'], health_scores['operational'],
              health_scores['workforce'], health_scores['customer'], insights_data.get('executive_summary', '')))
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': insights_data
        })
        
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500

@org_analyzer_bp.route('/org-analyzer/alerts')
def alerts_list():
    """View all organizational alerts"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    alerts = conn.execute('''
        SELECT a.*, u.username as acknowledged_by_name
        FROM org_alerts a
        LEFT JOIN users u ON a.acknowledged_by = u.id
        ORDER BY a.is_acknowledged ASC, a.severity DESC, a.created_at DESC
    ''').fetchall()
    
    conn.close()
    
    return render_template('org_analyzer/alerts.html', alerts=alerts)

@org_analyzer_bp.route('/org-analyzer/alerts/<int:alert_id>/acknowledge', methods=['POST'])
def acknowledge_alert(alert_id):
    """Acknowledge an alert"""
    if 'user_id' not in session:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    
    db = Database()
    conn = db.get_connection()
    
    conn.execute('''
        UPDATE org_alerts 
        SET is_acknowledged = 1, acknowledged_by = ?, acknowledged_at = datetime('now')
        WHERE id = ?
    ''', (session['user_id'], alert_id))
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True})

@org_analyzer_bp.route('/org-analyzer/recommendations')
def recommendations_list():
    """View all strategic recommendations"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    recommendations = conn.execute('''
        SELECT r.*, u.username as reviewed_by_name
        FROM org_recommendations r
        LEFT JOIN users u ON r.reviewed_by = u.id
        ORDER BY r.status ASC, r.priority DESC, r.confidence_score DESC, r.created_at DESC
    ''').fetchall()
    
    conn.close()
    
    return render_template('org_analyzer/recommendations.html', recommendations=recommendations)

@org_analyzer_bp.route('/org-analyzer/recommendations/<int:rec_id>/status', methods=['POST'])
def update_recommendation_status(rec_id):
    """Update recommendation status"""
    if 'user_id' not in session:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    
    status = request.json.get('status', 'Pending')
    
    db = Database()
    conn = db.get_connection()
    
    conn.execute('''
        UPDATE org_recommendations 
        SET status = ?, reviewed_by = ?, reviewed_at = datetime('now')
        WHERE id = ?
    ''', (status, session['user_id'], rec_id))
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True})

@org_analyzer_bp.route('/org-analyzer/forecast', methods=['GET', 'POST'])
def forecast():
    """Generate forecasts and scenarios"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        horizon_days = int(request.form.get('horizon_days', 30))
        scenario = request.form.get('scenario', 'Base Case')
        
        financial = calculate_financial_kpis(conn)
        operational = calculate_operational_kpis(conn)
        
        data_for_forecast = f"""
Current Revenue: ${financial['mtd_revenue']:,.2f}
Revenue Growth Rate: {financial['revenue_growth']:.1f}%
Gross Margin: {financial['gross_margin']:.1f}%
Work Order Completion Rate: {operational['wo_completion_rate']:.1f}%
On-Time Delivery: {operational['on_time_delivery_rate']:.1f}%
Active Work Orders: {operational['active_work_orders']}
Scenario: {scenario}
Forecast Horizon: {horizon_days} days
"""
        
        try:
            client = get_openai_client()
            
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "system",
                        "content": """Generate a business forecast based on the provided metrics. Return JSON with this structure:
{
    "forecasts": [
        {"metric_name": "Revenue", "predicted_value": number, "lower_bound": number, "upper_bound": number, "confidence": number},
        {"metric_name": "Work Orders", "predicted_value": number, "lower_bound": number, "upper_bound": number, "confidence": number},
        {"metric_name": "On-Time Delivery %", "predicted_value": number, "lower_bound": number, "upper_bound": number, "confidence": number}
    ],
    "assumptions": "string describing key assumptions",
    "risks": ["risk1", "risk2"]
}"""
                    },
                    {
                        "role": "user",
                        "content": f"Generate {horizon_days}-day {scenario} forecast:\n\n{data_for_forecast}"
                    }
                ],
                temperature=0.5,
                max_tokens=1000
            )
            
            content = response.choices[0].message.content
            content = content.strip()
            if content.startswith('```json'):
                content = content[7:]
            if content.startswith('```'):
                content = content[3:]
            if content.endswith('```'):
                content = content[:-3]
            
            forecast_data = json.loads(content)
            
            forecast_date = (datetime.now() + timedelta(days=horizon_days)).strftime('%Y-%m-%d')
            for fc in forecast_data.get('forecasts', []):
                conn.execute('''
                    INSERT INTO org_forecasts 
                    (forecast_type, scenario, horizon_days, forecast_date, metric_name, 
                     predicted_value, lower_bound, upper_bound, confidence_level, assumptions)
                    VALUES ('AI Generated', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (scenario, horizon_days, forecast_date, fc['metric_name'],
                      fc['predicted_value'], fc['lower_bound'], fc['upper_bound'],
                      fc['confidence'], forecast_data.get('assumptions', '')))
            
            conn.commit()
            flash('Forecast generated successfully!', 'success')
            
        except Exception as e:
            flash(f'Error generating forecast: {str(e)}', 'danger')
    
    forecasts = conn.execute('''
        SELECT * FROM org_forecasts ORDER BY created_at DESC LIMIT 20
    ''').fetchall()
    
    health_history = conn.execute('''
        SELECT * FROM org_health_scores ORDER BY score_date DESC LIMIT 30
    ''').fetchall()
    
    conn.close()
    
    return render_template('org_analyzer/forecast.html', 
                         forecasts=forecasts,
                         health_history=health_history)

@org_analyzer_bp.route('/org-analyzer/api/kpis')
def api_kpis():
    """API endpoint for real-time KPI data"""
    if 'user_id' not in session:
        return jsonify({'success': False, 'error': 'Not authenticated'}), 401
    
    db = Database()
    conn = db.get_connection()
    
    financial = calculate_financial_kpis(conn)
    operational = calculate_operational_kpis(conn)
    workforce = calculate_workforce_kpis(conn)
    customer = calculate_customer_kpis(conn)
    health_scores = calculate_health_score(financial, operational, workforce, customer)
    
    conn.close()
    
    return jsonify({
        'success': True,
        'financial': financial,
        'operational': operational,
        'workforce': workforce,
        'customer': customer,
        'health_scores': health_scores,
        'timestamp': datetime.now().isoformat()
    })
