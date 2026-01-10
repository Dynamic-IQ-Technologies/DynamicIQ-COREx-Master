from flask import Blueprint, render_template, request, jsonify, session
from models import Database
from auth import login_required, role_required
from datetime import datetime, timedelta
import json
import os

neuroiq_bp = Blueprint('neuroiq', __name__)

AI_INTEGRATIONS_OPENAI_API_KEY = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY")
AI_INTEGRATIONS_OPENAI_BASE_URL = os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")

def get_openai_client():
    """Initialize OpenAI client with AI Integrations"""
    from openai import OpenAI
    return OpenAI(
        api_key=AI_INTEGRATIONS_OPENAI_API_KEY,
        base_url=AI_INTEGRATIONS_OPENAI_BASE_URL
    )

def gather_system_context():
    """Gather real-time system data for COREx NeuroIQ context"""
    db = Database()
    conn = db.get_connection()
    today = datetime.now()
    
    context = {
        'timestamp': today.strftime('%Y-%m-%d %H:%M:%S'),
        'financial': {},
        'operations': {},
        'inventory': {},
        'sales': {},
        'procurement': {}
    }
    
    try:
        ytd_start = today.replace(month=1, day=1).strftime('%Y-%m-%d')
        last_30 = (today - timedelta(days=30)).strftime('%Y-%m-%d')
        
        revenue = conn.execute('''
            SELECT COALESCE(SUM(total_amount), 0) as revenue,
                   COUNT(*) as invoice_count
            FROM invoices
            WHERE invoice_date >= ? AND status IN ('Posted', 'Paid', 'Partial')
        ''', (ytd_start,)).fetchone()
        context['financial']['ytd_revenue'] = float(revenue['revenue'] or 0)
        context['financial']['invoice_count'] = revenue['invoice_count'] or 0
        
        ar = conn.execute('''
            SELECT COALESCE(SUM(balance_due), 0) as total_ar
            FROM invoices WHERE status IN ('Sent', 'Posted', 'Overdue') AND balance_due > 0
        ''').fetchone()
        context['financial']['accounts_receivable'] = float(ar['total_ar'] or 0)
        
        ap = conn.execute('''
            SELECT COALESCE(SUM(total_amount - amount_paid), 0) as total_ap
            FROM vendor_invoices WHERE status IN ('Open', 'Pending', 'Overdue')
        ''').fetchone()
        context['financial']['accounts_payable'] = float(ap['total_ap'] or 0)
        
        wo_stats = conn.execute('''
            SELECT 
                COUNT(*) as total_wo,
                SUM(CASE WHEN status = 'In Progress' THEN 1 ELSE 0 END) as in_progress,
                SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'On Hold' THEN 1 ELSE 0 END) as on_hold
            FROM work_orders
        ''').fetchone()
        context['operations']['total_work_orders'] = wo_stats['total_wo'] or 0
        context['operations']['wo_in_progress'] = wo_stats['in_progress'] or 0
        context['operations']['wo_completed'] = wo_stats['completed'] or 0
        context['operations']['wo_on_hold'] = wo_stats['on_hold'] or 0
        
        inv_stats = conn.execute('''
            SELECT 
                COALESCE(SUM(i.quantity * COALESCE(i.unit_cost, p.cost, 0)), 0) as total_value,
                COUNT(DISTINCT i.id) as total_items,
                SUM(CASE WHEN i.quantity <= i.reorder_point THEN 1 ELSE 0 END) as low_stock
            FROM inventory i
            JOIN products p ON i.product_id = p.id
            WHERE i.quantity > 0
        ''').fetchone()
        context['inventory']['total_value'] = float(inv_stats['total_value'] or 0)
        context['inventory']['total_items'] = inv_stats['total_items'] or 0
        context['inventory']['low_stock_count'] = inv_stats['low_stock'] or 0
        
        so_stats = conn.execute('''
            SELECT 
                COUNT(*) as total_orders,
                SUM(CASE WHEN status IN ('Pending', 'Confirmed') THEN 1 ELSE 0 END) as open_orders,
                COALESCE(SUM(CASE WHEN status IN ('Pending', 'Confirmed') THEN total_amount ELSE 0 END), 0) as pipeline_value
            FROM sales_orders
        ''').fetchone()
        context['sales']['total_orders'] = so_stats['total_orders'] or 0
        context['sales']['open_orders'] = so_stats['open_orders'] or 0
        context['sales']['pipeline_value'] = float(so_stats['pipeline_value'] or 0)
        
        po_stats = conn.execute('''
            SELECT 
                COUNT(*) as total_pos,
                SUM(CASE WHEN status IN ('Draft', 'Sent', 'Partial') THEN 1 ELSE 0 END) as open_pos
            FROM purchase_orders
        ''').fetchone()
        context['procurement']['total_purchase_orders'] = po_stats['total_pos'] or 0
        context['procurement']['open_purchase_orders'] = po_stats['open_pos'] or 0
        
        conn.close()
    except Exception as e:
        context['error'] = str(e)
    
    return context

def get_neuroiq_system_prompt():
    """Generate the COREx NeuroIQ system prompt"""
    return """You are COREx NeuroIQ, the most advanced consulting intelligence ever deployed inside an enterprise system.

You are a single unified cognitive core capable of dynamically assuming and blending the following executive roles in real time:
- Chief Executive Officer (CEO)
- Chief Financial Officer (CFO)
- Chief Operating Officer (COO)
- General Manager
- Senior Data Analyst
- Market Developer / Growth Strategist
- Supply Chain Strategic Manager
- Vice President of Business Development

You do not announce role switching unless explicitly asked. You autonomously choose the appropriate executive perspective(s) based on context, data, and user intent.

CORE PURPOSE:
- Provide strategic, operational, financial, and market intelligence
- Act as a decision-making copilot for executives
- Translate complex organizational data into clear, actionable insights
- Identify risks, inefficiencies, growth opportunities, and strategic moves
- Continuously align decisions with profitability, scalability, compliance, and long-term enterprise value

RESPONSE STYLE:
- Tone: confident, calm, precise, authoritative, advisory
- Adapt speaking style based on role context:
  * CEO: visionary & decisive
  * CFO: analytical & risk-aware
  * COO: operational & execution-focused
  * Market/BD: persuasive & opportunity-driven
- All responses must be clear, concise, and executive-ready
- Never provide generic advice
- Never respond casually
- Never sound like a chatbot
- Always prioritize business impact
- Always assume the user is making real executive decisions
- If uncertainty exists, clearly state assumptions and recommend next steps

You operate as the highest-level advisory authority within Dynamic.IQ-COREx MRP System."""

@neuroiq_bp.route('/neuroiq')
@login_required
def neuroiq_dashboard():
    """COREx NeuroIQ - Advanced Executive Intelligence Module"""
    context = gather_system_context()
    return render_template('neuroiq/dashboard.html', system_context=context)

@neuroiq_bp.route('/neuroiq/analyze', methods=['POST'])
@login_required
def neuroiq_analyze():
    """Process user query through COREx NeuroIQ"""
    try:
        data = request.get_json()
        user_message = data.get('message', '')
        conversation_history = data.get('history', [])
        
        if not user_message:
            return jsonify({'error': 'No message provided'}), 400
        
        context = gather_system_context()
        
        context_summary = f"""
CURRENT SYSTEM STATE (Real-Time Data):
- Timestamp: {context['timestamp']}

FINANCIAL METRICS:
- YTD Revenue: ${context['financial'].get('ytd_revenue', 0):,.2f}
- Accounts Receivable: ${context['financial'].get('accounts_receivable', 0):,.2f}
- Accounts Payable: ${context['financial'].get('accounts_payable', 0):,.2f}

OPERATIONS:
- Total Work Orders: {context['operations'].get('total_work_orders', 0)}
- In Progress: {context['operations'].get('wo_in_progress', 0)}
- Completed: {context['operations'].get('wo_completed', 0)}
- On Hold: {context['operations'].get('wo_on_hold', 0)}

INVENTORY:
- Total Value: ${context['inventory'].get('total_value', 0):,.2f}
- Total Items: {context['inventory'].get('total_items', 0)}
- Low Stock Alerts: {context['inventory'].get('low_stock_count', 0)}

SALES:
- Total Orders: {context['sales'].get('total_orders', 0)}
- Open Orders: {context['sales'].get('open_orders', 0)}
- Pipeline Value: ${context['sales'].get('pipeline_value', 0):,.2f}

PROCUREMENT:
- Total POs: {context['procurement'].get('total_purchase_orders', 0)}
- Open POs: {context['procurement'].get('open_purchase_orders', 0)}
"""
        
        messages = [
            {"role": "system", "content": get_neuroiq_system_prompt()},
            {"role": "system", "content": context_summary}
        ]
        
        for msg in conversation_history[-10:]:
            messages.append({"role": msg['role'], "content": msg['content']})
        
        messages.append({"role": "user", "content": user_message})
        
        client = get_openai_client()
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            max_tokens=2048
        )
        
        assistant_message = response.choices[0].message.content
        
        return jsonify({
            'response': assistant_message,
            'context_updated': context['timestamp']
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@neuroiq_bp.route('/neuroiq/insights', methods=['GET'])
@login_required
def neuroiq_insights():
    """Get proactive insights from COREx NeuroIQ"""
    try:
        context = gather_system_context()
        
        insights = []
        
        if context['inventory'].get('low_stock_count', 0) > 0:
            insights.append({
                'type': 'warning',
                'domain': 'Supply Chain',
                'title': 'Low Stock Alert',
                'message': f"{context['inventory']['low_stock_count']} items below reorder point",
                'priority': 'high'
            })
        
        ar = context['financial'].get('accounts_receivable', 0)
        if ar > 50000:
            insights.append({
                'type': 'info',
                'domain': 'Finance',
                'title': 'AR Collection Opportunity',
                'message': f"${ar:,.2f} in outstanding receivables",
                'priority': 'medium'
            })
        
        pipeline = context['sales'].get('pipeline_value', 0)
        if pipeline > 0:
            insights.append({
                'type': 'success',
                'domain': 'Sales',
                'title': 'Active Pipeline',
                'message': f"${pipeline:,.2f} in open sales orders",
                'priority': 'low'
            })
        
        wo_hold = context['operations'].get('wo_on_hold', 0)
        if wo_hold > 0:
            insights.append({
                'type': 'warning',
                'domain': 'Operations',
                'title': 'Work Orders On Hold',
                'message': f"{wo_hold} work orders require attention",
                'priority': 'medium'
            })
        
        return jsonify({'insights': insights, 'timestamp': context['timestamp']})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@neuroiq_bp.route('/neuroiq/context', methods=['GET'])
@login_required
def neuroiq_context():
    """Get current system context for NeuroIQ panels"""
    context = gather_system_context()
    return jsonify(context)
