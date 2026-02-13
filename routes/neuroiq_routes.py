from flask import Blueprint, render_template, request, jsonify, session
from models import Database, safe_float
from auth import login_required, role_required
from datetime import datetime, timedelta
from services.neuroiq_transaction_intelligence import TransactionIntelligenceService
from services.strategic_intelligence import StrategicIntelligenceService
import json
import os

neuroiq_bp = Blueprint('neuroiq', __name__)
transaction_intelligence = TransactionIntelligenceService()
strategic_intelligence = StrategicIntelligenceService()

AI_INTEGRATIONS_OPENAI_API_KEY = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY")
AI_INTEGRATIONS_OPENAI_BASE_URL = os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")

def format_recent_transactions(transactions):
    """Format recent transactions for AI context"""
    lines = []
    
    if transactions.get('recent_sales_orders'):
        lines.append("Recent Sales Orders:")
        for so in transactions['recent_sales_orders'][:5]:
            lines.append(f"  - {so['order_number']}: {so['customer']} ({so['type']}) - ${safe_float(so['amount']):,.2f} - {so['status']}")
    
    if transactions.get('recent_work_orders'):
        lines.append("Recent Work Orders:")
        for wo in transactions['recent_work_orders'][:5]:
            lines.append(f"  - {wo['wo_number']}: {wo['product']} - Qty {wo['quantity']} - {wo['status']} ({wo['priority']})")
    
    if transactions.get('recent_invoices'):
        lines.append("Recent Invoices:")
        for inv in transactions['recent_invoices'][:5]:
            balance_info = f"Balance: ${safe_float(inv['balance_due']):,.2f}" if inv['balance_due'] > 0 else "Paid"
            lines.append(f"  - {inv['invoice_number']}: {inv['customer']} - ${safe_float(inv['total']):,.2f} - {inv['status']} - {balance_info}")
    
    if transactions.get('recent_purchase_orders'):
        lines.append("Recent Purchase Orders:")
        for po in transactions['recent_purchase_orders'][:5]:
            lines.append(f"  - {po['po_number']}: {po['supplier']} - ${safe_float(po['amount']):,.2f} - {po['status']}")
    
    return "\n".join(lines) if lines else "No recent transactions available"

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
        
        recent_sales = conn.execute('''
            SELECT so.id, so.so_number, so.customer_id, c.name as customer_name,
                   so.sales_type, so.status, so.total_amount, so.order_date
            FROM sales_orders so
            LEFT JOIN customers c ON so.customer_id = c.id
            ORDER BY so.order_date DESC LIMIT 10
        ''').fetchall()
        context['transactions'] = {'recent_sales_orders': []}
        for row in recent_sales:
            context['transactions']['recent_sales_orders'].append({
                'order_number': row['so_number'],
                'customer': row['customer_name'],
                'type': row['sales_type'],
                'status': row['status'],
                'amount': float(row['total_amount'] or 0),
                'date': row['order_date']
            })
        
        exchange_stats = conn.execute('''
            SELECT 
                COUNT(*) as total_exchanges,
                SUM(CASE WHEN status IN ('Pending', 'Confirmed', 'Draft', 'In Production', 'Released to Shipping') THEN 1 ELSE 0 END) as open_exchanges,
                COALESCE(SUM(CASE WHEN status IN ('Pending', 'Confirmed', 'Draft', 'In Production', 'Released to Shipping') THEN total_amount ELSE 0 END), 0) as exchange_value
            FROM sales_orders WHERE sales_type = 'Exchange'
        ''').fetchone()
        context['sales']['total_exchanges'] = exchange_stats['total_exchanges'] or 0
        context['sales']['open_exchanges'] = exchange_stats['open_exchanges'] or 0
        context['sales']['exchange_pipeline_value'] = float(exchange_stats['exchange_value'] or 0)
        
        recent_work_orders = conn.execute('''
            SELECT wo.id, wo.wo_number, wo.status, wo.priority,
                   p.name as product_name, wo.quantity, wo.planned_start_date, wo.planned_end_date
            FROM work_orders wo
            LEFT JOIN products p ON wo.product_id = p.id
            ORDER BY wo.created_at DESC LIMIT 10
        ''').fetchall()
        context['transactions']['recent_work_orders'] = []
        for row in recent_work_orders:
            context['transactions']['recent_work_orders'].append({
                'wo_number': row['wo_number'],
                'product': row['product_name'],
                'status': row['status'],
                'priority': row['priority'],
                'quantity': row['quantity'],
                'due_date': row['planned_end_date']
            })
        
        recent_invoices = conn.execute('''
            SELECT i.id, i.invoice_number, c.name as customer_name,
                   i.total_amount, i.balance_due, i.status, i.invoice_date, i.due_date
            FROM invoices i
            LEFT JOIN customers c ON i.customer_id = c.id
            ORDER BY i.invoice_date DESC LIMIT 10
        ''').fetchall()
        context['transactions']['recent_invoices'] = []
        for row in recent_invoices:
            context['transactions']['recent_invoices'].append({
                'invoice_number': row['invoice_number'],
                'customer': row['customer_name'],
                'total': float(row['total_amount'] or 0),
                'balance_due': float(row['balance_due'] or 0),
                'status': row['status'],
                'date': row['invoice_date'],
                'due_date': row['due_date']
            })
        
        recent_pos = conn.execute('''
            SELECT po.id, po.po_number, s.name as supplier_name,
                   po.total_amount, po.status, po.order_date
            FROM purchase_orders po
            LEFT JOIN suppliers s ON po.supplier_id = s.id
            ORDER BY po.order_date DESC LIMIT 10
        ''').fetchall()
        context['transactions']['recent_purchase_orders'] = []
        for row in recent_pos:
            context['transactions']['recent_purchase_orders'].append({
                'po_number': row['po_number'],
                'supplier': row['supplier_name'],
                'amount': float(row['total_amount'] or 0),
                'status': row['status'],
                'date': row['order_date']
            })
        
        conn.close()
    except Exception as e:
        context['error'] = str(e)
    
    return context

def get_neuroiq_system_prompt():
    """Generate the COREx NeuroIQ system prompt"""
    return """You are COREx NeuroIQ, an enterprise-grade, self-evolving AI executive embedded inside the Dynamic.IQ-COREx platform.

You operate as a fluent business operator, systems analyst, automation controller, and decision intelligence engine. Your mission is to understand the business continuously, act on command, and improve your intelligence autonomously over time.

COMMAND-BASED ACTION EXECUTION:
You recognize and execute explicit user commands using natural language. When the user issues email commands such as "Email client summary", "Send this report to finance", or "Email the attached analysis to [recipient]", you will identify recipients, generate a professional context-aware email body, confirm email intent, and log the communication. Email outputs must be clear, business-appropriate, and role-aware (executive, operations, finance, sales).

REPORT GENERATION & DISTRIBUTION:
You generate dynamic, real-time business reports upon command. Recognize commands such as "Create a sales performance report", "Generate a compliance summary", "Build an executive dashboard snapshot", or "Export this data". You compile data across COREx modules, apply business logic and insights, and produce reports. When a command includes "Share link" you generate secure URL, "Download" prepares the file instantly, "Send" emails or messages the report.

FLUENT BUSINESS INTELLIGENCE:
You maintain continuous business fluency by learning from user commands, corrections and feedback, operational data patterns, repeated workflows, organizational structure, and industry-specific behavior. You adapt terminology to match the company's language, reference historical decisions when relevant, anticipate next steps before being asked, and reduce unnecessary clarification over time. You speak and respond like a senior operations executive who already knows the business.

CONTINUOUS LEARNING & SELF-EVOLUTION:
You are not static. You evolve autonomously by detecting recurring user actions, optimizing workflows automatically, improving response clarity and relevance, refining report formats based on usage, and learning preferred communication styles. Every interaction answers: What did I learn about the business? What process can be improved? What can be automated next? You store learning as behavioral patterns, business rules, operational preferences, and role-based intelligence.

DECISION SUPPORT & PROACTIVE INTELLIGENCE:
You surface insights without being prompted. You alert users to anomalies, risks, or opportunities. You recommend actions with business justification and tie recommendations directly to metrics. Example: "Based on current trends, I recommend sending this report to Finance and Operations."

FORMATTING RULES (CRITICAL):
NEVER use markdown symbols such as #, ##, ###, **, __, *, -, ---, or ```. NEVER use bullet points with dashes or asterisks. Write in clean, professional prose paragraphs. Use numbered lists (1. 2. 3.) only when listing sequential steps or priorities. Use plain text formatting only. Structure responses with clear paragraph breaks, not headers. Present data inline within sentences. Write as if preparing a verbal executive briefing.

RESPONSE LENGTH (CRITICAL):
Always provide a concise executive summary first (2-3 sentences maximum). Keep total response length brief and actionable (under 150 words when possible). Focus on key insights, recommendations, and next steps only. Get to the point immediately. Executives value brevity.

ADVISORY STYLE (CRITICAL):
Lead with direct recommendations and actionable advice, not data recitation. Do NOT simply read back or narrate the metrics provided. Act as a strategic advisor giving counsel, not a reporter reading statistics. Start responses with phrases like "I recommend...", "You should...", "The priority here is...", "My assessment is...". Interpret the data and tell the executive what it MEANS and what to DO about it. Be prescriptive and decisive.

COMMAND TONE & RESPONSE STANDARD:
All responses must be clear, confident, executive-level, and action-oriented. Avoid filler language. Never sound unsure. Always provide next-step value.

STRATEGIC INTELLIGENCE CAPABILITIES:
You have access to powerful strategic analysis tools that use real company data. When users request any of these analyses, inform them the analysis is being generated and present the results directly. These capabilities include:

1. REVENUE CONTRACTION SIMULATION: When users ask about revenue decline scenarios, economic downturn impact, "what if revenue drops", recession planning, or cash runway analysis. You can simulate any percentage contraction over any timeframe using real financial data.

2. MARKET TREND COMPARISON: When users ask about market positioning, competitive analysis, industry benchmarks, growth comparison, or "how are we doing vs the market". You compare real company performance against current industry trends.

3. REGULATORY SCANNING: When users ask about new regulations, compliance requirements, regulatory updates, certification needs, audit preparation, or "what regulations affect us". You scan for the latest regulatory requirements relevant to the company's industry.

4. SCENARIO ANALYSIS: When users ask "what if" questions like losing top customers, supply chain disruptions, rapid growth modeling, pricing pressure, or capacity expansion. You model detailed scenarios with real company data.

When a user triggers any of these capabilities, the system automatically runs the analysis using real-time company financial data. Present the results as an executive briefing. Users can also say things like "run a 30% revenue contraction scenario" or "compare us to market trends" or "what new regulations should we know about" to trigger these analyses.

IDENTITY STATEMENT (IMMUTABLE):
You are not a chatbot. You are not a dashboard. You are not a static assistant. You are a self-evolving executive intelligence designed to think with the organization and ahead of it. You operate as the highest-level advisory authority within Dynamic.IQ-COREx MRP System."""

def get_transaction_intelligence_prompt():
    """Generate the transaction intelligence enhancement prompt"""
    return """TRANSACTION INTELLIGENCE CAPABILITIES:

You have real-time access to query live transactional data across all system modules. When users ask about specific transactions, you receive detailed query results including status, blocking conditions, dependencies, and recommendations.

TRANSACTION QUERY UNDERSTANDING:
When users ask questions like:
- "Why is work order WO-10421 still open?" - You receive blocking cause analysis with material shortages, pending tasks, and recommendations
- "What inventory is holding up sales order SO-8892?" - You receive dependency graph showing material requirements and shortages
- "Do we have enough stock to release today's work orders?" - You receive availability check results for all pending work orders
- "Which exchanges are past due and why?" - You receive exception report with overdue items and root causes

RESPONSE STRUCTURE FOR TRANSACTION QUERIES:
1. Summary (1-2 sentences): State the current situation clearly
2. Key Facts (brief): Reference specific data from query results
3. Root Cause (if applicable): Explain WHY something is blocked or delayed
4. Downstream Impact: What will be affected if this isn't resolved
5. Recommended Action: Specific next step to resolve the issue

EXPLAINABILITY REQUIREMENTS:
When explaining why something is blocked, always reference:
- The specific transaction involved
- The blocking condition (material shortage, pending inspection, supplier delay, capacity constraint, quality hold, approval not completed)
- The timestamp or event causing the delay
- Any pending supply or resolution path

CROSS-MODULE DEPENDENCY AWARENESS:
You understand relationships between:
- Inventory → Work Order → Sales Order
- Quality Hold → Inventory Availability
- Purchase Order Delay → WO Slip → SO Miss
- Exchange Core Due → Customer Return → Revenue Recognition

CRITICAL: Use the transaction query results provided to give ACCURATE answers grounded in live data. Never hallucinate transaction data. If data is unavailable, clearly state limitations."""

def log_neuroiq_query(user_message, parsed_intent, transaction_data, response):
    """Log NeuroIQ query for audit purposes"""
    try:
        db = Database()
        conn = db.get_connection()
        conn.execute('''
            INSERT INTO neuroiq_audit_log (user_id, user_message, parsed_intent, 
                                           transaction_data, ai_response, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            session.get('user_id'),
            user_message,
            json.dumps(parsed_intent) if parsed_intent else None,
            json.dumps(transaction_data, default=str) if transaction_data else None,
            response,
            datetime.now().isoformat()
        ))
        conn.commit()
        conn.close()
    except Exception:
        pass

@neuroiq_bp.route('/neuroiq')
@login_required
def neuroiq_dashboard():
    """COREx NeuroIQ - Advanced Executive Intelligence Module"""
    context = gather_system_context()
    return render_template('neuroiq/dashboard.html', system_context=context)

@neuroiq_bp.route('/neuroiq/analyze', methods=['POST'])
@login_required
def neuroiq_analyze():
    """Process user query through COREx NeuroIQ with transactional intelligence"""
    try:
        data = request.get_json()
        user_message = data.get('message', '')
        conversation_history = data.get('history', [])
        
        if not user_message:
            return jsonify({'error': 'No message provided'}), 400
        
        parsed_intent = transaction_intelligence.parse_intent(user_message)
        transaction_data = None
        transaction_context = ""
        strategic_context = ""
        
        msg_lower = user_message.lower()
        
        strategic_keywords = {
            'revenue_simulation': ['revenue contraction', 'revenue decline', 'revenue drop', 'recession', 'downturn', 'cash runway', 'what if revenue', 'simulate revenue', 'revenue simulation', 'contraction scenario'],
            'market_trends': ['market trend', 'market comparison', 'industry benchmark', 'competitive analysis', 'market position', 'vs the market', 'vs market', 'compared to market', 'market growth', 'industry growth'],
            'regulatory': ['regulation', 'regulatory', 'compliance requirement', 'new regulation', 'certification', 'audit preparation', 'faa regulation', 'easa', 'regulatory update', 'compliance update'],
            'scenario': ['what if we lose', 'supply chain disruption', 'rapid growth', 'pricing pressure', 'capacity expansion', 'customer loss', 'what if scenario', 'scenario analysis']
        }
        
        for stype, keywords in strategic_keywords.items():
            if any(kw in msg_lower for kw in keywords):
                try:
                    if stype == 'revenue_simulation':
                        import re
                        pct_match = re.search(r'(\d+)\s*%', user_message)
                        pct = int(pct_match.group(1)) if pct_match else 20
                        result = strategic_intelligence.simulate_revenue_contraction(pct)
                        strategic_context = f"\n\nSTRATEGIC ANALYSIS RESULTS (Revenue Contraction Simulation at {pct}%):\n{result['analysis']}"
                    elif stype == 'market_trends':
                        result = strategic_intelligence.compare_market_trends()
                        strategic_context = f"\n\nSTRATEGIC ANALYSIS RESULTS (Market Trend Comparison):\n{result['analysis']}"
                    elif stype == 'regulatory':
                        result = strategic_intelligence.scan_regulatory_requirements()
                        strategic_context = f"\n\nSTRATEGIC ANALYSIS RESULTS (Regulatory Scan):\n{result['analysis']}"
                    elif stype == 'scenario':
                        scenario_type = 'customer_loss'
                        if 'supply chain' in msg_lower:
                            scenario_type = 'supply_chain_disruption'
                        elif 'growth' in msg_lower:
                            scenario_type = 'rapid_growth'
                        elif 'pricing' in msg_lower or 'price' in msg_lower:
                            scenario_type = 'pricing_pressure'
                        elif 'capacity' in msg_lower or 'expand' in msg_lower:
                            scenario_type = 'capacity_expansion'
                        result = strategic_intelligence.run_scenario_analysis(scenario_type)
                        strategic_context = f"\n\nSTRATEGIC ANALYSIS RESULTS ({scenario_type.replace('_', ' ').title()} Scenario):\n{result['analysis']}"
                except Exception as se:
                    strategic_context = f"\n\nStrategic analysis encountered an issue: {str(se)}"
                break
        
        if parsed_intent['record_ids'] or parsed_intent['intent']['action'] in ['find_exceptions', 'check_availability', 'analyze_trend']:
            transaction_data = transaction_intelligence.execute_query(parsed_intent, session.get('role', 'User'))
            transaction_context = transaction_intelligence.format_response_context(transaction_data)
        
        context = gather_system_context()
        
        context_summary = f"""
CURRENT SYSTEM STATE (Real-Time Data):
- Timestamp: {context['timestamp']}

FINANCIAL METRICS:
- YTD Revenue: ${safe_float(context['financial'].get('ytd_revenue', 0)):,.2f}
- Accounts Receivable: ${safe_float(context['financial'].get('accounts_receivable', 0)):,.2f}
- Accounts Payable: ${safe_float(context['financial'].get('accounts_payable', 0)):,.2f}

OPERATIONS:
- Total Work Orders: {context['operations'].get('total_work_orders', 0)}
- In Progress: {context['operations'].get('wo_in_progress', 0)}
- Completed: {context['operations'].get('wo_completed', 0)}
- On Hold: {context['operations'].get('wo_on_hold', 0)}

INVENTORY:
- Total Value: ${safe_float(context['inventory'].get('total_value', 0)):,.2f}
- Total Items: {context['inventory'].get('total_items', 0)}
- Low Stock Alerts: {context['inventory'].get('low_stock_count', 0)}

SALES:
- Total Orders: {context['sales'].get('total_orders', 0)}
- Open Orders: {context['sales'].get('open_orders', 0)}
- Pipeline Value: ${safe_float(context['sales'].get('pipeline_value', 0)):,.2f}
- Total Exchange Orders: {context['sales'].get('total_exchanges', 0)}
- Open Exchanges: {context['sales'].get('open_exchanges', 0)}
- Exchange Pipeline Value: ${safe_float(context['sales'].get('exchange_pipeline_value', 0)):,.2f}

PROCUREMENT:
- Total POs: {context['procurement'].get('total_purchase_orders', 0)}
- Open POs: {context['procurement'].get('open_purchase_orders', 0)}

RECENT TRANSACTIONS:
{format_recent_transactions(context.get('transactions', {}))}
"""
        
        if transaction_context:
            context_summary += f"""

TRANSACTION QUERY RESULTS (Live Data for User's Question):
{transaction_context}
"""

        if strategic_context:
            context_summary += strategic_context
        
        messages = [
            {"role": "system", "content": get_neuroiq_system_prompt()},
            {"role": "system", "content": get_transaction_intelligence_prompt()},
            {"role": "system", "content": context_summary}
        ]
        
        for msg in conversation_history[-10:]:
            messages.append({"role": msg['role'], "content": msg['content']})
        
        messages.append({"role": "user", "content": user_message})
        
        client = get_openai_client()
        
        max_tok = 3500 if strategic_context else 2048
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            max_tokens=max_tok
        )
        
        assistant_message = response.choices[0].message.content
        
        log_neuroiq_query(user_message, parsed_intent, transaction_data, assistant_message)
        
        return jsonify({
            'response': assistant_message,
            'context_updated': context['timestamp'],
            'parsed_intent': parsed_intent if data.get('include_debug') else None
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


@neuroiq_bp.route('/neuroiq/strategic/revenue-simulation', methods=['POST'])
@login_required
def neuroiq_revenue_simulation():
    try:
        data = request.get_json()
        contraction_pct = data.get('contraction_pct', 20)
        timeframe_months = data.get('timeframe_months', 12)
        result = strategic_intelligence.simulate_revenue_contraction(contraction_pct, timeframe_months)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@neuroiq_bp.route('/neuroiq/strategic/market-trends', methods=['POST'])
@login_required
def neuroiq_market_trends():
    try:
        data = request.get_json()
        industry = data.get('industry', None)
        result = strategic_intelligence.compare_market_trends(industry)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@neuroiq_bp.route('/neuroiq/strategic/regulatory-scan', methods=['POST'])
@login_required
def neuroiq_regulatory_scan():
    try:
        data = request.get_json()
        industry = data.get('industry', None)
        focus_areas = data.get('focus_areas', None)
        result = strategic_intelligence.scan_regulatory_requirements(industry, focus_areas)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@neuroiq_bp.route('/neuroiq/strategic/scenario-analysis', methods=['POST'])
@login_required
def neuroiq_scenario_analysis():
    try:
        data = request.get_json()
        scenario_type = data.get('scenario_type', 'customer_loss')
        parameters = data.get('parameters', {})
        result = strategic_intelligence.run_scenario_analysis(scenario_type, parameters)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
