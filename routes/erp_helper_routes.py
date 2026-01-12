from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database
from datetime import datetime, timedelta, date
from decimal import Decimal
import json
import os
from openai import OpenAI
from routes.it_manager_routes import track_ai_agent_action

erp_helper_bp = Blueprint('erp_helper_routes', __name__)

def get_openai_client():
    """Get OpenAI client configured with Replit AI Integrations"""
    api_key = os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY')
    base_url = os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
    
    if not api_key:
        raise ValueError("OpenAI API key not configured")
    
    return OpenAI(
        api_key=api_key,
        base_url=base_url,
        timeout=30.0
    )

def format_value_for_prompt(value):
    """Format a value for inclusion in AI prompt"""
    if value is None:
        return "N/A"
    if isinstance(value, (datetime, date)):
        return value.strftime('%Y-%m-%d')
    if isinstance(value, Decimal):
        return f"{float(value):,.2f}"
    if isinstance(value, float):
        return f"{value:,.2f}"
    return str(value)

def get_user_context():
    """Get current user context for AI assistance"""
    context = {
        'user_id': session.get('user_id'),
        'username': session.get('username', 'Unknown'),
        'role': session.get('role', 'User'),
        'is_customer_portal': session.get('is_customer_portal', False)
    }
    return context

def get_system_status_summary(conn):
    """Get a high-level summary of system status for AI context"""
    summary = {}
    
    open_so = conn.execute('''
        SELECT COUNT(*) as count FROM sales_orders 
        WHERE status NOT IN ('Shipped', 'Delivered', 'Cancelled', 'Closed')
    ''').fetchone()['count']
    
    open_wo = conn.execute('''
        SELECT COUNT(*) as count FROM work_orders 
        WHERE status NOT IN ('Completed', 'Cancelled', 'Closed')
    ''').fetchone()['count']
    
    open_po = conn.execute('''
        SELECT COUNT(*) as count FROM purchase_orders 
        WHERE status NOT IN ('Received', 'Cancelled', 'Closed')
    ''').fetchone()['count']
    
    low_stock = conn.execute('''
        SELECT COUNT(*) as count FROM inventory 
        WHERE quantity <= reorder_point
    ''').fetchone()['count']
    
    overdue_so = conn.execute('''
        SELECT COUNT(*) as count FROM sales_orders 
        WHERE status NOT IN ('Shipped', 'Delivered', 'Cancelled', 'Closed')
        AND expected_ship_date < date('now')
    ''').fetchone()['count']
    
    overdue_wo = conn.execute('''
        SELECT COUNT(*) as count FROM work_orders 
        WHERE status NOT IN ('Completed', 'Cancelled', 'Closed')
        AND planned_end_date < date('now')
    ''').fetchone()['count']
    
    pending_invoices = conn.execute('''
        SELECT COUNT(*) as count FROM invoices 
        WHERE status NOT IN ('Paid', 'Cancelled', 'Voided')
    ''').fetchone()['count']
    
    summary['open_sales_orders'] = open_so
    summary['open_work_orders'] = open_wo
    summary['open_purchase_orders'] = open_po
    summary['low_stock_items'] = low_stock
    summary['overdue_sales_orders'] = overdue_so
    summary['overdue_work_orders'] = overdue_wo
    summary['pending_invoices'] = pending_invoices
    
    return summary

def get_record_details(conn, record_type, record_id):
    """Get details for a specific record to provide context"""
    details = {}
    
    if record_type == 'sales_order' and record_id:
        so = conn.execute('''
            SELECT so.*, c.name as customer_name 
            FROM sales_orders so
            LEFT JOIN customers c ON so.customer_id = c.id
            WHERE so.id = ?
        ''', (record_id,)).fetchone()
        if so:
            details = dict(so)
            
    elif record_type == 'work_order' and record_id:
        wo = conn.execute('''
            SELECT wo.*, p.name as product_name, p.code as product_code
            FROM work_orders wo
            LEFT JOIN products p ON wo.product_id = p.id
            WHERE wo.id = ?
        ''', (record_id,)).fetchone()
        if wo:
            details = dict(wo)
            
    elif record_type == 'purchase_order' and record_id:
        po = conn.execute('''
            SELECT po.*, s.name as supplier_name
            FROM purchase_orders po
            LEFT JOIN suppliers s ON po.supplier_id = s.id
            WHERE po.id = ?
        ''', (record_id,)).fetchone()
        if po:
            details = dict(po)
            lines = conn.execute('''
                SELECT pol.*, p.name as product_name, p.code as product_code
                FROM purchase_order_lines pol
                LEFT JOIN products p ON pol.product_id = p.id
                WHERE pol.po_id = ?
            ''', (record_id,)).fetchall()
            details['lines'] = [dict(line) for line in lines]
            details['total_value'] = sum(line['quantity'] * line['unit_price'] for line in lines)
            
    elif record_type == 'invoice' and record_id:
        inv = conn.execute('''
            SELECT i.*, c.name as customer_name
            FROM invoices i
            LEFT JOIN customers c ON i.customer_id = c.id
            WHERE i.id = ?
        ''', (record_id,)).fetchone()
        if inv:
            details = dict(inv)
            
    elif record_type == 'product' and record_id:
        prod = conn.execute('''
            SELECT p.*, i.quantity as stock_quantity, i.reorder_point
            FROM products p
            LEFT JOIN inventory i ON p.id = i.product_id
            WHERE p.id = ?
        ''', (record_id,)).fetchone()
        if prod:
            details = dict(prod)
            
    return details

def build_system_prompt(user_context, is_customer_portal=False):
    """Build the system prompt for the AI assistant"""
    
    if is_customer_portal:
        return """You are ERP-Copilot, a friendly customer service assistant for Dynamic.IQ-COREx.

You are helping a customer view their orders and track progress. Use simple, non-technical language.

RULES:
- Never expose internal ERP terminology or system details
- Focus on order status, delivery estimates, and next steps
- Be helpful, professional, and reassuring
- If you don't know something, say so and offer to connect them with support

RESPONSE FORMAT:
- Keep responses concise and friendly
- Use bullet points for clarity when listing information
- Always end with a helpful next step or offer of assistance"""

    return f"""You are ERP-Copilot, an AI assistant embedded in Dynamic.IQ-COREx, a Manufacturing Resource Planning system.

CURRENT USER:
- Username: {user_context['username']}
- Role: {user_context['role']}

YOUR CAPABILITIES:
1. Explain what the user is seeing on screen and guide them through tasks
2. Answer questions about ERP processes, statuses, and workflows
3. Identify issues like missing approvals, invalid statuses, or incomplete data
4. Provide recommendations and next steps based on record status
5. Explain status meanings and workflow transitions

ERP STATUS DEFINITIONS:
- Sales Orders: Draft → Confirmed → Released → In Progress → Shipped → Delivered → Closed
- Work Orders: Draft → Released → In Progress → Completed → Closed
- Purchase Orders: Draft → Submitted → Approved → Partial → Received → Closed
- Invoices: Draft → Approved → Posted → Sent → Paid / Voided

RESPONSE FORMAT:
When providing guidance, structure your response clearly:

Summary: Brief explanation in plain language
Current Status: What the system indicates
Why This Matters: Business or operational impact (if applicable)
Next Step: Clear, actionable guidance

RULES:
- Be professional, clear, and ERP-focused
- Never guess or hallucinate data - if unsure, ask for clarification
- Never bypass controls or skip required steps
- Use bullet points where helpful
- Keep responses concise but complete
- If asked to perform an action, explain what will happen and ask for confirmation
- Do NOT use markdown special characters like ###, **, or ``` in your responses
- Format text naturally without markdown syntax - use plain text only

GUARDED MODE:
You DO NOT execute changes automatically. If user requests an action:
1. Explain what will happen
2. Ask for explicit confirmation
3. List impacted records"""

@erp_helper_bp.route('/erp-helper/chat', methods=['POST'])
def chat():
    """Handle chat messages from the ERP Helper"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    data = request.get_json()
    user_message = data.get('message', '')
    module_context = data.get('module', '')
    record_type = data.get('record_type', '')
    record_id = data.get('record_id')
    conversation_history = data.get('history', [])
    
    if not user_message:
        return jsonify({'error': 'No message provided'}), 400
    
    db = Database()
    conn = db.get_connection()
    
    try:
        user_context = get_user_context()
        is_customer_portal = user_context.get('is_customer_portal', False)
        
        system_status = get_system_status_summary(conn)
        
        record_details = {}
        if record_type and record_id:
            record_details = get_record_details(conn, record_type, record_id)
        
        system_prompt = build_system_prompt(user_context, is_customer_portal)
        
        context_info = f"\n\nCURRENT SYSTEM STATUS:\n"
        context_info += f"- Open Sales Orders: {system_status['open_sales_orders']} ({system_status['overdue_sales_orders']} overdue)\n"
        context_info += f"- Open Work Orders: {system_status['open_work_orders']} ({system_status['overdue_work_orders']} overdue)\n"
        context_info += f"- Open Purchase Orders: {system_status['open_purchase_orders']}\n"
        context_info += f"- Low Stock Items: {system_status['low_stock_items']}\n"
        context_info += f"- Pending Invoices: {system_status['pending_invoices']}\n"
        
        if module_context:
            context_info += f"\nCURRENT MODULE: {module_context}\n"
        
        if record_details:
            context_info += f"\nCURRENT RECORD ({record_type}):\n"
            for key, value in record_details.items():
                if key != 'lines' and value is not None:
                    context_info += f"- {key}: {format_value_for_prompt(value)}\n"
            if 'lines' in record_details:
                context_info += f"- Line items: {len(record_details['lines'])} items\n"
        
        full_system_prompt = system_prompt + context_info
        
        messages = [{"role": "system", "content": full_system_prompt}]
        
        for msg in conversation_history[-10:]:
            messages.append({
                "role": msg.get('role', 'user'),
                "content": msg.get('content', '')
            })
        
        messages.append({"role": "user", "content": user_message})
        
        try:
            client = get_openai_client()
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=messages,
                max_tokens=1000,
                temperature=0.7
            )
            
            assistant_message = response.choices[0].message.content
            track_ai_agent_action('ERP Copilot', 'chat', approved=True)
        except ValueError as ve:
            track_ai_agent_action('ERP Copilot', 'chat', approved=False)
            return jsonify({'error': 'AI service not configured. Please contact administrator.'}), 503
        except Exception as ai_error:
            track_ai_agent_action('ERP Copilot', 'chat', approved=False)
            return jsonify({'error': 'AI service temporarily unavailable. Please try again.'}), 503
        
        return jsonify({
            'response': assistant_message,
            'context': {
                'module': module_context,
                'record_type': record_type,
                'record_id': record_id
            }
        })
        
    except Exception as e:
        return jsonify({'error': f'System error: {str(e)}'}), 500
    finally:
        conn.close()

@erp_helper_bp.route('/erp-helper/quick-insights', methods=['GET'])
def quick_insights():
    """Get quick insights for the current user"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    db = Database()
    conn = db.get_connection()
    
    try:
        insights = []
        
        overdue_so = conn.execute('''
            SELECT COUNT(*) as count FROM sales_orders 
            WHERE status NOT IN ('Shipped', 'Delivered', 'Cancelled', 'Closed')
            AND expected_ship_date < date('now')
        ''').fetchone()['count']
        
        if overdue_so > 0:
            insights.append({
                'type': 'warning',
                'icon': 'exclamation-triangle',
                'title': f'{overdue_so} Overdue Sales Orders',
                'message': 'These orders need immediate attention to meet customer expectations.'
            })
        
        overdue_wo = conn.execute('''
            SELECT COUNT(*) as count FROM work_orders 
            WHERE status NOT IN ('Completed', 'Cancelled', 'Closed')
            AND planned_end_date < date('now')
        ''').fetchone()['count']
        
        if overdue_wo > 0:
            insights.append({
                'type': 'warning',
                'icon': 'clock-history',
                'title': f'{overdue_wo} Overdue Work Orders',
                'message': 'Production is behind schedule on these orders.'
            })
        
        low_stock = conn.execute('''
            SELECT COUNT(*) as count FROM inventory 
            WHERE quantity <= reorder_point AND quantity > 0
        ''').fetchone()['count']
        
        if low_stock > 0:
            insights.append({
                'type': 'info',
                'icon': 'box-seam',
                'title': f'{low_stock} Items Low in Stock',
                'message': 'Consider creating purchase orders to replenish inventory.'
            })
        
        out_of_stock = conn.execute('''
            SELECT COUNT(*) as count FROM inventory WHERE quantity <= 0
        ''').fetchone()['count']
        
        if out_of_stock > 0:
            insights.append({
                'type': 'danger',
                'icon': 'x-circle',
                'title': f'{out_of_stock} Items Out of Stock',
                'message': 'These items need immediate reorder to avoid production delays.'
            })
        
        overdue_invoices = conn.execute('''
            SELECT COUNT(*) as count FROM invoices 
            WHERE status NOT IN ('Paid', 'Cancelled', 'Voided') 
            AND due_date < date('now')
        ''').fetchone()['count']
        
        if overdue_invoices > 0:
            insights.append({
                'type': 'warning',
                'icon': 'receipt',
                'title': f'{overdue_invoices} Overdue Invoices',
                'message': 'Follow up on outstanding payments to improve cash flow.'
            })
        
        pending_approval = conn.execute('''
            SELECT COUNT(*) as count FROM purchase_orders WHERE status = 'Submitted'
        ''').fetchone()['count']
        
        if pending_approval > 0:
            insights.append({
                'type': 'info',
                'icon': 'clipboard-check',
                'title': f'{pending_approval} POs Pending Approval',
                'message': 'Purchase orders awaiting review and approval.'
            })
        
        if not insights:
            insights.append({
                'type': 'success',
                'icon': 'check-circle',
                'title': 'All Systems Normal',
                'message': 'No urgent issues detected. Operations running smoothly.'
            })
        
        return jsonify({'insights': insights})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@erp_helper_bp.route('/erp-helper/suggestions', methods=['POST'])
def get_suggestions():
    """Get context-aware suggestions for the current screen"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    data = request.get_json()
    module = data.get('module', '')
    record_type = data.get('record_type', '')
    record_id = data.get('record_id')
    status = data.get('status', '')
    
    suggestions = []
    
    if record_type == 'sales_order':
        if status == 'Draft':
            suggestions = [
                "What fields are required to confirm this order?",
                "Check inventory availability for this order",
                "What is the customer's credit status?"
            ]
        elif status == 'Confirmed':
            suggestions = [
                "What's the next step to release this order?",
                "Can I create a work order from this?",
                "What materials are needed?"
            ]
        elif status in ['Released', 'In Progress']:
            suggestions = [
                "What's the production status?",
                "When can this order be shipped?",
                "Are there any blocking issues?"
            ]
            
    elif record_type == 'work_order':
        if status == 'Draft':
            suggestions = [
                "What's needed to release this work order?",
                "Check material availability",
                "Review the BOM for this product"
            ]
        elif status == 'Released':
            suggestions = [
                "How do I start production?",
                "Issue materials to this work order",
                "Assign labor resources"
            ]
        elif status == 'In Progress':
            suggestions = [
                "How do I complete this work order?",
                "Log labor hours worked",
                "Report a quality issue"
            ]
            
    elif record_type == 'purchase_order':
        if status == 'Draft':
            suggestions = [
                "Submit this PO for approval",
                "Add more line items",
                "Check supplier lead times"
            ]
        elif status == 'Approved':
            suggestions = [
                "When is delivery expected?",
                "How do I receive this shipment?",
                "Track this order status"
            ]
            
    elif record_type == 'invoice':
        if status == 'Draft':
            suggestions = [
                "What's needed to approve this invoice?",
                "Review invoice line items",
                "Check customer payment terms"
            ]
        elif status in ['Posted', 'Sent']:
            suggestions = [
                "When is payment due?",
                "Record a payment",
                "Send payment reminder"
            ]
    
    if not suggestions and module:
        suggestions = [
            f"How do I navigate the {module} module?",
            "What reports are available?",
            "Show me my pending tasks"
        ]
    
    if not suggestions:
        suggestions = [
            "What can you help me with?",
            "Show me system alerts",
            "What needs my attention today?"
        ]
    
    return jsonify({'suggestions': suggestions})
