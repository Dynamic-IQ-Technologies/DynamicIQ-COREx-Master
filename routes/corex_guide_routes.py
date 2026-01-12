from flask import Blueprint, request, jsonify, session
from models import Database
import os
import json

corex_guide_bp = Blueprint('corex_guide_routes', __name__)

def get_openai_client():
    try:
        from openai import OpenAI
        return OpenAI()
    except Exception as e:
        print(f"OpenAI client error: {e}")
        return None

COREX_GUIDE_SYSTEM_PROMPT = """You are COREx Guide, an embedded, proactive AI assistant inside the Dynamic.IQ-COREx enterprise platform. Your purpose is to intelligently guide users step-by-step in creating, validating, and completing transactions across the system.

You operate with full awareness of:
- Current module (Sales, Inventory, MRO, Finance, Compliance, etc.)
- User role and permission level
- Field-level data states (empty, invalid, conflicting, complete)
- Transaction lifecycle status (draft, pending, submitted, approved)
- Business rules, validations, and compliance requirements

CORE BEHAVIORAL PRINCIPLES:

1. Proactive, Not Reactive
- Detect when a user starts, pauses, or enters invalid data
- Offer guidance before errors occur
- Anticipate next logical steps in the transaction

2. Friendly, Human-Like Guidance
- Use clear, conversational language
- Never sound technical unless required
- Avoid blame or correction language
- Encourage completion and confidence

3. Context-Driven Intelligence
- Tailor guidance to the specific transaction type
- Consider the user's role and experience level
- Respect industry and regulatory constraints
- Never give generic instructions

4. Actionable Assistance
- Explain what to do and why it matters
- Offer inline examples and smart defaults
- Suggest auto-completion where possible

RESPONSE FORMAT:
- Keep responses concise (2-4 sentences for quick guidance, up to a paragraph for explanations)
- Use bullet points for multi-step instructions
- Bold important field names or values using **field**
- Include specific examples when helpful
- End guidance with a clear next step or question

TRANSACTION TYPES YOU ASSIST WITH:
- Sales Orders (customer selection, pricing, shipping, terms)
- Purchase Orders (supplier, items, quantities, delivery)
- Work Orders (routing, materials, labor, scheduling)
- Inventory (receiving, adjustments, transfers, labels)
- Invoicing (billing, terms, accounting codes)
- Quotes (pricing strategies, margins, validity)

COMPLIANCE AWARENESS:
- Flag when hazardous materials require additional documentation
- Note when export regulations may apply
- Highlight when quality certifications are needed
- Remind about core return requirements for exchange orders

Always maintain a helpful, professional tone that makes users feel supported rather than monitored."""

@corex_guide_bp.route('/api/corex-guide/assist', methods=['POST'])
def guide_assist():
    """Main endpoint for COREx Guide assistance"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    data = request.get_json()
    context = data.get('context', {})
    user_query = data.get('query', '')
    field_context = data.get('field_context', {})
    transaction_type = data.get('transaction_type', 'general')
    page_data = data.get('page_data', {})
    
    db = Database()
    conn = db.get_connection()
    
    user = conn.execute('SELECT username, role FROM users WHERE id = ?', (session['user_id'],)).fetchone()
    user_role = user[1] if user else 'user'
    username = user[0] if user else 'User'
    
    context_message = f"""
CURRENT CONTEXT:
- User: {username} (Role: {user_role})
- Transaction Type: {transaction_type}
- Current Page: {context.get('page', 'Unknown')}
- Current Action: {context.get('action', 'viewing')}

FIELD CONTEXT:
{json.dumps(field_context, indent=2) if field_context else 'No specific field context'}

PAGE DATA SUMMARY:
{json.dumps(page_data, indent=2) if page_data else 'No page data available'}

USER REQUEST:
{user_query if user_query else 'User is viewing/interacting with the page - provide proactive guidance based on context.'}
"""
    
    conn.close()
    
    client = get_openai_client()
    if not client:
        return jsonify({
            'response': "I'm here to help you complete this transaction. What would you like assistance with?",
            'suggestions': ['Explain required fields', 'Check for errors', 'What should I do next?']
        })
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": COREX_GUIDE_SYSTEM_PROMPT},
                {"role": "user", "content": context_message}
            ],
            max_tokens=500,
            temperature=0.7
        )
        
        guide_response = response.choices[0].message.content
        
        suggestions = generate_contextual_suggestions(transaction_type, context)
        
        return jsonify({
            'response': guide_response,
            'suggestions': suggestions
        })
        
    except Exception as e:
        print(f"COREx Guide error: {e}")
        return jsonify({
            'response': "I'm ready to help you with this transaction. Let me know what you need assistance with.",
            'suggestions': ['Help me get started', 'What fields are required?', 'Check my entries']
        })

@corex_guide_bp.route('/api/corex-guide/validate-field', methods=['POST'])
def validate_field():
    """Validate a specific field and provide guidance"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    data = request.get_json()
    field_name = data.get('field_name', '')
    field_value = data.get('field_value', '')
    field_type = data.get('field_type', 'text')
    transaction_type = data.get('transaction_type', 'general')
    related_fields = data.get('related_fields', {})
    is_required = data.get('is_required', False)
    
    validation_context = f"""
A user is entering data in a {transaction_type} transaction.

FIELD BEING VALIDATED:
- Field Name: {field_name}
- Current Value: {field_value}
- Field Type: {field_type}
- Is Required: {'Yes' if is_required else 'No'}

RELATED FIELD VALUES:
{json.dumps(related_fields, indent=2) if related_fields else 'None provided'}

Provide a brief validation response:
1. If the field is empty and required, remind the user it's needed
2. If the field has a value, is it appropriate for this field?
3. Any warnings, suggestions, or conflicts with related fields?

Keep response to 1-2 sentences. Be encouraging if valid, helpful if needs correction.
"""
    
    client = get_openai_client()
    if not client:
        return jsonify({'valid': True, 'message': '', 'severity': 'info'})
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": COREX_GUIDE_SYSTEM_PROMPT},
                {"role": "user", "content": validation_context}
            ],
            max_tokens=150,
            temperature=0.5
        )
        
        validation_response = response.choices[0].message.content
        
        severity = 'info'
        if any(word in validation_response.lower() for word in ['error', 'invalid', 'must', 'required', 'cannot']):
            severity = 'warning'
        elif any(word in validation_response.lower() for word in ['great', 'good', 'perfect', 'looks good']):
            severity = 'success'
        
        return jsonify({
            'valid': severity != 'error',
            'message': validation_response,
            'severity': severity
        })
        
    except Exception as e:
        print(f"Field validation error: {e}")
        return jsonify({'valid': True, 'message': '', 'severity': 'info'})

@corex_guide_bp.route('/api/corex-guide/transaction-check', methods=['POST'])
def transaction_check():
    """Run a comprehensive transaction check before submission"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    data = request.get_json()
    transaction_type = data.get('transaction_type', 'general')
    form_data = data.get('form_data', {})
    required_fields = data.get('required_fields', [])
    
    missing_fields = []
    for field in required_fields:
        if field not in form_data or not form_data[field]:
            missing_fields.append(field)
    
    check_context = f"""
A user is about to submit a {transaction_type} transaction. Please review and provide a pre-submission check.

FORM DATA:
{json.dumps(form_data, indent=2)}

REQUIRED FIELDS: {', '.join(required_fields)}
MISSING FIELDS: {', '.join(missing_fields) if missing_fields else 'None'}

Provide a brief pre-submission summary:
1. List any missing required fields
2. Flag any potential issues or conflicts
3. Note any compliance considerations
4. Give a confidence assessment (ready to submit, needs attention, or has issues)

Format as a clear, actionable checklist.
"""
    
    client = get_openai_client()
    if not client:
        return jsonify({
            'ready': len(missing_fields) == 0,
            'missing_fields': missing_fields,
            'message': 'Unable to perform AI check. Please verify all required fields are complete.',
            'issues': []
        })
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": COREX_GUIDE_SYSTEM_PROMPT},
                {"role": "user", "content": check_context}
            ],
            max_tokens=400,
            temperature=0.5
        )
        
        check_response = response.choices[0].message.content
        
        return jsonify({
            'ready': len(missing_fields) == 0,
            'missing_fields': missing_fields,
            'message': check_response,
            'issues': []
        })
        
    except Exception as e:
        print(f"Transaction check error: {e}")
        return jsonify({
            'ready': len(missing_fields) == 0,
            'missing_fields': missing_fields,
            'message': 'Please ensure all required fields are completed before submitting.',
            'issues': []
        })

def generate_contextual_suggestions(transaction_type, context):
    """Generate contextual quick-action suggestions"""
    base_suggestions = ['What should I do next?', 'Check for issues']
    
    type_suggestions = {
        'sales_order': ['Explain order types', 'Help with pricing', 'Shipping options'],
        'purchase_order': ['Supplier guidance', 'Quantity help', 'Delivery terms'],
        'work_order': ['Routing steps', 'Material requirements', 'Labor estimation'],
        'inventory': ['Stock adjustments', 'Receiving process', 'Label printing'],
        'invoice': ['Billing terms', 'Payment options', 'Accounting codes'],
        'quote': ['Pricing strategy', 'Margin calculation', 'Validity period']
    }
    
    suggestions = type_suggestions.get(transaction_type, base_suggestions)
    return suggestions[:3]
