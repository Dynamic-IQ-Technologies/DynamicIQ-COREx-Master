from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from models import Database, AuditLogger
from datetime import datetime, timedelta
import secrets
import os

rfq_portal_bp = Blueprint('rfq_portal', __name__)

def generate_secure_token():
    """Generate a secure random token for supplier RFQ links"""
    return secrets.token_urlsafe(32)

def validate_token(token):
    """Validate an RFQ supplier token and return token data if valid"""
    db = Database()
    conn = db.get_connection()
    
    token_data = conn.execute('''
        SELECT rst.*, r.rfq_number, r.title, r.description, r.due_date, r.status as rfq_status,
               r.currency, r.terms_conditions, r.buyer_name, r.buyer_email, r.buyer_phone,
               s.name as supplier_name, s.code as supplier_code, s.email as supplier_email
        FROM rfq_supplier_tokens rst
        JOIN rfqs r ON rst.rfq_id = r.id
        JOIN suppliers s ON rst.supplier_id = s.id
        WHERE rst.token = ?
    ''', (token,)).fetchone()
    
    conn.close()
    
    if not token_data:
        return None, "Invalid or expired link"
    
    if datetime.now() > datetime.fromisoformat(token_data['expires_at']):
        return None, "This RFQ link has expired"
    
    if token_data['rfq_status'] == 'Closed':
        return None, "This RFQ has been closed"
    
    if token_data['is_used'] and not token_data['allow_multiple_submissions']:
        return None, "A response has already been submitted for this RFQ"
    
    return dict(token_data), None


@rfq_portal_bp.route('/rfq/submit/<token>')
def supplier_rfq_view(token):
    """Public supplier RFQ view - no login required"""
    token_data, error = validate_token(token)
    
    if error:
        return render_template('rfq_portal/error.html', error=error)
    
    db = Database()
    conn = db.get_connection()
    
    conn.execute('''
        UPDATE rfq_supplier_tokens SET last_accessed_at = ? WHERE token = ?
    ''', (datetime.now().isoformat(), token))
    conn.commit()
    
    lines = conn.execute('''
        SELECT rl.*, p.code as part_number, p.name as product_name, u.uom_code
        FROM rfq_lines rl
        LEFT JOIN products p ON rl.product_id = p.id
        LEFT JOIN uom_master u ON rl.uom_id = u.id
        WHERE rl.rfq_id = ?
        ORDER BY rl.line_number
    ''', (token_data['rfq_id'],)).fetchall()
    
    existing_response = conn.execute('''
        SELECT * FROM rfq_supplier_responses
        WHERE rfq_id = ? AND supplier_id = ?
        ORDER BY submitted_at DESC LIMIT 1
    ''', (token_data['rfq_id'], token_data['supplier_id'])).fetchone()
    
    existing_line_responses = {}
    if existing_response:
        line_responses = conn.execute('''
            SELECT * FROM rfq_response_lines WHERE response_id = ?
        ''', (existing_response['id'],)).fetchall()
        existing_line_responses = {lr['rfq_line_id']: dict(lr) for lr in line_responses}
    
    conn.close()
    
    return render_template('rfq_portal/submit.html',
                          token=token,
                          token_data=token_data,
                          lines=[dict(l) for l in lines],
                          existing_response=dict(existing_response) if existing_response else None,
                          existing_line_responses=existing_line_responses)


@rfq_portal_bp.route('/rfq/submit/<token>', methods=['POST'])
def supplier_rfq_submit(token):
    """Process supplier RFQ submission"""
    token_data, error = validate_token(token)
    
    if error:
        return render_template('rfq_portal/error.html', error=error)
    
    db = Database()
    conn = db.get_connection()
    
    try:
        valid_until = request.form.get('valid_until') or None
        notes = request.form.get('notes', '')
        
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO rfq_supplier_responses 
            (rfq_id, supplier_id, token_id, valid_until, notes, ip_address, user_agent)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            token_data['rfq_id'],
            token_data['supplier_id'],
            token_data['id'],
            valid_until,
            notes,
            request.remote_addr,
            request.user_agent.string[:500] if request.user_agent else None
        ))
        response_id = cursor.lastrowid
        
        lines = conn.execute('SELECT id FROM rfq_lines WHERE rfq_id = ?', 
                            (token_data['rfq_id'],)).fetchall()
        
        total_amount = 0
        for line in lines:
            line_id = line['id']
            unit_price = request.form.get(f'unit_price_{line_id}')
            lead_time = request.form.get(f'lead_time_{line_id}')
            line_notes = request.form.get(f'notes_{line_id}', '')
            
            if unit_price and lead_time:
                unit_price = float(unit_price)
                lead_time = int(lead_time)
                
                line_data = conn.execute('SELECT quantity FROM rfq_lines WHERE id = ?', (line_id,)).fetchone()
                total_amount += unit_price * (line_data['quantity'] or 1)
                
                conn.execute('''
                    INSERT INTO rfq_response_lines (response_id, rfq_line_id, unit_price, lead_time_days, notes)
                    VALUES (?, ?, ?, ?, ?)
                ''', (response_id, line_id, unit_price, lead_time, line_notes))
        
        conn.execute('UPDATE rfq_supplier_responses SET total_amount = ? WHERE id = ?',
                    (total_amount, response_id))
        
        if token_data['allow_multiple_submissions']:
            conn.execute('''
                UPDATE rfq_supplier_tokens 
                SET submission_count = submission_count + 1, last_accessed_at = ?
                WHERE id = ?
            ''', (datetime.now().isoformat(), token_data['id']))
        else:
            conn.execute('''
                UPDATE rfq_supplier_tokens 
                SET is_used = 1, submission_count = submission_count + 1, last_accessed_at = ?
                WHERE id = ?
            ''', (datetime.now().isoformat(), token_data['id']))
        
        conn.execute('''
            UPDATE rfq_suppliers 
            SET response_status = 'Received', response_date = ?
            WHERE rfq_id = ? AND supplier_id = ?
        ''', (datetime.now().isoformat(), token_data['rfq_id'], token_data['supplier_id']))
        
        current_status = conn.execute("SELECT status FROM rfqs WHERE id = ?", 
                                      (token_data['rfq_id'],)).fetchone()['status']
        if current_status == 'Issued':
            conn.execute("UPDATE rfqs SET status = 'Quotes Received' WHERE id = ?", 
                        (token_data['rfq_id'],))
        
        conn.commit()
        conn.close()
        
        return redirect(url_for('rfq_portal.submission_success', token=token))
        
    except Exception as e:
        conn.rollback()
        conn.close()
        return render_template('rfq_portal/error.html', error=f'An error occurred: {str(e)}')


@rfq_portal_bp.route('/rfq/success/<token>')
def submission_success(token):
    """Show submission success page"""
    token_data, _ = validate_token(token)
    return render_template('rfq_portal/success.html', token_data=token_data)
