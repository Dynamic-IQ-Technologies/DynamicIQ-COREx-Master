from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session
from functools import wraps
from models import Database, User
from auth import role_required
from datetime import datetime, timedelta
import secrets
import os

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.', 'danger')
            return redirect(url_for('auth_routes.login'))
        return f(*args, **kwargs)
    return decorated_function

class CurrentUser:
    """Session-based current user proxy"""
    @property
    def id(self):
        return session.get('user_id')
    
    @property
    def username(self):
        return session.get('username')
    
    @property
    def role(self):
        return session.get('role')

current_user = CurrentUser()

leads_bp = Blueprint('leads', __name__)
db = Database()

BUSINESS_TYPES = ['Airline', 'MRO', 'OEM', 'Supplier', 'Broker', 'Operator', 'Government', 'Other']
LEAD_SOURCES = ['Website Form', 'Referral', 'Trade Show', 'Cold Outreach', 'Partner', 'RFP/RFQ', 'LinkedIn', 'Email Campaign', 'Phone Inquiry', 'Other']
SERVICES = ['Repair', 'Overhaul', 'Exchange', 'Manufacturing', 'Software', 'Consulting', 'Parts Sales', 'Maintenance']
URGENCY_LEVELS = ['AOG', 'Critical', 'Urgent', 'Routine']
LEAD_STATUSES = ['New', 'Contacted', 'Qualified', 'In Evaluation', 'Approved for Conversion', 'Converted', 'Disqualified']
ACTIVITY_TYPES = ['Call', 'Email', 'Meeting', 'Note', 'Task', 'Follow-up', 'Quote Sent', 'Demo', 'Site Visit']

def generate_lead_number():
    conn = db.get_connection()
    result = conn.execute("SELECT MAX(CAST(SUBSTR(lead_number, 5) AS INTEGER)) as max_num FROM leads WHERE lead_number LIKE 'LD-%'").fetchone()
    next_num = (result['max_num'] or 0) + 1
    conn.close()
    return f"LD-{next_num:05d}"

def calculate_lead_score(lead):
    score = 0
    notes = []
    
    if lead.get('estimated_spend'):
        spend = float(lead.get('estimated_spend') or 0)
        if spend >= 100000:
            score += 30
            notes.append("High revenue potential (+30)")
        elif spend >= 50000:
            score += 20
            notes.append("Medium revenue potential (+20)")
        elif spend >= 10000:
            score += 10
            notes.append("Low revenue potential (+10)")
    
    urgency = lead.get('urgency', 'Routine')
    if urgency == 'AOG':
        score += 25
        notes.append("AOG urgency (+25)")
    elif urgency == 'Critical':
        score += 15
        notes.append("Critical urgency (+15)")
    elif urgency == 'Urgent':
        score += 10
        notes.append("Urgent need (+10)")
    
    business_type = lead.get('business_type', '')
    if business_type in ['Airline', 'OEM']:
        score += 15
        notes.append(f"Strategic business type: {business_type} (+15)")
    elif business_type in ['MRO', 'Operator']:
        score += 10
        notes.append(f"Good business type: {business_type} (+10)")
    
    if lead.get('compliance_certs'):
        certs = lead.get('compliance_certs', '')
        if 'FAA' in certs or 'EASA' in certs:
            score += 10
            notes.append("Compliance ready (+10)")
    
    if lead.get('contact_email') and lead.get('contact_phone'):
        score += 5
        notes.append("Complete contact info (+5)")
    
    if lead.get('services_of_interest'):
        services = lead.get('services_of_interest', '')
        service_count = len(services.split(',')) if services else 0
        if service_count >= 3:
            score += 10
            notes.append("Multiple services interest (+10)")
        elif service_count >= 1:
            score += 5
            notes.append("Service interest (+5)")
    
    if lead.get('website'):
        score += 5
        notes.append("Website provided (+5)")
    
    if score >= 70:
        category = 'Hot'
    elif score >= 40:
        category = 'Warm'
    else:
        category = 'Cold'
    
    return score, category, '\n'.join(notes)

@leads_bp.route('/leads')
@login_required
@role_required(['Admin', 'Finance', 'Supervisor', 'Sales'])
def leads_list():
    conn = db.get_connection()
    
    status_filter = request.args.get('status', '')
    source_filter = request.args.get('source', '')
    score_filter = request.args.get('score_category', '')
    type_filter = request.args.get('lead_type', '')
    search = request.args.get('search', '')
    
    query = '''
        SELECT l.*, u.username as assigned_username
        FROM leads l
        LEFT JOIN users u ON l.assigned_to = u.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND l.status = ?'
        params.append(status_filter)
    if source_filter:
        query += ' AND l.lead_source = ?'
        params.append(source_filter)
    if score_filter:
        query += ' AND l.score_category = ?'
        params.append(score_filter)
    if type_filter:
        query += ' AND l.lead_type = ?'
        params.append(type_filter)
    if search:
        query += ' AND (l.company_name LIKE ? OR l.contact_name LIKE ? OR l.contact_email LIKE ?)'
        params.extend([f'%{search}%', f'%{search}%', f'%{search}%'])
    
    query += ' ORDER BY l.created_at DESC'
    
    leads = conn.execute(query, params).fetchall()
    
    stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'New' THEN 1 ELSE 0 END) as new_count,
            SUM(CASE WHEN status = 'Qualified' THEN 1 ELSE 0 END) as qualified_count,
            SUM(CASE WHEN status = 'Converted' THEN 1 ELSE 0 END) as converted_count,
            SUM(CASE WHEN score_category = 'Hot' THEN 1 ELSE 0 END) as hot_count,
            SUM(CASE WHEN score_category = 'Warm' THEN 1 ELSE 0 END) as warm_count
        FROM leads
    ''').fetchone()
    
    users = conn.execute("SELECT id, username FROM users WHERE is_active = 1 ORDER BY username").fetchall()
    
    conn.close()
    
    return render_template('leads/list.html',
                         leads=leads,
                         stats=stats,
                         users=users,
                         statuses=LEAD_STATUSES,
                         sources=LEAD_SOURCES,
                         current_status=status_filter,
                         current_source=source_filter,
                         current_score=score_filter,
                         current_type=type_filter,
                         search=search)

@leads_bp.route('/leads/new', methods=['GET', 'POST'])
@login_required
@role_required(['Admin', 'Finance', 'Supervisor', 'Sales'])
def create_lead():
    conn = db.get_connection()
    
    if request.method == 'POST':
        lead_number = generate_lead_number()
        
        lead_data = {
            'lead_number': lead_number,
            'company_name': request.form.get('company_name'),
            'business_type': request.form.get('business_type'),
            'aircraft_platform_focus': request.form.get('aircraft_platform_focus'),
            'country': request.form.get('country'),
            'region': request.form.get('region'),
            'website': request.form.get('website'),
            'contact_name': request.form.get('contact_name'),
            'contact_title': request.form.get('contact_title'),
            'contact_email': request.form.get('contact_email'),
            'contact_phone': request.form.get('contact_phone'),
            'preferred_contact_method': request.form.get('preferred_contact_method', 'Email'),
            'lead_source': request.form.get('lead_source'),
            'lead_source_detail': request.form.get('lead_source_detail'),
            'services_of_interest': ','.join(request.form.getlist('services_of_interest')),
            'parts_ata_chapters': request.form.get('parts_ata_chapters'),
            'aircraft_types': request.form.get('aircraft_types'),
            'estimated_volume': request.form.get('estimated_volume'),
            'estimated_spend': request.form.get('estimated_spend') or None,
            'urgency': request.form.get('urgency', 'Routine'),
            'compliance_certs': ','.join(request.form.getlist('compliance_certs')),
            'customer_approval_required': 1 if request.form.get('customer_approval_required') else 0,
            'supplier_certification': request.form.get('supplier_certification'),
            'lead_type': request.form.get('lead_type', 'Customer'),
            'assigned_to': request.form.get('assigned_to') or None,
            'status': 'New',
            'created_by': current_user.id
        }
        
        score, category, notes = calculate_lead_score(lead_data)
        lead_data['score'] = score
        lead_data['score_category'] = category
        lead_data['evaluation_notes'] = notes
        
        columns = ', '.join(lead_data.keys())
        placeholders = ', '.join(['?' for _ in lead_data])
        
        cursor = conn.execute(f'INSERT INTO leads ({columns}) VALUES ({placeholders})', list(lead_data.values()))
        lead_id = cursor.lastrowid
        
        conn.execute('''
            INSERT INTO lead_activities (lead_id, activity_type, subject, description, created_by)
            VALUES (?, 'Note', 'Lead Created', ?, ?)
        ''', (lead_id, f'Lead {lead_number} created for {lead_data["company_name"]}', current_user.id))
        
        conn.commit()
        conn.close()
        
        flash(f'Lead {lead_number} created successfully', 'success')
        return redirect(url_for('leads.view_lead', lead_id=lead_id))
    
    users = conn.execute("SELECT id, username FROM users WHERE is_active = 1 ORDER BY username").fetchall()
    conn.close()
    
    return render_template('leads/form.html',
                         lead=None,
                         users=users,
                         business_types=BUSINESS_TYPES,
                         lead_sources=LEAD_SOURCES,
                         services=SERVICES,
                         urgency_levels=URGENCY_LEVELS)

@leads_bp.route('/leads/<int:lead_id>')
@login_required
@role_required(['Admin', 'Finance', 'Supervisor', 'Sales'])
def view_lead(lead_id):
    conn = db.get_connection()
    
    lead = conn.execute('''
        SELECT l.*, u.username as assigned_username, 
               cu.username as created_username,
               conv.username as converted_username,
               c.name as customer_name, c.customer_number,
               s.name as supplier_name, s.code as supplier_code
        FROM leads l
        LEFT JOIN users u ON l.assigned_to = u.id
        LEFT JOIN users cu ON l.created_by = cu.id
        LEFT JOIN users conv ON l.converted_by = conv.id
        LEFT JOIN customers c ON l.converted_to_customer_id = c.id
        LEFT JOIN suppliers s ON l.converted_to_supplier_id = s.id
        WHERE l.id = ?
    ''', (lead_id,)).fetchone()
    
    if not lead:
        flash('Lead not found', 'error')
        return redirect(url_for('leads.leads_list'))
    
    activities = conn.execute('''
        SELECT la.*, u.username
        FROM lead_activities la
        LEFT JOIN users u ON la.created_by = u.id
        WHERE la.lead_id = ?
        ORDER BY la.created_at DESC
    ''', (lead_id,)).fetchall()
    
    documents = conn.execute('''
        SELECT ld.*, u.username as uploader
        FROM lead_documents ld
        LEFT JOIN users u ON ld.uploaded_by = u.id
        WHERE ld.lead_id = ?
        ORDER BY ld.uploaded_at DESC
    ''', (lead_id,)).fetchall()
    
    users = conn.execute("SELECT id, username FROM users WHERE is_active = 1 ORDER BY username").fetchall()
    
    conn.close()
    
    return render_template('leads/view.html',
                         lead=lead,
                         activities=activities,
                         documents=documents,
                         users=users,
                         statuses=LEAD_STATUSES,
                         activity_types=ACTIVITY_TYPES)

@leads_bp.route('/leads/<int:lead_id>/edit', methods=['GET', 'POST'])
@login_required
@role_required(['Admin', 'Finance', 'Supervisor', 'Sales'])
def edit_lead(lead_id):
    conn = db.get_connection()
    
    lead = conn.execute('SELECT * FROM leads WHERE id = ?', (lead_id,)).fetchone()
    if not lead:
        flash('Lead not found', 'error')
        return redirect(url_for('leads.leads_list'))
    
    if lead['status'] == 'Converted':
        flash('Cannot edit a converted lead', 'error')
        return redirect(url_for('leads.view_lead', lead_id=lead_id))
    
    if request.method == 'POST':
        lead_data = {
            'company_name': request.form.get('company_name'),
            'business_type': request.form.get('business_type'),
            'aircraft_platform_focus': request.form.get('aircraft_platform_focus'),
            'country': request.form.get('country'),
            'region': request.form.get('region'),
            'website': request.form.get('website'),
            'contact_name': request.form.get('contact_name'),
            'contact_title': request.form.get('contact_title'),
            'contact_email': request.form.get('contact_email'),
            'contact_phone': request.form.get('contact_phone'),
            'preferred_contact_method': request.form.get('preferred_contact_method', 'Email'),
            'lead_source': request.form.get('lead_source'),
            'lead_source_detail': request.form.get('lead_source_detail'),
            'services_of_interest': ','.join(request.form.getlist('services_of_interest')),
            'parts_ata_chapters': request.form.get('parts_ata_chapters'),
            'aircraft_types': request.form.get('aircraft_types'),
            'estimated_volume': request.form.get('estimated_volume'),
            'estimated_spend': request.form.get('estimated_spend') or None,
            'urgency': request.form.get('urgency', 'Routine'),
            'compliance_certs': ','.join(request.form.getlist('compliance_certs')),
            'customer_approval_required': 1 if request.form.get('customer_approval_required') else 0,
            'supplier_certification': request.form.get('supplier_certification'),
            'lead_type': request.form.get('lead_type', 'Customer'),
            'assigned_to': request.form.get('assigned_to') or None,
            'updated_at': datetime.now().isoformat()
        }
        
        score, category, notes = calculate_lead_score(lead_data)
        lead_data['score'] = score
        lead_data['score_category'] = category
        lead_data['evaluation_notes'] = notes
        
        set_clause = ', '.join([f'{k} = ?' for k in lead_data.keys()])
        conn.execute(f'UPDATE leads SET {set_clause} WHERE id = ?', list(lead_data.values()) + [lead_id])
        conn.commit()
        conn.close()
        
        flash('Lead updated successfully', 'success')
        return redirect(url_for('leads.view_lead', lead_id=lead_id))
    
    users = conn.execute("SELECT id, username FROM users WHERE is_active = 1 ORDER BY username").fetchall()
    conn.close()
    
    return render_template('leads/form.html',
                         lead=lead,
                         users=users,
                         business_types=BUSINESS_TYPES,
                         lead_sources=LEAD_SOURCES,
                         services=SERVICES,
                         urgency_levels=URGENCY_LEVELS)

@leads_bp.route('/leads/<int:lead_id>/status', methods=['POST'])
@login_required
@role_required(['Admin', 'Finance', 'Supervisor', 'Sales'])
def update_lead_status(lead_id):
    conn = db.get_connection()
    
    lead = conn.execute('SELECT * FROM leads WHERE id = ?', (lead_id,)).fetchone()
    if not lead:
        return jsonify({'success': False, 'error': 'Lead not found'}), 404
    
    new_status = request.form.get('status')
    reason = request.form.get('reason', '')
    
    if new_status not in LEAD_STATUSES:
        return jsonify({'success': False, 'error': 'Invalid status'}), 400
    
    old_status = lead['status']
    
    conn.execute('''
        UPDATE leads SET status = ?, status_reason = ?, updated_at = ? WHERE id = ?
    ''', (new_status, reason, datetime.now().isoformat(), lead_id))
    
    conn.execute('''
        INSERT INTO lead_activities (lead_id, activity_type, subject, description, created_by)
        VALUES (?, 'Note', 'Status Changed', ?, ?)
    ''', (lead_id, f'Status changed from {old_status} to {new_status}. {reason}', current_user.id))
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True})

@leads_bp.route('/leads/<int:lead_id>/activity', methods=['POST'])
@login_required
@role_required(['Admin', 'Finance', 'Supervisor', 'Sales'])
def add_activity(lead_id):
    conn = db.get_connection()
    
    lead = conn.execute('SELECT * FROM leads WHERE id = ?', (lead_id,)).fetchone()
    if not lead:
        return jsonify({'success': False, 'error': 'Lead not found'}), 404
    
    activity_type = request.form.get('activity_type')
    subject = request.form.get('subject')
    description = request.form.get('description')
    outcome = request.form.get('outcome')
    next_action = request.form.get('next_action')
    next_action_date = request.form.get('next_action_date') or None
    
    conn.execute('''
        INSERT INTO lead_activities (lead_id, activity_type, subject, description, outcome, next_action, next_action_date, created_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (lead_id, activity_type, subject, description, outcome, next_action, next_action_date, current_user.id))
    
    if lead['status'] == 'New':
        conn.execute("UPDATE leads SET status = 'Contacted', updated_at = ? WHERE id = ?", 
                    (datetime.now().isoformat(), lead_id))
    
    conn.commit()
    conn.close()
    
    flash('Activity added successfully', 'success')
    return redirect(url_for('leads.view_lead', lead_id=lead_id))

@leads_bp.route('/leads/<int:lead_id>/convert', methods=['POST'])
@login_required
@role_required(['Admin', 'Finance', 'Supervisor'])
def convert_lead(lead_id):
    conn = db.get_connection()
    
    lead = conn.execute('SELECT * FROM leads WHERE id = ?', (lead_id,)).fetchone()
    if not lead:
        return jsonify({'success': False, 'error': 'Lead not found'}), 404
    
    if lead['status'] == 'Converted':
        return jsonify({'success': False, 'error': 'Lead already converted'}), 400
    
    convert_to = request.form.get('convert_to', 'customer')
    
    try:
        if convert_to == 'customer':
            existing = conn.execute('SELECT id FROM customers WHERE name = ? OR email = ?', 
                                   (lead['company_name'], lead['contact_email'])).fetchone()
            if existing:
                return jsonify({'success': False, 'error': 'A customer with this name or email already exists'}), 400
            
            result = conn.execute("SELECT MAX(CAST(SUBSTR(customer_number, 5) AS INTEGER)) as max_num FROM customers WHERE customer_number LIKE 'CUS-%'").fetchone()
            next_num = (result['max_num'] or 0) + 1
            customer_number = f"CUS-{next_num:05d}"
            
            cursor = conn.execute('''
                INSERT INTO customers (customer_number, name, contact_person, email, phone, 
                                       billing_address, notes, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'Active')
            ''', (customer_number, lead['company_name'], lead['contact_name'], 
                  lead['contact_email'], lead['contact_phone'],
                  f"{lead['country'] or ''}", f"Converted from lead {lead['lead_number']}"))
            
            customer_id = cursor.lastrowid
            
            if lead['contact_name']:
                conn.execute('''
                    INSERT INTO customer_contacts (customer_id, contact_name, title, email, phone, is_primary)
                    VALUES (?, ?, ?, ?, ?, 1)
                ''', (customer_id, lead['contact_name'], lead['contact_title'], 
                      lead['contact_email'], lead['contact_phone']))
            
            conn.execute('''
                UPDATE leads SET status = 'Converted', converted_to_customer_id = ?, 
                                 converted_at = ?, converted_by = ?, updated_at = ?
                WHERE id = ?
            ''', (customer_id, datetime.now().isoformat(), current_user.id, 
                  datetime.now().isoformat(), lead_id))
            
            conn.execute('''
                INSERT INTO lead_activities (lead_id, activity_type, subject, description, created_by)
                VALUES (?, 'Note', 'Lead Converted', ?, ?)
            ''', (lead_id, f'Lead converted to Customer {customer_number}', current_user.id))
            
            conn.commit()
            conn.close()
            
            return jsonify({'success': True, 'message': f'Lead converted to Customer {customer_number}',
                          'redirect': url_for('sales.view_customer', customer_id=customer_id)})
        
        else:
            existing = conn.execute('SELECT id FROM suppliers WHERE name = ? OR email = ?', 
                                   (lead['company_name'], lead['contact_email'])).fetchone()
            if existing:
                return jsonify({'success': False, 'error': 'A supplier with this name or email already exists'}), 400
            
            result = conn.execute("SELECT MAX(CAST(SUBSTR(code, 5) AS INTEGER)) as max_num FROM suppliers WHERE code LIKE 'SUP-%'").fetchone()
            next_num = (result['max_num'] or 0) + 1
            supplier_code = f"SUP-{next_num:05d}"
            
            cursor = conn.execute('''
                INSERT INTO suppliers (code, name, contact_person, email, phone, address)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (supplier_code, lead['company_name'], lead['contact_name'], 
                  lead['contact_email'], lead['contact_phone'], lead['country'] or ''))
            
            supplier_id = cursor.lastrowid
            
            if lead['contact_name']:
                conn.execute('''
                    INSERT INTO supplier_contacts (supplier_id, contact_name, title, email, phone, is_primary)
                    VALUES (?, ?, ?, ?, ?, 1)
                ''', (supplier_id, lead['contact_name'], lead['contact_title'], 
                      lead['contact_email'], lead['contact_phone']))
            
            conn.execute('''
                UPDATE leads SET status = 'Converted', converted_to_supplier_id = ?, 
                                 converted_at = ?, converted_by = ?, updated_at = ?
                WHERE id = ?
            ''', (supplier_id, datetime.now().isoformat(), current_user.id, 
                  datetime.now().isoformat(), lead_id))
            
            conn.execute('''
                INSERT INTO lead_activities (lead_id, activity_type, subject, description, created_by)
                VALUES (?, 'Note', 'Lead Converted', ?, ?)
            ''', (lead_id, f'Lead converted to Supplier {supplier_code}', current_user.id))
            
            conn.commit()
            conn.close()
            
            return jsonify({'success': True, 'message': f'Lead converted to Supplier {supplier_code}',
                          'redirect': url_for('suppliers.view_supplier', supplier_id=supplier_id)})
    
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500

@leads_bp.route('/leads/<int:lead_id>/assign', methods=['POST'])
@login_required
@role_required(['Admin', 'Supervisor', 'Sales'])
def assign_lead(lead_id):
    conn = db.get_connection()
    
    user_id = request.form.get('user_id')
    
    conn.execute('UPDATE leads SET assigned_to = ?, updated_at = ? WHERE id = ?', 
                (user_id or None, datetime.now().isoformat(), lead_id))
    
    user = conn.execute('SELECT username FROM users WHERE id = ?', (user_id,)).fetchone() if user_id else None
    
    conn.execute('''
        INSERT INTO lead_activities (lead_id, activity_type, subject, description, created_by)
        VALUES (?, 'Note', 'Lead Assigned', ?, ?)
    ''', (lead_id, f'Lead assigned to {user["username"] if user else "Unassigned"}', current_user.id))
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True})

@leads_bp.route('/leads/<int:lead_id>/rescore', methods=['POST'])
@login_required
@role_required(['Admin', 'Supervisor', 'Sales'])
def rescore_lead(lead_id):
    conn = db.get_connection()
    
    lead = conn.execute('SELECT * FROM leads WHERE id = ?', (lead_id,)).fetchone()
    if not lead:
        return jsonify({'success': False, 'error': 'Lead not found'}), 404
    
    lead_dict = dict(lead)
    score, category, notes = calculate_lead_score(lead_dict)
    
    conn.execute('''
        UPDATE leads SET score = ?, score_category = ?, evaluation_notes = ?, updated_at = ?
        WHERE id = ?
    ''', (score, category, notes, datetime.now().isoformat(), lead_id))
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True, 'score': score, 'category': category, 'notes': notes})

@leads_bp.route('/leads/public-form/<token>')
def public_lead_form(token):
    return render_template('leads/public_form.html', token=token)

@leads_bp.route('/leads/submit-public', methods=['POST'])
def submit_public_lead():
    token = request.form.get('token')
    
    if not token or len(token) < 10:
        return jsonify({'success': False, 'error': 'Invalid form token'}), 400
    
    honeypot = request.form.get('website_url', '')
    if honeypot:
        return jsonify({'success': False, 'error': 'Spam detected'}), 400
    
    form_timestamp = request.form.get('form_timestamp', '')
    if form_timestamp:
        try:
            ts = int(form_timestamp)
            elapsed = (datetime.now().timestamp() * 1000) - ts
            if elapsed < 2000:
                return jsonify({'success': False, 'error': 'Please take your time filling the form'}), 400
        except (ValueError, TypeError):
            pass
    
    conn = db.get_connection()
    lead_number = generate_lead_number()
    
    lead_data = {
        'lead_number': lead_number,
        'company_name': request.form.get('company_name'),
        'business_type': request.form.get('business_type'),
        'country': request.form.get('country'),
        'website': request.form.get('website'),
        'contact_name': request.form.get('contact_name'),
        'contact_title': request.form.get('contact_title'),
        'contact_email': request.form.get('contact_email'),
        'contact_phone': request.form.get('contact_phone'),
        'preferred_contact_method': request.form.get('preferred_contact_method', 'Email'),
        'lead_source': 'Website Form',
        'lead_source_detail': request.form.get('form_type', 'Contact Sales'),
        'services_of_interest': ','.join(request.form.getlist('services_of_interest')),
        'aircraft_types': request.form.get('aircraft_types'),
        'urgency': request.form.get('urgency', 'Routine'),
        'lead_type': request.form.get('lead_type', 'Customer'),
        'status': 'New',
        'submission_ip': request.remote_addr
    }
    
    score, category, notes = calculate_lead_score(lead_data)
    lead_data['score'] = score
    lead_data['score_category'] = category
    lead_data['evaluation_notes'] = notes
    
    columns = ', '.join(lead_data.keys())
    placeholders = ', '.join(['?' for _ in lead_data])
    
    cursor = conn.execute(f'INSERT INTO leads ({columns}) VALUES ({placeholders})', list(lead_data.values()))
    lead_id = cursor.lastrowid
    
    conn.execute('''
        INSERT INTO lead_activities (lead_id, activity_type, subject, description)
        VALUES (?, 'Note', 'Web Form Submission', ?)
    ''', (lead_id, f'Lead submitted via public web form from IP {request.remote_addr}'))
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True, 'message': 'Thank you! Your inquiry has been submitted successfully.'})

@leads_bp.route('/leads/generate-form-link', methods=['POST'])
@login_required
@role_required(['Admin', 'Sales'])
def generate_form_link():
    token = secrets.token_urlsafe(32)
    form_type = request.form.get('form_type', 'contact')
    
    base_url = request.host_url.rstrip('/')
    link = f"{base_url}/leads/public-form/{token}?type={form_type}"
    
    return jsonify({'success': True, 'link': link, 'token': token})

@leads_bp.route('/leads/analytics')
@login_required
@role_required(['Admin', 'Finance', 'Supervisor', 'Sales'])
def leads_analytics():
    conn = db.get_connection()
    
    stats = conn.execute('''
        SELECT 
            COUNT(*) as total_leads,
            SUM(CASE WHEN status = 'Converted' THEN 1 ELSE 0 END) as converted,
            SUM(CASE WHEN status = 'Disqualified' THEN 1 ELSE 0 END) as disqualified,
            SUM(CASE WHEN score_category = 'Hot' THEN 1 ELSE 0 END) as hot,
            SUM(CASE WHEN score_category = 'Warm' THEN 1 ELSE 0 END) as warm,
            SUM(CASE WHEN score_category = 'Cold' THEN 1 ELSE 0 END) as cold,
            AVG(CASE WHEN status = 'Converted' 
                THEN JULIANDAY(converted_at) - JULIANDAY(created_at) END) as avg_conversion_days,
            SUM(CASE WHEN lead_type = 'Customer' THEN 1 ELSE 0 END) as customer_leads,
            SUM(CASE WHEN lead_type = 'Supplier' THEN 1 ELSE 0 END) as supplier_leads
        FROM leads
    ''').fetchone()
    
    by_source = conn.execute('''
        SELECT lead_source, COUNT(*) as count,
               SUM(CASE WHEN status = 'Converted' THEN 1 ELSE 0 END) as converted
        FROM leads
        WHERE lead_source IS NOT NULL
        GROUP BY lead_source
        ORDER BY count DESC
    ''').fetchall()
    
    by_status = conn.execute('''
        SELECT status, COUNT(*) as count
        FROM leads
        GROUP BY status
        ORDER BY count DESC
    ''').fetchall()
    
    monthly_leads = conn.execute('''
        SELECT strftime('%Y-%m', created_at) as month,
               COUNT(*) as count,
               SUM(CASE WHEN status = 'Converted' THEN 1 ELSE 0 END) as converted
        FROM leads
        WHERE created_at >= date('now', '-12 months')
        GROUP BY month
        ORDER BY month
    ''').fetchall()
    
    top_performers = conn.execute('''
        SELECT u.username, COUNT(l.id) as leads_assigned,
               SUM(CASE WHEN l.status = 'Converted' THEN 1 ELSE 0 END) as converted
        FROM leads l
        JOIN users u ON l.assigned_to = u.id
        GROUP BY l.assigned_to
        ORDER BY converted DESC
        LIMIT 10
    ''').fetchall()
    
    disqualified_reasons = conn.execute('''
        SELECT status_reason, COUNT(*) as count
        FROM leads
        WHERE status = 'Disqualified' AND status_reason IS NOT NULL AND status_reason != ''
        GROUP BY status_reason
        ORDER BY count DESC
        LIMIT 10
    ''').fetchall()
    
    conn.close()
    
    conversion_rate = (stats['converted'] / stats['total_leads'] * 100) if stats['total_leads'] > 0 else 0
    
    return render_template('leads/analytics.html',
                         stats=stats,
                         by_source=by_source,
                         by_status=by_status,
                         monthly_leads=monthly_leads,
                         top_performers=top_performers,
                         disqualified_reasons=disqualified_reasons,
                         conversion_rate=conversion_rate)

@leads_bp.route('/leads/copilot', methods=['POST'])
@login_required
@role_required(['Admin', 'Finance', 'Supervisor', 'Sales'])
def leads_copilot():
    try:
        from openai import OpenAI
        client = OpenAI()
    except Exception as e:
        return jsonify({'success': False, 'error': 'AI service not available'}), 500
    
    question = request.json.get('question', '')
    
    if not question:
        return jsonify({'success': False, 'error': 'Please provide a question'}), 400
    
    conn = db.get_connection()
    
    context_data = conn.execute('''
        SELECT 
            COUNT(*) as total_leads,
            SUM(CASE WHEN status = 'New' THEN 1 ELSE 0 END) as new_leads,
            SUM(CASE WHEN status = 'Qualified' THEN 1 ELSE 0 END) as qualified_leads,
            SUM(CASE WHEN status = 'Converted' THEN 1 ELSE 0 END) as converted_leads,
            SUM(CASE WHEN status = 'Disqualified' THEN 1 ELSE 0 END) as disqualified_leads,
            SUM(CASE WHEN score_category = 'Hot' THEN 1 ELSE 0 END) as hot_leads,
            SUM(CASE WHEN score_category = 'Warm' THEN 1 ELSE 0 END) as warm_leads,
            SUM(CASE WHEN created_at >= date('now', '-7 days') THEN 1 ELSE 0 END) as leads_this_week,
            AVG(score) as avg_score
        FROM leads
    ''').fetchone()
    
    top_leads = conn.execute('''
        SELECT lead_number, company_name, score, score_category, status, urgency, estimated_spend
        FROM leads
        WHERE status NOT IN ('Converted', 'Disqualified')
        ORDER BY score DESC
        LIMIT 10
    ''').fetchall()
    
    source_stats = conn.execute('''
        SELECT lead_source, COUNT(*) as count,
               SUM(CASE WHEN status = 'Converted' THEN 1 ELSE 0 END) as converted
        FROM leads
        WHERE lead_source IS NOT NULL
        GROUP BY lead_source
    ''').fetchall()
    
    conn.close()
    
    context = f"""
    Lead Management Data:
    - Total Leads: {context_data['total_leads']}
    - New Leads: {context_data['new_leads']}
    - Qualified Leads: {context_data['qualified_leads']}
    - Converted Leads: {context_data['converted_leads']}
    - Disqualified Leads: {context_data['disqualified_leads']}
    - Hot Leads: {context_data['hot_leads']}
    - Warm Leads: {context_data['warm_leads']}
    - Leads This Week: {context_data['leads_this_week']}
    - Average Score: {context_data['avg_score']:.1f if context_data['avg_score'] else 0}
    
    Top Priority Leads:
    {chr(10).join([f"- {l['lead_number']}: {l['company_name']} (Score: {l['score']}, {l['score_category']}, {l['status']}, Urgency: {l['urgency']})" for l in top_leads])}
    
    Lead Sources Performance:
    {chr(10).join([f"- {s['lead_source']}: {s['count']} leads, {s['converted']} converted ({s['converted']/s['count']*100:.0f}% conversion)" for s in source_stats if s['count'] > 0])}
    """
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": """You are an AI Sales Engagement Copilot for an Aviation MRO company. 
                Provide concise, actionable insights about leads and sales opportunities. 
                Focus on prioritization, conversion likelihood, and strategic recommendations.
                Keep responses brief and focused on business value."""},
                {"role": "user", "content": f"Based on this data:\n{context}\n\nQuestion: {question}"}
            ],
            max_tokens=500,
            temperature=0.7
        )
        
        answer = response.choices[0].message.content
        return jsonify({'success': True, 'answer': answer})
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@leads_bp.route('/leads/<int:lead_id>/delete', methods=['POST'])
@login_required
@role_required(['Admin'])
def delete_lead(lead_id):
    conn = db.get_connection()
    
    lead = conn.execute('SELECT * FROM leads WHERE id = ?', (lead_id,)).fetchone()
    if not lead:
        return jsonify({'success': False, 'error': 'Lead not found'}), 404
    
    if lead['status'] == 'Converted':
        return jsonify({'success': False, 'error': 'Cannot delete converted lead'}), 400
    
    conn.execute('DELETE FROM lead_activities WHERE lead_id = ?', (lead_id,))
    conn.execute('DELETE FROM lead_documents WHERE lead_id = ?', (lead_id,))
    conn.execute('DELETE FROM leads WHERE id = ?', (lead_id,))
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True})
