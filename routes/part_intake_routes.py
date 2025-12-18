from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from flask_login import current_user
from models import Database, AuditLogger
from auth import login_required, role_required
from datetime import datetime
import json
import os
import re

part_intake_bp = Blueprint('part_intake_routes', __name__)

def generate_intake_number(conn):
    """Generate unique intake number"""
    result = conn.execute('''
        SELECT intake_number FROM part_intake_records
        WHERE intake_number LIKE 'INT-%'
        ORDER BY id DESC LIMIT 1
    ''').fetchone()
    
    if result:
        try:
            last_num = int(result['intake_number'].split('-')[1])
            return f"INT-{last_num + 1:06d}"
        except:
            pass
    return "INT-000001"

@part_intake_bp.route('/part-intake')
@login_required
def list_intakes():
    """List all part intake records"""
    db = Database()
    conn = db.get_connection()
    
    status_filter = request.args.get('status', '')
    search = request.args.get('search', '')
    
    query = '''
        SELECT pir.*, u.username as captured_by_name, 
               cu.username as converted_by_name,
               au.username as approved_by_name
        FROM part_intake_records pir
        LEFT JOIN users u ON pir.captured_by = u.id
        LEFT JOIN users cu ON pir.converted_by = cu.id
        LEFT JOIN users au ON pir.approved_by = au.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND pir.status = ?'
        params.append(status_filter)
    
    if search:
        query += ''' AND (pir.intake_number LIKE ? OR pir.supplier_part_number LIKE ? 
                    OR pir.manufacturer_part_number LIKE ? OR pir.short_description LIKE ?
                    OR pir.supplier_name LIKE ? OR pir.oem_name LIKE ?)'''
        search_param = f'%{search}%'
        params.extend([search_param] * 6)
    
    query += ' ORDER BY pir.captured_at DESC'
    
    intakes = conn.execute(query, params).fetchall()
    
    status_counts = conn.execute('''
        SELECT status, COUNT(*) as count FROM part_intake_records GROUP BY status
    ''').fetchall()
    
    conn.close()
    
    return render_template('part_intake/list.html', 
                          intakes=intakes,
                          status_counts={s['status']: s['count'] for s in status_counts},
                          filters={'status': status_filter, 'search': search})

@part_intake_bp.route('/part-intake/capture', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement', 'Planner')
def capture_part():
    """Capture a new part from URL or manual entry"""
    if request.method == 'POST':
        db = Database()
        conn = db.get_connection()
        
        intake_number = generate_intake_number(conn)
        source_type = request.form.get('source_type', 'URL')
        source_url = request.form.get('source_url', '')
        
        conn.execute('''
            INSERT INTO part_intake_records (
                intake_number, source_type, source_url, status,
                supplier_name, supplier_part_number, oem_name, manufacturer_part_number,
                short_description, long_description, category, base_uom, purchase_uom,
                packaging_quantity, technical_attributes, compliance_indicators,
                captured_by, notes
            ) VALUES (?, ?, ?, 'Pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            intake_number,
            source_type,
            source_url,
            request.form.get('supplier_name', ''),
            request.form.get('supplier_part_number', ''),
            request.form.get('oem_name', ''),
            request.form.get('manufacturer_part_number', ''),
            request.form.get('short_description', ''),
            request.form.get('long_description', ''),
            request.form.get('category', ''),
            request.form.get('base_uom', 'EA'),
            request.form.get('purchase_uom', 'EA'),
            float(request.form.get('packaging_quantity', 1) or 1),
            request.form.get('technical_attributes', ''),
            request.form.get('compliance_indicators', ''),
            session.get('user_id'),
            request.form.get('notes', '')
        ))
        
        intake_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        
        conn.execute('''
            INSERT INTO part_intake_audit (intake_id, action_type, action_details, performed_by, ip_address, user_agent)
            VALUES (?, 'Created', 'Part intake record created', ?, ?, ?)
        ''', (intake_id, session.get('user_id'), request.remote_addr, request.headers.get('User-Agent')))
        
        conn.commit()
        conn.close()
        
        flash(f'Part intake {intake_number} created successfully!', 'success')
        return redirect(url_for('part_intake_routes.view_intake', id=intake_id))
    
    return render_template('part_intake/capture.html')

@part_intake_bp.route('/part-intake/<int:id>')
@login_required
def view_intake(id):
    """View a part intake record"""
    db = Database()
    conn = db.get_connection()
    
    intake = conn.execute('''
        SELECT pir.*, u.username as captured_by_name,
               cu.username as converted_by_name,
               au.username as approved_by_name,
               p.code as converted_product_code, p.name as converted_product_name
        FROM part_intake_records pir
        LEFT JOIN users u ON pir.captured_by = u.id
        LEFT JOIN users cu ON pir.converted_by = cu.id
        LEFT JOIN users au ON pir.approved_by = au.id
        LEFT JOIN products p ON pir.converted_product_id = p.id
        WHERE pir.id = ?
    ''', (id,)).fetchone()
    
    if not intake:
        flash('Part intake record not found.', 'danger')
        conn.close()
        return redirect(url_for('part_intake_routes.list_intakes'))
    
    audit_trail = conn.execute('''
        SELECT pia.*, u.username
        FROM part_intake_audit pia
        LEFT JOIN users u ON pia.performed_by = u.id
        WHERE pia.intake_id = ?
        ORDER BY pia.performed_at DESC
    ''', (id,)).fetchall()
    
    matched_products = []
    if intake['matched_product_ids']:
        try:
            product_ids = json.loads(intake['matched_product_ids'])
            if product_ids:
                placeholders = ','.join('?' * len(product_ids))
                matched_products = conn.execute(f'''
                    SELECT * FROM products WHERE id IN ({placeholders})
                ''', product_ids).fetchall()
        except:
            pass
    
    conn.close()
    
    return render_template('part_intake/view.html', 
                          intake=intake, 
                          audit_trail=audit_trail,
                          matched_products=matched_products)

@part_intake_bp.route('/part-intake/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement', 'Planner')
def edit_intake(id):
    """Edit a part intake record"""
    db = Database()
    conn = db.get_connection()
    
    intake = conn.execute('SELECT * FROM part_intake_records WHERE id = ?', (id,)).fetchone()
    
    if not intake:
        flash('Part intake record not found.', 'danger')
        conn.close()
        return redirect(url_for('part_intake_routes.list_intakes'))
    
    if intake['status'] == 'Converted':
        flash('Cannot edit a converted intake record.', 'warning')
        conn.close()
        return redirect(url_for('part_intake_routes.view_intake', id=id))
    
    if request.method == 'POST':
        old_data = dict(intake)
        
        conn.execute('''
            UPDATE part_intake_records SET
                supplier_name = ?, supplier_part_number = ?, oem_name = ?, manufacturer_part_number = ?,
                short_description = ?, long_description = ?, category = ?, base_uom = ?, purchase_uom = ?,
                packaging_quantity = ?, technical_attributes = ?, compliance_indicators = ?, notes = ?
            WHERE id = ?
        ''', (
            request.form.get('supplier_name', ''),
            request.form.get('supplier_part_number', ''),
            request.form.get('oem_name', ''),
            request.form.get('manufacturer_part_number', ''),
            request.form.get('short_description', ''),
            request.form.get('long_description', ''),
            request.form.get('category', ''),
            request.form.get('base_uom', 'EA'),
            request.form.get('purchase_uom', 'EA'),
            float(request.form.get('packaging_quantity', 1) or 1),
            request.form.get('technical_attributes', ''),
            request.form.get('compliance_indicators', ''),
            request.form.get('notes', ''),
            id
        ))
        
        conn.execute('''
            INSERT INTO part_intake_audit (intake_id, action_type, action_details, performed_by, ip_address, user_agent)
            VALUES (?, 'Updated', 'Part intake record updated', ?, ?, ?)
        ''', (id, session.get('user_id'), request.remote_addr, request.headers.get('User-Agent')))
        
        conn.commit()
        conn.close()
        
        flash('Part intake record updated successfully!', 'success')
        return redirect(url_for('part_intake_routes.view_intake', id=id))
    
    conn.close()
    return render_template('part_intake/edit.html', intake=intake)

@part_intake_bp.route('/part-intake/<int:id>/check-duplicates', methods=['POST'])
@role_required('Admin', 'Procurement', 'Planner')
def check_duplicates(id):
    """Check for duplicate products"""
    db = Database()
    conn = db.get_connection()
    
    intake = conn.execute('SELECT * FROM part_intake_records WHERE id = ?', (id,)).fetchone()
    
    if not intake:
        conn.close()
        return jsonify({'error': 'Intake not found'}), 404
    
    matches = []
    match_reasons = []
    
    if intake['manufacturer_part_number']:
        mpn_matches = conn.execute('''
            SELECT id, code, name, 'Exact MPN Match' as match_reason
            FROM products WHERE code = ? OR name LIKE ?
        ''', (intake['manufacturer_part_number'], f'%{intake["manufacturer_part_number"]}%')).fetchall()
        for m in mpn_matches:
            matches.append(dict(m))
    
    xref_matches = conn.execute('''
        SELECT p.id, p.code, p.name, 'Supplier Cross-Reference Match' as match_reason
        FROM part_intake_supplier_xref xref
        JOIN products p ON xref.product_id = p.id
        WHERE xref.manufacturer_part_number = ? OR xref.supplier_part_number = ?
    ''', (intake['manufacturer_part_number'] or '', intake['supplier_part_number'] or '')).fetchall()
    for m in xref_matches:
        matches.append(dict(m))
    
    if intake['short_description']:
        desc_words = intake['short_description'].split()[:3]
        if desc_words:
            pattern = '%' + '%'.join(desc_words) + '%'
            desc_matches = conn.execute('''
                SELECT id, code, name, 'Description Similarity' as match_reason
                FROM products WHERE name LIKE ? OR description LIKE ?
                LIMIT 10
            ''', (pattern, pattern)).fetchall()
            for m in desc_matches:
                if not any(existing['id'] == m['id'] for existing in matches):
                    matches.append(dict(m))
    
    match_type = 'No Match'
    if matches:
        if any(m.get('match_reason') == 'Exact MPN Match' for m in matches):
            match_type = 'Exact Duplicate'
        elif any(m.get('match_reason') == 'Supplier Cross-Reference Match' for m in matches):
            match_type = 'Probable Match'
        else:
            match_type = 'Similar Alternate'
    
    matched_ids = list(set([m['id'] for m in matches]))
    
    conn.execute('''
        UPDATE part_intake_records SET
            duplicate_check_status = 'Checked',
            matched_product_ids = ?,
            match_type = ?
        WHERE id = ?
    ''', (json.dumps(matched_ids), match_type, id))
    
    conn.execute('''
        INSERT INTO part_intake_audit (intake_id, action_type, action_details, performed_by, ip_address)
        VALUES (?, 'Duplicate Check', ?, ?, ?)
    ''', (id, f'Found {len(matches)} potential matches. Match type: {match_type}', 
          session.get('user_id'), request.remote_addr))
    
    conn.commit()
    conn.close()
    
    return jsonify({
        'matches': matches,
        'match_type': match_type,
        'match_count': len(matches)
    })

@part_intake_bp.route('/part-intake/<int:id>/convert', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement', 'Planner')
def convert_to_product(id):
    """Convert part intake to ERP product"""
    db = Database()
    conn = db.get_connection()
    
    intake = conn.execute('SELECT * FROM part_intake_records WHERE id = ?', (id,)).fetchone()
    
    if not intake:
        flash('Part intake record not found.', 'danger')
        conn.close()
        return redirect(url_for('part_intake_routes.list_intakes'))
    
    if intake['status'] == 'Converted':
        flash('This intake has already been converted to a product.', 'warning')
        conn.close()
        return redirect(url_for('part_intake_routes.view_intake', id=id))
    
    if request.method == 'POST':
        action = request.form.get('action', 'create_new')
        
        if action == 'link_existing':
            existing_product_id = int(request.form.get('existing_product_id') or 0)
            
            conn.execute('''
                INSERT INTO part_intake_supplier_xref (
                    product_id, supplier_name, supplier_part_number,
                    manufacturer_name, manufacturer_part_number,
                    source_url, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                existing_product_id,
                intake['supplier_name'],
                intake['supplier_part_number'],
                intake['oem_name'],
                intake['manufacturer_part_number'],
                intake['source_url'],
                session.get('user_id')
            ))
            
            conn.execute('''
                UPDATE part_intake_records SET
                    status = 'Converted',
                    conversion_status = 'Linked to Existing',
                    converted_product_id = ?,
                    converted_by = ?,
                    converted_at = ?
                WHERE id = ?
            ''', (existing_product_id, session.get('user_id'), datetime.now().isoformat(), id))
            
            conn.execute('''
                INSERT INTO part_intake_audit (intake_id, action_type, action_details, performed_by, ip_address)
                VALUES (?, 'Converted', ?, ?, ?)
            ''', (id, f'Linked to existing product ID {existing_product_id}', session.get('user_id'), request.remote_addr))
            
            conn.commit()
            conn.close()
            
            flash('Part linked to existing product successfully!', 'success')
            return redirect(url_for('product_routes.view_product', id=existing_product_id))
        
        else:
            product_code = request.form.get('product_code', intake['manufacturer_part_number'] or intake['supplier_part_number'])
            product_name = request.form.get('product_name', intake['short_description'])
            product_type = request.form.get('product_type', 'Raw Material')
            part_category = request.form.get('part_category', 'Other')
            
            existing = conn.execute('SELECT id FROM products WHERE code = ?', (product_code,)).fetchone()
            if existing:
                flash('A product with this code already exists. Please use a different code.', 'danger')
                conn.close()
                return redirect(url_for('part_intake_routes.convert_to_product', id=id))
            
            conn.execute('''
                INSERT INTO products (
                    code, name, description, unit_of_measure, product_type, part_category,
                    lead_time, product_category, manufacturer, cost
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0.0)
            ''', (
                product_code,
                product_name,
                intake['long_description'] or intake['short_description'],
                intake['base_uom'] or 'EA',
                product_type,
                part_category,
                int(request.form.get('lead_time', 0) or 0),
                intake['category'],
                intake['oem_name']
            ))
            
            product_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
            
            conn.execute('''
                INSERT INTO inventory (product_id, quantity, reorder_point, safety_stock)
                VALUES (?, 0, ?, ?)
            ''', (product_id, float(request.form.get('reorder_point', 0) or 0), float(request.form.get('safety_stock', 0) or 0)))
            
            if intake['supplier_name'] or intake['supplier_part_number']:
                conn.execute('''
                    INSERT INTO part_intake_supplier_xref (
                        product_id, supplier_name, supplier_part_number,
                        manufacturer_name, manufacturer_part_number,
                        source_url, is_primary, created_by
                    ) VALUES (?, ?, ?, ?, ?, ?, 1, ?)
                ''', (
                    product_id,
                    intake['supplier_name'],
                    intake['supplier_part_number'],
                    intake['oem_name'],
                    intake['manufacturer_part_number'],
                    intake['source_url'],
                    session.get('user_id')
                ))
            
            conn.execute('''
                UPDATE part_intake_records SET
                    status = 'Converted',
                    conversion_status = 'Created New Product',
                    converted_product_id = ?,
                    converted_by = ?,
                    converted_at = ?
                WHERE id = ?
            ''', (product_id, session.get('user_id'), datetime.now().isoformat(), id))
            
            conn.execute('''
                INSERT INTO part_intake_audit (intake_id, action_type, action_details, raw_data_snapshot, performed_by, ip_address)
                VALUES (?, 'Converted', ?, ?, ?, ?)
            ''', (id, f'Created new product with ID {product_id}', json.dumps(dict(intake)), session.get('user_id'), request.remote_addr))
            
            AuditLogger.log_change(
                conn=conn,
                record_type='product',
                record_id=product_id,
                action_type='Created',
                modified_by=session.get('user_id'),
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
            
            conn.commit()
            conn.close()
            
            flash(f'Product {product_code} created successfully from intake!', 'success')
            return redirect(url_for('product_routes.view_product', id=product_id))
    
    matched_products = []
    if intake['matched_product_ids']:
        try:
            product_ids = json.loads(intake['matched_product_ids'])
            if product_ids:
                placeholders = ','.join('?' * len(product_ids))
                matched_products = conn.execute(f'''
                    SELECT * FROM products WHERE id IN ({placeholders})
                ''', product_ids).fetchall()
        except:
            pass
    
    next_code = intake['manufacturer_part_number'] or intake['supplier_part_number'] or ''
    if not next_code:
        last_product = conn.execute('''
            SELECT code FROM products WHERE code LIKE 'PART-%'
            ORDER BY id DESC LIMIT 1
        ''').fetchone()
        if last_product:
            try:
                num = int(last_product['code'].split('-')[1]) + 1
                next_code = f'PART-{num:06d}'
            except:
                next_code = 'PART-000001'
        else:
            next_code = 'PART-000001'
    
    conn.close()
    
    return render_template('part_intake/convert.html', 
                          intake=intake,
                          matched_products=matched_products,
                          suggested_code=next_code)

@part_intake_bp.route('/part-intake/<int:id>/extract-ai', methods=['POST'])
@role_required('Admin', 'Procurement', 'Planner')
def extract_with_ai(id):
    """Use AI to extract part data from URL or content"""
    db = Database()
    conn = db.get_connection()
    
    intake = conn.execute('SELECT * FROM part_intake_records WHERE id = ?', (id,)).fetchone()
    
    if not intake:
        conn.close()
        return jsonify({'error': 'Intake not found'}), 404
    
    try:
        from openai import OpenAI
        client = OpenAI(
            api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
            base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL'),
        )
        
        source_content = intake['raw_content'] or ''
        existing_desc = intake['short_description'] or ''
        existing_long_desc = intake['long_description'] or ''
        
        all_content = f"""
Source URL: {intake['source_url'] or 'Not provided'}
Raw Content: {source_content}
Existing Short Description: {existing_desc}
Existing Long Description: {existing_long_desc}
Existing Supplier: {intake['supplier_name'] or 'Unknown'}
Existing Supplier Part #: {intake['supplier_part_number'] or 'Unknown'}
Existing OEM: {intake['oem_name'] or 'Unknown'}
Existing MPN: {intake['manufacturer_part_number'] or 'Unknown'}
"""
        
        prompt = f"""Analyze the following supplier/manufacturer part information and extract structured data.

{all_content}

Extract and return a JSON object with the following fields (include confidence score 0-100 for each):
- supplier_name: The supplier/distributor name
- supplier_part_number: The supplier's part number
- oem_name: The original manufacturer name
- manufacturer_part_number: The manufacturer's part number (MPN)
- short_description: A concise product description (max 100 chars)
- long_description: Detailed product description
- category: Product category/commodity
- base_uom: Base unit of measure (EA, KG, M, etc.)
- purchase_uom: Purchase unit of measure
- packaging_quantity: Quantity per package
- technical_attributes: JSON object with specs (dimensions, material, ratings, etc.)
- compliance_indicators: Any compliance info (RoHS, certifications, etc.)

Return ONLY valid JSON, no markdown formatting."""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an expert at extracting and enriching structured part/product data. Based on the provided information (which may be limited), infer and extract as much relevant data as possible. For part numbers, try to identify patterns. For category, infer from description. Return only valid JSON with your best guesses and confidence scores."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2
        )
        
        result_text = (response.choices[0].message.content or '').strip()
        if result_text.startswith('```'):
            result_text = re.sub(r'^```(?:json)?\n?', '', result_text)
            result_text = re.sub(r'\n?```$', '', result_text)
        
        extracted_data = json.loads(result_text)
        
        confidence_scores = {}
        for key in extracted_data:
            if isinstance(extracted_data[key], dict) and 'confidence' in extracted_data[key]:
                confidence_scores[key] = extracted_data[key]['confidence']
                extracted_data[key] = extracted_data[key].get('value', extracted_data[key])
            else:
                confidence_scores[key] = 75
        
        update_fields = []
        update_values = []
        
        field_mapping = {
            'supplier_name': 'supplier_name',
            'supplier_part_number': 'supplier_part_number',
            'oem_name': 'oem_name',
            'manufacturer_part_number': 'manufacturer_part_number',
            'short_description': 'short_description',
            'long_description': 'long_description',
            'category': 'category',
            'base_uom': 'base_uom',
            'purchase_uom': 'purchase_uom',
            'packaging_quantity': 'packaging_quantity'
        }
        
        for json_key, db_field in field_mapping.items():
            if json_key in extracted_data and extracted_data[json_key]:
                update_fields.append(f'{db_field} = ?')
                value = extracted_data[json_key]
                if json_key == 'packaging_quantity':
                    try:
                        value = float(value)
                    except:
                        value = 1.0
                update_values.append(value)
        
        if 'technical_attributes' in extracted_data:
            update_fields.append('technical_attributes = ?')
            update_values.append(json.dumps(extracted_data['technical_attributes']) if isinstance(extracted_data['technical_attributes'], dict) else str(extracted_data['technical_attributes']))
        
        if 'compliance_indicators' in extracted_data:
            update_fields.append('compliance_indicators = ?')
            update_values.append(str(extracted_data['compliance_indicators']))
        
        update_fields.append('confidence_scores = ?')
        update_values.append(json.dumps(confidence_scores))
        
        if update_fields:
            update_values.append(id)
            conn.execute(f'''
                UPDATE part_intake_records SET {', '.join(update_fields)} WHERE id = ?
            ''', update_values)
        
        conn.execute('''
            INSERT INTO part_intake_audit (intake_id, action_type, action_details, performed_by, ip_address)
            VALUES (?, 'AI Extraction', 'AI extracted part data from source', ?, ?)
        ''', (id, session.get('user_id'), request.remote_addr))
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'extracted_data': extracted_data,
            'confidence_scores': confidence_scores
        })
        
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500

@part_intake_bp.route('/part-intake/<int:id>/reject', methods=['POST'])
@role_required('Admin', 'Procurement')
def reject_intake(id):
    """Reject a part intake record"""
    db = Database()
    conn = db.get_connection()
    
    reason = request.form.get('rejection_reason', 'No reason provided')
    
    conn.execute('''
        UPDATE part_intake_records SET
            status = 'Rejected',
            rejection_reason = ?
        WHERE id = ?
    ''', (reason, id))
    
    conn.execute('''
        INSERT INTO part_intake_audit (intake_id, action_type, action_details, performed_by, ip_address)
        VALUES (?, 'Rejected', ?, ?, ?)
    ''', (id, f'Rejected: {reason}', session.get('user_id'), request.remote_addr))
    
    conn.commit()
    conn.close()
    
    flash('Part intake record rejected.', 'info')
    return redirect(url_for('part_intake_routes.list_intakes'))

@part_intake_bp.route('/api/part-intake/quick-capture', methods=['POST'])
@login_required
def quick_capture():
    """Quick capture API for browser extension or quick entry"""
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    db = Database()
    conn = db.get_connection()
    
    intake_number = generate_intake_number(conn)
    
    conn.execute('''
        INSERT INTO part_intake_records (
            intake_number, source_type, source_url, raw_content, status,
            supplier_name, supplier_part_number, oem_name, manufacturer_part_number,
            short_description, captured_by
        ) VALUES (?, ?, ?, ?, 'Pending', ?, ?, ?, ?, ?, ?)
    ''', (
        intake_number,
        data.get('source_type', 'API'),
        data.get('source_url', ''),
        data.get('raw_content', ''),
        data.get('supplier_name', ''),
        data.get('supplier_part_number', ''),
        data.get('oem_name', ''),
        data.get('manufacturer_part_number', ''),
        data.get('short_description', ''),
        session.get('user_id')
    ))
    
    intake_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    
    conn.commit()
    conn.close()
    
    return jsonify({
        'success': True,
        'intake_id': intake_id,
        'intake_number': intake_number
    })
