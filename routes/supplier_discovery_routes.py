from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session
from models import Database, AuditLogger
from auth import login_required, role_required
from datetime import datetime
import json

supplier_discovery_bp = Blueprint('supplier_discovery_routes', __name__)

def generate_request_number(conn):
    """Generate sequential request number for supplier discovery"""
    last = conn.execute('''
        SELECT request_number FROM supplier_discovery_requests
        WHERE request_number LIKE 'SDR-%'
        ORDER BY CAST(SUBSTR(request_number, 5) AS INTEGER) DESC
        LIMIT 1
    ''').fetchone()
    
    if last:
        try:
            last_num = int(last['request_number'].split('-')[1])
            return f'SDR-{last_num + 1:05d}'
        except (ValueError, IndexError):
            return 'SDR-00001'
    return 'SDR-00001'


@supplier_discovery_bp.route('/supplier-discovery')
@login_required
def list_requests():
    """List all supplier discovery requests"""
    db = Database()
    conn = db.get_connection()
    
    status_filter = request.args.get('status', '')
    
    query = '''
        SELECT sdr.*, p.code as product_code, p.name as product_name,
               u.username as created_by_name,
               (SELECT COUNT(*) FROM discovered_suppliers WHERE request_id = sdr.id) as supplier_count
        FROM supplier_discovery_requests sdr
        LEFT JOIN products p ON sdr.product_id = p.id
        LEFT JOIN users u ON sdr.created_by = u.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND sdr.status = ?'
        params.append(status_filter)
    
    query += ' ORDER BY sdr.created_at DESC'
    
    requests = conn.execute(query, params).fetchall()
    conn.close()
    
    return render_template('supplier_discovery/list.html', 
                          requests=requests,
                          status_filter=status_filter)


@supplier_discovery_bp.route('/supplier-discovery/create', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement', 'Planner')
def create_request():
    """Create a new supplier discovery request"""
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            request_number = generate_request_number(conn)
            product_id = request.form.get('product_id') or None
            part_number = request.form.get('part_number', '').strip()
            description = request.form.get('description', '').strip()
            specifications = request.form.get('specifications', '').strip()
            quantity = float(request.form.get('quantity', 0)) if request.form.get('quantity') else None
            uom = request.form.get('uom', '').strip()
            need_by_date = request.form.get('need_by_date') or None
            urgency = request.form.get('urgency', 'Normal')
            plant_location = request.form.get('plant_location', '').strip()
            industry = request.form.get('industry', '').strip()
            preferred_regions = request.form.get('preferred_regions', '').strip()
            
            if not part_number:
                flash('Part number is required', 'danger')
                conn.close()
                return redirect(url_for('supplier_discovery_routes.create_request'))
            
            conn.execute('''
                INSERT INTO supplier_discovery_requests (
                    request_number, product_id, part_number, description, specifications,
                    quantity, uom, need_by_date, urgency, plant_location, industry,
                    preferred_regions, status, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Pending', ?)
            ''', (request_number, product_id, part_number, description, specifications,
                  quantity, uom, need_by_date, urgency, plant_location, industry,
                  preferred_regions, session.get('user_id')))
            
            request_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
            
            AuditLogger.log_change(
                conn=conn,
                record_type='supplier_discovery_request',
                record_id=request_id,
                action_type='Created',
                modified_by=session.get('user_id'),
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
            
            conn.commit()
            conn.close()
            
            flash(f'Supplier discovery request {request_number} created successfully!', 'success')
            return redirect(url_for('supplier_discovery_routes.view_request', id=request_id))
            
        except Exception as e:
            conn.close()
            flash(f'Error creating request: {str(e)}', 'danger')
            return redirect(url_for('supplier_discovery_routes.create_request'))
    
    products = conn.execute('SELECT id, code, name FROM products ORDER BY code').fetchall()
    conn.close()
    
    return render_template('supplier_discovery/create.html', products=products)


@supplier_discovery_bp.route('/supplier-discovery/<int:id>')
@login_required
def view_request(id):
    """View a supplier discovery request and its results"""
    db = Database()
    conn = db.get_connection()
    
    req = conn.execute('''
        SELECT sdr.*, p.code as product_code, p.name as product_name,
               u.username as created_by_name
        FROM supplier_discovery_requests sdr
        LEFT JOIN products p ON sdr.product_id = p.id
        LEFT JOIN users u ON sdr.created_by = u.id
        WHERE sdr.id = ?
    ''', (id,)).fetchone()
    
    if not req:
        conn.close()
        flash('Request not found', 'danger')
        return redirect(url_for('supplier_discovery_routes.list_requests'))
    
    suppliers = conn.execute('''
        SELECT ds.*, u.username as approved_by_name
        FROM discovered_suppliers ds
        LEFT JOIN users u ON ds.approved_by = u.id
        WHERE ds.request_id = ?
        ORDER BY ds.confidence_score DESC
    ''', (id,)).fetchall()
    
    conn.close()
    
    return render_template('supplier_discovery/view.html', 
                          request=req, 
                          suppliers=suppliers)


@supplier_discovery_bp.route('/supplier-discovery/<int:id>/run', methods=['POST'])
@role_required('Admin', 'Procurement', 'Planner')
def run_discovery(id):
    """Run AI-powered supplier discovery"""
    db = Database()
    conn = db.get_connection()
    
    req = conn.execute('SELECT * FROM supplier_discovery_requests WHERE id = ?', (id,)).fetchone()
    
    if not req:
        conn.close()
        return jsonify({'error': 'Request not found'}), 404
    
    try:
        conn.execute('''
            UPDATE supplier_discovery_requests 
            SET status = 'Processing' 
            WHERE id = ?
        ''', (id,))
        conn.commit()
        
        import os
        from openai import OpenAI
        client = OpenAI(
            api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
            base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
        )
        
        material_context = {
            'part_number': req['part_number'],
            'description': req['description'] or '',
            'specifications': req['specifications'] or '',
            'quantity': req['quantity'],
            'uom': req['uom'] or '',
            'need_by_date': req['need_by_date'] or '',
            'urgency': req['urgency'] or 'Normal',
            'industry': req['industry'] or 'Aerospace/MRO',
            'preferred_regions': req['preferred_regions'] or ''
        }
        
        prompt = f"""You are an expert procurement analyst specializing in supplier discovery for manufacturing and MRO (Maintenance, Repair, Operations) industries.

Given the following material requirement, identify potential suppliers who could provide this part or similar products:

Material Details:
- Part Number: {material_context['part_number']}
- Description: {material_context['description']}
- Specifications: {material_context['specifications']}
- Quantity Needed: {material_context['quantity']} {material_context['uom']}
- Need By Date: {material_context['need_by_date']}
- Urgency: {material_context['urgency']}
- Industry: {material_context['industry']}
- Preferred Regions: {material_context['preferred_regions']}

Based on your knowledge, provide 5-8 potential suppliers that could supply this material. For each supplier, provide:
1. Supplier name
2. Website URL (if known, otherwise leave empty)
3. Material match description (how well they match the requirement)
4. Known certifications (AS9100, ISO 9001, NADCAP, etc.)
5. Region/Country
6. Estimated lead time (if typical for this type of supplier)
7. Confidence score (0-100) based on likelihood they can supply this
8. Additional notes

IMPORTANT: All suppliers must be marked as "Unapproved" since they require human verification.

Respond ONLY with a valid JSON array of supplier objects with these exact keys:
[
  {{
    "supplier_name": "Company Name",
    "website": "https://example.com",
    "material_match": "Description of how they match the requirement",
    "certifications": "AS9100, ISO 9001",
    "region": "USA",
    "estimated_lead_time": "4-6 weeks",
    "confidence_score": 85,
    "notes": "Additional relevant information"
  }}
]

Return ONLY the JSON array, no other text."""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a supplier discovery assistant. Always respond with valid JSON only."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=2000
        )
        
        ai_response = (response.choices[0].message.content or '').strip()
        
        if ai_response.startswith('```json'):
            ai_response = ai_response[7:]
        if ai_response.startswith('```'):
            ai_response = ai_response[3:]
        if ai_response.endswith('```'):
            ai_response = ai_response[:-3]
        ai_response = ai_response.strip()
        
        suppliers_data = json.loads(ai_response)
        
        conn.execute('DELETE FROM discovered_suppliers WHERE request_id = ?', (id,))
        
        for supplier in suppliers_data:
            conn.execute('''
                INSERT INTO discovered_suppliers (
                    request_id, supplier_name, website, material_match,
                    certifications, region, estimated_lead_time,
                    confidence_score, notes, approval_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'Unapproved')
            ''', (
                id,
                supplier.get('supplier_name', 'Unknown'),
                supplier.get('website', ''),
                supplier.get('material_match', ''),
                supplier.get('certifications', ''),
                supplier.get('region', ''),
                supplier.get('estimated_lead_time', ''),
                supplier.get('confidence_score', 0),
                supplier.get('notes', '')
            ))
        
        conn.execute('''
            UPDATE supplier_discovery_requests 
            SET status = 'Completed', 
                completed_at = ?,
                ai_search_queries = ?
            WHERE id = ?
        ''', (datetime.now().isoformat(), prompt[:500], id))
        
        AuditLogger.log_change(
            conn=conn,
            record_type='supplier_discovery_request',
            record_id=id,
            action_type='AI Discovery Completed',
            modified_by=session.get('user_id'),
            changed_fields={'suppliers_found': len(suppliers_data)},
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Found {len(suppliers_data)} potential suppliers',
            'supplier_count': len(suppliers_data)
        })
        
    except json.JSONDecodeError as e:
        conn.execute('''
            UPDATE supplier_discovery_requests 
            SET status = 'Failed' 
            WHERE id = ?
        ''', (id,))
        conn.commit()
        conn.close()
        return jsonify({'error': f'Failed to parse AI response: {str(e)}'}), 500
        
    except Exception as e:
        conn.execute('''
            UPDATE supplier_discovery_requests 
            SET status = 'Failed' 
            WHERE id = ?
        ''', (id,))
        conn.commit()
        conn.close()
        return jsonify({'error': str(e)}), 500


def generate_supplier_code(conn):
    """Generate sequential supplier code"""
    last = conn.execute('''
        SELECT code FROM suppliers
        WHERE code LIKE 'SUP-%'
        ORDER BY CAST(SUBSTR(code, 5) AS INTEGER) DESC
        LIMIT 1
    ''').fetchone()
    
    if last:
        try:
            last_num = int(last['code'].split('-')[1])
            return f'SUP-{last_num + 1:05d}'
        except (ValueError, IndexError):
            return 'SUP-00001'
    return 'SUP-00001'


@supplier_discovery_bp.route('/supplier-discovery/supplier/<int:id>/approve', methods=['POST'])
@role_required('Admin', 'Procurement')
def approve_supplier(id):
    """Approve a discovered supplier and automatically create supplier record"""
    db = Database()
    conn = db.get_connection()
    
    supplier = conn.execute('SELECT * FROM discovered_suppliers WHERE id = ?', (id,)).fetchone()
    
    if not supplier:
        conn.close()
        return jsonify({'error': 'Supplier not found'}), 404
    
    try:
        supplier_code = generate_supplier_code(conn)
        supplier_name = supplier['supplier_name']
        
        notes_parts = []
        if supplier['certifications']:
            notes_parts.append(f"Certifications: {supplier['certifications']}")
        if supplier['region']:
            notes_parts.append(f"Region: {supplier['region']}")
        if supplier['estimated_lead_time']:
            notes_parts.append(f"Lead Time: {supplier['estimated_lead_time']}")
        if supplier['material_match']:
            notes_parts.append(f"Specialty: {supplier['material_match']}")
        if supplier['notes']:
            notes_parts.append(supplier['notes'])
        
        address = f"{supplier['region'] or ''}"
        if supplier['website']:
            address = f"{address} | Website: {supplier['website']}" if address else f"Website: {supplier['website']}"
        
        conn.execute('''
            INSERT INTO suppliers (code, name, contact_person, email, phone, address)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            supplier_code,
            supplier_name,
            '',
            '',
            '',
            address.strip()
        ))
        
        new_supplier_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        
        conn.execute('''
            UPDATE discovered_suppliers
            SET approval_status = 'Approved',
                approved_by = ?,
                approved_at = ?,
                notes = CASE WHEN notes IS NULL OR notes = '' 
                        THEN ? 
                        ELSE notes || ' | Created as ' || ? END
            WHERE id = ?
        ''', (session.get('user_id'), datetime.now().isoformat(), 
              f'Created as {supplier_code}', supplier_code, id))
        
        AuditLogger.log_change(
            conn=conn,
            record_type='discovered_supplier',
            record_id=id,
            action_type='Approved',
            modified_by=session.get('user_id'),
            changed_fields={'approval_status': {'old': 'Unapproved', 'new': 'Approved'}, 
                          'created_supplier_code': supplier_code},
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        AuditLogger.log_change(
            conn=conn,
            record_type='supplier',
            record_id=new_supplier_id,
            action_type='Created',
            modified_by=session.get('user_id'),
            changed_fields={'source': 'AI Supplier Discovery', 'discovered_supplier_id': id},
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Supplier approved and created as {supplier_code}',
            'supplier_code': supplier_code,
            'supplier_id': new_supplier_id
        })
        
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500


@supplier_discovery_bp.route('/supplier-discovery/supplier/<int:id>/reject', methods=['POST'])
@role_required('Admin', 'Procurement')
def reject_supplier(id):
    """Reject a discovered supplier"""
    db = Database()
    conn = db.get_connection()
    
    supplier = conn.execute('SELECT * FROM discovered_suppliers WHERE id = ?', (id,)).fetchone()
    
    if not supplier:
        conn.close()
        return jsonify({'error': 'Supplier not found'}), 404
    
    try:
        data = request.get_json() or {}
        rejection_reason = data.get('reason', '')
        
        conn.execute('''
            UPDATE discovered_suppliers
            SET approval_status = 'Rejected',
                notes = CASE WHEN notes IS NULL OR notes = '' 
                        THEN ? 
                        ELSE notes || ' | Rejected: ' || ? END
            WHERE id = ?
        ''', (f'Rejected: {rejection_reason}', rejection_reason, id))
        
        AuditLogger.log_change(
            conn=conn,
            record_type='discovered_supplier',
            record_id=id,
            action_type='Rejected',
            modified_by=session.get('user_id'),
            changed_fields={'approval_status': {'old': 'Unapproved', 'new': 'Rejected'}, 'reason': rejection_reason},
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Supplier rejected'})
        
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500


@supplier_discovery_bp.route('/supplier-discovery/<int:id>/delete', methods=['POST'])
@role_required('Admin')
def delete_request(id):
    """Delete a supplier discovery request"""
    db = Database()
    conn = db.get_connection()
    
    req = conn.execute('SELECT * FROM supplier_discovery_requests WHERE id = ?', (id,)).fetchone()
    
    if not req:
        conn.close()
        flash('Request not found', 'danger')
        return redirect(url_for('supplier_discovery_routes.list_requests'))
    
    try:
        AuditLogger.log_change(
            conn=conn,
            record_type='supplier_discovery_request',
            record_id=id,
            action_type='Deleted',
            modified_by=session.get('user_id'),
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.execute('DELETE FROM supplier_discovery_requests WHERE id = ?', (id,))
        conn.commit()
        conn.close()
        
        flash('Request deleted successfully', 'success')
        
    except Exception as e:
        conn.close()
        flash(f'Error deleting request: {str(e)}', 'danger')
    
    return redirect(url_for('supplier_discovery_routes.list_requests'))


@supplier_discovery_bp.route('/api/supplier-discovery/from-material', methods=['POST'])
@role_required('Admin', 'Procurement', 'Planner')
def create_from_material():
    """API endpoint to create supplier discovery request from material requirement"""
    db = Database()
    conn = db.get_connection()
    
    try:
        data = request.get_json()
        
        request_number = generate_request_number(conn)
        
        conn.execute('''
            INSERT INTO supplier_discovery_requests (
                request_number, product_id, part_number, description, specifications,
                quantity, uom, need_by_date, urgency, plant_location, industry,
                preferred_regions, status, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Pending', ?)
        ''', (
            request_number,
            data.get('product_id'),
            data.get('part_number', ''),
            data.get('description', ''),
            json.dumps(data.get('specifications', {})) if isinstance(data.get('specifications'), dict) else data.get('specifications', ''),
            data.get('quantity'),
            data.get('uom', ''),
            data.get('need_by_date'),
            data.get('urgency', 'Normal'),
            data.get('plant_location', ''),
            data.get('industry', 'Aerospace/MRO'),
            data.get('preferred_regions', '')
        , session.get('user_id')))
        
        request_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'request_id': request_id,
            'request_number': request_number,
            'redirect_url': url_for('supplier_discovery_routes.view_request', id=request_id)
        })
        
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500


@supplier_discovery_bp.route('/api/supplier-discovery/<int:id>/suppliers')
@login_required
def get_suppliers_json(id):
    """API endpoint to get suppliers for a discovery request as JSON"""
    db = Database()
    conn = db.get_connection()
    
    suppliers = conn.execute('''
        SELECT ds.*, u.username as approved_by_name
        FROM discovered_suppliers ds
        LEFT JOIN users u ON ds.approved_by = u.id
        WHERE ds.request_id = ?
        ORDER BY ds.confidence_score DESC
    ''', (id,)).fetchall()
    
    conn.close()
    
    suppliers_list = []
    for s in suppliers:
        suppliers_list.append({
            'id': s['id'],
            'supplier_name': s['supplier_name'],
            'website': s['website'],
            'material_match': s['material_match'],
            'certifications': s['certifications'],
            'region': s['region'],
            'estimated_lead_time': s['estimated_lead_time'],
            'confidence_score': s['confidence_score'],
            'notes': s['notes'],
            'approval_status': s['approval_status'],
            'approved_by_name': s['approved_by_name']
        })
    
    return jsonify({'suppliers': suppliers_list})
