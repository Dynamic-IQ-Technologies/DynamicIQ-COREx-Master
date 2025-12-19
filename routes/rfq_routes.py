from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database, AuditLogger
from auth import login_required, role_required
from datetime import datetime, timedelta
import secrets
import os

rfq_bp = Blueprint('rfq_routes', __name__)

def generate_rfq_number(conn):
    """Generate sequential RFQ number"""
    result = conn.execute('''
        SELECT rfq_number FROM rfqs 
        WHERE rfq_number LIKE 'RFQ-%' 
        ORDER BY id DESC LIMIT 1
    ''').fetchone()
    
    if result:
        try:
            last_num = int(result['rfq_number'].split('-')[1])
            return f"RFQ-{last_num + 1:05d}"
        except:
            pass
    return "RFQ-00001"

@rfq_bp.route('/rfqs')
@login_required
def list_rfqs():
    db = Database()
    conn = db.get_connection()
    
    rfqs = conn.execute('''
        SELECT r.*, 
               u.username as created_by_name,
               COUNT(DISTINCT rl.id) as line_count,
               COUNT(DISTINCT rs.id) as supplier_count
        FROM rfqs r
        LEFT JOIN users u ON r.created_by = u.id
        LEFT JOIN rfq_lines rl ON r.id = rl.rfq_id
        LEFT JOIN rfq_suppliers rs ON r.id = rs.rfq_id
        GROUP BY r.id
        ORDER BY r.created_at DESC
    ''').fetchall()
    
    stats = {
        'total': len(rfqs),
        'draft': sum(1 for r in rfqs if r['status'] == 'Draft'),
        'issued': sum(1 for r in rfqs if r['status'] == 'Issued'),
        'received': sum(1 for r in rfqs if r['status'] == 'Quotes Received'),
        'closed': sum(1 for r in rfqs if r['status'] == 'Closed')
    }
    
    conn.close()
    from datetime import date
    return render_template('rfqs/list.html', rfqs=rfqs, stats=stats, now=date.today().isoformat())

@rfq_bp.route('/rfqs/create', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement')
def create_rfq():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        rfq_number = generate_rfq_number(conn)
        
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO rfqs (rfq_number, title, description, status, issue_date, due_date,
                            currency, terms_conditions, notes, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            rfq_number,
            request.form['title'],
            request.form.get('description', ''),
            'Draft',
            request.form.get('issue_date') or None,
            request.form.get('due_date') or None,
            request.form.get('currency', 'USD'),
            request.form.get('terms_conditions', ''),
            request.form.get('notes', ''),
            session.get('user_id')
        ))
        
        rfq_id = cursor.lastrowid
        conn.commit()
        
        AuditLogger.log_change(conn, 'rfqs', rfq_id, 'CREATE', session.get('user_id'),
                              {'rfq_number': rfq_number, 'title': request.form['title']})
        conn.commit()
        
        flash(f'RFQ {rfq_number} created successfully!', 'success')
        conn.close()
        return redirect(url_for('rfq_routes.edit_rfq', rfq_id=rfq_id))
    
    conn.close()
    return render_template('rfqs/create.html')

@rfq_bp.route('/rfqs/<int:rfq_id>')
@login_required
def view_rfq(rfq_id):
    db = Database()
    conn = db.get_connection()
    
    rfq = conn.execute('''
        SELECT r.*, u.username as created_by_name
        FROM rfqs r
        LEFT JOIN users u ON r.created_by = u.id
        WHERE r.id = ?
    ''', (rfq_id,)).fetchone()
    
    if not rfq:
        flash('RFQ not found', 'danger')
        conn.close()
        return redirect(url_for('rfq_routes.list_rfqs'))
    
    lines = conn.execute('''
        SELECT rl.*, p.code as part_number, p.name as product_desc, u.uom_code
        FROM rfq_lines rl
        LEFT JOIN products p ON rl.product_id = p.id
        LEFT JOIN uom_master u ON rl.uom_id = u.id
        WHERE rl.rfq_id = ?
        ORDER BY rl.line_number
    ''', (rfq_id,)).fetchall()
    
    suppliers = conn.execute('''
        SELECT rs.*, s.name as supplier_name, s.code as supplier_code
        FROM rfq_suppliers rs
        JOIN suppliers s ON rs.supplier_id = s.id
        WHERE rs.rfq_id = ?
    ''', (rfq_id,)).fetchall()
    
    quotes = conn.execute('''
        SELECT rq.*, s.name as supplier_name, rl.description as line_desc
        FROM rfq_quotes rq
        JOIN suppliers s ON rq.supplier_id = s.id
        JOIN rfq_lines rl ON rq.rfq_line_id = rl.id
        WHERE rq.rfq_id = ?
        ORDER BY rq.rfq_line_id, rq.quoted_price
    ''', (rfq_id,)).fetchall()
    
    conn.close()
    return render_template('rfqs/view.html', rfq=rfq, lines=lines, suppliers=suppliers, quotes=quotes)

@rfq_bp.route('/rfqs/<int:rfq_id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement')
def edit_rfq(rfq_id):
    db = Database()
    conn = db.get_connection()
    
    rfq = conn.execute('SELECT * FROM rfqs WHERE id = ?', (rfq_id,)).fetchone()
    if not rfq:
        flash('RFQ not found', 'danger')
        conn.close()
        return redirect(url_for('rfq_routes.list_rfqs'))
    
    if request.method == 'POST':
        conn.execute('''
            UPDATE rfqs SET title = ?, description = ?, issue_date = ?, due_date = ?,
                          currency = ?, terms_conditions = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (
            request.form['title'],
            request.form.get('description', ''),
            request.form.get('issue_date') or None,
            request.form.get('due_date') or None,
            request.form.get('currency', 'USD'),
            request.form.get('terms_conditions', ''),
            request.form.get('notes', ''),
            rfq_id
        ))
        conn.commit()
        
        flash('RFQ updated successfully!', 'success')
        conn.close()
        return redirect(url_for('rfq_routes.view_rfq', rfq_id=rfq_id))
    
    lines = conn.execute('''
        SELECT rl.*, p.code as part_number, p.name as product_desc, u.uom_code
        FROM rfq_lines rl
        LEFT JOIN products p ON rl.product_id = p.id
        LEFT JOIN uom_master u ON rl.uom_id = u.id
        WHERE rl.rfq_id = ?
        ORDER BY rl.line_number
    ''', (rfq_id,)).fetchall()
    
    suppliers = conn.execute('''
        SELECT rs.*, s.name as supplier_name, s.code as supplier_code
        FROM rfq_suppliers rs
        JOIN suppliers s ON rs.supplier_id = s.id
        WHERE rs.rfq_id = ?
    ''', (rfq_id,)).fetchall()
    
    products = conn.execute('SELECT id, code as part_number, name as description FROM products ORDER BY code').fetchall()
    uoms = conn.execute('SELECT id, uom_code, uom_name FROM uom_master WHERE is_active = 1 ORDER BY uom_code').fetchall()
    all_suppliers = conn.execute('SELECT id, code, name FROM suppliers ORDER BY name').fetchall()
    
    conn.close()
    return render_template('rfqs/edit.html', rfq=rfq, lines=lines, suppliers=suppliers,
                          products=products, uoms=uoms, all_suppliers=all_suppliers)

@rfq_bp.route('/rfqs/<int:rfq_id>/add_line', methods=['POST'])
@role_required('Admin', 'Procurement')
def add_rfq_line(rfq_id):
    db = Database()
    conn = db.get_connection()
    
    last_line = conn.execute('SELECT MAX(line_number) as max_ln FROM rfq_lines WHERE rfq_id = ?', (rfq_id,)).fetchone()
    line_number = (last_line['max_ln'] or 0) + 1
    
    conn.execute('''
        INSERT INTO rfq_lines (rfq_id, line_number, product_id, description, quantity, uom_id, target_price, required_date, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        rfq_id,
        line_number,
        request.form.get('product_id') or None,
        request.form['description'],
        float(request.form.get('quantity', 1)),
        request.form.get('uom_id') or None,
        float(request.form.get('target_price')) if request.form.get('target_price') else None,
        request.form.get('required_date') or None,
        request.form.get('notes', '')
    ))
    conn.commit()
    
    flash('Line added successfully!', 'success')
    conn.close()
    return redirect(url_for('rfq_routes.edit_rfq', rfq_id=rfq_id))

@rfq_bp.route('/rfqs/<int:rfq_id>/remove_line/<int:line_id>', methods=['POST'])
@role_required('Admin', 'Procurement')
def remove_rfq_line(rfq_id, line_id):
    db = Database()
    conn = db.get_connection()
    conn.execute('DELETE FROM rfq_lines WHERE id = ? AND rfq_id = ?', (line_id, rfq_id))
    conn.commit()
    flash('Line removed successfully!', 'success')
    conn.close()
    return redirect(url_for('rfq_routes.edit_rfq', rfq_id=rfq_id))

@rfq_bp.route('/rfqs/<int:rfq_id>/update_line/<int:line_id>', methods=['POST'])
@role_required('Admin', 'Procurement')
def update_rfq_line(rfq_id, line_id):
    db = Database()
    conn = db.get_connection()
    
    conn.execute('''
        UPDATE rfq_lines 
        SET product_id = ?, description = ?, quantity = ?, uom_id = ?, 
            target_price = ?, required_date = ?, notes = ?
        WHERE id = ? AND rfq_id = ?
    ''', (
        request.form.get('product_id') or None,
        request.form['description'],
        float(request.form.get('quantity', 1)),
        request.form.get('uom_id') or None,
        float(request.form.get('target_price')) if request.form.get('target_price') else None,
        request.form.get('required_date') or None,
        request.form.get('notes', ''),
        line_id,
        rfq_id
    ))
    conn.commit()
    
    flash('Line updated successfully!', 'success')
    conn.close()
    return redirect(url_for('rfq_routes.edit_rfq', rfq_id=rfq_id))

@rfq_bp.route('/rfqs/<int:rfq_id>/add_supplier', methods=['POST'])
@role_required('Admin', 'Procurement')
def add_rfq_supplier(rfq_id):
    db = Database()
    conn = db.get_connection()
    
    existing = conn.execute('SELECT id FROM rfq_suppliers WHERE rfq_id = ? AND supplier_id = ?',
                           (rfq_id, request.form['supplier_id'])).fetchone()
    if existing:
        flash('Supplier already added to this RFQ', 'warning')
    else:
        conn.execute('''
            INSERT INTO rfq_suppliers (rfq_id, supplier_id, notes)
            VALUES (?, ?, ?)
        ''', (rfq_id, request.form['supplier_id'], request.form.get('notes', '')))
        conn.commit()
        flash('Supplier added successfully!', 'success')
    
    conn.close()
    return redirect(url_for('rfq_routes.edit_rfq', rfq_id=rfq_id))

@rfq_bp.route('/rfqs/<int:rfq_id>/remove_supplier/<int:supplier_id>', methods=['POST'])
@role_required('Admin', 'Procurement')
def remove_rfq_supplier(rfq_id, supplier_id):
    db = Database()
    conn = db.get_connection()
    conn.execute('DELETE FROM rfq_suppliers WHERE rfq_id = ? AND supplier_id = ?', (rfq_id, supplier_id))
    conn.commit()
    flash('Supplier removed successfully!', 'success')
    conn.close()
    return redirect(url_for('rfq_routes.edit_rfq', rfq_id=rfq_id))

@rfq_bp.route('/rfqs/<int:rfq_id>/issue', methods=['POST'])
@role_required('Admin', 'Procurement')
def issue_rfq(rfq_id):
    db = Database()
    conn = db.get_connection()
    
    lines = conn.execute('SELECT COUNT(*) as cnt FROM rfq_lines WHERE rfq_id = ?', (rfq_id,)).fetchone()
    suppliers = conn.execute('SELECT COUNT(*) as cnt FROM rfq_suppliers WHERE rfq_id = ?', (rfq_id,)).fetchone()
    
    if lines['cnt'] == 0:
        flash('Cannot issue RFQ without any line items', 'warning')
    elif suppliers['cnt'] == 0:
        flash('Cannot issue RFQ without any suppliers', 'warning')
    else:
        conn.execute('''
            UPDATE rfqs SET status = ?, issue_date = COALESCE(issue_date, ?), updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', ('Issued', datetime.now().strftime('%Y-%m-%d'), rfq_id))
        
        conn.execute('''
            UPDATE rfq_suppliers SET sent_date = CURRENT_TIMESTAMP, response_status = 'Pending'
            WHERE rfq_id = ?
        ''', (rfq_id,))
        
        conn.commit()
        flash('RFQ issued successfully!', 'success')
    
    conn.close()
    return redirect(url_for('rfq_routes.view_rfq', rfq_id=rfq_id))

@rfq_bp.route('/rfqs/<int:rfq_id>/add_quote', methods=['POST'])
@role_required('Admin', 'Procurement')
def add_rfq_quote(rfq_id):
    db = Database()
    conn = db.get_connection()
    
    conn.execute('''
        INSERT INTO rfq_quotes (rfq_id, rfq_line_id, supplier_id, quoted_price, quoted_quantity, lead_time_days, valid_until, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        rfq_id,
        request.form['rfq_line_id'],
        request.form['supplier_id'],
        float(request.form['quoted_price']),
        float(request.form.get('quoted_quantity')) if request.form.get('quoted_quantity') else None,
        int(request.form.get('lead_time_days')) if request.form.get('lead_time_days') else None,
        request.form.get('valid_until') or None,
        request.form.get('notes', '')
    ))
    
    conn.execute('''
        UPDATE rfq_suppliers SET response_date = CURRENT_TIMESTAMP, response_status = 'Received'
        WHERE rfq_id = ? AND supplier_id = ?
    ''', (rfq_id, request.form['supplier_id']))
    
    has_quotes = conn.execute('SELECT COUNT(*) as cnt FROM rfq_quotes WHERE rfq_id = ?', (rfq_id,)).fetchone()
    if has_quotes['cnt'] > 0:
        conn.execute("UPDATE rfqs SET status = 'Quotes Received', updated_at = CURRENT_TIMESTAMP WHERE id = ?", (rfq_id,))
    
    conn.commit()
    flash('Quote added successfully!', 'success')
    conn.close()
    return redirect(url_for('rfq_routes.view_rfq', rfq_id=rfq_id))

@rfq_bp.route('/rfqs/<int:rfq_id>/select_quote/<int:quote_id>', methods=['POST'])
@role_required('Admin', 'Procurement')
def select_quote(rfq_id, quote_id):
    db = Database()
    conn = db.get_connection()
    
    quote = conn.execute('SELECT rfq_line_id FROM rfq_quotes WHERE id = ?', (quote_id,)).fetchone()
    if quote:
        conn.execute('UPDATE rfq_quotes SET is_selected = 0 WHERE rfq_id = ? AND rfq_line_id = ?',
                    (rfq_id, quote['rfq_line_id']))
        conn.execute('UPDATE rfq_quotes SET is_selected = 1 WHERE id = ?', (quote_id,))
        conn.commit()
        flash('Quote selected!', 'success')
    
    conn.close()
    return redirect(url_for('rfq_routes.view_rfq', rfq_id=rfq_id))

@rfq_bp.route('/rfqs/<int:rfq_id>/close', methods=['POST'])
@role_required('Admin', 'Procurement')
def close_rfq(rfq_id):
    db = Database()
    conn = db.get_connection()
    conn.execute("UPDATE rfqs SET status = 'Closed', updated_at = CURRENT_TIMESTAMP WHERE id = ?", (rfq_id,))
    conn.commit()
    flash('RFQ closed successfully!', 'success')
    conn.close()
    return redirect(url_for('rfq_routes.view_rfq', rfq_id=rfq_id))

@rfq_bp.route('/rfqs/<int:rfq_id>/delete', methods=['POST'])
@role_required('Admin')
def delete_rfq(rfq_id):
    db = Database()
    conn = db.get_connection()
    
    rfq = conn.execute('SELECT * FROM rfqs WHERE id = ?', (rfq_id,)).fetchone()
    if rfq:
        AuditLogger.log_change(conn, 'rfqs', rfq_id, 'DELETE', session.get('user_id'),
                              {'rfq_number': rfq['rfq_number']})
        conn.execute('DELETE FROM rfqs WHERE id = ?', (rfq_id,))
        conn.commit()
        flash('RFQ deleted successfully!', 'success')
    
    conn.close()
    return redirect(url_for('rfq_routes.list_rfqs'))


@rfq_bp.route('/rfqs/<int:rfq_id>/send-to-supplier', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement')
def send_to_supplier(rfq_id):
    """Generate secure web link for supplier to submit quote"""
    db = Database()
    conn = db.get_connection()
    
    rfq = conn.execute('SELECT * FROM rfqs WHERE id = ?', (rfq_id,)).fetchone()
    if not rfq:
        flash('RFQ not found', 'danger')
        conn.close()
        return redirect(url_for('rfq_routes.list_rfqs'))
    
    if request.method == 'POST':
        supplier_id = request.form.get('supplier_id')
        allow_multiple = request.form.get('allow_multiple_submissions') == '1'
        
        if not supplier_id:
            flash('Please select a supplier', 'danger')
            conn.close()
            return redirect(url_for('rfq_routes.send_to_supplier', rfq_id=rfq_id))
        
        existing_token = conn.execute('''
            SELECT token FROM rfq_supplier_tokens
            WHERE rfq_id = ? AND supplier_id = ? AND expires_at > ?
        ''', (rfq_id, supplier_id, datetime.now().isoformat())).fetchone()
        
        if existing_token:
            flash('A valid link already exists for this supplier', 'warning')
            conn.close()
            return redirect(url_for('rfq_routes.view_rfq', rfq_id=rfq_id))
        
        token = secrets.token_urlsafe(32)
        expires_at = datetime.fromisoformat(rfq['due_date']) if rfq['due_date'] else datetime.now() + timedelta(days=30)
        
        conn.execute('''
            INSERT INTO rfq_supplier_tokens (rfq_id, supplier_id, token, expires_at, allow_multiple_submissions)
            VALUES (?, ?, ?, ?, ?)
        ''', (rfq_id, supplier_id, token, expires_at.isoformat(), 1 if allow_multiple else 0))
        
        existing_link = conn.execute('''
            SELECT id FROM rfq_suppliers WHERE rfq_id = ? AND supplier_id = ?
        ''', (rfq_id, supplier_id)).fetchone()
        
        if not existing_link:
            conn.execute('''
                INSERT INTO rfq_suppliers (rfq_id, supplier_id, sent_date, response_status)
                VALUES (?, ?, ?, 'Pending')
            ''', (rfq_id, supplier_id, datetime.now().isoformat()))
        else:
            conn.execute('''
                UPDATE rfq_suppliers SET sent_date = ?, response_status = 'Pending'
                WHERE rfq_id = ? AND supplier_id = ?
            ''', (datetime.now().isoformat(), rfq_id, supplier_id))
        
        if rfq['status'] == 'Draft':
            conn.execute("UPDATE rfqs SET status = 'Issued' WHERE id = ?", (rfq_id,))
        
        AuditLogger.log_change(conn, 'rfqs', rfq_id, 'SEND_TO_SUPPLIER', session.get('user_id'),
                              {'supplier_id': supplier_id, 'token_generated': True})
        
        conn.commit()
        
        base_url = os.environ.get('REPLIT_DEV_DOMAIN', request.host_url.rstrip('/'))
        if not base_url.startswith('http'):
            base_url = f'https://{base_url}'
        supplier_link = f"{base_url}/rfq/submit/{token}"
        
        flash(f'Supplier link generated successfully! Copy and send to supplier: {supplier_link}', 'success')
        conn.close()
        return redirect(url_for('rfq_routes.view_rfq', rfq_id=rfq_id))
    
    suppliers = conn.execute('SELECT id, code, name, email FROM suppliers ORDER BY name').fetchall()
    
    existing_suppliers = conn.execute('''
        SELECT rs.*, s.name as supplier_name, rst.token, rst.expires_at
        FROM rfq_suppliers rs
        JOIN suppliers s ON rs.supplier_id = s.id
        LEFT JOIN rfq_supplier_tokens rst ON rst.rfq_id = rs.rfq_id AND rst.supplier_id = rs.supplier_id
        WHERE rs.rfq_id = ?
    ''', (rfq_id,)).fetchall()
    
    conn.close()
    
    return render_template('rfqs/send_to_supplier.html',
                          rfq=rfq,
                          suppliers=[dict(s) for s in suppliers],
                          existing_suppliers=[dict(s) for s in existing_suppliers])


@rfq_bp.route('/rfqs/<int:rfq_id>/supplier-responses')
@login_required
def view_supplier_responses(rfq_id):
    """View all supplier responses for an RFQ"""
    db = Database()
    conn = db.get_connection()
    
    rfq = conn.execute('SELECT * FROM rfqs WHERE id = ?', (rfq_id,)).fetchone()
    if not rfq:
        flash('RFQ not found', 'danger')
        conn.close()
        return redirect(url_for('rfq_routes.list_rfqs'))
    
    responses = conn.execute('''
        SELECT rsr.*, s.name as supplier_name, s.code as supplier_code
        FROM rfq_supplier_responses rsr
        JOIN suppliers s ON rsr.supplier_id = s.id
        WHERE rsr.rfq_id = ?
        ORDER BY rsr.submitted_at DESC
    ''', (rfq_id,)).fetchall()
    
    lines = conn.execute('SELECT * FROM rfq_lines WHERE rfq_id = ? ORDER BY line_number', (rfq_id,)).fetchall()
    
    response_details = {}
    for response in responses:
        response_lines = conn.execute('''
            SELECT rrl.*, rl.description, rl.quantity
            FROM rfq_response_lines rrl
            JOIN rfq_lines rl ON rrl.rfq_line_id = rl.id
            WHERE rrl.response_id = ?
        ''', (response['id'],)).fetchall()
        response_details[response['id']] = [dict(rl) for rl in response_lines]
    
    conn.close()
    
    return render_template('rfqs/supplier_responses.html',
                          rfq=rfq,
                          responses=[dict(r) for r in responses],
                          lines=[dict(l) for l in lines],
                          response_details=response_details)


@rfq_bp.route('/rfqs/<int:rfq_id>/convert-to-po/<int:response_id>', methods=['POST'])
@role_required('Admin', 'Procurement')
def convert_response_to_po(rfq_id, response_id):
    """Convert selected RFQ response to Purchase Order"""
    db = Database()
    conn = db.get_connection()
    
    try:
        response = conn.execute('''
            SELECT rsr.*, s.name as supplier_name
            FROM rfq_supplier_responses rsr
            JOIN suppliers s ON rsr.supplier_id = s.id
            WHERE rsr.id = ? AND rsr.rfq_id = ?
        ''', (response_id, rfq_id)).fetchone()
        
        if not response:
            flash('Response not found', 'danger')
            conn.close()
            return redirect(url_for('rfq_routes.view_rfq', rfq_id=rfq_id))
        
        rfq = conn.execute('SELECT * FROM rfqs WHERE id = ?', (rfq_id,)).fetchone()
        
        result = conn.execute('''
            SELECT po_number FROM purchase_orders 
            WHERE po_number LIKE 'PO-%' 
            ORDER BY id DESC LIMIT 1
        ''').fetchone()
        
        if result:
            try:
                last_num = int(result['po_number'].split('-')[1])
                po_number = f"PO-{last_num + 1:05d}"
            except:
                po_number = "PO-00001"
        else:
            po_number = "PO-00001"
        
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO purchase_orders (po_number, supplier_id, status, order_date, notes)
            VALUES (?, ?, 'Draft', ?, ?)
        ''', (po_number, response['supplier_id'], datetime.now().strftime('%Y-%m-%d'),
              f'Created from RFQ {rfq["rfq_number"]}'))
        po_id = cursor.lastrowid
        
        response_lines = conn.execute('''
            SELECT rrl.*, rl.product_id, rl.description, rl.quantity, rl.uom_id
            FROM rfq_response_lines rrl
            JOIN rfq_lines rl ON rrl.rfq_line_id = rl.id
            WHERE rrl.response_id = ?
        ''', (response_id,)).fetchall()
        
        line_num = 1
        for rl in response_lines:
            conn.execute('''
                INSERT INTO purchase_order_lines (po_id, line_number, product_id, description, 
                    quantity, unit_price, uom_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (po_id, line_num, rl['product_id'], rl['description'], 
                  rl['quantity'], rl['unit_price'], rl['uom_id']))
            line_num += 1
        
        conn.execute("UPDATE rfq_supplier_responses SET status = 'Converted to PO' WHERE id = ?", (response_id,))
        conn.execute("UPDATE rfqs SET status = 'Closed' WHERE id = ?", (rfq_id,))
        
        AuditLogger.log_change(conn, 'purchase_orders', po_id, 'CREATE', session.get('user_id'),
                              {'source': 'RFQ', 'rfq_id': rfq_id, 'rfq_number': rfq['rfq_number']})
        
        conn.commit()
        
        flash(f'Purchase Order {po_number} created from RFQ response!', 'success')
        conn.close()
        return redirect(url_for('po_routes.view_purchaseorder', id=po_id))
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error creating PO: {str(e)}', 'danger')
        return redirect(url_for('rfq_routes.view_rfq', rfq_id=rfq_id))
