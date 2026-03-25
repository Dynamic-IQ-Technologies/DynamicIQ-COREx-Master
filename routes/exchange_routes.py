from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify, make_response
from functools import wraps
from models import Database, AuditLogger, safe_float
from datetime import datetime, date, timedelta
import json
import os
import logging

logger = logging.getLogger('exchange_routes')

exchange_bp = Blueprint('exchange', __name__, url_prefix='/exchanges')

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('auth_routes.login'))
        return f(*args, **kwargs)
    return decorated_function

def get_db():
    db = Database()
    return db.get_connection()

def generate_exchange_id(conn):
    result = conn.execute("SELECT MAX(id) as max_id FROM exchange_master").fetchone()
    next_id = (result['max_id'] or 0) + 1
    return f"EXC-{next_id:06d}"

def generate_agreement_number(conn):
    result = conn.execute("SELECT MAX(id) as max_id FROM exchange_agreements").fetchone()
    next_id = (result['max_id'] or 0) + 1
    return f"AGR-{next_id:06d}"

def log_exchange_audit(conn, exchange_id, action_type, previous_status, new_status, details, user_id, username, justification=None):
    conn.execute('''
        INSERT INTO exchange_audit_log (exchange_id, action_type, previous_status, new_status, 
            action_details, performed_by, performed_by_name, ip_address, justification)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (exchange_id, action_type, previous_status, new_status, details, 
          user_id, username, request.remote_addr, justification))

def calculate_days_outstanding(core_due_date):
    if not core_due_date:
        return 0
    if isinstance(core_due_date, str):
        core_due_date = datetime.strptime(core_due_date, '%Y-%m-%d').date()
    today = date.today()
    if today > core_due_date:
        return (today - core_due_date).days
    return 0

CORE_STATUSES = [
    'Awaiting Core',
    'Core Shipped by Customer',
    'Core Received',
    'Core Overdue',
    'Core Disputed',
    'Core Closed'
]

EXCHANGE_STATUSES = [
    'Open',
    'Pending Core',
    'Core Received',
    'Under Review',
    'Closed',
    'Escalated',
    'Cancelled'
]


def _auto_sync_exchange_sos(conn):
    """Auto-create exchange_master records for any Exchange-type SOs not yet tracked."""
    try:
        unlinked = conn.execute('''
            SELECT so.id, so.customer_id, so.exchange_type, so.core_charge, so.status
            FROM sales_orders so
            WHERE so.sales_type LIKE '%Exchange%'
              AND so.id NOT IN (SELECT sales_order_id FROM exchange_master WHERE sales_order_id IS NOT NULL)
        ''').fetchall()
        for so in unlinked:
            result = conn.execute('SELECT MAX(id) as max_id FROM exchange_master').fetchone()
            next_num = (result['max_id'] or 0) + 1
            eid = f'EXC-{next_num:06d}'
            line = conn.execute(
                'SELECT product_id, serial_number, core_charge FROM sales_order_lines WHERE so_id = ? LIMIT 1',
                (so['id'],)
            ).fetchone()
            product_id = line['product_id'] if line else None
            serial = (line['serial_number'] if line else '') or ''
            core_value = float((line['core_charge'] if line else None) or so['core_charge'] or 0)
            status_map = {'Shipped': 'Core Received', 'Completed': 'Closed'}
            exch_status = status_map.get(so['status'], 'Open')
            cur = conn.execute('''
                INSERT INTO exchange_master (exchange_id, sales_order_id, customer_id, product_id,
                    shipped_serial_number, exchange_type, core_due_date,
                    core_value, exchange_fee, deposit_amount, status, created_by)
                VALUES (?, ?, ?, ?, ?, ?, DATE('now', '+30 days'), ?, 0, 0, ?, 1)
            ''', (eid, so['id'], so['customer_id'], product_id, serial,
                  so['exchange_type'] or 'Standard', core_value, exch_status))
            master_id = cur.lastrowid
            if master_id:
                core_status = 'Core Received' if so['status'] == 'Shipped' else 'Awaiting Core'
                conn.execute('''
                    INSERT INTO exchange_cores (exchange_id, core_status, ownership_responsibility, financial_exposure)
                    VALUES (?, ?, 'Customer', ?)
                ''', (master_id, core_status, core_value))
                conn.execute('''
                    INSERT INTO exchange_audit_log (exchange_id, action_type, new_status, action_details, performed_by, performed_by_name, ip_address)
                    VALUES (?, 'Auto-Imported', ?, 'Auto-synced from Exchange Sales Order', 1, 'System', '127.0.0.1')
                ''', (master_id, exch_status))
        if unlinked:
            conn.commit()
    except Exception as e:
        logger.warning(f'Exchange auto-sync warning: {e}')


@exchange_bp.route('/')
@login_required
def exchange_dashboard():
    conn = get_db()
    _auto_sync_exchange_sos(conn)

    status_filter = request.args.get('status', '')
    customer_filter = request.args.get('customer_id', '')
    core_status_filter = request.args.get('core_status', '')
    
    query = '''
        SELECT em.*, c.name as customer_name, p.code as product_code, p.name as product_name,
               ec.core_status, ec.days_outstanding,
               so.so_number
        FROM exchange_master em
        JOIN customers c ON em.customer_id = c.id
        JOIN products p ON em.product_id = p.id
        LEFT JOIN exchange_cores ec ON ec.exchange_id = em.id
        LEFT JOIN sales_orders so ON em.sales_order_id = so.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND em.status = ?'
        params.append(status_filter)
    if customer_filter:
        query += ' AND em.customer_id = ?'
        params.append(customer_filter)
    if core_status_filter:
        query += ' AND ec.core_status = ?'
        params.append(core_status_filter)
    
    query += ' ORDER BY em.created_at DESC'
    
    exchanges = conn.execute(query, params).fetchall()
    
    open_count = conn.execute("SELECT COUNT(*) as cnt FROM exchange_master WHERE status = 'Open'").fetchone()['cnt']
    pending_core = conn.execute("SELECT COUNT(*) as cnt FROM exchange_cores WHERE core_status = 'Awaiting Core'").fetchone()['cnt']
    overdue_count = conn.execute("SELECT COUNT(*) as cnt FROM exchange_cores WHERE core_status = 'Core Overdue'").fetchone()['cnt']
    
    total_exposure = conn.execute("SELECT COALESCE(SUM(financial_exposure), 0) as total FROM exchange_cores WHERE core_status IN ('Awaiting Core', 'Core Overdue')").fetchone()['total']
    
    aging_0_30 = conn.execute("SELECT COUNT(*) as cnt FROM exchange_cores WHERE days_outstanding BETWEEN 0 AND 30 AND core_status NOT IN ('Core Closed', 'Core Received')").fetchone()['cnt']
    aging_31_60 = conn.execute("SELECT COUNT(*) as cnt FROM exchange_cores WHERE days_outstanding BETWEEN 31 AND 60 AND core_status NOT IN ('Core Closed', 'Core Received')").fetchone()['cnt']
    aging_61_90 = conn.execute("SELECT COUNT(*) as cnt FROM exchange_cores WHERE days_outstanding BETWEEN 61 AND 90 AND core_status NOT IN ('Core Closed', 'Core Received')").fetchone()['cnt']
    aging_90_plus = conn.execute("SELECT COUNT(*) as cnt FROM exchange_cores WHERE days_outstanding > 90 AND core_status NOT IN ('Core Closed', 'Core Received')").fetchone()['cnt']
    
    customers = conn.execute('SELECT id, name FROM customers ORDER BY name').fetchall()
    
    unread_alerts = conn.execute("SELECT COUNT(*) as cnt FROM exchange_alerts WHERE is_read = 0").fetchone()['cnt']
    
    exchange_pos = conn.execute('''
        SELECT po.id, po.po_number, po.status, po.order_date, po.expected_delivery_date,
               po.exchange_owner_type, po.exchange_owner_id, po.exchange_reference_id,
               po.exchange_status, po.source_sales_order_id,
               s.name as supplier_name,
               so.so_number,
               CASE 
                   WHEN po.exchange_owner_type = 'Customer' THEN c.name
                   WHEN po.exchange_owner_type = 'Supplier' THEN sup.name
               END as owner_name,
               COALESCE(SUM(pol.quantity * pol.unit_price), 0) as total_amount,
               CASE 
                   WHEN po.expected_delivery_date < date('now') AND po.status NOT IN ('Received', 'Closed', 'Cancelled') 
                   THEN julianday('now') - julianday(po.expected_delivery_date)
                   ELSE 0
               END as days_overdue
        FROM purchase_orders po
        JOIN suppliers s ON po.supplier_id = s.id
        LEFT JOIN sales_orders so ON po.source_sales_order_id = so.id
        LEFT JOIN customers c ON po.exchange_owner_type = 'Customer' AND po.exchange_owner_id = c.id
        LEFT JOIN suppliers sup ON po.exchange_owner_type = 'Supplier' AND po.exchange_owner_id = sup.id
        LEFT JOIN purchase_order_lines pol ON pol.po_id = po.id
        WHERE po.is_exchange = 1
        GROUP BY po.id, po.po_number, po.status, po.order_date, po.expected_delivery_date,
                 po.exchange_owner_type, po.exchange_owner_id, po.exchange_reference_id,
                 po.exchange_status, po.source_sales_order_id, po.created_at,
                 s.name, so.so_number, c.name, sup.name
        ORDER BY po.expected_delivery_date ASC, po.created_at DESC
    ''').fetchall()
    
    exchange_po_stats = {
        'total': len(exchange_pos),
        'customer_owned': sum(1 for p in exchange_pos if p['exchange_owner_type'] == 'Customer'),
        'supplier_owned': sum(1 for p in exchange_pos if p['exchange_owner_type'] == 'Supplier'),
        'overdue': sum(1 for p in exchange_pos if p['days_overdue'] and p['days_overdue'] > 0)
    }
    
    conn.close()
    
    return render_template('exchanges/dashboard.html',
                          exchanges=[dict(e) for e in exchanges],
                          customers=[dict(c) for c in customers],
                          open_count=open_count,
                          pending_core=pending_core,
                          overdue_count=overdue_count,
                          total_exposure=total_exposure,
                          aging_0_30=aging_0_30,
                          aging_31_60=aging_31_60,
                          aging_61_90=aging_61_90,
                          aging_90_plus=aging_90_plus,
                          unread_alerts=unread_alerts,
                          exchange_statuses=EXCHANGE_STATUSES,
                          core_statuses=CORE_STATUSES,
                          status_filter=status_filter,
                          customer_filter=customer_filter,
                          core_status_filter=core_status_filter,
                          exchange_pos=[dict(e) for e in exchange_pos],
                          exchange_po_stats=exchange_po_stats)


@exchange_bp.route('/create', methods=['GET', 'POST'])
@login_required
def create_exchange():
    conn = get_db()
    
    if request.method == 'POST':
        exchange_id = generate_exchange_id(conn)
        sales_order_id = request.form.get('sales_order_id')
        customer_id = request.form.get('customer_id')
        product_id = request.form.get('product_id')
        shipped_serial = request.form.get('shipped_serial_number')
        expected_core_serial = request.form.get('expected_core_serial')
        exchange_type = request.form.get('exchange_type', 'Standard')
        core_due_date = request.form.get('core_due_date')
        core_value = float(request.form.get('core_value') or 0)
        exchange_fee = float(request.form.get('exchange_fee') or 0)
        deposit_amount = float(request.form.get('deposit_amount') or 0)
        
        cursor = conn.execute('''
            INSERT INTO exchange_master (exchange_id, sales_order_id, customer_id, product_id,
                shipped_serial_number, expected_core_serial, exchange_type, core_due_date,
                core_value, exchange_fee, deposit_amount, status, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Open', ?)
        ''', (exchange_id, sales_order_id, customer_id, product_id, shipped_serial,
              expected_core_serial, exchange_type, core_due_date, core_value, 
              exchange_fee, deposit_amount, session.get('user_id')))
        
        master_id = cursor.lastrowid
        
        conn.execute('''
            INSERT INTO exchange_cores (exchange_id, core_status, ownership_responsibility, financial_exposure)
            VALUES (?, 'Awaiting Core', 'Customer', ?)
        ''', (master_id, core_value))
        
        log_exchange_audit(conn, master_id, 'Created', None, 'Open', 
                          f'Exchange {exchange_id} created from SO', 
                          session.get('user_id'), session.get('username'))
        
        conn.commit()
        conn.close()
        
        flash(f'Exchange {exchange_id} created successfully', 'success')
        return redirect(url_for('exchange.view_exchange', exchange_id=master_id))
    
    sales_orders = conn.execute('''
        SELECT so.*, c.name as customer_name 
        FROM sales_orders so 
        JOIN customers c ON so.customer_id = c.id
        WHERE so.sales_type LIKE '%Exchange%' OR so.sales_type = 'Repair Exchange'
        ORDER BY so.so_number DESC
    ''').fetchall()
    
    customers = conn.execute('SELECT id, name FROM customers ORDER BY name').fetchall()
    products = conn.execute('SELECT id, code, name FROM products ORDER BY code').fetchall()
    
    conn.close()
    
    return render_template('exchanges/create.html',
                          sales_orders=[dict(s) for s in sales_orders],
                          customers=[dict(c) for c in customers],
                          products=[dict(p) for p in products])


@exchange_bp.route('/<int:exchange_id>')
@login_required
def view_exchange(exchange_id):
    conn = get_db()
    
    exchange = conn.execute('''
        SELECT em.*, c.name as customer_name, c.email as customer_email,
               p.code as product_code, p.name as product_name,
               so.so_number, so.core_charge as so_core_charge, so.exchange_type as so_exchange_type,
               so_creator.username as created_by_name,
               wo.wo_number as repair_wo_number, wo.status as repair_wo_status,
               COALESCE(sol.serial_number, i.serial_number, i.msn_esn) as allocated_serial, 
               sol.quantity as line_qty,
               sol.unit_price as line_unit_price, sol.line_total as line_total,
               COALESCE(i.unit_cost, sol.cost, 0) as inventory_cost,
               COALESCE(sol.core_charge, 0) as line_exchange_fee,
               i.serial_number as inventory_serial,
               i.msn_esn as inventory_msn_esn,
               sol.serial_number as so_line_serial
        FROM exchange_master em
        JOIN customers c ON em.customer_id = c.id
        JOIN products p ON em.product_id = p.id
        LEFT JOIN sales_orders so ON em.sales_order_id = so.id
        LEFT JOIN users so_creator ON so.created_by = so_creator.id
        LEFT JOIN sales_order_lines sol ON so.id = sol.so_id AND sol.product_id = em.product_id
        LEFT JOIN inventory i ON sol.inventory_id = i.id
        LEFT JOIN work_orders wo ON em.repair_work_order_id = wo.id
        WHERE em.id = ?
    ''', (exchange_id,)).fetchone()
    
    if not exchange:
        flash('Exchange not found', 'error')
        return redirect(url_for('exchange.exchange_dashboard'))
    
    core = conn.execute('SELECT * FROM exchange_cores WHERE exchange_id = ?', (exchange_id,)).fetchone()
    
    linked_pos = conn.execute('''
        SELECT epo.*, po.po_number, s.name as supplier_name
        FROM exchange_purchase_orders epo
        JOIN purchase_orders po ON epo.purchase_order_id = po.id
        LEFT JOIN suppliers s ON po.supplier_id = s.id
        WHERE epo.exchange_id = ?
    ''', (exchange_id,)).fetchall()
    
    dual_exchange_pos = []
    if exchange['sales_order_id']:
        dual_exchange_pos = conn.execute('''
            SELECT po.id, po.po_number, po.status, po.order_date, po.expected_delivery_date,
                   po.exchange_owner_type, po.exchange_owner_id, po.exchange_reference_id,
                   po.exchange_status,
                   s.name as supplier_name,
                   CASE 
                       WHEN po.exchange_owner_type = 'Customer' THEN c.name
                       WHEN po.exchange_owner_type = 'Supplier' THEN sup.name
                   END as owner_name,
                   COALESCE(SUM(pol.quantity * pol.unit_price), 0) as total_amount
            FROM purchase_orders po
            JOIN suppliers s ON po.supplier_id = s.id
            LEFT JOIN customers c ON po.exchange_owner_type = 'Customer' AND po.exchange_owner_id = c.id
            LEFT JOIN suppliers sup ON po.exchange_owner_type = 'Supplier' AND po.exchange_owner_id = sup.id
            LEFT JOIN purchase_order_lines pol ON pol.po_id = po.id
            WHERE po.source_sales_order_id = ? AND po.is_exchange = 1
            GROUP BY po.id, po.po_number, po.status, po.order_date, po.expected_delivery_date,
                     po.exchange_owner_type, po.exchange_owner_id, po.exchange_reference_id,
                     po.exchange_status, s.name, c.name, sup.name
        ''', (exchange['sales_order_id'],)).fetchall()
    
    agreements = conn.execute('''
        SELECT * FROM exchange_agreements WHERE exchange_id = ? ORDER BY version DESC
    ''', (exchange_id,)).fetchall()
    
    audit_log = conn.execute('''
        SELECT * FROM exchange_audit_log WHERE exchange_id = ? ORDER BY performed_at DESC LIMIT 50
    ''', (exchange_id,)).fetchall()
    
    alerts = conn.execute('''
        SELECT * FROM exchange_alerts WHERE exchange_id = ? ORDER BY created_at DESC
    ''', (exchange_id,)).fetchall()
    
    products = conn.execute('''
        SELECT id, code, name FROM products ORDER BY code
    ''').fetchall()
    
    conn.close()
    
    return render_template('exchanges/view.html',
                          exchange=dict(exchange),
                          core=dict(core) if core else None,
                          linked_pos=[dict(p) for p in linked_pos],
                          dual_exchange_pos=[dict(p) for p in dual_exchange_pos],
                          agreements=[dict(a) for a in agreements],
                          audit_log=[dict(a) for a in audit_log],
                          alerts=[dict(a) for a in alerts],
                          products=[dict(p) for p in products],
                          core_statuses=CORE_STATUSES,
                          exchange_statuses=EXCHANGE_STATUSES)


@exchange_bp.route('/<int:exchange_id>/update-core-status', methods=['POST'])
@login_required
def update_core_status(exchange_id):
    conn = get_db()
    
    new_status = request.form.get('core_status')
    notes = request.form.get('notes', '')
    justification = request.form.get('justification', '')
    
    core = conn.execute('SELECT * FROM exchange_cores WHERE exchange_id = ?', (exchange_id,)).fetchone()
    if not core:
        conn.close()
        return jsonify({'success': False, 'error': 'Core record not found'})
    
    old_status = core['core_status']
    
    days_outstanding = 0
    exchange = conn.execute('SELECT core_due_date FROM exchange_master WHERE id = ?', (exchange_id,)).fetchone()
    if exchange and exchange['core_due_date']:
        days_outstanding = calculate_days_outstanding(exchange['core_due_date'])
    
    update_fields = {'core_status': new_status, 'days_outstanding': days_outstanding, 'last_updated': datetime.now()}
    
    if new_status == 'Core Received':
        update_fields['received_date'] = date.today()
        update_fields['received_by'] = session.get('user_id')
    elif new_status == 'Core Shipped by Customer':
        update_fields['shipped_by_customer_date'] = date.today()
    elif new_status == 'Core Disputed':
        update_fields['dispute_date'] = date.today()
        update_fields['dispute_reason'] = notes
    
    conn.execute('''
        UPDATE exchange_cores SET core_status = ?, days_outstanding = ?, last_updated = ?,
            received_date = COALESCE(?, received_date),
            received_by = COALESCE(?, received_by),
            shipped_by_customer_date = COALESCE(?, shipped_by_customer_date),
            dispute_date = COALESCE(?, dispute_date),
            dispute_reason = COALESCE(?, dispute_reason)
        WHERE exchange_id = ?
    ''', (new_status, days_outstanding, datetime.now(),
          update_fields.get('received_date'), update_fields.get('received_by'),
          update_fields.get('shipped_by_customer_date'), update_fields.get('dispute_date'),
          update_fields.get('dispute_reason'), exchange_id))
    
    if new_status == 'Core Received':
        conn.execute("UPDATE exchange_master SET status = 'Core Received' WHERE id = ?", (exchange_id,))
    elif new_status == 'Core Closed':
        conn.execute("UPDATE exchange_master SET status = 'Closed', closed_at = ?, closed_by = ? WHERE id = ?", 
                    (datetime.now(), session.get('user_id'), exchange_id))
    
    log_exchange_audit(conn, exchange_id, 'Core Status Updated', old_status, new_status,
                      notes, session.get('user_id'), session.get('username'), justification)
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True, 'message': f'Core status updated to {new_status}'})


@exchange_bp.route('/<int:exchange_id>/generate-agreement', methods=['POST'])
@login_required
def generate_agreement(exchange_id):
    print(f"[DEBUG] Generate agreement called for exchange_id={exchange_id}")
    conn = get_db()
    
    try:
        exchange = conn.execute('''
            SELECT em.*, c.name as customer_name, c.billing_address as customer_address,
                   p.code as product_code, p.name as product_name
            FROM exchange_master em
            JOIN customers c ON em.customer_id = c.id
            JOIN products p ON em.product_id = p.id
            WHERE em.id = ?
        ''', (exchange_id,)).fetchone()
        
        if not exchange:
            conn.close()
            return jsonify({'success': False, 'error': 'Exchange not found'})
        
        agreement_number = generate_agreement_number(conn)
        
        exchange_terms = request.form.get('exchange_terms', f'''
This Exchange Agreement governs the exchange transaction for the following unit:
- Part Number: {exchange['product_code']}
- Description: {exchange['product_name']}
- Shipped Serial: {exchange['shipped_serial_number'] or 'TBD'}

The Customer agrees to return a serviceable core unit within the specified timeframe.
''')
        
        penalty_terms = request.form.get('penalty_terms', f'''
Core Return Terms:
- Core Due Date: {exchange['core_due_date']}
- Core Value: ${safe_float(exchange['core_value']):,.2f}

Failure to return the core by the due date will result in:
- Full core charge of ${safe_float(exchange['core_value']):,.2f}
- Additional administrative fees may apply
''')
        
        legal_clauses = request.form.get('legal_clauses', '''
Standard Terms and Conditions:
1. The core must be returned in serviceable condition
2. All cores are subject to inspection upon receipt
3. Non-conforming cores may be rejected or subject to additional charges
4. Title to the shipped unit transfers upon receipt of acceptable core
''')
        
        cursor = conn.execute('''
            INSERT INTO exchange_agreements (exchange_id, agreement_number, customer_id, product_id,
                part_number, serial_number, core_due_date, exchange_terms, penalty_terms, 
                legal_clauses, status, generated_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Draft', ?)
        ''', (exchange_id, agreement_number, exchange['customer_id'], exchange['product_id'],
              exchange['product_code'], exchange['shipped_serial_number'], exchange['core_due_date'],
              exchange_terms, penalty_terms, legal_clauses, session.get('user_id')))
        
        agreement_id = cursor.lastrowid
        
        log_exchange_audit(conn, exchange_id, 'Agreement Generated', None, 'Draft',
                          f'Agreement {agreement_number} generated', 
                          session.get('user_id'), session.get('username'))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'agreement_id': agreement_id, 'agreement_number': agreement_number})
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)})


@exchange_bp.route('/agreement/<int:agreement_id>')
@login_required
def view_agreement(agreement_id):
    conn = get_db()
    
    agreement = conn.execute('''
        SELECT ea.*, c.name as customer_name, c.billing_address as customer_address,
               c.phone as customer_phone, c.email as customer_email,
               p.code as product_code, p.name as product_name,
               em.exchange_id as exchange_number, em.exchange_type, em.shipped_serial_number,
               em.core_value, em.exchange_fee
        FROM exchange_agreements ea
        JOIN customers c ON ea.customer_id = c.id
        JOIN products p ON ea.product_id = p.id
        JOIN exchange_master em ON ea.exchange_id = em.id
        WHERE ea.id = ?
    ''', (agreement_id,)).fetchone()
    
    conn.close()
    
    if not agreement:
        flash('Agreement not found', 'error')
        return redirect(url_for('exchange.exchange_dashboard'))
    
    return render_template('exchanges/view_agreement.html', agreement=agreement)


@exchange_bp.route('/agreement/<int:agreement_id>/download')
@login_required
def download_agreement(agreement_id):
    conn = get_db()
    
    agreement = conn.execute('''
        SELECT ea.*, c.name as customer_name, c.billing_address as customer_address,
               c.phone as customer_phone, c.email as customer_email,
               p.code as product_code, p.name as product_name,
               em.exchange_id as exchange_number, em.exchange_type, em.shipped_serial_number,
               em.core_value, em.exchange_fee
        FROM exchange_agreements ea
        JOIN customers c ON ea.customer_id = c.id
        JOIN products p ON ea.product_id = p.id
        JOIN exchange_master em ON ea.exchange_id = em.id
        WHERE ea.id = ?
    ''', (agreement_id,)).fetchone()
    
    conn.close()
    
    if not agreement:
        flash('Agreement not found', 'error')
        return redirect(url_for('exchange.exchange_dashboard'))
    
    html_content = f'''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Exchange Agreement - {agreement['agreement_number']}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }}
            .header {{ text-align: center; border-bottom: 2px solid #333; padding-bottom: 20px; margin-bottom: 30px; }}
            .header h1 {{ margin: 0; color: #333; }}
            .header p {{ margin: 5px 0; color: #666; }}
            .section {{ margin-bottom: 25px; }}
            .section h3 {{ color: #333; border-bottom: 1px solid #ddd; padding-bottom: 5px; }}
            .info-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
            .info-box {{ background: #f8f9fa; padding: 15px; border-radius: 5px; }}
            .info-box h4 {{ margin: 0 0 10px 0; color: #333; }}
            .info-box p {{ margin: 5px 0; }}
            .terms {{ background: #fff3cd; padding: 15px; border-radius: 5px; margin: 15px 0; }}
            .penalty {{ background: #f8d7da; padding: 15px; border-radius: 5px; margin: 15px 0; }}
            .legal {{ background: #e7f3ff; padding: 15px; border-radius: 5px; margin: 15px 0; }}
            .signature-section {{ margin-top: 50px; display: grid; grid-template-columns: 1fr 1fr; gap: 40px; }}
            .signature-box {{ border-top: 1px solid #333; padding-top: 10px; }}
            .signature-box p {{ margin: 5px 0; }}
            .footer {{ margin-top: 40px; text-align: center; font-size: 12px; color: #666; }}
            @media print {{ body {{ margin: 20px; }} }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>EXCHANGE AGREEMENT</h1>
            <p><strong>{agreement['agreement_number']}</strong> | Version {agreement['version']}</p>
            <p>Generated: {agreement['generated_at']}</p>
        </div>
        
        <div class="info-grid">
            <div class="info-box">
                <h4>Customer Information</h4>
                <p><strong>{agreement['customer_name']}</strong></p>
                <p>{agreement['customer_address'] or ''}</p>
                <p>Phone: {agreement['customer_phone'] or '-'} | Email: {agreement['customer_email'] or '-'}</p>
            </div>
            <div class="info-box">
                <h4>Exchange Details</h4>
                <p><strong>Exchange #:</strong> {agreement['exchange_number']}</p>
                <p><strong>Type:</strong> {agreement['exchange_type'] or 'Standard'}</p>
                <p><strong>Part Number:</strong> {agreement['part_number']}</p>
                <p><strong>Description:</strong> {agreement['product_name']}</p>
                <p><strong>Serial #:</strong> {agreement['shipped_serial_number'] or 'TBD'}</p>
            </div>
        </div>
        
        <div class="section">
            <h3>Financial Terms</h3>
            <div class="info-box">
                <p><strong>Core Value:</strong> ${agreement['core_value'] or 0:,.2f}</p>
                <p><strong>Exchange Fee:</strong> ${agreement['exchange_fee'] or 0:,.2f}</p>
                <p><strong>Core Due Date:</strong> {agreement['core_due_date']}</p>
            </div>
        </div>
        
        <div class="section">
            <h3>Exchange Terms</h3>
            <div class="terms">
                <pre style="white-space: pre-wrap; font-family: Arial;">{agreement['exchange_terms']}</pre>
            </div>
        </div>
        
        <div class="section">
            <h3>Penalty Terms</h3>
            <div class="penalty">
                <pre style="white-space: pre-wrap; font-family: Arial;">{agreement['penalty_terms']}</pre>
            </div>
        </div>
        
        <div class="section">
            <h3>Legal Terms & Conditions</h3>
            <div class="legal">
                <pre style="white-space: pre-wrap; font-family: Arial;">{agreement['legal_clauses']}</pre>
            </div>
        </div>
        
        <div class="signature-section">
            <div class="signature-box">
                <p><strong>Customer Signature</strong></p>
                <p>Name: _______________________</p>
                <p>Title: _______________________</p>
                <p>Date: _______________________</p>
            </div>
            <div class="signature-box">
                <p><strong>Company Representative</strong></p>
                <p>Name: _______________________</p>
                <p>Title: _______________________</p>
                <p>Date: _______________________</p>
            </div>
        </div>
        
        <div class="footer">
            <p>This agreement is legally binding upon signature by both parties.</p>
            <p>Agreement ID: {agreement['agreement_number']} | Status: {agreement['status']}</p>
        </div>
    </body>
    </html>
    '''
    
    response = make_response(html_content)
    response.headers['Content-Type'] = 'text/html'
    response.headers['Content-Disposition'] = f'attachment; filename="{agreement["agreement_number"]}.html"'
    return response


@exchange_bp.route('/<int:exchange_id>/create-late-fee-invoice', methods=['POST'])
@login_required
def create_late_fee_invoice(exchange_id):
    conn = get_db()
    
    try:
        exchange = conn.execute('''
            SELECT em.*, c.name as customer_name, p.code as product_code, p.name as product_name,
                   ec.days_outstanding
            FROM exchange_master em
            JOIN customers c ON em.customer_id = c.id
            JOIN products p ON em.product_id = p.id
            LEFT JOIN exchange_cores ec ON ec.exchange_id = em.id
            WHERE em.id = ?
        ''', (exchange_id,)).fetchone()
        
        if not exchange:
            conn.close()
            return jsonify({'success': False, 'error': 'Exchange not found'})
        
        fee_type = request.form.get('fee_type', 'percentage')
        core_value = safe_float(exchange['core_value'])
        notes = request.form.get('notes', '').strip()
        
        if fee_type == 'percentage':
            percentage = safe_float(request.form.get('fee_percentage', 10))
            late_fee = core_value * (percentage / 100)
            fee_desc = f"{percentage}% of core value"
        elif fee_type == 'flat':
            late_fee = safe_float(request.form.get('flat_fee', 0))
            fee_desc = "Flat fee"
        else:
            late_fee = core_value
            fee_desc = "Full core charge"
        
        count = conn.execute('SELECT COUNT(*) as count FROM invoices').fetchone()['count']
        invoice_number = f"INV-{count + 1:05d}"
        
        today = date.today()
        due_date = today + timedelta(days=30)
        
        cursor = conn.execute('''
            INSERT INTO invoices (
                invoice_number, invoice_type, customer_id, exchange_id,
                invoice_date, due_date, payment_terms, status,
                subtotal, tax_rate, tax_amount, discount_amount, total_amount,
                balance_due, notes, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            invoice_number, 'Exchange Late Fee', exchange['customer_id'], exchange_id,
            today.isoformat(), due_date.isoformat(), 30, 'Draft',
            late_fee, 0, 0, 0, late_fee,
            late_fee, f"Late fee for exchange {exchange['exchange_id']}: {fee_desc}. Days overdue: {exchange['days_outstanding'] or 0}. {notes}",
            session.get('user_id')
        ))
        
        invoice_id = cursor.lastrowid
        
        log_exchange_audit(conn, exchange_id, 'Late Fee Invoice Created', None, None,
                          f'Invoice {invoice_number} created for ${late_fee:,.2f}',
                          session.get('user_id'), session.get('username'))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'invoice_id': invoice_id, 'invoice_number': invoice_number})
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)})


@exchange_bp.route('/<int:exchange_id>/link-po', methods=['POST'])
@login_required
def link_purchase_order(exchange_id):
    conn = get_db()
    
    po_id = request.form.get('purchase_order_id')
    exchange_fee = float(request.form.get('po_exchange_fee') or 0)
    core_charge = float(request.form.get('po_core_charge') or 0)
    penalty = float(request.form.get('po_penalty') or 0)
    notes = request.form.get('notes', '')
    
    existing = conn.execute('''
        SELECT id FROM exchange_purchase_orders WHERE exchange_id = ? AND purchase_order_id = ?
    ''', (exchange_id, po_id)).fetchone()
    
    if existing:
        conn.close()
        return jsonify({'success': False, 'error': 'This PO is already linked to this exchange'})
    
    conn.execute('''
        INSERT INTO exchange_purchase_orders (exchange_id, purchase_order_id, po_exchange_fee,
            po_core_charge, po_penalty, notes)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (exchange_id, po_id, exchange_fee, core_charge, penalty, notes))
    
    po = conn.execute('SELECT po_number FROM purchase_orders WHERE id = ?', (po_id,)).fetchone()
    
    log_exchange_audit(conn, exchange_id, 'PO Linked', None, None,
                      f'Linked to PO {po["po_number"] if po else po_id}',
                      session.get('user_id'), session.get('username'))
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True, 'message': 'Purchase order linked successfully'})


@exchange_bp.route('/ai-coordinator', methods=['GET', 'POST'])
@login_required
def ai_coordinator():
    conn = get_db()
    
    if request.method == 'POST':
        analysis_type = request.form.get('analysis_type', 'risk_assessment')
        
        exchanges = conn.execute('''
            SELECT em.*, c.name as customer_name, p.code as product_code,
                   ec.core_status, ec.days_outstanding, ec.financial_exposure
            FROM exchange_master em
            JOIN customers c ON em.customer_id = c.id
            JOIN products p ON em.product_id = p.id
            LEFT JOIN exchange_cores ec ON ec.exchange_id = em.id
            WHERE em.status NOT IN ('Closed', 'Cancelled')
            ORDER BY ec.days_outstanding DESC
        ''').fetchall()
        
        customer_history = conn.execute('''
            SELECT c.id, c.name, 
                   COUNT(em.id) as total_exchanges,
                   SUM(CASE WHEN ec.core_status = 'Core Overdue' THEN 1 ELSE 0 END) as overdue_count,
                   AVG(ec.days_outstanding) as avg_days_outstanding
            FROM customers c
            JOIN exchange_master em ON em.customer_id = c.id
            LEFT JOIN exchange_cores ec ON ec.exchange_id = em.id
            GROUP BY c.id
        ''').fetchall()
        
        from openai import OpenAI
        
        client = OpenAI(
            api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
            base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
        )
        
        exchange_data = [dict(e) for e in exchanges]
        customer_data = [dict(c) for c in customer_history]
        
        prompt = f"""Analyze the current exchange portfolio and provide risk assessment and recommendations.

ACTIVE EXCHANGES:
{json.dumps(exchange_data, indent=2, default=str)}

CUSTOMER EXCHANGE HISTORY:
{json.dumps(customer_data, indent=2, default=str)}

Provide analysis including:
1. High-Risk Exchanges - Identify exchanges at risk of default or delay
2. Customer Risk Assessment - Flag customers with poor exchange return history
3. Financial Exposure Summary - Total exposure by risk category
4. Recommended Actions - Specific steps to mitigate risks
5. Follow-up Priorities - Which exchanges need immediate attention
6. Deposit Recommendations - Suggest deposit requirements for high-risk customers
7. Escalation Triggers - When to escalate to management

Use plain text without special characters. Use dashes for lists."""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are an AI Exchange Coordinator Manager for an aerospace MRO company. Provide actionable risk assessments and recommendations for managing exchange transactions. Use plain text only."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.5,
            max_tokens=2500
        )
        
        analysis = response.choices[0].message.content
        
        conn.execute('''
            INSERT INTO exchange_ai_analyses (analysis_type, risk_level, findings, recommendations, analyzed_at)
            VALUES (?, 'Mixed', ?, ?, datetime('now'))
        ''', (analysis_type, analysis, analysis))
        conn.commit()
        
        conn.close()
        
        return jsonify({'success': True, 'analysis': analysis})
    
    recent_analyses = conn.execute('''
        SELECT * FROM exchange_ai_analyses ORDER BY analyzed_at DESC LIMIT 10
    ''').fetchall()
    
    conn.close()
    
    return render_template('exchanges/ai_coordinator.html',
                          recent_analyses=[dict(a) for a in recent_analyses])


@exchange_bp.route('/aging-report')
@login_required
def aging_report():
    conn = get_db()
    
    aging_data = conn.execute('''
        SELECT em.exchange_id, em.core_due_date, em.core_value,
               c.name as customer_name, p.code as product_code,
               ec.core_status, ec.days_outstanding, ec.financial_exposure
        FROM exchange_master em
        JOIN customers c ON em.customer_id = c.id
        JOIN products p ON em.product_id = p.id
        LEFT JOIN exchange_cores ec ON ec.exchange_id = em.id
        WHERE em.status NOT IN ('Closed', 'Cancelled')
        AND ec.core_status NOT IN ('Core Received', 'Core Closed')
        ORDER BY ec.days_outstanding DESC
    ''').fetchall()
    
    exposure_by_customer = conn.execute('''
        SELECT c.name as customer_name, 
               SUM(ec.financial_exposure) as total_exposure,
               COUNT(em.id) as exchange_count
        FROM customers c
        JOIN exchange_master em ON em.customer_id = c.id
        LEFT JOIN exchange_cores ec ON ec.exchange_id = em.id
        WHERE em.status NOT IN ('Closed', 'Cancelled')
        GROUP BY c.id
        ORDER BY total_exposure DESC
    ''').fetchall()
    
    conn.close()
    
    return render_template('exchanges/aging_report.html',
                          aging_data=[dict(a) for a in aging_data],
                          exposure_by_customer=[dict(e) for e in exposure_by_customer])


@exchange_bp.route('/api/update-days-outstanding', methods=['POST'])
@login_required
def update_all_days_outstanding():
    conn = get_db()
    
    exchanges = conn.execute('''
        SELECT em.id, em.core_due_date, ec.id as core_id, ec.core_status
        FROM exchange_master em
        JOIN exchange_cores ec ON ec.exchange_id = em.id
        WHERE ec.core_status NOT IN ('Core Received', 'Core Closed')
    ''').fetchall()
    
    updated = 0
    for ex in exchanges:
        days = calculate_days_outstanding(ex['core_due_date'])
        new_status = ex['core_status']
        if days > 0 and ex['core_status'] == 'Awaiting Core':
            new_status = 'Core Overdue'
        
        conn.execute('''
            UPDATE exchange_cores SET days_outstanding = ?, core_status = ?, last_updated = ?
            WHERE id = ?
        ''', (days, new_status, datetime.now(), ex['core_id']))
        updated += 1
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True, 'updated': updated})


@exchange_bp.route('/<int:exchange_id>/receive-core', methods=['POST'])
@login_required
def receive_core(exchange_id):
    conn = get_db()
    
    try:
        exchange = conn.execute('SELECT * FROM exchange_master WHERE id = ?', (exchange_id,)).fetchone()
        if not exchange:
            flash('Exchange not found', 'error')
            conn.close()
            return redirect(url_for('exchange.exchange_dashboard'))
        
        core = conn.execute('SELECT * FROM exchange_cores WHERE exchange_id = ?', (exchange_id,)).fetchone()
        old_status = core['core_status'] if core else 'Unknown'
        
        core_serial = request.form.get('core_serial_number', '').strip()
        condition = request.form.get('condition_on_receipt', '')
        inspection_notes = request.form.get('inspection_notes', '')
        receiving_location = request.form.get('receiving_location', '')
        quantity_received = int(request.form.get('quantity_received', 1))
        
        pn_action = request.form.get('pn_action', 'same')
        core_product_id = exchange['product_id']
        pn_note = ''
        
        if pn_action == 'select':
            selected_product_id = request.form.get('core_product_id', '')
            if selected_product_id:
                core_product_id = int(selected_product_id)
                product = conn.execute('SELECT code FROM products WHERE id = ?', (core_product_id,)).fetchone()
                pn_note = f' (Different P/N: {product["code"]})'
        elif pn_action == 'new':
            new_pn_code = request.form.get('new_pn_code', '').strip()
            new_pn_name = request.form.get('new_pn_name', '').strip()
            if new_pn_code and new_pn_name:
                existing = conn.execute('SELECT id FROM products WHERE code = ?', (new_pn_code,)).fetchone()
                if existing:
                    core_product_id = existing['id']
                    pn_note = f' (Existing P/N: {new_pn_code})'
                else:
                    cursor = conn.execute('''
                        INSERT INTO products (code, name, category, active, is_serialized, created_at)
                        VALUES (?, ?, 'Core Return', 1, 1, ?)
                    ''', (new_pn_code, new_pn_name, datetime.now().isoformat()))
                    core_product_id = cursor.lastrowid
                    pn_note = f' (New P/N created: {new_pn_code})'
        
        cursor = conn.execute('''
            INSERT INTO inventory (product_id, quantity, warehouse_location, bin_location, condition, status, serial_number, reorder_point, safety_stock, unit_cost, last_updated)
            VALUES (?, ?, ?, 'CORE-RCV', ?, 'Available', ?, 0, 0, 0, ?)
        ''', (core_product_id, quantity_received, receiving_location or 'Receiving Inspection', condition, core_serial, datetime.now().isoformat()))
        inventory_id = cursor.lastrowid
        
        conn.execute('''
            UPDATE exchange_cores SET
                core_status = 'Core Received',
                core_serial_number = ?,
                received_date = ?,
                received_by = ?,
                condition_on_receipt = ?,
                inspection_notes = ?,
                receiving_location = ?,
                quantity_received = ?,
                days_outstanding = 0,
                ownership_responsibility = 'Company',
                inventory_id = ?,
                last_updated = ?
            WHERE exchange_id = ?
        ''', (core_serial, date.today().isoformat(), session.get('user_id'),
              condition, inspection_notes, receiving_location, quantity_received,
              inventory_id, datetime.now().isoformat(), exchange_id))
        
        conn.execute('''
            UPDATE exchange_master SET status = 'Core Received' WHERE id = ?
        ''', (exchange_id,))
        
        # Get exchange number and product info for journal entry description
        product_info = conn.execute('SELECT code, name FROM products WHERE id = ?', (core_product_id,)).fetchone()
        product_desc = f"{product_info['code']} - {product_info['name']}" if product_info else f"Product ID {core_product_id}"
        
        # Create GL Journal Entry for Core Receipt (even at $0 for complete traceability)
        # DR Inventory (1130) - Core received into stock at $0 value
        # CR Core Returns/Suspense - To track the receipt event
        from utils.gl_journal import create_journal_entry, GL_ACCOUNTS
        
        # Get the financial exposure from exchange for tracking purposes
        financial_exposure = float(exchange['financial_exposure']) if exchange['financial_exposure'] else 0
        
        # Journal entry for core receipt - records the event even if $0 value
        # Using Inventory account for the core asset coming in
        journal_lines = [
            {
                'account_code': GL_ACCOUNTS['INVENTORY'],  # 1130 - Inventory
                'debit': 0.00,  # Cores received at $0 cost
                'credit': 0,
                'description': f'Core received: {product_desc}, S/N: {core_serial}'
            }
        ]
        
        # Create GL entry for core receipt with idempotency check
        # Check if GL entry already exists for this exchange core receipt
        existing_gl = conn.execute('''
            SELECT id FROM gl_entries 
            WHERE reference_type = 'Exchange' AND reference_id = ? 
            AND transaction_source = 'Exchange Core Receipt'
        ''', (str(exchange_id),)).fetchone()
        
        if not existing_gl:
            entry_id = create_journal_entry(
                conn=conn,
                entry_date=date.today().isoformat(),
                description=f'Core Receipt - EX-{exchange_id:06d}: {product_desc}, S/N: {core_serial}, Condition: {condition}',
                transaction_source='Exchange Core Receipt',
                reference_type='Exchange',
                reference_id=exchange_id,
                lines=[
                    {
                        'account_code': GL_ACCOUNTS['INVENTORY'],
                        'debit': 0.01,  # Nominal value for balanced entry
                        'credit': 0,
                        'description': f'Core received into inventory: {product_desc}'
                    },
                    {
                        'account_code': GL_ACCOUNTS['INVENTORY'],
                        'debit': 0,
                        'credit': 0.01,  # Offsetting entry
                        'description': f'Core return credit: EX-{exchange_id:06d}'
                    }
                ],
                user_id=session.get('user_id'),
                auto_post=True
            )
            if not entry_id:
                raise Exception('Failed to create GL journal entry for core receipt - transaction rolled back')
            logger.info(f'GL Journal Entry {entry_id} created for Core Receipt EX-{exchange_id:06d}')
        
        log_exchange_audit(conn, exchange_id, 'Core Received', old_status, 'Core Received',
                          f'Core received: S/N {core_serial}, Condition: {condition}, Qty: {quantity_received}{pn_note}, INV-{inventory_id:06d}',
                          session.get('user_id'), session.get('username'))
        
        conn.commit()
        flash(f'Core received successfully! Serial: {core_serial}, Inventory: INV-{inventory_id:06d}{pn_note}', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error receiving core: {str(e)}', 'error')
    finally:
        conn.close()
    
    return redirect(url_for('exchange.view_exchange', exchange_id=exchange_id))


@exchange_bp.route('/<int:exchange_id>/create-repair-wo', methods=['POST'])
@login_required
def create_repair_work_order(exchange_id):
    conn = get_db()
    
    try:
        exchange = conn.execute('''
            SELECT em.*, c.name as customer_name, p.code as product_code, p.name as product_name,
                   ec.core_serial_number, ec.condition_on_receipt
            FROM exchange_master em
            JOIN customers c ON em.customer_id = c.id
            JOIN products p ON em.product_id = p.id
            LEFT JOIN exchange_cores ec ON ec.exchange_id = em.id
            WHERE em.id = ?
        ''', (exchange_id,)).fetchone()
        
        if not exchange:
            flash('Exchange not found', 'error')
            conn.close()
            return redirect(url_for('exchange.exchange_dashboard'))
        
        if exchange['repair_work_order_id']:
            flash('A repair work order already exists for this exchange', 'warning')
            conn.close()
            return redirect(url_for('exchange.view_exchange', exchange_id=exchange_id))
        
        last_wo = conn.execute(
            'SELECT wo_number FROM work_orders ORDER BY id DESC LIMIT 1'
        ).fetchone()
        
        if last_wo and last_wo['wo_number']:
            try:
                parts = last_wo['wo_number'].split('-')
                if len(parts) >= 2:
                    last_num = int(parts[1])
                    wo_number = f'WO-{last_num + 1:06d}'
                else:
                    wo_number = 'WO-000001'
            except:
                wo_number = 'WO-000001'
        else:
            wo_number = 'WO-000001'
        
        priority = request.form.get('priority', 'Medium')
        notes = request.form.get('notes', '')
        planned_start = request.form.get('planned_start_date') or date.today().isoformat()
        planned_end = request.form.get('planned_end_date')
        
        # Use core serial number for work order serial number
        core_serial = exchange['core_serial_number'] or None
        
        cursor = conn.execute('''
            INSERT INTO work_orders (
                wo_number, product_id, quantity, disposition, status, priority,
                planned_start_date, planned_end_date, customer_id, notes, created_by, created_at, serial_number
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            wo_number, exchange['product_id'], 1, 'Repair', 'Draft', priority,
            planned_start, planned_end, exchange['customer_id'],
            f"Exchange Core Repair - {exchange['exchange_id']}\nCore S/N: {exchange['core_serial_number'] or 'N/A'}\nCondition: {exchange['condition_on_receipt'] or 'N/A'}\n{notes}",
            session.get('user_id'), datetime.now().isoformat(), core_serial
        ))
        
        wo_id = cursor.lastrowid
        
        conn.execute('''
            UPDATE exchange_master SET repair_work_order_id = ? WHERE id = ?
        ''', (wo_id, exchange_id))
        
        conn.execute('''
            UPDATE exchange_cores SET work_order_id = ? WHERE exchange_id = ?
        ''', (wo_id, exchange_id))
        
        log_exchange_audit(conn, exchange_id, 'Repair WO Created', exchange['status'], exchange['status'],
                          f'Created repair work order {wo_number} (ID: {wo_id})',
                          session.get('user_id'), session.get('username'))
        
        conn.commit()
        flash(f'Repair Work Order {wo_number} created successfully!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error creating repair work order: {str(e)}', 'error')
    finally:
        conn.close()
    
    return redirect(url_for('exchange.view_exchange', exchange_id=exchange_id))
