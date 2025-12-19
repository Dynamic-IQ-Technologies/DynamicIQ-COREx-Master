from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database
from datetime import datetime, timedelta, date
import json

ndt_bp = Blueprint('ndt_routes', __name__)

NDT_METHODS = ['UT', 'MT', 'PT', 'RT', 'VT', 'ET']
NDT_CODES = ['ASME', 'AWS', 'ASTM', 'ISO', 'MIL-STD', 'Customer Spec']
CERTIFICATION_LEVELS = ['Level I', 'Level II', 'Level III']
NDT_STATUSES = ['Draft', 'Scheduled', 'In Inspection', 'Results Recorded', 'Under Review', 'Approved', 'Rejected', 'Closed']

def get_next_ndt_wo_number(conn):
    """Generate next NDT work order number"""
    result = conn.execute('''
        SELECT ndt_wo_number FROM ndt_work_orders 
        ORDER BY id DESC LIMIT 1
    ''').fetchone()
    if result:
        try:
            num = int(result['ndt_wo_number'].replace('NDT-', ''))
            return f"NDT-{num + 1:05d}"
        except:
            pass
    return "NDT-00001"

def get_next_technician_number(conn):
    """Generate next technician number"""
    result = conn.execute('''
        SELECT technician_number FROM ndt_technicians 
        ORDER BY id DESC LIMIT 1
    ''').fetchone()
    if result:
        try:
            num = int(result['technician_number'].replace('TECH-', ''))
            return f"TECH-{num + 1:04d}"
        except:
            pass
    return "TECH-0001"

def check_technician_certified(conn, technician_id, method, inspection_date=None):
    """Check if technician is certified for the method on the given date"""
    if not inspection_date:
        inspection_date = date.today().isoformat()
    
    cert = conn.execute('''
        SELECT * FROM ndt_certifications 
        WHERE technician_id = ? AND method = ? AND status = 'Active'
        AND expiration_date >= ?
    ''', (technician_id, method, inspection_date)).fetchone()
    
    return cert is not None

def log_status_change(conn, ndt_wo_id, old_status, new_status, user_id, reason=None):
    """Log a status change in the history"""
    conn.execute('''
        INSERT INTO ndt_status_history (ndt_wo_id, old_status, new_status, changed_by, change_reason)
        VALUES (?, ?, ?, ?, ?)
    ''', (ndt_wo_id, old_status, new_status, user_id, reason))

@ndt_bp.route('/ndt')
def dashboard():
    """NDT Operations Dashboard"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    open_wo = conn.execute('''
        SELECT COUNT(*) as count FROM ndt_work_orders 
        WHERE status NOT IN ('Approved', 'Rejected', 'Closed')
    ''').fetchone()['count']
    
    in_inspection = conn.execute('''
        SELECT COUNT(*) as count FROM ndt_work_orders WHERE status = 'In Inspection'
    ''').fetchone()['count']
    
    pending_approval = conn.execute('''
        SELECT COUNT(*) as count FROM ndt_work_orders WHERE status IN ('Results Recorded', 'Under Review')
    ''').fetchone()['count']
    
    overdue = conn.execute('''
        SELECT COUNT(*) as count FROM ndt_work_orders 
        WHERE status NOT IN ('Approved', 'Rejected', 'Closed')
        AND planned_end_date < date('now')
    ''').fetchone()['count']
    
    expiring_certs = conn.execute('''
        SELECT COUNT(*) as count FROM ndt_certifications 
        WHERE status = 'Active' 
        AND expiration_date <= date('now', '+30 days')
    ''').fetchone()['count']
    
    expired_certs = conn.execute('''
        SELECT COUNT(*) as count FROM ndt_certifications 
        WHERE status = 'Active' AND expiration_date < date('now')
    ''').fetchone()['count']
    
    level3_review = conn.execute('''
        SELECT COUNT(*) as count FROM ndt_work_orders WHERE status = 'Under Review'
    ''').fetchone()['count']
    
    rejected_pending = conn.execute('''
        SELECT COUNT(*) as count FROM ndt_work_orders 
        WHERE status = 'Rejected' AND disposition IS NULL
    ''').fetchone()['count']
    
    completed_30d = conn.execute('''
        SELECT COUNT(*) as count FROM ndt_work_orders 
        WHERE status = 'Approved' AND actual_end_date >= date('now', '-30 days')
    ''').fetchone()['count']
    
    pass_count = conn.execute('''
        SELECT COUNT(*) as count FROM ndt_inspection_results WHERE result = 'Pass'
    ''').fetchone()['count']
    
    fail_count = conn.execute('''
        SELECT COUNT(*) as count FROM ndt_inspection_results WHERE result = 'Fail'
    ''').fetchone()['count']
    
    total_inspections = pass_count + fail_count
    first_pass_yield = (pass_count / total_inspections * 100) if total_inspections > 0 else 0
    rejection_rate = (fail_count / total_inspections * 100) if total_inspections > 0 else 0
    
    cycle_time = conn.execute('''
        SELECT AVG(julianday(actual_end_date) - julianday(actual_start_date)) as avg_days
        FROM ndt_work_orders 
        WHERE status = 'Approved' AND actual_start_date IS NOT NULL AND actual_end_date IS NOT NULL
    ''').fetchone()['avg_days'] or 0
    
    recent_wo = conn.execute('''
        SELECT nw.*, c.name as customer_name, 
               t.first_name || ' ' || t.last_name as technician_name
        FROM ndt_work_orders nw
        LEFT JOIN customers c ON nw.customer_id = c.id
        LEFT JOIN ndt_technicians t ON nw.assigned_technician_id = t.id
        ORDER BY nw.created_at DESC LIMIT 10
    ''').fetchall()
    
    rejection_by_method = conn.execute('''
        SELECT method, 
               COUNT(*) as total,
               SUM(CASE WHEN result = 'Fail' THEN 1 ELSE 0 END) as failures
        FROM ndt_inspection_results
        GROUP BY method
    ''').fetchall()
    
    technician_utilization = conn.execute('''
        SELECT t.first_name || ' ' || t.last_name as name,
               COUNT(nw.id) as work_orders
        FROM ndt_technicians t
        LEFT JOIN ndt_work_orders nw ON t.id = nw.assigned_technician_id
            AND nw.status NOT IN ('Draft', 'Closed')
        WHERE t.contract_status = 'Active'
        GROUP BY t.id
        ORDER BY work_orders DESC
        LIMIT 5
    ''').fetchall()
    
    conn.close()
    
    return render_template('ndt/dashboard.html',
        open_wo=open_wo,
        in_inspection=in_inspection,
        pending_approval=pending_approval,
        overdue=overdue,
        expiring_certs=expiring_certs,
        expired_certs=expired_certs,
        level3_review=level3_review,
        rejected_pending=rejected_pending,
        completed_30d=completed_30d,
        first_pass_yield=first_pass_yield,
        rejection_rate=rejection_rate,
        avg_cycle_time=cycle_time,
        recent_wo=recent_wo,
        rejection_by_method=rejection_by_method,
        technician_utilization=technician_utilization,
        ndt_methods=NDT_METHODS
    )

@ndt_bp.route('/ndt/technicians')
def technicians_list():
    """List all NDT technicians"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    technicians = conn.execute('''
        SELECT t.*, 
               GROUP_CONCAT(DISTINCT c.method || ' (' || c.level || ')') as certifications,
               MIN(CASE WHEN c.expiration_date < date('now') THEN 1 ELSE 0 END) as has_expired,
               MIN(c.expiration_date) as next_expiry
        FROM ndt_technicians t
        LEFT JOIN ndt_certifications c ON t.id = c.technician_id AND c.status = 'Active'
        GROUP BY t.id
        ORDER BY t.last_name, t.first_name
    ''').fetchall()
    
    conn.close()
    
    return render_template('ndt/technicians_list.html', technicians=technicians)

@ndt_bp.route('/ndt/technicians/new', methods=['GET', 'POST'])
def technician_new():
    """Create new technician"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    if request.method == 'POST':
        db = Database()
        conn = db.get_connection()
        
        tech_number = get_next_technician_number(conn)
        
        conn.execute('''
            INSERT INTO ndt_technicians 
            (technician_number, first_name, last_name, email, phone, employer, contract_status, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            tech_number,
            request.form['first_name'],
            request.form['last_name'],
            request.form.get('email'),
            request.form.get('phone'),
            request.form.get('employer'),
            request.form.get('contract_status', 'Active'),
            request.form.get('notes')
        ))
        
        conn.commit()
        conn.close()
        
        flash(f'Technician {tech_number} created successfully', 'success')
        return redirect(url_for('ndt_routes.technicians_list'))
    
    return render_template('ndt/technician_form.html', technician=None, ndt_methods=NDT_METHODS, levels=CERTIFICATION_LEVELS)

@ndt_bp.route('/ndt/technicians/<int:id>')
def technician_view(id):
    """View technician details"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    technician = conn.execute('SELECT * FROM ndt_technicians WHERE id = ?', (id,)).fetchone()
    if not technician:
        flash('Technician not found', 'error')
        return redirect(url_for('ndt_routes.technicians_list'))
    
    certifications = conn.execute('''
        SELECT * FROM ndt_certifications WHERE technician_id = ?
        ORDER BY method, expiration_date DESC
    ''', (id,)).fetchall()
    
    work_orders = conn.execute('''
        SELECT nw.*, c.name as customer_name
        FROM ndt_work_orders nw
        LEFT JOIN customers c ON nw.customer_id = c.id
        WHERE nw.assigned_technician_id = ?
        ORDER BY nw.created_at DESC LIMIT 20
    ''', (id,)).fetchall()
    
    conn.close()
    
    return render_template('ndt/technician_view.html', 
        technician=technician, 
        certifications=certifications,
        work_orders=work_orders,
        ndt_methods=NDT_METHODS,
        levels=CERTIFICATION_LEVELS
    )

@ndt_bp.route('/ndt/technicians/<int:id>/edit', methods=['GET', 'POST'])
def technician_edit(id):
    """Edit technician"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    technician = conn.execute('SELECT * FROM ndt_technicians WHERE id = ?', (id,)).fetchone()
    if not technician:
        flash('Technician not found', 'error')
        return redirect(url_for('ndt_routes.technicians_list'))
    
    if request.method == 'POST':
        conn.execute('''
            UPDATE ndt_technicians SET
                first_name = ?, last_name = ?, email = ?, phone = ?,
                employer = ?, contract_status = ?, notes = ?
            WHERE id = ?
        ''', (
            request.form['first_name'],
            request.form['last_name'],
            request.form.get('email'),
            request.form.get('phone'),
            request.form.get('employer'),
            request.form.get('contract_status', 'Active'),
            request.form.get('notes'),
            id
        ))
        
        conn.commit()
        conn.close()
        
        flash('Technician updated successfully', 'success')
        return redirect(url_for('ndt_routes.technician_view', id=id))
    
    conn.close()
    
    return render_template('ndt/technician_form.html', technician=technician, ndt_methods=NDT_METHODS, levels=CERTIFICATION_LEVELS)

@ndt_bp.route('/ndt/technicians/<int:id>/certifications/add', methods=['POST'])
def certification_add(id):
    """Add certification to technician"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    db = Database()
    conn = db.get_connection()
    
    conn.execute('''
        INSERT INTO ndt_certifications 
        (technician_id, method, level, certification_number, issued_date, expiration_date, issuing_body, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        id,
        request.form['method'],
        request.form['level'],
        request.form.get('certification_number'),
        request.form.get('issued_date'),
        request.form['expiration_date'],
        request.form.get('issuing_body'),
        request.form.get('notes')
    ))
    
    conn.commit()
    conn.close()
    
    flash('Certification added successfully', 'success')
    return redirect(url_for('ndt_routes.technician_view', id=id))

@ndt_bp.route('/ndt/work-orders')
def wo_list():
    """List NDT work orders"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    status_filter = request.args.get('status', '')
    customer_filter = request.args.get('customer', '')
    method_filter = request.args.get('method', '')
    
    query = '''
        SELECT nw.*, c.name as customer_name, 
               t.first_name || ' ' || t.last_name as technician_name,
               p.code as product_code, p.name as product_name
        FROM ndt_work_orders nw
        LEFT JOIN customers c ON nw.customer_id = c.id
        LEFT JOIN ndt_technicians t ON nw.assigned_technician_id = t.id
        LEFT JOIN products p ON nw.product_id = p.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND nw.status = ?'
        params.append(status_filter)
    if customer_filter:
        query += ' AND nw.customer_id = ?'
        params.append(customer_filter)
    if method_filter:
        query += ' AND nw.ndt_methods LIKE ?'
        params.append(f'%{method_filter}%')
    
    query += ' ORDER BY nw.created_at DESC'
    
    work_orders = conn.execute(query, params).fetchall()
    
    customers = conn.execute('SELECT id, name FROM customers ORDER BY name').fetchall()
    
    conn.close()
    
    return render_template('ndt/wo_list.html',
        work_orders=work_orders,
        customers=customers,
        ndt_methods=NDT_METHODS,
        ndt_statuses=NDT_STATUSES,
        status_filter=status_filter,
        customer_filter=customer_filter,
        method_filter=method_filter
    )

@ndt_bp.route('/ndt/work-orders/new', methods=['GET', 'POST'])
def wo_new():
    """Create new NDT work order"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        wo_number = get_next_ndt_wo_number(conn)
        methods = ','.join(request.form.getlist('ndt_methods'))
        
        conn.execute('''
            INSERT INTO ndt_work_orders 
            (ndt_wo_number, order_type, customer_id, sales_order_id, work_order_id, 
             product_id, serial_number, heat_number, part_description,
             ndt_methods, applicable_code, acceptance_criteria, inspection_location,
             priority, status, planned_start_date, planned_end_date, 
             assigned_technician_id, notes, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Draft', ?, ?, ?, ?, ?)
        ''', (
            wo_number,
            request.form.get('order_type', 'Standalone'),
            request.form.get('customer_id') or None,
            request.form.get('sales_order_id') or None,
            request.form.get('work_order_id') or None,
            request.form.get('product_id') or None,
            request.form.get('serial_number'),
            request.form.get('heat_number'),
            request.form.get('part_description'),
            methods,
            request.form.get('applicable_code'),
            request.form.get('acceptance_criteria'),
            request.form.get('inspection_location'),
            request.form.get('priority', 'Normal'),
            request.form.get('planned_start_date'),
            request.form.get('planned_end_date'),
            request.form.get('assigned_technician_id') or None,
            request.form.get('notes'),
            session['user_id']
        ))
        
        ndt_wo_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        log_status_change(conn, ndt_wo_id, None, 'Draft', session['user_id'], 'NDT Work Order created')
        
        conn.commit()
        conn.close()
        
        flash(f'NDT Work Order {wo_number} created successfully', 'success')
        return redirect(url_for('ndt_routes.wo_view', id=ndt_wo_id))
    
    customers = conn.execute('SELECT id, name FROM customers ORDER BY name').fetchall()
    products = conn.execute('SELECT id, code, name FROM products ORDER BY code').fetchall()
    technicians = conn.execute('''
        SELECT id, technician_number, first_name, last_name FROM ndt_technicians 
        WHERE contract_status = 'Active' ORDER BY last_name
    ''').fetchall()
    sales_orders = conn.execute('''
        SELECT id, so_number FROM sales_orders 
        WHERE status NOT IN ('Closed', 'Cancelled') ORDER BY so_number DESC LIMIT 50
    ''').fetchall()
    work_orders = conn.execute('''
        SELECT id, wo_number FROM work_orders 
        WHERE status NOT IN ('Closed', 'Cancelled', 'Completed') ORDER BY wo_number DESC LIMIT 50
    ''').fetchall()
    
    conn.close()
    
    return render_template('ndt/wo_form.html',
        ndt_wo=None,
        customers=customers,
        products=products,
        technicians=technicians,
        sales_orders=sales_orders,
        work_orders=work_orders,
        ndt_methods=NDT_METHODS,
        ndt_codes=NDT_CODES,
        ndt_statuses=NDT_STATUSES
    )

@ndt_bp.route('/ndt/work-orders/<int:id>/edit', methods=['GET', 'POST'])
def wo_edit(id):
    """Edit NDT work order"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    ndt_wo = conn.execute('SELECT * FROM ndt_work_orders WHERE id = ?', (id,)).fetchone()
    if not ndt_wo:
        flash('NDT Work Order not found', 'error')
        return redirect(url_for('ndt_routes.wo_list'))
    
    if ndt_wo['status'] == 'Closed':
        flash('Cannot edit closed work order', 'error')
        return redirect(url_for('ndt_routes.wo_view', id=id))
    
    if request.method == 'POST':
        methods = ','.join(request.form.getlist('ndt_methods'))
        
        conn.execute('''
            UPDATE ndt_work_orders SET
                order_type = ?, customer_id = ?, sales_order_id = ?, work_order_id = ?,
                product_id = ?, serial_number = ?, heat_number = ?, part_description = ?,
                ndt_methods = ?, applicable_code = ?, acceptance_criteria = ?, 
                inspection_location = ?, priority = ?, planned_start_date = ?, 
                planned_end_date = ?, assigned_technician_id = ?, notes = ?
            WHERE id = ?
        ''', (
            request.form.get('order_type', 'Standalone'),
            request.form.get('customer_id') or None,
            request.form.get('sales_order_id') or None,
            request.form.get('work_order_id') or None,
            request.form.get('product_id') or None,
            request.form.get('serial_number'),
            request.form.get('heat_number'),
            request.form.get('part_description'),
            methods,
            request.form.get('applicable_code'),
            request.form.get('acceptance_criteria'),
            request.form.get('inspection_location'),
            request.form.get('priority', 'Normal'),
            request.form.get('planned_start_date'),
            request.form.get('planned_end_date'),
            request.form.get('assigned_technician_id') or None,
            request.form.get('notes'),
            id
        ))
        
        conn.commit()
        conn.close()
        
        flash('NDT Work Order updated successfully', 'success')
        return redirect(url_for('ndt_routes.wo_view', id=id))
    
    customers = conn.execute('SELECT id, name FROM customers ORDER BY name').fetchall()
    products = conn.execute('SELECT id, code, name FROM products ORDER BY code').fetchall()
    technicians = conn.execute('''
        SELECT id, technician_number, first_name, last_name FROM ndt_technicians 
        WHERE contract_status = 'Active' ORDER BY last_name
    ''').fetchall()
    sales_orders = conn.execute('''
        SELECT id, so_number FROM sales_orders 
        WHERE status NOT IN ('Closed', 'Cancelled') ORDER BY so_number DESC LIMIT 50
    ''').fetchall()
    work_orders = conn.execute('''
        SELECT id, wo_number FROM work_orders 
        WHERE status NOT IN ('Closed', 'Cancelled', 'Completed') ORDER BY wo_number DESC LIMIT 50
    ''').fetchall()
    
    conn.close()
    
    return render_template('ndt/wo_form.html',
        ndt_wo=ndt_wo,
        customers=customers,
        products=products,
        technicians=technicians,
        sales_orders=sales_orders,
        work_orders=work_orders,
        ndt_methods=NDT_METHODS,
        ndt_codes=NDT_CODES,
        ndt_statuses=NDT_STATUSES
    )

@ndt_bp.route('/ndt/work-orders/<int:id>')
def wo_view(id):
    """View NDT work order details"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    ndt_wo = conn.execute('''
        SELECT nw.*, c.name as customer_name, 
               t.first_name || ' ' || t.last_name as technician_name,
               t.technician_number,
               r.first_name || ' ' || r.last_name as reviewer_name,
               p.code as product_code, p.name as product_name,
               so.so_number, wo.wo_number as mfg_wo_number
        FROM ndt_work_orders nw
        LEFT JOIN customers c ON nw.customer_id = c.id
        LEFT JOIN ndt_technicians t ON nw.assigned_technician_id = t.id
        LEFT JOIN ndt_technicians r ON nw.reviewer_id = r.id
        LEFT JOIN products p ON nw.product_id = p.id
        LEFT JOIN sales_orders so ON nw.sales_order_id = so.id
        LEFT JOIN work_orders wo ON nw.work_order_id = wo.id
        WHERE nw.id = ?
    ''', (id,)).fetchone()
    
    if not ndt_wo:
        flash('NDT Work Order not found', 'error')
        return redirect(url_for('ndt_routes.wo_list'))
    
    inspection_results = conn.execute('''
        SELECT ir.*, t.first_name || ' ' || t.last_name as technician_name,
               t.technician_number
        FROM ndt_inspection_results ir
        LEFT JOIN ndt_technicians t ON ir.technician_id = t.id
        WHERE ir.ndt_wo_id = ?
        ORDER BY ir.inspection_date, ir.method
    ''', (id,)).fetchall()
    
    attachments = conn.execute('''
        SELECT * FROM ndt_attachments WHERE ndt_wo_id = ?
        ORDER BY created_at DESC
    ''', (id,)).fetchall()
    
    status_history = conn.execute('''
        SELECT sh.*, u.username
        FROM ndt_status_history sh
        LEFT JOIN users u ON sh.changed_by = u.id
        WHERE sh.ndt_wo_id = ?
        ORDER BY sh.created_at DESC
    ''', (id,)).fetchall()
    
    technicians = conn.execute('''
        SELECT id, technician_number, first_name, last_name FROM ndt_technicians 
        WHERE contract_status = 'Active' ORDER BY last_name
    ''').fetchall()
    
    level3_technicians = conn.execute('''
        SELECT DISTINCT t.id, t.technician_number, t.first_name, t.last_name
        FROM ndt_technicians t
        JOIN ndt_certifications c ON t.id = c.technician_id
        WHERE c.level = 'Level III' AND c.status = 'Active' AND c.expiration_date >= date('now')
        AND t.contract_status = 'Active'
    ''').fetchall()
    
    # Get currently clocked-in employees for this NDT work order
    clocked_in_employees = conn.execute('''
        SELECT tcp.id as punch_id, tcp.punch_time, tcp.notes,
               lr.id as employee_id, lr.first_name, lr.last_name, lr.employee_code
        FROM time_clock_punches tcp
        JOIN labor_resources lr ON tcp.employee_id = lr.id
        WHERE tcp.ndt_work_order_id = ?
          AND tcp.punch_type = 'Clock In'
          AND NOT EXISTS (
              SELECT 1 FROM time_clock_punches tcp2 
              WHERE tcp2.employee_id = tcp.employee_id 
              AND tcp2.punch_type = 'Clock Out'
              AND tcp2.punch_time > tcp.punch_time
          )
        ORDER BY tcp.punch_time DESC
    ''', (id,)).fetchall()
    
    # Get labor resources with NDT skills who can clock in
    ndt_resources = conn.execute('''
        SELECT DISTINCT lr.id, lr.first_name, lr.last_name, lr.employee_code, lr.status
        FROM labor_resources lr
        LEFT JOIN labor_resource_skills lrs ON lr.id = lrs.labor_resource_id
        LEFT JOIN skillsets s ON lrs.skillset_id = s.id
        WHERE lr.status = 'Active'
          AND (
              s.skillset_name LIKE '%NDT%' OR s.skillset_name LIKE '%Ultrasonic%' OR s.skillset_name LIKE '%Radiography%'
              OR s.skillset_name LIKE '%Magnetic Particle%' OR s.skillset_name LIKE '%Liquid Penetrant%'
              OR s.skillset_name LIKE '%Eddy Current%' OR s.skillset_name LIKE '%Visual Inspection%'
              OR lr.skillset LIKE '%NDT%' OR lr.skillset LIKE '%Ultrasonic%' OR lr.skillset LIKE '%Radiography%'
              OR lr.skillset LIKE '%Magnetic Particle%' OR lr.skillset LIKE '%Liquid Penetrant%'
              OR lr.skillset LIKE '%Eddy Current%' OR lr.skillset LIKE '%Visual Inspection%'
          )
        ORDER BY lr.last_name, lr.first_name
    ''').fetchall()
    
    conn.close()
    
    return render_template('ndt/wo_view.html',
        ndt_wo=ndt_wo,
        inspection_results=inspection_results,
        attachments=attachments,
        status_history=status_history,
        technicians=technicians,
        level3_technicians=level3_technicians,
        clocked_in_employees=clocked_in_employees,
        ndt_resources=ndt_resources,
        ndt_methods=NDT_METHODS,
        ndt_statuses=NDT_STATUSES
    )

@ndt_bp.route('/ndt/work-orders/<int:id>/status', methods=['POST'])
def wo_update_status(id):
    """Update NDT work order status"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    new_status = request.form['status']
    reason = request.form.get('reason', '')
    
    db = Database()
    conn = db.get_connection()
    
    ndt_wo = conn.execute('SELECT * FROM ndt_work_orders WHERE id = ?', (id,)).fetchone()
    if not ndt_wo:
        conn.close()
        flash('NDT Work Order not found', 'error')
        return redirect(url_for('ndt_routes.wo_list'))
    
    old_status = ndt_wo['status']
    
    valid_transitions = {
        'Draft': ['Scheduled'],
        'Scheduled': ['In Inspection', 'Draft'],
        'In Inspection': ['Results Recorded'],
        'Results Recorded': ['Under Review'],
        'Under Review': ['Approved', 'Rejected'],
        'Approved': ['Closed'],
        'Rejected': ['Closed', 'Draft'],
        'Closed': []
    }
    
    if new_status not in valid_transitions.get(old_status, []):
        flash(f'Invalid status transition from {old_status} to {new_status}', 'error')
        conn.close()
        return redirect(url_for('ndt_routes.wo_view', id=id))
    
    if new_status == 'Approved':
        results = conn.execute('''
            SELECT ir.*, t.id as tech_id
            FROM ndt_inspection_results ir
            JOIN ndt_technicians t ON ir.technician_id = t.id
            WHERE ir.ndt_wo_id = ?
        ''', (id,)).fetchall()
        
        for result in results:
            if not check_technician_certified(conn, result['tech_id'], result['method'], result['inspection_date']):
                flash(f'Cannot approve: Technician not certified for {result["method"]} on inspection date', 'error')
                conn.close()
                return redirect(url_for('ndt_routes.wo_view', id=id))
    
    update_fields = ['status = ?']
    params = [new_status]
    
    if new_status == 'In Inspection' and not ndt_wo['actual_start_date']:
        update_fields.append('actual_start_date = date("now")')
    
    if new_status in ['Approved', 'Rejected']:
        update_fields.append('actual_end_date = date("now")')
    
    if new_status == 'Rejected':
        update_fields.append('rejection_reason = ?')
        params.append(reason)
    
    if new_status == 'Approved':
        reviewer_id = request.form.get('reviewer_id')
        if reviewer_id:
            update_fields.append('reviewer_id = ?')
            params.append(reviewer_id)
    
    params.append(id)
    
    conn.execute(f'''
        UPDATE ndt_work_orders SET {', '.join(update_fields)} WHERE id = ?
    ''', params)
    
    log_status_change(conn, id, old_status, new_status, session['user_id'], reason)
    
    conn.commit()
    conn.close()
    
    flash(f'Status updated to {new_status}', 'success')
    return redirect(url_for('ndt_routes.wo_view', id=id))

@ndt_bp.route('/ndt/work-orders/<int:id>/results/add', methods=['POST'])
def add_result(id):
    """Add inspection result"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    db = Database()
    conn = db.get_connection()
    
    technician_id = request.form['technician_id']
    method = request.form['method']
    inspection_date = request.form['inspection_date']
    
    if not check_technician_certified(conn, technician_id, method, inspection_date):
        flash(f'Technician is not certified for {method} on the inspection date', 'error')
        conn.close()
        return redirect(url_for('ndt_routes.wo_view', id=id))
    
    conn.execute('''
        INSERT INTO ndt_inspection_results 
        (ndt_wo_id, method, inspection_date, technician_id, equipment_used, 
         calibration_reference, procedure_reference, area_inspected,
         defect_type, defect_size, defect_location, indication_details, result, remarks)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        id,
        method,
        inspection_date,
        technician_id,
        request.form.get('equipment_used'),
        request.form.get('calibration_reference'),
        request.form.get('procedure_reference'),
        request.form.get('area_inspected'),
        request.form.get('defect_type'),
        request.form.get('defect_size'),
        request.form.get('defect_location'),
        request.form.get('indication_details'),
        request.form['result'],
        request.form.get('remarks')
    ))
    
    conn.commit()
    conn.close()
    
    flash('Inspection result added successfully', 'success')
    return redirect(url_for('ndt_routes.wo_view', id=id))

@ndt_bp.route('/ndt/api/certified-technicians')
def api_certified_technicians():
    """API to get certified technicians for a method"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    method = request.args.get('method', '')
    
    db = Database()
    conn = db.get_connection()
    
    technicians = conn.execute('''
        SELECT DISTINCT t.id, t.technician_number, t.first_name, t.last_name, c.level
        FROM ndt_technicians t
        JOIN ndt_certifications c ON t.id = c.technician_id
        WHERE c.method = ? AND c.status = 'Active' AND c.expiration_date >= date('now')
        AND t.contract_status = 'Active'
        ORDER BY t.last_name, t.first_name
    ''', (method,)).fetchall()
    
    conn.close()
    
    return jsonify([{
        'id': t['id'],
        'number': t['technician_number'],
        'name': f"{t['first_name']} {t['last_name']}",
        'level': t['level']
    } for t in technicians])


@ndt_bp.route('/ndt/api/mass-update', methods=['POST'])
def api_mass_update():
    """API endpoint to mass update multiple NDT work orders"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    user_role = session.get('role', '')
    if user_role not in ['Admin', 'Planner']:
        return jsonify({'error': 'Unauthorized'}), 403
    
    data = request.get_json()
    wo_ids = data.get('wo_ids', [])
    updates = data.get('updates', {})
    
    if not wo_ids:
        return jsonify({'success': False, 'error': 'No work orders selected'}), 400
    
    if not updates:
        return jsonify({'success': False, 'error': 'No updates specified'}), 400
    
    db = Database()
    conn = db.get_connection()
    
    try:
        updated_count = 0
        
        for wo_id in wo_ids:
            ndt_wo = conn.execute('SELECT * FROM ndt_work_orders WHERE id = ?', (wo_id,)).fetchone()
            if not ndt_wo:
                continue
            
            update_fields = []
            update_values = []
            
            if 'status' in updates and updates['status']:
                update_fields.append('status = ?')
                update_values.append(updates['status'])
            
            if 'priority' in updates and updates['priority']:
                update_fields.append('priority = ?')
                update_values.append(updates['priority'])
            
            if 'assigned_technician_id' in updates:
                update_fields.append('assigned_technician_id = ?')
                update_values.append(int(updates['assigned_technician_id']) if updates['assigned_technician_id'] else None)
            
            if 'inspection_location' in updates and updates['inspection_location']:
                update_fields.append('inspection_location = ?')
                update_values.append(updates['inspection_location'])
            
            if 'planned_start_date' in updates:
                update_fields.append('planned_start_date = ?')
                update_values.append(updates['planned_start_date'] or None)
            
            if 'planned_end_date' in updates:
                update_fields.append('planned_end_date = ?')
                update_values.append(updates['planned_end_date'] or None)
            
            if update_fields:
                update_values.append(wo_id)
                conn.execute(f'''
                    UPDATE ndt_work_orders 
                    SET {', '.join(update_fields)}
                    WHERE id = ?
                ''', update_values)
                updated_count += 1
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'updated_count': updated_count,
            'message': f'Successfully updated {updated_count} NDT work orders'
        })
        
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500


@ndt_bp.route('/ndt/api/technicians')
def api_technicians():
    """API to get all active technicians for mass update"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    db = Database()
    conn = db.get_connection()
    
    technicians = conn.execute('''
        SELECT id, technician_number, first_name, last_name
        FROM ndt_technicians 
        WHERE contract_status = 'Active'
        ORDER BY last_name, first_name
    ''').fetchall()
    
    conn.close()
    
    return jsonify([{
        'id': t['id'],
        'number': t['technician_number'],
        'name': f"{t['first_name']} {t['last_name']}"
    } for t in technicians])


NDT_INVOICE_STATUSES = ['Draft', 'Pending', 'Sent', 'Partially Paid', 'Paid', 'Overdue', 'Cancelled']

def get_next_ndt_invoice_number(conn):
    """Generate next NDT invoice number"""
    result = conn.execute('''
        SELECT invoice_number FROM ndt_invoices 
        ORDER BY id DESC LIMIT 1
    ''').fetchone()
    if result:
        try:
            num = int(result['invoice_number'].replace('NDT-INV-', ''))
            return f"NDT-INV-{num + 1:05d}"
        except:
            pass
    return "NDT-INV-00001"


@ndt_bp.route('/ndt/invoices')
def invoices_list():
    """List all NDT invoices"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    status_filter = request.args.get('status', '')
    customer_filter = request.args.get('customer', '')
    
    query = '''
        SELECT ni.*, c.name as customer_name,
               nw.ndt_wo_number
        FROM ndt_invoices ni
        LEFT JOIN customers c ON ni.customer_id = c.id
        LEFT JOIN ndt_work_orders nw ON ni.ndt_wo_id = nw.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND ni.status = ?'
        params.append(status_filter)
    
    if customer_filter:
        query += ' AND ni.customer_id = ?'
        params.append(int(customer_filter))
    
    query += ' ORDER BY ni.invoice_date DESC, ni.id DESC'
    
    invoices = conn.execute(query, params).fetchall()
    
    customers = conn.execute('SELECT id, name FROM customers ORDER BY name').fetchall()
    
    # Calculate summary stats
    total_invoices = len(invoices)
    total_amount = sum(inv['total_amount'] or 0 for inv in invoices)
    total_paid = sum(inv['amount_paid'] or 0 for inv in invoices)
    total_outstanding = sum(inv['balance_due'] or 0 for inv in invoices)
    
    overdue_count = conn.execute('''
        SELECT COUNT(*) as count FROM ndt_invoices 
        WHERE status NOT IN ('Paid', 'Cancelled') AND due_date < date('now')
    ''').fetchone()['count']
    
    conn.close()
    
    return render_template('ndt/invoices_list.html',
        invoices=invoices,
        customers=customers,
        statuses=NDT_INVOICE_STATUSES,
        status_filter=status_filter,
        customer_filter=customer_filter,
        total_invoices=total_invoices,
        total_amount=total_amount,
        total_paid=total_paid,
        total_outstanding=total_outstanding,
        overdue_count=overdue_count,
        today=date.today().isoformat()
    )


@ndt_bp.route('/ndt/work-orders/<int:ndt_wo_id>/invoice')
def invoice_from_wo(ndt_wo_id):
    """Create NDT invoice pre-filled from work order"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    ndt_wo = conn.execute('''
        SELECT nw.*, c.name as customer_name, c.id as customer_id
        FROM ndt_work_orders nw
        LEFT JOIN customers c ON nw.customer_id = c.id
        WHERE nw.id = ?
    ''', (ndt_wo_id,)).fetchone()
    
    if not ndt_wo:
        flash('NDT Work Order not found', 'error')
        conn.close()
        return redirect(url_for('ndt_routes.wo_list'))
    
    # Check if invoice already exists for this work order
    existing_invoice = conn.execute('''
        SELECT id, invoice_number FROM ndt_invoices WHERE ndt_wo_id = ?
    ''', (ndt_wo_id,)).fetchone()
    
    if existing_invoice:
        flash(f'Invoice {existing_invoice["invoice_number"]} already exists for this work order', 'warning')
        conn.close()
        return redirect(url_for('ndt_routes.invoice_view', id=existing_invoice['id']))
    
    customers = conn.execute('SELECT id, name FROM customers ORDER BY name').fetchall()
    ndt_work_orders = conn.execute('''
        SELECT nw.id, nw.ndt_wo_number, nw.ndt_methods, nw.part_description,
               nw.serial_number, c.name as customer_name, c.id as customer_id
        FROM ndt_work_orders nw
        LEFT JOIN customers c ON nw.customer_id = c.id
        WHERE nw.status IN ('Approved', 'Closed') OR nw.id = ?
        ORDER BY nw.ndt_wo_number DESC
    ''', (ndt_wo_id,)).fetchall()
    
    conn.close()
    
    # Pre-fill invoice data from work order
    prefilled_invoice = {
        'ndt_wo_id': ndt_wo_id,
        'customer_id': ndt_wo['customer_id'],
        'ndt_methods': ndt_wo['ndt_methods'],
        'part_description': ndt_wo['part_description'],
        'serial_number': ndt_wo['serial_number'],
        'inspection_type': ndt_wo['inspection_type'] if 'inspection_type' in ndt_wo.keys() else '',
    }
    
    return render_template('ndt/invoice_form.html',
        invoice=prefilled_invoice,
        customers=customers,
        ndt_work_orders=ndt_work_orders,
        ndt_methods=NDT_METHODS,
        today=date.today().isoformat(),
        prefilled=True
    )


@ndt_bp.route('/ndt/invoices/new', methods=['GET', 'POST'])
def invoice_new():
    """Create new NDT invoice"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        invoice_number = get_next_ndt_invoice_number(conn)
        invoice_date = request.form.get('invoice_date', date.today().isoformat())
        payment_terms = int(request.form.get('payment_terms', 30))
        
        invoice_date_obj = datetime.strptime(invoice_date, '%Y-%m-%d')
        due_date = (invoice_date_obj + timedelta(days=payment_terms)).strftime('%Y-%m-%d')
        
        subtotal = float(request.form.get('subtotal', 0))
        tax_rate = float(request.form.get('tax_rate', 0))
        tax_amount = subtotal * (tax_rate / 100)
        discount_amount = float(request.form.get('discount_amount', 0))
        total_amount = subtotal + tax_amount - discount_amount
        
        conn.execute('''
            INSERT INTO ndt_invoices 
            (invoice_number, ndt_wo_id, customer_id, invoice_date, due_date, payment_terms,
             status, ndt_methods, part_description, serial_number, inspection_type,
             subtotal, tax_rate, tax_amount, discount_amount, total_amount, balance_due,
             notes, created_by)
            VALUES (?, ?, ?, ?, ?, ?, 'Draft', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            invoice_number,
            request.form.get('ndt_wo_id') or None,
            request.form.get('customer_id') or None,
            invoice_date,
            due_date,
            payment_terms,
            request.form.get('ndt_methods'),
            request.form.get('part_description'),
            request.form.get('serial_number'),
            request.form.get('inspection_type'),
            subtotal,
            tax_rate,
            tax_amount,
            discount_amount,
            total_amount,
            total_amount,
            request.form.get('notes'),
            session['user_id']
        ))
        
        conn.commit()
        conn.close()
        
        flash(f'NDT Invoice {invoice_number} created successfully', 'success')
        return redirect(url_for('ndt_routes.invoices_list'))
    
    customers = conn.execute('SELECT id, name FROM customers ORDER BY name').fetchall()
    ndt_work_orders = conn.execute('''
        SELECT nw.id, nw.ndt_wo_number, nw.ndt_methods, nw.part_description,
               nw.serial_number, c.name as customer_name, c.id as customer_id
        FROM ndt_work_orders nw
        LEFT JOIN customers c ON nw.customer_id = c.id
        WHERE nw.status IN ('Approved', 'Closed')
        ORDER BY nw.ndt_wo_number DESC
    ''').fetchall()
    
    conn.close()
    
    return render_template('ndt/invoice_form.html',
        invoice=None,
        customers=customers,
        ndt_work_orders=ndt_work_orders,
        ndt_methods=NDT_METHODS,
        today=date.today().isoformat()
    )


@ndt_bp.route('/ndt/invoices/<int:id>')
def invoice_view(id):
    """View NDT invoice details"""
    if 'user_id' not in session:
        return redirect(url_for('auth_routes.login'))
    
    db = Database()
    conn = db.get_connection()
    
    invoice = conn.execute('''
        SELECT ni.*, c.name as customer_name, c.billing_address,
               nw.ndt_wo_number, nw.product_id,
               p.name as product_name, p.code as product_code,
               u.username as created_by_name
        FROM ndt_invoices ni
        LEFT JOIN customers c ON ni.customer_id = c.id
        LEFT JOIN ndt_work_orders nw ON ni.ndt_wo_id = nw.id
        LEFT JOIN products p ON nw.product_id = p.id
        LEFT JOIN users u ON ni.created_by = u.id
        WHERE ni.id = ?
    ''', (id,)).fetchone()
    
    if not invoice:
        flash('NDT Invoice not found', 'error')
        return redirect(url_for('ndt_routes.invoices_list'))
    
    conn.close()
    
    return render_template('ndt/invoice_view.html',
        invoice=invoice,
        statuses=NDT_INVOICE_STATUSES
    )


@ndt_bp.route('/ndt/invoices/<int:id>/status', methods=['POST'])
def invoice_update_status(id):
    """Update NDT invoice status"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    new_status = request.form['status']
    
    db = Database()
    conn = db.get_connection()
    
    invoice = conn.execute('SELECT * FROM ndt_invoices WHERE id = ?', (id,)).fetchone()
    if not invoice:
        conn.close()
        flash('NDT Invoice not found', 'error')
        return redirect(url_for('ndt_routes.invoices_list'))
    
    conn.execute('UPDATE ndt_invoices SET status = ? WHERE id = ?', (new_status, id))
    conn.commit()
    conn.close()
    
    flash(f'Invoice status updated to {new_status}', 'success')
    return redirect(url_for('ndt_routes.invoice_view', id=id))


@ndt_bp.route('/ndt/work-orders/<int:id>/clock-in', methods=['POST'])
def wo_clock_in(id):
    """Clock in an NDT resource directly from the work order page"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    employee_id = request.form.get('employee_id')
    notes = request.form.get('notes', '')
    
    if not employee_id:
        flash('Please select an employee', 'error')
        return redirect(url_for('ndt_routes.wo_view', id=id))
    
    db = Database()
    conn = db.get_connection()
    
    ndt_wo = conn.execute('SELECT * FROM ndt_work_orders WHERE id = ?', (id,)).fetchone()
    if not ndt_wo:
        conn.close()
        flash('NDT Work Order not found', 'error')
        return redirect(url_for('ndt_routes.wo_list'))
    
    employee = conn.execute('SELECT * FROM labor_resources WHERE id = ?', (employee_id,)).fetchone()
    if not employee:
        conn.close()
        flash('Employee not found', 'error')
        return redirect(url_for('ndt_routes.wo_view', id=id))
    
    existing_punch = conn.execute('''
        SELECT tcp.id FROM time_clock_punches tcp
        WHERE tcp.employee_id = ?
          AND tcp.punch_type = 'Clock In'
          AND NOT EXISTS (
              SELECT 1 FROM time_clock_punches tcp2 
              WHERE tcp2.employee_id = tcp.employee_id 
              AND tcp2.punch_type = 'Clock Out'
              AND tcp2.punch_time > tcp.punch_time
          )
    ''', (employee_id,)).fetchone()
    
    if existing_punch:
        conn.close()
        flash(f'{employee["first_name"]} {employee["last_name"]} is already clocked in', 'warning')
        return redirect(url_for('ndt_routes.wo_view', id=id))
    
    punch_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Generate punch number
    count = conn.execute('SELECT COUNT(*) FROM time_clock_punches').fetchone()[0]
    punch_number = f"PUNCH-{count + 1:07d}"
    
    conn.execute('''
        INSERT INTO time_clock_punches 
        (punch_number, employee_id, punch_time, punch_type, ndt_work_order_id, notes, location)
        VALUES (?, ?, ?, 'Clock In', ?, ?, 'NDT Work Order')
    ''', (punch_number, employee_id, punch_time, id, notes))
    
    if ndt_wo['status'] == 'Scheduled':
        conn.execute('''
            UPDATE ndt_work_orders SET status = 'In Inspection', actual_start_date = ?
            WHERE id = ? AND status = 'Scheduled'
        ''', (date.today().isoformat(), id))
        
        conn.execute('''
            INSERT INTO ndt_status_history (ndt_wo_id, old_status, new_status, changed_by, notes)
            VALUES (?, 'Scheduled', 'In Inspection', ?, 'Auto-updated on clock in')
        ''', (id, session.get('user_id')))
    
    conn.commit()
    conn.close()
    
    flash(f'{employee["first_name"]} {employee["last_name"]} clocked in successfully', 'success')
    return redirect(url_for('ndt_routes.wo_view', id=id))


@ndt_bp.route('/ndt/work-orders/<int:id>/clock-out', methods=['POST'])
def wo_clock_out(id):
    """Clock out an NDT resource directly from the work order page"""
    if 'user_id' not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    employee_id = request.form.get('employee_id')
    
    if not employee_id:
        flash('Please select an employee', 'error')
        return redirect(url_for('ndt_routes.wo_view', id=id))
    
    db = Database()
    conn = db.get_connection()
    
    employee = conn.execute('SELECT * FROM labor_resources WHERE id = ?', (employee_id,)).fetchone()
    if not employee:
        conn.close()
        flash('Employee not found', 'error')
        return redirect(url_for('ndt_routes.wo_view', id=id))
    
    last_clock_in = conn.execute('''
        SELECT tcp.* FROM time_clock_punches tcp
        WHERE tcp.employee_id = ?
          AND tcp.ndt_work_order_id = ?
          AND tcp.punch_type = 'Clock In'
          AND NOT EXISTS (
              SELECT 1 FROM time_clock_punches tcp2 
              WHERE tcp2.employee_id = tcp.employee_id 
              AND tcp2.punch_type = 'Clock Out'
              AND tcp2.punch_time > tcp.punch_time
          )
        ORDER BY tcp.punch_time DESC LIMIT 1
    ''', (employee_id, id)).fetchone()
    
    if not last_clock_in:
        conn.close()
        flash(f'{employee["first_name"]} {employee["last_name"]} is not clocked in to this work order', 'warning')
        return redirect(url_for('ndt_routes.wo_view', id=id))
    
    punch_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    clock_in_time = datetime.strptime(last_clock_in['punch_time'], '%Y-%m-%d %H:%M:%S')
    hours_worked = (datetime.now() - clock_in_time).total_seconds() / 3600
    
    # Generate punch number
    count = conn.execute('SELECT COUNT(*) FROM time_clock_punches').fetchone()[0]
    punch_number = f"PUNCH-{count + 1:07d}"
    
    conn.execute('''
        INSERT INTO time_clock_punches 
        (punch_number, employee_id, punch_time, punch_type, ndt_work_order_id, location)
        VALUES (?, ?, ?, 'Clock Out', ?, 'NDT Work Order')
    ''', (punch_number, employee_id, punch_time, id))
    
    conn.commit()
    conn.close()
    
    hours_display = f'{int(hours_worked)}h {int((hours_worked % 1) * 60)}m'
    flash(f'{employee["first_name"]} {employee["last_name"]} clocked out ({hours_display})', 'success')
    return redirect(url_for('ndt_routes.wo_view', id=id))
