from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import Database, AuditLogger
from mrp_logic import MRPEngine
from auth import login_required, role_required
from datetime import datetime
from routes.master_routing_routes import apply_routing_to_work_order

workorder_bp = Blueprint('workorder_routes', __name__)

def generate_task_number_wo(conn):
    """Generate unique task number for work order tasks"""
    last_task = conn.execute('SELECT task_number FROM work_order_tasks ORDER BY id DESC LIMIT 1').fetchone()
    if last_task:
        last_number = int(last_task['task_number'].split('-')[1])
        new_number = last_number + 1
    else:
        new_number = 1
    return f'TASK-{new_number:06d}'

@workorder_bp.route('/workorders')
@login_required
def list_workorders():
    db = Database()
    conn = db.get_connection()
    
    # Get filter parameters
    status_filter = request.args.get('status', '')
    disposition_filter = request.args.get('disposition', '')
    priority_filter = request.args.get('priority', '')
    operational_status_filter = request.args.get('operational_status', '')
    customer_filter = request.args.get('customer', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    search = request.args.get('search', '')
    
    # Get sort parameters
    sort_by = request.args.get('sort_by', 'planned_start_date')
    sort_order = request.args.get('sort_order', 'DESC')
    
    # Build dynamic query
    query = '''
        SELECT wo.*, p.code, p.name, c.customer_number, c.name as customer_full_name,
               wos.name as stage_name, wos.color as stage_color
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN customers c ON wo.customer_id = c.id
        LEFT JOIN work_order_stages wos ON wo.stage_id = wos.id
        WHERE 1=1
    '''
    params = []
    
    # Apply filters
    if status_filter:
        query += ' AND wo.status = ?'
        params.append(status_filter)
    
    if disposition_filter:
        query += ' AND wo.disposition = ?'
        params.append(disposition_filter)
    
    if priority_filter:
        query += ' AND wo.priority = ?'
        params.append(priority_filter)
    
    if operational_status_filter:
        query += ' AND wo.operational_status = ?'
        params.append(operational_status_filter)
    
    if customer_filter:
        query += ' AND wo.customer_id = ?'
        params.append(int(customer_filter))
    
    if date_from:
        query += ' AND wo.planned_start_date >= ?'
        params.append(date_from)
    
    if date_to:
        query += ' AND wo.planned_start_date <= ?'
        params.append(date_to)
    
    if search:
        query += ''' AND (wo.wo_number LIKE ? OR p.code LIKE ? OR p.name LIKE ? 
                     OR c.customer_number LIKE ? OR c.name LIKE ?)'''
        search_param = f'%{search}%'
        params.extend([search_param] * 5)
    
    # Validate and apply sorting
    valid_sort_columns = {
        'wo_number': 'wo.wo_number',
        'product': 'p.code',
        'customer': 'c.customer_number',
        'quantity': 'wo.quantity',
        'disposition': 'wo.disposition',
        'status': 'wo.status',
        'operational_status': 'wo.operational_status',
        'priority': 'wo.priority',
        'planned_start_date': 'wo.planned_start_date',
        'planned_end_date': 'wo.planned_end_date'
    }
    
    sort_column = valid_sort_columns.get(sort_by, 'wo.planned_start_date')
    sort_direction = 'ASC' if sort_order.upper() == 'ASC' else 'DESC'
    query += f' ORDER BY {sort_column} {sort_direction}'
    
    workorders = conn.execute(query, params).fetchall()
    
    # Get distinct values for filter dropdowns
    customers = conn.execute('SELECT id, customer_number, name FROM customers ORDER BY customer_number').fetchall()
    statuses = conn.execute('SELECT DISTINCT status FROM work_orders WHERE status IS NOT NULL ORDER BY status').fetchall()
    dispositions = conn.execute('SELECT DISTINCT disposition FROM work_orders WHERE disposition IS NOT NULL ORDER BY disposition').fetchall()
    priorities = conn.execute('SELECT DISTINCT priority FROM work_orders WHERE priority IS NOT NULL ORDER BY priority').fetchall()
    operational_statuses = conn.execute('SELECT DISTINCT operational_status FROM work_orders WHERE operational_status IS NOT NULL ORDER BY operational_status').fetchall()
    stages = conn.execute('SELECT * FROM work_order_stages WHERE is_active = 1 ORDER BY sequence').fetchall()
    
    conn.close()
    
    return render_template('workorders/list.html', 
                         workorders=workorders,
                         customers=customers,
                         statuses=statuses,
                         dispositions=dispositions,
                         priorities=priorities,
                         operational_statuses=operational_statuses,
                         stages=stages,
                         filters={
                             'status': status_filter,
                             'disposition': disposition_filter,
                             'priority': priority_filter,
                             'operational_status': operational_status_filter,
                             'customer': customer_filter,
                             'date_from': date_from,
                             'date_to': date_to,
                             'search': search,
                             'sort_by': sort_by,
                             'sort_order': sort_order
                         })

@workorder_bp.route('/workorders/list-json')
@login_required
def list_workorders_json():
    from flask import jsonify
    db = Database()
    conn = db.get_connection()
    workorders = conn.execute('''
        SELECT wo.id, wo.wo_number, wo.status, p.code as product_code, p.name as product_name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        ORDER BY wo.planned_start_date DESC
    ''').fetchall()
    conn.close()
    return jsonify([dict(wo) for wo in workorders])

@workorder_bp.route('/workorders/create', methods=['GET', 'POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def create_workorder():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        max_attempts = 5
        wo_number = None
        wo_id = None
        
        for attempt in range(max_attempts):
            try:
                last_wo = conn.execute('''
                    SELECT wo_number FROM work_orders 
                    WHERE wo_number LIKE 'WO-%'
                    ORDER BY CAST(SUBSTR(wo_number, 4) AS INTEGER) DESC 
                    LIMIT 1
                ''').fetchone()
                
                if last_wo:
                    try:
                        last_number = int(last_wo['wo_number'].split('-')[1])
                        next_number = last_number + 1
                    except (ValueError, IndexError):
                        next_number = 1
                else:
                    next_number = 1
                
                wo_number = f'WO-{next_number:06d}'
                
                # Get customer_id and populate customer_name from customer record
                customer_id = request.form.get('customer_id')
                customer_name = None
                if customer_id:
                    customer_id = int(customer_id)
                    customer = conn.execute('SELECT name FROM customers WHERE id = ?', (customer_id,)).fetchone()
                    if customer:
                        customer_name = customer['name']
                else:
                    customer_id = None
                
                stage_id = request.form.get('stage_id')
                stage_id = int(stage_id) if stage_id else None
                
                is_aog = 1 if request.form.get('is_aog') else 0
                
                serial_number = request.form.get('serial_number', '').strip() or None
                
                conn.execute('''
                    INSERT INTO work_orders 
                    (wo_number, product_id, quantity, disposition, status, priority, planned_start_date, planned_end_date, labor_cost, overhead_cost, customer_id, customer_name, operational_status, stage_id, repair_category, workorder_type, is_aog, serial_number)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    wo_number,
                    int(request.form['product_id']),
                    float(request.form['quantity']),
                    request.form.get('disposition', 'Manufacture'),
                    request.form['status'],
                    request.form.get('priority', 'Medium'),
                    request.form.get('planned_start_date'),
                    request.form.get('planned_end_date'),
                    float(request.form.get('labor_cost', 0)),
                    float(request.form.get('overhead_cost', 0)),
                    customer_id,
                    customer_name,
                    request.form.get('operational_status') or None,
                    stage_id,
                    request.form.get('repair_category') or None,
                    request.form.get('workorder_type') or None,
                    is_aog,
                    serial_number
                ))
                
                wo_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
                
                # Track initial stage history if stage is set
                if stage_id:
                    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    conn.execute('''
                        INSERT INTO work_order_stage_history (work_order_id, stage_id, entered_at, changed_by)
                        VALUES (?, ?, ?, ?)
                    ''', (wo_id, stage_id, now, session.get('user_id')))
                
                # Log audit trail
                AuditLogger.log_change(
                    conn=conn,
                    record_type='work_order',
                    record_id=wo_id,
                    action_type='Created',
                    modified_by=session.get('user_id'),
                    ip_address=request.remote_addr,
                    user_agent=request.headers.get('User-Agent')
                )
                
                conn.commit()
                break
                
            except Exception as e:
                if 'UNIQUE constraint failed' in str(e) and attempt < max_attempts - 1:
                    conn.rollback()
                    continue
                else:
                    conn.close()
                    flash(f'Error creating work order: {str(e)}', 'danger')
                    return redirect(url_for('workorder_routes.list_workorders'))
        
        conn.close()
        
        if wo_id:
            product_id = int(request.form['product_id'])
            master_routing_id = request.form.get('master_routing_id')
            if master_routing_id:
                master_routing_id = int(master_routing_id)
            
            routing_applied = None
            routing_error_msg = None
            if master_routing_id or product_id:
                routing_conn = db.get_connection()
                try:
                    routing_applied = apply_routing_to_work_order(
                        routing_conn, 
                        wo_id, 
                        routing_id=master_routing_id, 
                        product_id=product_id
                    )
                    routing_conn.commit()
                except Exception as routing_error:
                    routing_error_msg = str(routing_error)
                    try:
                        routing_conn.rollback()
                    except:
                        pass
                    try:
                        AuditLogger.log_change(
                            conn=routing_conn,
                            record_type='work_order',
                            record_id=wo_id,
                            action_type='Routing Application Failed',
                            modified_by=session.get('user_id'),
                            ip_address=request.remote_addr,
                            user_agent=request.headers.get('User-Agent'),
                            notes=f'Failed to apply master routing: {routing_error_msg}'
                        )
                        routing_conn.commit()
                    except:
                        pass
                finally:
                    try:
                        routing_conn.close()
                    except:
                        pass
            
            mrp = MRPEngine()
            mrp.calculate_requirements(wo_id)
            
            if routing_applied:
                flash(f'Work Order {wo_number} created successfully with master routing applied!', 'success')
            elif routing_error_msg:
                flash(f'Work Order {wo_number} created but routing application failed. Please apply routing manually.', 'warning')
            else:
                flash(f'Work Order {wo_number} created successfully! Material requirements calculated.', 'success')
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        else:
            flash('Failed to create work order after multiple attempts', 'danger')
            return redirect(url_for('workorder_routes.list_workorders'))
    
    products = conn.execute('SELECT * FROM products WHERE product_type="Finished Good" ORDER BY code').fetchall()
    customers = conn.execute('SELECT * FROM customers WHERE status = "Active" ORDER BY name').fetchall()
    stages = conn.execute('SELECT * FROM work_order_stages WHERE is_active = 1 ORDER BY sequence').fetchall()
    master_routings = conn.execute('''
        SELECT id, routing_code, routing_name, routing_type, product_id, status 
        FROM master_routings 
        WHERE status IN ('Active', 'Approved')
        ORDER BY CASE status WHEN 'Active' THEN 1 WHEN 'Approved' THEN 2 END, routing_code
    ''').fetchall()
    
    last_wo = conn.execute('''
        SELECT wo_number FROM work_orders 
        WHERE wo_number LIKE 'WO-%'
        ORDER BY CAST(SUBSTR(wo_number, 4) AS INTEGER) DESC 
        LIMIT 1
    ''').fetchone()
    
    if last_wo:
        try:
            last_number = int(last_wo['wo_number'].split('-')[1])
            next_number = last_number + 1
        except (ValueError, IndexError):
            next_number = 1
    else:
        next_number = 1
    
    next_wo_number = f'WO-{next_number:06d}'
    
    conn.close()
    
    return render_template('workorders/create.html', products=products, customers=customers, stages=stages, next_wo_number=next_wo_number, master_routings=master_routings)

@workorder_bp.route('/workorders/<int:id>')
@login_required
def view_workorder(id):
    db = Database()
    conn = db.get_connection()
    mrp = MRPEngine()
    
    workorder = conn.execute('''
        SELECT wo.*, p.code, p.name, p.unit_of_measure, p.description as product_description,
               c.customer_number, c.name as customer_full_name, c.email as customer_email, c.phone as customer_phone,
               wos.name as stage_name, wos.color as stage_color,
               u.username as ri_inspector_name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN customers c ON wo.customer_id = c.id
        LEFT JOIN work_order_stages wos ON wo.stage_id = wos.id
        LEFT JOIN users u ON wo.ri_inspector_id = u.id
        WHERE wo.id=?
    ''', (id,)).fetchone()
    
    requirements = conn.execute('''
        SELECT 
            mr.*, 
            p.code, 
            p.name, 
            p.unit_of_measure,
            COALESCE(p.cost, 0) as unit_cost,
            COALESCE(
                (SELECT SUM(mi.quantity_issued) 
                 FROM material_issues mi 
                 WHERE mi.work_order_id = mr.work_order_id 
                   AND mi.product_id = mr.product_id), 0
            ) as quantity_issued
        FROM material_requirements mr
        JOIN products p ON mr.product_id = p.id
        WHERE mr.work_order_id=?
    ''', (id,)).fetchall()
    
    all_products = conn.execute('SELECT * FROM products ORDER BY code').fetchall()
    
    task_summary = conn.execute('''
        SELECT 
            COUNT(*) as total_tasks,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed_tasks,
            SUM(CASE WHEN status = 'In Progress' THEN 1 ELSE 0 END) as in_progress_tasks,
            SUM(planned_hours) as total_planned_hours,
            SUM(actual_hours) as total_actual_hours,
            SUM(planned_labor_cost) as total_planned_labor_cost,
            SUM(actual_labor_cost) as total_actual_labor_cost
        FROM work_order_tasks
        WHERE work_order_id = ?
    ''', (id,)).fetchone()
    
    tasks = conn.execute('''
        SELECT 
            wot.*,
            lr.first_name || ' ' || lr.last_name as assigned_to_name,
            wc.name as work_center_name,
            (SELECT COUNT(*) FROM labor_issuance WHERE task_id = wot.id) as labor_count
        FROM work_order_tasks wot
        LEFT JOIN labor_resources lr ON wot.assigned_resource_id = lr.id
        LEFT JOIN work_centers wc ON wot.work_center_id = wc.id
        WHERE wot.work_order_id = ?
        ORDER BY wot.sequence_number, wot.id
    ''', (id,)).fetchall()
    
    task_materials = {}
    task_material_summary = {}
    for task in tasks:
        materials = conn.execute('''
            SELECT tm.*, p.code, p.name, p.unit_of_measure as product_uom,
                   COALESCE(tm.unit_cost, p.cost, 0) as unit_cost
            FROM work_order_task_materials tm
            JOIN products p ON tm.product_id = p.id
            WHERE tm.task_id = ?
            ORDER BY tm.id
        ''', (task['id'],)).fetchall()
        task_materials[task['id']] = materials
        
        total_required = sum(m['required_qty'] or 0 for m in materials)
        total_issued = sum(m['issued_qty'] or 0 for m in materials)
        total_consumed = sum(m['consumed_qty'] or 0 for m in materials)
        task_material_summary[task['id']] = {
            'count': len(materials),
            'total_required': total_required,
            'total_issued': total_issued,
            'total_consumed': total_consumed,
            'shortage': any((m['required_qty'] or 0) > (m['issued_qty'] or 0) for m in materials)
        }
    
    task_templates = conn.execute('''
        SELECT tt.id, tt.template_code, tt.template_name, tt.category,
               (SELECT COUNT(*) FROM task_template_items WHERE template_id = tt.id) as item_count
        FROM task_templates tt
        WHERE tt.status = 'Active'
        ORDER BY tt.template_name
    ''').fetchall()
    
    labor_resources = conn.execute('''
        SELECT id, first_name, last_name, employee_code, role
        FROM labor_resources 
        WHERE status = 'Active'
        ORDER BY first_name, last_name
    ''').fetchall()
    
    documents = conn.execute('''
        SELECT wod.*, u.username as uploader_name FROM work_order_documents wod
        LEFT JOIN users u ON wod.uploaded_by = u.id
        WHERE wod.work_order_id = ? AND wod.is_active = 1
        ORDER BY wod.uploaded_at DESC
    ''', (id,)).fetchall() if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='work_order_documents'").fetchone() else []
    
    notes = conn.execute('''
        SELECT n.*, u.username
        FROM work_order_notes n
        LEFT JOIN users u ON n.created_by = u.id
        WHERE n.work_order_id = ?
        ORDER BY n.created_at DESC
    ''', (id,)).fetchall() if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='work_order_notes'").fetchone() else []
    
    cost_info = mrp.calculate_work_order_cost(id)
    
    misc_cost_data = conn.execute('''
        SELECT 
            COALESCE(SUM(CASE WHEN status = 'Received' THEN total_cost ELSE 0 END), 0) as total_misc_cost,
            COALESCE(SUM(total_cost), 0) as total_all_misc_cost,
            COUNT(CASE WHEN status = 'Pending' THEN 1 END) as pending_count,
            COUNT(*) as total_lines
        FROM purchase_order_service_lines
        WHERE work_order_id = ?
    ''', (id,)).fetchone()
    
    # Include Component Buyout costs in misc/service cost totals
    buyout_cost_data = conn.execute('''
        SELECT 
            COALESCE(SUM(CASE WHEN pol.received_quantity >= pol.quantity THEN pol.quantity * pol.unit_price ELSE 0 END), 0) as received_cost,
            COALESCE(SUM(pol.quantity * pol.unit_price), 0) as all_cost,
            COUNT(CASE WHEN pol.received_quantity < pol.quantity THEN 1 END) as pending_count,
            COUNT(*) as total_lines
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        WHERE po.work_order_id = ? AND po.component_buyout_flag = 1
    ''', (id,)).fetchone()
    
    misc_cost_info = {
        'total_misc_cost': (misc_cost_data['total_misc_cost'] if misc_cost_data else 0) + (buyout_cost_data['received_cost'] if buyout_cost_data else 0),
        'total_all_misc_cost': (misc_cost_data['total_all_misc_cost'] if misc_cost_data else 0) + (buyout_cost_data['all_cost'] if buyout_cost_data else 0),
        'pending_count': (misc_cost_data['pending_count'] if misc_cost_data else 0) + (buyout_cost_data['pending_count'] if buyout_cost_data else 0),
        'total_lines': (misc_cost_data['total_lines'] if misc_cost_data else 0) + (buyout_cost_data['total_lines'] if buyout_cost_data else 0)
    }
    
    # Detailed cost breakdowns for Cost tab (include both WO-level and task-level materials)
    material_cost_details = conn.execute('''
        SELECT code, name, required_quantity, issued_qty, unit_cost, total_cost, inventory_id, 
               material_id, source_type, allocated_quantity, task_id FROM (
            SELECT 
                p.code as code, p.name as name, mr.required_quantity as required_quantity,
                COALESCE((SELECT SUM(mi.quantity_issued) FROM material_issues mi 
                          WHERE mi.work_order_id = mr.work_order_id AND mi.product_id = mr.product_id), 0) as issued_qty,
                p.cost as unit_cost,
                COALESCE((SELECT SUM(mi.quantity_issued) FROM material_issues mi 
                          WHERE mi.work_order_id = mr.work_order_id AND mi.product_id = mr.product_id), 0) * COALESCE(p.cost, 0) as total_cost,
                (SELECT i.id FROM inventory i WHERE i.product_id = mr.product_id LIMIT 1) as inventory_id,
                mr.id as material_id,
                'wo' as source_type,
                mr.allocated_quantity as allocated_quantity,
                NULL as task_id
            FROM material_requirements mr
            JOIN products p ON mr.product_id = p.id
            WHERE mr.work_order_id = ?
            UNION ALL
            SELECT 
                p.code as code, p.name as name, tm.required_qty as required_quantity,
                tm.issued_qty as issued_qty,
                tm.unit_cost as unit_cost,
                tm.issued_qty * COALESCE(tm.unit_cost, 0) as total_cost,
                (SELECT i.id FROM inventory i WHERE i.product_id = tm.product_id LIMIT 1) as inventory_id,
                tm.id as material_id,
                'task' as source_type,
                tm.required_qty as allocated_quantity,
                tm.task_id as task_id
            FROM work_order_task_materials tm
            JOIN work_order_tasks wot ON tm.task_id = wot.id
            JOIN products p ON tm.product_id = p.id
            WHERE wot.work_order_id = ?
        ) combined_materials
        ORDER BY code
    ''', (id, id)).fetchall()
    
    labor_cost_details = conn.execute('''
        SELECT 
            wot.task_name, wot.sequence_number,
            COALESCE(lr.first_name || ' ' || lr.last_name, 'Unassigned') as resource_name,
            wot.planned_hours, wot.actual_hours,
            COALESCE(wot.planned_labor_cost, 0) as planned_labor_cost,
            COALESCE(wot.actual_labor_cost, 0) as actual_labor_cost
        FROM work_order_tasks wot
        LEFT JOIN labor_resources lr ON wot.assigned_resource_id = lr.id
        WHERE wot.work_order_id = ?
        ORDER BY wot.sequence_number
    ''', (id,)).fetchall()
    
    overhead_cost_details = conn.execute('''
        SELECT 
            wot.task_name, wot.sequence_number,
            0 as planned_overhead_cost,
            0 as actual_overhead_cost
        FROM work_order_tasks wot
        WHERE wot.work_order_id = ?
        ORDER BY wot.sequence_number
    ''', (id,)).fetchall()
    
    service_cost_details = conn.execute('''
        SELECT 
            psl.id, psl.service_category as category, psl.description, psl.quantity, psl.unit_cost, psl.total_cost, psl.status,
            po.id as po_id, po.po_number, s.name as supplier_name
        FROM purchase_order_service_lines psl
        JOIN purchase_orders po ON psl.po_id = po.id
        LEFT JOIN suppliers s ON po.supplier_id = s.id
        WHERE psl.work_order_id = ?
        UNION ALL
        SELECT 
            pol.id, 'Component Buyout' as category, pol.description, pol.quantity, pol.unit_price as unit_cost, 
            (pol.quantity * pol.unit_price) as total_cost,
            CASE WHEN pol.received_quantity >= pol.quantity THEN 'Received' ELSE 'Pending' END as status,
            po.id as po_id, po.po_number, s.name as supplier_name
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        LEFT JOIN suppliers s ON po.supplier_id = s.id
        WHERE po.work_order_id = ? AND po.component_buyout_flag = 1
        ORDER BY po_number
    ''', (id, id)).fetchall()
    
    stages = conn.execute('SELECT * FROM work_order_stages WHERE is_active = 1 ORDER BY sequence').fetchall()
    
    # Fetch work order quotes
    wo_quotes = conn.execute('''
        SELECT q.*, u.username as prepared_by_name
        FROM work_order_quotes q
        LEFT JOIN users u ON q.prepared_by = u.id
        WHERE q.work_order_id = ?
        ORDER BY q.created_at DESC
    ''', (id,)).fetchall()
    
    # Fetch work order invoices
    wo_invoices = conn.execute('''
        SELECT i.*, c.name as customer_name,
               COALESCE(i.total_amount, 0) - COALESCE(i.amount_paid, 0) as balance_due
        FROM invoices i
        LEFT JOIN customers c ON i.customer_id = c.id
        WHERE i.wo_id = ?
        ORDER BY i.invoice_date DESC
    ''', (id,)).fetchall()
    
    # Fetch suppliers and customers for Component Buyout
    suppliers = conn.execute('SELECT id, code, name FROM suppliers ORDER BY name').fetchall()
    buyout_customers = conn.execute('SELECT id, customer_number, name FROM customers WHERE status = "Active" ORDER BY name').fetchall()
    
    # Fetch Component Buyout Purchase Orders linked to this work order
    component_buyout_pos = conn.execute('''
        SELECT po.id, po.po_number, po.order_date, po.status, po.notes,
               s.name as supplier_name,
               COALESCE(SUM(pol.quantity * pol.unit_price), 0) as total_amount
        FROM purchase_orders po
        LEFT JOIN suppliers s ON po.supplier_id = s.id
        LEFT JOIN purchase_order_lines pol ON po.id = pol.po_id
        WHERE po.work_order_id = ? AND po.component_buyout_flag = 1
        GROUP BY po.id, po.po_number, po.order_date, po.status, po.notes, s.name
        ORDER BY po.order_date DESC
    ''', (id,)).fetchall()
    
    conn.close()
    
    return render_template('workorders/view.html', 
                         workorder=workorder, 
                         requirements=requirements,
                         cost_info=cost_info,
                         misc_cost_info=misc_cost_info,
                         material_cost_details=material_cost_details,
                         labor_cost_details=labor_cost_details,
                         overhead_cost_details=overhead_cost_details,
                         service_cost_details=service_cost_details,
                         all_products=all_products,
                         task_summary=task_summary,
                         tasks=tasks,
                         task_materials=task_materials,
                         task_material_summary=task_material_summary,
                         task_templates=task_templates,
                         labor_resources=labor_resources,
                         documents=documents,
                         notes=notes,
                         stages=stages,
                         wo_quotes=wo_quotes,
                         wo_invoices=wo_invoices,
                         suppliers=suppliers,
                         buyout_customers=buyout_customers,
                         component_buyout_pos=component_buyout_pos)

@workorder_bp.route('/workorders/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def edit_workorder(id):
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            # Get old record for audit
            old_record = conn.execute('SELECT * FROM work_orders WHERE id=?', (id,)).fetchone()
            
            # Check if work order is completed
            if old_record['status'] == 'Completed':
                flash('Cannot edit a completed work order.', 'danger')
                conn.close()
                return redirect(url_for('workorder_routes.view_workorder', id=id))
            
            # Get customer_id and populate customer_name from customer record
            customer_id = request.form.get('customer_id')
            customer_name = None
            if customer_id:
                customer_id = int(customer_id)
                customer = conn.execute('SELECT name FROM customers WHERE id = ?', (customer_id,)).fetchone()
                if customer:
                    customer_name = customer['name']
            else:
                customer_id = None
            
            stage_id = request.form.get('stage_id')
            stage_id = int(stage_id) if stage_id else None
            
            # Get product description for auto-population if description not provided
            product_id = int(request.form['product_id'])
            description = request.form.get('description', '').strip()
            if not description:
                product = conn.execute('SELECT description FROM products WHERE id = ?', (product_id,)).fetchone()
                if product:
                    description = product['description'] or ''
            
            is_aog = 1 if request.form.get('is_aog') else 0
            
            # Update work order
            conn.execute('''
                UPDATE work_orders 
                SET product_id = ?,
                    quantity = ?,
                    disposition = ?,
                    status = ?,
                    priority = ?,
                    serial_number = ?,
                    description = ?,
                    planned_start_date = ?,
                    planned_end_date = ?,
                    customer_id = ?,
                    customer_name = ?,
                    operational_status = ?,
                    stage_id = ?,
                    repair_category = ?,
                    workorder_type = ?,
                    is_aog = ?
                WHERE id = ?
            ''', (
                product_id,
                float(request.form['quantity']),
                request.form.get('disposition', 'Manufacture'),
                request.form['status'],
                request.form.get('priority', 'Medium'),
                request.form.get('serial_number', '').strip() or None,
                description or None,
                request.form.get('planned_start_date') or None,
                request.form.get('planned_end_date') or None,
                customer_id,
                customer_name,
                request.form.get('operational_status') or None,
                stage_id,
                request.form.get('repair_category') or None,
                request.form.get('workorder_type') or None,
                is_aog,
                id
            ))
            
            # Get new record for audit
            new_record = conn.execute('SELECT * FROM work_orders WHERE id=?', (id,)).fetchone()
            
            # Track stage history if stage changed
            old_stage_id = old_record['stage_id']
            new_stage_id = stage_id
            if old_stage_id != new_stage_id:
                user_id = session.get('user_id')
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                if old_stage_id:
                    conn.execute('''
                        UPDATE work_order_stage_history 
                        SET exited_at = ?,
                            duration_hours = (julianday(?) - julianday(entered_at)) * 24
                        WHERE work_order_id = ? AND stage_id = ? AND exited_at IS NULL
                    ''', (now, now, id, old_stage_id))
                
                if new_stage_id:
                    conn.execute('''
                        INSERT INTO work_order_stage_history (work_order_id, stage_id, entered_at, changed_by)
                        VALUES (?, ?, ?, ?)
                    ''', (id, new_stage_id, now, user_id))
            
            # Log audit trail
            changes = AuditLogger.compare_records(dict(old_record), dict(new_record))
            if changes:
                AuditLogger.log_change(
                    conn=conn,
                    record_type='work_order',
                    record_id=id,
                    action_type='Updated',
                    modified_by=session.get('user_id'),
                    changed_fields=changes,
                    ip_address=request.remote_addr,
                    user_agent=request.headers.get('User-Agent')
                )
            
            # Check if product changed (before committing)
            product_changed = old_record['product_id'] != int(request.form['product_id'])
            
            # Commit the work order changes first
            conn.commit()
            
            # Recalculate material requirements AFTER commit if product changed
            if product_changed:
                # Delete old requirements
                conn.execute('DELETE FROM material_requirements WHERE work_order_id = ?', (id,))
                conn.commit()
                
                # Calculate new requirements (MRPEngine uses its own connection)
                mrp = MRPEngine()
                mrp.calculate_requirements(id)
                
                flash('Work Order updated successfully! Material requirements recalculated.', 'success')
            else:
                flash('Work Order updated successfully!', 'success')
            
            conn.close()
            
            return redirect(url_for('workorder_routes.view_workorder', id=id))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error updating work order: {str(e)}', 'danger')
            return redirect(url_for('workorder_routes.edit_workorder', id=id))
    
    # GET request - show edit form
    workorder = conn.execute('''
        SELECT wo.*, p.code, p.name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.id=?
    ''', (id,)).fetchone()
    
    if not workorder:
        flash('Work Order not found.', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    if workorder['status'] == 'Completed':
        flash('Cannot edit a completed work order.', 'warning')
        conn.close()
        return redirect(url_for('workorder_routes.view_workorder', id=id))
    
    products = conn.execute('SELECT * FROM products WHERE product_type="Finished Good" ORDER BY code').fetchall()
    customers = conn.execute('SELECT * FROM customers WHERE status = "Active" ORDER BY name').fetchall()
    stages = conn.execute('SELECT * FROM work_order_stages WHERE is_active = 1 ORDER BY sequence').fetchall()
    
    conn.close()
    
    return render_template('workorders/edit.html', workorder=workorder, products=products, customers=customers, stages=stages)

@workorder_bp.route('/workorders/<int:id>/update-status', methods=['POST'])
@role_required('Admin', 'Production Staff')
def update_workorder_status(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        # Get old record for audit
        old_record = conn.execute('SELECT * FROM work_orders WHERE id=?', (id,)).fetchone()
        
        new_status = request.form['status']
        
        # Business rule: Work order must be reconciled before closing/completing
        if new_status == 'Completed' and old_record['status'] != 'Completed':
            reconciliation_status = old_record.get('reconciliation_status') or 'Not Reconciled'
            if reconciliation_status != 'Reconciled':
                flash('Work order must be reconciled before it can be completed. Please reconcile first.', 'warning')
                conn.close()
                return redirect(url_for('workorder_routes.view_workorder', id=id))
        
        conn.execute('UPDATE work_orders SET status=? WHERE id=?', (new_status, id))
        
        if new_status == 'Completed':
            conn.execute('UPDATE work_orders SET actual_end_date=CURRENT_DATE WHERE id=?', (id,))
            
            # Get work order details for GL posting
            wo = conn.execute('''
                SELECT wo.*, p.name as product_name, p.code as product_code
                FROM work_orders wo
                JOIN products p ON wo.product_id = p.id
                WHERE wo.id = ?
            ''', (id,)).fetchone()
            
            # Calculate total WIP cost (Material + Labor + Overhead)
            material_cost = wo['material_cost'] or 0
            labor_cost = wo['labor_cost'] or 0
            overhead_cost = wo['overhead_cost'] or 0
            total_wip_cost = material_cost + labor_cost + overhead_cost
            
            # Only post GL entry if there are accumulated costs
            if total_wip_cost > 0:
                # Create GL entry: Transfer WIP to Finished Goods
                # DR: Finished Goods Inventory (1150)
                # CR: WIP - Work in Process (1140)
                gl_lines = [
                    {
                        'account_code': '1150',  # Finished Goods Inventory
                        'debit': total_wip_cost,
                        'credit': 0,
                        'description': f'Completed production - {wo["product_code"]} {wo["product_name"]} ({wo["wo_number"]})'
                    },
                    {
                        'account_code': '1140',  # WIP - Work in Process
                        'debit': 0,
                        'credit': total_wip_cost,
                        'description': f'WIP transferred to FG - {wo["wo_number"]}'
                    }
                ]
                
                from models import GLAutoPost
                from datetime import datetime
                
                GLAutoPost.create_auto_journal_entry(
                    conn=conn,
                    entry_date=datetime.now().strftime('%Y-%m-%d'),
                    description=f'Work Order Completion - {wo["wo_number"]}',
                    transaction_source='Work Order Completion',
                    reference_type='work_order',
                    reference_id=id,
                    lines=gl_lines,
                    created_by=session['user_id']
                )
                
                # Update finished goods inventory
                inventory = conn.execute('''
                    SELECT * FROM inventory WHERE product_id = ?
                ''', (wo['product_id'],)).fetchone()
                
                inventory_id = None
                if inventory:
                    # Update existing inventory
                    new_quantity = inventory['quantity'] + wo['quantity']
                    conn.execute('''
                        UPDATE inventory 
                        SET quantity = ?,
                            last_updated = CURRENT_TIMESTAMP
                        WHERE product_id = ?
                    ''', (new_quantity, wo['product_id']))
                    inventory_id = inventory['id']
                else:
                    # Create new inventory record
                    product = conn.execute('''
                        SELECT unit_of_measure FROM products WHERE id = ?
                    ''', (wo['product_id'],)).fetchone()
                    
                    cursor = conn.execute('''
                        INSERT INTO inventory (product_id, quantity, unit_of_measure, location)
                        VALUES (?, ?, ?, ?)
                    ''', (wo['product_id'], wo['quantity'], 
                          product['unit_of_measure'], 'Finished Goods'))
                    inventory_id = cursor.lastrowid
                
                # Link work order to created/updated inventory record
                if inventory_id:
                    conn.execute('''
                        UPDATE work_orders SET inventory_id = ? WHERE id = ?
                    ''', (inventory_id, id))
                
                # Update product cost based on actual production cost
                unit_cost = total_wip_cost / wo['quantity'] if wo['quantity'] > 0 else 0
                conn.execute('''
                    UPDATE products 
                    SET cost = ?
                    WHERE id = ?
                ''', (unit_cost, wo['product_id']))
                
                flash(f'Work Order completed! Transferred ${total_wip_cost:,.2f} from WIP to Finished Goods.', 'success')
            else:
                flash(f'Work Order status updated to {new_status}!', 'success')
                
        elif new_status == 'In Progress':
            conn.execute('UPDATE work_orders SET actual_start_date=CURRENT_DATE WHERE id=?', (id,))
            flash(f'Work Order status updated to {new_status}!', 'success')
        else:
            flash(f'Work Order status updated to {new_status}!', 'success')
        
        # Get new record for audit
        new_record = conn.execute('SELECT * FROM work_orders WHERE id=?', (id,)).fetchone()
        
        # Log audit trail
        changes = AuditLogger.compare_records(dict(old_record), dict(new_record))
        if changes:
            AuditLogger.log_change(
                conn=conn,
                record_type='work_order',
                record_id=id,
                action_type='Updated',
                modified_by=session.get('user_id'),
                changed_fields=changes,
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        flash(f'Error updating work order: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('workorder_routes.view_workorder', id=id))

@workorder_bp.route('/workorders/<int:id>/receiving-inspection', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def update_receiving_inspection(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        # Get old record for audit
        old_record = conn.execute('SELECT * FROM work_orders WHERE id=?', (id,)).fetchone()
        
        # Get form values
        ri_document_tracing = request.form.get('ri_document_tracing')
        ri_part_identification = request.form.get('ri_part_identification')
        ri_part_matching = request.form.get('ri_part_matching')
        ri_traceability = request.form.get('ri_traceability')
        ri_verified_requirements = request.form.get('ri_verified_requirements')
        ri_visual_damages = request.form.get('ri_visual_damages')
        ri_material_discrepancies = request.form.get('ri_material_discrepancies')
        ri_d100_requirements = request.form.get('ri_d100_requirements')
        pkg_crate_requirement = request.form.get('pkg_crate_requirement')
        pkg_crate_dimensions = request.form.get('pkg_crate_dimensions')
        
        # Crate Requirements Assessment checkboxes
        cra_structural_integrity = 1 if request.form.get('cra_structural_integrity') else 0
        cra_dimensional_fit = 1 if request.form.get('cra_dimensional_fit') else 0
        cra_protection_requirements = 1 if request.form.get('cra_protection_requirements') else 0
        cra_storage_duration = 1 if request.form.get('cra_storage_duration') else 0
        cra_customer_oem_spec = 1 if request.form.get('cra_customer_oem_spec') else 0
        cra_return_shipping = 1 if request.form.get('cra_return_shipping') else 0
        cra_hazmat_handling = 1 if request.form.get('cra_hazmat_handling') else 0
        
        # Update receiving inspection fields
        conn.execute('''
            UPDATE work_orders SET 
                ri_document_tracing = ?,
                ri_part_identification = ?,
                ri_part_matching = ?,
                ri_traceability = ?,
                ri_verified_requirements = ?,
                ri_visual_damages = ?,
                ri_material_discrepancies = ?,
                ri_d100_requirements = ?,
                pkg_crate_requirement = ?,
                pkg_crate_dimensions = ?,
                cra_structural_integrity = ?,
                cra_dimensional_fit = ?,
                cra_protection_requirements = ?,
                cra_storage_duration = ?,
                cra_customer_oem_spec = ?,
                cra_return_shipping = ?,
                cra_hazmat_handling = ?,
                ri_inspector_id = ?,
                ri_inspection_date = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (ri_document_tracing, ri_part_identification, ri_part_matching, 
              ri_traceability, ri_verified_requirements, ri_visual_damages,
              ri_material_discrepancies, ri_d100_requirements, pkg_crate_requirement,
              pkg_crate_dimensions, cra_structural_integrity, cra_dimensional_fit,
              cra_protection_requirements, cra_storage_duration, cra_customer_oem_spec,
              cra_return_shipping, cra_hazmat_handling, session.get('user_id'), id))
        
        # Get new record for audit
        new_record = conn.execute('SELECT * FROM work_orders WHERE id=?', (id,)).fetchone()
        
        # Log audit trail
        changes = AuditLogger.compare_records(dict(old_record), dict(new_record))
        if changes:
            AuditLogger.log_change(
                conn=conn,
                record_type='work_order',
                record_id=id,
                action_type='Updated',
                modified_by=session.get('user_id'),
                changed_fields=changes,
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
        
        # Auto-create "Work Order Crate" material when crate requirement is Yes
        if pkg_crate_requirement == 'Yes':
            # Find or create "Incoming Inspection" task
            incoming_task = conn.execute('''
                SELECT id FROM work_order_tasks 
                WHERE work_order_id = ? AND task_name = 'Incoming Inspection'
            ''', (id,)).fetchone()
            
            if not incoming_task:
                # Create "Incoming Inspection" task
                task_number = generate_task_number_wo(conn)
                cursor = conn.execute('''
                    INSERT INTO work_order_tasks 
                    (task_number, work_order_id, task_name, description, category, 
                     sequence_number, priority, status)
                    VALUES (?, ?, 'Incoming Inspection', 'Incoming inspection task for receiving', 
                            'Inspection', 10, 'High', 'Not Started')
                ''', (task_number, id))
                incoming_task_id = cursor.lastrowid
            else:
                incoming_task_id = incoming_task['id']
            
            # Find or create the "Work Order Crate" product (non-inventory item)
            crate_product = conn.execute('''
                SELECT id FROM products WHERE code = 'WO-CRATE'
            ''').fetchone()
            
            if not crate_product:
                # Create a special non-inventory crate product
                cursor = conn.execute('''
                    INSERT INTO products (code, name, description, unit_of_measure, product_type, cost)
                    VALUES ('WO-CRATE', 'Work Order Crate', 'Non-inventory packaging crate for work orders', 
                            'EA', 'Non-Inventory', 0)
                ''')
                crate_product_id = cursor.lastrowid
            else:
                crate_product_id = crate_product['id']
            
            # Check if "Work Order Crate" material already exists for this task
            existing_crate = conn.execute('''
                SELECT id FROM work_order_task_materials 
                WHERE task_id = ? AND product_id = ?
            ''', (incoming_task_id, crate_product_id)).fetchone()
            
            if not existing_crate:
                # Add "Work Order Crate" material requirement to task materials only
                conn.execute('''
                    INSERT INTO work_order_task_materials 
                    (task_id, product_id, required_qty, unit_of_measure, notes, material_status, created_by)
                    VALUES (?, ?, 1, 'EA', 'Crate requirement for work order packaging', 'Planned', ?)
                ''', (incoming_task_id, crate_product_id, session.get('user_id')))
        
        conn.commit()
        flash('Receiving inspection updated successfully!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error updating receiving inspection: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('workorder_routes.view_workorder', id=id))

@workorder_bp.route('/workorders/<int:id>/management', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def update_workorder_management(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        old_record = conn.execute('SELECT * FROM work_orders WHERE id=?', (id,)).fetchone()
        
        new_status = request.form.get('status')
        stage_id = request.form.get('stage_id')
        stage_id = int(stage_id) if stage_id else None
        disposition = request.form.get('disposition') or None
        repair_category = request.form.get('repair_category') or None
        workorder_type = request.form.get('workorder_type') or None
        
        # Business rule: Work order must be reconciled before closing/completing
        if new_status == 'Completed' and old_record['status'] != 'Completed':
            reconciliation_status = old_record.get('reconciliation_status') or 'Not Reconciled'
            if reconciliation_status != 'Reconciled':
                flash('Work order must be reconciled before it can be completed. Please reconcile first.', 'warning')
                conn.close()
                return redirect(url_for('workorder_routes.view_workorder', id=id))
        
        conn.execute('''
            UPDATE work_orders 
            SET status = ?, stage_id = ?, disposition = ?, repair_category = ?, workorder_type = ?
            WHERE id = ?
        ''', (new_status, stage_id, disposition, repair_category, workorder_type, id))
        
        # Track stage history if stage changed
        old_stage_id = old_record['stage_id']
        if old_stage_id != stage_id:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            user_id = session.get('user_id')
            
            if old_stage_id:
                conn.execute('''
                    UPDATE work_order_stage_history 
                    SET exited_at = ?,
                        duration_hours = (julianday(?) - julianday(entered_at)) * 24
                    WHERE work_order_id = ? AND stage_id = ? AND exited_at IS NULL
                ''', (now, now, id, old_stage_id))
            
            if stage_id:
                conn.execute('''
                    INSERT INTO work_order_stage_history (work_order_id, stage_id, entered_at, changed_by)
                    VALUES (?, ?, ?, ?)
                ''', (id, stage_id, now, user_id))
        
        if new_status == 'Completed' and old_record['status'] != 'Completed':
            conn.execute('UPDATE work_orders SET actual_end_date=CURRENT_DATE WHERE id=?', (id,))
            
            wo = conn.execute('''
                SELECT wo.*, p.name as product_name, p.code as product_code
                FROM work_orders wo
                JOIN products p ON wo.product_id = p.id
                WHERE wo.id = ?
            ''', (id,)).fetchone()
            
            material_cost = wo['material_cost'] or 0
            labor_cost = wo['labor_cost'] or 0
            overhead_cost = wo['overhead_cost'] or 0
            total_wip_cost = material_cost + labor_cost + overhead_cost
            
            if total_wip_cost > 0:
                gl_lines = [
                    {
                        'account_code': '1150',
                        'debit': total_wip_cost,
                        'credit': 0,
                        'description': f'Completed production - {wo["product_code"]} {wo["product_name"]} ({wo["wo_number"]})'
                    },
                    {
                        'account_code': '1140',
                        'debit': 0,
                        'credit': total_wip_cost,
                        'description': f'WIP transferred to FG - {wo["wo_number"]}'
                    }
                ]
                
                from models import GLAutoPost
                from datetime import datetime
                
                GLAutoPost.create_gl_entry(
                    conn=conn,
                    source_module='Manufacturing',
                    source_document=wo['wo_number'],
                    source_id=id,
                    transaction_date=datetime.now().strftime('%Y-%m-%d'),
                    description=f'Work Order Completion - {wo["wo_number"]}',
                    lines=gl_lines,
                    created_by=session.get('user_id')
                )
        
        new_record = conn.execute('SELECT * FROM work_orders WHERE id=?', (id,)).fetchone()
        changes = AuditLogger.compare_records(dict(old_record), dict(new_record))
        if changes:
            AuditLogger.log_change(
                conn=conn,
                record_type='work_order',
                record_id=id,
                action_type='Updated',
                modified_by=session.get('user_id'),
                changed_fields=changes,
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
        
        conn.commit()
        flash('Work order updated successfully!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error updating work order: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('workorder_routes.view_workorder', id=id))

@workorder_bp.route('/workorders/<int:id>/notes', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def update_workorder_notes(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        old_record = conn.execute('SELECT * FROM work_orders WHERE id=?', (id,)).fetchone()
        notes = request.form.get('notes', '').strip()
        
        conn.execute('UPDATE work_orders SET notes = ? WHERE id = ?', (notes if notes else None, id))
        
        new_record = conn.execute('SELECT * FROM work_orders WHERE id=?', (id,)).fetchone()
        changes = AuditLogger.compare_records(dict(old_record), dict(new_record))
        if changes:
            AuditLogger.log_change(
                conn=conn,
                record_type='work_order',
                record_id=id,
                action_type='Updated',
                modified_by=session.get('user_id'),
                changed_fields=changes,
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
        
        conn.commit()
        flash('Notes updated successfully!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error updating notes: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('workorder_routes.view_workorder', id=id))

@workorder_bp.route('/workorders/<int:id>/technical-data', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def update_technical_data(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        # Get old record for audit
        old_record = conn.execute('SELECT * FROM work_orders WHERE id=?', (id,)).fetchone()
        
        # Get form values
        tech_data_reference = request.form.get('tech_data_reference')
        tech_manual_number = request.form.get('tech_manual_number')
        tech_make_model = request.form.get('tech_make_model')
        tech_revision = request.form.get('tech_revision')
        tech_revision_date = request.form.get('tech_revision_date') or None
        tech_capability_code = request.form.get('tech_capability_code')
        tech_release_number = request.form.get('tech_release_number')
        tech_title = request.form.get('tech_title')
        
        # Update technical data fields
        conn.execute('''
            UPDATE work_orders SET 
                tech_data_reference = ?,
                tech_manual_number = ?,
                tech_make_model = ?,
                tech_revision = ?,
                tech_revision_date = ?,
                tech_capability_code = ?,
                tech_release_number = ?,
                tech_title = ?
            WHERE id = ?
        ''', (tech_data_reference, tech_manual_number, tech_make_model,
              tech_revision, tech_revision_date, tech_capability_code,
              tech_release_number, tech_title, id))
        
        # Get new record for audit
        new_record = conn.execute('SELECT * FROM work_orders WHERE id=?', (id,)).fetchone()
        
        # Log audit trail
        changes = AuditLogger.compare_records(dict(old_record), dict(new_record))
        if changes:
            AuditLogger.log_change(
                conn=conn,
                record_type='work_order',
                record_id=id,
                action_type='Updated',
                modified_by=session.get('user_id'),
                changed_fields=changes,
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
        
        conn.commit()
        flash('Technical data updated successfully!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error updating technical data: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('workorder_routes.view_workorder', id=id))

@workorder_bp.route('/workorders/<int:wo_id>/materials/add', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def add_material_requirement(wo_id):
    db = Database()
    conn = db.get_connection()
    
    try:
        product_id = int(request.form['product_id'])
        required_quantity = int(request.form['required_quantity'])
        
        # Check if this material requirement already exists
        existing = conn.execute('''
            SELECT id FROM material_requirements 
            WHERE work_order_id = ? AND product_id = ?
        ''', (wo_id, product_id)).fetchone()
        
        if existing:
            flash('This material is already in the requirements list. Use Edit to update it.', 'warning')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        # Get available quantity from inventory
        inventory = conn.execute('''
            SELECT quantity FROM inventory WHERE product_id = ?
        ''', (product_id,)).fetchone()
        
        available_quantity = inventory['quantity'] if inventory else 0
        shortage_quantity = max(0, required_quantity - available_quantity)
        status = 'Satisfied' if shortage_quantity == 0 else 'Shortage'
        
        conn.execute('''
            INSERT INTO material_requirements 
            (work_order_id, product_id, required_quantity, available_quantity, shortage_quantity, status)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (wo_id, product_id, required_quantity, available_quantity, shortage_quantity, status))
        
        conn.commit()
        flash('Material requirement added successfully!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error adding material requirement: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('workorder_routes.view_workorder', id=wo_id))

@workorder_bp.route('/workorders/<int:wo_id>/materials/<int:req_id>/edit', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def edit_material_requirement(wo_id, req_id):
    db = Database()
    conn = db.get_connection()
    
    try:
        required_quantity = int(request.form['required_quantity'])
        
        # Get current material requirement
        req = conn.execute('''
            SELECT product_id FROM material_requirements WHERE id = ?
        ''', (req_id,)).fetchone()
        
        if not req:
            flash('Material requirement not found.', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        # Get available quantity from inventory
        inventory = conn.execute('''
            SELECT quantity FROM inventory WHERE product_id = ?
        ''', (req['product_id'],)).fetchone()
        
        available_quantity = inventory['quantity'] if inventory else 0
        shortage_quantity = max(0, required_quantity - available_quantity)
        status = 'Satisfied' if shortage_quantity == 0 else 'Shortage'
        
        conn.execute('''
            UPDATE material_requirements 
            SET required_quantity = ?, available_quantity = ?, shortage_quantity = ?, status = ?
            WHERE id = ?
        ''', (required_quantity, available_quantity, shortage_quantity, status, req_id))
        
        conn.commit()
        flash('Material requirement updated successfully!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error updating material requirement: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('workorder_routes.view_workorder', id=wo_id))

@workorder_bp.route('/workorders/<int:wo_id>/materials/<int:req_id>/delete', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def delete_material_requirement(wo_id, req_id):
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('DELETE FROM material_requirements WHERE id = ?', (req_id,))
        conn.commit()
        flash('Material requirement deleted successfully!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting material requirement: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('workorder_routes.view_workorder', id=wo_id))

@workorder_bp.route('/workorders/<int:wo_id>/allocate-material/<int:requirement_id>', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def allocate_material(wo_id, requirement_id):
    """Allocate material to work order"""
    db = Database()
    conn = db.get_connection()
    
    try:
        quantity_to_allocate = float(request.form.get('quantity', 0))
        
        # Get requirement details
        requirement = conn.execute('''
            SELECT mr.*, p.code, p.name
            FROM material_requirements mr
            JOIN products p ON mr.product_id = p.id
            WHERE mr.id = ? AND mr.work_order_id = ?
        ''', (requirement_id, wo_id)).fetchone()
        
        if not requirement:
            flash('Material requirement not found.', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        # Check available inventory
        inventory = conn.execute('''
            SELECT quantity FROM inventory WHERE product_id = ?
        ''', (requirement['product_id'],)).fetchone()
        
        available_qty = inventory['quantity'] if inventory else 0
        current_allocated = requirement['allocated_quantity'] or 0
        
        # Validate allocation
        if quantity_to_allocate <= 0:
            flash('Allocation quantity must be greater than zero.', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        if current_allocated + quantity_to_allocate > requirement['required_quantity']:
            flash(f'Cannot allocate more than required quantity ({requirement["required_quantity"]}).', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        if quantity_to_allocate > available_qty:
            flash(f'Insufficient inventory. Available: {available_qty}, Requested: {quantity_to_allocate}', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        # Update allocated quantity
        new_allocated_qty = current_allocated + quantity_to_allocate
        
        # Determine allocation status
        if new_allocated_qty >= requirement['required_quantity']:
            allocation_status = 'Fully Allocated'
        elif new_allocated_qty > 0:
            allocation_status = 'Partially Allocated'
        else:
            allocation_status = 'Not Allocated'
        
        # Update material requirement
        conn.execute('''
            UPDATE material_requirements
            SET allocated_quantity = ?,
                allocation_status = ?,
                allocated_by = ?,
                allocated_at = ?
            WHERE id = ?
        ''', (new_allocated_qty, allocation_status, session.get('user_id'), datetime.now(), requirement_id))
        
        # Log audit trail
        AuditLogger.log_change(
            conn=conn,
            record_type='material_allocation',
            record_id=requirement_id,
            action_type='Allocated',
            modified_by=session.get('user_id'),
            changed_fields=f'Allocated {quantity_to_allocate} units of {requirement["code"]} to WO',
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        conn.close()
        
        flash(f'Successfully allocated {quantity_to_allocate} units of {requirement["code"]}. Total allocated: {new_allocated_qty}', 'success')
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error allocating material: {str(e)}', 'danger')
    
    return redirect(url_for('workorder_routes.view_workorder', id=wo_id))

@workorder_bp.route('/workorders/<int:wo_id>/deallocate-material/<int:requirement_id>', methods=['POST'])
@role_required('Admin', 'Planner')
def deallocate_material(wo_id, requirement_id):
    """Deallocate material from work order"""
    db = Database()
    conn = db.get_connection()
    
    try:
        quantity_to_deallocate = float(request.form.get('quantity', 0))
        
        # Get requirement details
        requirement = conn.execute('''
            SELECT mr.*, p.code
            FROM material_requirements mr
            JOIN products p ON mr.product_id = p.id
            WHERE mr.id = ? AND mr.work_order_id = ?
        ''', (requirement_id, wo_id)).fetchone()
        
        if not requirement:
            flash('Material requirement not found.', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        current_allocated = requirement['allocated_quantity'] or 0
        issued_qty = requirement['issued_quantity'] or 0
        
        # Validate deallocation
        if quantity_to_deallocate <= 0:
            flash('Deallocation quantity must be greater than zero.', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        if quantity_to_deallocate > (current_allocated - issued_qty):
            flash(f'Cannot deallocate more than allocated but not issued quantity ({current_allocated - issued_qty}).', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        # Update allocated quantity
        new_allocated_qty = current_allocated - quantity_to_deallocate
        
        # Determine allocation status
        if issued_qty > 0:
            allocation_status = 'Partially Issued'
        elif new_allocated_qty >= requirement['required_quantity']:
            allocation_status = 'Fully Allocated'
        elif new_allocated_qty > 0:
            allocation_status = 'Partially Allocated'
        else:
            allocation_status = 'Not Allocated'
        
        # Update material requirement
        conn.execute('''
            UPDATE material_requirements
            SET allocated_quantity = ?,
                allocation_status = ?
            WHERE id = ?
        ''', (new_allocated_qty, allocation_status, requirement_id))
        
        # Log audit trail
        AuditLogger.log_change(
            conn=conn,
            record_type='material_allocation',
            record_id=requirement_id,
            action_type='Deallocated',
            modified_by=session.get('user_id'),
            changed_fields=f'Deallocated {quantity_to_deallocate} units of {requirement["code"]} from WO',
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        conn.close()
        
        flash(f'Successfully deallocated {quantity_to_deallocate} units of {requirement["code"]}.', 'success')
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error deallocating material: {str(e)}', 'danger')
    
    return redirect(url_for('workorder_routes.view_workorder', id=wo_id))

@workorder_bp.route('/workorders/<int:wo_id>/issue-material/<int:requirement_id>', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def issue_material(wo_id, requirement_id):
    """Issue allocated material to work order floor"""
    db = Database()
    conn = db.get_connection()
    
    try:
        quantity_to_issue = float(request.form.get('quantity', 0))
        
        # Get requirement details
        requirement = conn.execute('''
            SELECT mr.*, p.code, p.name, p.cost
            FROM material_requirements mr
            JOIN products p ON mr.product_id = p.id
            WHERE mr.id = ? AND mr.work_order_id = ?
        ''', (requirement_id, wo_id)).fetchone()
        
        if not requirement:
            flash('Material requirement not found.', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        current_allocated = requirement['allocated_quantity'] or 0
        current_issued = requirement['issued_quantity'] or 0
        
        # Validate issuance
        if quantity_to_issue <= 0:
            flash('Issue quantity must be greater than zero.', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        if quantity_to_issue > (current_allocated - current_issued):
            flash(f'Cannot issue more than allocated quantity. Allocated: {current_allocated}, Already Issued: {current_issued}', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        # Check inventory
        inventory = conn.execute('SELECT quantity FROM inventory WHERE product_id = ?', (requirement['product_id'],)).fetchone()
        available_qty = inventory['quantity'] if inventory else 0
        
        if quantity_to_issue > available_qty:
            flash(f'Insufficient inventory. Available: {available_qty}', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        # Update issued quantity
        new_issued_qty = current_issued + quantity_to_issue
        
        # Determine allocation status
        if new_issued_qty >= requirement['required_quantity']:
            allocation_status = 'Fully Issued'
        elif new_issued_qty > 0:
            allocation_status = 'Partially Issued'
        else:
            allocation_status = current_allocated >= requirement['required_quantity'] if current_allocated else 'Not Allocated'
        
        # Update material requirement
        conn.execute('''
            UPDATE material_requirements
            SET issued_quantity = ?,
                allocation_status = ?,
                issued_by = ?,
                issued_at = ?
            WHERE id = ?
        ''', (new_issued_qty, allocation_status, session.get('user_id'), datetime.now(), requirement_id))
        
        # Deduct from inventory
        conn.execute('''
            UPDATE inventory
            SET quantity = quantity - ?
            WHERE product_id = ?
        ''', (quantity_to_issue, requirement['product_id']))
        
        # Post to GL: DR WIP, CR Inventory
        material_cost = quantity_to_issue * (requirement['cost'] or 0)
        
        # DR: WIP (1140)
        conn.execute('''
            INSERT INTO general_ledger (account_id, entry_date, description, debit, credit, reference_type, reference_id, created_by)
            VALUES (?, ?, ?, ?, 0, ?, ?, ?)
        ''', (11, datetime.now().strftime('%Y-%m-%d'), 
              f'Material issued to WO-{wo_id}: {requirement["code"]}',
              material_cost, 'work_order', wo_id, session.get('user_id')))
        
        # CR: Inventory (1100)
        conn.execute('''
            INSERT INTO general_ledger (account_id, entry_date, description, debit, credit, reference_type, reference_id, created_by)
            VALUES (?, ?, ?, 0, ?, ?, ?, ?)
        ''', (1, datetime.now().strftime('%Y-%m-%d'),
              f'Material issued to WO-{wo_id}: {requirement["code"]}',
              material_cost, 'work_order', wo_id, session.get('user_id')))
        
        # Log audit trail
        AuditLogger.log_change(
            conn=conn,
            record_type='material_issuance',
            record_id=requirement_id,
            action_type='Issued',
            modified_by=session.get('user_id'),
            changed_fields=f'Issued {quantity_to_issue} units of {requirement["code"]} to WO',
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        conn.close()
        
        flash(f'Successfully issued {quantity_to_issue} units of {requirement["code"]} to work order floor.', 'success')
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error issuing material: {str(e)}', 'danger')
    
    return redirect(url_for('workorder_routes.view_workorder', id=wo_id))

@workorder_bp.route('/workorders/<int:wo_id>/return-material/<int:requirement_id>', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def return_material(wo_id, requirement_id):
    """Return issued material from work order floor back to inventory"""
    db = Database()
    conn = db.get_connection()
    
    try:
        quantity_to_return = float(request.form.get('quantity', 0))
        
        # Get requirement details
        requirement = conn.execute('''
            SELECT mr.*, p.code, p.name, p.cost
            FROM material_requirements mr
            JOIN products p ON mr.product_id = p.id
            WHERE mr.id = ? AND mr.work_order_id = ?
        ''', (requirement_id, wo_id)).fetchone()
        
        if not requirement:
            flash('Material requirement not found.', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        current_issued = requirement['issued_quantity'] or 0
        
        # Validate return
        if quantity_to_return <= 0:
            flash('Return quantity must be greater than zero.', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        if quantity_to_return > current_issued:
            flash(f'Cannot return more than issued quantity ({current_issued}).', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        # Update issued quantity
        new_issued_qty = current_issued - quantity_to_return
        current_allocated = requirement['allocated_quantity'] or 0
        
        # Determine allocation status
        if new_issued_qty > 0:
            allocation_status = 'Partially Issued'
        elif current_allocated >= requirement['required_quantity']:
            allocation_status = 'Fully Allocated'
        elif current_allocated > 0:
            allocation_status = 'Partially Allocated'
        else:
            allocation_status = 'Not Allocated'
        
        # Update material requirement
        conn.execute('''
            UPDATE material_requirements
            SET issued_quantity = ?,
                allocation_status = ?
            WHERE id = ?
        ''', (new_issued_qty, allocation_status, requirement_id))
        
        # Add back to inventory
        conn.execute('''
            UPDATE inventory
            SET quantity = quantity + ?
            WHERE product_id = ?
        ''', (quantity_to_return, requirement['product_id']))
        
        # Reverse GL posting: DR Inventory, CR WIP
        material_cost = quantity_to_return * (requirement['cost'] or 0)
        
        # DR: Inventory (1100)
        conn.execute('''
            INSERT INTO general_ledger (account_id, entry_date, description, debit, credit, reference_type, reference_id, created_by)
            VALUES (?, ?, ?, ?, 0, ?, ?, ?)
        ''', (1, datetime.now().strftime('%Y-%m-%d'),
              f'Material returned from WO-{wo_id}: {requirement["code"]}',
              material_cost, 'work_order', wo_id, session.get('user_id')))
        
        # CR: WIP (1140)
        conn.execute('''
            INSERT INTO general_ledger (account_id, entry_date, description, debit, credit, reference_type, reference_id, created_by)
            VALUES (?, ?, ?, 0, ?, ?, ?, ?)
        ''', (11, datetime.now().strftime('%Y-%m-%d'),
              f'Material returned from WO-{wo_id}: {requirement["code"]}',
              material_cost, 'work_order', wo_id, session.get('user_id')))
        
        # Log audit trail
        AuditLogger.log_change(
            conn=conn,
            record_type='material_return',
            record_id=requirement_id,
            action_type='Returned',
            modified_by=session.get('user_id'),
            changed_fields=f'Returned {quantity_to_return} units of {requirement["code"]} from WO',
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        conn.close()
        
        flash(f'Successfully returned {quantity_to_return} units of {requirement["code"]} to inventory.', 'success')
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error returning material: {str(e)}', 'danger')
    
    return redirect(url_for('workorder_routes.view_workorder', id=wo_id))

@workorder_bp.route('/workorders/<int:id>/traveler')
@login_required
def work_order_traveler(id):
    from models import CompanySettings
    db = Database()
    conn = db.get_connection()
    
    # Get work order details
    workorder = conn.execute('''
        SELECT wo.*, p.code, p.name, p.unit_of_measure, p.description
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.id=?
    ''', (id,)).fetchone()
    
    if not workorder:
        flash('Work Order not found.', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    # Get material requirements
    requirements = conn.execute('''
        SELECT 
            mr.*, 
            p.code, 
            p.name, 
            p.unit_of_measure,
            COALESCE(
                (SELECT SUM(mi.quantity_issued) 
                 FROM material_issues mi 
                 WHERE mi.work_order_id = mr.work_order_id 
                   AND mi.product_id = mr.product_id), 0
            ) as quantity_issued
        FROM material_requirements mr
        JOIN products p ON mr.product_id = p.id
        WHERE mr.work_order_id=?
        ORDER BY p.code
    ''', (id,)).fetchall()
    
    # Get all tasks for this work order
    tasks = conn.execute('''
        SELECT 
            wot.*,
            (lr.first_name || ' ' || lr.last_name) as assigned_resource_name
        FROM work_order_tasks wot
        LEFT JOIN labor_resources lr ON wot.assigned_resource_id = lr.id
        WHERE wot.work_order_id = ?
        ORDER BY wot.sequence_number, wot.id
    ''', (id,)).fetchall()
    
    # Get company settings
    company_settings = CompanySettings.get_or_create_default()
    
    conn.close()
    
    return render_template('workorders/traveler.html', 
                         workorder=workorder, 
                         requirements=requirements,
                         tasks=tasks,
                         company_settings=company_settings,
                         now=datetime.now)


@workorder_bp.route('/api/workorders/<int:id>/update-stage', methods=['POST'])
@login_required
def api_update_workorder_stage(id):
    """API endpoint to update a single work order's stage"""
    from flask import jsonify
    
    data = request.get_json()
    new_stage_id = data.get('stage_id')
    
    db = Database()
    conn = db.get_connection()
    
    try:
        if new_stage_id:
            new_stage_id = int(new_stage_id)
        else:
            new_stage_id = None
        
        old_wo = conn.execute('SELECT stage_id FROM work_orders WHERE id = ?', (id,)).fetchone()
        old_stage_id = old_wo['stage_id'] if old_wo else None
        
        conn.execute('UPDATE work_orders SET stage_id = ? WHERE id = ?', (new_stage_id, id))
        
        if old_stage_id != new_stage_id:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            user_id = session.get('user_id')
            
            if old_stage_id:
                conn.execute('''
                    UPDATE work_order_stage_history 
                    SET exited_at = ?,
                        duration_hours = (julianday(?) - julianday(entered_at)) * 24
                    WHERE work_order_id = ? AND stage_id = ? AND exited_at IS NULL
                ''', (now, now, id, old_stage_id))
            
            if new_stage_id:
                conn.execute('''
                    INSERT INTO work_order_stage_history (work_order_id, stage_id, entered_at, changed_by)
                    VALUES (?, ?, ?, ?)
                ''', (id, new_stage_id, now, user_id))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True})
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500


@workorder_bp.route('/api/workorders/mass-update', methods=['POST'])
@role_required('Admin', 'Planner')
def api_mass_update_workorders():
    """API endpoint to mass update multiple work orders"""
    from flask import jsonify
    
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
            old_wo = conn.execute('SELECT * FROM work_orders WHERE id = ?', (wo_id,)).fetchone()
            if not old_wo:
                continue
            
            update_fields = []
            update_values = []
            
            if 'status' in updates:
                update_fields.append('status = ?')
                update_values.append(updates['status'])
            
            if 'priority' in updates:
                update_fields.append('priority = ?')
                update_values.append(updates['priority'])
            
            if 'operational_status' in updates:
                update_fields.append('operational_status = ?')
                update_values.append(updates['operational_status'])
            
            if 'disposition' in updates:
                update_fields.append('disposition = ?')
                update_values.append(updates['disposition'])
            
            if 'planned_start_date' in updates:
                update_fields.append('planned_start_date = ?')
                update_values.append(updates['planned_start_date'] or None)
            
            if 'planned_end_date' in updates:
                update_fields.append('planned_end_date = ?')
                update_values.append(updates['planned_end_date'] or None)
            
            new_stage_id = None
            if 'stage_id' in updates:
                new_stage_id = int(updates['stage_id']) if updates['stage_id'] else None
                update_fields.append('stage_id = ?')
                update_values.append(new_stage_id)
            
            if update_fields:
                update_values.append(wo_id)
                conn.execute(f'''
                    UPDATE work_orders 
                    SET {', '.join(update_fields)}
                    WHERE id = ?
                ''', update_values)
                
                if 'stage_id' in updates:
                    old_stage_id = old_wo['stage_id']
                    if old_stage_id != new_stage_id:
                        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        user_id = session.get('user_id')
                        
                        if old_stage_id:
                            conn.execute('''
                                UPDATE work_order_stage_history 
                                SET exited_at = ?,
                                    duration_hours = (julianday(?) - julianday(entered_at)) * 24
                                WHERE work_order_id = ? AND stage_id = ? AND exited_at IS NULL
                            ''', (now, now, wo_id, old_stage_id))
                        
                        if new_stage_id:
                            conn.execute('''
                                INSERT INTO work_order_stage_history (work_order_id, stage_id, entered_at, changed_by)
                                VALUES (?, ?, ?, ?)
                            ''', (wo_id, new_stage_id, now, user_id))
                
                new_wo = conn.execute('SELECT * FROM work_orders WHERE id = ?', (wo_id,)).fetchone()
                
                AuditLogger.log_change(
                    'work_orders',
                    wo_id,
                    'UPDATE',
                    session.get('user_id'),
                    dict(old_wo) if old_wo else {},
                    dict(new_wo) if new_wo else {}
                )
                
                updated_count += 1
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'updated_count': updated_count,
            'message': f'Successfully updated {updated_count} work orders'
        })
        
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500


# Work Order Stages Management Routes
@workorder_bp.route('/workorders/stages')
@role_required('Admin')
def list_stages():
    """List all work order stages"""
    db = Database()
    conn = db.get_connection()
    
    stages = conn.execute('''
        SELECT wos.*, 
               (SELECT COUNT(*) FROM work_orders WHERE stage_id = wos.id) as usage_count
        FROM work_order_stages wos
        ORDER BY wos.sequence, wos.name
    ''').fetchall()
    
    conn.close()
    return render_template('workorders/stages.html', stages=stages)


@workorder_bp.route('/workorders/stages/create', methods=['POST'])
@role_required('Admin')
def create_stage():
    """Create a new work order stage"""
    from flask import jsonify
    
    db = Database()
    conn = db.get_connection()
    
    try:
        name = request.form.get('name', '').strip()
        description = request.form.get('description', '').strip()
        color = request.form.get('color', '#6c757d')
        
        if not name:
            flash('Stage name is required', 'error')
            return redirect(url_for('workorder_routes.list_stages'))
        
        # Get next sequence number
        max_seq = conn.execute('SELECT MAX(sequence) as max_seq FROM work_order_stages').fetchone()
        sequence = (max_seq['max_seq'] or 0) + 1
        
        conn.execute('''
            INSERT INTO work_order_stages (name, description, color, sequence, is_active)
            VALUES (?, ?, ?, ?, 1)
        ''', (name, description, color, sequence))
        
        conn.commit()
        flash(f'Stage "{name}" created successfully', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error creating stage: {str(e)}', 'error')
    
    conn.close()
    return redirect(url_for('workorder_routes.list_stages'))


@workorder_bp.route('/workorders/stages/<int:id>/update', methods=['POST'])
@role_required('Admin')
def update_stage(id):
    """Update a work order stage"""
    db = Database()
    conn = db.get_connection()
    
    try:
        name = request.form.get('name', '').strip()
        description = request.form.get('description', '').strip()
        color = request.form.get('color', '#6c757d')
        sequence = request.form.get('sequence', 0)
        is_active = 1 if request.form.get('is_active') else 0
        
        if not name:
            flash('Stage name is required', 'error')
            return redirect(url_for('workorder_routes.list_stages'))
        
        conn.execute('''
            UPDATE work_order_stages 
            SET name = ?, description = ?, color = ?, sequence = ?, is_active = ?
            WHERE id = ?
        ''', (name, description, color, sequence, is_active, id))
        
        conn.commit()
        flash(f'Stage "{name}" updated successfully', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error updating stage: {str(e)}', 'error')
    
    conn.close()
    return redirect(url_for('workorder_routes.list_stages'))


@workorder_bp.route('/workorders/stages/<int:id>/delete', methods=['POST'])
@role_required('Admin')
def delete_stage(id):
    """Delete a work order stage"""
    db = Database()
    conn = db.get_connection()
    
    try:
        # Check if stage is in use
        usage = conn.execute('SELECT COUNT(*) as count FROM work_orders WHERE stage_id = ?', (id,)).fetchone()
        
        if usage['count'] > 0:
            flash(f'Cannot delete stage - it is used by {usage["count"]} work orders', 'error')
        else:
            stage = conn.execute('SELECT name FROM work_order_stages WHERE id = ?', (id,)).fetchone()
            conn.execute('DELETE FROM work_order_stages WHERE id = ?', (id,))
            conn.commit()
            flash(f'Stage "{stage["name"]}" deleted successfully', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting stage: {str(e)}', 'error')
    
    conn.close()
    return redirect(url_for('workorder_routes.list_stages'))


@workorder_bp.route('/api/workorder-stages')
@workorder_bp.route('/api/workorders/stages')
@login_required
def api_list_stages():
    """API endpoint to get all active stages"""
    from flask import jsonify
    
    db = Database()
    conn = db.get_connection()
    
    stages = conn.execute('''
        SELECT id, name, description, color, sequence
        FROM work_order_stages
        WHERE is_active = 1
        ORDER BY sequence, name
    ''').fetchall()
    
    conn.close()
    
    return jsonify({
        'success': True,
        'stages': [dict(s) for s in stages]
    })


@workorder_bp.route('/workorders/<int:id>/release-to-shipping', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def release_wo_to_shipping(id):
    """Release completed Work Order to Pending Shipments"""
    db = Database()
    conn = db.get_connection()
    
    try:
        # Get work order details with customer and product info
        wo = conn.execute('''
            SELECT wo.*, p.code as product_code, p.name as product_name,
                   c.name as customer_name, c.customer_number, c.shipping_address, c.billing_address
            FROM work_orders wo
            JOIN products p ON wo.product_id = p.id
            LEFT JOIN customers c ON wo.customer_id = c.id
            WHERE wo.id = ?
        ''', (id,)).fetchone()
        
        if not wo:
            flash('Work Order not found', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.list_workorders'))
        
        # Only Completed work orders can be released to shipping
        if wo['status'] != 'Completed':
            flash('Only Completed work orders can be released to shipping.', 'warning')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=id))
        
        # Check if already released
        existing = conn.execute('''
            SELECT id FROM shipments 
            WHERE reference_type = 'Work Order' AND reference_id = ? AND status IN ('Pending', 'Shipped')
        ''', (id,)).fetchone()
        
        if existing:
            flash('This work order has already been released to shipping.', 'warning')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=id))
        
        # Generate shipment number
        last_shipment = conn.execute(
            'SELECT shipment_number FROM shipments ORDER BY id DESC LIMIT 1'
        ).fetchone()
        
        if last_shipment and last_shipment['shipment_number']:
            try:
                last_num = int(last_shipment['shipment_number'].split('-')[1])
                shipment_number = f'SHIP-{last_num + 1:05d}'
            except:
                shipment_number = 'SHIP-00001'
        else:
            shipment_number = 'SHIP-00001'
        
        # Get ship-to info from customer or work order
        ship_to_name = wo['customer_name'] or ''
        ship_to_address = wo['shipping_address'] or wo['billing_address'] or ''
        
        # Create pending shipment record
        cursor = conn.execute('''
            INSERT INTO shipments (
                shipment_number, shipment_type, reference_type, reference_id,
                status, shipment_stage, ship_to_name, ship_to_address,
                released_by, released_at, created_by, created_at
            ) VALUES (?, 'Outbound', 'Work Order', ?, 'Pending', 'Pending',
                      ?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP)
        ''', (
            shipment_number, id,
            ship_to_name, ship_to_address,
            session.get('user_id'), session.get('user_id')
        ))
        
        shipment_id = cursor.lastrowid
        
        # Auto-populate shipment line with work order product
        product = conn.execute('''
            SELECT p.id, p.code, p.name
            FROM products p
            WHERE p.id = ?
        ''', (wo['product_id'],)).fetchone()
        
        if product:
            conn.execute('''
                INSERT INTO shipment_lines (
                    shipment_id, line_number, product_id, quantity_shipped,
                    serial_number, lot_number, condition, notes
                ) VALUES (?, 1, ?, ?, ?, ?, 'New', ?)
            ''', (
                shipment_id, product['id'], wo['quantity'] or 1,
                wo.get('serial_number', '') or '', wo.get('lot_number', '') or '',
                f"From WO {wo['wo_number']}: {product['code']} - {product['name']}"
            ))
        
        # Update work order disposition to indicate released to shipping
        conn.execute('''
            UPDATE work_orders 
            SET disposition = 'Released to Shipping'
            WHERE id = ?
        ''', (id,))
        
        # Log activity
        from models import AuditLogger
        AuditLogger.log_change(
            conn=conn,
            record_type='work_orders',
            record_id=id,
            action_type='UPDATE',
            modified_by=session.get('user_id'),
            changed_fields={'disposition': {'old': wo['disposition'], 'new': 'Released to Shipping'}},
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        flash(f'Work Order {wo["wo_number"]} released to shipping! Shipment {shipment_number} created.', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error releasing to shipping: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('workorder_routes.view_workorder', id=id))


@workorder_bp.route('/workorders/<int:id>/turn-into-stock', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def turn_into_stock(id):
    """Explicitly turn completed Work Order into stock (updates disposition)"""
    db = Database()
    conn = db.get_connection()
    
    try:
        # Get work order details
        wo = conn.execute('''
            SELECT wo.*, p.code as product_code, p.name as product_name
            FROM work_orders wo
            JOIN products p ON wo.product_id = p.id
            WHERE wo.id = ?
        ''', (id,)).fetchone()
        
        if not wo:
            flash('Work Order not found', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.list_workorders'))
        
        # Only Completed work orders can be turned into stock
        if wo['status'] != 'Completed':
            flash('Only Completed work orders can be turned into stock.', 'warning')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=id))
        
        # Check if already in stock
        if wo['disposition'] == 'Turned into Stock':
            flash('This work order has already been turned into stock.', 'info')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=id))
        
        # Calculate total work order cost
        material_cost = float(wo['material_cost'] or 0)
        labor_cost = float(wo['labor_cost'] or 0)
        overhead_cost = float(wo['overhead_cost'] or 0)
        
        # Get misc/service PO costs (received only)
        misc_cost_result = conn.execute('''
            SELECT COALESCE(SUM(psl.total_cost), 0) as misc_cost
            FROM purchase_order_service_lines psl
            WHERE psl.work_order_id = ? AND psl.status = 'Received'
        ''', (id,)).fetchone()
        misc_cost = float(misc_cost_result['misc_cost'] or 0) if misc_cost_result else 0
        
        total_wo_cost = material_cost + labor_cost + overhead_cost + misc_cost
        wo_quantity = float(wo['quantity'] or 1)
        
        # Unit cost = total work order cost (not divided by quantity)
        unit_cost = total_wo_cost
        
        # Create inventory record with work order quantity and total cost as unit cost
        serial_number = wo.get('serial_number') or None
        is_serialized = 1 if serial_number else 0
        
        cursor = conn.execute('''
            INSERT INTO inventory (
                product_id, quantity, unit_cost, condition, status, 
                warehouse_location, is_serialized, serial_number, last_received_date
            ) VALUES (?, ?, ?, 'New', 'Available', 'Main', ?, ?, date('now'))
        ''', (wo['product_id'], wo_quantity, unit_cost, is_serialized, serial_number))
        
        inventory_id = cursor.lastrowid
        
        # Link work order to inventory
        conn.execute('''
            UPDATE work_orders 
            SET disposition = 'Turned into Stock', inventory_id = ?
            WHERE id = ?
        ''', (inventory_id, id))
        
        # Log activity
        from models import AuditLogger
        AuditLogger.log_change(
            conn=conn,
            record_type='work_orders',
            record_id=id,
            action_type='UPDATE',
            modified_by=session.get('user_id'),
            changed_fields={
                'disposition': {'old': wo['disposition'], 'new': 'Turned into Stock'},
                'inventory_id': {'old': None, 'new': inventory_id}
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        # Also log inventory creation
        AuditLogger.log(conn, 'inventory', inventory_id, 'CREATE',
                       {'product_id': wo['product_id'], 'quantity': wo_quantity, 
                        'unit_cost': unit_cost, 'from_work_order': wo['wo_number']},
                       session.get('user_id'))
        
        conn.commit()
        flash(f'Work Order {wo["wo_number"]} turned into stock. Created inventory with Qty: {wo_quantity}, Unit Cost: ${unit_cost:.2f}', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error updating work order: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('workorder_routes.view_workorder', id=id))


@workorder_bp.route('/workorders/<int:id>/generate-8130', methods=['GET', 'POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def generate_8130(id):
    """Generate FAA Form 8130-3 for completed Work Order"""
    db = Database()
    conn = db.get_connection()
    
    # Get work order details
    wo = conn.execute('''
        SELECT wo.*, p.code as product_code, p.name as product_name,
               c.name as customer_name,
               cs.company_name, cs.address_line1 as company_address, 
               cs.city as company_city, cs.state as company_state, cs.postal_code as company_zip
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN customers c ON wo.customer_id = c.id
        LEFT JOIN company_settings cs ON cs.id = 1
        WHERE wo.id = ?
    ''', (id,)).fetchone()
    
    if not wo:
        flash('Work Order not found', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    if wo['status'] != 'Completed':
        flash('Only completed work orders can have 8130 certificates generated.', 'warning')
        conn.close()
        return redirect(url_for('workorder_routes.view_workorder', id=id))
    
    # Check for existing certificate
    existing_cert = conn.execute('''
        SELECT * FROM faa_8130_certificates 
        WHERE work_order_id = ? AND status = 'Issued'
    ''', (id,)).fetchone()
    
    if request.method == 'POST':
        if existing_cert:
            flash(f'Certificate {existing_cert["certificate_number"]} already exists for this work order.', 'warning')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=id))
        
        try:
            from services.faa8130_service import FAA8130Service
            
            form_data = {
                'issuing_authority': request.form.get('issuing_authority', 'FAA / United States'),
                'organization_name': request.form.get('organization_name', ''),
                'organization_address': request.form.get('organization_address', ''),
                'serial_number': request.form.get('serial_number', ''),
                'batch_number': request.form.get('batch_number', ''),
                'status_work': request.form.get('status_work', wo['disposition'] or 'Overhauled'),
                'approval_number': request.form.get('approval_number', ''),
                'remarks': request.form.get('remarks', ''),
                'certifier_name': request.form.get('certifier_name', ''),
                'certifier_certificate_number': request.form.get('certifier_certificate_number', ''),
                'certifier_signature_date': request.form.get('certifier_signature_date', datetime.now().strftime('%Y-%m-%d')),
                'authorized_signature_name': request.form.get('authorized_signature_name', ''),
                'authorized_signature_date': request.form.get('authorized_signature_date', datetime.now().strftime('%Y-%m-%d')),
            }
            
            result = FAA8130Service.create_certificate(conn, id, form_data, session.get('user_id'))
            conn.commit()
            
            flash(f'FAA Form 8130-3 Certificate {result["certificate_number"]} generated successfully!', 'success')
            conn.close()
            return redirect(url_for('workorder_routes.view_8130', id=id))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error generating certificate: {str(e)}', 'danger')
            conn.close()
            return redirect(url_for('workorder_routes.generate_8130', id=id))
    
    # Build organization address from company settings
    company = conn.execute('SELECT * FROM company_settings LIMIT 1').fetchone()
    org_address = ''
    if company and company['address_line1']:
        org_address = f"{company['address_line1']}, {company['city'] or ''}, {company['state'] or ''} {company['postal_code'] or ''}"
    
    conn.close()
    return render_template('workorders/generate_8130.html', 
                          workorder=wo,
                          existing_cert=existing_cert,
                          org_address=org_address)


@workorder_bp.route('/workorders/<int:id>/view-8130')
@login_required
def view_8130(id):
    """View existing FAA Form 8130-3 certificate for Work Order"""
    db = Database()
    conn = db.get_connection()
    
    # Get work order details
    wo = conn.execute('''
        SELECT wo.*, p.code as product_code, p.name as product_name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.id = ?
    ''', (id,)).fetchone()
    
    if not wo:
        flash('Work Order not found', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    # Get certificate
    certificate = conn.execute('''
        SELECT c.*, u.username as created_by_name
        FROM faa_8130_certificates c
        LEFT JOIN users u ON c.created_by = u.id
        WHERE c.work_order_id = ? AND c.status = 'Issued'
        ORDER BY c.created_at DESC
        LIMIT 1
    ''', (id,)).fetchone()
    
    conn.close()
    
    if not certificate:
        flash('No 8130 certificate found for this work order.', 'warning')
        return redirect(url_for('workorder_routes.generate_8130', id=id))
    
    return render_template('workorders/view_8130.html', 
                          workorder=wo,
                          certificate=certificate)


@workorder_bp.route('/workorders/<int:id>/download-8130')
@login_required
def download_8130(id):
    """Download the 8130 PDF file"""
    from flask import send_file
    
    db = Database()
    conn = db.get_connection()
    
    certificate = conn.execute('''
        SELECT * FROM faa_8130_certificates 
        WHERE work_order_id = ? AND status = 'Issued'
        ORDER BY created_at DESC LIMIT 1
    ''', (id,)).fetchone()
    
    conn.close()
    
    if not certificate or not certificate['pdf_file_path']:
        flash('Certificate PDF not found.', 'danger')
        return redirect(url_for('workorder_routes.view_workorder', id=id))
    
    import os
    if os.path.exists(certificate['pdf_file_path']):
        return send_file(
            certificate['pdf_file_path'],
            as_attachment=True,
            download_name=f"{certificate['certificate_number']}.pdf"
        )
    else:
        flash('Certificate PDF file not found on server.', 'danger')
        return redirect(url_for('workorder_routes.view_workorder', id=id))


@workorder_bp.route('/workorders/<int:id>/create-service-po', methods=['GET', 'POST'])
@login_required
@role_required('Admin', 'Procurement Staff', 'Production Staff')
def create_service_po(id):
    """Create a Purchase Order for miscellaneous charges/services linked to a work order"""
    db = Database()
    conn = db.get_connection()
    
    wo = conn.execute('''
        SELECT wo.*, p.code as product_code, p.name as product_name, c.name as customer_name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN customers c ON wo.customer_id = c.id
        WHERE wo.id = ?
    ''', (id,)).fetchone()
    
    if not wo:
        flash('Work Order not found', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    if request.method == 'POST':
        supplier_id = request.form.get('supplier_id')
        expected_delivery_date = request.form.get('expected_delivery_date')
        notes = request.form.get('notes', '')
        
        service_categories = request.form.getlist('service_category[]')
        descriptions = request.form.getlist('description[]')
        quantities = request.form.getlist('quantity[]')
        unit_costs = request.form.getlist('unit_cost[]')
        
        if not supplier_id:
            flash('Please select a supplier', 'danger')
            suppliers = conn.execute('SELECT * FROM suppliers ORDER BY name').fetchall()
            conn.close()
            return render_template('workorders/create_service_po.html', 
                                 workorder=wo, suppliers=suppliers)
        
        if not service_categories or not any(service_categories):
            flash('Please add at least one service line', 'danger')
            suppliers = conn.execute('SELECT * FROM suppliers ORDER BY name').fetchall()
            conn.close()
            return render_template('workorders/create_service_po.html',
                                 workorder=wo, suppliers=suppliers)
        
        try:
            last_po = conn.execute('''
                SELECT po_number FROM purchase_orders 
                ORDER BY id DESC LIMIT 1
            ''').fetchone()
            
            if last_po:
                try:
                    last_num = int(last_po['po_number'].replace('PO', ''))
                    po_number = f"PO{last_num + 1:05d}"
                except:
                    po_number = f"PO{datetime.now().strftime('%Y%m%d%H%M%S')}"
            else:
                po_number = "PO00001"
            
            cursor = conn.execute('''
                INSERT INTO purchase_orders (po_number, supplier_id, status, order_date, 
                                            expected_delivery_date, notes, work_order_id, po_type)
                VALUES (?, ?, 'Draft', ?, ?, ?, ?, 'Service')
            ''', (po_number, supplier_id, datetime.now().strftime('%Y-%m-%d'),
                  expected_delivery_date, notes, id))
            po_id = cursor.lastrowid
            
            total_amount = 0
            for i, (cat, desc, qty, unit_cost) in enumerate(zip(service_categories, descriptions, quantities, unit_costs)):
                if cat and desc and qty and unit_cost:
                    qty = float(qty)
                    cost = float(unit_cost)
                    line_total = qty * cost
                    total_amount += line_total
                    
                    conn.execute('''
                        INSERT INTO purchase_order_service_lines 
                        (po_id, work_order_id, line_number, service_category, description,
                         quantity, unit_cost, total_cost, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'Pending')
                    ''', (po_id, id, i + 1, cat, desc, qty, cost, line_total))
            
            AuditLogger.log(conn, 'purchase_orders', po_id, 'CREATE',
                           {'po_number': po_number, 'type': 'Service', 'work_order_id': id, 
                            'total_amount': total_amount},
                           session.get('user_id'))
            
            conn.commit()
            flash(f'Service Purchase Order {po_number} created successfully', 'success')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=id))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error creating Service PO: {str(e)}', 'danger')
    
    suppliers = conn.execute('SELECT * FROM suppliers ORDER BY name').fetchall()
    
    service_categories = [
        'Outside Processing',
        'Heat Treatment',
        'Plating/Coating',
        'Testing/Inspection',
        'Machining',
        'NDT Services',
        'Calibration',
        'Engineering Services',
        'Expedite Fee',
        'Freight/Shipping',
        'Tooling',
        'Consulting',
        'Other'
    ]
    
    conn.close()
    return render_template('workorders/create_service_po.html',
                          workorder=wo, 
                          suppliers=suppliers,
                          service_categories=service_categories)


@workorder_bp.route('/workorders/<int:id>/service-pos')
@login_required
def list_service_pos(id):
    """List all service/misc purchase orders linked to a work order"""
    db = Database()
    conn = db.get_connection()
    
    wo = conn.execute('''
        SELECT wo.*, p.code as product_code, p.name as product_name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.id = ?
    ''', (id,)).fetchone()
    
    if not wo:
        flash('Work Order not found', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    service_pos = conn.execute('''
        SELECT po.*, s.name as supplier_name,
               (SELECT COALESCE(SUM(total_cost), 0) FROM purchase_order_service_lines 
                WHERE po_id = po.id) as total_amount
        FROM purchase_orders po
        JOIN suppliers s ON po.supplier_id = s.id
        WHERE po.work_order_id = ?
        ORDER BY po.created_at DESC
    ''', (id,)).fetchall()
    
    service_lines = conn.execute('''
        SELECT psl.*, po.po_number, s.name as supplier_name
        FROM purchase_order_service_lines psl
        JOIN purchase_orders po ON psl.po_id = po.id
        JOIN suppliers s ON po.supplier_id = s.id
        WHERE psl.work_order_id = ?
        ORDER BY psl.created_at DESC
    ''', (id,)).fetchall()
    
    total_misc_cost = sum(line['total_cost'] for line in service_lines if line['status'] == 'Received')
    
    conn.close()
    return render_template('workorders/service_pos.html',
                          workorder=wo,
                          service_pos=service_pos,
                          service_lines=service_lines,
                          total_misc_cost=total_misc_cost)


@workorder_bp.route('/workorders/receive-service-line/<int:line_id>', methods=['POST'])
@login_required
@role_required('Admin', 'Procurement Staff', 'Receiving Staff')
def receive_service_line(line_id):
    """Mark a service line as received"""
    db = Database()
    conn = db.get_connection()
    
    line = conn.execute('''
        SELECT psl.*, po.po_number
        FROM purchase_order_service_lines psl
        JOIN purchase_orders po ON psl.po_id = po.id
        WHERE psl.id = ?
    ''', (line_id,)).fetchone()
    
    if not line:
        flash('Service line not found', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    conn.execute('''
        UPDATE purchase_order_service_lines 
        SET status = 'Received', received_date = ?, received_by = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (datetime.now().strftime('%Y-%m-%d'), session.get('user_id'), line_id))
    
    all_lines = conn.execute('''
        SELECT COUNT(*) as total, SUM(CASE WHEN status = 'Received' THEN 1 ELSE 0 END) as received
        FROM purchase_order_service_lines WHERE po_id = ?
    ''', (line['po_id'],)).fetchone()
    
    if all_lines['total'] == all_lines['received']:
        conn.execute('''
            UPDATE purchase_orders SET status = 'Received' WHERE id = ?
        ''', (line['po_id'],))
    
    AuditLogger.log(conn, 'purchase_order_service_lines', line_id, 'RECEIVE',
                   {'service_line_id': line_id, 'po_number': line['po_number']},
                   session.get('user_id'))
    
    conn.commit()
    flash('Service line marked as received', 'success')
    conn.close()
    
    work_order_id = line['work_order_id']
    return redirect(url_for('workorder_routes.list_service_pos', id=work_order_id))


@workorder_bp.route('/workorders/tasks/<int:task_id>/materials/add', methods=['POST'])
@login_required
@role_required('Admin', 'Planner', 'Production Staff')
def add_task_material(task_id):
    db = Database()
    conn = db.get_connection()
    
    task = conn.execute('SELECT * FROM work_order_tasks WHERE id = ?', (task_id,)).fetchone()
    if not task:
        flash('Task not found', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    product_id = request.form.get('product_id')
    required_qty = request.form.get('required_qty', 0)
    unit_of_measure = request.form.get('unit_of_measure', 'EA')
    warehouse_location = request.form.get('warehouse_location', '')
    required_by_date = request.form.get('required_by_date') or None
    notes = request.form.get('notes', '')
    
    product = conn.execute('''
        SELECT p.cost, COALESCE(i.unit_cost, 0) as inv_cost 
        FROM products p 
        LEFT JOIN inventory i ON i.product_id = p.id
        WHERE p.id = ?
    ''', (product_id,)).fetchone()
    unit_cost = float(product['cost'] or 0) if product else 0
    if unit_cost == 0 and product:
        unit_cost = float(product['inv_cost'] or 0)
    
    try:
        cursor = conn.execute('''
            INSERT INTO work_order_task_materials 
            (task_id, product_id, required_qty, unit_of_measure, warehouse_location, 
             required_by_date, notes, material_status, unit_cost, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'Planned', ?, ?)
        ''', (task_id, product_id, float(required_qty), unit_of_measure, 
              warehouse_location, required_by_date, notes, unit_cost, session.get('user_id')))
        
        material_id = cursor.lastrowid
        
        AuditLogger.log(conn, 'work_order_task_materials', material_id, 'CREATE',
                       {'task_id': task_id, 'product_id': product_id, 'required_qty': required_qty},
                       session.get('user_id'))
        
        conn.commit()
        flash('Material requirement added to task', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error adding material: {str(e)}', 'danger')
    
    conn.close()
    return redirect(url_for('workorder_routes.view_workorder', id=task['work_order_id']) + '#task-' + str(task_id))


@workorder_bp.route('/workorders/tasks/<int:task_id>/materials/<int:material_id>/edit', methods=['POST'])
@login_required
@role_required('Admin', 'Planner', 'Production Staff')
def edit_task_material(task_id, material_id):
    db = Database()
    conn = db.get_connection()
    
    material = conn.execute('''
        SELECT tm.*, wot.work_order_id 
        FROM work_order_task_materials tm
        JOIN work_order_tasks wot ON tm.task_id = wot.id
        WHERE tm.id = ? AND tm.task_id = ?
    ''', (material_id, task_id)).fetchone()
    
    if not material:
        flash('Material not found', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    required_qty = request.form.get('required_qty', material['required_qty'])
    warehouse_location = request.form.get('warehouse_location', material['warehouse_location'])
    required_by_date = request.form.get('required_by_date') or material['required_by_date']
    notes = request.form.get('notes', material['notes'])
    
    try:
        conn.execute('''
            UPDATE work_order_task_materials 
            SET required_qty = ?, warehouse_location = ?, required_by_date = ?, notes = ?
            WHERE id = ?
        ''', (float(required_qty), warehouse_location, required_by_date, notes, material_id))
        
        AuditLogger.log(conn, 'work_order_task_materials', material_id, 'UPDATE',
                       {'required_qty': required_qty, 'warehouse_location': warehouse_location},
                       session.get('user_id'))
        
        conn.commit()
        flash('Material requirement updated', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error updating material: {str(e)}', 'danger')
    
    conn.close()
    return redirect(url_for('workorder_routes.view_workorder', id=material['work_order_id']) + '#task-' + str(task_id))


@workorder_bp.route('/workorders/tasks/<int:task_id>/materials/<int:material_id>/delete', methods=['POST'])
@login_required
@role_required('Admin', 'Planner', 'Production Staff')
def delete_task_material(task_id, material_id):
    db = Database()
    conn = db.get_connection()
    
    material = conn.execute('''
        SELECT tm.*, wot.work_order_id 
        FROM work_order_task_materials tm
        JOIN work_order_tasks wot ON tm.task_id = wot.id
        WHERE tm.id = ? AND tm.task_id = ?
    ''', (material_id, task_id)).fetchone()
    
    if not material:
        flash('Material not found', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    if (material['issued_qty'] or 0) > 0:
        flash('Cannot delete material that has been issued', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.view_workorder', id=material['work_order_id']))
    
    try:
        conn.execute('DELETE FROM work_order_task_materials WHERE id = ?', (material_id,))
        
        AuditLogger.log(conn, 'work_order_task_materials', material_id, 'DELETE',
                       {'task_id': task_id},
                       session.get('user_id'))
        
        conn.commit()
        flash('Material requirement deleted', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error deleting material: {str(e)}', 'danger')
    
    conn.close()
    return redirect(url_for('workorder_routes.view_workorder', id=material['work_order_id']) + '#task-' + str(task_id))


@workorder_bp.route('/workorders/tasks/<int:task_id>/materials/<int:material_id>/issue', methods=['POST'])
@login_required
@role_required('Admin', 'Production Staff', 'Warehouse Staff')
def issue_task_material(task_id, material_id):
    db = Database()
    conn = db.get_connection()
    
    material = conn.execute('''
        SELECT tm.*, wot.work_order_id, p.code, p.name
        FROM work_order_task_materials tm
        JOIN work_order_tasks wot ON tm.task_id = wot.id
        JOIN products p ON tm.product_id = p.id
        WHERE tm.id = ? AND tm.task_id = ?
    ''', (material_id, task_id)).fetchone()
    
    if not material:
        flash('Material not found', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    try:
        issue_qty = float(request.form.get('issue_qty', 0))
    except (ValueError, TypeError):
        issue_qty = 0
    
    if issue_qty <= 0:
        flash('Issue quantity must be greater than zero', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.view_workorder', id=material['work_order_id']))
    
    lot_number = request.form.get('lot_number', '')
    serial_number = request.form.get('serial_number', '')
    
    max_issue = (material['required_qty'] or 0) - (material['issued_qty'] or 0)
    if issue_qty > max_issue:
        flash(f'Cannot issue more than required. Maximum: {max_issue}', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.view_workorder', id=material['work_order_id']))
    
    try:
        new_issued = (material['issued_qty'] or 0) + issue_qty
        new_status = 'Issued' if new_issued >= (material['required_qty'] or 0) else 'Partially Issued'
        
        conn.execute('''
            UPDATE work_order_task_materials 
            SET issued_qty = ?, lot_number = COALESCE(?, lot_number), 
                serial_number = COALESCE(?, serial_number),
                material_status = ?, issued_by = ?, issued_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (new_issued, lot_number or None, serial_number or None, 
              new_status, session.get('user_id'), material_id))
        
        AuditLogger.log(conn, 'work_order_task_materials', material_id, 'ISSUE',
                       {'issue_qty': issue_qty, 'new_total': new_issued, 
                        'lot_number': lot_number, 'serial_number': serial_number},
                       session.get('user_id'))
        
        conn.commit()
        flash(f'Issued {issue_qty} of {material["code"]} - {material["name"]}', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error issuing material: {str(e)}', 'danger')
    
    conn.close()
    return redirect(url_for('workorder_routes.view_workorder', id=material['work_order_id']) + '#task-' + str(task_id))


@workorder_bp.route('/workorders/tasks/<int:task_id>/materials/<int:material_id>/consume', methods=['POST'])
@login_required
@role_required('Admin', 'Production Staff')
def consume_task_material(task_id, material_id):
    db = Database()
    conn = db.get_connection()
    
    material = conn.execute('''
        SELECT tm.*, wot.work_order_id 
        FROM work_order_task_materials tm
        JOIN work_order_tasks wot ON tm.task_id = wot.id
        WHERE tm.id = ? AND tm.task_id = ?
    ''', (material_id, task_id)).fetchone()
    
    if not material:
        flash('Material not found', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    try:
        consume_qty = float(request.form.get('consume_qty', 0))
    except (ValueError, TypeError):
        consume_qty = 0
    
    if consume_qty <= 0:
        flash('Consume quantity must be greater than zero', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.view_workorder', id=material['work_order_id']))
    
    max_consume = (material['issued_qty'] or 0) - (material['consumed_qty'] or 0)
    
    if consume_qty > max_consume:
        flash(f'Cannot consume more than issued. Maximum: {max_consume}', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.view_workorder', id=material['work_order_id']))
    
    try:
        new_consumed = (material['consumed_qty'] or 0) + consume_qty
        new_status = 'Consumed' if new_consumed >= (material['required_qty'] or 0) else material['material_status']
        
        conn.execute('''
            UPDATE work_order_task_materials 
            SET consumed_qty = ?, material_status = ?
            WHERE id = ?
        ''', (new_consumed, new_status, material_id))
        
        AuditLogger.log(conn, 'work_order_task_materials', material_id, 'CONSUME',
                       {'consume_qty': consume_qty, 'new_total': new_consumed},
                       session.get('user_id'))
        
        conn.commit()
        flash(f'Consumed {consume_qty} units', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error consuming material: {str(e)}', 'danger')
    
    conn.close()
    return redirect(url_for('workorder_routes.view_workorder', id=material['work_order_id']) + '#task-' + str(task_id))


@workorder_bp.route('/workorders/tasks/<int:task_id>/materials/<int:material_id>/unissue', methods=['POST'])
@login_required
@role_required('Admin', 'Production Staff', 'Planner')
def unissue_task_material(task_id, material_id):
    """Return/Unissue issued materials back to inventory"""
    db = Database()
    conn = db.get_connection()
    
    material = conn.execute('''
        SELECT tm.*, wot.work_order_id, p.code, p.name
        FROM work_order_task_materials tm
        JOIN work_order_tasks wot ON tm.task_id = wot.id
        JOIN products p ON tm.product_id = p.id
        WHERE tm.id = ? AND tm.task_id = ?
    ''', (material_id, task_id)).fetchone()
    
    if not material:
        flash('Material not found', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    try:
        unissue_qty = float(request.form.get('unissue_qty', 0))
    except (ValueError, TypeError):
        unissue_qty = 0
    
    if unissue_qty <= 0:
        flash('Return quantity must be greater than zero', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.view_workorder', id=material['work_order_id']))
    
    max_unissue = (material['issued_qty'] or 0) - (material['consumed_qty'] or 0)
    
    if unissue_qty > max_unissue:
        flash(f'Cannot return more than available. Maximum: {max_unissue}', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.view_workorder', id=material['work_order_id']))
    
    try:
        new_issued = (material['issued_qty'] or 0) - unissue_qty
        
        if new_issued <= 0:
            new_status = 'Planned'
        elif new_issued < (material['required_qty'] or 0):
            new_status = 'Partially Issued'
        else:
            new_status = 'Issued'
        
        conn.execute('''
            UPDATE work_order_task_materials 
            SET issued_qty = ?, material_status = ?
            WHERE id = ?
        ''', (new_issued, new_status, material_id))
        
        AuditLogger.log(conn, 'work_order_task_materials', material_id, 'UNISSUE',
                       {'unissue_qty': unissue_qty, 'new_issued_total': new_issued, 'reason': 'Return to inventory'},
                       session.get('user_id'))
        
        conn.commit()
        flash(f'Returned {unissue_qty} of {material["code"]} - {material["name"]} to inventory', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error returning material: {str(e)}', 'danger')
    
    conn.close()
    return redirect(url_for('workorder_routes.view_workorder', id=material['work_order_id']) + '#materialsPane')


@workorder_bp.route('/workorders/<int:wo_id>/bulk-update-tasks', methods=['POST'])
@login_required
@role_required('Admin', 'Production Staff')
def bulk_update_tasks(wo_id):
    """Mass update multiple tasks at once"""
    db = Database()
    conn = db.get_connection()
    
    try:
        task_ids = request.form.getlist('task_ids')
        if not task_ids:
            flash('No tasks selected', 'warning')
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
        action = request.form.get('bulk_action')
        new_status = request.form.get('bulk_status')
        new_assignee = request.form.get('bulk_assignee')
        
        updated_count = 0
        now = datetime.now()
        
        for task_id in task_ids:
            task = conn.execute('SELECT * FROM work_order_tasks WHERE id = ? AND work_order_id = ?', 
                              (task_id, wo_id)).fetchone()
            if not task:
                continue
            
            if action == 'status' and new_status:
                if new_status == 'Completed':
                    materials = conn.execute('''
                        SELECT * FROM work_order_task_materials 
                        WHERE task_id = ? AND (issued_qty IS NULL OR issued_qty < required_qty)
                    ''', (task_id,)).fetchall()
                    if materials:
                        continue
                
                if new_status == 'In Progress' and not task['actual_start_date']:
                    conn.execute('''
                        UPDATE work_order_tasks 
                        SET status = ?, actual_start_date = ?
                        WHERE id = ?
                    ''', (new_status, now.strftime('%Y-%m-%d %H:%M:%S'), task_id))
                elif new_status == 'Completed':
                    conn.execute('''
                        UPDATE work_order_tasks 
                        SET status = ?, actual_end_date = ?
                        WHERE id = ?
                    ''', (new_status, now.strftime('%Y-%m-%d %H:%M:%S'), task_id))
                else:
                    conn.execute('UPDATE work_order_tasks SET status = ? WHERE id = ?', (new_status, task_id))
                updated_count += 1
            
            elif action == 'assignee' and new_assignee:
                assignee_id = int(new_assignee) if new_assignee != '' else None
                conn.execute('UPDATE work_order_tasks SET assigned_resource_id = ? WHERE id = ?', (assignee_id, task_id))
                updated_count += 1
        
        conn.commit()
        
        if updated_count > 0:
            flash(f'Successfully updated {updated_count} task(s)', 'success')
        else:
            flash('No tasks were updated', 'warning')
            
    except Exception as e:
        conn.rollback()
        flash(f'Error updating tasks: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('workorder_routes.view_workorder', id=wo_id))


@workorder_bp.route('/workorders/tasks/<int:task_id>/update-status', methods=['POST'])
@login_required
@role_required('Admin', 'Production Staff')
def update_task_status(task_id):
    db = Database()
    conn = db.get_connection()
    
    task = conn.execute('SELECT * FROM work_order_tasks WHERE id = ?', (task_id,)).fetchone()
    if not task:
        flash('Task not found', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    new_status = request.form.get('status')
    
    if new_status == 'Completed':
        materials = conn.execute('''
            SELECT * FROM work_order_task_materials 
            WHERE task_id = ? AND (issued_qty IS NULL OR issued_qty < required_qty)
        ''', (task_id,)).fetchall()
        
        if materials:
            flash('Cannot complete task - materials have not been fully issued', 'warning')
            conn.close()
            return redirect(url_for('workorder_routes.view_workorder', id=task['work_order_id']) + '#task-' + str(task_id))
    
    try:
        now = datetime.now()
        
        if new_status == 'In Progress' and not task['actual_start_date']:
            conn.execute('''
                UPDATE work_order_tasks 
                SET status = ?, actual_start_date = ?
                WHERE id = ?
            ''', (new_status, now.strftime('%Y-%m-%d %H:%M:%S'), task_id))
        elif new_status == 'Completed':
            conn.execute('''
                UPDATE work_order_tasks 
                SET status = ?, actual_end_date = ?
                WHERE id = ?
            ''', (new_status, now.strftime('%Y-%m-%d %H:%M:%S'), task_id))
        else:
            conn.execute('UPDATE work_order_tasks SET status = ? WHERE id = ?', (new_status, task_id))
        
        AuditLogger.log(conn, 'work_order_tasks', task_id, 'STATUS_CHANGE',
                       {'old_status': task['status'], 'new_status': new_status},
                       session.get('user_id'))
        
        conn.commit()
        flash(f'Task status updated to {new_status}', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error updating task: {str(e)}', 'danger')
    
    conn.close()
    return redirect(url_for('workorder_routes.view_workorder', id=task['work_order_id']) + '#task-' + str(task_id))


@workorder_bp.route('/workorders/<int:id>/packaging-assessment')
@login_required
def generate_packaging_assessment(id):
    """Generate a formal Packaging Requirements Assessment PDF document"""
    from flask import send_file
    from reportlab.lib.pagesizes import letter
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
    import io
    
    db = Database()
    conn = db.get_connection()
    
    workorder = conn.execute('''
        SELECT wo.*, p.code, p.name as product_name, p.description as product_description,
               c.name as customer_name, c.customer_number
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN customers c ON wo.customer_id = c.id
        WHERE wo.id = ?
    ''', (id,)).fetchone()
    
    if not workorder:
        conn.close()
        flash('Work order not found', 'danger')
        return redirect(url_for('workorder_routes.list_workorders'))
    
    workorder = dict(workorder)
    
    company = conn.execute('SELECT * FROM company_settings LIMIT 1').fetchone()
    company = dict(company) if company else {}
    
    user = conn.execute('SELECT username FROM users WHERE id = ?', (session.get('user_id'),)).fetchone()
    prepared_by = user['username'] if user else 'Unknown'
    
    conn.close()
    
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, 
                           leftMargin=0.75*inch, rightMargin=0.75*inch,
                           topMargin=0.75*inch, bottomMargin=0.75*inch)
    
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=18, 
                                  textColor=colors.HexColor('#1e3a5f'), spaceAfter=6, alignment=TA_CENTER)
    subtitle_style = ParagraphStyle('Subtitle', parent=styles['Normal'], fontSize=11,
                                     textColor=colors.HexColor('#64748b'), alignment=TA_CENTER, spaceAfter=20)
    section_header = ParagraphStyle('SectionHeader', parent=styles['Heading2'], fontSize=12,
                                     textColor=colors.HexColor('#1e3a5f'), spaceBefore=15, spaceAfter=8,
                                     borderColor=colors.HexColor('#1e3a5f'), borderWidth=1, borderPadding=5)
    item_title = ParagraphStyle('ItemTitle', parent=styles['Normal'], fontSize=11, 
                                 textColor=colors.HexColor('#1e293b'), fontName='Helvetica-Bold', spaceAfter=3)
    item_desc = ParagraphStyle('ItemDesc', parent=styles['Normal'], fontSize=10,
                                textColor=colors.HexColor('#475569'), alignment=TA_JUSTIFY, spaceAfter=12,
                                leftIndent=20)
    normal_style = ParagraphStyle('NormalText', parent=styles['Normal'], fontSize=10,
                                   textColor=colors.HexColor('#1e293b'), spaceAfter=6)
    
    elements = []
    
    elements.append(Paragraph(company.get('company_name', 'Dynamic.IQ-COREx'), title_style))
    elements.append(Paragraph("PACKAGING REQUIREMENTS ASSESSMENT", 
                              ParagraphStyle('DocTitle', parent=title_style, fontSize=14, spaceAfter=4)))
    elements.append(Paragraph(f"Document generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}", subtitle_style))
    elements.append(Spacer(1, 10))
    
    wo_info = [
        ['Work Order:', workorder.get('wo_number', 'N/A'), 'Status:', workorder.get('status') or 'N/A'],
        ['Product:', workorder.get('code', 'N/A'), 'Quantity:', str(workorder.get('quantity', 1))],
        ['Description:', workorder.get('product_name') or '', '', ''],
        ['Customer:', workorder.get('customer_name') or 'N/A', 'Customer #:', workorder.get('customer_number') or 'N/A'],
    ]
    
    wo_table = Table(wo_info, colWidths=[1.2*inch, 2.5*inch, 1.2*inch, 2.1*inch])
    wo_table.setStyle(TableStyle([
        ('FONT', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONT', (2, 0), (2, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#64748b')),
        ('TEXTCOLOR', (2, 0), (2, -1), colors.HexColor('#64748b')),
        ('TEXTCOLOR', (1, 0), (1, -1), colors.HexColor('#1e293b')),
        ('TEXTCOLOR', (3, 0), (3, -1), colors.HexColor('#1e293b')),
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#f8fafc')),
        ('BOX', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
        ('INNERGRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e2e8f0')),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
    ]))
    elements.append(wo_table)
    elements.append(Spacer(1, 15))
    
    elements.append(Paragraph("CRATE SPECIFICATION", section_header))
    
    crate_info = [
        ['Crate Required:', workorder.get('pkg_crate_requirement') or 'Not Specified'],
        ['Crate Dimensions:', workorder.get('pkg_crate_dimensions') or 'Not Specified'],
    ]
    crate_table = Table(crate_info, colWidths=[2*inch, 5*inch])
    crate_table.setStyle(TableStyle([
        ('FONT', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#64748b')),
        ('TEXTCOLOR', (1, 0), (1, -1), colors.HexColor('#1e293b')),
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#f8fafc')),
        ('BOX', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
        ('INNERGRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e2e8f0')),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
    ]))
    elements.append(crate_table)
    elements.append(Spacer(1, 15))
    
    elements.append(Paragraph("ASSESSMENT FINDINGS", section_header))
    elements.append(Paragraph("The following packaging requirements have been identified based on inspection and operational requirements:", normal_style))
    elements.append(Spacer(1, 10))
    
    assessment_items = []
    
    if workorder.get('cra_structural_integrity'):
        assessment_items.append({
            'title': 'Structural Integrity',
            'description': 'Existing crate was inspected and found to have structural deficiencies (cracks, loose joints, or weakened panels). Crate cannot safely support the unit\'s weight or handling loads. A new crate is required to ensure secure containment and transport.'
        })
    
    if workorder.get('cra_dimensional_fit'):
        assessment_items.append({
            'title': 'Dimensional Fit',
            'description': 'Current crate dimensions are not compatible with the unit configuration. Unit does not fit securely within the crate or exceeds allowable internal clearances. New crate required to ensure proper fit and protection during handling.'
        })
    
    if workorder.get('cra_protection_requirements'):
        assessment_items.append({
            'title': 'Protection Requirements',
            'description': 'Existing crate lacks adequate internal protection for the unit type. Insufficient cushioning, moisture barrier, or vibration control identified. A new crate with proper protective lining and supports is required.'
        })
    
    if workorder.get('cra_storage_duration'):
        assessment_items.append({
            'title': 'Storage Duration',
            'description': 'Unit is scheduled for extended storage and current crate does not provide sufficient environmental protection. A new, sealed crate with moisture barrier or lined interior is required for long-term storage compliance.'
        })
    
    if workorder.get('cra_customer_oem_spec'):
        assessment_items.append({
            'title': 'Customer / OEM Specification',
            'description': 'Current crate design does not comply with customer or OEM packaging requirements as defined in the applicable specification or SOW. A compliant crate must be sourced or fabricated to meet contractual standards.'
        })
    
    if workorder.get('cra_return_shipping'):
        assessment_items.append({
            'title': 'Return Shipping Requirement',
            'description': 'Original crate is not suitable for reuse following inbound shipment or repair cycle. Damage, wear, or missing hardware identified. New crate required for safe return shipment of the unit.'
        })
    
    if workorder.get('cra_hazmat_handling'):
        assessment_items.append({
            'title': 'Hazardous Material Handling',
            'description': 'Unit contains or was exposed to hazardous materials (e.g., oil, fuel, or batteries). Existing crate does not meet hazardous material containment or labeling requirements. New compliant crate required per IATA/ICAO and DOT regulations.'
        })
    
    if assessment_items:
        for idx, item in enumerate(assessment_items, 1):
            elements.append(Paragraph(f"{idx}. {item['title']}", item_title))
            elements.append(Paragraph(item['description'], item_desc))
    else:
        elements.append(Paragraph("No specific packaging assessment criteria have been identified for this work order.", 
                                   ParagraphStyle('NoItems', parent=normal_style, textColor=colors.HexColor('#64748b'),
                                                  fontName='Helvetica-Oblique')))
    
    elements.append(Spacer(1, 20))
    elements.append(Paragraph("RECOMMENDATION", section_header))
    
    if workorder.get('pkg_crate_requirement') == 'Yes' and assessment_items:
        recommendation = f"Based on the above assessment findings, it is recommended that a NEW CRATE be procured or fabricated for this unit. The crate must address all {len(assessment_items)} identified requirement(s) to ensure safe handling, storage, and transportation of the unit."
    elif workorder.get('pkg_crate_requirement') == 'Yes':
        recommendation = "A new crate has been specified as required for this work order. Please ensure appropriate crating solution is sourced before shipment."
    else:
        recommendation = "No new crate is required for this work order based on the current assessment."
    
    elements.append(Paragraph(recommendation, normal_style))
    
    elements.append(Spacer(1, 30))
    
    current_date = datetime.now().strftime('%B %d, %Y')
    sig_data = [
        ['Prepared By:', prepared_by, 'Date:', current_date],
    ]
    sig_table = Table(sig_data, colWidths=[1.2*inch, 2.5*inch, 0.8*inch, 2.5*inch])
    sig_table.setStyle(TableStyle([
        ('FONT', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONT', (2, 0), (2, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.HexColor('#64748b')),
        ('TOPPADDING', (0, 0), (-1, -1), 12),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]))
    elements.append(sig_table)
    
    elements.append(Spacer(1, 20))
    footer_text = f"{company.get('company_name', '')} | {company.get('address_line1', '')} | {company.get('city', '')}, {company.get('state', '')} {company.get('postal_code', '')}"
    elements.append(Paragraph(footer_text, 
                              ParagraphStyle('Footer', parent=normal_style, fontSize=8, 
                                             textColor=colors.HexColor('#94a3b8'), alignment=TA_CENTER)))
    
    doc.build(elements)
    buffer.seek(0)
    
    filename = f"Packaging_Assessment_{workorder['wo_number']}_{datetime.now().strftime('%Y%m%d')}.pdf"
    
    return send_file(
        buffer,
        as_attachment=False,
        download_name=filename,
        mimetype='application/pdf'
    )

@workorder_bp.route('/workorders/<int:id>/reconciliation-data')
@login_required
@role_required('Admin', 'Finance', 'Supervisor', 'Planner')
def get_reconciliation_data(id):
    """Get planned vs actual data for work order reconciliation"""
    import json
    from flask import jsonify
    
    db = Database()
    conn = db.get_connection()
    
    workorder = conn.execute('SELECT * FROM work_orders WHERE id = ?', (id,)).fetchone()
    if not workorder:
        conn.close()
        return jsonify({'error': 'Work order not found'}), 404
    
    task_summary = conn.execute('''
        SELECT 
            SUM(COALESCE(planned_hours, 0)) as planned_labor_hours,
            SUM(COALESCE(actual_hours, 0)) as actual_labor_hours,
            SUM(COALESCE(planned_labor_cost, 0)) as planned_labor_cost,
            SUM(COALESCE(actual_labor_cost, 0)) as actual_labor_cost
        FROM work_order_tasks
        WHERE work_order_id = ?
    ''', (id,)).fetchone()
    
    task_materials = conn.execute('''
        SELECT 
            SUM(COALESCE(tm.required_qty, 0)) as planned_material_qty,
            SUM(COALESCE(tm.consumed_qty, tm.issued_qty, 0)) as actual_material_qty,
            SUM(COALESCE(tm.required_qty, 0) * COALESCE(tm.unit_cost, p.cost, 0)) as planned_material_cost,
            SUM(COALESCE(tm.consumed_qty, tm.issued_qty, 0) * COALESCE(tm.unit_cost, p.cost, 0)) as actual_material_cost
        FROM work_order_task_materials tm
        JOIN work_order_tasks wot ON tm.task_id = wot.id
        JOIN products p ON tm.product_id = p.id
        WHERE wot.work_order_id = ?
    ''', (id,)).fetchone()
    
    wo_materials = conn.execute('''
        SELECT 
            SUM(COALESCE(mr.required_quantity, 0)) as planned_material_qty,
            SUM(COALESCE(
                (SELECT SUM(mi.quantity_issued) FROM material_issues mi WHERE mi.work_order_id = mr.work_order_id AND mi.product_id = mr.product_id),
                0
            )) as actual_material_qty,
            SUM(COALESCE(mr.required_quantity, 0) * COALESCE(p.cost, 0)) as planned_material_cost,
            SUM(COALESCE(
                (SELECT SUM(mi.quantity_issued) FROM material_issues mi WHERE mi.work_order_id = mr.work_order_id AND mi.product_id = mr.product_id),
                0
            ) * COALESCE(p.cost, 0)) as actual_material_cost
        FROM material_requirements mr
        JOIN products p ON mr.product_id = p.id
        WHERE mr.work_order_id = ?
    ''', (id,)).fetchone()
    
    outside_services = conn.execute('''
        SELECT 
            COALESCE(SUM(CASE WHEN status = 'Pending' THEN total_cost ELSE 0 END), 0) as planned_services_cost,
            COALESCE(SUM(CASE WHEN status = 'Received' THEN total_cost ELSE 0 END), 0) as actual_services_cost
        FROM work_order_service_pos
        WHERE work_order_id = ?
    ''', (id,)).fetchone()
    
    conn.close()
    
    planned_labor_hours = float(task_summary['planned_labor_hours'] or 0)
    actual_labor_hours = float(task_summary['actual_labor_hours'] or 0)
    planned_labor_cost = float(task_summary['planned_labor_cost'] or 0)
    actual_labor_cost = float(task_summary['actual_labor_cost'] or 0)
    
    tm_planned_qty = float(task_materials['planned_material_qty'] or 0)
    tm_actual_qty = float(task_materials['actual_material_qty'] or 0)
    tm_planned_cost = float(task_materials['planned_material_cost'] or 0)
    tm_actual_cost = float(task_materials['actual_material_cost'] or 0)
    
    wo_planned_qty = float(wo_materials['planned_material_qty'] or 0)
    wo_actual_qty = float(wo_materials['actual_material_qty'] or 0)
    wo_planned_cost = float(wo_materials['planned_material_cost'] or 0)
    wo_actual_cost = float(wo_materials['actual_material_cost'] or 0)
    
    planned_material_qty = tm_planned_qty + wo_planned_qty
    actual_material_qty = tm_actual_qty + wo_actual_qty
    planned_material_cost = tm_planned_cost + wo_planned_cost
    actual_material_cost = tm_actual_cost + wo_actual_cost
    
    planned_services_cost = float(outside_services['planned_services_cost'] or 0)
    actual_services_cost = float(outside_services['actual_services_cost'] or 0)
    
    planned_total = planned_labor_cost + planned_material_cost + planned_services_cost
    actual_total = actual_labor_cost + actual_material_cost + actual_services_cost
    
    data = {
        'wo_number': workorder['wo_number'],
        'status': workorder['status'],
        'reconciliation_status': workorder['reconciliation_status'] or 'Not Reconciled',
        'labor': {
            'planned_hours': planned_labor_hours,
            'actual_hours': actual_labor_hours,
            'variance_hours': actual_labor_hours - planned_labor_hours,
            'planned_cost': planned_labor_cost,
            'actual_cost': actual_labor_cost,
            'variance_cost': actual_labor_cost - planned_labor_cost
        },
        'materials': {
            'planned_qty': planned_material_qty,
            'actual_qty': actual_material_qty,
            'variance_qty': actual_material_qty - planned_material_qty,
            'planned_cost': planned_material_cost,
            'actual_cost': actual_material_cost,
            'variance_cost': actual_material_cost - planned_material_cost
        },
        'services': {
            'planned_cost': planned_services_cost,
            'actual_cost': actual_services_cost,
            'variance_cost': actual_services_cost - planned_services_cost
        },
        'total': {
            'planned_cost': planned_total,
            'actual_cost': actual_total,
            'variance_cost': actual_total - planned_total,
            'variance_percent': ((actual_total - planned_total) / planned_total * 100) if planned_total > 0 else 0
        }
    }
    
    return jsonify(data)

@workorder_bp.route('/workorders/<int:id>/reconcile', methods=['POST'])
@login_required
@role_required('Admin', 'Finance', 'Supervisor')
def submit_reconciliation(id):
    """Submit work order reconciliation"""
    import json
    
    db = Database()
    conn = db.get_connection()
    
    workorder = conn.execute('SELECT * FROM work_orders WHERE id = ?', (id,)).fetchone()
    if not workorder:
        conn.close()
        flash('Work order not found.', 'error')
        return redirect(url_for('workorder_routes.list_workorders'))
    
    if workorder['reconciliation_status'] == 'Reconciled':
        conn.close()
        flash('Work order is already reconciled.', 'warning')
        return redirect(url_for('workorder_routes.view_workorder', id=id))
    
    notes = request.form.get('reconciliation_notes', '').strip()
    if not notes:
        conn.close()
        flash('Reconciliation notes are required.', 'error')
        return redirect(url_for('workorder_routes.view_workorder', id=id))
    
    variance_data = request.form.get('variance_summary', '{}')
    
    conn.execute('''
        UPDATE work_orders 
        SET reconciliation_status = 'Reconciled',
            reconciled_by = ?,
            reconciled_at = datetime('now'),
            reconciliation_notes = ?,
            variance_summary = ?
        WHERE id = ?
    ''', (session.get('user_id'), notes, variance_data, id))
    conn.commit()
    
    AuditLogger.log(
        conn=conn,
        user_id=session.get('user_id'),
        action='RECONCILE',
        entity_type='work_order',
        entity_id=id,
        details=f"Work order {workorder['wo_number']} reconciled. Notes: {notes[:100]}..."
    )
    
    conn.close()
    
    flash(f"Work order {workorder['wo_number']} has been reconciled successfully.", 'success')
    return redirect(url_for('workorder_routes.view_workorder', id=id))

@workorder_bp.route('/workorders/<int:id>/invalidate-reconciliation', methods=['POST'])
@login_required
@role_required('Admin')
def invalidate_reconciliation(id):
    """Invalidate reconciliation (Admin only override)"""
    db = Database()
    conn = db.get_connection()
    
    workorder = conn.execute('SELECT * FROM work_orders WHERE id = ?', (id,)).fetchone()
    if not workorder:
        conn.close()
        flash('Work order not found.', 'error')
        return redirect(url_for('workorder_routes.list_workorders'))
    
    if workorder['reconciliation_status'] != 'Reconciled':
        conn.close()
        flash('Work order is not reconciled.', 'warning')
        return redirect(url_for('workorder_routes.view_workorder', id=id))
    
    conn.execute('''
        UPDATE work_orders 
        SET reconciliation_status = 'Not Reconciled',
            reconciled_by = NULL,
            reconciled_at = NULL,
            reconciliation_notes = NULL,
            variance_summary = NULL
        WHERE id = ?
    ''', (id,))
    conn.commit()
    
    AuditLogger.log(
        conn=conn,
        user_id=session.get('user_id'),
        action='INVALIDATE_RECONCILIATION',
        entity_type='work_order',
        entity_id=id,
        details=f"Reconciliation invalidated for work order {workorder['wo_number']} by Admin"
    )
    
    conn.close()
    
    flash(f"Reconciliation for {workorder['wo_number']} has been invalidated.", 'warning')
    return redirect(url_for('workorder_routes.view_workorder', id=id))

@workorder_bp.route('/workorders/<int:id>/documents/upload', methods=['POST'])
@login_required
@role_required('Admin', 'Production Staff', 'Planner')
def upload_wo_document(id):
    """Upload a document to a work order"""
    import os
    import uuid
    from werkzeug.utils import secure_filename
    
    db = Database()
    conn = db.get_connection()
    
    workorder = conn.execute('SELECT * FROM work_orders WHERE id = ?', (id,)).fetchone()
    if not workorder:
        conn.close()
        flash('Work order not found.', 'error')
        return redirect(url_for('workorder_routes.list_workorders'))
    
    if 'document' not in request.files:
        flash('No file selected.', 'warning')
        return redirect(url_for('workorder_routes.view_workorder', id=id))
    
    file = request.files['document']
    if file.filename == '':
        flash('No file selected.', 'warning')
        return redirect(url_for('workorder_routes.view_workorder', id=id))
    
    document_type = request.form.get('document_type', 'General')
    description = request.form.get('description', '')
    
    upload_dir = os.path.join('uploads', 'work_order_documents', str(id))
    os.makedirs(upload_dir, exist_ok=True)
    
    original_filename = secure_filename(file.filename)
    file_ext = os.path.splitext(original_filename)[1]
    unique_filename = f"{uuid.uuid4().hex}{file_ext}"
    file_path = os.path.join(upload_dir, unique_filename)
    
    file.save(file_path)
    file_size = os.path.getsize(file_path)
    
    mime_type = file.content_type or 'application/octet-stream'
    
    conn.execute('''
        INSERT INTO work_order_documents 
        (work_order_id, document_type, document_name, file_path, original_filename, file_size, mime_type, description, uploaded_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (id, document_type, original_filename, file_path, original_filename, file_size, mime_type, description, session.get('user_id')))
    
    conn.commit()
    conn.close()
    
    flash(f'Document "{original_filename}" uploaded successfully.', 'success')
    return redirect(url_for('workorder_routes.view_workorder', id=id) + '#docs-tab')

@workorder_bp.route('/workorders/<int:wo_id>/documents/<int:doc_id>/download')
@login_required
def download_wo_document(wo_id, doc_id):
    """Download a work order document"""
    from flask import send_file
    import os
    
    db = Database()
    conn = db.get_connection()
    
    document = conn.execute('''
        SELECT * FROM work_order_documents 
        WHERE id = ? AND work_order_id = ? AND is_active = 1
    ''', (doc_id, wo_id)).fetchone()
    
    conn.close()
    
    if not document:
        flash('Document not found.', 'error')
        return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
    
    if not os.path.exists(document['file_path']):
        flash('File not found on server.', 'error')
        return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
    
    return send_file(
        document['file_path'],
        download_name=document['original_filename'],
        as_attachment=True
    )

@workorder_bp.route('/workorders/<int:wo_id>/documents/<int:doc_id>/view')
@login_required
def view_wo_document(wo_id, doc_id):
    """View a work order document inline in browser"""
    from flask import send_file
    import os
    
    db = Database()
    conn = db.get_connection()
    
    document = conn.execute('''
        SELECT * FROM work_order_documents 
        WHERE id = ? AND work_order_id = ? AND is_active = 1
    ''', (doc_id, wo_id)).fetchone()
    
    conn.close()
    
    if not document:
        flash('Document not found.', 'error')
        return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
    
    if not os.path.exists(document['file_path']):
        flash('File not found on server.', 'error')
        return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
    
    mime_type = document['mime_type'] or 'application/octet-stream'
    
    return send_file(
        document['file_path'],
        mimetype=mime_type,
        as_attachment=False
    )

@workorder_bp.route('/workorders/<int:wo_id>/documents/<int:doc_id>/delete', methods=['POST'])
@login_required
@role_required('Admin', 'Production Staff')
def delete_wo_document(wo_id, doc_id):
    """Delete a work order document (soft delete)"""
    db = Database()
    conn = db.get_connection()
    
    document = conn.execute('''
        SELECT * FROM work_order_documents 
        WHERE id = ? AND work_order_id = ?
    ''', (doc_id, wo_id)).fetchone()
    
    if not document:
        conn.close()
        flash('Document not found.', 'error')
        return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
    
    conn.execute('UPDATE work_order_documents SET is_active = 0 WHERE id = ?', (doc_id,))
    conn.commit()
    conn.close()
    
    flash(f'Document "{document["original_filename"]}" deleted.', 'success')
    return redirect(url_for('workorder_routes.view_workorder', id=wo_id) + '#docs-tab')


@workorder_bp.route('/api/workorders/<int:wo_id>/component-buyout', methods=['POST'])
@login_required
@role_required('Admin', 'Procurement', 'Production Staff', 'Supervisor')
def create_component_buyout(wo_id):
    """Create a Component Buyout Purchase Order from a Work Order"""
    from flask import jsonify, request
    import traceback
    
    data = request.get_json() or {}
    unit_price = float(data.get('unit_price', 0))
    
    db = Database()
    conn = db.get_connection()
    
    try:
        wo = conn.execute('''
            SELECT wo.*, p.code as product_code, p.name as product_name, p.id as prod_id,
                   c.id as cust_id, c.name as customer_name, c.customer_number,
                   inv.serial_number as inventory_serial
            FROM work_orders wo
            JOIN products p ON wo.product_id = p.id
            LEFT JOIN customers c ON wo.customer_id = c.id
            LEFT JOIN inventory inv ON wo.inventory_id = inv.id
            WHERE wo.id = ?
        ''', (wo_id,)).fetchone()
        
        if not wo:
            conn.close()
            return jsonify({'success': False, 'error': 'Work Order not found'}), 404
        
        if wo['component_buyout_flag'] == 1:
            conn.close()
            return jsonify({'success': False, 'error': 'Component Buyout already exists for this Work Order'}), 400
        
        if wo['status'] in ('Completed', 'Cancelled'):
            conn.close()
            return jsonify({'success': False, 'error': 'Cannot create buyout for completed or cancelled Work Orders'}), 400
        
        if not wo['product_id']:
            conn.close()
            return jsonify({'success': False, 'error': 'Work Order must have a Part Number assigned'}), 400
        
        # Component Buyout: Supplier on PO must be the Work Order's Customer
        if not wo['cust_id']:
            conn.close()
            return jsonify({'success': False, 'error': 'Work Order must have a Customer assigned to create Component Buyout'}), 400
        
        # Find supplier matching the customer name or create reference
        wo_customer = conn.execute('SELECT id, name, customer_number FROM customers WHERE id = ?', (wo['cust_id'],)).fetchone()
        
        # Try to find a supplier that matches the customer name
        matching_supplier = conn.execute('''
            SELECT id, code, name FROM suppliers 
            WHERE LOWER(name) = LOWER(?) 
            ORDER BY id LIMIT 1
        ''', (wo_customer['name'],)).fetchone()
        
        if matching_supplier:
            supplier_id = matching_supplier['id']
            buyout_source_type = 'Customer (as Supplier)'
            buyout_source_name = f"{wo_customer['customer_number']} - {wo_customer['name']}"
        else:
            # No matching supplier found - auto-create supplier from customer data
            supplier_code = f"SUP-{wo_customer['customer_number']}"
            cursor = conn.execute('''
                INSERT INTO suppliers (code, name, created_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', (supplier_code, wo_customer['name']))
            supplier_id = cursor.lastrowid
            buyout_source_type = 'Customer (Auto-Created Supplier)'
            buyout_source_name = f"{wo_customer['customer_number']} - {wo_customer['name']}"
        
        last_po = conn.execute('''
            SELECT po_number FROM purchase_orders 
            WHERE po_number LIKE 'PO-%'
            ORDER BY CAST(SUBSTR(po_number, 4) AS INTEGER) DESC 
            LIMIT 1
        ''').fetchone()
        
        if last_po:
            try:
                last_number = int(last_po['po_number'].split('-')[1])
                next_number = last_number + 1
            except (ValueError, IndexError):
                next_number = 1
        else:
            next_number = 1
        
        po_number = f'PO-{next_number:06d}'
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Build notes with source information
        notes = f'Component Buyout generated from Work Order {wo["wo_number"]}'
        if buyout_source_type and buyout_source_name:
            notes += f' | Buyout Source: {buyout_source_type} - {buyout_source_name}'
        
        cursor = conn.execute('''
            INSERT INTO purchase_orders (
                po_number, supplier_id, status, order_date, notes, po_type, work_order_id, component_buyout_flag
            ) VALUES (?, ?, 'Draft', ?, ?, 'Component Buyout', ?, 1)
        ''', (
            po_number,
            supplier_id,
            today,
            notes,
            wo_id
        ))
        po_id = cursor.lastrowid
        
        default_uom = conn.execute("SELECT id FROM unit_of_measure WHERE uom_code = 'EA' OR uom_code = 'EACH' LIMIT 1").fetchone()
        uom_id = default_uom['id'] if default_uom else None
        
        # Get or create "COMPONENT-BUYOUT" non-inventory product to avoid duplicate inventory on receiving
        buyout_product = conn.execute('''
            SELECT id FROM products WHERE code = 'COMPONENT-BUYOUT' AND product_type = 'Non-Inventory'
        ''').fetchone()
        
        if not buyout_product:
            conn.execute('''
                INSERT INTO products (code, name, description, unit_of_measure, product_type, cost)
                VALUES ('COMPONENT-BUYOUT', 'Component Buyout', 'Non-inventory item for Component Buyout POs', 'EA', 'Non-Inventory', 0)
            ''')
            buyout_product = conn.execute("SELECT id FROM products WHERE code = 'COMPONENT-BUYOUT'").fetchone()
        
        buyout_product_id = buyout_product['id']
        
        # Get serial number from linked inventory if available
        serial_number = wo['inventory_serial'] or ''
        
        conn.execute('''
            INSERT INTO purchase_order_lines (
                po_id, line_number, product_id, quantity, unit_price, uom_id,
                description, line_type, work_order_reference,
                reference_part_number, reference_serial_number
            ) VALUES (?, 1, ?, 1, ?, ?, ?, 'Component Buyout', ?, ?, ?)
        ''', (
            po_id,
            buyout_product_id,
            unit_price,
            uom_id,
            f"Component Buyout for WO {wo['wo_number']} - P/N: {wo['product_code']} - {wo['product_name']}",
            wo_id,
            wo['product_code'],
            serial_number
        ))
        
        conn.execute('''
            UPDATE work_orders 
            SET component_buyout_flag = 1, buyout_po_id = ?
            WHERE id = ?
        ''', (po_id, wo_id))
        
        AuditLogger.log(
            conn=conn,
            record_type='work_order',
            record_id=str(wo_id),
            action_type='Component Buyout Created',
            modified_by=session.get('user_id'),
            changed_fields={
                'buyout_po_id': po_id,
                'po_number': po_number,
                'component_buyout_flag': True
            }
        )
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'po_id': po_id,
            'po_number': po_number,
            'message': f'Component Buyout PO {po_number} successfully created and linked.'
        })
        
    except Exception as e:
        print(f"Component Buyout Error: {str(e)}")
        print(traceback.format_exc())
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500
