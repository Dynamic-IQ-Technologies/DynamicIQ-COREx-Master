from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database, AuditLogger
from auth import login_required, role_required
from datetime import datetime, timedelta

capacity_bp = Blueprint('capacity_routes', __name__)


def calculate_available_capacity(conn, work_center_id, date_from, date_to, default_hours_per_day, default_days_per_week, efficiency_factor):
    """
    Calculate available capacity for a work center considering:
    1. Capacity overrides for specific dates
    2. Labor resource utilization percentages
    """
    start_date = datetime.strptime(date_from, '%Y-%m-%d')
    end_date = datetime.strptime(date_to, '%Y-%m-%d')
    total_days = (end_date - start_date).days + 1
    
    overrides = conn.execute('''
        SELECT capacity_date, available_hours 
        FROM work_center_capacity 
        WHERE work_center_id = ? 
        AND capacity_date BETWEEN ? AND ?
    ''', (work_center_id, date_from, date_to)).fetchall()
    
    override_dict = {row['capacity_date']: row['available_hours'] for row in overrides}
    
    total_hours = 0
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        weekday = current_date.weekday()
        
        if date_str in override_dict:
            total_hours += override_dict[date_str]
        elif weekday < default_days_per_week:
            total_hours += default_hours_per_day
        
        current_date += timedelta(days=1)
    
    resource_utilization = conn.execute('''
        SELECT COALESCE(SUM(utilization_percent), 100) as total_util
        FROM work_center_resources
        WHERE work_center_id = ?
        AND (effective_start_date IS NULL OR effective_start_date <= ?)
        AND (effective_end_date IS NULL OR effective_end_date >= ?)
    ''', (work_center_id, date_to, date_from)).fetchone()
    
    resource_count = conn.execute('''
        SELECT COUNT(*) as count
        FROM work_center_resources
        WHERE work_center_id = ?
        AND (effective_start_date IS NULL OR effective_start_date <= ?)
        AND (effective_end_date IS NULL OR effective_end_date >= ?)
    ''', (work_center_id, date_to, date_from)).fetchone()['count']
    
    if resource_count > 0:
        resource_factor = resource_utilization['total_util'] / 100.0
    else:
        resource_factor = 1.0
    
    return total_hours * efficiency_factor * resource_factor


def generate_work_center_code(conn):
    last_wc = conn.execute('''
        SELECT code FROM work_centers 
        WHERE code LIKE 'WC-%'
        ORDER BY CAST(SUBSTR(code, 4) AS INTEGER) DESC 
        LIMIT 1
    ''').fetchone()
    
    if last_wc:
        try:
            last_number = int(last_wc['code'].split('-')[1])
            next_number = last_number + 1
        except (ValueError, IndexError):
            next_number = 1
    else:
        next_number = 1
    
    return f'WC-{next_number:04d}'


@capacity_bp.route('/capacity')
@login_required
def dashboard():
    db = Database()
    conn = db.get_connection()
    
    date_from = request.args.get('date_from', datetime.now().strftime('%Y-%m-%d'))
    date_to = request.args.get('date_to', (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d'))
    
    work_centers = conn.execute('''
        SELECT wc.*, 
               (SELECT COUNT(*) FROM work_center_resources wcr WHERE wcr.work_center_id = wc.id) as resource_count
        FROM work_centers wc
        WHERE wc.status = 'Active'
        ORDER BY wc.code
    ''').fetchall()
    
    capacity_data = []
    for wc in work_centers:
        operations_load = conn.execute('''
            SELECT COALESCE(SUM(woo.planned_hours + woo.setup_hours), 0) as total_planned
            FROM work_order_operations woo
            JOIN work_orders wo ON woo.work_order_id = wo.id
            WHERE woo.work_center_id = ?
            AND woo.status IN ('Pending', 'In Progress')
            AND wo.status IN ('Planned', 'In Progress', 'Released')
            AND (woo.planned_start_date IS NULL OR woo.planned_start_date <= ?)
            AND (woo.planned_end_date IS NULL OR woo.planned_end_date >= ?)
        ''', (wc['id'], date_to, date_from)).fetchone()
        
        tasks_load = conn.execute('''
            SELECT COALESCE(SUM(wot.planned_hours), 0) as total_planned
            FROM work_order_tasks wot
            JOIN work_orders wo ON wot.work_order_id = wo.id
            WHERE wot.work_center_id = ?
            AND wot.status IN ('Not Started', 'In Progress', 'On Hold')
            AND wo.status IN ('Planned', 'In Progress', 'Released')
            AND (COALESCE(wot.planned_start_date, wo.planned_start_date) IS NULL OR COALESCE(wot.planned_start_date, wo.planned_start_date) <= ?)
            AND (COALESCE(wot.planned_end_date, wo.planned_end_date) IS NULL OR COALESCE(wot.planned_end_date, wo.planned_end_date) >= ?)
        ''', (wc['id'], date_to, date_from)).fetchone()
        
        planned_load = {'total_planned': operations_load['total_planned'] + tasks_load['total_planned']}
        
        available_capacity = calculate_available_capacity(
            conn, wc['id'], date_from, date_to,
            wc['default_hours_per_day'], wc['default_days_per_week'], wc['efficiency_factor']
        )
        
        override_count = conn.execute('''
            SELECT COUNT(*) as count FROM work_center_capacity 
            WHERE work_center_id = ? AND capacity_date BETWEEN ? AND ?
        ''', (wc['id'], date_from, date_to)).fetchone()['count']
        
        utilization = (planned_load['total_planned'] / available_capacity * 100) if available_capacity > 0 else 0
        
        capacity_data.append({
            'work_center': dict(wc),
            'available_capacity': available_capacity,
            'planned_load': planned_load['total_planned'],
            'utilization': round(utilization, 1),
            'override_count': override_count,
            'status': 'Critical' if utilization > 100 else 'Warning' if utilization > 85 else 'Normal'
        })
    
    total_operations = conn.execute('''
        SELECT COUNT(*) as count FROM work_order_operations
        WHERE status IN ('Pending', 'In Progress')
    ''').fetchone()['count']
    
    unassigned_operations = conn.execute('''
        SELECT COUNT(*) as count FROM work_order_operations
        WHERE work_center_id IS NULL
        AND status IN ('Pending', 'In Progress')
    ''').fetchone()['count']
    
    bottleneck_centers = [c for c in capacity_data if c['utilization'] > 85]
    
    conn.close()
    
    return render_template('capacity/dashboard.html',
                          capacity_data=capacity_data,
                          date_from=date_from,
                          date_to=date_to,
                          total_operations=total_operations,
                          unassigned_operations=unassigned_operations,
                          bottleneck_count=len(bottleneck_centers))


@capacity_bp.route('/capacity/work-centers')
@login_required
def list_work_centers():
    db = Database()
    conn = db.get_connection()
    
    status_filter = request.args.get('status', '')
    search = request.args.get('search', '')
    
    query = '''
        SELECT wc.*, 
               (SELECT COUNT(*) FROM work_center_resources wcr WHERE wcr.work_center_id = wc.id) as resource_count,
               (SELECT COUNT(*) FROM work_order_operations woo WHERE woo.work_center_id = wc.id AND woo.status IN ('Pending', 'In Progress')) as active_operations
        FROM work_centers wc
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND wc.status = ?'
        params.append(status_filter)
    
    if search:
        query += ' AND (wc.code LIKE ? OR wc.name LIKE ? OR wc.description LIKE ?)'
        search_param = f'%{search}%'
        params.extend([search_param, search_param, search_param])
    
    query += ' ORDER BY wc.code'
    
    work_centers = conn.execute(query, params).fetchall()
    conn.close()
    
    return render_template('capacity/work_centers_list.html',
                          work_centers=work_centers,
                          status_filter=status_filter,
                          search=search)


@capacity_bp.route('/capacity/work-centers/create', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def create_work_center():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            code = request.form.get('code', '').strip()
            if not code:
                code = generate_work_center_code(conn)
            
            name = request.form.get('name', '').strip()
            description = request.form.get('description', '').strip()
            default_hours_per_day = float(request.form.get('default_hours_per_day', 8.0))
            default_days_per_week = int(request.form.get('default_days_per_week', 5))
            efficiency_factor = float(request.form.get('efficiency_factor', 1.0))
            cost_per_hour = float(request.form.get('cost_per_hour', 0))
            status = request.form.get('status', 'Active')
            
            if not name:
                flash('Work center name is required', 'danger')
                conn.close()
                return render_template('capacity/work_center_form.html', action='create')
            
            conn.execute('''
                INSERT INTO work_centers (code, name, description, default_hours_per_day, 
                                         default_days_per_week, efficiency_factor, cost_per_hour, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (code, name, description, default_hours_per_day, default_days_per_week, 
                  efficiency_factor, cost_per_hour, status))
            
            wc_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
            
            AuditLogger.log_change(
                conn=conn,
                record_type='work_center',
                record_id=wc_id,
                action_type='Created',
                modified_by=session.get('user_id'),
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
            
            conn.commit()
            conn.close()
            
            flash(f'Work Center {code} created successfully!', 'success')
            return redirect(url_for('capacity_routes.view_work_center', id=wc_id))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error creating work center: {str(e)}', 'danger')
            return redirect(url_for('capacity_routes.create_work_center'))
    
    next_code = generate_work_center_code(conn)
    conn.close()
    
    return render_template('capacity/work_center_form.html', action='create', next_code=next_code)


@capacity_bp.route('/capacity/work-centers/<int:id>')
@login_required
def view_work_center(id):
    db = Database()
    conn = db.get_connection()
    
    work_center = conn.execute('SELECT * FROM work_centers WHERE id = ?', (id,)).fetchone()
    
    if not work_center:
        conn.close()
        flash('Work center not found', 'danger')
        return redirect(url_for('capacity_routes.list_work_centers'))
    
    resources = conn.execute('''
        SELECT wcr.*, lr.employee_code, lr.first_name, lr.last_name, lr.role, lr.hourly_rate
        FROM work_center_resources wcr
        JOIN labor_resources lr ON wcr.labor_resource_id = lr.id
        WHERE wcr.work_center_id = ?
        ORDER BY wcr.is_primary DESC, lr.last_name, lr.first_name
    ''', (id,)).fetchall()
    
    operations = conn.execute('''
        SELECT woo.*, wo.wo_number, p.code as product_code, p.name as product_name
        FROM work_order_operations woo
        JOIN work_orders wo ON woo.work_order_id = wo.id
        JOIN products p ON wo.product_id = p.id
        WHERE woo.work_center_id = ?
        ORDER BY woo.planned_start_date DESC, woo.operation_seq
        LIMIT 50
    ''', (id,)).fetchall()
    
    capacity_overrides = conn.execute('''
        SELECT * FROM work_center_capacity
        WHERE work_center_id = ?
        AND capacity_date >= date('now')
        ORDER BY capacity_date
        LIMIT 30
    ''', (id,)).fetchall()
    
    weekly_hours = work_center['default_hours_per_day'] * work_center['default_days_per_week']
    
    pending_load = conn.execute('''
        SELECT COALESCE(SUM(planned_hours + setup_hours), 0) as total
        FROM work_order_operations
        WHERE work_center_id = ?
        AND status IN ('Pending', 'In Progress')
    ''', (id,)).fetchone()['total']
    
    conn.close()
    
    return render_template('capacity/work_center_detail.html',
                          work_center=work_center,
                          resources=resources,
                          operations=operations,
                          capacity_overrides=capacity_overrides,
                          weekly_hours=weekly_hours,
                          pending_load=pending_load)


@capacity_bp.route('/capacity/work-centers/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def edit_work_center(id):
    db = Database()
    conn = db.get_connection()
    
    work_center = conn.execute('SELECT * FROM work_centers WHERE id = ?', (id,)).fetchone()
    
    if not work_center:
        conn.close()
        flash('Work center not found', 'danger')
        return redirect(url_for('capacity_routes.list_work_centers'))
    
    if request.method == 'POST':
        try:
            old_record = dict(work_center)
            
            name = request.form.get('name', '').strip()
            description = request.form.get('description', '').strip()
            default_hours_per_day = float(request.form.get('default_hours_per_day', 8.0))
            default_days_per_week = int(request.form.get('default_days_per_week', 5))
            efficiency_factor = float(request.form.get('efficiency_factor', 1.0))
            cost_per_hour = float(request.form.get('cost_per_hour', 0))
            status = request.form.get('status', 'Active')
            
            if not name:
                flash('Work center name is required', 'danger')
                conn.close()
                return render_template('capacity/work_center_form.html', action='edit', work_center=work_center)
            
            conn.execute('''
                UPDATE work_centers 
                SET name = ?, description = ?, default_hours_per_day = ?,
                    default_days_per_week = ?, efficiency_factor = ?, cost_per_hour = ?,
                    status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (name, description, default_hours_per_day, default_days_per_week,
                  efficiency_factor, cost_per_hour, status, id))
            
            new_record = conn.execute('SELECT * FROM work_centers WHERE id = ?', (id,)).fetchone()
            changes = AuditLogger.compare_records(old_record, dict(new_record))
            
            if changes:
                AuditLogger.log_change(
                    conn=conn,
                    record_type='work_center',
                    record_id=id,
                    action_type='Updated',
                    modified_by=session.get('user_id'),
                    changed_fields=changes,
                    ip_address=request.remote_addr,
                    user_agent=request.headers.get('User-Agent')
                )
            
            conn.commit()
            conn.close()
            
            flash('Work center updated successfully!', 'success')
            return redirect(url_for('capacity_routes.view_work_center', id=id))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error updating work center: {str(e)}', 'danger')
            return redirect(url_for('capacity_routes.edit_work_center', id=id))
    
    conn.close()
    return render_template('capacity/work_center_form.html', action='edit', work_center=work_center)


@capacity_bp.route('/capacity/work-centers/<int:id>/delete', methods=['POST'])
@role_required('Admin')
def delete_work_center(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        operation_count = conn.execute('''
            SELECT COUNT(*) as count FROM work_order_operations 
            WHERE work_center_id = ? AND status IN ('Pending', 'In Progress')
        ''', (id,)).fetchone()['count']
        
        if operation_count > 0:
            flash(f'Cannot delete work center with {operation_count} active operations', 'danger')
            conn.close()
            return redirect(url_for('capacity_routes.view_work_center', id=id))
        
        work_center = conn.execute('SELECT code FROM work_centers WHERE id = ?', (id,)).fetchone()
        
        conn.execute('DELETE FROM work_center_resources WHERE work_center_id = ?', (id,))
        conn.execute('DELETE FROM work_center_capacity WHERE work_center_id = ?', (id,))
        conn.execute('DELETE FROM work_centers WHERE id = ?', (id,))
        
        AuditLogger.log_change(
            conn=conn,
            record_type='work_center',
            record_id=id,
            action_type='Deleted',
            modified_by=session.get('user_id'),
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        conn.close()
        
        flash(f'Work Center {work_center["code"]} deleted successfully!', 'success')
        return redirect(url_for('capacity_routes.list_work_centers'))
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error deleting work center: {str(e)}', 'danger')
        return redirect(url_for('capacity_routes.view_work_center', id=id))


@capacity_bp.route('/capacity/work-centers/<int:id>/resources', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def manage_resources(id):
    db = Database()
    conn = db.get_connection()
    
    work_center = conn.execute('SELECT * FROM work_centers WHERE id = ?', (id,)).fetchone()
    
    if not work_center:
        conn.close()
        flash('Work center not found', 'danger')
        return redirect(url_for('capacity_routes.list_work_centers'))
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'add':
            labor_resource_id = int(request.form.get('labor_resource_id', 0))
            utilization_percent = float(request.form.get('utilization_percent', 100.0))
            is_primary = 1 if request.form.get('is_primary') else 0
            effective_start_date = request.form.get('effective_start_date') or None
            effective_end_date = request.form.get('effective_end_date') or None
            
            try:
                conn.execute('''
                    INSERT INTO work_center_resources 
                    (work_center_id, labor_resource_id, utilization_percent, is_primary, 
                     effective_start_date, effective_end_date)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (id, labor_resource_id, utilization_percent, is_primary,
                      effective_start_date, effective_end_date))
                conn.commit()
                flash('Resource assigned successfully!', 'success')
            except Exception as e:
                conn.rollback()
                flash(f'Error assigning resource: {str(e)}', 'danger')
        
        elif action == 'remove':
            resource_id = int(request.form.get('resource_id', 0))
            conn.execute('DELETE FROM work_center_resources WHERE id = ?', (resource_id,))
            conn.commit()
            flash('Resource removed successfully!', 'success')
        
        elif action == 'update':
            resource_id = int(request.form.get('resource_id', 0))
            utilization_percent = float(request.form.get('utilization_percent', 100.0))
            is_primary = 1 if request.form.get('is_primary') else 0
            effective_start_date = request.form.get('effective_start_date') or None
            effective_end_date = request.form.get('effective_end_date') or None
            
            conn.execute('''
                UPDATE work_center_resources 
                SET utilization_percent = ?, is_primary = ?, 
                    effective_start_date = ?, effective_end_date = ?
                WHERE id = ?
            ''', (utilization_percent, is_primary, effective_start_date, effective_end_date, resource_id))
            conn.commit()
            flash('Resource updated successfully!', 'success')
        
        conn.close()
        return redirect(url_for('capacity_routes.manage_resources', id=id))
    
    assigned_resources = conn.execute('''
        SELECT wcr.*, lr.employee_code, lr.first_name, lr.last_name, lr.role, lr.hourly_rate, lr.status as lr_status
        FROM work_center_resources wcr
        JOIN labor_resources lr ON wcr.labor_resource_id = lr.id
        WHERE wcr.work_center_id = ?
        ORDER BY wcr.is_primary DESC, lr.last_name, lr.first_name
    ''', (id,)).fetchall()
    
    assigned_ids = [r['labor_resource_id'] for r in assigned_resources]
    
    if assigned_ids:
        placeholders = ','.join(['?' for _ in assigned_ids])
        available_resources = conn.execute(f'''
            SELECT * FROM labor_resources 
            WHERE status = 'Active' AND id NOT IN ({placeholders})
            ORDER BY last_name, first_name
        ''', assigned_ids).fetchall()
    else:
        available_resources = conn.execute('''
            SELECT * FROM labor_resources 
            WHERE status = 'Active'
            ORDER BY last_name, first_name
        ''').fetchall()
    
    conn.close()
    
    return render_template('capacity/manage_resources.html',
                          work_center=work_center,
                          assigned_resources=assigned_resources,
                          available_resources=available_resources)


@capacity_bp.route('/capacity/work-centers/<int:id>/capacity-overrides', methods=['POST'])
@role_required('Admin', 'Planner')
def add_capacity_override(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        capacity_date = request.form.get('capacity_date')
        available_hours = float(request.form.get('available_hours', 0))
        override_reason = request.form.get('override_reason', '').strip()
        
        conn.execute('''
            INSERT OR REPLACE INTO work_center_capacity 
            (work_center_id, capacity_date, available_hours, override_reason)
            VALUES (?, ?, ?, ?)
        ''', (id, capacity_date, available_hours, override_reason))
        
        conn.commit()
        conn.close()
        
        flash('Capacity override added successfully!', 'success')
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error adding capacity override: {str(e)}', 'danger')
    
    return redirect(url_for('capacity_routes.view_work_center', id=id))


@capacity_bp.route('/capacity/operations')
@login_required
def list_operations():
    db = Database()
    conn = db.get_connection()
    
    work_center_filter = request.args.get('work_center', '')
    status_filter = request.args.get('status', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    
    query = '''
        SELECT woo.*, wo.wo_number, wo.status as wo_status, wo.priority,
               p.code as product_code, p.name as product_name,
               wc.code as wc_code, wc.name as wc_name
        FROM work_order_operations woo
        JOIN work_orders wo ON woo.work_order_id = wo.id
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN work_centers wc ON woo.work_center_id = wc.id
        WHERE 1=1
    '''
    params = []
    
    if work_center_filter:
        query += ' AND woo.work_center_id = ?'
        params.append(int(work_center_filter))
    
    if status_filter:
        query += ' AND woo.status = ?'
        params.append(status_filter)
    
    if date_from:
        query += ' AND woo.planned_start_date >= ?'
        params.append(date_from)
    
    if date_to:
        query += ' AND woo.planned_end_date <= ?'
        params.append(date_to)
    
    query += ' ORDER BY woo.planned_start_date, woo.operation_seq'
    
    operations = conn.execute(query, params).fetchall()
    
    work_centers = conn.execute('SELECT id, code, name FROM work_centers WHERE status = "Active" ORDER BY code').fetchall()
    
    conn.close()
    
    return render_template('capacity/operations_list.html',
                          operations=operations,
                          work_centers=work_centers,
                          work_center_filter=work_center_filter,
                          status_filter=status_filter,
                          date_from=date_from,
                          date_to=date_to)


@capacity_bp.route('/capacity/operations/<int:wo_id>/add', methods=['GET', 'POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def add_operation(wo_id):
    db = Database()
    conn = db.get_connection()
    
    work_order = conn.execute('''
        SELECT wo.*, p.code as product_code, p.name as product_name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.id = ?
    ''', (wo_id,)).fetchone()
    
    if not work_order:
        conn.close()
        flash('Work order not found', 'danger')
        return redirect(url_for('workorder_routes.list_workorders'))
    
    if request.method == 'POST':
        try:
            operation_name = request.form.get('operation_name', '').strip()
            work_center_id = request.form.get('work_center_id') or None
            if work_center_id:
                work_center_id = int(work_center_id)
            
            operation_seq = int(request.form.get('operation_seq', 10))
            planned_hours = float(request.form.get('planned_hours', 0))
            setup_hours = float(request.form.get('setup_hours', 0))
            planned_start_date = request.form.get('planned_start_date') or None
            planned_end_date = request.form.get('planned_end_date') or None
            notes = request.form.get('notes', '').strip()
            
            if not operation_name:
                flash('Operation name is required', 'danger')
                work_centers = conn.execute('SELECT id, code, name FROM work_centers WHERE status = "Active" ORDER BY code').fetchall()
                conn.close()
                return render_template('capacity/operation_form.html', action='add', work_order=work_order, work_centers=work_centers)
            
            conn.execute('''
                INSERT INTO work_order_operations 
                (work_order_id, operation_name, work_center_id, operation_seq, planned_hours, 
                 setup_hours, planned_start_date, planned_end_date, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (wo_id, operation_name, work_center_id, operation_seq, planned_hours,
                  setup_hours, planned_start_date, planned_end_date, notes))
            
            conn.commit()
            conn.close()
            
            flash('Operation added successfully!', 'success')
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error adding operation: {str(e)}', 'danger')
            return redirect(url_for('capacity_routes.add_operation', wo_id=wo_id))
    
    work_centers = conn.execute('SELECT id, code, name FROM work_centers WHERE status = "Active" ORDER BY code').fetchall()
    
    last_seq = conn.execute('''
        SELECT MAX(operation_seq) as max_seq FROM work_order_operations WHERE work_order_id = ?
    ''', (wo_id,)).fetchone()['max_seq'] or 0
    next_seq = last_seq + 10
    
    conn.close()
    
    return render_template('capacity/operation_form.html', 
                          action='add', 
                          work_order=work_order, 
                          work_centers=work_centers,
                          next_seq=next_seq)


@capacity_bp.route('/capacity/operations/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def edit_operation(id):
    db = Database()
    conn = db.get_connection()
    
    operation = conn.execute('''
        SELECT woo.*, wo.wo_number, p.code as product_code, p.name as product_name
        FROM work_order_operations woo
        JOIN work_orders wo ON woo.work_order_id = wo.id
        JOIN products p ON wo.product_id = p.id
        WHERE woo.id = ?
    ''', (id,)).fetchone()
    
    if not operation:
        conn.close()
        flash('Operation not found', 'danger')
        return redirect(url_for('capacity_routes.list_operations'))
    
    if request.method == 'POST':
        try:
            operation_name = request.form.get('operation_name', '').strip()
            work_center_id = request.form.get('work_center_id') or None
            if work_center_id:
                work_center_id = int(work_center_id)
            
            operation_seq = int(request.form.get('operation_seq', 10))
            planned_hours = float(request.form.get('planned_hours', 0))
            setup_hours = float(request.form.get('setup_hours', 0))
            planned_start_date = request.form.get('planned_start_date') or None
            planned_end_date = request.form.get('planned_end_date') or None
            actual_hours = float(request.form.get('actual_hours', 0))
            status = request.form.get('status', 'Pending')
            notes = request.form.get('notes', '').strip()
            
            conn.execute('''
                UPDATE work_order_operations 
                SET operation_name = ?, work_center_id = ?, operation_seq = ?,
                    planned_hours = ?, setup_hours = ?, planned_start_date = ?,
                    planned_end_date = ?, actual_hours = ?, status = ?, notes = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (operation_name, work_center_id, operation_seq, planned_hours,
                  setup_hours, planned_start_date, planned_end_date, actual_hours,
                  status, notes, id))
            
            conn.commit()
            conn.close()
            
            flash('Operation updated successfully!', 'success')
            return redirect(url_for('workorder_routes.view_workorder', id=operation['work_order_id']))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error updating operation: {str(e)}', 'danger')
            return redirect(url_for('capacity_routes.edit_operation', id=id))
    
    work_centers = conn.execute('SELECT id, code, name FROM work_centers WHERE status = "Active" ORDER BY code').fetchall()
    conn.close()
    
    return render_template('capacity/operation_form.html', 
                          action='edit', 
                          operation=operation,
                          work_centers=work_centers)


@capacity_bp.route('/capacity/operations/<int:id>/delete', methods=['POST'])
@role_required('Admin', 'Planner')
def delete_operation(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        operation = conn.execute('SELECT work_order_id FROM work_order_operations WHERE id = ?', (id,)).fetchone()
        
        if not operation:
            flash('Operation not found', 'danger')
            conn.close()
            return redirect(url_for('capacity_routes.list_operations'))
        
        wo_id = operation['work_order_id']
        
        conn.execute('DELETE FROM work_order_operations WHERE id = ?', (id,))
        conn.commit()
        conn.close()
        
        flash('Operation deleted successfully!', 'success')
        return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error deleting operation: {str(e)}', 'danger')
        return redirect(url_for('capacity_routes.list_operations'))


@capacity_bp.route('/capacity/api/utilization')
@login_required
def api_utilization():
    db = Database()
    conn = db.get_connection()
    
    date_from = request.args.get('date_from', datetime.now().strftime('%Y-%m-%d'))
    date_to = request.args.get('date_to', (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d'))
    
    work_centers = conn.execute('''
        SELECT * FROM work_centers WHERE status = 'Active' ORDER BY code
    ''').fetchall()
    
    data = []
    for wc in work_centers:
        operations_load = conn.execute('''
            SELECT COALESCE(SUM(woo.planned_hours + woo.setup_hours), 0) as total_planned
            FROM work_order_operations woo
            JOIN work_orders wo ON woo.work_order_id = wo.id
            WHERE woo.work_center_id = ?
            AND woo.status IN ('Pending', 'In Progress')
            AND wo.status IN ('Planned', 'In Progress', 'Released')
            AND (woo.planned_start_date IS NULL OR woo.planned_start_date <= ?)
            AND (woo.planned_end_date IS NULL OR woo.planned_end_date >= ?)
        ''', (wc['id'], date_to, date_from)).fetchone()
        
        tasks_load = conn.execute('''
            SELECT COALESCE(SUM(wot.planned_hours), 0) as total_planned
            FROM work_order_tasks wot
            JOIN work_orders wo ON wot.work_order_id = wo.id
            WHERE wot.work_center_id = ?
            AND wot.status IN ('Not Started', 'In Progress', 'On Hold')
            AND wo.status IN ('Planned', 'In Progress', 'Released')
            AND (COALESCE(wot.planned_start_date, wo.planned_start_date) IS NULL OR COALESCE(wot.planned_start_date, wo.planned_start_date) <= ?)
            AND (COALESCE(wot.planned_end_date, wo.planned_end_date) IS NULL OR COALESCE(wot.planned_end_date, wo.planned_end_date) >= ?)
        ''', (wc['id'], date_to, date_from)).fetchone()
        
        total_planned = operations_load['total_planned'] + tasks_load['total_planned']
        
        available = calculate_available_capacity(
            conn, wc['id'], date_from, date_to,
            wc['default_hours_per_day'], wc['default_days_per_week'], wc['efficiency_factor']
        )
        utilization = (total_planned / available * 100) if available > 0 else 0
        
        data.append({
            'code': wc['code'],
            'name': wc['name'],
            'available': round(available, 1),
            'planned': round(total_planned, 1),
            'utilization': round(utilization, 1)
        })
    
    conn.close()
    return jsonify(data)


@capacity_bp.route('/capacity/report')
@login_required
def capacity_report():
    db = Database()
    conn = db.get_connection()
    
    date_from = request.args.get('date_from', datetime.now().strftime('%Y-%m-%d'))
    date_to = request.args.get('date_to', (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d'))
    
    work_centers = conn.execute('''
        SELECT wc.*, 
               (SELECT COUNT(*) FROM work_center_resources wcr WHERE wcr.work_center_id = wc.id) as resource_count
        FROM work_centers wc
        WHERE wc.status = 'Active'
        ORDER BY wc.code
    ''').fetchall()
    
    report_data = []
    for wc in work_centers:
        operations = conn.execute('''
            SELECT woo.*, wo.wo_number, wo.priority, p.code as product_code
            FROM work_order_operations woo
            JOIN work_orders wo ON woo.work_order_id = wo.id
            JOIN products p ON wo.product_id = p.id
            WHERE woo.work_center_id = ?
            AND woo.status IN ('Pending', 'In Progress')
            AND wo.status IN ('Planned', 'In Progress', 'Released')
            AND (woo.planned_start_date IS NULL OR woo.planned_start_date <= ?)
            AND (woo.planned_end_date IS NULL OR woo.planned_end_date >= ?)
            ORDER BY wo.priority DESC, woo.planned_start_date
        ''', (wc['id'], date_to, date_from)).fetchall()
        
        tasks = conn.execute('''
            SELECT wot.*, wo.wo_number, wo.priority, p.code as product_code
            FROM work_order_tasks wot
            JOIN work_orders wo ON wot.work_order_id = wo.id
            JOIN products p ON wo.product_id = p.id
            WHERE wot.work_center_id = ?
            AND wot.status IN ('Not Started', 'In Progress', 'On Hold')
            AND wo.status IN ('Planned', 'In Progress', 'Released')
            AND (COALESCE(wot.planned_start_date, wo.planned_start_date) IS NULL OR COALESCE(wot.planned_start_date, wo.planned_start_date) <= ?)
            AND (COALESCE(wot.planned_end_date, wo.planned_end_date) IS NULL OR COALESCE(wot.planned_end_date, wo.planned_end_date) >= ?)
            ORDER BY wo.priority DESC, wot.planned_start_date
        ''', (wc['id'], date_to, date_from)).fetchall()
        
        operations_planned = sum(op['planned_hours'] + op['setup_hours'] for op in operations)
        tasks_planned = sum(t['planned_hours'] for t in tasks)
        total_planned = operations_planned + tasks_planned
        
        available = calculate_available_capacity(
            conn, wc['id'], date_from, date_to,
            wc['default_hours_per_day'], wc['default_days_per_week'], wc['efficiency_factor']
        )
        utilization = (total_planned / available * 100) if available > 0 else 0
        
        overrides = conn.execute('''
            SELECT * FROM work_center_capacity 
            WHERE work_center_id = ? AND capacity_date BETWEEN ? AND ?
            ORDER BY capacity_date
        ''', (wc['id'], date_from, date_to)).fetchall()
        
        report_data.append({
            'work_center': dict(wc),
            'operations': [dict(op) for op in operations],
            'tasks': [dict(t) for t in tasks],
            'overrides': [dict(o) for o in overrides],
            'total_planned': total_planned,
            'operations_planned': operations_planned,
            'tasks_planned': tasks_planned,
            'available_capacity': available,
            'utilization': round(utilization, 1),
            'status': 'Critical' if utilization > 100 else 'Warning' if utilization > 85 else 'Normal'
        })
    
    conn.close()
    
    return render_template('capacity/capacity_report.html',
                          report_data=report_data,
                          date_from=date_from,
                          date_to=date_to)
