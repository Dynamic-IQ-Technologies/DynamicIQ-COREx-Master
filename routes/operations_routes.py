from flask import Blueprint, render_template, request, session
from models import Database
from auth import login_required, role_required
from datetime import datetime, timedelta

operations_bp = Blueprint('operations_routes', __name__)

@operations_bp.route('/operations-dashboard')
@login_required
@role_required('Admin', 'Planner', 'Production Staff', 'Accountant')
def operations_dashboard():
    db = Database()
    conn = db.get_connection()
    
    today = datetime.now()
    today_str = today.strftime('%Y-%m-%d')
    
    wo_overview = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Planned' THEN 1 ELSE 0 END) as planned,
            SUM(CASE WHEN status = 'Released' THEN 1 ELSE 0 END) as released,
            SUM(CASE WHEN status = 'In Progress' THEN 1 ELSE 0 END) as in_progress,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN status NOT IN ('Completed', 'Cancelled') THEN 1 ELSE 0 END) as active
        FROM work_orders
    ''').fetchone()
    
    stage_load_raw = conn.execute('''
        SELECT 
            wos.id,
            wos.name as stage_name,
            wos.color,
            wos.sequence,
            COUNT(wo.id) as wo_count,
            SUM(CASE WHEN wo.status = 'In Progress' THEN 1 ELSE 0 END) as in_progress_count,
            COALESCE(SUM(wo.quantity), 0) as total_qty,
            COALESCE(AVG(
                CASE WHEN wo.actual_end_date IS NOT NULL AND wo.created_at IS NOT NULL 
                THEN julianday(wo.actual_end_date) - julianday(date(wo.created_at))
                ELSE NULL END
            ), 0) as avg_tat_days,
            COALESCE((
                SELECT SUM(wot.planned_hours)
                FROM work_order_tasks wot
                WHERE wot.work_order_id IN (
                    SELECT wo2.id FROM work_orders wo2 
                    WHERE wo2.stage_id = wos.id 
                    AND wo2.status NOT IN ('Completed', 'Cancelled')
                )
            ), 0) as total_planned_hours,
            COALESCE((
                SELECT COUNT(DISTINCT wot.assigned_resource_id)
                FROM work_order_tasks wot
                WHERE wot.work_order_id IN (
                    SELECT wo2.id FROM work_orders wo2 
                    WHERE wo2.stage_id = wos.id 
                    AND wo2.status NOT IN ('Completed', 'Cancelled')
                )
                AND wot.assigned_resource_id IS NOT NULL
            ), 0) as assigned_resources
        FROM work_order_stages wos
        LEFT JOIN work_orders wo ON wos.id = wo.stage_id AND wo.status NOT IN ('Completed', 'Cancelled')
        WHERE wos.is_active = 1
        GROUP BY wos.id, wos.name, wos.color, wos.sequence
        ORDER BY wos.sequence
    ''').fetchall()
    
    total_resources = conn.execute('SELECT COUNT(*) as count FROM labor_resources WHERE status = "Active"').fetchone()
    total_resource_count = total_resources['count'] or 1
    
    stage_load = [{
        'id': row['id'],
        'stage_name': row['stage_name'],
        'color': row['color'],
        'sequence': row['sequence'],
        'wo_count': row['wo_count'],
        'in_progress_count': row['in_progress_count'],
        'total_qty': row['total_qty'],
        'avg_tat_days': round(row['avg_tat_days'], 1) if row['avg_tat_days'] else 0,
        'total_planned_hours': round(row['total_planned_hours'], 1) if row['total_planned_hours'] else 0,
        'assigned_resources': row['assigned_resources'] or 0,
        'capacity_pct': min(100, round((row['assigned_resources'] or 0) / total_resource_count * 100)) if total_resource_count > 0 else 0
    } for row in stage_load_raw]
    
    tat_metrics = conn.execute('''
        SELECT 
            AVG(CASE 
                WHEN actual_end_date IS NOT NULL AND created_at IS NOT NULL 
                THEN julianday(actual_end_date) - julianday(date(created_at))
                ELSE NULL 
            END) as avg_tat_days,
            AVG(CASE 
                WHEN planned_end_date IS NOT NULL AND planned_start_date IS NOT NULL 
                THEN julianday(planned_end_date) - julianday(planned_start_date)
                ELSE NULL 
            END) as avg_planned_tat,
            COUNT(CASE 
                WHEN status NOT IN ('Completed', 'Cancelled') 
                AND planned_end_date < ? THEN 1 
            END) as overdue_count,
            COUNT(CASE 
                WHEN status NOT IN ('Completed', 'Cancelled') 
                AND planned_end_date BETWEEN ? AND date(?, '+7 days') THEN 1 
            END) as due_this_week
        FROM work_orders
    ''', (today_str, today_str, today_str)).fetchone()
    
    task_progress = conn.execute('''
        SELECT 
            COUNT(*) as total_tasks,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed_tasks,
            SUM(CASE WHEN status = 'In Progress' THEN 1 ELSE 0 END) as in_progress_tasks,
            SUM(planned_hours) as total_planned_hours,
            SUM(actual_hours) as total_actual_hours
        FROM work_order_tasks
        WHERE work_order_id IN (SELECT id FROM work_orders WHERE status NOT IN ('Completed', 'Cancelled'))
    ''').fetchone()
    
    task_completion_pct = 0
    if task_progress['total_tasks'] and task_progress['total_tasks'] > 0:
        task_completion_pct = round((task_progress['completed_tasks'] or 0) / task_progress['total_tasks'] * 100, 1)
    
    etc_data = conn.execute('''
        SELECT 
            wo.id,
            wo.wo_number,
            wo.planned_end_date,
            wo.status,
            p.code as product_code,
            p.name as product_name,
            wos.name as stage_name,
            wos.color as stage_color,
            COALESCE(
                (SELECT SUM(planned_hours) FROM work_order_tasks WHERE work_order_id = wo.id),
                0
            ) as total_planned_hours,
            COALESCE(
                (SELECT SUM(actual_hours) FROM work_order_tasks WHERE work_order_id = wo.id),
                0
            ) as total_actual_hours,
            COALESCE(
                (SELECT SUM(planned_hours) FROM work_order_tasks WHERE work_order_id = wo.id AND status != 'Completed'),
                0
            ) as remaining_hours,
            julianday(wo.planned_end_date) - julianday(?) as days_remaining
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN work_order_stages wos ON wo.stage_id = wos.id
        WHERE wo.status NOT IN ('Completed', 'Cancelled')
        ORDER BY wo.planned_end_date ASC NULLS LAST
        LIMIT 15
    ''', (today_str,)).fetchall()
    
    disposition_breakdown = conn.execute('''
        SELECT 
            disposition,
            COUNT(*) as count,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN status = 'In Progress' THEN 1 ELSE 0 END) as in_progress
        FROM work_orders
        WHERE status NOT IN ('Cancelled')
        GROUP BY disposition
        ORDER BY count DESC
    ''').fetchall()
    
    category_breakdown = conn.execute('''
        SELECT 
            COALESCE(repair_category, 'Not Assigned') as category,
            COUNT(*) as count,
            SUM(CASE WHEN status NOT IN ('Completed', 'Cancelled') THEN 1 ELSE 0 END) as active
        FROM work_orders
        GROUP BY repair_category
    ''').fetchall()
    
    type_breakdown = conn.execute('''
        SELECT 
            COALESCE(workorder_type, 'Not Assigned') as wo_type,
            COUNT(*) as count,
            SUM(CASE WHEN status NOT IN ('Completed', 'Cancelled') THEN 1 ELSE 0 END) as active
        FROM work_orders
        GROUP BY workorder_type
    ''').fetchall()
    
    labor_utilization = conn.execute('''
        SELECT 
            lr.id,
            lr.first_name || ' ' || lr.last_name as name,
            lr.employee_code,
            COUNT(DISTINCT wot.id) as assigned_tasks,
            COALESCE(SUM(wot.actual_hours), 0) as total_hours,
            (SELECT COUNT(*) FROM time_clock_punches tcp 
             WHERE tcp.employee_id = lr.id 
             AND tcp.punch_type = 'Clock In'
             AND NOT EXISTS (
                 SELECT 1 FROM time_clock_punches tcp2 
                 WHERE tcp2.employee_id = lr.id 
                 AND tcp2.punch_type = 'Clock Out' 
                 AND tcp2.punch_time > tcp.punch_time
             )) as currently_clocked
        FROM labor_resources lr
        LEFT JOIN work_order_tasks wot ON lr.id = wot.assigned_resource_id 
            AND wot.status NOT IN ('Completed', 'Cancelled')
        WHERE lr.status = 'Active'
        GROUP BY lr.id, lr.first_name, lr.last_name, lr.employee_code
        ORDER BY assigned_tasks DESC
        LIMIT 10
    ''').fetchall()
    
    weekly_completion = conn.execute('''
        SELECT 
            strftime('%Y-%W', actual_end_date) as week,
            COUNT(*) as completed_count
        FROM work_orders
        WHERE status = 'Completed'
        AND actual_end_date >= date('now', '-8 weeks')
        GROUP BY strftime('%Y-%W', actual_end_date)
        ORDER BY week
    ''').fetchall()
    
    monthly_wo_trend_raw = conn.execute('''
        SELECT 
            strftime('%Y-%m', created_at) as month,
            COUNT(*) as created,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed
        FROM work_orders
        WHERE created_at >= date('now', '-6 months')
        GROUP BY strftime('%Y-%m', created_at)
        ORDER BY month
    ''').fetchall()
    monthly_wo_trend = [{'month': row['month'], 'created': row['created'], 'completed': row['completed']} for row in monthly_wo_trend_raw]
    
    on_time_delivery = conn.execute('''
        SELECT 
            COUNT(*) as total_completed,
            SUM(CASE WHEN actual_end_date <= planned_end_date THEN 1 ELSE 0 END) as on_time,
            SUM(CASE WHEN actual_end_date > planned_end_date THEN 1 ELSE 0 END) as late
        FROM work_orders
        WHERE status = 'Completed'
        AND actual_end_date IS NOT NULL
        AND planned_end_date IS NOT NULL
        AND created_at >= date('now', '-90 days')
    ''').fetchone()
    
    otd_rate = 0
    if on_time_delivery['total_completed'] and on_time_delivery['total_completed'] > 0:
        otd_rate = round((on_time_delivery['on_time'] or 0) / on_time_delivery['total_completed'] * 100, 1)
    
    material_shortage = conn.execute('''
        SELECT COUNT(DISTINCT mr.work_order_id) as wo_with_shortage
        FROM material_requirements mr
        LEFT JOIN (
            SELECT product_id, SUM(quantity) as available 
            FROM inventory 
            GROUP BY product_id
        ) inv ON mr.product_id = inv.product_id
        WHERE mr.required_quantity > COALESCE(inv.available, 0)
        AND mr.work_order_id IN (SELECT id FROM work_orders WHERE status NOT IN ('Completed', 'Cancelled'))
    ''').fetchone()
    
    sched_status = request.args.get('sched_status', '')
    sched_stage = request.args.get('sched_stage', '')
    sched_priority = request.args.get('sched_priority', '')
    sched_date_from = request.args.get('sched_date_from', '')
    sched_date_to = request.args.get('sched_date_to', '')
    sched_sort = request.args.get('sched_sort', 'planned_start_date')
    sched_order = request.args.get('sched_order', 'asc')
    
    valid_sort_cols = ['wo_number', 'planned_start_date', 'planned_end_date', 'priority', 'status']
    if sched_sort not in valid_sort_cols:
        sched_sort = 'planned_start_date'
    if sched_order not in ['asc', 'desc']:
        sched_order = 'asc'
    
    schedule_query = '''
        SELECT 
            wo.id,
            wo.wo_number,
            wo.status,
            wo.planned_start_date,
            wo.planned_end_date,
            wo.actual_start_date,
            wo.actual_end_date,
            wo.priority,
            p.code as product_code,
            p.name as product_name,
            wos.name as stage_name,
            wos.color as stage_color,
            c.name as customer_name
        FROM work_orders wo
        LEFT JOIN products p ON wo.product_id = p.id
        LEFT JOIN work_order_stages wos ON wo.stage_id = wos.id
        LEFT JOIN customers c ON wo.customer_id = c.id
        WHERE wo.status NOT IN ('Completed', 'Cancelled')
        AND (wo.planned_start_date IS NOT NULL OR wo.planned_end_date IS NOT NULL)
    '''
    sched_params = []
    
    if sched_status:
        schedule_query += ' AND wo.status = ?'
        sched_params.append(sched_status)
    
    if sched_stage:
        schedule_query += ' AND wo.stage_id = ?'
        sched_params.append(sched_stage)
    
    if sched_priority:
        schedule_query += ' AND wo.priority = ?'
        sched_params.append(sched_priority)
    
    if sched_date_from:
        schedule_query += ' AND wo.planned_start_date >= ?'
        sched_params.append(sched_date_from)
    
    if sched_date_to:
        schedule_query += ' AND wo.planned_end_date <= ?'
        sched_params.append(sched_date_to)
    
    schedule_query += f' ORDER BY wo.{sched_sort} {sched_order.upper()}'
    schedule_query += ' LIMIT 100'
    
    schedule_data_raw = conn.execute(schedule_query, sched_params).fetchall()
    
    schedule_stages = conn.execute('''
        SELECT id, name FROM work_order_stages WHERE is_active = 1 ORDER BY sequence
    ''').fetchall()
    
    schedule_statuses = ['Planned', 'Released', 'In Progress']
    schedule_priorities = ['Low', 'Medium', 'High', 'Critical']
    
    schedule_data = [{
        'id': row['id'],
        'wo_number': row['wo_number'],
        'status': row['status'],
        'planned_start_date': row['planned_start_date'],
        'planned_end_date': row['planned_end_date'],
        'actual_start_date': row['actual_start_date'],
        'actual_end_date': row['actual_end_date'],
        'priority': row['priority'],
        'product_code': row['product_code'],
        'product_name': row['product_name'],
        'stage_name': row['stage_name'],
        'stage_color': row['stage_color'],
        'customer_name': row['customer_name']
    } for row in schedule_data_raw]
    
    schedule_stages_list = [{'id': s['id'], 'name': s['name']} for s in schedule_stages]
    
    conn.close()
    
    return render_template('operations/dashboard.html',
                         wo_overview=wo_overview,
                         stage_load=stage_load,
                         tat_metrics=tat_metrics,
                         task_progress=task_progress,
                         task_completion_pct=task_completion_pct,
                         etc_data=etc_data,
                         disposition_breakdown=disposition_breakdown,
                         category_breakdown=category_breakdown,
                         type_breakdown=type_breakdown,
                         labor_utilization=labor_utilization,
                         weekly_completion=weekly_completion,
                         monthly_wo_trend=monthly_wo_trend,
                         on_time_delivery=on_time_delivery,
                         otd_rate=otd_rate,
                         material_shortage=material_shortage,
                         schedule_data=schedule_data,
                         schedule_stages=schedule_stages_list,
                         schedule_statuses=schedule_statuses,
                         schedule_priorities=schedule_priorities,
                         sched_status=sched_status,
                         sched_stage=sched_stage,
                         sched_priority=sched_priority,
                         sched_date_from=sched_date_from,
                         sched_date_to=sched_date_to,
                         sched_sort=sched_sort,
                         sched_order=sched_order,
                         today=today_str)


@operations_bp.route('/work-order-quotes-dashboard')
@login_required
@role_required('Admin', 'Planner', 'Production Staff', 'Accountant', 'Sales')
def work_order_quotes_dashboard():
    db = Database()
    conn = db.get_connection()
    
    quoting_stage = conn.execute(
        "SELECT id FROM work_order_stages WHERE name = 'Quoting' LIMIT 1"
    ).fetchone()
    quoting_stage_id = quoting_stage['id'] if quoting_stage else None
    
    wo_in_quoting = []
    if quoting_stage_id:
        wo_in_quoting = conn.execute('''
            SELECT 
                wo.id,
                wo.wo_number,
                wo.status,
                wo.priority,
                wo.is_aog,
                wo.created_at,
                wo.planned_start_date,
                p.code as product_code,
                p.name as product_name,
                COALESCE(wo.customer_name, cust.name) as customer_name,
                woq.id as quote_id,
                woq.quote_number,
                woq.total_amount as quote_amount,
                woq.status as quote_status,
                CAST(julianday('now') - julianday(wo.created_at) AS INTEGER) as tat_days
            FROM work_orders wo
            LEFT JOIN products p ON wo.product_id = p.id
            LEFT JOIN customers cust ON wo.customer_id = cust.id
            LEFT JOIN work_order_quotes woq ON wo.id = woq.work_order_id
            WHERE wo.stage_id = ?
            AND wo.status NOT IN ('Completed', 'Cancelled')
            ORDER BY wo.is_aog DESC, wo.priority DESC, wo.created_at DESC
        ''', (quoting_stage_id,)).fetchall()
    
    quotes_awaiting_approval = conn.execute('''
        SELECT 
            woq.id,
            woq.quote_number,
            woq.status,
            woq.total_amount,
            woq.created_at,
            woq.updated_at,
            woq.customer_name as quote_customer_name,
            woq.estimated_turnaround_days,
            wo.id as work_order_id,
            wo.wo_number,
            wo.is_aog,
            wo.priority,
            p.code as product_code,
            p.name as product_name,
            COALESCE(wo.customer_name, cust.name) as customer_name,
            u.username as prepared_by_name,
            CAST(julianday('now') - julianday(COALESCE(woq.updated_at, woq.created_at)) AS INTEGER) as tat_days
        FROM work_order_quotes woq
        JOIN work_orders wo ON woq.work_order_id = wo.id
        LEFT JOIN products p ON wo.product_id = p.id
        LEFT JOIN customers cust ON wo.customer_id = cust.id
        LEFT JOIN users u ON woq.prepared_by = u.id
        WHERE woq.status IN ('Pending Approval', 'Sent', 'Quoted', 'Submitted')
        ORDER BY wo.is_aog DESC, wo.priority DESC, woq.created_at DESC
    ''').fetchall()
    
    recently_approved = conn.execute('''
        SELECT 
            woq.id,
            woq.quote_number,
            woq.status,
            woq.total_amount,
            woq.customer_approved_at,
            woq.customer_approved_by,
            woq.acknowledged,
            woq.acknowledged_at,
            wo.id as work_order_id,
            wo.wo_number,
            wo.is_aog,
            wo.priority,
            p.code as product_code,
            p.name as product_name,
            COALESCE(wo.customer_name, cust.name) as customer_name,
            u.username as acknowledged_by_name
        FROM work_order_quotes woq
        JOIN work_orders wo ON woq.work_order_id = wo.id
        LEFT JOIN products p ON wo.product_id = p.id
        LEFT JOIN customers cust ON wo.customer_id = cust.id
        LEFT JOIN users u ON woq.acknowledged_by = u.id
        WHERE woq.status = 'Approved'
        ORDER BY woq.customer_approved_at DESC
        LIMIT 50
    ''').fetchall()
    
    summary_stats = {
        'wo_in_quoting': len(wo_in_quoting),
        'awaiting_approval': len(quotes_awaiting_approval),
        'recently_approved': len(recently_approved),
        'total_quoting_value': sum(row['quote_amount'] or 0 for row in wo_in_quoting if row['quote_amount']),
        'total_pending_value': sum(row['total_amount'] or 0 for row in quotes_awaiting_approval),
        'total_approved_value': sum(row['total_amount'] or 0 for row in recently_approved),
        'unacknowledged': sum(1 for row in recently_approved if not row['acknowledged'])
    }
    
    conn.close()
    
    return render_template('operations/wo_quotes_dashboard.html',
                         wo_in_quoting=wo_in_quoting,
                         quotes_awaiting_approval=quotes_awaiting_approval,
                         recently_approved=recently_approved,
                         summary_stats=summary_stats)
