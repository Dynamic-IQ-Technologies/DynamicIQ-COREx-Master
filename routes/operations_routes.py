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
    
    stage_load = conn.execute('''
        SELECT 
            wos.id,
            wos.name as stage_name,
            wos.color,
            wos.sequence,
            COUNT(wo.id) as wo_count,
            SUM(CASE WHEN wo.status = 'In Progress' THEN 1 ELSE 0 END) as in_progress_count,
            COALESCE(SUM(wo.quantity), 0) as total_qty
        FROM work_order_stages wos
        LEFT JOIN work_orders wo ON wos.id = wo.stage_id AND wo.status NOT IN ('Completed', 'Cancelled')
        WHERE wos.is_active = 1
        GROUP BY wos.id, wos.name, wos.color, wos.sequence
        ORDER BY wos.sequence
    ''').fetchall()
    
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
    
    monthly_wo_trend = conn.execute('''
        SELECT 
            strftime('%Y-%m', created_at) as month,
            COUNT(*) as created,
            SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed
        FROM work_orders
        WHERE created_at >= date('now', '-6 months')
        GROUP BY strftime('%Y-%m', created_at)
        ORDER BY month
    ''').fetchall()
    
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
        WHERE mr.quantity_required > COALESCE(inv.available, 0)
        AND mr.work_order_id IN (SELECT id FROM work_orders WHERE status NOT IN ('Completed', 'Cancelled'))
    ''').fetchone()
    
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
                         today=today_str)
