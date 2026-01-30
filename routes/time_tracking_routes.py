from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database, GLAutoPost
from auth import login_required, role_required
from datetime import datetime, date


def parse_datetime(value):
    """Parse datetime from either string (SQLite) or datetime object (PostgreSQL)"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
import math

time_tracking_bp = Blueprint('time_tracking_routes', __name__)

@time_tracking_bp.route('/time-tracking')
@login_required
def time_tracking_page():
    db = Database()
    conn = db.get_connection()
    
    user_id = session.get('user_id')
    
    # Get employee record for current user
    employee = conn.execute('''
        SELECT lr.*, (lr.first_name || ' ' || lr.last_name) as name
        FROM labor_resources lr
        WHERE lr.user_id = ?
    ''', (user_id,)).fetchone()
    
    # Show alert if no employee record found
    if not employee:
        flash('You do not have an employee record linked to your account. Please contact your administrator to set up time tracking access.', 'warning')
        conn.close()
        return render_template('time_tracking/clock_in_out.html',
                             employee=None,
                             active_entry=None,
                             work_orders=[],
                             todays_entries=[])
    
    # Get active work orders
    work_orders = conn.execute('''
        SELECT wo.*, p.name as product_name, p.code as product_code
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.status IN ('Released', 'In Progress')
        ORDER BY wo.wo_number DESC
    ''').fetchall()
    
    # Get current active clock-in for this employee
    active_entry = None
    if employee:
        active_entry = conn.execute('''
            SELECT tt.*, wo.wo_number, p.name as product_name, 
                   wot.task_name, (lr.first_name || ' ' || lr.last_name) as employee_name
            FROM work_order_time_tracking tt
            JOIN work_orders wo ON tt.work_order_id = wo.id
            JOIN products p ON wo.product_id = p.id
            JOIN labor_resources lr ON tt.employee_id = lr.id
            LEFT JOIN work_order_tasks wot ON tt.task_id = wot.id
            WHERE tt.employee_id = ? AND tt.status = 'In Progress'
            ORDER BY tt.clock_in_time DESC
            LIMIT 1
        ''', (employee['id'],)).fetchone()
    
    # Get today's completed entries
    todays_entries = []
    if employee:
        todays_entries = conn.execute('''
            SELECT tt.*, wo.wo_number, p.name as product_name, 
                   wot.task_name, (lr.first_name || ' ' || lr.last_name) as employee_name
            FROM work_order_time_tracking tt
            JOIN work_orders wo ON tt.work_order_id = wo.id
            JOIN products p ON wo.product_id = p.id
            JOIN labor_resources lr ON tt.employee_id = lr.id
            LEFT JOIN work_order_tasks wot ON tt.task_id = wot.id
            WHERE tt.employee_id = ? 
            AND DATE(tt.clock_in_time) = DATE('now')
            AND tt.status = 'Completed'
            ORDER BY tt.clock_in_time DESC
        ''', (employee['id'],)).fetchall()
    
    conn.close()
    
    return render_template('time_tracking/clock_in_out.html',
                         employee=employee,
                         work_orders=work_orders,
                         active_entry=active_entry,
                         todays_entries=todays_entries)

@time_tracking_bp.route('/time-tracking/tasks/<int:wo_id>')
@login_required
def get_tasks(wo_id):
    db = Database()
    conn = db.get_connection()
    
    tasks = conn.execute('''
        SELECT * FROM work_order_tasks
        WHERE work_order_id = ?
        ORDER BY task_name
    ''', (wo_id,)).fetchall()
    
    conn.close()
    
    return jsonify([{
        'id': task['id'],
        'task_name': task['task_name'],
        'description': task['description']
    } for task in tasks])

@time_tracking_bp.route('/time-tracking/clock-in', methods=['POST'])
@login_required
def clock_in():
    db = Database()
    conn = db.get_connection()
    
    try:
        work_order_id = int(request.form['work_order_id'])
        task_id = request.form.get('task_id', '')
        task_id = int(task_id) if task_id else None
        notes = request.form.get('notes', '')
        user_id = session.get('user_id')
        
        # Get employee record for logged-in user
        employee = conn.execute('''
            SELECT lr.*, (lr.first_name || ' ' || lr.last_name) as name
            FROM labor_resources lr
            WHERE lr.user_id = ?
        ''', (user_id,)).fetchone()
        
        if not employee:
            flash('No employee record found. Please contact administrator.', 'danger')
            return redirect(url_for('time_tracking_routes.time_tracking_page'))
        
        # Check if already clocked in
        existing = conn.execute('''
            SELECT * FROM work_order_time_tracking
            WHERE employee_id = ? AND status = 'In Progress'
        ''', (employee['id'],)).fetchone()
        
        if existing:
            flash('You are already clocked in! Please clock out first.', 'warning')
            return redirect(url_for('time_tracking_routes.time_tracking_page'))
        
        # Generate entry number
        last_entry = conn.execute('''
            SELECT entry_number FROM work_order_time_tracking 
            WHERE entry_number LIKE 'CLK-%'
            ORDER BY CAST(SUBSTR(entry_number, 5) AS INTEGER) DESC 
            LIMIT 1
        ''').fetchone()
        
        if last_entry:
            try:
                last_number = int(last_entry['entry_number'].split('-')[1])
                next_number = last_number + 1
            except (ValueError, IndexError):
                next_number = 1
        else:
            next_number = 1
        
        entry_number = f'CLK-{next_number:06d}'
        
        clock_in_time = datetime.now().isoformat()
        
        conn.execute('''
            INSERT INTO work_order_time_tracking 
            (entry_number, employee_id, work_order_id, task_id, clock_in_time, 
             hourly_rate, status, notes, created_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?, 'In Progress', ?, ?, ?)
        ''', (entry_number, employee['id'], work_order_id, task_id, clock_in_time,
              employee['hourly_rate'], notes, session.get('user_id'), clock_in_time))
        
        # Update work order status to In Progress if it's Released
        conn.execute('''
            UPDATE work_orders 
            SET status = 'In Progress', actual_start_date = CURRENT_DATE
            WHERE id = ? AND status = 'Released'
        ''', (work_order_id,))
        
        # Update task status to In Progress if task is selected
        if task_id:
            conn.execute('''
                UPDATE work_order_tasks 
                SET status = 'In Progress'
                WHERE id = ? AND status = 'Not Started'
            ''', (task_id,))
        
        conn.commit()
        flash(f'Clocked in successfully! Entry: {entry_number}', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error clocking in: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('time_tracking_routes.time_tracking_page'))

@time_tracking_bp.route('/time-tracking/clock-out/<int:entry_id>', methods=['POST'])
@login_required
def clock_out(entry_id):
    db = Database()
    conn = db.get_connection()
    
    try:
        notes = request.form.get('notes', '')
        user_id = session.get('user_id')
        
        # Get employee record for logged-in user
        employee = conn.execute('''
            SELECT lr.*, (lr.first_name || ' ' || lr.last_name) as name
            FROM labor_resources lr
            WHERE lr.user_id = ?
        ''', (user_id,)).fetchone()
        
        if not employee:
            flash('You do not have an employee record linked to your account.', 'danger')
            return redirect(url_for('time_tracking_routes.time_tracking_page'))
        
        # Get the time tracking entry and verify ownership
        entry = conn.execute('''
            SELECT tt.*, lr.hourly_rate
            FROM work_order_time_tracking tt
            JOIN labor_resources lr ON tt.employee_id = lr.id
            WHERE tt.id = ? AND tt.employee_id = ?
        ''', (entry_id, employee['id'])).fetchone()
        
        if not entry:
            flash('Time tracking entry not found or you do not have permission to clock out this entry.', 'danger')
            return redirect(url_for('time_tracking_routes.time_tracking_page'))
        
        if entry['status'] != 'In Progress':
            flash('This entry is already clocked out.', 'warning')
            return redirect(url_for('time_tracking_routes.time_tracking_page'))
        
        clock_out_time = datetime.now()
        clock_in_time = parse_datetime(entry['clock_in_time'])
        
        # Calculate hours worked
        time_diff = clock_out_time - clock_in_time
        hours_worked = time_diff.total_seconds() / 3600
        
        if not math.isfinite(hours_worked) or hours_worked < 0:
            hours_worked = 0
        
        # Calculate labor cost
        labor_cost = hours_worked * entry['hourly_rate']
        
        # Update time tracking entry
        conn.execute('''
            UPDATE work_order_time_tracking
            SET clock_out_time = ?, hours_worked = ?, labor_cost = ?, 
                status = 'Completed', notes = ?, 
                modified_by = ?, modified_at = ?
            WHERE id = ?
        ''', (clock_out_time.isoformat(), hours_worked, labor_cost, notes,
              session.get('user_id'), clock_out_time.isoformat(), entry_id))
        
        # Update work order labor cost
        conn.execute('''
            UPDATE work_orders
            SET labor_cost = labor_cost + ?
            WHERE id = ?
        ''', (labor_cost, entry['work_order_id']))
        
        # Update task actual hours and cost if task is assigned
        if entry['task_id']:
            conn.execute('''
                UPDATE work_order_tasks
                SET actual_hours = actual_hours + ?,
                    actual_labor_cost = actual_labor_cost + ?
                WHERE id = ?
            ''', (hours_worked, labor_cost, entry['task_id']))
        
        # Create GL journal entry for labor cost
        if labor_cost > 0:
            wo = conn.execute('SELECT wo_number FROM work_orders WHERE id = ?', (entry['work_order_id'],)).fetchone()
            wo_number = wo['wo_number'] if wo else f'WO-{entry["work_order_id"]}'
            
            gl_lines = [
                {
                    'account_code': '5100',
                    'debit': round(labor_cost, 2),
                    'credit': 0,
                    'description': f'WO Labor - {employee["name"]} ({wo_number})'
                },
                {
                    'account_code': '2100',
                    'debit': 0,
                    'credit': round(labor_cost, 2),
                    'description': f'Wages Payable - WO Labor ({wo_number})'
                }
            ]
            
            GLAutoPost.create_auto_journal_entry(
                conn=conn,
                entry_date=date.today().isoformat(),
                description=f'Work Order Labor - {wo_number}',
                transaction_source='WO Labor',
                reference_type='work_order_time_tracking',
                reference_id=entry_id,
                lines=gl_lines,
                created_by=session.get('user_id')
            )
        
        conn.commit()
        flash(f'Clocked out successfully! Hours worked: {hours_worked:.2f}, Labor cost: ${labor_cost:.2f}', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error clocking out: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('time_tracking_routes.time_tracking_page'))

@time_tracking_bp.route('/time-tracking/history')
@login_required
def time_tracking_history():
    db = Database()
    conn = db.get_connection()
    
    user_id = session.get('user_id')
    
    # Get employee record for logged-in user
    employee = conn.execute('''
        SELECT lr.*, (lr.first_name || ' ' || lr.last_name) as name
        FROM labor_resources lr
        WHERE lr.user_id = ?
    ''', (user_id,)).fetchone()
    
    # Get all time tracking entries for this employee
    entries = []
    if employee:
        entries = conn.execute('''
            SELECT tt.*, wo.wo_number, p.name as product_name, 
                   wot.task_name, (lr.first_name || ' ' || lr.last_name) as employee_name
            FROM work_order_time_tracking tt
            JOIN work_orders wo ON tt.work_order_id = wo.id
            JOIN products p ON wo.product_id = p.id
            JOIN labor_resources lr ON tt.employee_id = lr.id
            LEFT JOIN work_order_tasks wot ON tt.task_id = wot.id
            WHERE tt.employee_id = ?
            ORDER BY tt.clock_in_time DESC
        ''', (employee['id'],)).fetchall()
    
    conn.close()
    
    return render_template('time_tracking/history.html',
                         employee=employee,
                         entries=entries)

@time_tracking_bp.route('/time-tracking/active-labor-report')
@login_required
@role_required('Admin', 'Planner')
def active_labor_report():
    db = Database()
    conn = db.get_connection()
    
    # Get all currently clocked in employees
    active_entries = conn.execute('''
        SELECT tt.*, wo.wo_number, p.name as product_name, p.code as product_code,
               wot.task_name, (lr.first_name || ' ' || lr.last_name) as employee_name, 
               lr.employee_code, lr.hourly_rate
        FROM work_order_time_tracking tt
        JOIN work_orders wo ON tt.work_order_id = wo.id
        JOIN products p ON wo.product_id = p.id
        JOIN labor_resources lr ON tt.employee_id = lr.id
        LEFT JOIN work_order_tasks wot ON tt.task_id = wot.id
        WHERE tt.status = 'In Progress'
        ORDER BY tt.clock_in_time ASC
    ''').fetchall()
    
    # Calculate summary statistics
    total_employees = len(active_entries)
    total_hourly_cost = sum(entry['hourly_rate'] for entry in active_entries)
    
    # Calculate estimated current labor cost based on elapsed time
    current_labor_cost = 0
    for entry in active_entries:
        clock_in = parse_datetime(entry['clock_in_time'])
        elapsed_hours = (datetime.now() - clock_in).total_seconds() / 3600
        current_labor_cost += elapsed_hours * entry['hourly_rate']
    
    conn.close()
    
    return render_template('time_tracking/active_labor_report.html',
                         active_entries=active_entries,
                         total_employees=total_employees,
                         total_hourly_cost=total_hourly_cost,
                         current_labor_cost=current_labor_cost)

@time_tracking_bp.route('/time-tracking/sync-to-accounting', methods=['POST'])
@login_required
@role_required('Admin', 'Accountant')
def sync_labor_to_accounting():
    """Sync all existing work order labor entries to accounting (GL entries)"""
    db = Database()
    conn = db.get_connection()
    
    # Get existing GL entries for work order time tracking
    existing_gl_refs = conn.execute('''
        SELECT reference_id FROM gl_entries 
        WHERE reference_type = 'work_order_time_tracking' AND reference_id IS NOT NULL
    ''').fetchall()
    existing_gl_ids = set(str(r['reference_id']) for r in existing_gl_refs)
    
    # Get all completed time tracking entries with labor cost
    entries = conn.execute('''
        SELECT tt.id, tt.labor_cost, tt.work_order_id, tt.employee_id, tt.clock_in_time,
               wo.wo_number, (lr.first_name || ' ' || lr.last_name) as employee_name
        FROM work_order_time_tracking tt
        JOIN work_orders wo ON tt.work_order_id = wo.id
        JOIN labor_resources lr ON tt.employee_id = lr.id
        WHERE tt.status = 'Completed' AND tt.labor_cost > 0
    ''').fetchall()
    
    synced_count = 0
    for entry in entries:
        if str(entry['id']) in existing_gl_ids:
            continue
        
        labor_cost = float(entry['labor_cost'])
        wo_number = entry['wo_number']
        emp_name = entry['employee_name']
        
        gl_lines = [
            {
                'account_code': '5100',
                'debit': round(labor_cost, 2),
                'credit': 0,
                'description': f'WO Labor - {emp_name} ({wo_number})'
            },
            {
                'account_code': '2100',
                'debit': 0,
                'credit': round(labor_cost, 2),
                'description': f'Wages Payable - WO Labor ({wo_number})'
            }
        ]
        
        entry_date = entry['clock_in_time'][:10] if entry['clock_in_time'] else date.today().isoformat()
        
        GLAutoPost.create_auto_journal_entry(
            conn=conn,
            entry_date=entry_date,
            description=f'Work Order Labor - {wo_number} (Backfill)',
            transaction_source='WO Labor',
            reference_type='work_order_time_tracking',
            reference_id=entry['id'],
            lines=gl_lines,
            created_by=session.get('user_id')
        )
        synced_count += 1
    
    conn.commit()
    conn.close()
    
    if synced_count > 0:
        flash(f'Successfully synced {synced_count} work order labor entries to accounting', 'success')
    else:
        flash('All work order labor entries are already synced to accounting', 'info')
    
    return redirect(url_for('time_tracking_routes.supervisor_view'))


@time_tracking_bp.route('/time-tracking/supervisor')
@login_required
@role_required('Admin', 'Planner')
def supervisor_view():
    db = Database()
    conn = db.get_connection()
    
    # Get all time tracking entries
    entries = conn.execute('''
        SELECT tt.*, wo.wo_number, p.name as product_name, 
               wot.task_name, (lr.first_name || ' ' || lr.last_name) as employee_name, lr.employee_code
        FROM work_order_time_tracking tt
        JOIN work_orders wo ON tt.work_order_id = wo.id
        JOIN products p ON wo.product_id = p.id
        JOIN labor_resources lr ON tt.employee_id = lr.id
        LEFT JOIN work_order_tasks wot ON tt.task_id = wot.id
        ORDER BY tt.clock_in_time DESC
        LIMIT 100
    ''').fetchall()
    
    # Get currently clocked in employees
    active_entries = conn.execute('''
        SELECT tt.*, wo.wo_number, p.name as product_name, 
               wot.task_name, (lr.first_name || ' ' || lr.last_name) as employee_name, lr.employee_code
        FROM work_order_time_tracking tt
        JOIN work_orders wo ON tt.work_order_id = wo.id
        JOIN products p ON wo.product_id = p.id
        JOIN labor_resources lr ON tt.employee_id = lr.id
        LEFT JOIN work_order_tasks wot ON tt.task_id = wot.id
        WHERE tt.status = 'In Progress'
        ORDER BY tt.clock_in_time DESC
    ''').fetchall()
    
    conn.close()
    
    return render_template('time_tracking/supervisor.html',
                         entries=entries,
                         active_entries=active_entries)
