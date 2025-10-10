from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database
from datetime import datetime, timedelta
from functools import wraps
from werkzeug.security import check_password_hash, generate_password_hash

clock_station_bp = Blueprint('clock_station_routes', __name__)

def clock_auth_required(f):
    """Decorator for clock station authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'clock_employee_id' not in session:
            return redirect(url_for('clock_station_routes.clock_login'))
        return f(*args, **kwargs)
    return decorated_function

@clock_station_bp.route('/clock')
def clock_login():
    """Clock station login page - PIN authentication"""
    return render_template('clock_station/login.html')

@clock_station_bp.route('/clock/auth', methods=['POST'])
def clock_auth():
    """Authenticate employee with PIN"""
    employee_code = request.form.get('employee_code', '').strip().upper()
    pin = request.form.get('pin', '').strip()
    
    if not employee_code or not pin:
        flash('Please enter both Employee Code and PIN.', 'danger')
        return redirect(url_for('clock_station_routes.clock_login'))
    
    # Get IP address for tracking
    ip_address = request.remote_addr or 'unknown'
    
    db = Database()
    conn = db.get_connection()
    
    # SERVER-SIDE BRUTE-FORCE PROTECTION: Check failed attempts in last 15 minutes
    cutoff_time = (datetime.now() - timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')
    failed_count = conn.execute('''
        SELECT COUNT(*) as count FROM clock_login_attempts
        WHERE employee_code = ? AND ip_address = ? AND attempt_time >= ? AND success = 0
    ''', (employee_code, ip_address, cutoff_time)).fetchone()['count']
    
    if failed_count >= 5:
        flash('Too many failed attempts. Please try again in 15 minutes.', 'danger')
        conn.close()
        return redirect(url_for('clock_station_routes.clock_login'))
    
    # Find employee with matching code
    employee = conn.execute('''
        SELECT id, employee_code, first_name, last_name, hourly_rate, status, clock_pin
        FROM labor_resources
        WHERE UPPER(employee_code) = ? AND status = 'Active'
    ''', (employee_code,)).fetchone()
    
    if not employee:
        # Log failed attempt
        conn.execute('''
            INSERT INTO clock_login_attempts (employee_code, ip_address, attempt_time, success)
            VALUES (?, ?, ?, 0)
        ''', (employee_code, ip_address, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
        conn.close()
        flash('Invalid employee code or account is inactive.', 'danger')
        return redirect(url_for('clock_station_routes.clock_login'))
    
    # SECURITY: Require PIN to be set
    if not employee['clock_pin']:
        conn.close()
        flash('Your PIN has not been set. Please contact your manager to set up your clock station PIN.', 'warning')
        return redirect(url_for('clock_station_routes.clock_login'))
    
    # SECURITY: Verify hashed PIN (with safe fallback for legacy plaintext PINs)
    try:
        # Try to verify as hashed PIN
        pin_valid = check_password_hash(employee['clock_pin'], pin)
    except (ValueError, TypeError):
        # Legacy plaintext PIN detected - reject and prompt for reset
        conn.close()
        flash('Your PIN needs to be reset for security. Please contact your manager to reset your clock station PIN.', 'warning')
        return redirect(url_for('clock_station_routes.clock_login'))
    
    if not pin_valid:
        # Log failed attempt
        conn.execute('''
            INSERT INTO clock_login_attempts (employee_code, ip_address, attempt_time, success)
            VALUES (?, ?, ?, 0)
        ''', (employee_code, ip_address, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
        conn.close()
        flash('Invalid PIN. Please try again.', 'danger')
        return redirect(url_for('clock_station_routes.clock_login'))
    
    # Log successful login
    conn.execute('''
        INSERT INTO clock_login_attempts (employee_code, ip_address, attempt_time, success)
        VALUES (?, ?, ?, 1)
    ''', (employee_code, ip_address, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    conn.commit()
    conn.close()
    
    # Store employee in session
    session['clock_employee_id'] = employee['id']
    session['clock_employee_name'] = f"{employee['first_name']} {employee['last_name']}"
    session['clock_employee_code'] = employee['employee_code']
    
    return redirect(url_for('clock_station_routes.clock_dashboard'))

@clock_station_bp.route('/clock/dashboard')
@clock_auth_required
def clock_dashboard():
    """Main clock station dashboard"""
    db = Database()
    conn = db.get_connection()
    
    employee_id = session['clock_employee_id']
    
    # Get employee details
    employee = conn.execute('''
        SELECT id, employee_code, first_name, last_name, role, hourly_rate
        FROM labor_resources
        WHERE id = ?
    ''', (employee_id,)).fetchone()
    
    # Get current status (last punch)
    last_punch = conn.execute('''
        SELECT punch_type, punch_time, location, notes, project_name
        FROM time_clock_punches
        WHERE employee_id = ?
        ORDER BY punch_time DESC
        LIMIT 1
    ''', (employee_id,)).fetchone()
    
    # Calculate hours worked today
    today = datetime.now().strftime('%Y-%m-%d')
    todays_punches = conn.execute('''
        SELECT punch_type, punch_time
        FROM time_clock_punches
        WHERE employee_id = ? AND DATE(punch_time) = ?
        ORDER BY punch_time ASC
    ''', (employee_id, today)).fetchall()
    
    hours_today = calculate_hours_from_punches(todays_punches)
    
    # Get recent punches (last 7 days)
    week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    recent_punches = conn.execute('''
        SELECT punch_number, punch_type, punch_time, location, project_name, notes
        FROM time_clock_punches
        WHERE employee_id = ? AND DATE(punch_time) >= ?
        ORDER BY punch_time DESC
        LIMIT 20
    ''', (employee_id, week_ago)).fetchall()
    
    # Determine current status
    is_clocked_in = last_punch and last_punch['punch_type'] == 'Clock In'
    
    conn.close()
    
    return render_template('clock_station/dashboard.html',
                         employee=employee,
                         last_punch=last_punch,
                         is_clocked_in=is_clocked_in,
                         hours_today=hours_today,
                         recent_punches=recent_punches)

@clock_station_bp.route('/clock/punch', methods=['POST'])
@clock_auth_required
def clock_punch():
    """Record a clock punch (in or out)"""
    db = Database()
    conn = db.get_connection()
    
    employee_id = session['clock_employee_id']
    punch_type = request.form.get('punch_type')  # 'Clock In' or 'Clock Out'
    location = request.form.get('location', '')
    project_name = request.form.get('project_name', '')
    notes = request.form.get('notes', '')
    
    # Get client info
    ip_address = request.remote_addr
    device_info = request.user_agent.string[:200] if request.user_agent else ''
    
    # Generate punch number
    count = conn.execute('SELECT COUNT(*) as count FROM time_clock_punches').fetchone()['count']
    punch_number = f"PUNCH-{count + 1:07d}"
    
    # Insert punch record
    conn.execute('''
        INSERT INTO time_clock_punches (
            punch_number, employee_id, punch_type, punch_time,
            location, ip_address, device_info, project_name, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        punch_number, employee_id, punch_type, datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        location, ip_address, device_info, project_name, notes
    ))
    
    conn.commit()
    conn.close()
    
    if punch_type == 'Clock In':
        flash('Successfully clocked in! Have a productive shift.', 'success')
    else:
        flash('Successfully clocked out! Great work today.', 'success')
    
    return redirect(url_for('clock_station_routes.clock_dashboard'))

@clock_station_bp.route('/clock/logout')
def clock_logout():
    """Logout from clock station"""
    session.pop('clock_employee_id', None)
    session.pop('clock_employee_name', None)
    session.pop('clock_employee_code', None)
    flash('Logged out successfully.', 'info')
    return redirect(url_for('clock_station_routes.clock_login'))

@clock_station_bp.route('/clock/reports')
@clock_auth_required
def clock_reports():
    """Employee time reports"""
    db = Database()
    conn = db.get_connection()
    
    employee_id = session['clock_employee_id']
    period = request.args.get('period', 'week')  # week, month, custom
    
    # Calculate date range
    end_date = datetime.now()
    if period == 'week':
        start_date = end_date - timedelta(days=7)
    elif period == 'month':
        start_date = end_date - timedelta(days=30)
    else:
        start_date = datetime.strptime(request.args.get('start_date', ''), '%Y-%m-%d') if request.args.get('start_date') else end_date - timedelta(days=7)
        end_date = datetime.strptime(request.args.get('end_date', ''), '%Y-%m-%d') if request.args.get('end_date') else datetime.now()
    
    # Get punches for period
    punches = conn.execute('''
        SELECT punch_number, punch_type, punch_time, location, project_name, notes
        FROM time_clock_punches
        WHERE employee_id = ? AND punch_time BETWEEN ? AND ?
        ORDER BY punch_time ASC
    ''', (employee_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d %H:%M:%S'))).fetchall()
    
    # Group by day and calculate hours
    daily_summary = calculate_daily_hours(punches)
    total_hours = sum(day['hours'] for day in daily_summary)
    
    employee = conn.execute('''
        SELECT first_name, last_name, employee_code, hourly_rate
        FROM labor_resources WHERE id = ?
    ''', (employee_id,)).fetchone()
    
    conn.close()
    
    return render_template('clock_station/reports.html',
                         employee=employee,
                         period=period,
                         start_date=start_date.strftime('%Y-%m-%d'),
                         end_date=end_date.strftime('%Y-%m-%d'),
                         daily_summary=daily_summary,
                         total_hours=total_hours,
                         punches=punches)

def calculate_hours_from_punches(punches):
    """Calculate total hours from punch list"""
    total_hours = 0.0
    clock_in_time = None
    
    for punch in punches:
        if punch['punch_type'] == 'Clock In':
            clock_in_time = datetime.strptime(punch['punch_time'], '%Y-%m-%d %H:%M:%S')
        elif punch['punch_type'] == 'Clock Out' and clock_in_time:
            clock_out_time = datetime.strptime(punch['punch_time'], '%Y-%m-%d %H:%M:%S')
            hours = (clock_out_time - clock_in_time).total_seconds() / 3600
            total_hours += hours
            clock_in_time = None
    
    # If still clocked in, calculate up to now
    if clock_in_time:
        hours = (datetime.now() - clock_in_time).total_seconds() / 3600
        total_hours += hours
    
    return round(total_hours, 2)

def calculate_daily_hours(punches):
    """Calculate hours worked per day from punches"""
    daily_data = {}
    
    for punch in punches:
        punch_date = punch['punch_time'][:10]
        if punch_date not in daily_data:
            daily_data[punch_date] = {'date': punch_date, 'punches': [], 'hours': 0.0}
        daily_data[punch_date]['punches'].append(punch)
    
    # Calculate hours for each day
    summary = []
    for date, data in sorted(daily_data.items()):
        hours = calculate_hours_from_punches(data['punches'])
        summary.append({
            'date': date,
            'hours': hours,
            'punch_count': len(data['punches'])
        })
    
    return summary

# Manager Routes
@clock_station_bp.route('/clock/manager')
def manager_dashboard():
    """Manager dashboard for viewing all employee time"""
    from auth import login_required, role_required
    from functools import wraps
    
    # Check if user is logged in and has manager role
    if 'user_id' not in session:
        flash('Please login to access manager dashboard.', 'warning')
        return redirect(url_for('auth_routes.login'))
    
    if session.get('role') not in ['Admin', 'Planner']:
        flash('Access denied. Manager privileges required.', 'danger')
        return redirect(url_for('main_routes.index'))
    
    db = Database()
    conn = db.get_connection()
    
    # Get date range
    period = request.args.get('period', 'today')
    today = datetime.now().strftime('%Y-%m-%d')
    
    if period == 'today':
        start_date = today
        end_date = today
    elif period == 'week':
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        end_date = today
    else:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        end_date = today
    
    # Get all employees with their time stats
    employees = conn.execute('''
        SELECT 
            lr.id,
            lr.employee_code,
            lr.first_name,
            lr.last_name,
            lr.role,
            lr.hourly_rate,
            (SELECT COUNT(*) FROM time_clock_punches 
             WHERE employee_id = lr.id 
             AND DATE(punch_time) >= ? 
             AND DATE(punch_time) <= ?) as punch_count
        FROM labor_resources lr
        WHERE lr.status = 'Active'
        ORDER BY lr.employee_code
    ''', (start_date, end_date)).fetchall()
    
    # Calculate hours for each employee
    employee_stats = []
    for emp in employees:
        punches = conn.execute('''
            SELECT punch_type, punch_time
            FROM time_clock_punches
            WHERE employee_id = ? AND DATE(punch_time) >= ? AND DATE(punch_time) <= ?
            ORDER BY punch_time ASC
        ''', (emp['id'], start_date, end_date)).fetchall()
        
        hours = calculate_hours_from_punches(punches)
        
        # Check if currently clocked in
        last_punch = conn.execute('''
            SELECT punch_type FROM time_clock_punches
            WHERE employee_id = ?
            ORDER BY punch_time DESC LIMIT 1
        ''', (emp['id'],)).fetchone()
        
        is_clocked_in = last_punch and last_punch['punch_type'] == 'Clock In'
        
        employee_stats.append({
            'id': emp['id'],
            'code': emp['employee_code'],
            'name': f"{emp['first_name']} {emp['last_name']}",
            'role': emp['role'],
            'hours': hours,
            'labor_cost': hours * emp['hourly_rate'],
            'punch_count': emp['punch_count'],
            'is_clocked_in': is_clocked_in
        })
    
    # Get currently clocked in employees
    clocked_in = conn.execute('''
        SELECT 
            lr.employee_code,
            lr.first_name || ' ' || lr.last_name as name,
            tcp.punch_time,
            tcp.location,
            tcp.project_name
        FROM time_clock_punches tcp
        JOIN labor_resources lr ON tcp.employee_id = lr.id
        WHERE tcp.id IN (
            SELECT MAX(id) FROM time_clock_punches GROUP BY employee_id
        ) AND tcp.punch_type = 'Clock In'
        ORDER BY tcp.punch_time DESC
    ''').fetchall()
    
    total_hours = sum(emp['hours'] for emp in employee_stats)
    total_labor_cost = sum(emp['labor_cost'] for emp in employee_stats)
    
    conn.close()
    
    return render_template('clock_station/manager.html',
                         employees=employee_stats,
                         clocked_in=clocked_in,
                         total_hours=total_hours,
                         total_labor_cost=total_labor_cost,
                         period=period,
                         start_date=start_date,
                         end_date=end_date)

@clock_station_bp.route('/clock/manager/employee/<int:employee_id>')
def manager_employee_detail(employee_id):
    """View detailed time report for a specific employee"""
    from auth import login_required, role_required
    
    if 'user_id' not in session or session.get('role') not in ['Admin', 'Planner']:
        flash('Access denied.', 'danger')
        return redirect(url_for('main_routes.index'))
    
    db = Database()
    conn = db.get_connection()
    
    employee = conn.execute('''
        SELECT id, employee_code, first_name, last_name, role, hourly_rate
        FROM labor_resources WHERE id = ?
    ''', (employee_id,)).fetchone()
    
    if not employee:
        flash('Employee not found.', 'danger')
        conn.close()
        return redirect(url_for('clock_station_routes.manager_dashboard'))
    
    # Get date range
    period = request.args.get('period', 'week')
    end_date = datetime.now()
    
    if period == 'week':
        start_date = end_date - timedelta(days=7)
    else:
        start_date = end_date - timedelta(days=30)
    
    # Get all punches
    punches = conn.execute('''
        SELECT punch_number, punch_type, punch_time, location, project_name, notes, ip_address
        FROM time_clock_punches
        WHERE employee_id = ? AND punch_time BETWEEN ? AND ?
        ORDER BY punch_time DESC
    ''', (employee_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d %H:%M:%S'))).fetchall()
    
    daily_summary = calculate_daily_hours(punches)
    total_hours = sum(day['hours'] for day in daily_summary)
    
    conn.close()
    
    return render_template('clock_station/manager_employee.html',
                         employee=employee,
                         punches=punches,
                         daily_summary=daily_summary,
                         total_hours=total_hours,
                         period=period,
                         start_date=start_date.strftime('%Y-%m-%d'),
                         end_date=end_date.strftime('%Y-%m-%d'))

@clock_station_bp.route('/clock/manager/export-csv')
def export_timesheet_csv():
    """Export timesheet data as CSV"""
    from auth import login_required, role_required
    import csv
    from io import StringIO
    from flask import make_response
    
    if 'user_id' not in session or session.get('role') not in ['Admin', 'Planner']:
        flash('Access denied.', 'danger')
        return redirect(url_for('main_routes.index'))
    
    db = Database()
    conn = db.get_connection()
    
    period = request.args.get('period', 'week')
    today = datetime.now().strftime('%Y-%m-%d')
    
    if period == 'week':
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    else:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    # Get all punches for period
    punches = conn.execute('''
        SELECT 
            lr.employee_code,
            lr.first_name || ' ' || lr.last_name as name,
            tcp.punch_type,
            tcp.punch_time,
            tcp.location,
            tcp.project_name,
            tcp.notes
        FROM time_clock_punches tcp
        JOIN labor_resources lr ON tcp.employee_id = lr.id
        WHERE DATE(tcp.punch_time) >= ?
        ORDER BY tcp.punch_time DESC
    ''', (start_date,)).fetchall()
    
    conn.close()
    
    # Create CSV
    si = StringIO()
    writer = csv.writer(si)
    writer.writerow(['Employee Code', 'Name', 'Punch Type', 'Date', 'Time', 'Location', 'Project', 'Notes'])
    
    for punch in punches:
        writer.writerow([
            punch['employee_code'],
            punch['name'],
            punch['punch_type'],
            punch['punch_time'][:10],
            punch['punch_time'][11:19],
            punch['location'] or '',
            punch['project_name'] or '',
            punch['notes'] or ''
        ])
    
    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = f"attachment; filename=timesheet_{start_date}_to_{today}.csv"
    output.headers["Content-type"] = "text/csv"
    
    return output

# Admin/Manager PIN Management
@clock_station_bp.route('/clock/admin/set-pin/<int:employee_id>', methods=['GET', 'POST'])
def set_employee_pin(employee_id):
    """Admin/Manager route to set employee clock PIN"""
    if 'user_id' not in session or session.get('role') not in ['Admin', 'Planner']:
        flash('Access denied. Manager privileges required.', 'danger')
        return redirect(url_for('main_routes.index'))
    
    db = Database()
    conn = db.get_connection()
    
    employee = conn.execute('''
        SELECT id, employee_code, first_name, last_name
        FROM labor_resources WHERE id = ?
    ''', (employee_id,)).fetchone()
    
    if not employee:
        flash('Employee not found.', 'danger')
        conn.close()
        return redirect(url_for('labor_routes.list_labor_resources'))
    
    if request.method == 'POST':
        new_pin = request.form.get('pin', '').strip()
        confirm_pin = request.form.get('confirm_pin', '').strip()
        
        if not new_pin or len(new_pin) < 4:
            flash('PIN must be at least 4 digits.', 'danger')
        elif new_pin != confirm_pin:
            flash('PINs do not match.', 'danger')
        else:
            # Hash the PIN before storing
            hashed_pin = generate_password_hash(new_pin)
            
            conn.execute('''
                UPDATE labor_resources 
                SET clock_pin = ?
                WHERE id = ?
            ''', (hashed_pin, employee_id))
            
            conn.commit()
            flash(f'Clock PIN set successfully for {employee["first_name"]} {employee["last_name"]}.', 'success')
            conn.close()
            return redirect(url_for('labor_routes.list_labor_resources'))
    
    conn.close()
    
    return render_template('clock_station/set_pin.html', employee=employee)
