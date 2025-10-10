from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database
from datetime import datetime, timedelta
from functools import wraps

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
    
    db = Database()
    conn = db.get_connection()
    
    # Find employee with matching code and PIN
    employee = conn.execute('''
        SELECT id, employee_code, first_name, last_name, hourly_rate, status, clock_pin
        FROM labor_resources
        WHERE UPPER(employee_code) = ? AND status = 'Active'
    ''', (employee_code,)).fetchone()
    
    conn.close()
    
    if not employee:
        flash('Invalid employee code or account is inactive.', 'danger')
        return redirect(url_for('clock_station_routes.clock_login'))
    
    # Check PIN (if set, otherwise allow access)
    if employee['clock_pin'] and employee['clock_pin'] != pin:
        flash('Invalid PIN. Please try again.', 'danger')
        return redirect(url_for('clock_station_routes.clock_login'))
    
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
