from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import Database, AuditLogger
from auth import login_required, role_required
from datetime import datetime

tools_bp = Blueprint('tools_routes', __name__)

def generate_tool_number(conn):
    """Generate sequential tool number"""
    result = conn.execute('''
        SELECT tool_number FROM tools 
        WHERE tool_number LIKE 'TOOL-%' 
        ORDER BY id DESC LIMIT 1
    ''').fetchone()
    
    if result:
        try:
            last_num = int(result['tool_number'].split('-')[1])
            return f"TOOL-{last_num + 1:05d}"
        except:
            pass
    return "TOOL-00001"

@tools_bp.route('/tools')
@login_required
def list_tools():
    db = Database()
    conn = db.get_connection()
    
    tools = conn.execute('''
        SELECT t.*, 
               (lr.first_name || ' ' || lr.last_name) as assigned_to_name,
               (SELECT COUNT(*) FROM tool_checkouts tc WHERE tc.tool_id = t.id AND tc.return_date IS NULL) as is_checked_out
        FROM tools t
        LEFT JOIN labor_resources lr ON t.assigned_to = lr.id
        ORDER BY t.tool_number
    ''').fetchall()
    
    stats = {
        'total': len(tools),
        'available': sum(1 for t in tools if t['status'] == 'Available'),
        'in_use': sum(1 for t in tools if t['status'] == 'In Use'),
        'maintenance': sum(1 for t in tools if t['status'] == 'Maintenance'),
        'calibration_due': sum(1 for t in tools if t['next_calibration_date'] and t['next_calibration_date'] <= datetime.now().strftime('%Y-%m-%d'))
    }
    
    conn.close()
    return render_template('tools/list.html', tools=tools, stats=stats)

@tools_bp.route('/tools/create', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement')
def create_tool():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        tool_number = generate_tool_number(conn)
        
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO tools (tool_number, name, description, category, manufacturer, 
                             model_number, serial_number, location, status, condition,
                             purchase_date, purchase_cost, last_calibration_date, 
                             next_calibration_date, calibration_interval_days, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            tool_number,
            request.form['name'],
            request.form.get('description', ''),
            request.form.get('category', ''),
            request.form.get('manufacturer', ''),
            request.form.get('model_number', ''),
            request.form.get('serial_number', ''),
            request.form.get('location', ''),
            request.form.get('status', 'Available'),
            request.form.get('condition', 'Good'),
            request.form.get('purchase_date') or None,
            float(request.form.get('purchase_cost') or 0),
            request.form.get('last_calibration_date') or None,
            request.form.get('next_calibration_date') or None,
            int(request.form.get('calibration_interval_days') or 0) or None,
            request.form.get('notes', '')
        ))
        
        tool_id = cursor.lastrowid
        conn.commit()
        
        AuditLogger.log_change(conn, 'tools', tool_id, 'CREATE', session.get('user_id'),
                              {'tool_number': tool_number, 'name': request.form['name']})
        conn.commit()
        
        flash(f'Tool {tool_number} created successfully!', 'success')
        conn.close()
        return redirect(url_for('tools_routes.list_tools'))
    
    categories = ['Hand Tool', 'Power Tool', 'Measuring', 'Calibrated', 'Safety', 'Specialty', 'Other']
    conn.close()
    return render_template('tools/create.html', categories=categories)

@tools_bp.route('/tools/<int:tool_id>')
@login_required
def view_tool(tool_id):
    db = Database()
    conn = db.get_connection()
    
    tool = conn.execute('''
        SELECT t.*, (lr.first_name || ' ' || lr.last_name) as assigned_to_name
        FROM tools t
        LEFT JOIN labor_resources lr ON t.assigned_to = lr.id
        WHERE t.id = ?
    ''', (tool_id,)).fetchone()
    
    if not tool:
        flash('Tool not found', 'danger')
        conn.close()
        return redirect(url_for('tools_routes.list_tools'))
    
    checkouts = conn.execute('''
        SELECT tc.*, (lr.first_name || ' ' || lr.last_name) as employee_name, wo.wo_number
        FROM tool_checkouts tc
        JOIN labor_resources lr ON tc.checked_out_by = lr.id
        LEFT JOIN work_orders wo ON tc.work_order_id = wo.id
        WHERE tc.tool_id = ?
        ORDER BY tc.checkout_date DESC
        LIMIT 20
    ''', (tool_id,)).fetchall()
    
    conn.close()
    return render_template('tools/view.html', tool=tool, checkouts=checkouts)

@tools_bp.route('/tools/<int:tool_id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Procurement')
def edit_tool(tool_id):
    db = Database()
    conn = db.get_connection()
    
    tool = conn.execute('SELECT * FROM tools WHERE id = ?', (tool_id,)).fetchone()
    if not tool:
        flash('Tool not found', 'danger')
        conn.close()
        return redirect(url_for('tools_routes.list_tools'))
    
    if request.method == 'POST':
        old_values = dict(tool)
        
        conn.execute('''
            UPDATE tools SET name = ?, description = ?, category = ?, manufacturer = ?,
                           model_number = ?, serial_number = ?, location = ?, status = ?,
                           condition = ?, purchase_date = ?, purchase_cost = ?,
                           last_calibration_date = ?, next_calibration_date = ?,
                           calibration_interval_days = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (
            request.form['name'],
            request.form.get('description', ''),
            request.form.get('category', ''),
            request.form.get('manufacturer', ''),
            request.form.get('model_number', ''),
            request.form.get('serial_number', ''),
            request.form.get('location', ''),
            request.form.get('status', 'Available'),
            request.form.get('condition', 'Good'),
            request.form.get('purchase_date') or None,
            float(request.form.get('purchase_cost') or 0),
            request.form.get('last_calibration_date') or None,
            request.form.get('next_calibration_date') or None,
            int(request.form.get('calibration_interval_days') or 0) or None,
            request.form.get('notes', ''),
            tool_id
        ))
        
        AuditLogger.log_change(conn, 'tools', tool_id, 'UPDATE', session.get('user_id'),
                              {'name': request.form['name']})
        conn.commit()
        
        flash('Tool updated successfully!', 'success')
        conn.close()
        return redirect(url_for('tools_routes.view_tool', tool_id=tool_id))
    
    categories = ['Hand Tool', 'Power Tool', 'Measuring', 'Calibrated', 'Safety', 'Specialty', 'Other']
    labor_resources = conn.execute("SELECT id, (first_name || ' ' || last_name) as employee_name FROM labor_resources WHERE status = 'Active' ORDER BY last_name, first_name").fetchall()
    conn.close()
    return render_template('tools/edit.html', tool=tool, categories=categories, labor_resources=labor_resources)

@tools_bp.route('/tools/<int:tool_id>/checkout', methods=['POST'])
@role_required('Admin', 'Procurement', 'Production Staff')
def checkout_tool(tool_id):
    db = Database()
    conn = db.get_connection()
    
    tool = conn.execute('SELECT * FROM tools WHERE id = ?', (tool_id,)).fetchone()
    if not tool:
        flash('Tool not found', 'danger')
        conn.close()
        return redirect(url_for('tools_routes.list_tools'))
    
    if tool['status'] != 'Available':
        flash('Tool is not available for checkout', 'warning')
        conn.close()
        return redirect(url_for('tools_routes.view_tool', tool_id=tool_id))
    
    conn.execute('''
        INSERT INTO tool_checkouts (tool_id, checked_out_by, work_order_id, expected_return_date, condition_on_checkout, notes)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        tool_id,
        request.form['checked_out_by'],
        request.form.get('work_order_id') or None,
        request.form.get('expected_return_date') or None,
        tool['condition'],
        request.form.get('notes', '')
    ))
    
    conn.execute('UPDATE tools SET status = ?, assigned_to = ? WHERE id = ?',
                ('In Use', request.form['checked_out_by'], tool_id))
    conn.commit()
    
    flash('Tool checked out successfully!', 'success')
    conn.close()
    return redirect(url_for('tools_routes.view_tool', tool_id=tool_id))

@tools_bp.route('/tools/<int:tool_id>/checkin', methods=['POST'])
@role_required('Admin', 'Procurement', 'Production Staff')
def checkin_tool(tool_id):
    db = Database()
    conn = db.get_connection()
    
    checkout = conn.execute('''
        SELECT * FROM tool_checkouts 
        WHERE tool_id = ? AND return_date IS NULL
        ORDER BY checkout_date DESC LIMIT 1
    ''', (tool_id,)).fetchone()
    
    if not checkout:
        flash('No active checkout found', 'warning')
        conn.close()
        return redirect(url_for('tools_routes.view_tool', tool_id=tool_id))
    
    condition = request.form.get('condition_on_return', 'Good')
    
    conn.execute('''
        UPDATE tool_checkouts SET return_date = CURRENT_TIMESTAMP, condition_on_return = ?, notes = ?
        WHERE id = ?
    ''', (condition, request.form.get('notes', ''), checkout['id']))
    
    conn.execute('UPDATE tools SET status = ?, condition = ?, assigned_to = NULL WHERE id = ?',
                ('Available', condition, tool_id))
    conn.commit()
    
    flash('Tool checked in successfully!', 'success')
    conn.close()
    return redirect(url_for('tools_routes.view_tool', tool_id=tool_id))

@tools_bp.route('/tools/<int:tool_id>/delete', methods=['POST'])
@role_required('Admin')
def delete_tool(tool_id):
    db = Database()
    conn = db.get_connection()
    
    tool = conn.execute('SELECT * FROM tools WHERE id = ?', (tool_id,)).fetchone()
    if tool:
        AuditLogger.log_change(conn, 'tools', tool_id, 'DELETE', session.get('user_id'),
                              {'tool_number': tool['tool_number']})
        conn.execute('DELETE FROM tools WHERE id = ?', (tool_id,))
        conn.commit()
        flash('Tool deleted successfully!', 'success')
    
    conn.close()
    return redirect(url_for('tools_routes.list_tools'))
