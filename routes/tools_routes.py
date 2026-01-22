from flask import Blueprint, render_template, request, redirect, url_for, flash, session, Response
from models import Database, AuditLogger
from auth import login_required, role_required
from datetime import datetime, timedelta
from reportlab.lib.pagesizes import letter, inch
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from reportlab.graphics.barcode import code128
import io

tools_bp = Blueprint('tools_routes', __name__)

def create_tool_journal_entry(conn, tool_number, tool_name, amount, is_addition=True):
    """Create journal entry for tool capitalization or disposal"""
    if amount <= 0:
        return None
    
    # Get Equipment account (1210) and Cash account (1110)
    equipment_account = conn.execute(
        "SELECT id FROM chart_of_accounts WHERE account_code = '1210'"
    ).fetchone()
    cash_account = conn.execute(
        "SELECT id FROM chart_of_accounts WHERE account_code = '1110'"
    ).fetchone()
    
    if not equipment_account or not cash_account:
        return None
    
    # Generate entry number
    last_entry = conn.execute('''
        SELECT entry_number FROM gl_entries 
        WHERE entry_number LIKE 'JE-%'
        ORDER BY CAST(SUBSTR(entry_number, 4) AS INTEGER) DESC 
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
    
    entry_number = f'JE-{next_number:06d}'
    
    if is_addition:
        # Tool purchase: Debit Equipment (asset up), Credit Cash (asset down)
        description = f'Purchase tool: {tool_number} - {tool_name}'
        debit_account = equipment_account['id']
        credit_account = cash_account['id']
    else:
        # Tool disposal: Debit Cash (asset up if sold), Credit Equipment (asset down)
        description = f'Dispose tool: {tool_number} - {tool_name}'
        debit_account = cash_account['id']
        credit_account = equipment_account['id']
    
    # Create journal entry
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO gl_entries (entry_number, entry_date, description, transaction_source, status, created_by, created_at)
        VALUES (?, date('now'), ?, 'Tool Capitalization', 'Posted', ?, datetime('now'))
    ''', (entry_number, description, session.get('user_id')))
    
    entry_id = cursor.lastrowid
    
    # Create journal entry lines
    cursor.execute('''
        INSERT INTO gl_entry_lines (gl_entry_id, account_id, debit, credit, description)
        VALUES (?, ?, ?, 0, ?)
    ''', (entry_id, debit_account, amount, f'{tool_number} - {tool_name}'))
    
    cursor.execute('''
        INSERT INTO gl_entry_lines (gl_entry_id, account_id, debit, credit, description)
        VALUES (?, ?, 0, ?, ?)
    ''', (entry_id, credit_account, amount, f'{tool_number} - {tool_name}'))
    
    return entry_number

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

def calculate_next_calibration_date(last_calibration_date, calibration_interval_days):
    """Calculate next calibration date based on last calibration and interval"""
    if not last_calibration_date or not calibration_interval_days:
        return None
    
    try:
        if isinstance(last_calibration_date, str):
            last_date = datetime.strptime(last_calibration_date, '%Y-%m-%d')
        else:
            last_date = last_calibration_date
        
        next_date = last_date + timedelta(days=int(calibration_interval_days))
        return next_date.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return None

@tools_bp.route('/tools')
@login_required
def list_tools():
    db = Database()
    conn = db.get_connection()
    
    status_filter = request.args.get('status', '')
    category_filter = request.args.get('category', '')
    condition_filter = request.args.get('condition', '')
    location_filter = request.args.get('location', '')
    search = request.args.get('search', '')
    sort_by = request.args.get('sort', 'tool_number')
    sort_dir = request.args.get('dir', 'asc')
    
    valid_sort_columns = {
        'tool_number': 't.tool_number',
        'name': 't.name',
        'category': 't.category',
        'location': 't.location',
        'status': 't.status',
        'condition': 't.condition',
        'next_calibration_date': 't.next_calibration_date'
    }
    sort_column = valid_sort_columns.get(sort_by, 't.tool_number')
    sort_direction = 'DESC' if sort_dir.lower() == 'desc' else 'ASC'
    
    query = '''
        SELECT t.*, 
               (lr.first_name || ' ' || lr.last_name) as assigned_to_name,
               (SELECT COUNT(*) FROM tool_checkouts tc WHERE tc.tool_id = t.id AND tc.return_date IS NULL) as is_checked_out
        FROM tools t
        LEFT JOIN labor_resources lr ON t.assigned_to = lr.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND t.status = ?'
        params.append(status_filter)
    
    if category_filter:
        query += ' AND t.category = ?'
        params.append(category_filter)
    
    if condition_filter:
        query += ' AND t.condition = ?'
        params.append(condition_filter)
    
    if location_filter:
        query += ' AND t.location = ?'
        params.append(location_filter)
    
    if search:
        query += ' AND (t.tool_number LIKE ? OR t.name LIKE ? OR t.serial_number LIKE ? OR t.description LIKE ?)'
        search_term = f'%{search}%'
        params.extend([search_term, search_term, search_term, search_term])
    
    query += f' ORDER BY {sort_column} {sort_direction}'
    
    tools = conn.execute(query, params).fetchall()
    
    all_tools = conn.execute('SELECT * FROM tools').fetchall()
    stats = {
        'total': len(all_tools),
        'available': sum(1 for t in all_tools if t['status'] == 'Available'),
        'in_use': sum(1 for t in all_tools if t['status'] == 'In Use'),
        'maintenance': sum(1 for t in all_tools if t['status'] == 'Maintenance'),
        'calibration_due': sum(1 for t in all_tools if t['next_calibration_date'] and t['next_calibration_date'] <= datetime.now().strftime('%Y-%m-%d'))
    }
    
    categories = conn.execute('SELECT DISTINCT category FROM tools WHERE category IS NOT NULL AND category != "" ORDER BY category').fetchall()
    locations = conn.execute('SELECT DISTINCT location FROM tools WHERE location IS NOT NULL AND location != "" ORDER BY location').fetchall()
    
    conn.close()
    return render_template('tools/list.html', 
                         tools=tools, 
                         stats=stats, 
                         now=datetime.now().strftime('%Y-%m-%d'),
                         categories=[c['category'] for c in categories],
                         locations=[l['location'] for l in locations],
                         filters={
                             'status': status_filter,
                             'category': category_filter,
                             'condition': condition_filter,
                             'location': location_filter,
                             'search': search,
                             'sort': sort_by,
                             'dir': sort_dir
                         })

@tools_bp.route('/tools/create', methods=['GET', 'POST'])
@login_required
@role_required('Admin', 'Procurement')
def create_tool():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        tool_number = generate_tool_number(conn)
        
        last_calibration = request.form.get('last_calibration_date') or None
        calibration_interval = int(request.form.get('calibration_interval_days') or 0) or None
        next_calibration = calculate_next_calibration_date(last_calibration, calibration_interval)
        
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO tools (tool_number, name, description, category, manufacturer, 
                             model_number, serial_number, location, status, condition,
                             purchase_date, purchase_cost, supplier_id, last_calibration_date, 
                             next_calibration_date, calibration_interval_days, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            int(request.form.get('supplier_id')) if request.form.get('supplier_id') else None,
            last_calibration,
            next_calibration,
            calibration_interval,
            request.form.get('notes', '')
        ))
        
        tool_id = cursor.lastrowid
        
        # Create journal entry for tool capitalization if purchase cost > 0
        purchase_cost = float(request.form.get('purchase_cost') or 0)
        tool_name = request.form['name']
        if purchase_cost > 0:
            je_number = create_tool_journal_entry(conn, tool_number, tool_name, purchase_cost, is_addition=True)
            if je_number:
                flash(f'Journal Entry {je_number} created for tool capitalization', 'info')
        
        conn.commit()
        
        AuditLogger.log_change(conn, 'tools', tool_id, 'CREATE', session.get('user_id'),
                              {'tool_number': tool_number, 'name': tool_name})
        conn.commit()
        
        flash(f'Tool {tool_number} created successfully!', 'success')
        conn.close()
        return redirect(url_for('tools_routes.list_tools'))
    
    categories = ['Hand Tool', 'Power Tool', 'Measuring', 'Calibrated', 'Safety', 'Specialty', 'Other']
    suppliers = conn.execute("SELECT id, name FROM suppliers WHERE status = 'Active' ORDER BY name").fetchall()
    conn.close()
    return render_template('tools/create.html', categories=categories, suppliers=suppliers)

@tools_bp.route('/tools/<int:tool_id>')
@login_required
def view_tool(tool_id):
    db = Database()
    conn = db.get_connection()
    
    tool = conn.execute('''
        SELECT t.*, (lr.first_name || ' ' || lr.last_name) as assigned_to_name,
               s.name as supplier_name
        FROM tools t
        LEFT JOIN labor_resources lr ON t.assigned_to = lr.id
        LEFT JOIN suppliers s ON t.supplier_id = s.id
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
    return render_template('tools/view.html', tool=tool, checkouts=checkouts, now=datetime.now().strftime('%Y-%m-%d'))

@tools_bp.route('/tools/<int:tool_id>/edit', methods=['GET', 'POST'])
@login_required
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
        
        last_calibration = request.form.get('last_calibration_date') or None
        calibration_interval = int(request.form.get('calibration_interval_days') or 0) or None
        next_calibration = calculate_next_calibration_date(last_calibration, calibration_interval)
        
        conn.execute('''
            UPDATE tools SET name = ?, description = ?, category = ?, manufacturer = ?,
                           model_number = ?, serial_number = ?, location = ?, status = ?,
                           condition = ?, purchase_date = ?, purchase_cost = ?, supplier_id = ?,
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
            int(request.form.get('supplier_id')) if request.form.get('supplier_id') else None,
            last_calibration,
            next_calibration,
            calibration_interval,
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
    suppliers = conn.execute("SELECT id, name FROM suppliers WHERE status = 'Active' ORDER BY name").fetchall()
    conn.close()
    return render_template('tools/edit.html', tool=tool, categories=categories, labor_resources=labor_resources, suppliers=suppliers)

@tools_bp.route('/tools/<int:tool_id>/checkout', methods=['POST'])
@login_required
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
@login_required
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
@login_required
@role_required('Admin')
def delete_tool(tool_id):
    db = Database()
    conn = db.get_connection()
    
    tool = conn.execute('SELECT * FROM tools WHERE id = ?', (tool_id,)).fetchone()
    if tool:
        # Create reversing journal entry if tool had purchase cost
        purchase_cost = float(tool['purchase_cost'] or 0)
        if purchase_cost > 0:
            je_number = create_tool_journal_entry(
                conn, tool['tool_number'], tool['name'], purchase_cost, is_addition=False
            )
            if je_number:
                flash(f'Journal Entry {je_number} created for tool disposal', 'info')
        
        AuditLogger.log_change(conn, 'tools', tool_id, 'DELETE', session.get('user_id'),
                              {'tool_number': tool['tool_number']})
        conn.execute('DELETE FROM tools WHERE id = ?', (tool_id,))
        conn.commit()
        flash('Tool deleted successfully!', 'success')
    
    conn.close()
    return redirect(url_for('tools_routes.list_tools'))


@tools_bp.route('/tools/mass-update', methods=['POST'])
@login_required
@role_required('Admin', 'Procurement')
def mass_update_tools():
    """Mass update selected tools"""
    db = Database()
    conn = db.get_connection()
    
    try:
        tool_ids = request.form.getlist('tool_ids')
        action = request.form.get('action', '')
        
        if not tool_ids:
            flash('No tools selected', 'warning')
            conn.close()
            return redirect(url_for('tools_routes.list_tools'))
        
        updated_count = 0
        
        if action == 'update_status':
            new_status = request.form.get('new_status')
            if new_status:
                for tool_id in tool_ids:
                    conn.execute('UPDATE tools SET status = ? WHERE id = ?', (new_status, int(tool_id)))
                    AuditLogger.log_change(conn, 'tools', int(tool_id), 'UPDATE', session.get('user_id'),
                                          {'field': 'status', 'new_value': new_status})
                    updated_count += 1
                conn.commit()
                flash(f'Updated status to "{new_status}" for {updated_count} tools', 'success')
        
        elif action == 'update_condition':
            new_condition = request.form.get('new_condition')
            if new_condition:
                for tool_id in tool_ids:
                    conn.execute('UPDATE tools SET condition = ? WHERE id = ?', (new_condition, int(tool_id)))
                    AuditLogger.log_change(conn, 'tools', int(tool_id), 'UPDATE', session.get('user_id'),
                                          {'field': 'condition', 'new_value': new_condition})
                    updated_count += 1
                conn.commit()
                flash(f'Updated condition to "{new_condition}" for {updated_count} tools', 'success')
        
        elif action == 'update_location':
            new_location = request.form.get('new_location')
            if new_location:
                for tool_id in tool_ids:
                    conn.execute('UPDATE tools SET location = ? WHERE id = ?', (new_location, int(tool_id)))
                    AuditLogger.log_change(conn, 'tools', int(tool_id), 'UPDATE', session.get('user_id'),
                                          {'field': 'location', 'new_value': new_location})
                    updated_count += 1
                conn.commit()
                flash(f'Updated location to "{new_location}" for {updated_count} tools', 'success')
        
        elif action == 'update_category':
            new_category = request.form.get('new_category')
            if new_category:
                for tool_id in tool_ids:
                    conn.execute('UPDATE tools SET category = ? WHERE id = ?', (new_category, int(tool_id)))
                    AuditLogger.log_change(conn, 'tools', int(tool_id), 'UPDATE', session.get('user_id'),
                                          {'field': 'category', 'new_value': new_category})
                    updated_count += 1
                conn.commit()
                flash(f'Updated category to "{new_category}" for {updated_count} tools', 'success')
        
        elif action == 'delete':
            for tool_id in tool_ids:
                tool = conn.execute('SELECT * FROM tools WHERE id = ?', (int(tool_id),)).fetchone()
                if tool:
                    purchase_cost = float(tool['purchase_cost'] or 0)
                    if purchase_cost > 0:
                        create_tool_journal_entry(conn, tool['tool_number'], tool['name'], purchase_cost, is_addition=False)
                    AuditLogger.log_change(conn, 'tools', int(tool_id), 'DELETE', session.get('user_id'),
                                          {'tool_number': tool['tool_number']})
                    conn.execute('DELETE FROM tools WHERE id = ?', (int(tool_id),))
                    updated_count += 1
            conn.commit()
            flash(f'Deleted {updated_count} tools', 'success')
        
        else:
            flash('Invalid action', 'danger')
        
        conn.close()
        return redirect(url_for('tools_routes.list_tools'))
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error updating tools: {str(e)}', 'danger')
        return redirect(url_for('tools_routes.list_tools'))


def draw_tool_label(c, tool_data, width, height):
    """Draw a single tool label on the canvas"""
    margin = 0.25 * inch
    y = height - margin
    
    c.setLineWidth(2)
    c.rect(margin/2, margin/2, width - margin, height - margin)
    
    c.setFont("Helvetica-Bold", 14)
    c.drawString(margin, y - 20, "TOOL IDENTIFICATION LABEL")
    y -= 35
    
    c.setLineWidth(1)
    c.line(margin, y, width - margin, y)
    y -= 20
    
    c.setFont("Helvetica-Bold", 11)
    c.drawString(margin, y, "Tool Name:")
    c.setFont("Helvetica", 11)
    tool_name = tool_data.get('name', 'N/A')
    if len(tool_name) > 35:
        tool_name = tool_name[:32] + "..."
    c.drawString(margin + 80, y, tool_name)
    y -= 18
    
    c.setFont("Helvetica-Bold", 10)
    c.drawString(margin, y, "Tool #:")
    c.setFont("Helvetica", 10)
    c.drawString(margin + 50, y, tool_data.get('tool_number', 'N/A'))
    y -= 16
    
    if tool_data.get('description'):
        c.setFont("Helvetica-Bold", 9)
        c.drawString(margin, y, "Description:")
        c.setFont("Helvetica", 9)
        desc = tool_data.get('description', '')[:50]
        if len(tool_data.get('description', '')) > 50:
            desc += "..."
        c.drawString(margin + 65, y, desc)
        y -= 14
    
    y -= 5
    c.setLineWidth(0.5)
    c.line(margin, y, width - margin, y)
    y -= 15
    
    c.setFont("Helvetica-Bold", 9)
    c.drawString(margin, y, "Manufacturer:")
    c.setFont("Helvetica", 9)
    c.drawString(margin + 75, y, tool_data.get('manufacturer', 'N/A') or 'N/A')
    y -= 14
    
    c.setFont("Helvetica-Bold", 9)
    c.drawString(margin, y, "Supplier:")
    c.setFont("Helvetica", 9)
    c.drawString(margin + 55, y, tool_data.get('supplier_name', 'N/A') or 'N/A')
    y -= 14
    
    c.setFont("Helvetica-Bold", 9)
    c.drawString(margin, y, "Purchase Date:")
    c.setFont("Helvetica", 9)
    purchase_date = tool_data.get('purchase_date', 'N/A') or 'N/A'
    c.drawString(margin + 80, y, str(purchase_date))
    y -= 18
    
    c.setLineWidth(0.5)
    c.line(margin, y, width - margin, y)
    y -= 15
    
    c.setFont("Helvetica-Bold", 10)
    c.drawString(margin, y, "CALIBRATION INFORMATION")
    y -= 16
    
    c.setFont("Helvetica-Bold", 9)
    c.drawString(margin, y, "Last Calibration:")
    c.setFont("Helvetica", 9)
    last_cal = tool_data.get('last_calibration_date', 'N/A') or 'N/A'
    c.drawString(margin + 90, y, str(last_cal))
    y -= 14
    
    c.setFont("Helvetica-Bold", 9)
    c.drawString(margin, y, "Next Calibration:")
    c.setFont("Helvetica", 9)
    next_cal = tool_data.get('next_calibration_date', 'N/A') or 'N/A'
    if next_cal != 'N/A':
        try:
            next_date = datetime.strptime(str(next_cal), '%Y-%m-%d')
            if next_date < datetime.now():
                c.setFillColor(colors.red)
                next_cal = f"{next_cal} (OVERDUE)"
            elif next_date < datetime.now() + timedelta(days=30):
                c.setFillColor(colors.orange)
                next_cal = f"{next_cal} (DUE SOON)"
        except:
            pass
    c.drawString(margin + 90, y, str(next_cal))
    c.setFillColor(colors.black)
    y -= 14
    
    if tool_data.get('calibration_interval_days'):
        c.setFont("Helvetica-Bold", 9)
        c.drawString(margin, y, "Cal. Interval:")
        c.setFont("Helvetica", 9)
        c.drawString(margin + 75, y, f"{tool_data.get('calibration_interval_days')} days")
        y -= 14
    
    y -= 5
    try:
        barcode = code128.Code128(tool_data.get('tool_number', 'N/A'), barHeight=25, barWidth=1.2)
        barcode.drawOn(c, margin, y - 30)
        c.setFont("Helvetica", 8)
        c.drawCentredString(width/2, y - 42, tool_data.get('tool_number', 'N/A'))
    except:
        c.setFont("Helvetica", 10)
        c.drawString(margin, y - 20, f"Tool #: {tool_data.get('tool_number', 'N/A')}")
    
    c.setFont("Helvetica", 7)
    c.drawString(margin, margin + 5, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")


@tools_bp.route('/tools/<int:tool_id>/label')
@login_required
def generate_tool_label(tool_id):
    """Generate printable tool label PDF"""
    db = Database()
    conn = db.get_connection()
    
    try:
        tool = conn.execute('''
            SELECT t.*, s.name as supplier_name
            FROM tools t
            LEFT JOIN suppliers s ON t.supplier_id = s.id
            WHERE t.id = ?
        ''', (tool_id,)).fetchone()
        
        if not tool:
            flash('Tool not found', 'danger')
            return redirect(url_for('tools_routes.list_tools'))
        
        tool_data = dict(tool)
        
        buffer = io.BytesIO()
        label_size = request.args.get('size', '4x3')
        copies = min(max(int(request.args.get('copies', 1)), 1), 10)
        
        if label_size == '4x6':
            page_width = 4 * inch
            page_height = 6 * inch
        elif label_size == '4x4':
            page_width = 4 * inch
            page_height = 4 * inch
        elif label_size == '4x3':
            page_width = 4 * inch
            page_height = 3 * inch
        elif label_size == '3x2':
            page_width = 3 * inch
            page_height = 2 * inch
        else:
            page_width = 4 * inch
            page_height = 3 * inch
        
        c = canvas.Canvas(buffer, pagesize=(page_width, page_height))
        
        for copy_num in range(copies):
            if copy_num > 0:
                c.showPage()
            draw_tool_label(c, tool_data, page_width, page_height)
        
        c.save()
        buffer.seek(0)
        
        tool_number = tool['tool_number'] or 'UNKNOWN'
        filename = f"Tool_Label_{tool_number}.pdf"
        
        return Response(
            buffer.getvalue(),
            mimetype='application/pdf',
            headers={
                'Content-Disposition': f'inline; filename="{filename}"',
                'Content-Type': 'application/pdf'
            }
        )
        
    finally:
        conn.close()


@tools_bp.route('/tools/mass-print-labels')
@login_required
def mass_print_labels():
    """Generate labels for multiple tools"""
    ids_param = request.args.get('ids', '')
    label_size = request.args.get('size', '4x3')
    
    if not ids_param:
        flash('No tools selected', 'warning')
        return redirect(url_for('tools_routes.list_tools'))
    
    try:
        ids = [int(id.strip()) for id in ids_param.split(',') if id.strip().isdigit()]
    except ValueError:
        flash('Invalid tool IDs provided', 'danger')
        return redirect(url_for('tools_routes.list_tools'))
    
    if not ids:
        flash('No valid tools selected', 'warning')
        return redirect(url_for('tools_routes.list_tools'))
    
    db = Database()
    conn = db.get_connection()
    
    try:
        placeholders = ','.join(['?' for _ in ids])
        tools = conn.execute(f'''
            SELECT t.*, s.name as supplier_name
            FROM tools t
            LEFT JOIN suppliers s ON t.supplier_id = s.id
            WHERE t.id IN ({placeholders})
        ''', ids).fetchall()
        
        if not tools:
            flash('No tools found', 'warning')
            return redirect(url_for('tools_routes.list_tools'))
        
        if label_size == '4x6':
            page_width = 4 * inch
            page_height = 6 * inch
        elif label_size == '4x4':
            page_width = 4 * inch
            page_height = 4 * inch
        elif label_size == '4x3':
            page_width = 4 * inch
            page_height = 3 * inch
        elif label_size == '3x2':
            page_width = 3 * inch
            page_height = 2 * inch
        else:
            page_width = 4 * inch
            page_height = 3 * inch
        
        buffer = io.BytesIO()
        c = canvas.Canvas(buffer, pagesize=(page_width, page_height))
        
        for i, tool in enumerate(tools):
            if i > 0:
                c.showPage()
            draw_tool_label(c, dict(tool), page_width, page_height)
        
        c.save()
        buffer.seek(0)
        
        filename = f"Tool_Labels_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        
        return Response(
            buffer.getvalue(),
            mimetype='application/pdf',
            headers={
                'Content-Disposition': f'inline; filename="{filename}"',
                'Content-Type': 'application/pdf'
            }
        )
        
    finally:
        conn.close()
