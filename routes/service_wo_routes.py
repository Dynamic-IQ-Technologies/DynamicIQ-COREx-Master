from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import Database
from decorators import login_required, role_required
from datetime import datetime

service_wo_bp = Blueprint('service_wo_routes', __name__)

@service_wo_bp.route('/service-work-orders')
@login_required
def list_service_work_orders():
    """List all service work orders"""
    db = Database()
    conn = db.get_connection()
    
    # Get filter parameters
    status_filter = request.args.get('status', '')
    service_type_filter = request.args.get('service_type', '')
    
    # Build query
    query = '''
        SELECT 
            swo.*,
            c.customer_number,
            c.company_name as customer_company,
            lr.first_name || ' ' || lr.last_name as assigned_name
        FROM service_work_orders swo
        LEFT JOIN customers c ON swo.customer_id = c.id
        LEFT JOIN labor_resources lr ON swo.assigned_to = lr.id
        WHERE 1=1
    '''
    params = []
    
    if status_filter:
        query += ' AND swo.status = ?'
        params.append(status_filter)
    
    if service_type_filter:
        query += ' AND swo.service_type = ?'
        params.append(service_type_filter)
    
    query += ' ORDER BY swo.created_at DESC'
    
    service_work_orders = conn.execute(query, params).fetchall()
    conn.close()
    
    return render_template('service_wo/list.html', 
                         service_work_orders=service_work_orders,
                         status_filter=status_filter,
                         service_type_filter=service_type_filter)

@service_wo_bp.route('/service-work-orders/new', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def create_service_work_order():
    """Create new service work order"""
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            # Get form data
            service_type = request.form.get('service_type')
            customer_id = request.form.get('customer_id')
            equipment_description = request.form.get('equipment_description', '').strip()
            equipment_serial = request.form.get('equipment_serial', '').strip()
            equipment_model = request.form.get('equipment_model', '').strip()
            priority = request.form.get('priority', 'Medium')
            due_date = request.form.get('due_date') or None
            assigned_to = request.form.get('assigned_to') or None
            location = request.form.get('location', '').strip()
            description = request.form.get('description', '').strip()
            
            # Get customer name if customer selected
            customer_name = None
            if customer_id:
                customer = conn.execute('SELECT company_name FROM customers WHERE id = ?', (customer_id,)).fetchone()
                if customer:
                    customer_name = customer['company_name']
            
            # Generate SWO number
            last_swo = conn.execute('SELECT swo_number FROM service_work_orders ORDER BY id DESC LIMIT 1').fetchone()
            if last_swo and last_swo['swo_number']:
                last_num = int(last_swo['swo_number'].replace('SWO-', ''))
                swo_number = f"SWO-{last_num + 1:05d}"
            else:
                swo_number = "SWO-00001"
            
            # Insert service work order
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO service_work_orders (
                    swo_number, service_type, customer_id, customer_name,
                    equipment_description, equipment_serial, equipment_model,
                    priority, status, due_date, assigned_to, location, description,
                    created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (swo_number, service_type, customer_id, customer_name,
                  equipment_description, equipment_serial, equipment_model,
                  priority, 'Open', due_date, assigned_to, location, description,
                  session.get('user_id')))
            
            swo_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            flash(f'Service Work Order {swo_number} created successfully!', 'success')
            return redirect(url_for('service_wo_routes.view_service_work_order', id=swo_id))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error creating service work order: {str(e)}', 'danger')
            return redirect(url_for('service_wo_routes.create_service_work_order'))
    
    # GET request - show form
    customers = conn.execute('SELECT * FROM customers ORDER BY company_name').fetchall()
    employees = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY last_name, first_name').fetchall()
    conn.close()
    
    return render_template('service_wo/create.html', customers=customers, employees=employees)

@service_wo_bp.route('/service-work-orders/<int:id>')
@login_required
def view_service_work_order(id):
    """View service work order details"""
    db = Database()
    conn = db.get_connection()
    
    # Get service work order
    swo = conn.execute('''
        SELECT 
            swo.*,
            c.customer_number,
            c.company_name as customer_company,
            c.billing_address,
            c.contact_person,
            c.email as customer_email,
            c.phone as customer_phone,
            lr.first_name || ' ' || lr.last_name as assigned_name,
            lr.employee_code as assigned_code
        FROM service_work_orders swo
        LEFT JOIN customers c ON swo.customer_id = c.id
        LEFT JOIN labor_resources lr ON swo.assigned_to = lr.id
        WHERE swo.id = ?
    ''', (id,)).fetchone()
    
    if not swo:
        flash('Service work order not found', 'danger')
        conn.close()
        return redirect(url_for('service_wo_routes.list_service_work_orders'))
    
    # Get labor entries
    labor_entries = conn.execute('''
        SELECT 
            swl.*,
            lr.employee_code,
            lr.first_name || ' ' || lr.last_name as employee_name
        FROM service_wo_labor swl
        JOIN labor_resources lr ON swl.employee_id = lr.id
        WHERE swl.swo_id = ?
        ORDER BY swl.work_date DESC, swl.id DESC
    ''', (id,)).fetchall()
    
    # Get materials
    materials = conn.execute('''
        SELECT 
            swm.*,
            p.code as product_code,
            p.name as product_name
        FROM service_wo_materials swm
        JOIN products p ON swm.product_id = p.id
        WHERE swm.swo_id = ?
        ORDER BY swm.id
    ''', (id,)).fetchall()
    
    # Get expenses
    expenses = conn.execute('''
        SELECT * FROM service_wo_expenses
        WHERE swo_id = ?
        ORDER BY expense_date DESC, id DESC
    ''', (id,)).fetchall()
    
    conn.close()
    
    return render_template('service_wo/view.html', 
                         swo=swo,
                         labor_entries=labor_entries,
                         materials=materials,
                         expenses=expenses)

@service_wo_bp.route('/service-work-orders/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def edit_service_work_order(id):
    """Edit service work order"""
    db = Database()
    conn = db.get_connection()
    
    swo = conn.execute('SELECT * FROM service_work_orders WHERE id = ?', (id,)).fetchone()
    
    if not swo:
        flash('Service work order not found', 'danger')
        conn.close()
        return redirect(url_for('service_wo_routes.list_service_work_orders'))
    
    # Prevent editing if invoiced
    if swo['invoiced']:
        flash('Cannot edit invoiced service work order', 'warning')
        conn.close()
        return redirect(url_for('service_wo_routes.view_service_work_order', id=id))
    
    if request.method == 'POST':
        try:
            # Get form data
            service_type = request.form.get('service_type')
            customer_id = request.form.get('customer_id')
            equipment_description = request.form.get('equipment_description', '').strip()
            equipment_serial = request.form.get('equipment_serial', '').strip()
            equipment_model = request.form.get('equipment_model', '').strip()
            priority = request.form.get('priority', 'Medium')
            due_date = request.form.get('due_date') or None
            assigned_to = request.form.get('assigned_to') or None
            location = request.form.get('location', '').strip()
            description = request.form.get('description', '').strip()
            service_notes = request.form.get('service_notes', '').strip()
            
            # Get customer name if customer selected
            customer_name = None
            if customer_id:
                customer = conn.execute('SELECT company_name FROM customers WHERE id = ?', (customer_id,)).fetchone()
                if customer:
                    customer_name = customer['company_name']
            
            # Update service work order
            conn.execute('''
                UPDATE service_work_orders 
                SET service_type = ?, customer_id = ?, customer_name = ?,
                    equipment_description = ?, equipment_serial = ?, equipment_model = ?,
                    priority = ?, due_date = ?, assigned_to = ?, location = ?,
                    description = ?, service_notes = ?, modified_by = ?, modified_at = ?
                WHERE id = ?
            ''', (service_type, customer_id, customer_name,
                  equipment_description, equipment_serial, equipment_model,
                  priority, due_date, assigned_to, location, description, service_notes,
                  session.get('user_id'), datetime.now(), id))
            
            conn.commit()
            conn.close()
            
            flash('Service work order updated successfully!', 'success')
            return redirect(url_for('service_wo_routes.view_service_work_order', id=id))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error updating service work order: {str(e)}', 'danger')
            return redirect(url_for('service_wo_routes.edit_service_work_order', id=id))
    
    # GET request - show form
    customers = conn.execute('SELECT * FROM customers ORDER BY company_name').fetchall()
    employees = conn.execute('SELECT * FROM labor_resources WHERE status = "Active" ORDER BY last_name, first_name').fetchall()
    conn.close()
    
    return render_template('service_wo/edit.html', swo=swo, customers=customers, employees=employees)

@service_wo_bp.route('/service-work-orders/<int:id>/add-labor', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def add_labor(id):
    """Add labor entry to service work order"""
    db = Database()
    conn = db.get_connection()
    
    try:
        employee_id = request.form.get('employee_id')
        labor_type = request.form.get('labor_type')
        hours_worked = float(request.form.get('hours_worked', 0))
        work_date = request.form.get('work_date') or datetime.now().strftime('%Y-%m-%d')
        description = request.form.get('description', '').strip()
        
        # Get employee hourly rate
        employee = conn.execute('SELECT hourly_rate FROM labor_resources WHERE id = ?', (employee_id,)).fetchone()
        if not employee:
            flash('Employee not found', 'danger')
            conn.close()
            return redirect(url_for('service_wo_routes.view_service_work_order', id=id))
        
        hourly_rate = employee['hourly_rate']
        
        # Apply multiplier for overtime/NDT
        if labor_type == 'Overtime':
            hourly_rate = hourly_rate * 1.5
        elif labor_type == 'NDT':
            hourly_rate = hourly_rate * 1.3  # 30% premium for NDT work
        
        labor_cost = hours_worked * hourly_rate
        
        # Insert labor entry
        conn.execute('''
            INSERT INTO service_wo_labor (
                swo_id, employee_id, labor_type, hours_worked, hourly_rate,
                labor_cost, work_date, description, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (id, employee_id, labor_type, hours_worked, hourly_rate,
              labor_cost, work_date, description, session.get('user_id')))
        
        # Update service work order labor subtotal
        update_service_wo_costs(conn, id)
        
        conn.commit()
        conn.close()
        
        flash(f'Labor entry added successfully! ({hours_worked} hours @ ${hourly_rate:.2f}/hr = ${labor_cost:.2f})', 'success')
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error adding labor entry: {str(e)}', 'danger')
    
    return redirect(url_for('service_wo_routes.view_service_work_order', id=id))

@service_wo_bp.route('/service-work-orders/<int:id>/add-material', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def add_material(id):
    """Add material to service work order"""
    db = Database()
    conn = db.get_connection()
    
    try:
        product_id = request.form.get('product_id')
        quantity = float(request.form.get('quantity', 0))
        unit_price = float(request.form.get('unit_price', 0))
        allocate_inventory = request.form.get('allocate_inventory') == 'on'
        description = request.form.get('description', '').strip()
        
        total_cost = quantity * unit_price
        
        # Insert material entry
        conn.execute('''
            INSERT INTO service_wo_materials (
                swo_id, product_id, quantity, unit_price, total_cost,
                allocated_from_inventory, description, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (id, product_id, quantity, unit_price, total_cost,
              1 if allocate_inventory else 0, description, session.get('user_id')))
        
        # If allocating from inventory, deduct stock
        if allocate_inventory:
            product = conn.execute('SELECT code, name FROM products WHERE id = ?', (product_id,)).fetchone()
            conn.execute('''
                UPDATE inventory 
                SET quantity = quantity - ?
                WHERE product_id = ?
            ''', (quantity, product_id))
            
            # Log material issuance
            conn.execute('''
                INSERT INTO material_issuances (
                    product_id, quantity, issued_to, reference, notes, issued_by
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (product_id, quantity, f'Service WO', f'SWO-{id}',
                  f'Material issued to service work order', session.get('user_id')))
        
        # Update service work order materials subtotal
        update_service_wo_costs(conn, id)
        
        conn.commit()
        conn.close()
        
        flash(f'Material added successfully! (Qty: {quantity} @ ${unit_price:.2f} = ${total_cost:.2f})', 'success')
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error adding material: {str(e)}', 'danger')
    
    return redirect(url_for('service_wo_routes.view_service_work_order', id=id))

@service_wo_bp.route('/service-work-orders/<int:id>/add-expense', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def add_expense(id):
    """Add expense to service work order"""
    db = Database()
    conn = db.get_connection()
    
    try:
        expense_type = request.form.get('expense_type')
        amount = float(request.form.get('amount', 0))
        description = request.form.get('description', '').strip()
        vendor_name = request.form.get('vendor_name', '').strip()
        receipt_reference = request.form.get('receipt_reference', '').strip()
        expense_date = request.form.get('expense_date') or datetime.now().strftime('%Y-%m-%d')
        
        # Insert expense entry
        conn.execute('''
            INSERT INTO service_wo_expenses (
                swo_id, expense_type, description, amount, vendor_name,
                receipt_reference, expense_date, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (id, expense_type, description, amount, vendor_name,
              receipt_reference, expense_date, session.get('user_id')))
        
        # Update service work order expenses subtotal
        update_service_wo_costs(conn, id)
        
        conn.commit()
        conn.close()
        
        flash(f'{expense_type} expense added successfully! (${amount:.2f})', 'success')
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error adding expense: {str(e)}', 'danger')
    
    return redirect(url_for('service_wo_routes.view_service_work_order', id=id))

def update_service_wo_costs(conn, swo_id):
    """Update service work order cost totals"""
    # Calculate labor subtotal
    labor_subtotal = conn.execute('''
        SELECT COALESCE(SUM(labor_cost), 0) as total
        FROM service_wo_labor
        WHERE swo_id = ?
    ''', (swo_id,)).fetchone()['total']
    
    # Calculate materials subtotal
    materials_subtotal = conn.execute('''
        SELECT COALESCE(SUM(total_cost), 0) as total
        FROM service_wo_materials
        WHERE swo_id = ?
    ''', (swo_id,)).fetchone()['total']
    
    # Calculate expenses subtotal
    expenses_subtotal = conn.execute('''
        SELECT COALESCE(SUM(amount), 0) as total
        FROM service_wo_expenses
        WHERE swo_id = ?
    ''', (swo_id,)).fetchone()['total']
    
    total_cost = labor_subtotal + materials_subtotal + expenses_subtotal
    
    # Update service work order
    conn.execute('''
        UPDATE service_work_orders
        SET labor_subtotal = ?, materials_subtotal = ?, expenses_subtotal = ?, total_cost = ?
        WHERE id = ?
    ''', (labor_subtotal, materials_subtotal, expenses_subtotal, total_cost, swo_id))

@service_wo_bp.route('/service-work-orders/<int:id>/update-status', methods=['POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def update_status(id):
    """Update service work order status"""
    db = Database()
    conn = db.get_connection()
    
    try:
        new_status = request.form.get('status')
        
        update_data = {'status': new_status}
        
        # If completing, set completed_at
        if new_status == 'Completed':
            update_data['completed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Build update query
        set_clause = ', '.join([f'{key} = ?' for key in update_data.keys()])
        values = list(update_data.values()) + [id]
        
        conn.execute(f'UPDATE service_work_orders SET {set_clause} WHERE id = ?', values)
        conn.commit()
        conn.close()
        
        flash(f'Service work order status updated to {new_status}', 'success')
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error updating status: {str(e)}', 'danger')
    
    return redirect(url_for('service_wo_routes.view_service_work_order', id=id))

@service_wo_bp.route('/service-work-orders/<int:id>/approve', methods=['POST'])
@role_required('Admin', 'Planner')
def approve_service_work_order(id):
    """Approve service work order for invoicing"""
    db = Database()
    conn = db.get_connection()
    
    try:
        swo = conn.execute('SELECT * FROM service_work_orders WHERE id = ?', (id,)).fetchone()
        
        if not swo:
            flash('Service work order not found', 'danger')
            conn.close()
            return redirect(url_for('service_wo_routes.list_service_work_orders'))
        
        if swo['status'] != 'Completed':
            flash('Can only approve completed service work orders', 'warning')
            conn.close()
            return redirect(url_for('service_wo_routes.view_service_work_order', id=id))
        
        # Update approval
        conn.execute('''
            UPDATE service_work_orders
            SET approved_by = ?, approved_at = ?
            WHERE id = ?
        ''', (session.get('user_id'), datetime.now(), id))
        
        conn.commit()
        conn.close()
        
        flash('Service work order approved! Ready for invoicing.', 'success')
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error approving service work order: {str(e)}', 'danger')
    
    return redirect(url_for('service_wo_routes.view_service_work_order', id=id))

@service_wo_bp.route('/service-work-orders/<int:id>/delete-labor/<int:labor_id>', methods=['POST'])
@role_required('Admin', 'Planner')
def delete_labor(id, labor_id):
    """Delete labor entry"""
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('DELETE FROM service_wo_labor WHERE id = ? AND swo_id = ?', (labor_id, id))
        update_service_wo_costs(conn, id)
        conn.commit()
        conn.close()
        flash('Labor entry deleted successfully', 'success')
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error deleting labor entry: {str(e)}', 'danger')
    
    return redirect(url_for('service_wo_routes.view_service_work_order', id=id))

@service_wo_bp.route('/service-work-orders/<int:id>/delete-material/<int:material_id>', methods=['POST'])
@role_required('Admin', 'Planner')
def delete_material(id, material_id):
    """Delete material entry"""
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('DELETE FROM service_wo_materials WHERE id = ? AND swo_id = ?', (material_id, id))
        update_service_wo_costs(conn, id)
        conn.commit()
        conn.close()
        flash('Material entry deleted successfully', 'success')
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error deleting material entry: {str(e)}', 'danger')
    
    return redirect(url_for('service_wo_routes.view_service_work_order', id=id))

@service_wo_bp.route('/service-work-orders/<int:id>/delete-expense/<int:expense_id>', methods=['POST'])
@role_required('Admin', 'Planner')
def delete_expense(id, expense_id):
    """Delete expense entry"""
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('DELETE FROM service_wo_expenses WHERE id = ? AND swo_id = ?', (expense_id, id))
        update_service_wo_costs(conn, id)
        conn.commit()
        conn.close()
        flash('Expense entry deleted successfully', 'success')
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error deleting expense entry: {str(e)}', 'danger')
    
    return redirect(url_for('service_wo_routes.view_service_work_order', id=id))

@service_wo_bp.route('/service-work-orders/<int:id>/generate-invoice', methods=['POST'])
@role_required('Admin', 'Planner')
def generate_invoice(id):
    """Generate invoice from service work order"""
    db = Database()
    conn = db.get_connection()
    
    try:
        swo = conn.execute('SELECT * FROM service_work_orders WHERE id = ?', (id,)).fetchone()
        
        if not swo:
            flash('Service work order not found', 'danger')
            conn.close()
            return redirect(url_for('service_wo_routes.list_service_work_orders'))
        
        # Validation: Must be completed and approved
        if swo['status'] != 'Completed':
            flash('Service work order must be completed before invoicing', 'warning')
            conn.close()
            return redirect(url_for('service_wo_routes.view_service_work_order', id=id))
        
        if not swo['approved_at']:
            flash('Service work order must be approved before invoicing', 'warning')
            conn.close()
            return redirect(url_for('service_wo_routes.view_service_work_order', id=id))
        
        if swo['invoiced']:
            flash('Service work order has already been invoiced', 'warning')
            conn.close()
            return redirect(url_for('service_wo_routes.view_service_work_order', id=id))
        
        # Generate invoice number
        last_invoice = conn.execute('SELECT invoice_number FROM invoices ORDER BY id DESC LIMIT 1').fetchone()
        if last_invoice and last_invoice['invoice_number']:
            last_num = int(last_invoice['invoice_number'].replace('INV-', ''))
            invoice_number = f"INV-{last_num + 1:05d}"
        else:
            invoice_number = "INV-00001"
        
        # Create invoice
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO invoices (
                invoice_number, customer_id, invoice_date, due_date, status,
                subtotal, tax_amount, total_amount, source_type, source_id,
                notes, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (invoice_number, swo['customer_id'], datetime.now().strftime('%Y-%m-%d'),
              None, 'Draft', swo['total_cost'], 0, swo['total_cost'],
              'Service Work Order', id, f"Invoice for Service Work Order {swo['swo_number']}",
              session.get('user_id')))
        
        invoice_id = cursor.lastrowid
        
        # Create invoice lines from labor entries
        labor_entries = conn.execute('SELECT * FROM service_wo_labor WHERE swo_id = ?', (id,)).fetchall()
        line_number = 1
        for labor in labor_entries:
            emp = conn.execute('SELECT first_name, last_name FROM labor_resources WHERE id = ?', (labor['employee_id'],)).fetchone()
            emp_name = f"{emp['first_name']} {emp['last_name']}" if emp else "Unknown"
            description = f"{labor['labor_type']} Labor - {emp_name} ({labor['hours_worked']} hrs)"
            if labor['description']:
                description += f" - {labor['description']}"
            
            conn.execute('''
                INSERT INTO invoice_lines (
                    invoice_id, line_number, description, quantity, unit_price, line_total
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (invoice_id, line_number, description, labor['hours_worked'], 
                  labor['hourly_rate'], labor['labor_cost']))
            line_number += 1
        
        # Create invoice lines from materials
        materials = conn.execute('''
            SELECT swm.*, p.code, p.name
            FROM service_wo_materials swm
            JOIN products p ON swm.product_id = p.id
            WHERE swm.swo_id = ?
        ''', (id,)).fetchall()
        
        for material in materials:
            description = f"Material: {material['code']} - {material['name']}"
            if material['description']:
                description += f" - {material['description']}"
            
            conn.execute('''
                INSERT INTO invoice_lines (
                    invoice_id, line_number, description, quantity, unit_price, line_total
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (invoice_id, line_number, description, material['quantity'],
                  material['unit_price'], material['total_cost']))
            line_number += 1
        
        # Create invoice lines from expenses
        expenses = conn.execute('SELECT * FROM service_wo_expenses WHERE swo_id = ?', (id,)).fetchall()
        for expense in expenses:
            description = f"{expense['expense_type']}"
            if expense['description']:
                description += f" - {expense['description']}"
            
            conn.execute('''
                INSERT INTO invoice_lines (
                    invoice_id, line_number, description, quantity, unit_price, line_total
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (invoice_id, line_number, description, 1, expense['amount'], expense['amount']))
            line_number += 1
        
        # Update service work order as invoiced
        conn.execute('''
            UPDATE service_work_orders
            SET invoiced = 1, invoice_id = ?
            WHERE id = ?
        ''', (invoice_id, id))
        
        conn.commit()
        conn.close()
        
        flash(f'Invoice {invoice_number} generated successfully!', 'success')
        return redirect(url_for('invoice_routes.view_invoice', id=invoice_id))
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error generating invoice: {str(e)}', 'danger')
        return redirect(url_for('service_wo_routes.view_service_work_order', id=id))
