from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database, AuditLogger
from auth import login_required, role_required
import csv
import io
import secrets
from datetime import datetime

customer_bp = Blueprint('customer_routes', __name__)

@customer_bp.route('/customers')
@login_required
def list_customers():
    db = Database()
    conn = db.get_connection()
    
    customers = conn.execute('''
        SELECT * FROM customers
        ORDER BY customer_number DESC
    ''').fetchall()
    
    conn.close()
    return render_template('customers/list.html', customers=customers)

@customer_bp.route('/customers/create', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def create_customer():
    if request.method == 'POST':
        db = Database()
        conn = db.get_connection()
        try:
            payment_terms = request.form.get('payment_terms', '30').strip()
            payment_terms = int(payment_terms) if payment_terms else 30
            
            credit_limit = request.form.get('credit_limit', '0').strip()
            credit_limit = float(credit_limit) if credit_limit else 0.0
            
            # Generate customer number
            last_customer = conn.execute(
                'SELECT customer_number FROM customers ORDER BY id DESC LIMIT 1'
            ).fetchone()
            
            if last_customer:
                last_num = int(last_customer['customer_number'].split('-')[1])
                customer_number = f'CUST-{last_num + 1:06d}'
            else:
                customer_number = 'CUST-000001'
            
            # Insert customer
            conn.execute('''
                INSERT INTO customers (
                    customer_number, name, contact_person, email, phone,
                    billing_address, shipping_address, payment_terms, credit_limit,
                    tax_exempt, notes, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                customer_number,
                request.form['name'],
                request.form.get('contact_person', ''),
                request.form.get('email', ''),
                request.form.get('phone', ''),
                request.form.get('billing_address', ''),
                request.form.get('shipping_address', ''),
                payment_terms,
                credit_limit,
                1 if request.form.get('tax_exempt') else 0,
                request.form.get('notes', ''),
                request.form.get('status', 'Active')
            ))
            
            customer_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
            AuditLogger.log_change(conn, 'customers', customer_id, 'CREATE', session.get('user_id'),
                                  {'customer_number': customer_number, 'name': request.form['name']})
            conn.commit()
            flash(f'Customer created successfully! Customer #: {customer_number}', 'success')
            return redirect(url_for('customer_routes.list_customers'))
            
        except ValueError:
            conn.rollback()
            flash('Please enter valid numbers for payment terms and credit limit.', 'danger')
        except Exception as e:
            conn.rollback()
            flash('An error occurred while creating the customer. Please try again.', 'danger')
        finally:
            conn.close()
    
    return render_template('customers/create.html')

@customer_bp.route('/customers/<int:id>')
@login_required
def view_customer(id):
    db = Database()
    conn = db.get_connection()
    
    customer = conn.execute('SELECT * FROM customers WHERE id = ?', (id,)).fetchone()
    
    if not customer:
        flash('Customer not found', 'danger')
        conn.close()
        return redirect(url_for('customer_routes.list_customers'))
    
    # Get sales history
    sales_history = conn.execute('''
        SELECT so.*, COUNT(sol.id) as line_count
        FROM sales_orders so
        LEFT JOIN sales_order_lines sol ON so.id = sol.so_id
        WHERE so.customer_id = ?
        GROUP BY so.id
        ORDER BY so.order_date DESC
    ''', (id,)).fetchall()
    
    # Get financial metrics
    financials = {}
    
    # Total sales (all time)
    total_sales = conn.execute('''
        SELECT COALESCE(SUM(total_amount), 0) as total
        FROM sales_orders WHERE customer_id = ? AND status NOT IN ('Cancelled', 'Draft')
    ''', (id,)).fetchone()
    financials['total_sales'] = total_sales['total'] if total_sales else 0
    
    # Total sales this year
    ytd_sales = conn.execute('''
        SELECT COALESCE(SUM(total_amount), 0) as total
        FROM sales_orders 
        WHERE customer_id = ? AND status NOT IN ('Cancelled', 'Draft')
        AND strftime('%Y', order_date) = strftime('%Y', 'now')
    ''', (id,)).fetchone()
    financials['ytd_sales'] = ytd_sales['total'] if ytd_sales else 0
    
    # Total sales last year
    last_year_sales = conn.execute('''
        SELECT COALESCE(SUM(total_amount), 0) as total
        FROM sales_orders 
        WHERE customer_id = ? AND status NOT IN ('Cancelled', 'Draft')
        AND strftime('%Y', order_date) = strftime('%Y', 'now', '-1 year')
    ''', (id,)).fetchone()
    financials['last_year_sales'] = last_year_sales['total'] if last_year_sales else 0
    
    # Pending invoice amount (balance due) - exclude Draft and Cancelled orders
    pending_invoices = conn.execute('''
        SELECT COALESCE(SUM(balance_due), 0) as total
        FROM sales_orders WHERE customer_id = ? AND balance_due > 0
        AND status NOT IN ('Cancelled', 'Draft')
    ''', (id,)).fetchone()
    financials['pending_invoices'] = pending_invoices['total'] if pending_invoices else 0
    
    # Total orders count
    order_count = conn.execute('''
        SELECT COUNT(*) as count FROM sales_orders 
        WHERE customer_id = ? AND status NOT IN ('Cancelled', 'Draft')
    ''', (id,)).fetchone()
    financials['order_count'] = order_count['count'] if order_count else 0
    
    # Average order value
    financials['avg_order_value'] = financials['total_sales'] / financials['order_count'] if financials['order_count'] > 0 else 0
    
    # Last order date
    last_order = conn.execute('''
        SELECT order_date FROM sales_orders 
        WHERE customer_id = ? AND status NOT IN ('Cancelled', 'Draft')
        ORDER BY order_date DESC LIMIT 1
    ''', (id,)).fetchone()
    financials['last_order_date'] = last_order['order_date'] if last_order else None
    
    # First order date (customer since)
    first_order = conn.execute('''
        SELECT order_date FROM sales_orders 
        WHERE customer_id = ? AND status NOT IN ('Cancelled', 'Draft')
        ORDER BY order_date ASC LIMIT 1
    ''', (id,)).fetchone()
    financials['first_order_date'] = first_order['order_date'] if first_order else None
    
    # Credit utilization
    if customer['credit_limit'] and customer['credit_limit'] > 0:
        financials['credit_utilization'] = (financials['pending_invoices'] / customer['credit_limit']) * 100
    else:
        financials['credit_utilization'] = 0
    
    # Get audit trail
    audit_trail = conn.execute('''
        SELECT at.*, u.username 
        FROM audit_trail at
        LEFT JOIN users u ON at.modified_by = u.id
        WHERE at.record_type = 'customers' AND at.record_id = ?
        ORDER BY at.modified_at DESC
        LIMIT 50
    ''', (str(id),)).fetchall()
    
    conn.close()
    return render_template('customers/view.html', customer=customer, sales_history=sales_history, 
                          audit_trail=audit_trail, financials=financials)

@customer_bp.route('/customers/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def edit_customer(id):
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            payment_terms = request.form.get('payment_terms', '30').strip()
            payment_terms = int(payment_terms) if payment_terms else 30
            
            credit_limit = request.form.get('credit_limit', '0').strip()
            credit_limit = float(credit_limit) if credit_limit else 0.0
            
            old_customer = conn.execute('SELECT * FROM customers WHERE id = ?', (id,)).fetchone()
            
            conn.execute('''
                UPDATE customers SET
                    name = ?, contact_person = ?, email = ?, phone = ?,
                    billing_address = ?, shipping_address = ?, payment_terms = ?,
                    credit_limit = ?, tax_exempt = ?, notes = ?, status = ?
                WHERE id = ?
            ''', (
                request.form['name'],
                request.form.get('contact_person', ''),
                request.form.get('email', ''),
                request.form.get('phone', ''),
                request.form.get('billing_address', ''),
                request.form.get('shipping_address', ''),
                payment_terms,
                credit_limit,
                1 if request.form.get('tax_exempt') else 0,
                request.form.get('notes', ''),
                request.form.get('status', 'Active'),
                id
            ))
            
            AuditLogger.log_change(conn, 'customers', id, 'UPDATE', session.get('user_id'),
                                  {'name': request.form['name'], 'status': request.form.get('status', 'Active'),
                                   'old_name': old_customer['name']})
            conn.commit()
            flash('Customer updated successfully!', 'success')
            return redirect(url_for('customer_routes.view_customer', id=id))
            
        except ValueError:
            conn.rollback()
            flash('Please enter valid numbers for payment terms and credit limit.', 'danger')
        except Exception as e:
            conn.rollback()
            flash('An error occurred while updating the customer. Please try again.', 'danger')
        finally:
            conn.close()
            return redirect(url_for('customer_routes.edit_customer', id=id))
    
    customer = conn.execute('SELECT * FROM customers WHERE id = ?', (id,)).fetchone()
    
    if not customer:
        conn.close()
        flash('Customer not found', 'danger')
        return redirect(url_for('customer_routes.list_customers'))
    
    contacts = conn.execute('''
        SELECT * FROM customer_contacts WHERE customer_id = ? ORDER BY is_primary DESC, contact_name
    ''', (id,)).fetchall()
    conn.close()
    
    return render_template('customers/edit.html', customer=customer, contacts=contacts)

# Customer Contacts Management
@customer_bp.route('/customers/<int:customer_id>/contacts')
@role_required('Admin', 'Planner')
def list_customer_contacts(customer_id):
    """Get contacts for a customer as JSON"""
    db = Database()
    conn = db.get_connection()
    
    customer = conn.execute('SELECT id FROM customers WHERE id = ?', (customer_id,)).fetchone()
    if not customer:
        conn.close()
        return jsonify({'error': 'Customer not found'}), 404
    
    contacts = conn.execute('''
        SELECT id, contact_name, title, email, phone, mobile, department, is_primary
        FROM customer_contacts WHERE customer_id = ? ORDER BY is_primary DESC, contact_name
    ''', (customer_id,)).fetchall()
    conn.close()
    return jsonify([dict(c) for c in contacts])

@customer_bp.route('/customers/<int:customer_id>/contacts/add', methods=['POST'])
@role_required('Admin', 'Planner')
def add_customer_contact(customer_id):
    """Add a new contact to a customer"""
    db = Database()
    conn = db.get_connection()
    
    customer = conn.execute('SELECT id FROM customers WHERE id = ?', (customer_id,)).fetchone()
    if not customer:
        conn.close()
        flash('Customer not found', 'danger')
        return redirect(url_for('customer_routes.list_customers'))
    
    is_primary = 1 if request.form.get('is_primary') else 0
    
    if is_primary:
        conn.execute('UPDATE customer_contacts SET is_primary = 0 WHERE customer_id = ?', (customer_id,))
    
    conn.execute('''
        INSERT INTO customer_contacts (customer_id, contact_name, title, email, phone, mobile, department, is_primary, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        customer_id,
        request.form['contact_name'],
        request.form.get('title', ''),
        request.form.get('email', ''),
        request.form.get('phone', ''),
        request.form.get('mobile', ''),
        request.form.get('department', ''),
        is_primary,
        request.form.get('notes', '')
    ))
    conn.commit()
    conn.close()
    flash('Contact added successfully', 'success')
    return redirect(url_for('customer_routes.edit_customer', id=customer_id))

@customer_bp.route('/customers/<int:customer_id>/contacts/<int:contact_id>/edit', methods=['POST'])
@role_required('Admin', 'Planner')
def edit_customer_contact(customer_id, contact_id):
    """Edit a customer contact"""
    db = Database()
    conn = db.get_connection()
    
    contact = conn.execute('SELECT id FROM customer_contacts WHERE id = ? AND customer_id = ?', (contact_id, customer_id)).fetchone()
    if not contact:
        conn.close()
        flash('Contact not found', 'danger')
        return redirect(url_for('customer_routes.list_customers'))
    
    is_primary = 1 if request.form.get('is_primary') else 0
    
    if is_primary:
        conn.execute('UPDATE customer_contacts SET is_primary = 0 WHERE customer_id = ?', (customer_id,))
    
    conn.execute('''
        UPDATE customer_contacts SET 
            contact_name = ?, title = ?, email = ?, phone = ?, mobile = ?, department = ?, is_primary = ?, notes = ?
        WHERE id = ? AND customer_id = ?
    ''', (
        request.form['contact_name'],
        request.form.get('title', ''),
        request.form.get('email', ''),
        request.form.get('phone', ''),
        request.form.get('mobile', ''),
        request.form.get('department', ''),
        is_primary,
        request.form.get('notes', ''),
        contact_id,
        customer_id
    ))
    conn.commit()
    conn.close()
    flash('Contact updated successfully', 'success')
    return redirect(url_for('customer_routes.edit_customer', id=customer_id))

@customer_bp.route('/customers/<int:customer_id>/contacts/<int:contact_id>/delete', methods=['POST'])
@role_required('Admin', 'Planner')
def delete_customer_contact(customer_id, contact_id):
    """Delete a customer contact"""
    db = Database()
    conn = db.get_connection()
    
    contact = conn.execute('SELECT id FROM customer_contacts WHERE id = ? AND customer_id = ?', (contact_id, customer_id)).fetchone()
    if not contact:
        conn.close()
        flash('Contact not found', 'danger')
        return redirect(url_for('customer_routes.list_customers'))
    
    conn.execute('DELETE FROM customer_contacts WHERE id = ? AND customer_id = ?', (contact_id, customer_id))
    conn.commit()
    conn.close()
    flash('Contact deleted', 'success')
    return redirect(url_for('customer_routes.edit_customer', id=customer_id))

@customer_bp.route('/customers/<int:customer_id>/contacts/<int:contact_id>/set-primary', methods=['POST'])
@role_required('Admin', 'Planner')
def set_customer_primary_contact(customer_id, contact_id):
    """Set a contact as the primary contact"""
    db = Database()
    conn = db.get_connection()
    
    contact = conn.execute('SELECT id FROM customer_contacts WHERE id = ? AND customer_id = ?', (contact_id, customer_id)).fetchone()
    if not contact:
        conn.close()
        flash('Contact not found', 'danger')
        return redirect(url_for('customer_routes.list_customers'))
    
    conn.execute('UPDATE customer_contacts SET is_primary = 0 WHERE customer_id = ?', (customer_id,))
    conn.execute('UPDATE customer_contacts SET is_primary = 1 WHERE id = ? AND customer_id = ?', (contact_id, customer_id))
    conn.commit()
    conn.close()
    flash('Primary contact updated', 'success')
    return redirect(url_for('customer_routes.edit_customer', id=customer_id))


@customer_bp.route('/customers/<int:customer_id>/portal/generate', methods=['POST'])
@role_required('Admin', 'Planner')
def generate_portal_link(customer_id):
    """Generate a new portal token for a customer"""
    db = Database()
    conn = db.get_connection()
    
    customer = conn.execute('SELECT id FROM customers WHERE id = ?', (customer_id,)).fetchone()
    if not customer:
        conn.close()
        flash('Customer not found', 'danger')
        return redirect(url_for('customer_routes.list_customers'))
    
    token = secrets.token_urlsafe(32)
    
    conn.execute('''
        UPDATE customers 
        SET portal_token = ?, portal_enabled = 1
        WHERE id = ?
    ''', (token, customer_id))
    conn.commit()
    conn.close()
    
    flash('Portal link generated successfully', 'success')
    return redirect(url_for('customer_routes.edit_customer', id=customer_id))


@customer_bp.route('/customers/<int:customer_id>/portal/toggle', methods=['POST'])
@role_required('Admin', 'Planner')
def toggle_portal(customer_id):
    """Enable or disable the customer portal"""
    db = Database()
    conn = db.get_connection()
    
    customer = conn.execute('SELECT id, portal_enabled FROM customers WHERE id = ?', (customer_id,)).fetchone()
    if not customer:
        conn.close()
        flash('Customer not found', 'danger')
        return redirect(url_for('customer_routes.list_customers'))
    
    new_status = 0 if customer['portal_enabled'] else 1
    
    conn.execute('''
        UPDATE customers 
        SET portal_enabled = ?
        WHERE id = ?
    ''', (new_status, customer_id))
    conn.commit()
    conn.close()
    
    status_text = 'enabled' if new_status else 'disabled'
    flash(f'Customer portal {status_text}', 'success')
    return redirect(url_for('customer_routes.edit_customer', id=customer_id))


@customer_bp.route('/customers/<int:customer_id>/portal/regenerate', methods=['POST'])
@role_required('Admin', 'Planner')
def regenerate_portal_link(customer_id):
    """Regenerate the portal token (invalidates old link)"""
    db = Database()
    conn = db.get_connection()
    
    customer = conn.execute('SELECT id FROM customers WHERE id = ?', (customer_id,)).fetchone()
    if not customer:
        conn.close()
        flash('Customer not found', 'danger')
        return redirect(url_for('customer_routes.list_customers'))
    
    token = secrets.token_urlsafe(32)
    
    conn.execute('''
        UPDATE customers 
        SET portal_token = ?
        WHERE id = ?
    ''', (token, customer_id))
    conn.commit()
    conn.close()
    
    flash('Portal link regenerated. Previous link is now invalid.', 'warning')
    return redirect(url_for('customer_routes.edit_customer', id=customer_id))


@customer_bp.route('/api/customers/quick-create', methods=['POST'])
@role_required('Admin', 'Planner')
def quick_create_customer():
    """API endpoint for quick customer creation from work order form"""
    data = request.get_json()
    
    if not data or not data.get('name'):
        return jsonify({'success': False, 'error': 'Customer name is required'}), 400
    
    db = Database()
    conn = db.get_connection()
    
    try:
        # Generate customer number
        last_customer = conn.execute(
            'SELECT customer_number FROM customers ORDER BY id DESC LIMIT 1'
        ).fetchone()
        
        if last_customer:
            # Handle different formats (CUS-00001 or CUST-000001)
            last_num_str = last_customer['customer_number']
            if '-' in last_num_str:
                last_num = int(last_num_str.split('-')[1])
            else:
                last_num = 0
            customer_number = f'CUS-{last_num + 1:05d}'
        else:
            customer_number = 'CUS-00001'
        
        # Insert customer with minimal required fields
        conn.execute('''
            INSERT INTO customers (
                customer_number, name, contact_person, email, phone,
                billing_address, shipping_address, payment_terms, credit_limit,
                tax_exempt, notes, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            customer_number,
            data['name'],
            data.get('contact_person', ''),
            data.get('email', ''),
            data.get('phone', ''),
            '',  # billing_address
            '',  # shipping_address
            30,  # default payment_terms
            0.0,  # default credit_limit
            0,  # not tax_exempt
            '',  # notes
            'Active'
        ))
        
        customer_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        AuditLogger.log_change(conn, 'customers', customer_id, 'CREATE', session.get('user_id'),
                              {'customer_number': customer_number, 'name': data['name'], 'source': 'quick-create'})
        conn.commit()
        
        return jsonify({
            'success': True,
            'customer': {
                'id': customer_id,
                'customer_number': customer_number,
                'name': data['name']
            }
        })
        
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        conn.close()
