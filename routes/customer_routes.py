from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database, AuditLogger
from auth import login_required, role_required
import csv
import io
import secrets
import os
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
            
            customer_name = request.form['name'].strip()
            
            existing = conn.execute('''
                SELECT id, customer_number, name FROM customers 
                WHERE LOWER(name) = LOWER(?)
            ''', (customer_name,)).fetchone()
            
            if existing:
                conn.close()
                flash(f'A customer with this name already exists: {existing["name"]} ({existing["customer_number"]})', 'danger')
                return redirect(url_for('customer_routes.create_customer'))
            
            # Insert customer
            conn.execute('''
                INSERT INTO customers (
                    customer_number, name, contact_person, email, phone,
                    billing_address, shipping_address, payment_terms, credit_limit,
                    tax_exempt, notes, status, website
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                customer_number,
                customer_name,
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
                request.form.get('website', '').strip()
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

@customer_bp.route('/customers/<int:id>/intel', methods=['POST'])
@login_required
def customer_intel(id):
    """AI-generated market intelligence for a customer"""
    db = Database()
    conn = db.get_connection()

    customer = conn.execute('SELECT * FROM customers WHERE id = ?', (id,)).fetchone()
    if not customer:
        conn.close()
        return jsonify({'success': False, 'error': 'Customer not found'}), 404

    top_products = conn.execute('''
        SELECT p.name, p.code, SUM(sol.quantity) as qty, SUM(sol.line_total) as revenue
        FROM sales_order_lines sol
        JOIN sales_orders so ON sol.so_id = so.id
        JOIN products p ON sol.product_id = p.id
        WHERE so.customer_id = ? AND so.status NOT IN ('Cancelled', 'Draft')
        GROUP BY p.id ORDER BY revenue DESC LIMIT 5
    ''', (id,)).fetchall()

    order_count = conn.execute(
        "SELECT COUNT(*) as c FROM sales_orders WHERE customer_id = ? AND status NOT IN ('Cancelled','Draft')", (id,)
    ).fetchone()['c']

    total_revenue = conn.execute(
        "SELECT COALESCE(SUM(total_amount),0) as t FROM sales_orders WHERE customer_id = ? AND status NOT IN ('Cancelled','Draft')", (id,)
    ).fetchone()['t']

    conn.close()

    api_key = os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY')
    if not api_key:
        return jsonify({'success': False, 'error': 'AI service not configured. Please set up the OpenAI integration.'}), 503

    try:
        from openai import OpenAI
        client = OpenAI(
            api_key=api_key,
            base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
        )

        top_prod_text = ', '.join([f"{r['name']} ({r['code']})" for r in top_products]) if top_products else 'No purchase history'
        cust_info = (
            f"Company Name: {customer['name']}\n"
            f"Customer Number: {customer['customer_number']}\n"
            f"Billing Address: {customer['billing_address'] or 'Not specified'}\n"
            f"Status: {customer['status']}\n"
            f"Payment Terms: Net {customer['payment_terms']} days\n"
            f"Credit Limit: ${float(customer['credit_limit'] or 0):,.0f}\n"
            f"Total Orders with Us: {order_count}\n"
            f"Total Revenue with Us: ${float(total_revenue or 0):,.0f}\n"
            f"Top Products Purchased: {top_prod_text}\n"
            f"Notes: {customer['notes'] or 'None'}"
        )

        prompt = f"""You are an expert business intelligence analyst. Based on the customer information below, generate a comprehensive market intelligence report.

Customer Information:
{cust_info}

Generate a structured intelligence report covering the following sections. Write in clear, professional prose — no bullet symbols, no asterisks, no markdown formatting. Use plain numbered sections only.

1. Company Overview
Brief background on the company, their likely industry sector, size, and what they do based on their name, address, and purchasing history.

2. Market Position and Industry Context
Their likely position in their industry, competitive landscape they operate in, and key market dynamics affecting them.

3. Business Relationship Analysis
Analysis of their purchasing patterns with us, payment behavior, and what their buying history tells us about their operations and priorities.

4. Market Trends and Opportunities
Current industry trends that may affect this customer, growth opportunities we could help them with, and potential expansion of the relationship.

5. Risk Factors
Any risks in the customer relationship — payment terms, credit exposure, market risks their industry faces, or concentration risk.

6. Strategic Recommendations
Specific actionable recommendations for how to grow, protect, or improve this customer relationship.

Note: Base your analysis on the company name, location, and purchasing history provided. Clearly state where you are making reasonable inferences versus stating confirmed facts. Do not fabricate specific financial figures for the customer's own business."""

        response = client.chat.completions.create(
            model='gpt-4o',
            messages=[
                {'role': 'system', 'content': 'You are an expert business intelligence and market research analyst. Generate insightful, professional reports based on available customer data. Never use markdown symbols like asterisks or hashes in your output.'},
                {'role': 'user', 'content': prompt}
            ],
            temperature=0.6,
            max_tokens=1800
        )

        report_text = response.choices[0].message.content.strip()

        return jsonify({
            'success': True,
            'report': report_text,
            'customer_name': customer['name'],
            'generated_at': datetime.now().strftime('%B %d, %Y at %I:%M %p')
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


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
                    credit_limit = ?, tax_exempt = ?, notes = ?, status = ?,
                    website = ?
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
                request.form.get('website', '').strip(),
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
        
        customer_name = data['name'].strip()
        
        existing = conn.execute('''
            SELECT id, customer_number, name FROM customers 
            WHERE LOWER(name) = LOWER(?)
        ''', (customer_name,)).fetchone()
        
        if existing:
            conn.close()
            return jsonify({
                'success': False, 
                'error': f'A customer with this name already exists: {existing["name"]} ({existing["customer_number"]})'
            }), 400
        
        # Insert customer with minimal required fields
        conn.execute('''
            INSERT INTO customers (
                customer_number, name, contact_person, email, phone,
                billing_address, shipping_address, payment_terms, credit_limit,
                tax_exempt, notes, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            customer_number,
            customer_name,
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

@customer_bp.route('/customers/import-template')
@login_required
def import_template():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['name', 'contact_person', 'email', 'phone', 'billing_address',
                     'shipping_address', 'payment_terms', 'credit_limit', 'tax_exempt',
                     'tax_id', 'currency', 'notes', 'status'])
    writer.writerow(['Acme Corp', 'John Smith', 'john@acme.com', '555-0100',
                     '123 Main St', '123 Main St', '30', '50000', 'No',
                     '', 'USD', '', 'Active'])
    from flask import Response
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=customer_import_template.csv'}
    )

@customer_bp.route('/customers/import', methods=['POST'])
@role_required('Admin', 'Planner')
def import_customers():
    file = request.files.get('csv_file')
    if not file or not file.filename.endswith('.csv'):
        flash('Please upload a valid CSV file', 'danger')
        return redirect(url_for('customer_routes.list_customers'))

    skip_duplicates = 'skip_duplicates' in request.form

    try:
        stream = io.StringIO(file.stream.read().decode('utf-8-sig'))
        reader = csv.DictReader(stream)

        fieldnames = [f.strip().lower() for f in (reader.fieldnames or [])]
        if 'name' not in fieldnames:
            flash('CSV must have a "name" column', 'danger')
            return redirect(url_for('customer_routes.list_customers'))

        db = Database()
        conn = db.get_connection()

        last_customer = conn.execute(
            'SELECT customer_number FROM customers ORDER BY id DESC LIMIT 1'
        ).fetchone()
        if last_customer:
            next_num = int(last_customer['customer_number'].split('-')[1]) + 1
        else:
            next_num = 1

        imported = 0
        skipped = 0
        errors = []

        for row_num, raw_row in enumerate(reader, start=2):
            row = {k.strip().lower(): (v.strip() if v else '') for k, v in raw_row.items()}
            name = row.get('name', '').strip()
            if not name:
                errors.append(f'Row {row_num}: Missing name')
                continue

            if skip_duplicates:
                existing = conn.execute(
                    'SELECT id FROM customers WHERE LOWER(name) = LOWER(?)', (name,)
                ).fetchone()
                if existing:
                    skipped += 1
                    continue

            customer_number = f'CUST-{next_num:06d}'
            next_num += 1

            payment_terms = row.get('payment_terms', '30').strip()
            try:
                payment_terms = int(payment_terms) if payment_terms else 30
            except ValueError:
                payment_terms = 30

            credit_limit = row.get('credit_limit', '0').strip()
            try:
                credit_limit = float(credit_limit) if credit_limit else 0.0
            except ValueError:
                credit_limit = 0.0

            tax_exempt_val = row.get('tax_exempt', '').lower()
            tax_exempt = 1 if tax_exempt_val in ('yes', 'true', '1', 'y') else 0

            status = row.get('status', 'Active').strip()
            if status not in ('Active', 'Inactive'):
                status = 'Active'

            try:
                conn.execute('''
                    INSERT INTO customers 
                    (customer_number, name, contact_person, email, phone,
                     billing_address, shipping_address, payment_terms, credit_limit,
                     tax_exempt, tax_id, currency, notes, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    customer_number,
                    name,
                    row.get('contact_person', ''),
                    row.get('email', ''),
                    row.get('phone', ''),
                    row.get('billing_address', ''),
                    row.get('shipping_address', ''),
                    payment_terms,
                    credit_limit,
                    tax_exempt,
                    row.get('tax_id', ''),
                    row.get('currency', 'USD') or 'USD',
                    row.get('notes', ''),
                    status
                ))
                imported += 1
            except Exception as e:
                errors.append(f'Row {row_num} ({name}): {str(e)}')

        conn.commit()
        conn.close()

        msg = f'Import complete: {imported} customers imported'
        if skipped:
            msg += f', {skipped} duplicates skipped'
        if errors:
            msg += f', {len(errors)} errors'
            for err in errors[:5]:
                flash(err, 'warning')

        flash(msg, 'success' if not errors else 'warning')

    except Exception as e:
        flash(f'Import failed: {str(e)}', 'danger')

    return redirect(url_for('customer_routes.list_customers'))
