from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import Database
from auth import login_required, role_required
import csv
import io
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
    
    conn.close()
    return render_template('customers/view.html', customer=customer, sales_history=sales_history)

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
    conn.close()
    
    if not customer:
        flash('Customer not found', 'danger')
        return redirect(url_for('customer_routes.list_customers'))
    
    return render_template('customers/edit.html', customer=customer)
