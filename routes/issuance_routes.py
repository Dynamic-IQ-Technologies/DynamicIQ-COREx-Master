from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import Database
from auth import login_required, role_required
from datetime import datetime

issuance_bp = Blueprint('issuance_routes', __name__)

@issuance_bp.route('/issuance')
@login_required
def list_issues():
    db = Database()
    conn = db.get_connection()
    
    issues = conn.execute('''
        SELECT 
            mi.*,
            wo.wo_number,
            p.code as product_code,
            p.name as product_name,
            p.unit_of_measure,
            u.username as issued_by_name
        FROM material_issues mi
        JOIN work_orders wo ON mi.work_order_id = wo.id
        JOIN products p ON mi.product_id = p.id
        LEFT JOIN users u ON mi.issued_by = u.id
        ORDER BY mi.issue_date DESC, mi.created_at DESC
    ''').fetchall()
    
    conn.close()
    return render_template('issuance/list.html', issues=issues)

@issuance_bp.route('/issuance/create', methods=['GET', 'POST'])
@role_required('Admin', 'Production Staff', 'Planner')
def create_issue():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            wo_id = int(request.form['work_order_id'])
            product_id = int(request.form['product_id'])
            quantity_issued = float(request.form['quantity_issued'])
            issue_date = request.form['issue_date']
            warehouse = request.form.get('warehouse_location', 'Main')
            bin_location = request.form.get('bin_location', '')
            issued_to = request.form.get('issued_to', '')
            task_ref = request.form.get('task_reference', '')
            remarks = request.form.get('remarks', '')
            
            # Validate work order exists and is active
            wo = conn.execute('''
                SELECT * FROM work_orders WHERE id = ? AND status != 'Completed'
            ''', (wo_id,)).fetchone()
            
            if not wo:
                flash('Work Order not found or already completed.', 'danger')
                conn.close()
                return redirect(url_for('issuance_routes.create_issue'))
            
            # Check inventory availability
            inventory = conn.execute('''
                SELECT * FROM inventory WHERE product_id = ?
            ''', (product_id,)).fetchone()
            
            if not inventory:
                flash('Product not found in inventory.', 'danger')
                conn.close()
                return redirect(url_for('issuance_routes.create_issue'))
            
            available = inventory['quantity'] - (inventory['reserved_quantity'] or 0)
            
            if quantity_issued > available:
                flash(f'Insufficient inventory. Available: {available} {inventory["unit_of_measure"]}', 'danger')
                conn.close()
                return redirect(url_for('issuance_routes.create_issue'))
            
            # Get product cost
            product = conn.execute('SELECT cost FROM products WHERE id = ?', (product_id,)).fetchone()
            unit_cost = product['cost'] if product else 0
            total_cost = unit_cost * quantity_issued
            
            # Generate issue number
            last_issue = conn.execute('''
                SELECT issue_number FROM material_issues 
                WHERE issue_number LIKE 'ISS-%'
                ORDER BY CAST(SUBSTR(issue_number, 5) AS INTEGER) DESC 
                LIMIT 1
            ''').fetchone()
            
            if last_issue:
                try:
                    last_number = int(last_issue['issue_number'].split('-')[1])
                    next_number = last_number + 1
                except (ValueError, IndexError):
                    next_number = 1
            else:
                next_number = 1
            
            issue_number = f'ISS-{next_number:06d}'
            
            # Create material issue
            conn.execute('''
                INSERT INTO material_issues 
                (issue_number, work_order_id, product_id, quantity_issued, issue_date, 
                 warehouse_location, bin_location, issued_to, task_reference, 
                 unit_cost, total_cost, remarks, issued_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (issue_number, wo_id, product_id, quantity_issued, issue_date,
                  warehouse, bin_location, issued_to, task_ref, unit_cost, total_cost, remarks, session['user_id']))
            
            # Update inventory - deduct quantity
            new_qty = inventory['quantity'] - quantity_issued
            conn.execute('''
                UPDATE inventory 
                SET quantity = ?,
                    last_updated = CURRENT_TIMESTAMP,
                    status = CASE WHEN ? <= 0 THEN 'Out of Stock' ELSE status END
                WHERE product_id = ?
            ''', (new_qty, new_qty, product_id))
            
            # Update work order material cost
            current_cost = conn.execute(
                'SELECT material_cost FROM work_orders WHERE id = ?', (wo_id,)
            ).fetchone()
            new_material_cost = (current_cost['material_cost'] or 0) + total_cost
            
            conn.execute('''
                UPDATE work_orders 
                SET material_cost = ?
                WHERE id = ?
            ''', (new_material_cost, wo_id))
            
            conn.commit()
            flash(f'Material issued successfully! Issue Number: {issue_number}', 'success')
            return redirect(url_for('issuance_routes.view_issue', issue_number=issue_number))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error issuing material: {str(e)}', 'danger')
        finally:
            conn.close()
    
    # GET request - show form
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Get active work orders
    work_orders = conn.execute('''
        SELECT 
            wo.*,
            p.code as product_code,
            p.name as product_name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.status != 'Completed'
        ORDER BY wo.planned_start_date DESC, wo.created_at DESC
    ''').fetchall()
    
    # Get available inventory
    inventory_items = conn.execute('''
        SELECT 
            i.*,
            p.code as product_code,
            p.name as product_name,
            p.unit_of_measure,
            p.cost,
            (i.quantity - COALESCE(i.reserved_quantity, 0)) as available_quantity
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE (i.quantity - COALESCE(i.reserved_quantity, 0)) > 0
        ORDER BY p.code
    ''').fetchall()
    
    conn.close()
    return render_template('issuance/create.html', 
                         work_orders=work_orders, 
                         inventory_items=inventory_items,
                         today=today)

@issuance_bp.route('/issuance/<issue_number>')
@login_required
def view_issue(issue_number):
    db = Database()
    conn = db.get_connection()
    
    issue = conn.execute('''
        SELECT 
            mi.*,
            wo.wo_number,
            wo.status as wo_status,
            p.code as product_code,
            p.name as product_name,
            p.unit_of_measure,
            u.username as issued_by_name
        FROM material_issues mi
        JOIN work_orders wo ON mi.work_order_id = wo.id
        JOIN products p ON mi.product_id = p.id
        LEFT JOIN users u ON mi.issued_by = u.id
        WHERE mi.issue_number = ?
    ''', (issue_number,)).fetchone()
    
    if not issue:
        flash('Issue record not found.', 'danger')
        conn.close()
        return redirect(url_for('issuance_routes.list_issues'))
    
    conn.close()
    return render_template('issuance/view.html', issue=issue)
