from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import Database, GLAutoPost
from auth import login_required, role_required
from datetime import datetime

issuance_bp = Blueprint('issuance_routes', __name__)

def update_material_requirement_status(conn, work_order_id, product_id):
    """Update the status of a material requirement based on issued quantities."""
    # Check if material requirement exists for this product
    requirement = conn.execute('''
        SELECT id, required_quantity FROM material_requirements
        WHERE work_order_id = ? AND product_id = ?
    ''', (work_order_id, product_id)).fetchone()
    
    if requirement:
        # Get current inventory quantity
        inventory = conn.execute('''
            SELECT quantity FROM inventory WHERE product_id = ?
        ''', (product_id,)).fetchone()
        
        available_quantity = inventory['quantity'] if inventory else 0
        
        # Get total issued quantity for this work order and product
        issued = conn.execute('''
            SELECT COALESCE(SUM(quantity_issued), 0) as total_issued
            FROM material_issues
            WHERE work_order_id = ? AND product_id = ?
        ''', (work_order_id, product_id)).fetchone()
        
        total_issued = issued['total_issued'] if issued else 0
        
        # Status based on whether materials have been issued, not inventory availability
        shortage_quantity = max(0, requirement['required_quantity'] - total_issued)
        status = 'Satisfied' if shortage_quantity == 0 else 'Shortage'
        
        # Update material requirement
        conn.execute('''
            UPDATE material_requirements
            SET available_quantity = ?,
                shortage_quantity = ?,
                status = ?
            WHERE id = ?
        ''', (available_quantity, shortage_quantity, status, requirement['id']))

@issuance_bp.route('/issuance')
@login_required
def list_issues():
    db = Database()
    conn = db.get_connection()
    
    # Get available materials ready to be issued
    available_materials = conn.execute('''
        SELECT 
            i.id,
            p.id as product_id,
            p.code as product_code,
            p.name as product_name,
            p.unit_of_measure,
            i.quantity,
            COALESCE(i.reserved_quantity, 0) as reserved_quantity,
            (i.quantity - COALESCE(i.reserved_quantity, 0)) as available_quantity,
            i.warehouse_location,
            i.bin_location,
            i.condition,
            i.status
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.quantity > 0 
          AND (i.quantity - COALESCE(i.reserved_quantity, 0)) > 0
        ORDER BY p.code
    ''').fetchall()
    
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
    return render_template('issuance/list.html', issues=issues, available_materials=available_materials)

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
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO material_issues 
                (issue_number, work_order_id, product_id, quantity_issued, issue_date, 
                 warehouse_location, bin_location, issued_to, task_reference, 
                 unit_cost, total_cost, remarks, issued_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (issue_number, wo_id, product_id, quantity_issued, issue_date,
                  warehouse, bin_location, issued_to, task_ref, unit_cost, total_cost, remarks, session['user_id']))
            
            issue_id = cursor.lastrowid
            
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
            
            # Update material requirement status
            update_material_requirement_status(conn, wo_id, product_id)
            
            # Auto-post GL entry for material issuance
            # Debit: WIP - Work in Process (increase WIP asset)
            # Credit: Inventory (decrease inventory asset)
            product_info = conn.execute('''
                SELECT name FROM products WHERE id = ?
            ''', (product_id,)).fetchone()
            
            gl_lines = [
                {
                    'account_code': '1140',  # WIP - Work in Process
                    'debit': total_cost,
                    'credit': 0,
                    'description': f'Material issued to WO {wo["wo_number"]} - {product_info["name"]} ({issue_number})'
                },
                {
                    'account_code': '1130',  # Inventory
                    'debit': 0,
                    'credit': total_cost,
                    'description': f'Material issued from inventory - {product_info["name"]} ({issue_number})'
                }
            ]
            
            GLAutoPost.create_auto_journal_entry(
                conn=conn,
                entry_date=issue_date,
                description=f'Material Issuance - {issue_number}',
                transaction_source='Material Issuance',
                reference_type='material_issue',
                reference_id=issue_id,
                lines=gl_lines,
                created_by=session['user_id']
            )
            
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

@issuance_bp.route('/issuance/batch-issue', methods=['POST'])
@role_required('Admin', 'Production Staff', 'Planner')
def batch_issue():
    db = Database()
    conn = db.get_connection()
    
    try:
        wo_id = int(request.form['work_order_id'])
        issue_date = request.form['issue_date']
        issued_to = request.form.get('issued_to', '')
        task_ref = request.form.get('task_reference', '')
        remarks = request.form.get('remarks', '')
        
        # Get all materials data from form
        materials_data = []
        index = 0
        while f'materials[{index}][product_id]' in request.form:
            product_id = int(request.form[f'materials[{index}][product_id]'])
            quantity = float(request.form[f'materials[{index}][quantity]'])
            materials_data.append({'product_id': product_id, 'quantity': quantity})
            index += 1
        
        if not materials_data:
            flash('No materials selected for issuance.', 'danger')
            conn.close()
            return redirect(url_for('issuance_routes.list_issues'))
        
        # Validate work order
        wo = conn.execute('SELECT * FROM work_orders WHERE id = ? AND status != "Completed"', (wo_id,)).fetchone()
        if not wo:
            flash('Work Order not found or already completed.', 'danger')
            conn.close()
            return redirect(url_for('issuance_routes.list_issues'))
        
        issued_count = 0
        errors = []
        
        for idx, material in enumerate(materials_data):
            product_id = None
            product_code = None
            savepoint_name = f'sp_material_{idx}'
            
            try:
                product_id = material['product_id']
                quantity_issued = material['quantity']
                
                # Check inventory availability before savepoint
                inventory = conn.execute('SELECT * FROM inventory WHERE product_id = ?', (product_id,)).fetchone()
                
                if not inventory:
                    errors.append(f'Product ID {product_id} not found in inventory')
                    continue
                
                available = inventory['quantity'] - (inventory['reserved_quantity'] or 0)
                
                if quantity_issued > available:
                    product_info = conn.execute('SELECT code FROM products WHERE id = ?', (product_id,)).fetchone()
                    product_code = product_info['code'] if product_info else f'ID {product_id}'
                    errors.append(f'{product_code}: Insufficient inventory (available: {available})')
                    continue
                
                # Get product cost
                product = conn.execute('SELECT cost, code FROM products WHERE id = ?', (product_id,)).fetchone()
                unit_cost = product['cost'] if product else 0
                total_cost = unit_cost * quantity_issued
                product_code = product['code'] if product else f'ID {product_id}'
                
                # Create savepoint for this material
                conn.execute(f'SAVEPOINT {savepoint_name}')
                
                try:
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
                         warehouse_location, issued_to, task_reference, remarks, unit_cost, total_cost, issued_by)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (issue_number, wo_id, product_id, quantity_issued, issue_date,
                          inventory['warehouse_location'], issued_to, task_ref, remarks, 
                          unit_cost, total_cost, session['user_id']))
                    
                    # Update inventory
                    new_quantity = inventory['quantity'] - quantity_issued
                    conn.execute('''
                        UPDATE inventory 
                        SET quantity = ?,
                            status = CASE WHEN ? <= 0 THEN 'Out of Stock' ELSE status END,
                            last_updated = CURRENT_TIMESTAMP
                        WHERE product_id = ?
                    ''', (new_quantity, new_quantity, product_id))
                    
                    # Update work order material cost
                    conn.execute('''
                        UPDATE work_orders 
                        SET material_cost = material_cost + ?
                        WHERE id = ?
                    ''', (total_cost, wo_id))
                    
                    # Update material requirement status if exists
                    update_material_requirement_status(conn, wo_id, product_id)
                    
                    # Release savepoint on success
                    conn.execute(f'RELEASE SAVEPOINT {savepoint_name}')
                    issued_count += 1
                    
                except Exception as inner_e:
                    # Rollback this specific material's changes
                    conn.execute(f'ROLLBACK TO SAVEPOINT {savepoint_name}')
                    conn.execute(f'RELEASE SAVEPOINT {savepoint_name}')
                    errors.append(f'{product_code}: {str(inner_e)}')
                
            except Exception as e:
                # Outer error (validation, etc.) - no savepoint to rollback
                product_ref = product_code if 'product_code' in locals() else (product_id if product_id else 'Unknown')
                errors.append(f'Error processing {product_ref}: {str(e)}')
        
        # Commit all successful transactions
        try:
            conn.commit()
        except Exception as commit_error:
            conn.rollback()
            flash(f'Error committing changes: {str(commit_error)}', 'danger')
            conn.close()
            return redirect(url_for('issuance_routes.list_issues'))
        
        # Provide feedback to user
        if issued_count > 0 and len(errors) == 0:
            flash(f'Successfully issued {issued_count} material(s) to work order!', 'success')
        elif issued_count > 0 and len(errors) > 0:
            flash(f'Partially completed: {issued_count} material(s) issued, {len(errors)} failed.', 'warning')
        elif issued_count == 0:
            flash('No materials were issued. Check errors below.', 'danger')
        
        # Show errors
        if errors:
            for error in errors[:5]:  # Show first 5 errors
                flash(error, 'warning')
            if len(errors) > 5:
                flash(f'...and {len(errors) - 5} more errors', 'warning')
        
    except Exception as e:
        conn.rollback()
        flash(f'Critical error during batch issuance: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('issuance_routes.list_issues'))
