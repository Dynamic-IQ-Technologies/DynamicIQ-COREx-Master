from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database, GLAutoPost
from auth import login_required, role_required
from datetime import datetime
import json

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
    
    # Get available materials ready to be issued with associated work orders
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
            i.status,
            (SELECT GROUP_CONCAT(DISTINCT wo.wo_number || ':' || wo.id) 
             FROM material_requirements mr 
             JOIN work_orders wo ON mr.work_order_id = wo.id 
             WHERE mr.product_id = p.id AND wo.status NOT IN ('Completed', 'Cancelled')
            ) as associated_work_orders
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
            wo.id as work_order_id,
            p.code as product_code,
            p.name as product_name,
            p.unit_of_measure,
            u.username as issued_by_name
        FROM material_issues mi
        JOIN work_orders wo ON mi.work_order_id = wo.id
        JOIN products p ON mi.product_id = p.id
        LEFT JOIN users u ON mi.issued_by = u.id
        ORDER BY wo.wo_number DESC, mi.issue_date DESC, mi.created_at DESC
    ''').fetchall()
    
    # Group issues by work order
    grouped_issues = {}
    wo_totals = {}
    for issue in issues:
        wo_num = issue['wo_number']
        wo_id = issue['work_order_id']
        if wo_num not in grouped_issues:
            grouped_issues[wo_num] = {'wo_id': wo_id, 'issues': [], 'total_cost': 0, 'item_count': 0}
        grouped_issues[wo_num]['issues'].append(issue)
        grouped_issues[wo_num]['total_cost'] += issue['total_cost'] or 0
        grouped_issues[wo_num]['item_count'] += 1
    
    conn.close()
    return render_template('issuance/list.html', issues=issues, available_materials=available_materials, grouped_issues=grouped_issues)

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
            
            # ── Compliance block check ────────────────────────────────────────
            try:
                from routes.inv_compliance_routes import check_inventory_compliance_block
                blocked, block_reason = check_inventory_compliance_block(conn, inventory['id'])
                if blocked:
                    flash(
                        f'Issuance blocked — Compliance hold: {block_reason}. '
                        f'A QMS Manager must log an override before this part can be issued.',
                        'danger'
                    )
                    conn.close()
                    return redirect(url_for('issuance_routes.create_issue'))
            except Exception:
                pass  # Compliance tables not yet ready; allow issuance

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


# ===== DYNAMIC MATERIAL ISSUE MODULE =====

@issuance_bp.route('/issuance/dynamic/<int:wo_id>')
@login_required
@role_required('Admin', 'Production Staff', 'Planner')
def dynamic_material_issue(wo_id):
    """Dynamic Material Issue page for multi-material issuance"""
    db = Database()
    conn = db.get_connection()
    
    wo = conn.execute('''
        SELECT 
            wo.*,
            p.code as product_code,
            p.name as product_name,
            p.unit_of_measure,
            c.name as customer_name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN customers c ON wo.customer_id = c.id
        WHERE wo.id = ?
    ''', (wo_id,)).fetchone()
    
    if not wo:
        flash('Work Order not found.', 'danger')
        conn.close()
        return redirect(url_for('issuance_routes.list_issues'))
    
    if wo['status'] in ('Completed', 'Cancelled'):
        flash('Cannot issue materials to a completed or cancelled work order.', 'warning')
        conn.close()
        return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    user_permissions = {
        'can_override_shortage': session.get('role') in ['Admin', 'Planner'],
        'can_add_non_bom': session.get('role') in ['Admin', 'Planner', 'Production Staff']
    }
    
    conn.close()
    return render_template('issuance/dynamic_issue.html', 
                          wo=wo, 
                          today=today,
                          user_permissions=user_permissions)


@issuance_bp.route('/api/issuance/wo-context/<int:wo_id>')
@login_required
def api_get_wo_context(wo_id):
    """Get work order context for dynamic issue page"""
    db = Database()
    conn = db.get_connection()
    
    try:
        wo = conn.execute('''
            SELECT 
                wo.id,
                wo.wo_number,
                wo.status,
                wo.quantity,
                wo.quantity_completed,
                wo.planned_start_date,
                wo.planned_end_date,
                wo.warehouse_location,
                wo.material_cost,
                p.code as product_code,
                p.name as product_name,
                p.unit_of_measure,
                c.name as customer_name
            FROM work_orders wo
            JOIN products p ON wo.product_id = p.id
            LEFT JOIN customers c ON wo.customer_id = c.id
            WHERE wo.id = ?
        ''', (wo_id,)).fetchone()
        
        if not wo:
            return jsonify({'success': False, 'error': 'Work Order not found'}), 404
        
        return jsonify({
            'success': True,
            'work_order': dict(wo)
        })
    finally:
        conn.close()


@issuance_bp.route('/api/issuance/bom-materials/<int:wo_id>')
@login_required
def api_get_bom_materials(wo_id):
    """Get BOM materials with availability for auto-population"""
    db = Database()
    conn = db.get_connection()
    
    try:
        wo = conn.execute('SELECT product_id, quantity FROM work_orders WHERE id = ?', (wo_id,)).fetchone()
        if not wo:
            return jsonify({'success': False, 'error': 'Work Order not found'}), 404
        
        wo_qty = wo['quantity'] or 1
        
        bom_materials = conn.execute('''
            SELECT 
                bc.id as bom_component_id,
                bc.component_id as product_id,
                p.code as product_code,
                p.name as product_name,
                p.unit_of_measure,
                p.cost as unit_cost,
                bc.quantity as bom_qty,
                (bc.quantity * ?) as required_qty,
                COALESCE(i.quantity, 0) as on_hand_qty,
                COALESCE(i.reserved_quantity, 0) as allocated_qty,
                (COALESCE(i.quantity, 0) - COALESCE(i.reserved_quantity, 0)) as available_qty,
                i.warehouse_location,
                i.bin_location,
                COALESCE(mr.issued_quantity, 0) as already_issued,
                COALESCE(mr.required_quantity, bc.quantity * ?) as requirement_qty,
                mr.allocation_status
            FROM bom_components bc
            JOIN products p ON bc.component_id = p.id
            LEFT JOIN inventory i ON bc.component_id = i.product_id
            LEFT JOIN material_requirements mr ON mr.work_order_id = ? AND mr.product_id = bc.component_id
            WHERE bc.product_id = ?
            ORDER BY p.code
        ''', (wo_qty, wo_qty, wo_id, wo['product_id'])).fetchall()
        
        materials = []
        for m in bom_materials:
            mat = dict(m)
            remaining_to_issue = max(0, mat['requirement_qty'] - mat['already_issued'])
            mat['remaining_to_issue'] = remaining_to_issue
            
            if mat['available_qty'] >= remaining_to_issue:
                mat['availability_status'] = 'sufficient'
            elif mat['available_qty'] > 0:
                mat['availability_status'] = 'partial'
            else:
                mat['availability_status'] = 'insufficient'
            
            materials.append(mat)
        
        return jsonify({
            'success': True,
            'materials': materials,
            'wo_quantity': wo_qty
        })
    finally:
        conn.close()


@issuance_bp.route('/api/issuance/search-products')
@login_required
def api_search_products():
    """Search products for manual addition with inventory availability"""
    db = Database()
    conn = db.get_connection()
    
    try:
        query = request.args.get('q', '').strip()
        if len(query) < 2:
            return jsonify({'success': True, 'products': []})
        
        products = conn.execute('''
            SELECT 
                p.id as product_id,
                p.code as product_code,
                p.name as product_name,
                p.unit_of_measure,
                p.cost as unit_cost,
                COALESCE(i.quantity, 0) as on_hand_qty,
                COALESCE(i.reserved_quantity, 0) as allocated_qty,
                (COALESCE(i.quantity, 0) - COALESCE(i.reserved_quantity, 0)) as available_qty,
                i.warehouse_location,
                i.bin_location
            FROM products p
            LEFT JOIN inventory i ON p.id = i.product_id
            WHERE (p.code LIKE ? OR p.name LIKE ?)
              AND p.product_type IN ('Component', 'Raw Material', 'Consumable')
            ORDER BY p.code
            LIMIT 20
        ''', (f'%{query}%', f'%{query}%')).fetchall()
        
        return jsonify({
            'success': True,
            'products': [dict(p) for p in products]
        })
    finally:
        conn.close()


@issuance_bp.route('/api/issuance/validate-inventory', methods=['POST'])
@login_required
def api_validate_inventory():
    """Real-time inventory validation for issue quantities"""
    db = Database()
    conn = db.get_connection()
    
    try:
        data = request.get_json()
        product_id = data.get('product_id')
        issue_qty = float(data.get('issue_qty', 0))
        wo_id = data.get('work_order_id')
        
        inventory = conn.execute('''
            SELECT 
                i.quantity as on_hand,
                COALESCE(i.reserved_quantity, 0) as allocated,
                (i.quantity - COALESCE(i.reserved_quantity, 0)) as available,
                i.warehouse_location,
                i.bin_location,
                p.code as product_code,
                p.name as product_name,
                p.cost as unit_cost,
                p.unit_of_measure
            FROM inventory i
            JOIN products p ON i.product_id = p.id
            WHERE i.product_id = ?
        ''', (product_id,)).fetchone()
        
        if not inventory:
            return jsonify({
                'success': True,
                'valid': False,
                'status': 'not_found',
                'message': 'Product not found in inventory',
                'available_qty': 0
            })
        
        available = inventory['available']
        
        if issue_qty <= 0:
            status = 'invalid'
            valid = False
            message = 'Issue quantity must be greater than zero'
        elif issue_qty > available:
            status = 'insufficient'
            valid = False
            message = f'Insufficient inventory. Available: {available}'
        elif issue_qty > available * 0.8:
            status = 'warning'
            valid = True
            message = f'This will consume {(issue_qty/available*100):.0f}% of available stock'
        else:
            status = 'valid'
            valid = True
            message = 'Quantity available'
        
        return jsonify({
            'success': True,
            'valid': valid,
            'status': status,
            'message': message,
            'available_qty': available,
            'on_hand_qty': inventory['on_hand'],
            'allocated_qty': inventory['allocated'],
            'unit_cost': inventory['unit_cost'] or 0,
            'total_cost': (inventory['unit_cost'] or 0) * issue_qty,
            'warehouse_location': inventory['warehouse_location'],
            'bin_location': inventory['bin_location']
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        conn.close()


@issuance_bp.route('/api/issuance/execute-multi-issue', methods=['POST'])
@login_required
@role_required('Admin', 'Production Staff', 'Planner')
def api_execute_multi_issue():
    """Execute atomic multi-material issuance transaction"""
    db = Database()
    conn = db.get_connection()
    
    try:
        data = request.get_json()
        wo_id = data.get('work_order_id')
        issue_date = data.get('issue_date', datetime.now().strftime('%Y-%m-%d'))
        issued_to = data.get('issued_to', '')
        task_reference = data.get('task_reference', '')
        remarks = data.get('remarks', '')
        materials = data.get('materials', [])
        override_shortages = data.get('override_shortages', False)
        
        if not materials:
            return jsonify({
                'success': False,
                'error': 'No materials to issue'
            }), 400
        
        wo = conn.execute('''
            SELECT wo.*, p.code as product_code 
            FROM work_orders wo 
            JOIN products p ON wo.product_id = p.id
            WHERE wo.id = ? AND wo.status NOT IN ('Completed', 'Cancelled')
        ''', (wo_id,)).fetchone()
        
        if not wo:
            return jsonify({
                'success': False,
                'error': 'Work Order not found or not active'
            }), 400
        
        validation_errors = []
        validated_materials = []
        
        for idx, mat in enumerate(materials):
            product_id = mat.get('product_id')
            issue_qty = float(mat.get('issue_qty', 0))
            
            if issue_qty <= 0:
                validation_errors.append({
                    'row': idx,
                    'product_id': product_id,
                    'field': 'issue_qty',
                    'error': 'Issue quantity must be greater than zero'
                })
                continue
            
            inventory = conn.execute('''
                SELECT 
                    i.*,
                    p.code as product_code,
                    p.name as product_name,
                    p.cost,
                    p.unit_of_measure,
                    (i.quantity - COALESCE(i.reserved_quantity, 0)) as available
                FROM inventory i
                JOIN products p ON i.product_id = p.id
                WHERE i.product_id = ?
            ''', (product_id,)).fetchone()
            
            if not inventory:
                validation_errors.append({
                    'row': idx,
                    'product_id': product_id,
                    'field': 'product_id',
                    'error': 'Product not found in inventory'
                })
                continue
            
            if issue_qty > inventory['available']:
                if not override_shortages:
                    validation_errors.append({
                        'row': idx,
                        'product_id': product_id,
                        'product_code': inventory['product_code'],
                        'field': 'issue_qty',
                        'error': f'Insufficient inventory. Available: {inventory["available"]}',
                        'available': inventory['available'],
                        'requested': issue_qty
                    })
                    continue
            
            validated_materials.append({
                'product_id': product_id,
                'product_code': inventory['product_code'],
                'product_name': inventory['product_name'],
                'issue_qty': issue_qty,
                'unit_cost': inventory['cost'] or 0,
                'total_cost': (inventory['cost'] or 0) * issue_qty,
                'unit_of_measure': inventory['unit_of_measure'],
                'warehouse_location': mat.get('warehouse_location') or inventory['warehouse_location'],
                'bin_location': mat.get('bin_location') or inventory['bin_location'],
                'current_qty': inventory['quantity'],
                'available_qty': inventory['available']
            })
        
        if validation_errors:
            return jsonify({
                'success': False,
                'error': 'Validation failed',
                'validation_errors': validation_errors,
                'partial_valid': len(validated_materials)
            }), 400
        
        issued_items = []
        total_cost = 0
        
        try:
            last_issue = conn.execute('''
                SELECT issue_number FROM material_issues 
                WHERE issue_number LIKE 'ISS-%'
                ORDER BY CAST(SUBSTR(issue_number, 5) AS INTEGER) DESC 
                LIMIT 1
            ''').fetchone()
            
            if last_issue:
                try:
                    base_number = int(last_issue['issue_number'].split('-')[1])
                except (ValueError, IndexError):
                    base_number = 0
            else:
                base_number = 0
            
            for idx, mat in enumerate(validated_materials):
                issue_number = f'ISS-{base_number + idx + 1:06d}'
                
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO material_issues 
                    (issue_number, work_order_id, product_id, quantity_issued, issue_date,
                     warehouse_location, bin_location, issued_to, task_reference, 
                     unit_cost, total_cost, remarks, issued_by)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (issue_number, wo_id, mat['product_id'], mat['issue_qty'], issue_date,
                      mat['warehouse_location'], mat['bin_location'], issued_to, task_reference,
                      mat['unit_cost'], mat['total_cost'], remarks, session['user_id']))
                
                issue_id = cursor.lastrowid
                
                new_qty = mat['current_qty'] - mat['issue_qty']
                conn.execute('''
                    UPDATE inventory 
                    SET quantity = ?,
                        status = CASE WHEN ? <= 0 THEN 'Out of Stock' ELSE status END,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE product_id = ?
                ''', (new_qty, new_qty, mat['product_id']))
                
                update_material_requirement_status(conn, wo_id, mat['product_id'])
                
                gl_lines = [
                    {
                        'account_code': '1140',
                        'debit': mat['total_cost'],
                        'credit': 0,
                        'description': f'Material issued to WO {wo["wo_number"]} - {mat["product_name"]} ({issue_number})'
                    },
                    {
                        'account_code': '1130',
                        'debit': 0,
                        'credit': mat['total_cost'],
                        'description': f'Material issued from inventory - {mat["product_name"]} ({issue_number})'
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
                
                total_cost += mat['total_cost']
                issued_items.append({
                    'issue_number': issue_number,
                    'product_code': mat['product_code'],
                    'product_name': mat['product_name'],
                    'quantity': mat['issue_qty'],
                    'total_cost': mat['total_cost']
                })
            
            conn.execute('''
                UPDATE work_orders 
                SET material_cost = COALESCE(material_cost, 0) + ?
                WHERE id = ?
            ''', (total_cost, wo_id))
            
            conn.execute('''
                INSERT INTO audit_trail 
                (table_name, record_id, action_type, performed_by, action_timestamp, changed_fields)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', ('material_issues', wo_id, 'Multi-Issue', session['user_id'], 
                  datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                  f'Issued {len(issued_items)} materials totaling ${total_cost:.2f} to WO {wo["wo_number"]}'))
            
            conn.commit()
            
            return jsonify({
                'success': True,
                'message': f'Successfully issued {len(issued_items)} material(s)',
                'issued_count': len(issued_items),
                'total_cost': total_cost,
                'issued_items': issued_items,
                'work_order': wo['wo_number']
            })
            
        except Exception as e:
            conn.rollback()
            return jsonify({
                'success': False,
                'error': f'Transaction failed: {str(e)}',
                'rollback': True
            }), 500
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        conn.close()


@issuance_bp.route('/api/issuance/transaction-summary', methods=['POST'])
@login_required
def api_transaction_summary():
    """Get live transaction summary before commit"""
    db = Database()
    conn = db.get_connection()
    
    try:
        data = request.get_json()
        materials = data.get('materials', [])
        wo_id = data.get('work_order_id')
        
        summary = {
            'total_materials': len(materials),
            'total_quantity': 0,
            'total_cost': 0,
            'shortages': [],
            'warnings': [],
            'inventory_impacts': []
        }
        
        for mat in materials:
            product_id = mat.get('product_id')
            issue_qty = float(mat.get('issue_qty', 0))
            
            if issue_qty <= 0:
                continue
            
            inventory = conn.execute('''
                SELECT 
                    p.code, p.name, p.cost, p.unit_of_measure,
                    COALESCE(i.quantity, 0) as on_hand,
                    (COALESCE(i.quantity, 0) - COALESCE(i.reserved_quantity, 0)) as available
                FROM products p
                LEFT JOIN inventory i ON p.id = i.product_id
                WHERE p.id = ?
            ''', (product_id,)).fetchone()
            
            if inventory:
                unit_cost = inventory['cost'] or 0
                line_cost = unit_cost * issue_qty
                summary['total_quantity'] += issue_qty
                summary['total_cost'] += line_cost
                
                remaining = inventory['available'] - issue_qty
                summary['inventory_impacts'].append({
                    'product_code': inventory['code'],
                    'product_name': inventory['name'],
                    'current_available': inventory['available'],
                    'issue_qty': issue_qty,
                    'remaining': remaining,
                    'unit_of_measure': inventory['unit_of_measure'],
                    'line_cost': line_cost
                })
                
                if remaining < 0:
                    summary['shortages'].append({
                        'product_code': inventory['code'],
                        'shortage': abs(remaining)
                    })
                elif remaining < inventory['on_hand'] * 0.1:
                    summary['warnings'].append({
                        'product_code': inventory['code'],
                        'message': f'Low stock after issue: {remaining} remaining'
                    })
        
        return jsonify({
            'success': True,
            'summary': summary
        })
    finally:
        conn.close()
