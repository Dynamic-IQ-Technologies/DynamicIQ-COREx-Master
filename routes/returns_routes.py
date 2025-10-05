from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import Database
from auth import login_required, role_required
from datetime import datetime

returns_bp = Blueprint('returns_routes', __name__)

@returns_bp.route('/returns')
@login_required
def list_returns():
    db = Database()
    conn = db.get_connection()
    
    returns = conn.execute('''
        SELECT 
            mr.*,
            wo.wo_number,
            p.code as product_code,
            p.name as product_name,
            p.unit_of_measure,
            u.username as returned_by_name
        FROM material_returns mr
        JOIN work_orders wo ON mr.work_order_id = wo.id
        JOIN products p ON mr.product_id = p.id
        LEFT JOIN users u ON mr.returned_by = u.id
        ORDER BY mr.return_date DESC, mr.created_at DESC
    ''').fetchall()
    
    conn.close()
    return render_template('returns/list.html', returns=returns)

@returns_bp.route('/returns/create', methods=['GET', 'POST'])
@role_required('Admin', 'Production Staff', 'Planner')
def create_return():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            wo_id = int(request.form['work_order_id'])
            product_id = int(request.form['product_id'])
            quantity_returned = float(request.form['quantity_returned'])
            return_date = request.form['return_date']
            warehouse = request.form.get('warehouse_location', 'Main')
            bin_location = request.form.get('bin_location', '')
            condition = request.form.get('condition', 'Serviceable')
            reason = request.form.get('reason', '')
            remarks = request.form.get('remarks', '')
            
            # Validate work order exists
            wo = conn.execute('SELECT * FROM work_orders WHERE id = ?', (wo_id,)).fetchone()
            
            if not wo:
                flash('Work Order not found.', 'danger')
                conn.close()
                return redirect(url_for('returns_routes.create_return'))
            
            # Verify product was issued to this work order
            issued = conn.execute('''
                SELECT SUM(quantity_issued) as total_issued
                FROM material_issues
                WHERE work_order_id = ? AND product_id = ?
            ''', (wo_id, product_id)).fetchone()
            
            returned_already = conn.execute('''
                SELECT SUM(quantity_returned) as total_returned
                FROM material_returns
                WHERE work_order_id = ? AND product_id = ?
            ''', (wo_id, product_id)).fetchone()
            
            total_issued = issued['total_issued'] if issued and issued['total_issued'] else 0
            total_returned = returned_already['total_returned'] if returned_already and returned_already['total_returned'] else 0
            
            if total_issued == 0:
                flash('This product was never issued to this work order.', 'danger')
                conn.close()
                return redirect(url_for('returns_routes.create_return'))
            
            if (total_returned + quantity_returned) > total_issued:
                flash(f'Cannot return more than issued. Issued: {total_issued}, Already returned: {total_returned}', 'danger')
                conn.close()
                return redirect(url_for('returns_routes.create_return'))
            
            # Get product cost
            product = conn.execute('SELECT cost FROM products WHERE id = ?', (product_id,)).fetchone()
            unit_cost = product['cost'] if product else 0
            
            # Warn if product has zero cost
            if unit_cost == 0:
                flash('Warning: This product has zero cost. Cost tracking may be inaccurate.', 'warning')
            
            total_cost = unit_cost * quantity_returned
            
            # Generate return number
            last_return = conn.execute('''
                SELECT return_number FROM material_returns 
                WHERE return_number LIKE 'RET-%'
                ORDER BY CAST(SUBSTR(return_number, 5) AS INTEGER) DESC 
                LIMIT 1
            ''').fetchone()
            
            if last_return:
                try:
                    last_number = int(last_return['return_number'].split('-')[1])
                    next_number = last_number + 1
                except (ValueError, IndexError):
                    next_number = 1
            else:
                next_number = 1
            
            return_number = f'RET-{next_number:06d}'
            
            # Create material return with cost tracking
            conn.execute('''
                INSERT INTO material_returns 
                (return_number, work_order_id, product_id, quantity_returned, return_date, 
                 warehouse_location, bin_location, condition, reason, remarks, returned_by,
                 unit_cost, total_cost)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (return_number, wo_id, product_id, quantity_returned, return_date,
                  warehouse, bin_location, condition, reason, remarks, session['user_id'],
                  unit_cost, total_cost))
            
            # Update inventory - add quantity back
            inventory = conn.execute('''
                SELECT * FROM inventory WHERE product_id = ?
            ''', (product_id,)).fetchone()
            
            if inventory:
                new_qty = inventory['quantity'] + quantity_returned
                conn.execute('''
                    UPDATE inventory 
                    SET quantity = ?,
                        last_updated = CURRENT_TIMESTAMP,
                        status = 'Available'
                    WHERE product_id = ?
                ''', (new_qty, product_id))
            else:
                # Create inventory record if doesn't exist
                conn.execute('''
                    INSERT INTO inventory 
                    (product_id, quantity, condition, warehouse_location, status)
                    VALUES (?, ?, ?, ?, 'Available')
                ''', (product_id, quantity_returned, condition, warehouse))
            
            # Reduce work order material cost (already calculated as total_cost above)
            current_cost = conn.execute(
                'SELECT material_cost FROM work_orders WHERE id = ?', (wo_id,)
            ).fetchone()
            new_material_cost = max(0, (current_cost['material_cost'] or 0) - total_cost)
            
            conn.execute('''
                UPDATE work_orders 
                SET material_cost = ?
                WHERE id = ?
            ''', (new_material_cost, wo_id))
            
            conn.commit()
            flash(f'Material returned successfully! Return Number: {return_number}', 'success')
            return redirect(url_for('returns_routes.view_return', return_number=return_number))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error returning material: {str(e)}', 'danger')
        finally:
            conn.close()
    
    # GET request - show form
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Get work orders with issued materials
    work_orders = conn.execute('''
        SELECT DISTINCT
            wo.id,
            wo.wo_number,
            wo.status,
            p.code as product_code,
            p.name as product_name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        JOIN material_issues mi ON wo.id = mi.work_order_id
        ORDER BY wo.created_at DESC
    ''').fetchall()
    
    conn.close()
    return render_template('returns/create.html', work_orders=work_orders, today=today)

@returns_bp.route('/returns/get_issued_materials/<int:wo_id>')
@login_required
def get_issued_materials(wo_id):
    db = Database()
    conn = db.get_connection()
    
    # Get issued materials for this work order
    materials = conn.execute('''
        SELECT 
            p.id as product_id,
            p.code as product_code,
            p.name as product_name,
            p.unit_of_measure,
            SUM(mi.quantity_issued) as total_issued,
            COALESCE((SELECT SUM(quantity_returned) 
                     FROM material_returns 
                     WHERE work_order_id = ? AND product_id = p.id), 0) as total_returned
        FROM material_issues mi
        JOIN products p ON mi.product_id = p.id
        WHERE mi.work_order_id = ?
        GROUP BY p.id, p.code, p.name, p.unit_of_measure
        HAVING (total_issued - total_returned) > 0
    ''', (wo_id, wo_id)).fetchall()
    
    conn.close()
    
    # Convert to list of dicts for JSON response
    result = []
    for m in materials:
        result.append({
            'product_id': m['product_id'],
            'product_code': m['product_code'],
            'product_name': m['product_name'],
            'unit_of_measure': m['unit_of_measure'],
            'available_to_return': m['total_issued'] - m['total_returned']
        })
    
    from flask import jsonify
    return jsonify(result)

@returns_bp.route('/returns/<return_number>')
@login_required
def view_return(return_number):
    db = Database()
    conn = db.get_connection()
    
    ret = conn.execute('''
        SELECT 
            mr.*,
            wo.wo_number,
            wo.status as wo_status,
            p.code as product_code,
            p.name as product_name,
            p.unit_of_measure,
            u.username as returned_by_name
        FROM material_returns mr
        JOIN work_orders wo ON mr.work_order_id = wo.id
        JOIN products p ON mr.product_id = p.id
        LEFT JOIN users u ON mr.returned_by = u.id
        WHERE mr.return_number = ?
    ''', (return_number,)).fetchone()
    
    if not ret:
        flash('Return record not found.', 'danger')
        conn.close()
        return redirect(url_for('returns_routes.list_returns'))
    
    conn.close()
    return render_template('returns/view.html', ret=ret)
