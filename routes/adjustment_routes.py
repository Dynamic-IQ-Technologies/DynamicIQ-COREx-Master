from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import Database, GLAutoPost
from auth import login_required, role_required
from datetime import datetime

adjustment_bp = Blueprint('adjustment_routes', __name__)

@adjustment_bp.route('/adjustments')
@login_required
def list_adjustments():
    db = Database()
    conn = db.get_connection()
    
    adjustments = conn.execute('''
        SELECT 
            ia.*,
            p.code as product_code,
            p.name as product_name,
            p.unit_of_measure,
            u.username as adjusted_by_name
        FROM inventory_adjustments ia
        JOIN products p ON ia.product_id = p.id
        LEFT JOIN users u ON ia.adjusted_by = u.id
        ORDER BY ia.adjustment_date DESC, ia.created_at DESC
    ''').fetchall()
    
    conn.close()
    return render_template('adjustments/list.html', adjustments=adjustments)

@adjustment_bp.route('/adjustments/create', methods=['GET', 'POST'])
@role_required('Admin', 'Planner')
def create_adjustment():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            product_id = int(request.form['product_id'])
            adjustment_type = request.form['adjustment_type']
            quantity_adjusted = float(request.form['quantity_adjusted'])
            adjustment_date = request.form['adjustment_date']
            reason = request.form['reason']
            reference = request.form.get('reference', '')
            remarks = request.form.get('remarks', '')
            
            # Validate product exists
            product = conn.execute('''
                SELECT p.*, i.quantity as current_quantity
                FROM products p
                LEFT JOIN inventory i ON p.id = i.product_id
                WHERE p.id = ?
            ''', (product_id,)).fetchone()
            
            if not product:
                flash('Product not found.', 'danger')
                conn.close()
                return redirect(url_for('adjustment_routes.create_adjustment'))
            
            current_qty = product['current_quantity'] if product['current_quantity'] is not None else 0
            
            # Get product cost and calculate cost impact
            unit_cost = product['cost'] if product['cost'] else 0
            
            # Warn if product has zero cost
            if unit_cost == 0:
                flash('Warning: This product has zero cost. Inventory value will not be affected.', 'warning')
            
            # Calculate new quantity and cost impact
            if adjustment_type == 'Increase':
                new_qty = current_qty + quantity_adjusted
                cost_impact = unit_cost * quantity_adjusted  # Positive impact (inventory value increases)
            else:  # Decrease
                new_qty = current_qty - quantity_adjusted
                if new_qty < 0:
                    flash(f'Cannot decrease by {quantity_adjusted}. Current quantity: {current_qty}', 'danger')
                    conn.close()
                    return redirect(url_for('adjustment_routes.create_adjustment'))
                cost_impact = -(unit_cost * quantity_adjusted)  # Negative impact (inventory value decreases)
            
            # Generate adjustment number
            last_adj = conn.execute('''
                SELECT adjustment_number FROM inventory_adjustments 
                WHERE adjustment_number LIKE 'ADJ-%'
                ORDER BY CAST(SUBSTR(adjustment_number, 5) AS INTEGER) DESC 
                LIMIT 1
            ''').fetchone()
            
            if last_adj:
                try:
                    last_number = int(last_adj['adjustment_number'].split('-')[1])
                    next_number = last_number + 1
                except (ValueError, IndexError):
                    next_number = 1
            else:
                next_number = 1
            
            adjustment_number = f'ADJ-{next_number:06d}'
            
            # Create inventory adjustment with cost tracking
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO inventory_adjustments 
                (adjustment_number, product_id, adjustment_type, quantity_adjusted, 
                 quantity_before, quantity_after, adjustment_date, reason, reference, remarks, adjusted_by,
                 unit_cost, cost_impact)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (adjustment_number, product_id, adjustment_type, quantity_adjusted,
                  current_qty, new_qty, adjustment_date, reason, reference, remarks, session['user_id'],
                  unit_cost, cost_impact))
            
            adjustment_id = cursor.lastrowid
            
            # Update inventory
            inventory = conn.execute('SELECT * FROM inventory WHERE product_id = ?', (product_id,)).fetchone()
            
            if inventory:
                # Update quantity and set unit_cost if not already set
                current_unit_cost = inventory['unit_cost'] if inventory['unit_cost'] else None
                if current_unit_cost is None and unit_cost > 0:
                    # Set unit_cost from product cost if inventory doesn't have one
                    conn.execute('''
                        UPDATE inventory 
                        SET quantity = ?,
                            unit_cost = ?,
                            last_updated = CURRENT_TIMESTAMP,
                            status = CASE WHEN ? <= 0 THEN 'Out of Stock' 
                                         WHEN ? > 0 THEN 'Available' 
                                         ELSE status END
                        WHERE product_id = ?
                    ''', (new_qty, unit_cost, new_qty, new_qty, product_id))
                else:
                    conn.execute('''
                        UPDATE inventory 
                        SET quantity = ?,
                            last_updated = CURRENT_TIMESTAMP,
                            status = CASE WHEN ? <= 0 THEN 'Out of Stock' 
                                         WHEN ? > 0 THEN 'Available' 
                                         ELSE status END
                        WHERE product_id = ?
                    ''', (new_qty, new_qty, new_qty, product_id))
            else:
                # Create inventory record if doesn't exist - include unit_cost
                conn.execute('''
                    INSERT INTO inventory 
                    (product_id, quantity, unit_cost, status)
                    VALUES (?, ?, ?, 'Available')
                ''', (product_id, new_qty, unit_cost if unit_cost > 0 else None))
            
            # Auto-post GL entry for inventory adjustment (only if cost_impact != 0)
            if cost_impact != 0:
                if adjustment_type == 'Increase':
                    # Inventory increased
                    # Debit: Inventory (increase asset)
                    # Credit: Other Income (increase revenue/income)
                    gl_lines = [
                        {
                            'account_code': '1130',  # Inventory
                            'debit': abs(cost_impact),
                            'credit': 0,
                            'description': f'Inventory increase - {product["name"]} ({adjustment_number})'
                        },
                        {
                            'account_code': '4300',  # Other Income
                            'debit': 0,
                            'credit': abs(cost_impact),
                            'description': f'Inventory adjustment gain - {product["name"]} ({adjustment_number})'
                        }
                    ]
                else:  # Decrease
                    # Inventory decreased
                    # Debit: Material Cost (increase expense)
                    # Credit: Inventory (decrease asset)
                    gl_lines = [
                        {
                            'account_code': '5100',  # Material Cost
                            'debit': abs(cost_impact),
                            'credit': 0,
                            'description': f'Inventory adjustment loss - {product["name"]} ({adjustment_number})'
                        },
                        {
                            'account_code': '1130',  # Inventory
                            'debit': 0,
                            'credit': abs(cost_impact),
                            'description': f'Inventory decrease - {product["name"]} ({adjustment_number})'
                        }
                    ]
                
                GLAutoPost.create_auto_journal_entry(
                    conn=conn,
                    entry_date=adjustment_date,
                    description=f'Inventory Adjustment - {adjustment_number}',
                    transaction_source='Inventory Adjustment',
                    reference_type='inventory_adjustment',
                    reference_id=adjustment_id,
                    lines=gl_lines,
                    created_by=session['user_id']
                )
            
            conn.commit()
            flash(f'Inventory adjusted successfully! Adjustment Number: {adjustment_number}', 'success')
            return redirect(url_for('adjustment_routes.view_adjustment', adjustment_number=adjustment_number))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error adjusting inventory: {str(e)}', 'danger')
        finally:
            conn.close()
    
    # GET request - show form
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Get all products with current inventory
    products = conn.execute('''
        SELECT 
            p.*,
            COALESCE(i.quantity, 0) as current_quantity,
            i.warehouse_location,
            i.status as inventory_status
        FROM products p
        LEFT JOIN inventory i ON p.id = i.product_id
        ORDER BY p.code
    ''').fetchall()
    
    conn.close()
    return render_template('adjustments/create.html', products=products, today=today)

@adjustment_bp.route('/adjustments/<adjustment_number>')
@login_required
def view_adjustment(adjustment_number):
    db = Database()
    conn = db.get_connection()
    
    adjustment = conn.execute('''
        SELECT 
            ia.*,
            p.code as product_code,
            p.name as product_name,
            p.unit_of_measure,
            u.username as adjusted_by_name
        FROM inventory_adjustments ia
        JOIN products p ON ia.product_id = p.id
        LEFT JOIN users u ON ia.adjusted_by = u.id
        WHERE ia.adjustment_number = ?
    ''', (adjustment_number,)).fetchone()
    
    if not adjustment:
        flash('Adjustment record not found.', 'danger')
        conn.close()
        return redirect(url_for('adjustment_routes.list_adjustments'))
    
    conn.close()
    return render_template('adjustments/view.html', adjustment=adjustment)
