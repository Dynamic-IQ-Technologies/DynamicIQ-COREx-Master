from flask import Blueprint, render_template, request, redirect, url_for, flash, Response, session, send_from_directory, jsonify
from models import Database, AuditLogger
from auth import login_required, role_required
from werkzeug.utils import secure_filename
from reportlab.lib.pagesizes import letter, inch
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.graphics.barcode import code128
from reportlab.pdfgen import canvas
from datetime import datetime
import csv
import io
import math
import os
import uuid

INVENTORY_UPLOAD_FOLDER = 'static/uploads/inventory_documents'
ALLOWED_EXTENSIONS = {'pdf', 'doc', 'docx', 'xls', 'xlsx', 'png', 'jpg', 'jpeg', 'gif', 'txt', 'csv'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def ensure_upload_folder():
    if not os.path.exists(INVENTORY_UPLOAD_FOLDER):
        os.makedirs(INVENTORY_UPLOAD_FOLDER)

inventory_bp = Blueprint('inventory_routes', __name__)

@inventory_bp.route('/inventory')
@login_required
def list_inventory():
    db = Database()
    conn = db.get_connection()
    
    # Get filter parameters
    filter_serialized = request.args.get('filter_serialized', 'all')
    search_serial = request.args.get('search_serial', '').strip()
    search_part = request.args.get('search_part', '').strip()
    filter_warehouse = request.args.get('filter_warehouse', '').strip()
    filter_bin = request.args.get('filter_bin', '').strip()
    
    # Get distinct warehouse and bin locations for filter dropdowns
    warehouses = conn.execute('''
        SELECT DISTINCT warehouse_location FROM inventory 
        WHERE warehouse_location IS NOT NULL AND warehouse_location != ''
        ORDER BY warehouse_location
    ''').fetchall()
    
    bins = conn.execute('''
        SELECT DISTINCT bin_location FROM inventory 
        WHERE bin_location IS NOT NULL AND bin_location != ''
        ORDER BY bin_location
    ''').fetchall()
    
    # Build query with filters
    # Use inventory.unit_cost if set, otherwise fall back to product cost
    query = '''
        SELECT i.*, p.code, p.name, p.unit_of_measure, 
               COALESCE(i.unit_cost, p.cost, 0) as display_unit_cost,
               (i.quantity * COALESCE(i.unit_cost, p.cost, 0)) as inventory_value
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE 1=1
    '''
    params = []
    
    # Apply serialization filter
    if filter_serialized == 'serialized':
        query += ' AND i.is_serialized = 1'
    elif filter_serialized == 'non_serialized':
        query += ' AND (i.is_serialized = 0 OR i.is_serialized IS NULL)'
    
    # Apply serial number search
    if search_serial:
        query += ' AND i.serial_number LIKE ?'
        params.append(f'%{search_serial}%')
    
    # Apply part number search
    if search_part:
        query += ' AND (p.code LIKE ? OR p.name LIKE ?)'
        params.append(f'%{search_part}%')
        params.append(f'%{search_part}%')
    
    # Apply warehouse location filter
    if filter_warehouse:
        query += ' AND i.warehouse_location = ?'
        params.append(filter_warehouse)
    
    # Apply bin location filter
    if filter_bin:
        query += ' AND i.bin_location = ?'
        params.append(filter_bin)
    
    query += ' ORDER BY p.code'
    
    inventory = conn.execute(query, params).fetchall()
    
    # Calculate total inventory value (defensive against any potential NULLs)
    total_value = sum(item['inventory_value'] or 0 for item in inventory)
    
    conn.close()
    
    return render_template('inventory/list.html', 
                         inventory=inventory,
                         filter_serialized=filter_serialized,
                         search_serial=search_serial,
                         search_part=search_part,
                         filter_warehouse=filter_warehouse,
                         filter_bin=filter_bin,
                         warehouses=warehouses,
                         bins=bins,
                         total_value=total_value)

@inventory_bp.route('/inventory/<int:id>/view')
@login_required
def view_inventory(id):
    db = Database()
    conn = db.get_connection()
    
    # Get inventory details with product information and cost
    # Use inventory.unit_cost if set, otherwise fall back to product cost
    inventory = conn.execute('''
        SELECT i.*, p.code, p.name, p.description, p.unit_of_measure, 
               COALESCE(i.unit_cost, p.cost, 0) as display_unit_cost,
               (i.quantity * COALESCE(i.unit_cost, p.cost, 0)) as inventory_value
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.id = ?
    ''', (id,)).fetchone()
    
    if not inventory:
        conn.close()
        flash('Inventory record not found', 'danger')
        return redirect(url_for('inventory_routes.list_inventory'))
    
    product_id = inventory['product_id']
    
    # Get related Purchase Orders (receiving transactions)
    receiving_history = conn.execute('''
        SELECT rt.*, po.po_number, s.name as supplier_name
        FROM receiving_transactions rt
        JOIN purchase_orders po ON rt.po_id = po.id
        LEFT JOIN suppliers s ON po.supplier_id = s.id
        WHERE rt.product_id = ?
        ORDER BY rt.receipt_date DESC
        LIMIT 10
    ''', (product_id,)).fetchall()
    
    # Get related Work Orders (material issues)
    material_issues = conn.execute('''
        SELECT mi.*, wo.wo_number
        FROM material_issues mi
        JOIN work_orders wo ON mi.work_order_id = wo.id
        WHERE mi.product_id = ?
        ORDER BY mi.issue_date DESC
        LIMIT 10
    ''', (product_id,)).fetchall()
    
    # Get related Work Orders (material returns)
    material_returns = conn.execute('''
        SELECT mr.*, wo.wo_number
        FROM material_returns mr
        JOIN work_orders wo ON mr.work_order_id = wo.id
        WHERE mr.product_id = ?
        ORDER BY mr.return_date DESC
        LIMIT 10
    ''', (product_id,)).fetchall()
    
    # Get related Sales Order allocations
    sales_allocations = conn.execute('''
        SELECT sol.*, so.so_number, c.name as customer_name
        FROM sales_order_lines sol
        JOIN sales_orders so ON sol.so_id = so.id
        LEFT JOIN customers c ON so.customer_id = c.id
        WHERE sol.product_id = ? AND sol.allocated_quantity > 0
        ORDER BY so.order_date DESC
        LIMIT 10
    ''', (product_id,)).fetchall()
    
    # Get inventory adjustments
    adjustments = conn.execute('''
        SELECT ia.*, u.username as adjusted_by_name
        FROM inventory_adjustments ia
        LEFT JOIN users u ON ia.adjusted_by = u.id
        WHERE ia.product_id = ?
        ORDER BY ia.adjustment_date DESC
        LIMIT 10
    ''', (product_id,)).fetchall()
    
    # Get work orders that were turned into stock for this inventory record
    wo_turnins = conn.execute('''
        SELECT wo.id, wo.wo_number, wo.quantity, wo.status, wo.disposition,
               wo.actual_end_date, wo.material_cost, wo.labor_cost, wo.overhead_cost,
               p.code as product_code, p.name as product_name
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.inventory_id = ?
        ORDER BY wo.actual_end_date DESC
    ''', (id,)).fetchall()
    
    # Get audit trail for this inventory record
    audit_trail = conn.execute('''
        SELECT * FROM audit_trail
        WHERE record_type = 'inventory' AND record_id = ?
        ORDER BY modified_at DESC
        LIMIT 50
    ''', (str(id),)).fetchall()
    
    # Get documents for this inventory record
    documents = conn.execute('''
        SELECT id.*, u.username as uploaded_by_name
        FROM inventory_documents id
        LEFT JOIN users u ON id.uploaded_by = u.id
        WHERE id.inventory_id = ? AND id.is_active = 1
        ORDER BY id.uploaded_at DESC
    ''', (id,)).fetchall()
    
    conn.close()
    
    return render_template('inventory/view.html', 
                          inventory=inventory,
                          receiving_history=receiving_history,
                          material_issues=material_issues,
                          material_returns=material_returns,
                          sales_allocations=sales_allocations,
                          adjustments=adjustments,
                          wo_turnins=wo_turnins,
                          audit_trail=audit_trail,
                          documents=documents)

@inventory_bp.route('/inventory/create', methods=['GET', 'POST'])
@role_required('Admin', 'Production Staff')
def create_inventory():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        product_id = int(request.form['product_id'])
        
        existing = conn.execute('SELECT id FROM inventory WHERE product_id=?', (product_id,)).fetchone()
        
        if existing:
            conn.close()
            flash('Inventory already exists for this product. Use adjust instead.', 'warning')
            return redirect(url_for('inventory_routes.list_inventory'))
        
        # Get serialization fields
        is_serialized = 1 if request.form.get('is_serialized') else 0
        serial_number = request.form.get('serial_number', '').strip()
        
        # Validate serial number for serialized items
        if is_serialized:
            if not serial_number:
                conn.close()
                flash('Serial number is required for serialized products.', 'danger')
                return redirect(url_for('inventory_routes.create_inventory'))
            
            # Check if serial number already exists
            existing_serial = conn.execute(
                'SELECT id FROM inventory WHERE serial_number = ?', 
                (serial_number,)
            ).fetchone()
            
            if existing_serial:
                conn.close()
                flash(f'Serial number "{serial_number}" is already in use. Please use a unique serial number.', 'danger')
                return redirect(url_for('inventory_routes.create_inventory'))
        
        conn.execute('''
            INSERT INTO inventory (product_id, quantity, reorder_point, safety_stock, 
                                   warehouse_location, bin_location, condition, status,
                                   is_serialized, serial_number)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'Available', ?, ?)
        ''', (
            product_id,
            float(request.form.get('quantity', 0)),
            float(request.form.get('reorder_point', 0)),
            float(request.form.get('safety_stock', 0)),
            request.form.get('warehouse_location', 'Main'),
            request.form.get('bin_location', ''),
            request.form.get('condition', 'Serviceable'),
            is_serialized,
            serial_number if serial_number else None
        ))
        
        inventory_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        
        product = conn.execute('SELECT code, name FROM products WHERE id = ?', (product_id,)).fetchone()
        AuditLogger.log_change(conn, 'inventory', inventory_id, 'CREATE', session.get('user_id'),
                              {'product_code': product['code'], 'quantity': float(request.form.get('quantity', 0)),
                               'serial_number': serial_number if serial_number else None})
        conn.commit()
        conn.close()
        
        serial_msg = f' (S/N: {serial_number})' if serial_number else ''
        flash(f'Inventory created successfully! Inventory ID: INV-{inventory_id:06d}{serial_msg}', 'success')
        return redirect(url_for('inventory_routes.list_inventory'))
    
    products = conn.execute('''
        SELECT p.* FROM products p
        WHERE p.id NOT IN (SELECT product_id FROM inventory)
        ORDER BY p.code
    ''').fetchall()
    conn.close()
    
    return render_template('inventory/create.html', products=products)

@inventory_bp.route('/inventory/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Production Staff')
def edit_inventory(id):
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            # Parse numeric fields with error handling
            try:
                quantity = float(request.form.get('quantity', 0))
                reorder_point = float(request.form.get('reorder_point', 0))
                safety_stock = float(request.form.get('safety_stock', 0))
                unit_cost_str = request.form.get('unit_cost', '').strip()
                unit_cost = float(unit_cost_str) if unit_cost_str else None
            except (ValueError, TypeError):
                flash('Invalid numeric value provided', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            warehouse_location = request.form.get('warehouse_location', '').strip()
            bin_location = request.form.get('bin_location', '').strip()
            condition = request.form.get('condition', 'Serviceable')
            status = request.form.get('status', 'Available')
            
            # Validate numeric fields are finite
            if not math.isfinite(quantity):
                flash('Quantity must be a valid number', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            if not math.isfinite(reorder_point):
                flash('Reorder point must be a valid number', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            if not math.isfinite(safety_stock):
                flash('Safety stock must be a valid number', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            # Validate numeric fields are non-negative
            if quantity < 0:
                flash('Quantity cannot be negative', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            if reorder_point < 0:
                flash('Reorder point cannot be negative', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            if safety_stock < 0:
                flash('Safety stock cannot be negative', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            # Validate required fields
            if not warehouse_location:
                flash('Warehouse location is required', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            if not bin_location:
                flash('Bin location is required', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            # Validate condition and status values
            valid_conditions = ['New', 'Serviceable', 'Overhauled', 'Repaired']
            if condition not in valid_conditions:
                flash('Invalid condition value', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            valid_statuses = ['Available', 'Reserved', 'Out of Stock']
            if status not in valid_statuses:
                flash('Invalid status value', 'danger')
                conn.close()
                return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            # Get serialization fields
            is_serialized = 1 if request.form.get('is_serialized') else 0
            serial_number = request.form.get('serial_number', '').strip()
            
            # Get inventory control fields
            expiration_date = request.form.get('expiration_date', '').strip() or None
            last_inspection_date = request.form.get('last_inspection_date', '').strip() or None
            next_inspection_date = request.form.get('next_inspection_date', '').strip() or None
            inspected_by = request.form.get('inspected_by', '').strip() or None
            inspection_notes = request.form.get('inspection_notes', '').strip() or None
            
            # Get inventory tracing fields
            trace_tag = request.form.get('trace_tag', '').strip() or None
            trace = request.form.get('trace', '').strip() or None
            trace_type = request.form.get('trace_type', '').strip() or None
            msn_esn = request.form.get('msn_esn', '').strip() or None
            mfr_code = request.form.get('mfr_code', '').strip() or None
            lot_number = request.form.get('lot_number', '').strip() or None
            source = request.form.get('source', '').strip() or None
            manufactured_date = request.form.get('manufactured_date', '').strip() or None
            country_of_origin = request.form.get('country_of_origin', '').strip() or None
            
            # Times & Cycles fields
            cycle_limit_str = request.form.get('cycle_limit', '').strip()
            cycle_limit = float(cycle_limit_str) if cycle_limit_str else None
            csn_str = request.form.get('csn', '').strip()
            csn = float(csn_str) if csn_str else None
            cso_str = request.form.get('cso', '').strip()
            cso = float(cso_str) if cso_str else None
            cycles_remaining_str = request.form.get('cycles_remaining', '').strip()
            cycles_remaining = float(cycles_remaining_str) if cycles_remaining_str else None
            time_limit_str = request.form.get('time_limit', '').strip()
            time_limit = float(time_limit_str) if time_limit_str else None
            tsn_str = request.form.get('tsn', '').strip()
            tsn = float(tsn_str) if tsn_str else None
            tso_str = request.form.get('tso', '').strip()
            tso = float(tso_str) if tso_str else None
            time_remaining_str = request.form.get('time_remaining', '').strip()
            time_remaining = float(time_remaining_str) if time_remaining_str else None
            
            # Calibration fields
            last_calibration_date = request.form.get('last_calibration_date', '').strip() or None
            calibration_frequency_str = request.form.get('calibration_frequency', '').strip()
            calibration_frequency = int(calibration_frequency_str) if calibration_frequency_str else None
            next_calibration_date = request.form.get('next_calibration_date', '').strip() or None
            
            # Auto-calculate next calibration date if last date and frequency are set but next is not
            if last_calibration_date and calibration_frequency and not next_calibration_date:
                from datetime import datetime, timedelta
                last_cal = datetime.strptime(last_calibration_date, '%Y-%m-%d')
                next_cal = last_cal + timedelta(days=calibration_frequency)
                next_calibration_date = next_cal.strftime('%Y-%m-%d')
            
            # Validate serial number for serialized items
            if is_serialized:
                if not serial_number:
                    flash('Serial number is required for serialized products.', 'danger')
                    conn.close()
                    return redirect(url_for('inventory_routes.edit_inventory', id=id))
                
                # Check if serial number already exists (excluding current record)
                existing_serial = conn.execute(
                    'SELECT id FROM inventory WHERE serial_number = ? AND id != ?', 
                    (serial_number, id)
                ).fetchone()
                
                if existing_serial:
                    flash(f'Serial number "{serial_number}" is already in use. Please use a unique serial number.', 'danger')
                    conn.close()
                    return redirect(url_for('inventory_routes.edit_inventory', id=id))
            
            # Get old values for audit
            old_record = conn.execute('SELECT * FROM inventory WHERE id = ?', (id,)).fetchone()
            
            # Update inventory
            conn.execute('''
                UPDATE inventory 
                SET quantity=?, 
                    reorder_point=?, 
                    safety_stock=?, 
                    unit_cost=?,
                    warehouse_location=?,
                    bin_location=?,
                    condition=?,
                    status=?,
                    is_serialized=?,
                    serial_number=?,
                    expiration_date=?,
                    last_inspection_date=?,
                    next_inspection_date=?,
                    inspected_by=?,
                    inspection_notes=?,
                    trace_tag=?,
                    trace=?,
                    trace_type=?,
                    msn_esn=?,
                    mfr_code=?,
                    lot_number=?,
                    source=?,
                    manufactured_date=?,
                    country_of_origin=?,
                    cycle_limit=?,
                    csn=?,
                    cso=?,
                    cycles_remaining=?,
                    time_limit=?,
                    tsn=?,
                    tso=?,
                    time_remaining=?,
                    last_calibration_date=?,
                    calibration_frequency=?,
                    next_calibration_date=?,
                    last_updated=CURRENT_TIMESTAMP 
                WHERE id=?
            ''', (quantity, reorder_point, safety_stock, unit_cost, warehouse_location, bin_location, 
                  condition, status, is_serialized, serial_number if serial_number else None,
                  expiration_date, last_inspection_date, next_inspection_date,
                  inspected_by, inspection_notes, trace_tag, trace, trace_type,
                  msn_esn, mfr_code, lot_number, source, manufactured_date, country_of_origin,
                  cycle_limit, csn, cso, cycles_remaining, time_limit, tsn, tso, time_remaining,
                  last_calibration_date, calibration_frequency, next_calibration_date, id))
            
            AuditLogger.log_change(conn, 'inventory', id, 'UPDATE', session.get('user_id'),
                                  {'quantity': quantity, 'old_quantity': old_record['quantity'],
                                   'warehouse_location': warehouse_location, 'condition': condition, 'status': status})
            conn.commit()
            flash('Inventory updated successfully!', 'success')
            
        except Exception as e:
            conn.rollback()
            flash(f'Error updating inventory: {str(e)}', 'danger')
        finally:
            conn.close()
        
        return redirect(url_for('inventory_routes.list_inventory'))
    
    # GET request - show edit form
    inventory = conn.execute('''
        SELECT i.*, p.code, p.name, p.unit_of_measure, p.description
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.id = ?
    ''', (id,)).fetchone()
    
    conn.close()
    
    if not inventory:
        flash('Inventory record not found', 'danger')
        return redirect(url_for('inventory_routes.list_inventory'))
    
    return render_template('inventory/edit.html', inventory=inventory)

@inventory_bp.route('/inventory/<int:id>/adjust', methods=['GET', 'POST'])
@role_required('Admin', 'Production Staff')
def adjust_inventory(id):
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        adjustment_type = request.form['adjustment_type']
        quantity = float(request.form['quantity'])
        
        current = conn.execute('SELECT quantity FROM inventory WHERE id=?', (id,)).fetchone()
        
        if adjustment_type == 'add':
            new_quantity = current['quantity'] + quantity
        elif adjustment_type == 'subtract':
            new_quantity = max(0, current['quantity'] - quantity)
        else:
            new_quantity = quantity
        
        conn.execute('UPDATE inventory SET quantity=?, last_updated=CURRENT_TIMESTAMP WHERE id=?', 
                    (new_quantity, id))
        
        reason = request.form.get('reason', '')
        AuditLogger.log_change(conn, 'inventory', id, 'ADJUST', session.get('user_id'),
                              {'adjustment_type': adjustment_type, 'adjustment_qty': quantity,
                               'old_quantity': current['quantity'], 'new_quantity': new_quantity,
                               'reason': reason})
        conn.commit()
        conn.close()
        
        flash('Inventory adjusted successfully!', 'success')
        return redirect(url_for('inventory_routes.list_inventory'))
    
    inventory_item = conn.execute('''
        SELECT i.*, p.code, p.name, p.unit_of_measure
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.id=?
    ''', (id,)).fetchone()
    conn.close()
    
    return render_template('inventory/adjust.html', item=inventory_item)

@inventory_bp.route('/inventory/export')
@login_required
def export_inventory():
    db = Database()
    conn = db.get_connection()
    inventory = conn.execute('''
        SELECT p.code, p.name, i.quantity, i.reorder_point, i.safety_stock
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        ORDER BY p.code
    ''').fetchall()
    conn.close()
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Product Code', 'Product Name', 'Quantity', 'Reorder Point', 'Safety Stock'])
    
    for item in inventory:
        writer.writerow([item['code'], item['name'], item['quantity'], 
                        item['reorder_point'], item['safety_stock']])
    
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=inventory_export.csv'}
    )

@inventory_bp.route('/inventory/template')
@login_required
def download_template():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Product Code', 'Product Name', 'Quantity', 'Reorder Point', 'Safety Stock'])
    
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=inventory_import_template.csv'}
    )

@inventory_bp.route('/inventory/mass-update', methods=['GET', 'POST'])
@role_required('Admin', 'Production Staff')
def mass_update_inventory():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            selected_ids = request.form.getlist('selected_ids')
            
            if not selected_ids:
                flash('No items selected for update.', 'warning')
                return redirect(url_for('inventory_routes.mass_update_inventory'))
            
            updated_count = 0
            
            for item_id in selected_ids:
                try:
                    item_id_int = int(item_id)
                    
                    quantity = float(request.form.get(f'quantity_{item_id}', 0))
                    reorder_point = float(request.form.get(f'reorder_point_{item_id}', 0))
                    safety_stock = float(request.form.get(f'safety_stock_{item_id}', 0))
                    warehouse_location = request.form.get(f'warehouse_location_{item_id}', 'Main')
                    bin_location = request.form.get(f'bin_location_{item_id}', '')
                    condition = request.form.get(f'condition_{item_id}', 'Serviceable')
                    
                    conn.execute('''
                        UPDATE inventory
                        SET quantity = ?,
                            reorder_point = ?,
                            safety_stock = ?,
                            warehouse_location = ?,
                            bin_location = ?,
                            condition = ?,
                            status = CASE WHEN ? <= 0 THEN 'Out of Stock' ELSE status END,
                            last_updated = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (quantity, reorder_point, safety_stock, warehouse_location, 
                          bin_location, condition, quantity, item_id_int))
                    
                    updated_count += 1
                except Exception as e:
                    flash(f'Error updating inventory ID {item_id}: {str(e)}', 'warning')
            
            conn.commit()
            flash(f'Successfully updated {updated_count} inventory item(s)!', 'success')
            return redirect(url_for('inventory_routes.list_inventory'))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error during mass update: {str(e)}', 'danger')
        finally:
            conn.close()
        
        return redirect(url_for('inventory_routes.mass_update_inventory'))
    
    # GET request - show form
    inventory = conn.execute('''
        SELECT i.*, p.code, p.name, p.unit_of_measure
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        ORDER BY p.code
    ''').fetchall()
    conn.close()
    
    return render_template('inventory/mass_update.html', inventory=inventory)

@inventory_bp.route('/inventory/import', methods=['POST'])
@role_required('Admin', 'Production Staff')
def import_inventory():
    if 'file' not in request.files:
        flash('No file uploaded', 'danger')
        return redirect(url_for('inventory_routes.list_inventory'))
    
    file = request.files['file']
    if not file or not file.filename:
        flash('No file selected', 'danger')
        return redirect(url_for('inventory_routes.list_inventory'))
    
    if not file.filename.lower().endswith('.csv'):
        flash('Please upload a CSV file', 'danger')
        return redirect(url_for('inventory_routes.list_inventory'))
    
    db = Database()
    conn = None
    
    try:
        stream = io.StringIO(file.stream.read().decode('UTF8'), newline=None)
        csv_reader = csv.DictReader(stream)
        
        conn = db.get_connection()
        
        imported_count = 0
        skipped_count = 0
        errors = []
        
        for row_num, row in enumerate(csv_reader, start=2):
            try:
                code = row.get('Product Code', '').strip()
                quantity_str = row.get('Quantity', '').strip()
                reorder_str = row.get('Reorder Point', '').strip()
                safety_str = row.get('Safety Stock', '').strip()
                
                if not code:
                    skipped_count += 1
                    errors.append(f"Row {row_num}: Missing product code")
                    continue
                
                try:
                    quantity = float(quantity_str) if quantity_str else 0.0
                    reorder_point = float(reorder_str) if reorder_str else 0.0
                    safety_stock = float(safety_str) if safety_str else 0.0
                except ValueError:
                    skipped_count += 1
                    errors.append(f"Row {row_num}: Invalid number format")
                    continue
                
                product = conn.execute('SELECT id FROM products WHERE code = ?', (code,)).fetchone()
                
                if not product:
                    skipped_count += 1
                    errors.append(f"Row {row_num}: Product '{code}' not found")
                    continue
                
                conn.execute('''
                    UPDATE inventory 
                    SET quantity=?, reorder_point=?, safety_stock=?, last_updated=CURRENT_TIMESTAMP
                    WHERE product_id=?
                ''', (quantity, reorder_point, safety_stock, product['id']))
                
                imported_count += 1
            except Exception as row_error:
                skipped_count += 1
                errors.append(f"Row {row_num}: {str(row_error)}")
        
        conn.commit()
        
        if imported_count > 0:
            flash(f'Successfully imported {imported_count} inventory items. Skipped {skipped_count} rows.', 'success')
        else:
            flash(f'No inventory items imported. Skipped {skipped_count} rows.', 'warning')
        
        if errors and len(errors) <= 10:
            for error in errors:
                flash(error, 'warning')
        elif errors:
            flash(f'First 10 errors: {"; ".join(errors[:10])}', 'warning')
            
    except Exception as e:
        flash(f'Error importing inventory: {str(e)}', 'danger')
    finally:
        if conn:
            conn.close()
    
    return redirect(url_for('inventory_routes.list_inventory'))

@inventory_bp.route('/inventory/<int:id>/documents/upload', methods=['POST'])
@role_required('Admin', 'Production Staff', 'Procurement')
def upload_inventory_document(id):
    """Upload a document to an inventory line"""
    ensure_upload_folder()
    db = Database()
    conn = db.get_connection()
    
    try:
        inventory = conn.execute('SELECT * FROM inventory WHERE id = ?', (id,)).fetchone()
        if not inventory:
            flash('Inventory record not found', 'danger')
            return redirect(url_for('inventory_routes.list_inventory'))
        
        if 'document' not in request.files:
            flash('No file selected', 'danger')
            return redirect(url_for('inventory_routes.view_inventory', id=id))
        
        file = request.files['document']
        
        if file.filename == '':
            flash('No file selected', 'danger')
            return redirect(url_for('inventory_routes.view_inventory', id=id))
        
        if not file.filename or not allowed_file(file.filename):
            flash(f'File type not allowed. Allowed types: {", ".join(ALLOWED_EXTENSIONS)}', 'danger')
            return redirect(url_for('inventory_routes.view_inventory', id=id))
        
        original_filename = secure_filename(file.filename or 'document')
        file_ext = original_filename.rsplit('.', 1)[1].lower() if '.' in original_filename else ''
        unique_filename = f"{uuid.uuid4().hex}_{original_filename}"
        file_path = os.path.join(INVENTORY_UPLOAD_FOLDER, unique_filename)
        
        file.save(file_path)
        file_size = os.path.getsize(file_path)
        
        document_type = request.form.get('document_type', 'General')
        document_name = request.form.get('document_name', original_filename)
        description = request.form.get('description', '')
        
        mime_types = {
            'pdf': 'application/pdf',
            'doc': 'application/msword',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'xls': 'application/vnd.ms-excel',
            'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'png': 'image/png',
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'gif': 'image/gif',
            'txt': 'text/plain',
            'csv': 'text/csv'
        }
        mime_type = mime_types.get(file_ext, 'application/octet-stream')
        
        conn.execute('''
            INSERT INTO inventory_documents 
            (inventory_id, document_type, document_name, file_path, original_filename, file_size, mime_type, description, uploaded_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (id, document_type, document_name, file_path, original_filename, file_size, mime_type, description, session.get('user_id')))
        
        conn.commit()
        
        AuditLogger.log_change(conn, session.get('user_id'), 'inventory', str(id), 
                              'Document Upload', None, f"Uploaded: {document_name}")
        
        flash(f'Document "{document_name}" uploaded successfully', 'success')
        
    except Exception as e:
        flash(f'Error uploading document: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('inventory_routes.view_inventory', id=id))

@inventory_bp.route('/inventory/documents/<int:doc_id>/download')
@login_required
def download_inventory_document(doc_id):
    """Download an inventory document"""
    db = Database()
    conn = db.get_connection()
    
    try:
        doc = conn.execute('''
            SELECT * FROM inventory_documents WHERE id = ? AND is_active = 1
        ''', (doc_id,)).fetchone()
        
        if not doc:
            flash('Document not found', 'danger')
            return redirect(url_for('inventory_routes.list_inventory'))
        
        if os.path.exists(doc['file_path']):
            directory = os.path.dirname(doc['file_path'])
            filename = os.path.basename(doc['file_path'])
            return send_from_directory(
                directory, 
                filename, 
                as_attachment=True,
                download_name=doc['original_filename']
            )
        else:
            flash('File not found on server', 'danger')
            return redirect(url_for('inventory_routes.view_inventory', id=doc['inventory_id']))
            
    finally:
        conn.close()

@inventory_bp.route('/inventory/documents/<int:doc_id>/view')
@login_required
def view_inventory_document(doc_id):
    """View an inventory document inline (for preview)"""
    db = Database()
    conn = db.get_connection()
    
    try:
        doc = conn.execute('''
            SELECT * FROM inventory_documents WHERE id = ? AND is_active = 1
        ''', (doc_id,)).fetchone()
        
        if not doc:
            flash('Document not found', 'danger')
            return redirect(url_for('inventory_routes.list_inventory'))
        
        if os.path.exists(doc['file_path']):
            directory = os.path.dirname(doc['file_path'])
            filename = os.path.basename(doc['file_path'])
            return send_from_directory(
                directory, 
                filename, 
                as_attachment=False,
                mimetype=doc['mime_type']
            )
        else:
            flash('File not found on server', 'danger')
            return redirect(url_for('inventory_routes.view_inventory', id=doc['inventory_id']))
            
    finally:
        conn.close()

@inventory_bp.route('/inventory/documents/<int:doc_id>/delete', methods=['POST'])
@role_required('Admin', 'Production Staff')
def delete_inventory_document(doc_id):
    """Delete (deactivate) an inventory document"""
    db = Database()
    conn = db.get_connection()
    
    try:
        doc = conn.execute('SELECT * FROM inventory_documents WHERE id = ?', (doc_id,)).fetchone()
        
        if not doc:
            flash('Document not found', 'danger')
            return redirect(url_for('inventory_routes.list_inventory'))
        
        inventory_id = doc['inventory_id']
        
        conn.execute('UPDATE inventory_documents SET is_active = 0 WHERE id = ?', (doc_id,))
        conn.commit()
        
        AuditLogger.log_change(conn, session.get('user_id'), 'inventory', str(inventory_id), 
                              'Document Delete', doc['document_name'], 'Deleted')
        
        flash(f'Document "{doc["document_name"]}" deleted successfully', 'success')
        return redirect(url_for('inventory_routes.view_inventory', id=inventory_id))
        
    except Exception as e:
        flash(f'Error deleting document: {str(e)}', 'danger')
        return redirect(url_for('inventory_routes.list_inventory'))
    finally:
        conn.close()


@inventory_bp.route('/inventory/<int:id>/label')
@login_required
def generate_inventory_label(id):
    """Generate FAA-compliant inventory label PDF"""
    db = Database()
    conn = db.get_connection()
    
    try:
        inventory = conn.execute('''
            SELECT i.*, p.code, p.name, p.description as product_description, 
                   p.unit_of_measure
            FROM inventory i
            JOIN products p ON i.product_id = p.id
            WHERE i.id = ?
        ''', (id,)).fetchone()
        
        if not inventory:
            flash('Inventory record not found', 'danger')
            return redirect(url_for('inventory_routes.list_inventory'))
        
        related_po = conn.execute('''
            SELECT po.po_number, s.name as supplier_name
            FROM purchase_order_lines pol
            JOIN purchase_orders po ON pol.po_id = po.id
            LEFT JOIN suppliers s ON po.supplier_id = s.id
            WHERE pol.product_id = ?
            ORDER BY po.order_date DESC
            LIMIT 1
        ''', (inventory['product_id'],)).fetchone()
        
        related_wo = conn.execute('''
            SELECT wo.wo_number, c.name as customer_name
            FROM work_orders wo
            LEFT JOIN sales_orders so ON wo.sales_order_id = so.id
            LEFT JOIN customers c ON so.customer_id = c.id
            WHERE wo.inventory_id = ? OR wo.product_id = ?
            ORDER BY wo.created_at DESC
            LIMIT 1
        ''', (id, inventory['product_id'])).fetchone()
        
        related_ro = conn.execute('''
            SELECT ro.ro_number, s.name as supplier_name, c.name as customer_name
            FROM repair_orders ro
            LEFT JOIN suppliers s ON ro.supplier_id = s.id
            LEFT JOIN customers c ON ro.customer_id = c.id
            WHERE ro.inventory_id = ?
            ORDER BY ro.created_at DESC
            LIMIT 1
        ''', (id,)).fetchone()
        
        label_data = {
            'inventory': inventory,
            'related_po': related_po,
            'related_wo': related_wo,
            'related_ro': related_ro
        }
        
        buffer = io.BytesIO()
        label_size = request.args.get('size', '4x6')
        copies = int(request.args.get('copies', 1))
        
        if label_size == '4x6':
            page_width = 4 * inch
            page_height = 6 * inch
        elif label_size == '4x4':
            page_width = 4 * inch
            page_height = 4 * inch
        elif label_size == '2x4':
            page_width = 4 * inch
            page_height = 2 * inch
        else:
            page_width = 4 * inch
            page_height = 6 * inch
        
        c = canvas.Canvas(buffer, pagesize=(page_width, page_height))
        
        for copy_num in range(copies):
            if copy_num > 0:
                c.showPage()
            
            draw_faa_label(c, label_data, page_width, page_height)
        
        c.save()
        buffer.seek(0)
        
        part_number = inventory['code'] or 'UNKNOWN'
        serial = inventory['serial_number'] or 'NS'
        filename = f"FAA_Label_{part_number}_{serial}.pdf"
        
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


@inventory_bp.route('/inventory/mass-print-labels')
@login_required
def mass_print_labels():
    """Generate FAA-compliant labels for multiple inventory items"""
    ids_param = request.args.get('ids', '')
    label_size = request.args.get('size', '4x6')
    
    if not ids_param:
        flash('No inventory items selected', 'warning')
        return redirect(url_for('inventory_routes.list_inventory'))
    
    try:
        ids = [int(id.strip()) for id in ids_param.split(',') if id.strip().isdigit()]
    except ValueError:
        flash('Invalid inventory IDs provided', 'danger')
        return redirect(url_for('inventory_routes.list_inventory'))
    
    if not ids:
        flash('No valid inventory items selected', 'warning')
        return redirect(url_for('inventory_routes.list_inventory'))
    
    db = Database()
    conn = db.get_connection()
    
    try:
        if label_size == '4x6':
            page_width = 4 * inch
            page_height = 6 * inch
        elif label_size == '4x4':
            page_width = 4 * inch
            page_height = 4 * inch
        elif label_size == '2x4':
            page_width = 4 * inch
            page_height = 2 * inch
        else:
            page_width = 4 * inch
            page_height = 6 * inch
        
        buffer = io.BytesIO()
        c = canvas.Canvas(buffer, pagesize=(page_width, page_height))
        
        labels_generated = 0
        
        for inv_id in ids:
            inventory = conn.execute('''
                SELECT i.*, p.code, p.name, p.description as product_description, 
                       p.unit_of_measure
                FROM inventory i
                JOIN products p ON i.product_id = p.id
                WHERE i.id = ?
            ''', (inv_id,)).fetchone()
            
            if not inventory:
                continue
            
            related_po = conn.execute('''
                SELECT po.po_number, s.name as supplier_name
                FROM purchase_order_lines pol
                JOIN purchase_orders po ON pol.po_id = po.id
                LEFT JOIN suppliers s ON po.supplier_id = s.id
                WHERE pol.product_id = ?
                ORDER BY po.order_date DESC
                LIMIT 1
            ''', (inventory['product_id'],)).fetchone()
            
            related_wo = conn.execute('''
                SELECT wo.wo_number, c.name as customer_name
                FROM work_orders wo
                LEFT JOIN sales_orders so ON wo.sales_order_id = so.id
                LEFT JOIN customers c ON so.customer_id = c.id
                WHERE wo.inventory_id = ? OR wo.product_id = ?
                ORDER BY wo.created_at DESC
                LIMIT 1
            ''', (inv_id, inventory['product_id'])).fetchone()
            
            related_ro = conn.execute('''
                SELECT ro.ro_number, s.name as supplier_name, c.name as customer_name
                FROM repair_orders ro
                LEFT JOIN suppliers s ON ro.supplier_id = s.id
                LEFT JOIN customers c ON ro.customer_id = c.id
                WHERE ro.inventory_id = ?
                ORDER BY ro.created_at DESC
                LIMIT 1
            ''', (inv_id,)).fetchone()
            
            label_data = {
                'inventory': inventory,
                'related_po': related_po,
                'related_wo': related_wo,
                'related_ro': related_ro
            }
            
            if labels_generated > 0:
                c.showPage()
            
            draw_faa_label(c, label_data, page_width, page_height)
            labels_generated += 1
        
        c.save()
        buffer.seek(0)
        
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"FAA_Labels_Batch_{timestamp}.pdf"
        
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


def draw_faa_label(c, label_data, width, height):
    """Draw FAA-compliant label content on canvas - size-aware"""
    inventory = label_data['inventory']
    related_po = label_data.get('related_po')
    related_wo = label_data.get('related_wo')
    related_ro = label_data.get('related_ro')
    
    is_small = height <= 2.5 * inch
    is_medium = height <= 4.5 * inch and not is_small
    
    margin = 0.12 * inch
    col1_x = margin
    col2_x = width / 2
    
    c.setStrokeColor(colors.black)
    c.setLineWidth(2)
    c.rect(margin/2, margin/2, width - margin, height - margin)
    
    y = height - margin - 5
    
    if is_small:
        c.setFont("Helvetica-Bold", 7)
        c.drawString(margin, y, "FAA PARTS ID")
    else:
        c.setFont("Helvetica-Bold", 9)
        c.drawString(margin, y, "FAA COMPLIANT PARTS IDENTIFICATION")
    y -= 10
    
    c.setStrokeColor(colors.black)
    c.setLineWidth(1)
    c.line(margin, y, width - margin, y)
    y -= 12
    
    part_number = inventory['code'] or 'N/A'
    c.setFont("Helvetica-Bold", 7)
    c.drawString(margin, y, "PART NUMBER:")
    c.setFont("Helvetica-Bold", 12 if is_small else 14)
    c.drawString(margin + 72, y, str(part_number))
    y -= 16 if is_small else 18
    
    if part_number and part_number != 'N/A' and len(part_number) <= 20:
        try:
            bh = 18 if is_small else 24
            bw = 0.9 if is_small else 1.1
            barcode = code128.Code128(part_number, barHeight=bh, barWidth=bw)
            barcode.drawOn(c, margin, y - bh - 3)
            y -= bh + 8
        except:
            pass
    
    c.setFont("Helvetica-Bold", 6)
    c.drawString(margin, y, "DESC:")
    c.setFont("Helvetica", 6)
    desc = inventory['name'] or inventory['product_description'] or 'N/A'
    max_len = 35 if is_small else 48
    if len(desc) > max_len:
        desc = desc[:max_len-3] + '...'
    c.drawString(margin + 28, y, desc)
    y -= 10
    
    if inventory['is_serialized'] and inventory['serial_number']:
        c.setFont("Helvetica-Bold", 6)
        c.drawString(margin, y, "S/N:")
        c.setFont("Helvetica-Bold", 9 if is_small else 11)
        c.drawString(margin + 22, y, str(inventory['serial_number']))
        y -= 12
        
        if not is_small:
            try:
                sn_barcode = code128.Code128(str(inventory['serial_number']), barHeight=16, barWidth=0.85)
                sn_barcode.drawOn(c, margin, y - 18)
                y -= 22
            except:
                pass
    
    c.setFont("Helvetica-Bold", 6)
    c.drawString(col1_x, y, "QTY:")
    c.setFont("Helvetica-Bold", 9)
    qty = inventory['quantity'] or 0
    uom = inventory['unit_of_measure'] or 'EA'
    c.drawString(col1_x + 20, y, f"{qty} {uom}")
    
    if inventory['condition']:
        c.setFont("Helvetica-Bold", 6)
        c.drawString(col2_x, y, "COND:")
        c.setFont("Helvetica-Bold", 8)
        cond = str(inventory['condition']).upper()
        if cond in ['NEW', 'NE']:
            c.setFillColor(colors.darkgreen)
        elif cond in ['SERVICEABLE', 'SV', 'OVERHAULED', 'OH']:
            c.setFillColor(colors.darkblue)
        else:
            c.setFillColor(colors.black)
        c.drawString(col2_x + 28, y, cond)
        c.setFillColor(colors.black)
    y -= 10
    
    if is_small:
        c.setFont("Helvetica", 5)
        c.drawString(margin, y, f"14 CFR 45 | ID:{inventory['id']} | {datetime.now().strftime('%m/%d/%y')}")
        return
    
    c.setFont("Helvetica-Bold", 6)
    c.drawString(col1_x, y, "LOC:")
    c.setFont("Helvetica", 6)
    loc = f"{inventory['warehouse_location'] or 'N/A'} / {inventory['bin_location'] or 'N/A'}"
    if len(loc) > 25:
        loc = loc[:22] + '...'
    c.drawString(col1_x + 22, y, loc)
    
    if inventory['expiration_date']:
        c.setFont("Helvetica-Bold", 6)
        c.drawString(col2_x, y, "EXP:")
        c.setFont("Helvetica-Bold", 7)
        c.setFillColor(colors.red)
        exp_date = str(inventory['expiration_date'])[:10]
        c.drawString(col2_x + 22, y, exp_date)
        c.setFillColor(colors.black)
    y -= 10
    
    if inventory.get('category'):
        c.setFont("Helvetica-Bold", 6)
        c.drawString(col1_x, y, "CAT:")
        c.setFont("Helvetica", 6)
        cat = str(inventory['category'])
        if len(cat) > 20:
            cat = cat[:17] + '...'
        c.drawString(col1_x + 22, y, cat)
        y -= 9
    
    if inventory['lot_number']:
        c.setFont("Helvetica-Bold", 6)
        c.drawString(col1_x, y, "LOT:")
        c.setFont("Helvetica", 6)
        c.drawString(col1_x + 22, y, str(inventory['lot_number']))
        y -= 9
    
    if inventory['trace_tag'] or inventory['trace']:
        c.setFont("Helvetica-Bold", 6)
        c.drawString(col1_x, y, "TRACE:")
        c.setFont("Helvetica", 6)
        trace_val = inventory['trace_tag'] or inventory['trace'] or ''
        if len(trace_val) > 25:
            trace_val = trace_val[:22] + '...'
        c.drawString(col1_x + 35, y, trace_val)
        if inventory['trace_type']:
            c.setFont("Helvetica", 5)
            c.drawString(col2_x + 20, y, f"({inventory['trace_type']})")
        y -= 9
    
    c.setStrokeColor(colors.black)
    c.setLineWidth(0.5)
    c.line(margin, y, width - margin, y)
    y -= 8
    
    if inventory['mfr_code'] or inventory['msn_esn']:
        c.setFont("Helvetica-Bold", 6)
        info_parts = []
        if inventory['mfr_code']:
            info_parts.append(f"MFR:{inventory['mfr_code']}")
        if inventory['msn_esn']:
            info_parts.append(f"MSN:{inventory['msn_esn']}")
        if inventory['country_of_origin']:
            info_parts.append(f"ORIGIN:{inventory['country_of_origin']}")
        c.setFont("Helvetica", 6)
        c.drawString(margin, y, " | ".join(info_parts))
        y -= 9
    
    has_lifecycle = any([inventory.get('tsn'), inventory.get('tso'), inventory.get('csn'), inventory.get('cso')])
    if has_lifecycle:
        lifecycle_items = []
        if inventory.get('tsn'):
            lifecycle_items.append(f"TSN:{inventory['tsn']}")
        if inventory.get('tso'):
            lifecycle_items.append(f"TSO:{inventory['tso']}")
        if inventory.get('csn'):
            lifecycle_items.append(f"CSN:{inventory['csn']}")
        if inventory.get('cso'):
            lifecycle_items.append(f"CSO:{inventory['cso']}")
        c.setFont("Helvetica", 5)
        c.drawString(margin, y, " | ".join(lifecycle_items[:4]))
        y -= 8
    
    if inventory.get('manufactured_date'):
        c.setFont("Helvetica-Bold", 6)
        c.drawString(col1_x, y, "MFG:")
        c.setFont("Helvetica", 6)
        mfg_date = str(inventory['manufactured_date'])[:10]
        c.drawString(col1_x + 25, y, mfg_date)
        y -= 9
    
    if not is_medium:
        c.setStrokeColor(colors.black)
        c.setLineWidth(0.5)
        c.line(margin, y, width - margin, y)
        y -= 8
        c.setFont("Helvetica-Bold", 6)
        c.drawString(margin, y, "SOURCE DOCS:")
        y -= 9
        
        if related_po:
            c.setFont("Helvetica", 5)
            po_text = f"PO: {related_po['po_number']}"
            if related_po.get('supplier_name'):
                supplier = related_po['supplier_name'][:15]
                po_text += f" ({supplier})"
            c.drawString(col1_x, y, po_text)
            y -= 8
        
        if related_wo:
            c.setFont("Helvetica", 5)
            wo_text = f"WO: {related_wo['wo_number']}"
            if related_wo.get('customer_name'):
                customer = related_wo['customer_name'][:15]
                wo_text += f" ({customer})"
            c.drawString(col1_x, y, wo_text)
            y -= 8
        
        if related_ro:
            c.setFont("Helvetica", 5)
            ro_text = f"RO: {related_ro['ro_number']}"
            party = (related_ro.get('supplier_name') or related_ro.get('customer_name') or '')[:15]
            if party:
                ro_text += f" ({party})"
            c.drawString(col1_x, y, ro_text)
            y -= 8
        
        if not related_po and not related_wo and not related_ro:
            c.setFont("Helvetica", 5)
            c.drawString(col1_x, y, "No linked documents")
            y -= 8
    
    c.setStrokeColor(colors.black)
    c.setLineWidth(0.5)
    c.line(margin, y, width - margin, y)
    y -= 7
    
    c.setFont("Helvetica", 5)
    c.drawString(margin, y, f"14 CFR Part 45 | INV:{inventory['id']} | {datetime.now().strftime('%m/%d/%Y %I:%M %p')}")
