from flask import Blueprint, render_template, request, redirect, url_for, flash, session, send_file
from models import Database, AuditLogger
from auth import login_required, role_required
from datetime import datetime
import io
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.enums import TA_CENTER, TA_RIGHT

quote_bp = Blueprint('quote_routes', __name__)

@quote_bp.route('/quotes')
@login_required
def list_quotes():
    db = Database()
    conn = db.get_connection()
    
    quotes = conn.execute('''
        SELECT q.*, wo.wo_number, u.username as prepared_by_name
        FROM work_order_quotes q
        LEFT JOIN work_orders wo ON q.work_order_id = wo.id
        LEFT JOIN users u ON q.prepared_by = u.id
        ORDER BY q.created_at DESC
    ''').fetchall()
    
    conn.close()
    return render_template('quotes/list.html', quotes=quotes)

@quote_bp.route('/quotes/<int:id>')
@login_required
def view_quote(id):
    db = Database()
    conn = db.get_connection()
    
    quote = conn.execute('''
        SELECT q.*, wo.wo_number, wo.quantity as wo_quantity,
               p.code as product_code, p.name as product_name,
               prep.username as prepared_by_name,
               appr.username as approved_by_name
        FROM work_order_quotes q
        LEFT JOIN work_orders wo ON q.work_order_id = wo.id
        LEFT JOIN products p ON wo.product_id = p.id
        LEFT JOIN users prep ON q.prepared_by = prep.id
        LEFT JOIN users appr ON q.approved_by = appr.id
        WHERE q.id = ?
    ''', (id,)).fetchone()
    
    if not quote:
        flash('Quote not found.', 'danger')
        conn.close()
        return redirect(url_for('quote_routes.list_quotes'))
    
    quote_lines = conn.execute('''
        SELECT ql.*, p.code as product_code, p.name as product_name
        FROM work_order_quote_lines ql
        LEFT JOIN products p ON ql.product_id = p.id
        WHERE ql.quote_id = ?
        ORDER BY ql.sequence_number, ql.id
    ''', (id,)).fetchall()
    
    conn.close()
    return render_template('quotes/view.html', quote=quote, quote_lines=quote_lines)

@quote_bp.route('/workorders/<int:wo_id>/generate-quote', methods=['GET', 'POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def generate_quote(wo_id):
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            # Generate quote number
            last_quote = conn.execute('''
                SELECT quote_number FROM work_order_quotes 
                WHERE quote_number LIKE 'QT-%'
                ORDER BY CAST(SUBSTR(quote_number, 4) AS INTEGER) DESC 
                LIMIT 1
            ''').fetchone()
            
            if last_quote:
                last_number = int(last_quote['quote_number'].split('-')[1])
                next_number = last_number + 1
            else:
                next_number = 1
            
            quote_number = f'QT-{next_number:06d}'
            
            # Get markup percentage and additional amounts from form
            markup_percent = float(request.form.get('markup_percent', 0))
            labor_amount = float(request.form.get('labor_amount', 0))
            consumables_amount = float(request.form.get('consumables_amount', 0))
            other_fees_amount = float(request.form.get('other_fees_amount', 0))
            
            # Create quote header with all fields
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO work_order_quotes (
                    quote_number, work_order_id, customer_name, customer_account,
                    description, scope_of_work, estimated_turnaround_days,
                    assigned_to, department, status, subtotal, tax_rate, tax_amount, 
                    total_amount, markup_percent, labor_amount, consumables_amount, 
                    other_fees_amount, notes, prepared_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                quote_number,
                wo_id,
                request.form.get('customer_name', ''),
                request.form.get('customer_account', ''),
                request.form.get('description', ''),
                request.form.get('scope_of_work', ''),
                int(request.form.get('estimated_turnaround_days', 0)) if request.form.get('estimated_turnaround_days') else None,
                request.form.get('assigned_to', ''),
                request.form.get('department', ''),
                'Draft',
                0,  # subtotal - will be calculated from lines
                0,  # tax_rate - from form
                0,  # tax_amount - will be calculated
                0,  # total_amount - will be calculated
                markup_percent,
                labor_amount,
                consumables_amount,
                other_fees_amount,
                request.form.get('notes', ''),
                session['user_id']
            ))
            
            quote_id = cursor.lastrowid
            
            # Create quote lines from form data and calculate totals server-side
            line_types = request.form.getlist('line_type[]')
            descriptions = request.form.getlist('line_description[]')
            quantities = request.form.getlist('line_quantity[]')
            prices = request.form.getlist('line_price[]')
            
            subtotal = 0
            for i, line_type in enumerate(line_types):
                if descriptions[i]:  # Only add lines with descriptions
                    quantity = float(quantities[i]) if quantities[i] else 1
                    unit_price = float(prices[i]) if prices[i] else 0
                    base_amount = quantity * unit_price
                    
                    # Apply markup to get line total
                    markup_amount = base_amount * (markup_percent / 100)
                    line_total = base_amount + markup_amount
                    subtotal += line_total
                    
                    cursor.execute('''
                        INSERT INTO work_order_quote_lines (
                            quote_id, line_type, description, quantity, 
                            unit_price, line_total, sequence_number
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (quote_id, line_type, descriptions[i], quantity, unit_price, line_total, i))
            
            # Calculate tax and total server-side (don't trust client values)
            # Total = Parts Subtotal + Labor + Consumables + Other Fees + Tax
            tax_rate = float(request.form.get('tax_rate', 0))
            pre_tax_total = subtotal + labor_amount + consumables_amount + other_fees_amount
            tax_amount = pre_tax_total * (tax_rate / 100)
            total_amount = pre_tax_total + tax_amount
            
            # Update quote with calculated values
            cursor.execute('''
                UPDATE work_order_quotes 
                SET subtotal = ?, tax_rate = ?, tax_amount = ?, total_amount = ?, 
                    markup_percent = ?, labor_amount = ?, consumables_amount = ?, other_fees_amount = ?
                WHERE id = ?
            ''', (subtotal, tax_rate, tax_amount, total_amount, markup_percent, 
                  labor_amount, consumables_amount, other_fees_amount, quote_id))
            
            # Log activity
            AuditLogger.log_change(
                conn=conn,
                record_type='work_order_quote',
                record_id=quote_id,
                action_type='Created',
                modified_by=session['user_id'],
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
            
            conn.commit()
            conn.close()
            
            flash(f'Quote {quote_number} created successfully!', 'success')
            return redirect(url_for('quote_routes.view_quote', id=quote_id))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error creating quote: {str(e)}', 'danger')
            return redirect(url_for('workorder_routes.view_workorder', id=wo_id))
    
    # GET request - show quote generation form
    work_order = conn.execute('''
        SELECT wo.*, p.code, p.name, p.cost
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        WHERE wo.id = ?
    ''', (wo_id,)).fetchone()
    
    if not work_order:
        flash('Work Order not found.', 'danger')
        conn.close()
        return redirect(url_for('workorder_routes.list_workorders'))
    
    # Get work order level material requirements
    wo_materials = conn.execute('''
        SELECT mr.required_quantity, p.code, p.name, p.cost
        FROM material_requirements mr
        JOIN products p ON mr.product_id = p.id
        WHERE mr.work_order_id = ?
        ORDER BY p.code
    ''', (wo_id,)).fetchall()
    
    # Get task level materials
    task_materials = conn.execute('''
        SELECT tm.required_qty as required_quantity, p.code, p.name, p.cost,
               wot.task_number, wot.task_name
        FROM work_order_task_materials tm
        JOIN work_order_tasks wot ON tm.task_id = wot.id
        JOIN products p ON tm.product_id = p.id
        WHERE wot.work_order_id = ?
        ORDER BY wot.sequence_number, p.code
    ''', (wo_id,)).fetchall()
    
    # Combine both material lists
    materials = list(wo_materials) + list(task_materials)
    
    # Get task discrepancies
    task_discrepancies = conn.execute('''
        SELECT task_number, task_name, discrepancies, corrective_actions
        FROM work_order_tasks
        WHERE work_order_id = ? AND discrepancies IS NOT NULL AND discrepancies != ''
        ORDER BY sequence_number, id
    ''', (wo_id,)).fetchall()
    
    conn.close()
    
    return render_template('quotes/generate.html', 
                         work_order=work_order, 
                         materials=materials,
                         task_discrepancies=task_discrepancies)

@quote_bp.route('/quotes/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Planner', 'Production Staff')
def edit_quote(id):
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            # Get old record for audit
            old_record = conn.execute('SELECT * FROM work_order_quotes WHERE id=?', (id,)).fetchone()
            
            # Delete existing lines and recreate
            conn.execute('DELETE FROM work_order_quote_lines WHERE quote_id = ?', (id,))
            
            # Get markup percentage and additional amounts from form
            markup_percent = float(request.form.get('markup_percent', 0))
            labor_amount = float(request.form.get('labor_amount', 0))
            consumables_amount = float(request.form.get('consumables_amount', 0))
            other_fees_amount = float(request.form.get('other_fees_amount', 0))
            
            # Create new lines and calculate totals server-side
            line_types = request.form.getlist('line_type[]')
            descriptions = request.form.getlist('line_description[]')
            quantities = request.form.getlist('line_quantity[]')
            prices = request.form.getlist('line_price[]')
            
            subtotal = 0
            cursor = conn.cursor()
            for i, line_type in enumerate(line_types):
                if descriptions[i]:
                    quantity = float(quantities[i]) if quantities[i] else 1
                    unit_price = float(prices[i]) if prices[i] else 0
                    base_amount = quantity * unit_price
                    
                    # Apply markup to get line total
                    markup_amount = base_amount * (markup_percent / 100)
                    line_total = base_amount + markup_amount
                    subtotal += line_total
                    
                    cursor.execute('''
                        INSERT INTO work_order_quote_lines (
                            quote_id, line_type, description, quantity, 
                            unit_price, line_total, sequence_number
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (id, line_type, descriptions[i], quantity, unit_price, line_total, i))
            
            # Calculate tax and total server-side (don't trust client values)
            # Total = Parts Subtotal + Labor + Consumables + Other Fees + Tax
            tax_rate = float(request.form.get('tax_rate', 0))
            pre_tax_total = subtotal + labor_amount + consumables_amount + other_fees_amount
            tax_amount = pre_tax_total * (tax_rate / 100)
            total_amount = pre_tax_total + tax_amount
            
            # Update quote header with calculated values
            conn.execute('''
                UPDATE work_order_quotes 
                SET customer_name = ?, customer_account = ?, description = ?,
                    scope_of_work = ?, estimated_turnaround_days = ?,
                    assigned_to = ?, department = ?, subtotal = ?, 
                    tax_rate = ?, tax_amount = ?, total_amount = ?,
                    markup_percent = ?, labor_amount = ?, consumables_amount = ?, 
                    other_fees_amount = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (
                request.form.get('customer_name', ''),
                request.form.get('customer_account', ''),
                request.form.get('description', ''),
                request.form.get('scope_of_work', ''),
                int(request.form.get('estimated_turnaround_days', 0)) if request.form.get('estimated_turnaround_days') else None,
                request.form.get('assigned_to', ''),
                request.form.get('department', ''),
                subtotal,
                tax_rate,
                tax_amount,
                total_amount,
                markup_percent,
                labor_amount,
                consumables_amount,
                other_fees_amount,
                request.form.get('notes', ''),
                id
            ))
            
            # Log audit
            new_record = conn.execute('SELECT * FROM work_order_quotes WHERE id=?', (id,)).fetchone()
            changes = AuditLogger.compare_records(dict(old_record), dict(new_record))
            if changes:
                AuditLogger.log_change(
                    conn=conn,
                    record_type='work_order_quote',
                    record_id=id,
                    action_type='Updated',
                    modified_by=session['user_id'],
                    changed_fields=changes,
                    ip_address=request.remote_addr,
                    user_agent=request.headers.get('User-Agent')
                )
            
            conn.commit()
            conn.close()
            
            flash('Quote updated successfully!', 'success')
            return redirect(url_for('quote_routes.view_quote', id=id))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error updating quote: {str(e)}', 'danger')
            return redirect(url_for('quote_routes.edit_quote', id=id))
    
    # GET request
    quote = conn.execute('SELECT * FROM work_order_quotes WHERE id = ?', (id,)).fetchone()
    
    if not quote:
        flash('Quote not found.', 'danger')
        conn.close()
        return redirect(url_for('quote_routes.list_quotes'))
    
    quote_lines = conn.execute('''
        SELECT * FROM work_order_quote_lines 
        WHERE quote_id = ?
        ORDER BY sequence_number
    ''', (id,)).fetchall()
    
    conn.close()
    
    return render_template('quotes/edit.html', quote=quote, quote_lines=quote_lines)

@quote_bp.route('/quotes/<int:id>/update-status', methods=['POST'])
@role_required('Admin', 'Planner')
def update_quote_status(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        new_status = request.form['status']
        
        # If approving, record approved_by
        if new_status == 'Approved':
            conn.execute('''
                UPDATE work_order_quotes 
                SET status = ?, approved_by = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (new_status, session['user_id'], id))
        else:
            conn.execute('''
                UPDATE work_order_quotes 
                SET status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (new_status, id))
        
        # Log audit
        AuditLogger.log_change(
            conn=conn,
            record_type='work_order_quote',
            record_id=id,
            action_type='Status Updated',
            modified_by=session['user_id'],
            changed_fields={'status': new_status},
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        conn.commit()
        conn.close()
        
        flash(f'Quote status updated to {new_status}!', 'success')
        
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error updating status: {str(e)}', 'danger')
    
    return redirect(url_for('quote_routes.view_quote', id=id))

@quote_bp.route('/quotes/<int:id>/pdf')
@login_required
def generate_pdf(id):
    db = Database()
    conn = db.get_connection()
    
    quote = conn.execute('''
        SELECT q.*, wo.wo_number, p.code as product_code, p.name as product_name
        FROM work_order_quotes q
        LEFT JOIN work_orders wo ON q.work_order_id = wo.id
        LEFT JOIN products p ON wo.product_id = p.id
        WHERE q.id = ?
    ''', (id,)).fetchone()
    
    if not quote:
        flash('Quote not found.', 'danger')
        conn.close()
        return redirect(url_for('quote_routes.list_quotes'))
    
    quote_lines = conn.execute('''
        SELECT * FROM work_order_quote_lines 
        WHERE quote_id = ?
        ORDER BY sequence_number
    ''', (id,)).fetchall()
    
    conn.close()
    
    # Create PDF
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
    story = []
    styles = getSampleStyleSheet()
    
    # Title
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#1e3a8a'),
        spaceAfter=30,
        alignment=TA_CENTER
    )
    story.append(Paragraph("WORK ORDER QUOTE", title_style))
    story.append(Spacer(1, 0.2*inch))
    
    # Quote info
    info_data = [
        ['Quote Number:', quote['quote_number'], 'Date:', quote['created_at'][:10]],
        ['Work Order:', quote['wo_number'] or 'N/A', 'Status:', quote['status']],
        ['Customer:', quote['customer_name'] or 'N/A', 'Account:', quote['customer_account'] or 'N/A'],
    ]
    
    info_table = Table(info_data, colWidths=[1.5*inch, 2.5*inch, 1*inch, 2*inch])
    info_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))
    story.append(info_table)
    story.append(Spacer(1, 0.3*inch))
    
    # Description and scope
    if quote['description']:
        story.append(Paragraph(f"<b>Description:</b> {quote['description']}", styles['Normal']))
        story.append(Spacer(1, 0.1*inch))
    
    if quote['scope_of_work']:
        story.append(Paragraph(f"<b>Scope of Work:</b> {quote['scope_of_work']}", styles['Normal']))
        story.append(Spacer(1, 0.2*inch))
    
    # Line items table
    line_data = [['Type', 'Description', 'Qty', 'Unit Price', 'Total']]
    for line in quote_lines:
        line_data.append([
            line['line_type'],
            line['description'],
            str(line['quantity']),
            f"${line['unit_price']:,.2f}",
            f"${line['line_total']:,.2f}"
        ])
    
    lines_table = Table(line_data, colWidths=[1*inch, 3.5*inch, 0.7*inch, 1.2*inch, 1.2*inch])
    lines_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4a90e2')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.white),
        ('GRID', (0, 0), (-1, -1), 1, colors.grey),
    ]))
    story.append(lines_table)
    story.append(Spacer(1, 0.2*inch))
    
    # Totals
    totals_data = []
    
    # Add markup note if applicable
    if quote['markup_percent'] and quote['markup_percent'] > 0:
        totals_data.append(['', '', '', f"Markup ({quote['markup_percent']}%) Applied to Parts:", ''])
    
    totals_data.append(['', '', '', 'Parts Subtotal:', f"${quote['subtotal']:,.2f}"])
    
    # Add labor, consumables, other fees if present
    if quote['labor_amount'] and quote['labor_amount'] > 0:
        totals_data.append(['', '', '', 'Labor:', f"${quote['labor_amount']:,.2f}"])
    
    if quote['consumables_amount'] and quote['consumables_amount'] > 0:
        totals_data.append(['', '', '', 'Consumables:', f"${quote['consumables_amount']:,.2f}"])
    
    if quote['other_fees_amount'] and quote['other_fees_amount'] > 0:
        totals_data.append(['', '', '', 'Other Fees:', f"${quote['other_fees_amount']:,.2f}"])
    
    totals_data.extend([
        ['', '', '', f"Tax ({quote['tax_rate']}%):", f"${quote['tax_amount']:,.2f}"],
        ['', '', '', 'Total:', f"${quote['total_amount']:,.2f}"],
    ])
    
    totals_table = Table(totals_data, colWidths=[1*inch, 3.5*inch, 0.7*inch, 1.2*inch, 1.2*inch])
    totals_table.setStyle(TableStyle([
        ('ALIGN', (3, 0), (-1, -1), 'RIGHT'),
        ('FONTNAME', (3, 0), (3, -1), 'Helvetica-Bold'),
        ('FONTNAME', (3, 2), (-1, 2), 'Helvetica-Bold'),
        ('FONTSIZE', (3, 2), (-1, 2), 12),
        ('LINEABOVE', (3, 2), (-1, 2), 2, colors.black),
    ]))
    story.append(totals_table)
    
    # Notes
    if quote['notes']:
        story.append(Spacer(1, 0.3*inch))
        story.append(Paragraph(f"<b>Notes:</b> {quote['notes']}", styles['Normal']))
    
    # Build PDF
    doc.build(story)
    buffer.seek(0)
    
    return send_file(
        buffer,
        mimetype='application/pdf',
        as_attachment=True,
        download_name=f"Quote_{quote['quote_number']}.pdf"
    )
