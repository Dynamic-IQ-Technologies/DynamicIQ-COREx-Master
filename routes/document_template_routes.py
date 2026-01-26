from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from functools import wraps
from models import Database, AuditLogger
import json
from datetime import datetime

document_template_bp = Blueprint('document_template_routes', __name__)

def role_required(*roles):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user_id' not in session:
                flash('Please log in to access this page.', 'warning')
                return redirect(url_for('login'))
            if session.get('role') not in roles:
                flash('You do not have permission to access this page.', 'danger')
                return redirect(url_for('dashboard'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

DOCUMENT_TYPES = [
    ('work_order', 'Work Order'),
    ('quote', 'Quote / Estimate'),
    ('sales_order', 'Sales Order'),
    ('invoice', 'Invoice'),
    ('purchase_order', 'Purchase Order'),
    ('packing_slip', 'Packing Slip'),
    ('rma', 'RMA / Warranty Document'),
    ('certificate', 'Certificate (CoC, Compliance)'),
    ('rfq', 'Request for Quote'),
    ('receiving', 'Receiving Document')
]

TERM_CATEGORIES = [
    ('payment', 'Payment Terms'),
    ('warranty', 'Warranty'),
    ('shipping', 'Shipping Terms'),
    ('tax', 'Tax Notes'),
    ('returns', 'Return Policy'),
    ('legal', 'Legal Disclaimer'),
    ('custom', 'Custom')
]

@document_template_bp.route('/document-templates')
@role_required('Admin', 'Editor', 'Viewer', 'Procurement', 'Sales', 'Accountant')
def list_templates():
    db = Database()
    conn = db.get_connection()
    
    filter_type = request.args.get('type', '')
    filter_status = request.args.get('status', '')
    
    query = '''
        SELECT dt.*, u.username as created_by_name,
               (SELECT COUNT(*) FROM template_assignments WHERE template_id = dt.id) as assignment_count
        FROM document_templates dt
        LEFT JOIN users u ON dt.created_by = u.id
        WHERE 1=1
    '''
    params = []
    
    if filter_type:
        query += ' AND dt.document_type = ?'
        params.append(filter_type)
    
    if filter_status:
        query += ' AND dt.status = ?'
        params.append(filter_status)
    
    query += ' ORDER BY dt.document_type, dt.is_default DESC, dt.name'
    
    templates = conn.execute(query, params).fetchall()
    conn.close()
    
    return render_template('document_templates/list.html', 
                          templates=templates,
                          document_types=DOCUMENT_TYPES,
                          filter_type=filter_type,
                          filter_status=filter_status)

@document_template_bp.route('/document-templates/create', methods=['GET', 'POST'])
@role_required('Admin')
def create_template():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            template_code = request.form.get('template_code', '').strip().upper()
            name = request.form.get('name', '').strip()
            document_type = request.form.get('document_type', '')
            description = request.form.get('description', '').strip()
            
            if not template_code or not name or not document_type:
                flash('Template code, name, and document type are required.', 'danger')
                conn.close()
                return redirect(url_for('document_template_routes.create_template'))
            
            existing = conn.execute('SELECT id FROM document_templates WHERE template_code = ?', (template_code,)).fetchone()
            if existing:
                flash('A template with this code already exists.', 'danger')
                conn.close()
                return redirect(url_for('document_template_routes.create_template'))
            
            cursor = conn.execute('''
                INSERT INTO document_templates (template_code, name, document_type, description, status, created_by)
                VALUES (?, ?, ?, ?, 'Draft', ?)
            ''', (template_code, name, document_type, description, session.get('user_id')))
            
            template_id = cursor.lastrowid
            
            conn.execute('''
                INSERT INTO template_headers (template_id, company_name)
                VALUES (?, 'Your Company Name')
            ''', (template_id,))
            
            conn.execute('''
                INSERT INTO template_footers (template_id)
                VALUES (?)
            ''', (template_id,))
            
            AuditLogger.log_change(conn, 'document_templates', template_id, 'CREATE', session.get('user_id'),
                                  {'template_code': template_code, 'name': name, 'document_type': document_type})
            
            conn.commit()
            flash('Document template created successfully!', 'success')
            conn.close()
            return redirect(url_for('document_template_routes.edit_template', id=template_id))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error creating template: {str(e)}', 'danger')
            conn.close()
            return redirect(url_for('document_template_routes.create_template'))
    
    conn.close()
    return render_template('document_templates/create.html', document_types=DOCUMENT_TYPES)

@document_template_bp.route('/document-templates/<int:id>')
@role_required('Admin', 'Editor', 'Viewer', 'Procurement', 'Sales', 'Accountant')
def view_template(id):
    db = Database()
    conn = db.get_connection()
    
    template = conn.execute('''
        SELECT dt.*, u.username as created_by_name, u2.username as updated_by_name
        FROM document_templates dt
        LEFT JOIN users u ON dt.created_by = u.id
        LEFT JOIN users u2 ON dt.updated_by = u2.id
        WHERE dt.id = ?
    ''', (id,)).fetchone()
    
    if not template:
        flash('Template not found.', 'danger')
        conn.close()
        return redirect(url_for('document_template_routes.list_templates'))
    
    header = conn.execute('SELECT * FROM template_headers WHERE template_id = ?', (id,)).fetchone()
    footer = conn.execute('SELECT * FROM template_footers WHERE template_id = ?', (id,)).fetchone()
    sections = conn.execute('SELECT * FROM template_sections WHERE template_id = ? ORDER BY display_order', (id,)).fetchall()
    
    terms = conn.execute('''
        SELECT tt.*, tl.term_code, tl.name, tl.category, tl.content as original_content
        FROM template_terms tt
        JOIN template_terms_library tl ON tt.term_id = tl.id
        WHERE tt.template_id = ?
        ORDER BY tt.display_order
    ''', (id,)).fetchall()
    
    assignments = conn.execute('''
        SELECT ta.*, 
               CASE ta.assignment_type 
                   WHEN 'customer' THEN c.name
                   WHEN 'supplier' THEN s.name
                   ELSE ta.assignment_type
               END as assignment_name
        FROM template_assignments ta
        LEFT JOIN customers c ON ta.assignment_type = 'customer' AND ta.assignment_id = c.id
        LEFT JOIN suppliers s ON ta.assignment_type = 'supplier' AND ta.assignment_id = s.id
        WHERE ta.template_id = ?
    ''', (id,)).fetchall()
    
    versions = conn.execute('''
        SELECT tv.*, u.username as changed_by_name
        FROM template_versions tv
        LEFT JOIN users u ON tv.changed_by = u.id
        WHERE tv.template_id = ?
        ORDER BY tv.version_number DESC
        LIMIT 10
    ''', (id,)).fetchall()
    
    conn.close()
    
    doc_type_name = next((dt[1] for dt in DOCUMENT_TYPES if dt[0] == template['document_type']), template['document_type'])
    
    return render_template('document_templates/view.html',
                          template=template,
                          header=header,
                          footer=footer,
                          sections=sections,
                          terms=terms,
                          assignments=assignments,
                          versions=versions,
                          doc_type_name=doc_type_name,
                          document_types=DOCUMENT_TYPES)

@document_template_bp.route('/document-templates/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Editor')
def edit_template(id):
    db = Database()
    conn = db.get_connection()
    
    template = conn.execute('SELECT * FROM document_templates WHERE id = ?', (id,)).fetchone()
    if not template:
        flash('Template not found.', 'danger')
        conn.close()
        return redirect(url_for('document_template_routes.list_templates'))
    
    if request.method == 'POST':
        try:
            name = request.form.get('name', '').strip()
            description = request.form.get('description', '').strip()
            effective_date = request.form.get('effective_date', '').strip() or None
            expiration_date = request.form.get('expiration_date', '').strip() or None
            
            conn.execute('''
                UPDATE document_templates 
                SET name = ?, description = ?, effective_date = ?, expiration_date = ?,
                    updated_by = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (name, description, effective_date, expiration_date, session.get('user_id'), id))
            
            logo_position = request.form.get('logo_position', 'left')
            company_name = request.form.get('company_name', '').strip()
            company_name_font_size = int(request.form.get('company_name_font_size', 16))
            address_line1 = request.form.get('address_line1', '').strip()
            address_line2 = request.form.get('address_line2', '').strip()
            city_state_zip = request.form.get('city_state_zip', '').strip()
            country = request.form.get('country', '').strip()
            phone = request.form.get('phone', '').strip()
            email = request.form.get('email', '').strip()
            website = request.form.get('website', '').strip()
            registration_numbers = request.form.get('registration_numbers', '').strip()
            header_layout = request.form.get('header_layout', 'two-column')
            show_document_title = 1 if request.form.get('show_document_title') else 0
            document_title_position = request.form.get('document_title_position', 'center')
            document_title_font_size = int(request.form.get('document_title_font_size', 18))
            
            existing_header = conn.execute('SELECT id FROM template_headers WHERE template_id = ?', (id,)).fetchone()
            if existing_header:
                conn.execute('''
                    UPDATE template_headers SET
                        logo_position = ?, company_name = ?, company_name_font_size = ?,
                        address_line1 = ?, address_line2 = ?, city_state_zip = ?, country = ?,
                        phone = ?, email = ?, website = ?, registration_numbers = ?,
                        header_layout = ?, show_document_title = ?, document_title_position = ?,
                        document_title_font_size = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE template_id = ?
                ''', (logo_position, company_name, company_name_font_size, address_line1, address_line2,
                      city_state_zip, country, phone, email, website, registration_numbers,
                      header_layout, show_document_title, document_title_position, 
                      document_title_font_size, id))
            else:
                conn.execute('''
                    INSERT INTO template_headers (template_id, logo_position, company_name, company_name_font_size,
                        address_line1, address_line2, city_state_zip, country, phone, email, website,
                        registration_numbers, header_layout, show_document_title, document_title_position, document_title_font_size)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (id, logo_position, company_name, company_name_font_size, address_line1, address_line2,
                      city_state_zip, country, phone, email, website, registration_numbers,
                      header_layout, show_document_title, document_title_position, document_title_font_size))
            
            show_page_numbers = 1 if request.form.get('show_page_numbers') else 0
            page_number_format = request.form.get('page_number_format', 'Page {current} of {total}')
            page_number_position = request.form.get('page_number_position', 'center')
            legal_text = request.form.get('legal_text', '').strip()
            contact_info = request.form.get('footer_contact_info', '').strip()
            show_prepared_by = 1 if request.form.get('show_prepared_by') else 0
            show_approved_by = 1 if request.form.get('show_approved_by') else 0
            show_qr_code = 1 if request.form.get('show_qr_code') else 0
            show_document_hash = 1 if request.form.get('show_document_hash') else 0
            
            existing_footer = conn.execute('SELECT id FROM template_footers WHERE template_id = ?', (id,)).fetchone()
            if existing_footer:
                conn.execute('''
                    UPDATE template_footers SET
                        show_page_numbers = ?, page_number_format = ?, page_number_position = ?,
                        legal_text = ?, contact_info = ?, show_prepared_by = ?, show_approved_by = ?,
                        show_qr_code = ?, show_document_hash = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE template_id = ?
                ''', (show_page_numbers, page_number_format, page_number_position, legal_text,
                      contact_info, show_prepared_by, show_approved_by, show_qr_code, show_document_hash, id))
            else:
                conn.execute('''
                    INSERT INTO template_footers (template_id, show_page_numbers, page_number_format,
                        page_number_position, legal_text, contact_info, show_prepared_by, show_approved_by,
                        show_qr_code, show_document_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (id, show_page_numbers, page_number_format, page_number_position, legal_text,
                      contact_info, show_prepared_by, show_approved_by, show_qr_code, show_document_hash))
            
            AuditLogger.log_change(conn, 'document_templates', id, 'UPDATE', session.get('user_id'),
                                  {'name': name, 'updated_by': session.get('username')})
            
            conn.commit()
            flash('Template updated successfully!', 'success')
            conn.close()
            return redirect(url_for('document_template_routes.edit_template', id=id))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error updating template: {str(e)}', 'danger')
    
    header = conn.execute('SELECT * FROM template_headers WHERE template_id = ?', (id,)).fetchone()
    footer = conn.execute('SELECT * FROM template_footers WHERE template_id = ?', (id,)).fetchone()
    sections = conn.execute('SELECT * FROM template_sections WHERE template_id = ? ORDER BY display_order', (id,)).fetchall()
    
    template_terms = conn.execute('''
        SELECT tt.*, tl.term_code, tl.name as term_name, tl.category, tl.content as original_content
        FROM template_terms tt
        JOIN template_terms_library tl ON tt.term_id = tl.id
        WHERE tt.template_id = ?
        ORDER BY tt.display_order
    ''', (id,)).fetchall()
    
    available_terms = conn.execute('''
        SELECT * FROM template_terms_library WHERE is_active = 1 ORDER BY category, name
    ''').fetchall()
    
    tokens = conn.execute('SELECT * FROM template_tokens WHERE is_active = 1 ORDER BY token_category, token_name').fetchall()
    
    conn.close()
    
    doc_type_name = next((dt[1] for dt in DOCUMENT_TYPES if dt[0] == template['document_type']), template['document_type'])
    
    return render_template('document_templates/edit.html',
                          template=template,
                          header=header,
                          footer=footer,
                          sections=sections,
                          template_terms=template_terms,
                          available_terms=available_terms,
                          tokens=tokens,
                          doc_type_name=doc_type_name,
                          document_types=DOCUMENT_TYPES,
                          term_categories=TERM_CATEGORIES)

@document_template_bp.route('/document-templates/<int:id>/clone', methods=['POST'])
@role_required('Admin')
def clone_template(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        template = conn.execute('SELECT * FROM document_templates WHERE id = ?', (id,)).fetchone()
        if not template:
            flash('Template not found.', 'danger')
            conn.close()
            return redirect(url_for('document_template_routes.list_templates'))
        
        base_code = template['template_code'] + '-COPY'
        new_code = base_code
        counter = 1
        while conn.execute('SELECT id FROM document_templates WHERE template_code = ?', (new_code,)).fetchone():
            new_code = f"{base_code}-{counter}"
            counter += 1
        
        cursor = conn.execute('''
            INSERT INTO document_templates (template_code, name, document_type, description, status, created_by)
            VALUES (?, ?, ?, ?, 'Draft', ?)
        ''', (new_code, template['name'] + ' (Copy)', template['document_type'], template['description'], session.get('user_id')))
        
        new_id = cursor.lastrowid
        
        header = conn.execute('SELECT * FROM template_headers WHERE template_id = ?', (id,)).fetchone()
        if header:
            conn.execute('''
                INSERT INTO template_headers (template_id, logo_path, logo_width, logo_height, logo_position,
                    company_name, company_name_font_size, company_name_font_weight, address_line1, address_line2,
                    city_state_zip, country, phone, email, website, registration_numbers, header_layout,
                    show_document_title, document_title_position, document_title_font_size)
                SELECT ?, logo_path, logo_width, logo_height, logo_position, company_name, company_name_font_size,
                    company_name_font_weight, address_line1, address_line2, city_state_zip, country, phone, email,
                    website, registration_numbers, header_layout, show_document_title, document_title_position,
                    document_title_font_size
                FROM template_headers WHERE template_id = ?
            ''', (new_id, id))
        
        footer = conn.execute('SELECT * FROM template_footers WHERE template_id = ?', (id,)).fetchone()
        if footer:
            conn.execute('''
                INSERT INTO template_footers (template_id, show_page_numbers, page_number_format, page_number_position,
                    legal_text, contact_info, prepared_by_label, show_prepared_by, approved_by_label, show_approved_by,
                    show_qr_code, qr_code_content, show_document_hash, footer_height)
                SELECT ?, show_page_numbers, page_number_format, page_number_position, legal_text, contact_info,
                    prepared_by_label, show_prepared_by, approved_by_label, show_approved_by, show_qr_code,
                    qr_code_content, show_document_hash, footer_height
                FROM template_footers WHERE template_id = ?
            ''', (new_id, id))
        
        conn.execute('''
            INSERT INTO template_sections (template_id, section_type, section_name, display_order, is_visible,
                title, show_title, title_font_size, content_template, table_columns, show_subtotals, subtotal_label)
            SELECT ?, section_type, section_name, display_order, is_visible, title, show_title, title_font_size,
                content_template, table_columns, show_subtotals, subtotal_label
            FROM template_sections WHERE template_id = ?
        ''', (new_id, id))
        
        conn.execute('''
            INSERT INTO template_terms (template_id, term_id, display_order, is_overridden, override_content)
            SELECT ?, term_id, display_order, is_overridden, override_content
            FROM template_terms WHERE template_id = ?
        ''', (new_id, id))
        
        AuditLogger.log_change(conn, 'document_templates', new_id, 'CLONE', session.get('user_id'),
                              {'cloned_from': id, 'new_code': new_code})
        
        conn.commit()
        flash(f'Template cloned successfully as {new_code}!', 'success')
        conn.close()
        return redirect(url_for('document_template_routes.edit_template', id=new_id))
        
    except Exception as e:
        conn.rollback()
        flash(f'Error cloning template: {str(e)}', 'danger')
        conn.close()
        return redirect(url_for('document_template_routes.list_templates'))

@document_template_bp.route('/document-templates/<int:id>/activate', methods=['POST'])
@role_required('Admin')
def activate_template(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        template = conn.execute('SELECT * FROM document_templates WHERE id = ?', (id,)).fetchone()
        if not template:
            flash('Template not found.', 'danger')
            conn.close()
            return redirect(url_for('document_template_routes.list_templates'))
        
        template_snapshot = json.dumps({
            'template': dict(template),
            'header': dict(conn.execute('SELECT * FROM template_headers WHERE template_id = ?', (id,)).fetchone() or {}),
            'footer': dict(conn.execute('SELECT * FROM template_footers WHERE template_id = ?', (id,)).fetchone() or {}),
            'sections': [dict(s) for s in conn.execute('SELECT * FROM template_sections WHERE template_id = ?', (id,)).fetchall()],
            'terms': [dict(t) for t in conn.execute('SELECT * FROM template_terms WHERE template_id = ?', (id,)).fetchall()]
        }, default=str)
        
        new_version = template['version'] + 1
        
        conn.execute('''
            INSERT INTO template_versions (template_id, version_number, template_snapshot, change_reason, changed_by)
            VALUES (?, ?, ?, 'Template activated', ?)
        ''', (id, new_version, template_snapshot, session.get('user_id')))
        
        conn.execute('''
            UPDATE document_templates SET status = 'Active', version = ?, updated_by = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (new_version, session.get('user_id'), id))
        
        AuditLogger.log_change(conn, 'document_templates', id, 'ACTIVATE', session.get('user_id'),
                              {'version': new_version})
        
        conn.commit()
        flash('Template activated successfully!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error activating template: {str(e)}', 'danger')
    
    conn.close()
    return redirect(url_for('document_template_routes.view_template', id=id))

@document_template_bp.route('/document-templates/<int:id>/set-default', methods=['POST'])
@role_required('Admin')
def set_default_template(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        template = conn.execute('SELECT * FROM document_templates WHERE id = ?', (id,)).fetchone()
        if not template:
            flash('Template not found.', 'danger')
            conn.close()
            return redirect(url_for('document_template_routes.list_templates'))
        
        conn.execute('''
            UPDATE document_templates SET is_default = 0 WHERE document_type = ?
        ''', (template['document_type'],))
        
        conn.execute('''
            UPDATE document_templates SET is_default = 1, updated_by = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (session.get('user_id'), id))
        
        AuditLogger.log_change(conn, 'document_templates', id, 'SET_DEFAULT', session.get('user_id'),
                              {'document_type': template['document_type']})
        
        conn.commit()
        flash('Template set as default for this document type!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error setting default: {str(e)}', 'danger')
    
    conn.close()
    return redirect(url_for('document_template_routes.view_template', id=id))

@document_template_bp.route('/document-templates/<int:id>/archive', methods=['POST'])
@role_required('Admin')
def archive_template(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('''
            UPDATE document_templates SET status = 'Archived', is_default = 0, updated_by = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (session.get('user_id'), id))
        
        AuditLogger.log_change(conn, 'document_templates', id, 'ARCHIVE', session.get('user_id'), {})
        
        conn.commit()
        flash('Template archived successfully!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error archiving template: {str(e)}', 'danger')
    
    conn.close()
    return redirect(url_for('document_template_routes.list_templates'))

@document_template_bp.route('/document-templates/<int:id>/delete', methods=['POST'])
@role_required('Admin')
def delete_template(id):
    """Permanently delete a document template"""
    db = Database()
    conn = db.get_connection()
    
    try:
        # Check if template exists and is not a system template
        template = conn.execute('SELECT * FROM document_templates WHERE id = ?', (id,)).fetchone()
        
        if not template:
            flash('Template not found', 'danger')
            conn.close()
            return redirect(url_for('document_template_routes.list_templates'))
        
        if template['is_system']:
            flash('System templates cannot be deleted', 'danger')
            conn.close()
            return redirect(url_for('document_template_routes.list_templates'))
        
        # Store template info for audit before deletion
        template_code = template['template_code']
        template_name = template['name']
        
        # Delete related records (headers, footers, terms links, assignments)
        # Use try/except for each to handle if tables don't exist
        try:
            conn.execute('DELETE FROM template_headers WHERE template_id = ?', (id,))
        except:
            pass
        try:
            conn.execute('DELETE FROM template_footers WHERE template_id = ?', (id,))
        except:
            pass
        try:
            conn.execute('DELETE FROM template_terms WHERE template_id = ?', (id,))
        except:
            pass
        try:
            conn.execute('DELETE FROM template_assignments WHERE template_id = ?', (id,))
        except:
            pass
        
        # Delete the template itself
        conn.execute('DELETE FROM document_templates WHERE id = ?', (id,))
        
        conn.commit()
        
        # Log audit after commit to avoid issues
        try:
            AuditLogger.log_change(conn, 'document_templates', id, 'DELETE', session.get('user_id'), 
                                  {'template_code': template_code, 'name': template_name})
            conn.commit()
        except:
            pass
        
        flash('Template deleted successfully!', 'success')
        
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        flash(f'Error deleting template: {str(e)}', 'danger')
    finally:
        try:
            conn.close()
        except:
            pass
    
    return redirect(url_for('document_template_routes.list_templates'))

@document_template_bp.route('/document-templates/<int:id>/add-term', methods=['POST'])
@role_required('Admin', 'Editor')
def add_term_to_template(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        term_id = request.form.get('term_id')
        if not term_id:
            flash('Please select a term to add.', 'danger')
            conn.close()
            return redirect(url_for('document_template_routes.edit_template', id=id))
        
        existing = conn.execute('SELECT id FROM template_terms WHERE template_id = ? AND term_id = ?', (id, term_id)).fetchone()
        if existing:
            flash('This term is already added to the template.', 'warning')
            conn.close()
            return redirect(url_for('document_template_routes.edit_template', id=id))
        
        max_order = conn.execute('SELECT MAX(display_order) as max_order FROM template_terms WHERE template_id = ?', (id,)).fetchone()
        next_order = (max_order['max_order'] or 0) + 1
        
        conn.execute('''
            INSERT INTO template_terms (template_id, term_id, display_order)
            VALUES (?, ?, ?)
        ''', (id, term_id, next_order))
        
        conn.commit()
        flash('Term added to template!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error adding term: {str(e)}', 'danger')
    
    conn.close()
    return redirect(url_for('document_template_routes.edit_template', id=id))

@document_template_bp.route('/document-templates/<int:id>/remove-term/<int:term_link_id>', methods=['POST'])
@role_required('Admin', 'Editor')
def remove_term_from_template(id, term_link_id):
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('DELETE FROM template_terms WHERE id = ? AND template_id = ?', (term_link_id, id))
        conn.commit()
        flash('Term removed from template!', 'success')
        
    except Exception as e:
        conn.rollback()
        flash(f'Error removing term: {str(e)}', 'danger')
    
    conn.close()
    return redirect(url_for('document_template_routes.edit_template', id=id))

@document_template_bp.route('/terms-library')
@role_required('Admin', 'Editor', 'Viewer', 'Procurement', 'Sales', 'Accountant')
def list_terms():
    db = Database()
    conn = db.get_connection()
    
    filter_category = request.args.get('category', '')
    
    query = '''
        SELECT tl.*, u.username as created_by_name,
               (SELECT COUNT(*) FROM template_terms WHERE term_id = tl.id) as usage_count
        FROM template_terms_library tl
        LEFT JOIN users u ON tl.created_by = u.id
        WHERE 1=1
    '''
    params = []
    
    if filter_category:
        query += ' AND tl.category = ?'
        params.append(filter_category)
    
    query += ' ORDER BY tl.category, tl.name'
    
    terms = conn.execute(query, params).fetchall()
    conn.close()
    
    return render_template('document_templates/terms_list.html',
                          terms=terms,
                          term_categories=TERM_CATEGORIES,
                          filter_category=filter_category)

@document_template_bp.route('/terms-library/create', methods=['GET', 'POST'])
@role_required('Admin')
def create_term():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            term_code = request.form.get('term_code', '').strip().upper()
            name = request.form.get('name', '').strip()
            category = request.form.get('category', '')
            content = request.form.get('content', '').strip()
            is_global_default = 1 if request.form.get('is_global_default') else 0
            
            if not term_code or not name or not category or not content:
                flash('All fields are required.', 'danger')
                conn.close()
                return redirect(url_for('document_template_routes.create_term'))
            
            conn.execute('''
                INSERT INTO template_terms_library (term_code, name, category, content, is_global_default, created_by)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (term_code, name, category, content, is_global_default, session.get('user_id')))
            
            conn.commit()
            flash('Term created successfully!', 'success')
            conn.close()
            return redirect(url_for('document_template_routes.list_terms'))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error creating term: {str(e)}', 'danger')
    
    conn.close()
    return render_template('document_templates/term_create.html', term_categories=TERM_CATEGORIES)

@document_template_bp.route('/terms-library/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin')
def edit_term(id):
    db = Database()
    conn = db.get_connection()
    
    term = conn.execute('SELECT * FROM template_terms_library WHERE id = ?', (id,)).fetchone()
    if not term:
        flash('Term not found.', 'danger')
        conn.close()
        return redirect(url_for('document_template_routes.list_terms'))
    
    if request.method == 'POST':
        try:
            name = request.form.get('name', '').strip()
            category = request.form.get('category', '')
            content = request.form.get('content', '').strip()
            is_global_default = 1 if request.form.get('is_global_default') else 0
            is_active = 1 if request.form.get('is_active') else 0
            
            new_version = term['version'] + 1
            
            conn.execute('''
                UPDATE template_terms_library 
                SET name = ?, category = ?, content = ?, is_global_default = ?, is_active = ?,
                    version = ?, updated_by = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (name, category, content, is_global_default, is_active, new_version, session.get('user_id'), id))
            
            conn.commit()
            flash('Term updated successfully!', 'success')
            conn.close()
            return redirect(url_for('document_template_routes.list_terms'))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error updating term: {str(e)}', 'danger')
    
    conn.close()
    return render_template('document_templates/term_edit.html', term=term, term_categories=TERM_CATEGORIES)

@document_template_bp.route('/document-templates/tokens')
@role_required('Admin', 'Editor')
def list_tokens():
    db = Database()
    conn = db.get_connection()
    
    tokens = conn.execute('''
        SELECT * FROM template_tokens ORDER BY token_category, token_name
    ''').fetchall()
    
    conn.close()
    
    token_categories = {}
    for token in tokens:
        cat = token['token_category']
        if cat not in token_categories:
            token_categories[cat] = []
        token_categories[cat].append(token)
    
    return render_template('document_templates/tokens.html', token_categories=token_categories)

@document_template_bp.route('/document-templates/<int:id>/preview')
@role_required('Admin', 'Editor', 'Viewer')
def preview_template(id):
    db = Database()
    conn = db.get_connection()
    
    template = conn.execute('SELECT * FROM document_templates WHERE id = ?', (id,)).fetchone()
    if not template:
        flash('Template not found.', 'danger')
        conn.close()
        return redirect(url_for('document_template_routes.list_templates'))
    
    header = conn.execute('SELECT * FROM template_headers WHERE template_id = ?', (id,)).fetchone()
    footer = conn.execute('SELECT * FROM template_footers WHERE template_id = ?', (id,)).fetchone()
    sections = conn.execute('SELECT * FROM template_sections WHERE template_id = ? ORDER BY display_order', (id,)).fetchall()
    
    terms = conn.execute('''
        SELECT tt.*, tl.content as original_content
        FROM template_terms tt
        JOIN template_terms_library tl ON tt.term_id = tl.id
        WHERE tt.template_id = ?
        ORDER BY tt.display_order
    ''', (id,)).fetchall()
    
    conn.close()
    
    sample_data = {
        'Customer.Name': 'Acme Corporation',
        'Customer.Code': 'ACME-001',
        'Customer.Address': '123 Business St, Suite 100, New York, NY 10001',
        'Customer.Contact': 'John Smith',
        'Document.Number': 'DOC-2026-0001',
        'Document.Date': datetime.now().strftime('%Y-%m-%d'),
        'Document.DueDate': '2026-02-25',
        'Document.Status': 'Active',
        'Order.Number': 'SO-2026-0001',
        'Order.Reference': 'PO-12345',
        'Total.Subtotal': '$5,000.00',
        'Total.Tax': '$400.00',
        'Total.Amount': '$5,400.00',
        'PreparedBy': session.get('username', 'System User'),
        'CurrentDate': datetime.now().strftime('%Y-%m-%d'),
        'Company.Name': header['company_name'] if header else 'Your Company',
        'Company.Address': f"{header['address_line1'] or ''}, {header['city_state_zip'] or ''}" if header else '',
        'Company.Phone': header['phone'] if header else '',
        'Company.Email': header['email'] if header else ''
    }
    
    doc_type_name = next((dt[1] for dt in DOCUMENT_TYPES if dt[0] == template['document_type']), template['document_type'])
    
    return render_template('document_templates/preview.html',
                          template=template,
                          header=header,
                          footer=footer,
                          sections=sections,
                          terms=terms,
                          sample_data=sample_data,
                          doc_type_name=doc_type_name)
