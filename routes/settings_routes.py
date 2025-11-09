from flask import Blueprint, render_template, request, redirect, url_for, flash
from models import Database, CompanySettings, User
from auth import login_required, role_required
from werkzeug.utils import secure_filename
from datetime import datetime
import os

settings_bp = Blueprint('settings_routes', __name__)

UPLOAD_FOLDER = 'static/uploads/company'
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@settings_bp.route('/settings/company')
@login_required
def view_company_settings():
    settings = CompanySettings.get_or_create_default()
    
    updated_by_user = None
    if settings and settings['updated_by']:
        updated_by_user = User.get_by_id(settings['updated_by'])
    
    return render_template('settings/company.html', settings=settings, updated_by_user=updated_by_user)

@settings_bp.route('/settings/accounting')
@login_required
def view_accounting_preferences():
    """View accounting preferences"""
    settings = CompanySettings.get_or_create_default()
    
    updated_by_user = None
    if settings and settings['updated_by']:
        updated_by_user = User.get_by_id(settings['updated_by'])
    
    return render_template('settings/accounting_preferences.html', 
                         settings=settings, 
                         updated_by_user=updated_by_user)

@settings_bp.route('/settings/accounting/edit', methods=['GET', 'POST'])
@role_required('Admin')
def edit_accounting_preferences():
    """Edit accounting preferences"""
    if request.method == 'POST':
        from flask import session
        user_id = session.get('user_id')
        
        # Get current settings
        current_settings = CompanySettings.get()
        
        # Build data dict with current values, only updating accounting preference
        data = {
            'company_name': current_settings['company_name'],
            'dba': current_settings['dba'],
            'address_line1': current_settings['address_line1'],
            'address_line2': current_settings['address_line2'],
            'city': current_settings['city'],
            'state': current_settings['state'],
            'postal_code': current_settings['postal_code'],
            'country': current_settings['country'],
            'phone': current_settings['phone'],
            'email': current_settings['email'],
            'website': current_settings['website'],
            'tax_id': current_settings['tax_id'],
            'duns_number': current_settings['duns_number'],
            'cage_code': current_settings['cage_code'],
            'logo_filename': current_settings['logo_filename'],
            'auto_post_invoice_gl': 1 if request.form.get('auto_post_invoice_gl') == 'on' else 0
        }
        
        CompanySettings.create_or_update(data, user_id)
        
        flash('Accounting preferences updated successfully.', 'success')
        return redirect(url_for('settings_routes.view_accounting_preferences'))
    
    settings = CompanySettings.get_or_create_default()
    return render_template('settings/edit_accounting_preferences.html', settings=settings)

@settings_bp.route('/settings/company/edit', methods=['GET', 'POST'])
@role_required('Admin')
def edit_company_settings():
    if request.method == 'POST':
        company_name = request.form.get('company_name', '').strip()
        
        if not company_name:
            flash('Company name is required.', 'danger')
            settings = CompanySettings.get_or_create_default()
            return render_template('settings/edit.html', settings=settings)
        
        data = {
            'company_name': company_name,
            'dba': request.form.get('dba', ''),
            'address_line1': request.form.get('address_line1', ''),
            'address_line2': request.form.get('address_line2', ''),
            'city': request.form.get('city', ''),
            'state': request.form.get('state', ''),
            'postal_code': request.form.get('postal_code', ''),
            'country': request.form.get('country', ''),
            'phone': request.form.get('phone', ''),
            'email': request.form.get('email', ''),
            'website': request.form.get('website', ''),
            'tax_id': request.form.get('tax_id', ''),
            'duns_number': request.form.get('duns_number', ''),
            'cage_code': request.form.get('cage_code', ''),
            'logo_filename': None
        }
        
        current_settings = CompanySettings.get()
        if current_settings and current_settings['logo_filename']:
            data['logo_filename'] = current_settings['logo_filename']
        
        if 'logo' in request.files:
            file = request.files['logo']
            if file and file.filename:
                if not allowed_file(file.filename):
                    flash('Invalid file type. Only PNG, JPG, JPEG, and GIF files are allowed.', 'warning')
                else:
                    file.seek(0, os.SEEK_END)
                    file_size = file.tell()
                    file.seek(0)
                    
                    MAX_FILE_SIZE = 5 * 1024 * 1024
                    if file_size > MAX_FILE_SIZE:
                        flash('Logo file size exceeds 5MB limit.', 'warning')
                    else:
                        os.makedirs(UPLOAD_FOLDER, exist_ok=True)
                        
                        filename = secure_filename(file.filename)
                        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                        filename = f"logo_{timestamp}_{filename}"
                        filepath = os.path.join(UPLOAD_FOLDER, filename)
                        file.save(filepath)
                        data['logo_filename'] = filename
        
        from flask import session
        user_id = session.get('user_id')
        
        CompanySettings.create_or_update(data, user_id)
        
        flash('✅ Company information successfully updated.', 'success')
        return redirect(url_for('settings_routes.view_company_settings'))
    
    settings = CompanySettings.get_or_create_default()
    return render_template('settings/edit.html', settings=settings)
