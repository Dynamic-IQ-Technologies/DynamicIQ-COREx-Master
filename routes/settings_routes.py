from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from models import Database, CompanySettings, User
from auth import login_required, role_required
from werkzeug.utils import secure_filename
from datetime import datetime
import os
import json

def get_openai_client():
    """Get OpenAI client configured with Replit AI Integrations"""
    from openai import OpenAI
    return OpenAI(
        api_key=os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY"),
        base_url=os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")
    )

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
            'auto_post_invoice_gl': 1 if request.form.get('auto_post_invoice_gl') == 'on' else 0,
            'marketing_tagline': current_settings['marketing_tagline'],
            'brand_primary_color': current_settings['brand_primary_color'],
            'brand_secondary_color': current_settings['brand_secondary_color'],
            'brand_accent_color': current_settings['brand_accent_color'],
            'brand_tone': current_settings['brand_tone'],
            'marketing_description': current_settings['marketing_description'],
            'target_industries': current_settings['target_industries'],
            'key_differentiators': current_settings['key_differentiators']
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
        
        current_settings = CompanySettings.get()
        
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
            'logo_filename': None,
            'auto_post_invoice_gl': current_settings['auto_post_invoice_gl'] if current_settings else 0,
            'marketing_tagline': current_settings['marketing_tagline'] if current_settings else None,
            'brand_primary_color': current_settings['brand_primary_color'] if current_settings else '#1e40af',
            'brand_secondary_color': current_settings['brand_secondary_color'] if current_settings else '#f97316',
            'brand_accent_color': current_settings['brand_accent_color'] if current_settings else '#10b981',
            'brand_tone': current_settings['brand_tone'] if current_settings else 'Enterprise',
            'marketing_description': current_settings['marketing_description'] if current_settings else None,
            'target_industries': current_settings['target_industries'] if current_settings else None,
            'key_differentiators': current_settings['key_differentiators'] if current_settings else None
        }
        
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

@settings_bp.route('/settings/marketing', methods=['GET', 'POST'])
@role_required('Admin')
def edit_marketing_settings():
    """Edit marketing presentation generator settings"""
    if request.method == 'POST':
        from flask import session
        user_id = session.get('user_id')
        
        current_settings = CompanySettings.get()
        
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
            'auto_post_invoice_gl': current_settings['auto_post_invoice_gl'],
            'marketing_tagline': request.form.get('marketing_tagline', ''),
            'brand_primary_color': request.form.get('brand_primary_color', '#1e40af'),
            'brand_secondary_color': request.form.get('brand_secondary_color', '#f97316'),
            'brand_accent_color': request.form.get('brand_accent_color', '#10b981'),
            'brand_tone': request.form.get('brand_tone', 'Enterprise'),
            'marketing_description': request.form.get('marketing_description', ''),
            'target_industries': request.form.get('target_industries', ''),
            'key_differentiators': request.form.get('key_differentiators', '')
        }
        
        CompanySettings.create_or_update(data, user_id)
        
        flash('Marketing presentation settings updated successfully.', 'success')
        return redirect(url_for('settings_routes.view_company_settings'))
    
    settings = CompanySettings.get_or_create_default()
    return render_template('settings/edit_marketing.html', settings=settings)

@settings_bp.route('/settings/marketing/generate-presentation')
@role_required('Admin')
def generate_marketing_presentation():
    """Generate AI-powered marketing presentation"""
    settings = CompanySettings.get_or_create_default()
    return render_template('settings/generate_presentation.html', settings=settings)

def validate_presentation_schema(data):
    """Validate and ensure all required fields exist in presentation data"""
    if not isinstance(data, dict):
        data = {}
    
    defaults = {
        'hero': {'headline': 'Transform Your Manufacturing Operations', 'subheadline': 'Next-generation ERP for modern manufacturing'},
        'value_propositions': [],
        'capabilities': [],
        'industries': [],
        'stats': [],
        'testimonial': {'quote': '', 'author': '', 'title': ''},
        'cta': {'headline': 'Ready to Transform Your Operations?', 'button_text': 'Get Started'}
    }
    
    for key, default_val in defaults.items():
        if key not in data:
            data[key] = default_val
        elif isinstance(default_val, list) and not isinstance(data.get(key), list):
            data[key] = default_val
        elif isinstance(default_val, dict) and not isinstance(data.get(key), dict):
            data[key] = default_val
    
    if data.get('hero') and isinstance(data['hero'], dict):
        if 'headline' not in data['hero']:
            data['hero']['headline'] = defaults['hero']['headline']
        if 'subheadline' not in data['hero']:
            data['hero']['subheadline'] = defaults['hero']['subheadline']
    
    validated_vps = []
    for vp in data.get('value_propositions', []):
        if isinstance(vp, dict):
            validated_vps.append({
                'title': vp.get('title', 'Feature'),
                'description': vp.get('description', ''),
                'icon': vp.get('icon', 'bi-star')
            })
    data['value_propositions'] = validated_vps
    
    validated_caps = []
    for cap in data.get('capabilities', []):
        if isinstance(cap, dict):
            features = cap.get('features', [])
            if not isinstance(features, list):
                features = []
            validated_caps.append({
                'category': cap.get('category', 'Category'),
                'features': [str(f) for f in features if f]
            })
    data['capabilities'] = validated_caps
    
    validated_industries = []
    for ind in data.get('industries', []):
        if isinstance(ind, dict):
            validated_industries.append({
                'name': ind.get('name', 'Industry'),
                'use_case': ind.get('use_case', '')
            })
    data['industries'] = validated_industries
    
    validated_stats = []
    for stat in data.get('stats', []):
        if isinstance(stat, dict):
            validated_stats.append({
                'value': str(stat.get('value', '0')),
                'label': stat.get('label', '')
            })
    data['stats'] = validated_stats
    
    if isinstance(data.get('testimonial'), dict):
        data['testimonial'] = {
            'quote': data['testimonial'].get('quote', ''),
            'author': data['testimonial'].get('author', ''),
            'title': data['testimonial'].get('title', '')
        }
    else:
        data['testimonial'] = defaults['testimonial']
    
    if isinstance(data.get('cta'), dict):
        data['cta'] = {
            'headline': data['cta'].get('headline', defaults['cta']['headline']),
            'button_text': data['cta'].get('button_text', defaults['cta']['button_text'])
        }
    else:
        data['cta'] = defaults['cta']
    
    return data

@settings_bp.route('/api/marketing/generate', methods=['POST'])
@role_required('Admin')
def api_generate_presentation():
    """API endpoint to generate marketing presentation content using AI"""
    try:
        api_key = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY")
        if not api_key:
            return jsonify({'success': False, 'error': 'OpenAI API key not configured. Please set up the AI integration.'})
        
        settings_row = CompanySettings.get_or_create_default()
        settings = dict(settings_row) if settings_row else {}
        
        company_name = settings.get('company_name', 'Our Company')
        tagline = settings.get('marketing_tagline', '')
        description = settings.get('marketing_description', '')
        tone = settings.get('brand_tone', 'Enterprise')
        industries = settings.get('target_industries', '')
        differentiators = settings.get('key_differentiators', '')
        
        prompt = f"""Generate a professional marketing presentation for an ERP/MRP software company with the following details:

Company Name: {company_name}
Tagline: {tagline or 'AI-Powered Manufacturing Excellence'}
System Description: {description or 'A comprehensive manufacturing resource planning system'}
Brand Tone: {tone}
Target Industries: {industries or 'Aerospace, Defense, Manufacturing'}
Key Differentiators: {differentiators or 'AI-powered automation, real-time tracking, compliance management'}

Generate a JSON response with exactly this structure:
{{
    "hero": {{
        "headline": "A compelling headline for the hero section",
        "subheadline": "A supporting statement that reinforces the value proposition"
    }},
    "value_propositions": [
        {{
            "title": "Value Prop Title",
            "description": "2-3 sentence description",
            "icon": "bi-icon-name"
        }}
    ],
    "capabilities": [
        {{
            "category": "Category Name",
            "features": ["Feature 1", "Feature 2", "Feature 3"]
        }}
    ],
    "industries": [
        {{
            "name": "Industry Name",
            "use_case": "How the system helps this industry"
        }}
    ],
    "stats": [
        {{
            "value": "95%",
            "label": "Stat description"
        }}
    ],
    "testimonial": {{
        "quote": "A compelling testimonial quote",
        "author": "Executive Name",
        "title": "Title, Company"
    }},
    "cta": {{
        "headline": "Call to action headline",
        "button_text": "Button text"
    }}
}}

Use the {tone} tone throughout. Make it compelling for executives in the {industries or 'manufacturing'} industry.
Include 4 value propositions, 4 capability categories with 3-4 features each, 3-4 target industries, and 4 impressive statistics.
For icons, use Bootstrap Icons names (bi-rocket, bi-graph-up, bi-shield-check, bi-gear, bi-clock, bi-people, bi-award, bi-lightning, etc.)."""

        client = get_openai_client()
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a marketing expert specializing in B2B enterprise software. Generate compelling, professional marketing content. Always respond with valid JSON only, no markdown formatting."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=2000
        )
        
        content = response.choices[0].message.content
        if not content:
            return jsonify({'success': False, 'error': 'AI returned empty response'})
        content = content.strip()
        if content.startswith('```'):
            content = content.split('\n', 1)[1]
            if content.endswith('```'):
                content = content[:-3]
        
        presentation_data = json.loads(content)
        
        presentation_data = validate_presentation_schema(presentation_data)
        
        presentation_data['company'] = {
            'name': company_name,
            'tagline': tagline,
            'primary_color': settings.get('brand_primary_color', '#1e40af'),
            'secondary_color': settings.get('brand_secondary_color', '#f97316'),
            'accent_color': settings.get('brand_accent_color', '#10b981'),
            'logo': settings.get('logo_filename', '')
        }
        
        return jsonify({'success': True, 'presentation': presentation_data})
        
    except json.JSONDecodeError as e:
        return jsonify({'success': False, 'error': f'Failed to parse AI response. Please try again.'})
    except Exception as e:
        error_msg = str(e)
        if 'api_key' in error_msg.lower() or 'authentication' in error_msg.lower():
            return jsonify({'success': False, 'error': 'OpenAI API key is invalid or expired. Please check your AI integration settings.'})
        elif 'rate_limit' in error_msg.lower() or 'quota' in error_msg.lower():
            return jsonify({'success': False, 'error': 'API rate limit reached. Please try again in a moment.'})
        return jsonify({'success': False, 'error': f'Error generating presentation: {error_msg}'})
