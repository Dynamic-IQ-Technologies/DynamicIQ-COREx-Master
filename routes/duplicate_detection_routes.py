"""
Duplicate Detection Routes for Dynamic.IQ-COREx
Admin panel and API endpoints for duplicate detection management
"""
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database, AuditLogger
from auth import login_required, role_required
from services.duplicate_detection import DuplicateDetectionService, RECORD_TYPE_CONFIG, get_duplicate_service
from datetime import datetime
import json

duplicate_detection_bp = Blueprint('duplicate_detection', __name__)


@duplicate_detection_bp.route('/admin/duplicate-detection')
@login_required
@role_required(['Admin'])
def admin_dashboard():
    """Duplicate Detection Configuration Admin Panel"""
    service = get_duplicate_service()
    configs = service.get_all_configs()
    
    record_types_info = []
    for record_type, config in RECORD_TYPE_CONFIG.items():
        type_config = configs.get(record_type, {})
        record_types_info.append({
            'type': record_type,
            'display_name': config['display_name'],
            'table': config['table'],
            'key_fields': config['key_fields'],
            'exact_fields': config.get('exact_fields', []),
            'fuzzy_fields': config.get('fuzzy_fields', []),
            'is_enabled': type_config.get('is_enabled', True),
            'detection_mode': type_config.get('detection_mode', 'soft'),
            'similarity_threshold': type_config.get('similarity_threshold', 0.85),
            'allow_override': type_config.get('allow_override', True),
            'override_roles': type_config.get('override_roles', ['Admin', 'Manager'])
        })
    
    return render_template('duplicate_detection/admin.html',
                          record_types=record_types_info,
                          configs=configs)


@duplicate_detection_bp.route('/admin/duplicate-detection/config/<record_type>', methods=['GET', 'POST'])
@login_required
@role_required(['Admin'])
def edit_config(record_type):
    """Edit duplicate detection configuration for a record type"""
    if record_type not in RECORD_TYPE_CONFIG:
        flash('Invalid record type', 'danger')
        return redirect(url_for('duplicate_detection.admin_dashboard'))
    
    service = get_duplicate_service()
    type_config = RECORD_TYPE_CONFIG[record_type]
    
    if request.method == 'POST':
        threshold_raw = float(request.form.get('similarity_threshold', 0.85))
        if threshold_raw > 1.0:
            threshold_raw = threshold_raw / 100.0
        threshold_raw = max(0.5, min(1.0, threshold_raw))
        
        config_data = {
            'is_enabled': request.form.get('is_enabled') == '1',
            'detection_mode': request.form.get('detection_mode', 'soft'),
            'similarity_threshold': threshold_raw,
            'allow_override': request.form.get('allow_override') == '1',
            'override_roles': request.form.getlist('override_roles')
        }
        
        key_fields = request.form.getlist('key_fields')
        if key_fields:
            config_data['key_fields'] = key_fields
        
        service.save_config(record_type, config_data, session.get('user_id'))
        
        AuditLogger.log('duplicate_detection_config', None, 'UPDATE', session.get('user_id'),
                       {'record_type': record_type, 'config': config_data})
        
        flash(f'Configuration updated for {type_config["display_name"]}', 'success')
        return redirect(url_for('duplicate_detection.admin_dashboard'))
    
    current_config = service.get_config(record_type)
    
    return render_template('duplicate_detection/edit_config.html',
                          record_type=record_type,
                          type_config=type_config,
                          config=current_config)


@duplicate_detection_bp.route('/admin/duplicate-detection/audit-log')
@login_required
@role_required(['Admin', 'Manager'])
def audit_log():
    """View duplicate detection audit log"""
    service = get_duplicate_service()
    
    record_type = request.args.get('type', '')
    logs = service.get_audit_logs(record_type if record_type else None, limit=200)
    
    return render_template('duplicate_detection/audit_log.html',
                          logs=logs,
                          record_types=RECORD_TYPE_CONFIG,
                          filter_type=record_type)


@duplicate_detection_bp.route('/api/duplicate-detection/check', methods=['POST'])
@login_required
def check_duplicates():
    """API endpoint to check for duplicates before saving"""
    data = request.get_json()
    record_type = data.get('record_type')
    field_values = data.get('field_values', {})
    exclude_id = data.get('exclude_id')
    
    if not record_type or record_type not in RECORD_TYPE_CONFIG:
        return jsonify({'error': 'Invalid record type'}), 400
    
    service = get_duplicate_service()
    result = service.detect_duplicates(record_type, field_values, exclude_id)
    
    user_role = session.get('role', '')
    can_override = result['can_override'] and user_role in result.get('override_roles', ['Admin'])
    
    type_config = RECORD_TYPE_CONFIG[record_type]
    duplicates_display = []
    for dup in result['duplicates']:
        duplicates_display.append({
            'id': dup['id'],
            'display_value': dup['display_value'],
            'match_type': dup['match_type'],
            'match_field': dup['match_field'],
            'similarity_score': dup['similarity_score'],
            'similarity_percent': round(dup['similarity_score'] * 100, 1),
            'view_url': get_view_url(record_type, dup['id'])
        })
    
    return jsonify({
        'has_duplicates': result['has_duplicates'],
        'is_exact_match': result['is_exact_match'],
        'duplicates': duplicates_display,
        'highest_score': result['highest_score'],
        'highest_score_percent': round(result['highest_score'] * 100, 1),
        'can_override': can_override,
        'detection_mode': result['detection_mode'],
        'record_type_display': type_config['display_name']
    })


@duplicate_detection_bp.route('/api/duplicate-detection/log-decision', methods=['POST'])
@login_required
def log_decision():
    """Log user's decision about duplicate detection"""
    data = request.get_json()
    record_type = data.get('record_type')
    action_type = data.get('action_type', 'CREATE')
    source_data = data.get('source_data', {})
    duplicates = data.get('duplicates', [])
    user_decision = data.get('decision')
    justification = data.get('justification', '')
    
    if not record_type or not user_decision:
        return jsonify({'error': 'Missing required fields'}), 400
    
    service = get_duplicate_service()
    service.log_detection_event(
        record_type=record_type,
        action_type=action_type,
        source_data=source_data,
        duplicates=duplicates,
        user_decision=user_decision,
        justification=justification,
        user_id=session.get('user_id'),
        ip_address=request.remote_addr
    )
    
    return jsonify({'success': True})


@duplicate_detection_bp.route('/api/duplicate-detection/bulk-check', methods=['POST'])
@login_required
@role_required(['Admin', 'Manager', 'Planner'])
def bulk_check():
    """API endpoint for bulk import duplicate checking"""
    data = request.get_json()
    record_type = data.get('record_type')
    records = data.get('records', [])
    
    if not record_type or record_type not in RECORD_TYPE_CONFIG:
        return jsonify({'error': 'Invalid record type'}), 400
    
    service = get_duplicate_service()
    results = []
    
    for i, record in enumerate(records):
        result = service.detect_duplicates(record_type, record)
        results.append({
            'index': i,
            'has_duplicates': result['has_duplicates'],
            'is_exact_match': result['is_exact_match'],
            'duplicate_count': len(result['duplicates']),
            'highest_score': result['highest_score']
        })
    
    duplicates_found = sum(1 for r in results if r['has_duplicates'])
    exact_matches = sum(1 for r in results if r['is_exact_match'])
    
    return jsonify({
        'total_records': len(records),
        'duplicates_found': duplicates_found,
        'exact_matches': exact_matches,
        'results': results
    })


@duplicate_detection_bp.route('/api/duplicate-detection/enforce', methods=['POST'])
@login_required
def enforce_duplicate_check():
    """Server-side enforcement for form submissions - MUST be called before saves"""
    data = request.get_json()
    record_type = data.get('record_type')
    field_values = data.get('field_values', {})
    override_token = data.get('override_token')
    exclude_id = data.get('exclude_id')
    
    if not record_type:
        return jsonify({'error': 'Missing record_type'}), 400
    
    if record_type not in RECORD_TYPE_CONFIG:
        return jsonify({'allowed': True, 'reason': 'Unknown record type'})
    
    user_role = session.get('role', 'User')
    
    service = get_duplicate_service()
    result = service.enforce_server_side(
        record_type=record_type,
        field_values=field_values,
        user_role=user_role,
        override_token=override_token,
        exclude_id=exclude_id
    )
    
    if not result['allowed']:
        service.log_detection_event(
            record_type=record_type,
            action_type='CREATE' if not exclude_id else 'UPDATE',
            source_data=field_values,
            duplicates=result.get('duplicates', []),
            user_decision='blocked',
            justification=result['reason'],
            user_id=session.get('user_id'),
            ip_address=request.remote_addr
        )
    
    return jsonify(result)


def get_view_url(record_type, record_id):
    """Generate view URL for a record"""
    url_mappings = {
        'customers': 'customer_routes.view_customer',
        'suppliers': 'supplier_routes.view_supplier',
        'products': 'product_routes.view_product',
        'work_orders': 'workorder_routes.view_workorder',
        'purchase_orders': 'po_routes.view_po',
        'sales_orders': 'so_routes.view_so',
        'assets': 'asset_routes.view_asset',
        'labor_resources': 'labor_routes.view_resource',
        'leads': 'leads_routes.view_lead'
    }
    
    route = url_mappings.get(record_type)
    if route:
        try:
            return url_for(route, id=record_id)
        except:
            pass
    
    return '#'
