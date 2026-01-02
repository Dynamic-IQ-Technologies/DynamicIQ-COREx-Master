from flask import session, redirect, url_for, flash, request, jsonify
from functools import wraps
from models import User

def _is_json_request():
    """Check if the request expects a JSON response"""
    return (
        request.is_json or 
        request.headers.get('Content-Type', '').startswith('application/json') or
        request.headers.get('Accept', '').startswith('application/json') or
        request.headers.get('X-Requested-With') == 'XMLHttpRequest'
    )

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            if _is_json_request():
                return jsonify({'success': False, 'error': 'Authentication required. Please log in.'}), 401
            return redirect(url_for('auth_routes.login'))
        return f(*args, **kwargs)
    return decorated_function

def role_required(*roles):
    # Handle both @role_required(['Admin', 'Sales']) and @role_required('Admin', 'Sales')
    if len(roles) == 1 and isinstance(roles[0], (list, tuple)):
        allowed_roles = list(roles[0])
    else:
        allowed_roles = list(roles)
    
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'user_id' not in session:
                if _is_json_request():
                    return jsonify({'success': False, 'error': 'Authentication required. Please log in.'}), 401
                return redirect(url_for('auth_routes.login'))
            
            user = User.get_by_id(session['user_id'])
            if not user or user['role'] not in allowed_roles:
                if _is_json_request():
                    return jsonify({'success': False, 'error': 'You do not have permission to access this resource.'}), 403
                flash('You do not have permission to access this page.', 'danger')
                return redirect(url_for('main_routes.dashboard'))
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator
