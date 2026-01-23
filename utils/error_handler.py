"""
Centralized Error Handling Utilities for Dynamic.IQ-COREx

Provides:
- Route decorator for automatic error handling
- Safe data access utilities 
- Input validation helpers
- Standardized error responses
"""
import functools
import traceback
import logging
from flask import request, jsonify, render_template, current_app, g
from decimal import Decimal
from datetime import datetime, date

error_logger = logging.getLogger('route_error_handler')

class SafeDataAccessor:
    """Safe wrapper for dictionary/object access with fallbacks"""
    
    @staticmethod
    def get(data, key, default=None):
        """Safely get a value from dict or object with fallback"""
        if data is None:
            return default
        if isinstance(data, dict):
            return data.get(key, default)
        return getattr(data, key, default)
    
    @staticmethod
    def safe_float(value, default=0.0):
        """Convert value to float safely, handling Decimal and None"""
        if value is None:
            return default
        try:
            if isinstance(value, Decimal):
                return float(value)
            return float(value)
        except (TypeError, ValueError):
            return default
    
    @staticmethod
    def safe_int(value, default=0):
        """Convert value to int safely"""
        if value is None:
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
    
    @staticmethod
    def safe_str(value, default=''):
        """Convert value to string safely"""
        if value is None:
            return default
        try:
            return str(value)
        except:
            return default
    
    @staticmethod
    def safe_list(value, default=None):
        """Ensure value is a list"""
        if default is None:
            default = []
        if value is None:
            return default
        if isinstance(value, (list, tuple)):
            return list(value)
        return default

safe = SafeDataAccessor()

def validate_required_fields(data, required_fields):
    """
    Validate that required fields exist and are not empty in request data.
    Returns tuple: (is_valid, error_message)
    """
    if data is None:
        return False, "No data provided"
    
    missing = []
    for field in required_fields:
        value = data.get(field) if isinstance(data, dict) else getattr(data, field, None)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing.append(field)
    
    if missing:
        return False, f"Missing required fields: {', '.join(missing)}"
    return True, None

def validate_numeric_field(value, field_name, min_val=None, max_val=None, allow_none=False):
    """
    Validate a numeric field.
    Returns tuple: (is_valid, parsed_value_or_error)
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        if allow_none:
            return True, None
        return False, f"{field_name} is required"
    
    try:
        num_value = float(value)
        if min_val is not None and num_value < min_val:
            return False, f"{field_name} must be at least {min_val}"
        if max_val is not None and num_value > max_val:
            return False, f"{field_name} must be at most {max_val}"
        return True, num_value
    except (TypeError, ValueError):
        return False, f"{field_name} must be a valid number"

def validate_id_param(value, param_name="ID"):
    """
    Validate an ID parameter (typically from URL).
    Returns tuple: (is_valid, parsed_value_or_error)
    """
    if value is None:
        return False, f"{param_name} is required"
    try:
        id_val = int(value)
        if id_val <= 0:
            return False, f"{param_name} must be a positive integer"
        return True, id_val
    except (TypeError, ValueError):
        return False, f"Invalid {param_name} format"

def create_error_response(message, error_code="VALIDATION_ERROR", status_code=400, category="Validation"):
    """Create a standardized error response"""
    correlation_id = getattr(request, 'correlation_id', 'N/A')
    
    if request.is_json or request.headers.get('Accept', '').startswith('application/json'):
        return jsonify({
            'success': False,
            'error': message,
            'errorCode': error_code,
            'category': category,
            'correlation_id': correlation_id
        }), status_code
    
    return render_template('errors/error.html',
                          error_code=status_code,
                          error_title='Error',
                          error_message=message,
                          correlation_id=correlation_id,
                          category=category), status_code

def create_success_response(data=None, message=None):
    """Create a standardized success response"""
    response = {'success': True}
    if message:
        response['message'] = message
    if data:
        response['data'] = data
    return jsonify(response)

def handle_route_error(error, route_name=None):
    """
    Handle an error from a route, log it, and return appropriate response.
    """
    correlation_id = getattr(request, 'correlation_id', 'N/A')
    error_type = type(error).__name__
    
    error_logger.error(
        f"[{correlation_id}] Route Error in {route_name or request.endpoint}: "
        f"{error_type}: {str(error)}\n{traceback.format_exc()}"
    )
    
    user_message = "An unexpected error occurred. Please try again."
    
    if isinstance(error, ValueError):
        return create_error_response(str(error), "VALIDATION_ERROR", 400, "Validation")
    elif isinstance(error, KeyError):
        return create_error_response("Required data is missing", "MISSING_DATA", 400, "Data")
    elif isinstance(error, TypeError):
        return create_error_response(user_message, "TYPE_ERROR", 500, "System")
    elif isinstance(error, AttributeError):
        return create_error_response(user_message, "ATTRIBUTE_ERROR", 500, "System")
    else:
        return create_error_response(user_message, "SYSTEM_RUNTIME_ERROR", 500, "System")

def route_error_handler(func):
    """
    Decorator for Flask routes that provides automatic error handling.
    
    Usage:
        @bp.route('/some-path')
        @route_error_handler
        def some_route():
            ...
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as error:
            return handle_route_error(error, func.__name__)
    return wrapper

def api_error_handler(func):
    """
    Decorator for API routes that always returns JSON.
    
    Usage:
        @bp.route('/api/some-path')
        @api_error_handler
        def some_api():
            ...
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        correlation_id = getattr(request, 'correlation_id', 'N/A')
        try:
            result = func(*args, **kwargs)
            return result
        except ValueError as e:
            error_logger.warning(f"[{correlation_id}] Validation error in {func.__name__}: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'errorCode': 'VALIDATION_ERROR',
                'category': 'Validation',
                'correlation_id': correlation_id
            }), 400
        except KeyError as e:
            error_logger.warning(f"[{correlation_id}] Missing key in {func.__name__}: {e}")
            return jsonify({
                'success': False,
                'error': f'Missing required field: {e}',
                'errorCode': 'MISSING_FIELD',
                'category': 'Validation',
                'correlation_id': correlation_id
            }), 400
        except Exception as error:
            error_logger.error(
                f"[{correlation_id}] API Error in {func.__name__}: "
                f"{type(error).__name__}: {str(error)}\n{traceback.format_exc()}"
            )
            return jsonify({
                'success': False,
                'error': 'An unexpected error occurred. Please try again.',
                'errorCode': 'SYSTEM_RUNTIME_ERROR',
                'category': 'System',
                'correlation_id': correlation_id,
                'error_type': type(error).__name__
            }), 500
    return wrapper

def safe_render_template(template_name, **context):
    """
    Safely render a template with error handling for missing data.
    Converts all Decimal values to float and handles None values.
    """
    def convert_decimals(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, dict):
            return {k: convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [convert_decimals(item) for item in obj]
        return obj
    
    safe_context = {}
    for key, value in context.items():
        safe_context[key] = convert_decimals(value)
    
    return render_template(template_name, **safe_context)

def get_request_json_safe(silent=True):
    """Safely get JSON from request, returning empty dict on failure"""
    try:
        data = request.get_json(silent=silent)
        return data if data is not None else {}
    except Exception:
        return {}

def get_form_value(key, default=None, type_func=None):
    """Safely get a form value with optional type conversion"""
    value = request.form.get(key, default)
    if value is None or value == '':
        return default
    if type_func:
        try:
            return type_func(value)
        except (TypeError, ValueError):
            return default
    return value

def get_query_param(key, default=None, type_func=None):
    """Safely get a query parameter with optional type conversion"""
    value = request.args.get(key, default)
    if value is None or value == '':
        return default
    if type_func:
        try:
            return type_func(value)
        except (TypeError, ValueError):
            return default
    return value

class DatabaseSafeExecutor:
    """Context manager for safe database operations"""
    
    def __init__(self, db_connection, operation_name="database operation"):
        self.conn = db_connection
        self.operation_name = operation_name
        self.error = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.error = exc_val
            correlation_id = getattr(request, 'correlation_id', 'N/A') if request else 'N/A'
            error_logger.error(
                f"[{correlation_id}] Database error during {self.operation_name}: "
                f"{exc_type.__name__}: {exc_val}"
            )
            try:
                if hasattr(self.conn, 'rollback'):
                    self.conn.rollback()
            except:
                pass
            return False
        return False
    
    def execute_safe(self, query, params=None, default=None):
        """Execute a query safely, returning default on error"""
        try:
            if params:
                return self.conn.execute(query, params)
            return self.conn.execute(query)
        except Exception as e:
            self.error = e
            correlation_id = getattr(request, 'correlation_id', 'N/A') if request else 'N/A'
            error_logger.error(
                f"[{correlation_id}] Query error in {self.operation_name}: {e}"
            )
            return default

def check_env_vars(*required_vars):
    """
    Check that required environment variables are set.
    Returns list of missing variables.
    """
    import os
    missing = []
    for var in required_vars:
        if not os.environ.get(var):
            missing.append(var)
    return missing
