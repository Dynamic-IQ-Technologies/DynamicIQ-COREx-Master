from flask import Flask, session, render_template, request, jsonify
from models import Database, User
import uuid
import traceback
import logging
from datetime import datetime
from routes.auth_routes import auth_bp
from routes.main_routes import main_bp
from routes.product_routes import product_bp
from routes.bom_routes import bom_bp
from routes.supplier_routes import supplier_bp
from routes.inventory_routes import inventory_bp
from routes.workorder_routes import workorder_bp
from routes.purchaseorder_routes import po_bp
from routes.receiving_routes import receiving_bp
from routes.issuance_routes import issuance_bp
from routes.returns_routes import returns_bp
from routes.adjustment_routes import adjustment_bp
from routes.report_routes import report_bp
from routes.user_routes import user_bp
from routes.permission_routes import permission_bp
from routes.settings_routes import settings_bp
from routes.task_routes import task_bp
from routes.task_template_routes import task_template_bp
from routes.labor_routes import labor_bp
from routes.skillset_routes import skillset_bp
from routes.labor_issuance_routes import labor_issuance_bp
from routes.accounting_routes import accounting_bp
from routes.journal_routes import journal_bp
from routes.financial_reports_routes import financial_reports_bp
from routes.time_tracking_routes import time_tracking_bp
from routes.uom_routes import uom_bp
from routes.audit_routes import audit_bp
from routes.ap_routes import ap_bp
from routes.ar_routes import ar_bp
from routes.executive_routes import executive_routes
from routes.customer_routes import customer_bp
from routes.salesorder_routes import salesorder_bp
from routes.clock_station_routes import clock_station_bp
from routes.shipping_routes import shipping_bp
from routes.invoice_routes import invoice_bp
from routes.quote_routes import quote_bp
from routes.service_wo_routes import service_wo_bp
from routes.capability_routes import capability_bp
from routes.market_analysis_routes import market_analysis_bp
from routes.supplier_discovery_routes import supplier_discovery_bp
from routes.capacity_routes import capacity_bp
from routes.customer_service_routes import customer_service_bp
from routes.portal_routes import portal_bp
from routes.tools_routes import tools_bp
from routes.rfq_routes import rfq_bp
from routes.rfq_portal_routes import rfq_portal_bp
from routes.document_template_routes import document_template_bp
from routes.org_analyzer_routes import org_analyzer_bp
from routes.financial_analyzer_routes import financial_analyzer_bp
from routes.erp_helper_routes import erp_helper_bp
from routes.ndt_routes import ndt_bp
from routes.master_scheduler_routes import master_scheduler_bp
from routes.salesforce_migration_routes import sf_migration_bp
from routes.business_analytics_routes import business_analytics_bp
from routes.it_manager_routes import it_manager_bp
from routes.qms_routes import qms_bp
from routes.exchange_routes import exchange_bp
from routes.part_intake_routes import part_intake_bp
from routes.operations_routes import operations_bp
from routes.repair_order_routes import repair_order_bp
from routes.master_routing_routes import master_routing_bp
from routes.sales_dashboard_routes import sales_dashboard_bp
from routes.procurement_dashboard_routes import procurement_dashboard_bp
from routes.leads_routes import leads_bp
from routes.neuroiq_routes import neuroiq_bp
from routes.corex_guide_routes import corex_guide_bp
from routes.duplicate_detection_routes import duplicate_detection_bp
from routes.unplanned_receipt_routes import unplanned_receipt_bp
from routes.health_routes import health_bp
import os

app = Flask(__name__)
app.secret_key = os.environ.get('SESSION_SECRET', 'dev-secret-key-change-in-production')

@app.template_filter('currency')
def currency_filter(value):
    """Format a number as currency: $ 8,750.00"""
    try:
        if value is None:
            return '$ 0.00'
        num = float(value)
        if num < 0:
            return '-$ {:,.2f}'.format(abs(num))
        return '$ {:,.2f}'.format(num)
    except (ValueError, TypeError):
        return '$ 0.00'

@app.template_filter('format_date')
def format_date_filter(value):
    """Format a date string for display in standard 12-hour format."""
    if value is None or value == '':
        return '-'
    try:
        from datetime import datetime
        if isinstance(value, str):
            # Try common date formats
            for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%m/%d/%Y'):
                try:
                    dt = datetime.strptime(value, fmt)
                    return dt.strftime('%m/%d/%Y')
                except ValueError:
                    continue
            return value
        elif hasattr(value, 'strftime'):
            return value.strftime('%m/%d/%Y')
        return str(value)
    except Exception:
        return str(value) if value else '-'

@app.template_filter('datestr')
def datestr_filter(value, length=10):
    """Extract date string from datetime or string. Handles PostgreSQL datetime objects and SQLite strings."""
    if value is None or value == '':
        return ''
    try:
        from datetime import datetime, date
        if isinstance(value, (datetime, date)):
            if length == 10:
                return value.strftime('%Y-%m-%d')
            elif length == 16:
                return value.strftime('%Y-%m-%d %H:%M')
            elif length == 19:
                return value.strftime('%Y-%m-%d %H:%M:%S')
            else:
                return value.strftime('%Y-%m-%d')
        elif isinstance(value, str):
            return value[:length]
        return str(value)[:length]
    except Exception:
        return str(value)[:length] if value else ''

@app.template_filter('sf')
def safe_float_filter(value, default=0):
    """Safely convert Decimal/numeric values to float for Jinja formatting.
    PostgreSQL returns Decimal objects which can cause TypeError with format filters."""
    from decimal import Decimal
    if value is None:
        return float(default)
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)

# Override Jinja's default format filter to handle Decimal values
original_format = app.jinja_env.filters.get('format')
def safe_format(value, *args, **kwargs):
    """Safe format filter that converts Decimal to float before formatting."""
    from decimal import Decimal
    if isinstance(value, Decimal):
        value = float(value)
    elif value is None:
        value = 0
    if original_format:
        return original_format(value, *args, **kwargs)
    return format(value, *args) if args else str(value)
app.jinja_env.filters['format'] = safe_format

# Override Jinja's round filter to handle Decimal values
original_round = app.jinja_env.filters.get('round')
def safe_round(value, precision=0, method='common'):
    """Safe round filter that converts Decimal to float before rounding."""
    from decimal import Decimal
    if value is None:
        return 0
    if isinstance(value, Decimal):
        value = float(value)
    if original_round:
        return original_round(value, precision, method)
    return round(value, precision)
app.jinja_env.filters['round'] = safe_round

@app.template_filter('money')
def money_filter(value):
    """Format value as money with comma separators and 2 decimal places."""
    return "{:,.2f}".format(safe_float_filter(value))

@app.template_filter('num')
def num_filter(value, decimals=2):
    """Format value as number with specified decimal places."""
    return "{:.{d}f}".format(safe_float_filter(value), d=decimals)

def safe_format(format_string, *args):
    """Safe format function that converts Decimal values to float before formatting."""
    from decimal import Decimal
    converted_args = []
    for arg in args:
        if isinstance(arg, Decimal):
            converted_args.append(float(arg))
        else:
            converted_args.append(arg)
    return format_string.format(*converted_args)

app.jinja_env.globals['fmt'] = safe_format

def safe_get(obj, key, default=None):
    """Safely get a value from dict or object, returning default if missing or None."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        val = obj.get(key)
        return val if val is not None else default
    val = getattr(obj, key, None)
    return val if val is not None else default

def safe_int(value, default=0):
    """Safely convert to int."""
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def safe_float(value, default=0.0):
    """Safely convert to float, handling Decimal."""
    from decimal import Decimal
    if value is None:
        return default
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def safe_str(value, default=''):
    """Safely convert to string."""
    if value is None:
        return default
    try:
        return str(value)
    except:
        return default

def coalesce(*values):
    """Return first non-None value, like SQL COALESCE."""
    for v in values:
        if v is not None:
            return v
    return None

def format_dt(value, fmt='%Y-%m-%d %H:%M'):
    """Format datetime safely - handles both string (SQLite) and datetime (PostgreSQL) values."""
    if value is None:
        return ''
    if isinstance(value, str):
        return value[:16] if fmt == '%Y-%m-%d %H:%M' else value[:19]
    try:
        return value.strftime(fmt)
    except (AttributeError, ValueError):
        return str(value)[:16] if fmt == '%Y-%m-%d %H:%M' else str(value)[:19]

def is_past_date(value):
    """Check if a date is in the past - handles both string (SQLite) and date (PostgreSQL) values."""
    from datetime import date, datetime
    if value is None:
        return False
    if isinstance(value, str):
        try:
            parsed = datetime.strptime(value[:10], '%Y-%m-%d').date()
            return parsed < date.today()
        except (ValueError, TypeError):
            return False
    if isinstance(value, datetime):
        return value.date() < date.today()
    if isinstance(value, date):
        return value < date.today()
    return False

app.jinja_env.globals['safe_get'] = safe_get
app.jinja_env.globals['safe_int'] = safe_int
app.jinja_env.globals['safe_float'] = safe_float
app.jinja_env.globals['safe_str'] = safe_str
app.jinja_env.globals['coalesce'] = coalesce
app.jinja_env.globals['format_dt'] = format_dt
app.jinja_env.globals['is_past_date'] = is_past_date

@app.context_processor
def inject_now():
    """Make datetime.now available to all templates."""
    from datetime import datetime
    return {'now': datetime.now}

app.register_blueprint(auth_bp)
app.register_blueprint(main_bp)
app.register_blueprint(product_bp)
app.register_blueprint(bom_bp)
app.register_blueprint(supplier_bp)
app.register_blueprint(inventory_bp)
app.register_blueprint(workorder_bp)
app.register_blueprint(po_bp)
app.register_blueprint(receiving_bp)
app.register_blueprint(issuance_bp)
app.register_blueprint(returns_bp)
app.register_blueprint(adjustment_bp)
app.register_blueprint(report_bp)
app.register_blueprint(user_bp)
app.register_blueprint(permission_bp)
app.register_blueprint(settings_bp)
app.register_blueprint(task_bp)
app.register_blueprint(task_template_bp)
app.register_blueprint(labor_bp)
app.register_blueprint(skillset_bp)
app.register_blueprint(labor_issuance_bp)
app.register_blueprint(accounting_bp)
app.register_blueprint(journal_bp)
app.register_blueprint(financial_reports_bp)
app.register_blueprint(time_tracking_bp)
app.register_blueprint(uom_bp)
app.register_blueprint(audit_bp)
app.register_blueprint(ap_bp)
app.register_blueprint(ar_bp)
app.register_blueprint(executive_routes)
app.register_blueprint(customer_bp)
app.register_blueprint(salesorder_bp)
app.register_blueprint(clock_station_bp)
app.register_blueprint(shipping_bp)
app.register_blueprint(invoice_bp)
app.register_blueprint(quote_bp)
app.register_blueprint(service_wo_bp)
app.register_blueprint(capability_bp)
app.register_blueprint(market_analysis_bp)
app.register_blueprint(supplier_discovery_bp)
app.register_blueprint(capacity_bp)
app.register_blueprint(customer_service_bp)
app.register_blueprint(portal_bp)
app.register_blueprint(tools_bp)
app.register_blueprint(rfq_bp)
app.register_blueprint(rfq_portal_bp)
app.register_blueprint(document_template_bp)
app.register_blueprint(org_analyzer_bp)
app.register_blueprint(financial_analyzer_bp)
app.register_blueprint(erp_helper_bp)
app.register_blueprint(ndt_bp)
app.register_blueprint(master_scheduler_bp)
app.register_blueprint(sf_migration_bp)
app.register_blueprint(business_analytics_bp)
app.register_blueprint(it_manager_bp)
app.register_blueprint(qms_bp)
app.register_blueprint(exchange_bp)
app.register_blueprint(part_intake_bp)
app.register_blueprint(operations_bp)
app.register_blueprint(repair_order_bp)
app.register_blueprint(master_routing_bp)
app.register_blueprint(sales_dashboard_bp)
app.register_blueprint(procurement_dashboard_bp)
app.register_blueprint(leads_bp)
app.register_blueprint(neuroiq_bp)
app.register_blueprint(corex_guide_bp)
app.register_blueprint(duplicate_detection_bp)
app.register_blueprint(unplanned_receipt_bp)
app.register_blueprint(health_bp)

@app.context_processor
def inject_user():
    user = None
    user_permissions = {}
    if 'user_id' in session:
        user = User.get_by_id(session['user_id'])
        user_permissions = User.get_permissions(session['user_id'])
    return dict(user=user, user_permissions=user_permissions)

logging.basicConfig(level=logging.INFO)
error_logger = logging.getLogger('error_handler')

@app.before_request
def before_request():
    request.correlation_id = str(uuid.uuid4())[:8]
    request.start_time = datetime.now()

@app.errorhandler(400)
def bad_request_error(error):
    correlation_id = getattr(request, 'correlation_id', 'N/A')
    error_logger.warning(f"[{correlation_id}] Bad Request: {error}")
    if request.is_json:
        return jsonify({
            'error': 'Bad Request',
            'message': str(error.description) if hasattr(error, 'description') else 'Invalid request data',
            'category': 'Validation',
            'correlation_id': correlation_id
        }), 400
    return render_template('errors/error.html', 
                          error_code=400, 
                          error_title='Bad Request',
                          error_message='The request could not be processed. Please check your input and try again.',
                          correlation_id=correlation_id,
                          category='Validation'), 400

@app.errorhandler(401)
def unauthorized_error(error):
    correlation_id = getattr(request, 'correlation_id', 'N/A')
    error_logger.warning(f"[{correlation_id}] Unauthorized: {request.path}")
    if request.is_json:
        return jsonify({
            'error': 'Unauthorized',
            'message': 'Authentication required',
            'category': 'Authorization',
            'correlation_id': correlation_id
        }), 401
    return render_template('errors/error.html',
                          error_code=401,
                          error_title='Unauthorized',
                          error_message='Please log in to access this resource.',
                          correlation_id=correlation_id,
                          category='Authorization'), 401

@app.errorhandler(403)
def forbidden_error(error):
    correlation_id = getattr(request, 'correlation_id', 'N/A')
    error_logger.warning(f"[{correlation_id}] Forbidden: {request.path}")
    if request.is_json:
        return jsonify({
            'error': 'Forbidden',
            'message': 'You do not have permission to access this resource',
            'category': 'Authorization',
            'correlation_id': correlation_id
        }), 403
    return render_template('errors/error.html',
                          error_code=403,
                          error_title='Access Denied',
                          error_message='You do not have permission to access this resource.',
                          correlation_id=correlation_id,
                          category='Authorization'), 403

@app.errorhandler(404)
def not_found_error(error):
    correlation_id = getattr(request, 'correlation_id', 'N/A')
    error_logger.info(f"[{correlation_id}] Not Found: {request.path}")
    if request.is_json:
        return jsonify({
            'error': 'Not Found',
            'message': 'The requested resource was not found',
            'category': 'Data',
            'correlation_id': correlation_id
        }), 404
    return render_template('errors/error.html',
                          error_code=404,
                          error_title='Page Not Found',
                          error_message='The page you are looking for does not exist or has been moved.',
                          correlation_id=correlation_id,
                          category='Data'), 404

@app.errorhandler(500)
def internal_error(error):
    correlation_id = getattr(request, 'correlation_id', 'N/A')
    error_logger.error(f"[{correlation_id}] Internal Error: {request.path}\n{traceback.format_exc()}")
    if request.is_json:
        return jsonify({
            'error': 'Internal Server Error',
            'message': 'An unexpected error occurred. Please try again or contact support.',
            'category': 'System',
            'correlation_id': correlation_id
        }), 500
    return render_template('errors/error.html',
                          error_code=500,
                          error_title='Internal Server Error',
                          error_message='An unexpected error occurred. Please try again. If the problem persists, contact your administrator.',
                          correlation_id=correlation_id,
                          category='System'), 500

@app.errorhandler(Exception)
def handle_exception(error):
    correlation_id = getattr(request, 'correlation_id', 'N/A')
    error_logger.error(f"[{correlation_id}] Unhandled Exception: {request.path}\n{type(error).__name__}: {str(error)}\n{traceback.format_exc()}")
    
    # Show detailed error for diagnostic endpoint
    if request.path == '/dashboard-diagnostic':
        return jsonify({
            'error': type(error).__name__,
            'message': str(error),
            'traceback': traceback.format_exc(),
            'path': request.path
        }), 500
    
    if request.is_json:
        return jsonify({
            'error': 'Internal Server Error',
            'message': 'An unexpected error occurred. Please try again or contact support.',
            'category': 'System',
            'correlation_id': correlation_id,
            'error_type': type(error).__name__
        }), 500
    return render_template('errors/error.html',
                          error_code=500,
                          error_title='Something Went Wrong',
                          error_message=f'An unexpected error occurred ({type(error).__name__}). Please try again. If the problem persists, contact your administrator.',
                          correlation_id=correlation_id,
                          category='System',
                          error_detail=str(error) if app.debug else None), 500

def validate_environment():
    """Validate critical environment variables at startup"""
    warnings = []
    
    if not os.environ.get('SESSION_SECRET'):
        warnings.append("SESSION_SECRET not set - using default (insecure for production)")
    
    if os.environ.get('DATABASE_URL'):
        print("[Startup] PostgreSQL DATABASE_URL detected")
    else:
        print("[Startup] Using SQLite database (no DATABASE_URL)")
    
    if os.environ.get('REPLIT_DEPLOYMENT') == '1':
        print("[Startup] Running in PRODUCTION mode")
        if not os.environ.get('SESSION_SECRET'):
            print("WARNING: SESSION_SECRET should be set in production!")
    else:
        print("[Startup] Running in DEVELOPMENT mode")
    
    for warning in warnings:
        print(f"[Startup Warning] {warning}")
    
    return len(warnings) == 0

def initialize_application():
    """Run expensive initialization once at application startup"""
    validate_environment()
    
    db = Database()
    
    if db.use_postgres:
        try:
            conn = db.get_connection()
            result = conn.execute("SELECT COUNT(*) as cnt FROM users").fetchone()
            conn.close()
            print("[App] Using PostgreSQL database")
            # Seed data for PostgreSQL (if tables are empty)
            db.seed_chart_of_accounts()
            db.seed_unit_of_measure()
            db.seed_qms_sop_categories()
        except Exception as e:
            print("=" * 60)
            print("PostgreSQL database not initialized!")
            print("Please run: python scripts/init_postgres.py")
            print("=" * 60)
            raise SystemExit(f"Database not initialized: {e}")
    else:
        print("[App] Using SQLite database")
        db.init_db()
        db.seed_chart_of_accounts()
        db.seed_unit_of_measure()
        db.seed_qms_sop_categories()
    
    try:
        from services.exchange_chain_service import get_exchange_chain_service
        exchange_service = get_exchange_chain_service()
        exchange_service.load_graph_from_database()
    except Exception as e:
        print(f"Warning: Could not load exchange graph: {e}")
    
    try:
        from utils.production_hardening import run_production_startup
        run_production_startup(app)
    except Exception as e:
        print(f"[Startup] Production hardening check: {e}")

initialize_application()

if __name__ == '__main__':
    import os
    is_production = os.environ.get('REPLIT_DEPLOYMENT') == '1'
    app.run(host='0.0.0.0', port=5000, debug=not is_production)
