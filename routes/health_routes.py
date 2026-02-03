"""
Health Check Routes for Production Monitoring
Implements /health, /health/db, /health/transactions endpoints
"""

from flask import Blueprint, jsonify
from datetime import datetime
import logging

logger = logging.getLogger('health_routes')

health_bp = Blueprint('health_routes', __name__)


@health_bp.route('/health')
def health_check():
    """Basic health check endpoint"""
    from utils.production_hardening import is_app_ready, get_startup_audit
    
    startup_audit = get_startup_audit()
    
    return jsonify({
        'status': 'healthy' if is_app_ready() else 'unhealthy',
        'timestamp': datetime.utcnow().isoformat(),
        'ready': is_app_ready(),
        'environment': startup_audit.get('environment', 'unknown') if startup_audit else 'unknown',
        'checks': {
            'startup_audit_passed': startup_audit.get('passed', False) if startup_audit else False
        }
    }), 200 if is_app_ready() else 503


@health_bp.route('/health/db')
def health_db():
    """Database health check with actual query test"""
    from utils.production_hardening import validate_database_connection, validate_critical_tables
    
    result = {
        'status': 'unknown',
        'timestamp': datetime.utcnow().isoformat(),
        'checks': {}
    }
    
    try:
        conn_result = validate_database_connection()
        result['checks']['connection'] = conn_result
    except Exception as e:
        result['checks']['connection'] = {'valid': False, 'error': str(e)}
        result['status'] = 'unhealthy'
        return jsonify(result), 503
    
    try:
        table_result = validate_critical_tables()
        result['checks']['critical_tables'] = {
            'valid': table_result['valid'],
            'tables_checked': table_result['tables_checked'],
            'missing_tables': table_result.get('missing_tables', []),
            'missing_columns_count': sum(len(cols) for cols in table_result.get('missing_columns', {}).values())
        }
    except Exception as e:
        result['checks']['critical_tables'] = {'valid': False, 'error': str(e)}
    
    all_valid = all(
        check.get('valid', False) 
        for check in result['checks'].values()
    )
    
    result['status'] = 'healthy' if all_valid else 'degraded'
    return jsonify(result), 200 if all_valid else 503


@health_bp.route('/health/transactions')
def health_transactions():
    """Transaction health check - performs a real write + rollback"""
    from models import Database
    
    result = {
        'status': 'unknown',
        'timestamp': datetime.utcnow().isoformat(),
        'test_results': {}
    }
    
    try:
        db = Database()
        conn = db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO audit_log (user_id, action, table_name, record_id, details, timestamp)
            VALUES (0, 'HEALTH_CHECK', 'health_test', 0, 'Transaction test - will rollback', datetime('now'))
        """)
        
        result['test_results']['write'] = {'success': True}
        
        conn.rollback()
        result['test_results']['rollback'] = {'success': True}
        
        cursor.execute("SELECT 1 as verify")
        verify = cursor.fetchone()
        result['test_results']['post_rollback_query'] = {'success': verify is not None}
        
        conn.close()
        result['test_results']['connection_close'] = {'success': True}
        
        result['status'] = 'healthy'
        result['message'] = 'Transaction write + rollback test passed'
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Transaction health check failed: {e}")
        result['status'] = 'unhealthy'
        result['error'] = str(e)
        result['test_results']['transaction'] = {'success': False, 'error': str(e)}
        
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        
        return jsonify(result), 503


@health_bp.route('/health/schema')
def health_schema():
    """Schema drift detection endpoint"""
    from utils.production_hardening import get_schema_drift_report
    
    result = {
        'status': 'unknown',
        'timestamp': datetime.utcnow().isoformat()
    }
    
    try:
        drift_report = get_schema_drift_report()
        result['drift_report'] = {
            'valid': drift_report['valid'],
            'tables_with_drift': drift_report.get('tables_with_drift', []),
            'missing_columns': drift_report.get('missing_columns', {}),
        }
        
        if drift_report['valid']:
            result['status'] = 'healthy'
            result['message'] = 'No schema drift detected'
        else:
            result['status'] = 'drift_detected'
            result['message'] = f"Schema drift in {len(drift_report.get('tables_with_drift', []))} tables"
        
        return jsonify(result), 200 if drift_report['valid'] else 409
        
    except Exception as e:
        logger.error(f"Schema health check failed: {e}")
        result['status'] = 'error'
        result['error'] = str(e)
        return jsonify(result), 500


@health_bp.route('/health/full')
def health_full():
    """Complete health check - all systems"""
    from utils.production_hardening import (
        is_app_ready, get_startup_audit, 
        validate_database_connection, validate_critical_tables
    )
    
    result = {
        'status': 'unknown',
        'timestamp': datetime.utcnow().isoformat(),
        'app_ready': is_app_ready(),
        'checks': {}
    }
    
    startup_audit = get_startup_audit()
    if startup_audit:
        result['checks']['startup_audit'] = {
            'passed': startup_audit['passed'],
            'critical_failures': startup_audit.get('critical_failures', []),
            'environment': startup_audit.get('environment', 'unknown')
        }
    
    try:
        conn_result = validate_database_connection()
        result['checks']['database'] = conn_result
    except Exception as e:
        result['checks']['database'] = {'valid': False, 'error': str(e)}
    
    try:
        table_result = validate_critical_tables()
        result['checks']['tables'] = {
            'valid': table_result['valid'],
            'tables_checked': table_result['tables_checked']
        }
    except Exception as e:
        result['checks']['tables'] = {'valid': False, 'error': str(e)}
    
    all_healthy = (
        is_app_ready() and 
        result['checks'].get('database', {}).get('valid', False) and
        result['checks'].get('tables', {}).get('valid', False)
    )
    
    result['status'] = 'healthy' if all_healthy else 'unhealthy'
    
    return jsonify(result), 200 if all_healthy else 503
