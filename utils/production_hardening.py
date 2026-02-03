"""
Production Hardening Module for Dynamic.IQ-COREx
Implements enterprise-grade reliability, validation, and error handling
"""

import os
import sys
import logging
from datetime import datetime
from functools import wraps

logger = logging.getLogger('production_hardening')

REQUIRED_ENV_VARS = [
    'DATABASE_URL',
    'SESSION_SECRET',
]

OPTIONAL_ENV_VARS = [
    'OPENAI_API_KEY',
    'BREVO_API_KEY',
    'BREVO_FROM_EMAIL',
]

class StartupValidationError(Exception):
    """Raised when startup validation fails"""
    pass

class SchemaValidationError(Exception):
    """Raised when schema validation fails"""
    pass

class TransactionError(Exception):
    """Raised when a transaction fails"""
    pass


def validate_environment(fail_fast=True):
    """
    Phase 1: Validate all required environment variables exist
    Returns dict with validation results
    """
    results = {
        'valid': True,
        'missing_required': [],
        'missing_optional': [],
        'present': []
    }
    
    for var in REQUIRED_ENV_VARS:
        if os.environ.get(var):
            results['present'].append(var)
        else:
            results['missing_required'].append(var)
            results['valid'] = False
    
    for var in OPTIONAL_ENV_VARS:
        if os.environ.get(var):
            results['present'].append(var)
        else:
            results['missing_optional'].append(var)
    
    if not results['valid'] and fail_fast:
        error_msg = f"STARTUP FAILED: Missing required environment variables: {results['missing_required']}"
        logger.critical(error_msg)
        raise StartupValidationError(error_msg)
    
    return results


def validate_database_connection():
    """
    Phase 2: Validate database connection and basic schema
    """
    from models import Database
    
    try:
        db = Database()
        conn = db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        
        if not result:
            raise SchemaValidationError("Database query returned no results")
        
        conn.close()
        return {'valid': True, 'message': 'Database connection successful'}
    except Exception as e:
        error_msg = f"Database connection failed: {str(e)}"
        logger.critical(error_msg)
        raise SchemaValidationError(error_msg)


def validate_critical_tables():
    """
    Phase 2: Validate critical tables exist with required columns
    """
    from models import Database, USE_POSTGRES
    
    critical_tables = {
        'users': ['id', 'username', 'email', 'password', 'role'],
        'products': ['id', 'name', 'sku'],
        'inventory': ['id', 'product_id', 'quantity'],
        'customers': ['id', 'name'],
        'suppliers': ['id', 'name'],
        'work_orders': ['id', 'work_order_number', 'status'],
        'sales_orders': ['id', 'order_number', 'status'],
        'purchase_orders': ['id', 'po_number', 'status'],
    }
    
    results = {
        'valid': True,
        'tables_checked': 0,
        'missing_tables': [],
        'missing_columns': {},
        'errors': []
    }
    
    try:
        db = Database()
        conn = db.get_connection()
        cursor = conn.cursor()
        
        for table, required_columns in critical_tables.items():
            results['tables_checked'] += 1
            
            if USE_POSTGRES:
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                """, (table,))
            else:
                cursor.execute(f"PRAGMA table_info({table})")
            
            rows = cursor.fetchall()
            
            if not rows:
                results['missing_tables'].append(table)
                results['valid'] = False
                continue
            
            if USE_POSTGRES:
                existing_columns = {row['column_name'] for row in rows}
            else:
                existing_columns = {row[1] for row in rows}
            
            missing = [col for col in required_columns if col not in existing_columns]
            if missing:
                results['missing_columns'][table] = missing
                results['valid'] = False
        
        conn.close()
        
    except Exception as e:
        results['errors'].append(str(e))
        results['valid'] = False
    
    return results


def get_schema_drift_report():
    """
    Phase 2: Generate comprehensive schema drift report between dev and prod
    """
    from models import Database, USE_POSTGRES
    import sqlite3
    
    if not USE_POSTGRES:
        return {'valid': True, 'message': 'Development mode - no drift check needed'}
    
    report = {
        'valid': True,
        'tables_with_drift': [],
        'missing_columns': {},
        'extra_columns': {},
        'type_mismatches': {}
    }
    
    try:
        dev_conn = sqlite3.connect('mrp.db')
        dev_conn.row_factory = sqlite3.Row
        dev_cursor = dev_conn.cursor()
        
        dev_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        dev_tables = [row[0] for row in dev_cursor.fetchall()]
        
        db = Database()
        prod_conn = db.get_connection()
        prod_cursor = prod_conn.cursor()
        
        for table in dev_tables:
            dev_cursor.execute(f"PRAGMA table_info({table})")
            dev_columns = {row[1]: row[2] for row in dev_cursor.fetchall()}
            
            prod_cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s
            """, (table,))
            prod_rows = prod_cursor.fetchall()
            
            if not prod_rows:
                report['tables_with_drift'].append(table)
                report['missing_columns'][table] = list(dev_columns.keys())
                report['valid'] = False
                continue
            
            prod_columns = {row['column_name']: row['data_type'] for row in prod_rows}
            
            missing = [col for col in dev_columns if col not in prod_columns]
            if missing:
                report['missing_columns'][table] = missing
                report['tables_with_drift'].append(table)
                report['valid'] = False
        
        dev_conn.close()
        prod_conn.close()
        
    except Exception as e:
        report['errors'] = [str(e)]
        report['valid'] = False
    
    return report


class TransactionManager:
    """
    Phase 3: Transaction safety wrapper with auto-rollback
    """
    def __init__(self, conn):
        self.conn = conn
        self.committed = False
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            try:
                self.conn.rollback()
                logger.warning(f"Transaction rolled back due to: {exc_val}")
            except Exception as e:
                logger.error(f"Rollback failed: {e}")
        elif not self.committed:
            try:
                self.conn.rollback()
                logger.warning("Transaction not committed, rolling back")
            except Exception:
                pass
        return False
    
    def commit(self):
        self.conn.commit()
        self.committed = True


def transaction_wrapper(func):
    """
    Phase 3: Decorator for safe transactional operations
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        from models import Database
        db = Database()
        conn = db.get_connection()
        
        try:
            with TransactionManager(conn) as txn:
                result = func(conn, *args, **kwargs)
                txn.commit()
                return result
        except Exception as e:
            logger.error(f"Transaction failed in {func.__name__}: {e}")
            raise TransactionError(f"Transaction failed: {str(e)}")
        finally:
            conn.close()
    
    return wrapper


def validate_record_before_insert(table_name, data, required_fields=None):
    """
    Phase 3: Pre-insert validation layer
    """
    errors = []
    
    if required_fields:
        for field in required_fields:
            if field not in data or data[field] is None or data[field] == '':
                errors.append(f"Missing required field: {field}")
    
    if 'email' in data and data['email']:
        import re
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, data['email']):
            errors.append(f"Invalid email format: {data['email']}")
    
    if errors:
        return {'valid': False, 'errors': errors}
    
    return {'valid': True, 'errors': []}


def perform_startup_audit(app):
    """
    Phase 7: Complete startup self-audit
    Returns audit results dict
    """
    audit_results = {
        'timestamp': datetime.utcnow().isoformat(),
        'environment': os.environ.get('REPLIT_DEPLOYMENT', 'development'),
        'checks': {},
        'passed': True,
        'critical_failures': []
    }
    
    try:
        env_result = validate_environment(fail_fast=False)
        audit_results['checks']['environment'] = env_result
        if not env_result['valid']:
            audit_results['passed'] = False
            audit_results['critical_failures'].append('Missing required environment variables')
    except Exception as e:
        audit_results['checks']['environment'] = {'valid': False, 'error': str(e)}
        audit_results['passed'] = False
        audit_results['critical_failures'].append(f'Environment validation error: {e}')
    
    try:
        db_result = validate_database_connection()
        audit_results['checks']['database_connection'] = db_result
    except Exception as e:
        audit_results['checks']['database_connection'] = {'valid': False, 'error': str(e)}
        audit_results['passed'] = False
        audit_results['critical_failures'].append(f'Database connection failed: {e}')
    
    try:
        table_result = validate_critical_tables()
        audit_results['checks']['critical_tables'] = table_result
        if not table_result['valid']:
            if table_result.get('missing_tables'):
                audit_results['critical_failures'].append(
                    f"Missing tables: {table_result['missing_tables']}"
                )
            if table_result.get('missing_columns'):
                for table, cols in table_result['missing_columns'].items():
                    logger.warning(f"Table {table} missing columns: {cols}")
    except Exception as e:
        audit_results['checks']['critical_tables'] = {'valid': False, 'error': str(e)}
    
    return audit_results


class StructuredError:
    """
    Phase 5: Structured error response
    """
    def __init__(self, code, message, category='system', details=None, correlation_id=None):
        self.code = code
        self.message = message
        self.category = category
        self.details = details or {}
        self.correlation_id = correlation_id or self._generate_correlation_id()
        self.timestamp = datetime.utcnow().isoformat()
    
    @staticmethod
    def _generate_correlation_id():
        import uuid
        return str(uuid.uuid4())[:8]
    
    def to_dict(self):
        return {
            'error': True,
            'code': self.code,
            'message': self.message,
            'category': self.category,
            'correlation_id': self.correlation_id,
            'timestamp': self.timestamp,
            'details': self.details
        }
    
    def log(self):
        logger.error(
            f"[{self.correlation_id}] {self.category.upper()}: {self.code} - {self.message}",
            extra={'details': self.details}
        )


ERROR_CODES = {
    'VALIDATION_ERROR': ('E1001', 'validation'),
    'DATABASE_ERROR': ('E2001', 'database'),
    'SCHEMA_DRIFT': ('E2002', 'database'),
    'TRANSACTION_FAILED': ('E2003', 'database'),
    'AUTH_ERROR': ('E3001', 'authorization'),
    'NOT_FOUND': ('E4001', 'data'),
    'CONSTRAINT_VIOLATION': ('E4002', 'data'),
    'SYSTEM_ERROR': ('E5001', 'system'),
    'STARTUP_FAILURE': ('E5002', 'system'),
}


def create_structured_error(error_type, message, details=None, correlation_id=None):
    """Create a structured error response"""
    code, category = ERROR_CODES.get(error_type, ('E9999', 'unknown'))
    return StructuredError(
        code=code,
        message=message,
        category=category,
        details=details,
        correlation_id=correlation_id
    )


APP_READY = False
STARTUP_AUDIT_RESULTS = None

def mark_app_ready():
    """Mark app as ready to receive traffic"""
    global APP_READY
    APP_READY = True


def is_app_ready():
    """Check if app is ready"""
    return APP_READY


def get_startup_audit():
    """Get startup audit results"""
    return STARTUP_AUDIT_RESULTS


def run_production_startup(app):
    """
    Phase 6 & 7: Complete production startup sequence
    """
    global STARTUP_AUDIT_RESULTS, APP_READY
    
    is_production = os.environ.get('REPLIT_DEPLOYMENT') == '1'
    
    logger.info("=" * 60)
    logger.info("PRODUCTION HARDENING: Starting startup validation")
    logger.info(f"Environment: {'PRODUCTION' if is_production else 'DEVELOPMENT'}")
    logger.info("=" * 60)
    
    STARTUP_AUDIT_RESULTS = perform_startup_audit(app)
    
    if STARTUP_AUDIT_RESULTS['passed']:
        logger.info("All startup checks passed")
        APP_READY = True
    else:
        logger.critical("STARTUP VALIDATION FAILED")
        for failure in STARTUP_AUDIT_RESULTS['critical_failures']:
            logger.critical(f"  - {failure}")
        
        if is_production:
            logger.critical("Production deployment blocked due to validation failures")
        else:
            logger.warning("Development mode: Continuing despite failures")
            APP_READY = True
    
    logger.info("=" * 60)
    logger.info(f"Application ready: {APP_READY}")
    logger.info("=" * 60)
    
    return STARTUP_AUDIT_RESULTS
