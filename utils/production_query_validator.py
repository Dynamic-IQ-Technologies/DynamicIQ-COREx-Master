"""
Production Query Validator
Validates SQL queries work correctly in both SQLite and PostgreSQL environments.
Run before deploying to catch compatibility issues early.
"""

import os
import re
from typing import List, Dict, Tuple

class QueryCompatibilityChecker:
    """Checks SQL queries for PostgreSQL compatibility issues."""
    
    COMMON_ISSUES = [
        {
            'pattern': r'(?<!IS NULL OR )\b(is_core|is_replacement)\s*=\s*0\b(?!\s*\))',
            'description': 'NULL comparison issue: column = 0 will fail if column is NULL in PostgreSQL',
            'severity': 'error',
            'suggestion': 'Use COALESCE(column, 0) = 0 or (column IS NULL OR column = 0)'
        },
        {
            'pattern': r'(?<!COALESCE\()\b(is_core|is_replacement)\s*=\s*1\b',
            'description': 'NULL comparison issue: column = 1 will fail if column is NULL in PostgreSQL',
            'severity': 'error', 
            'suggestion': 'Use COALESCE(column, 0) = 1'
        },
        {
            'pattern': r'sales_order_lines\s+\w+.*\.\s*sales_order_id',
            'description': 'Column name mismatch - sales_order_lines uses so_id not sales_order_id',
            'severity': 'error',
            'suggestion': 'Use sol.so_id instead of sol.sales_order_id for sales_order_lines table'
        },
        {
            'pattern': r'IFNULL\s*\(',
            'description': 'SQLite-specific IFNULL function (not auto-translated)',
            'severity': 'error',
            'suggestion': 'Use COALESCE() which works in both databases'
        }
    ]
    
    INFO_ONLY_ISSUES = [
        {
            'pattern': r"datetime\s*\(\s*'now'\s*\)",
            'description': 'SQLite datetime function (auto-translated by PostgresTranslatingCursor)',
            'severity': 'info'
        },
        {
            'pattern': r"date\s*\(\s*'now'\s*\)",
            'description': 'SQLite date function (auto-translated)',
            'severity': 'info'
        },
        {
            'pattern': r'julianday\s*\(',
            'description': 'SQLite julianday function (auto-translated)',
            'severity': 'info'
        },
        {
            'pattern': r'strftime\s*\(',
            'description': 'SQLite strftime function (auto-translated)',
            'severity': 'info'
        },
        {
            'pattern': r'GROUP_CONCAT\s*\(',
            'description': 'SQLite GROUP_CONCAT function (auto-translated to STRING_AGG)',
            'severity': 'info'
        }
    ]
    
    NULLABLE_BOOLEAN_COLUMNS = [
        'is_core', 'is_replacement', 'is_serialized', 'is_exchange',
        'is_active', 'is_primary', 'is_pinned', 'is_read', 'is_resolved',
        'is_acknowledged', 'is_mandatory', 'is_critical', 'is_default'
    ]
    
    def check_query(self, query: str, context: str = '') -> List[Dict]:
        """Check a single query for compatibility issues."""
        issues = []
        
        for check in self.COMMON_ISSUES:
            matches = re.finditer(check['pattern'], query, re.IGNORECASE)
            for match in matches:
                column_match = match.group(1) if match.lastindex else match.group(0)
                
                if check['pattern'].startswith(r'\b(\w+)\s*=\s*[01]'):
                    if not any(col in column_match.lower() for col in self.NULLABLE_BOOLEAN_COLUMNS):
                        continue
                
                issues.append({
                    'context': context,
                    'match': match.group(0),
                    'position': match.start(),
                    'severity': check['severity'],
                    'description': check['description'],
                    'suggestion': check['suggestion']
                })
        
        return issues
    
    def scan_file(self, filepath: str) -> List[Dict]:
        """Scan a Python file for SQL queries and check them."""
        issues = []
        
        try:
            with open(filepath, 'r') as f:
                content = f.read()
        except Exception as e:
            return [{'error': f'Could not read file: {e}'}]
        
        sql_patterns = [
            r"conn\.execute\s*\(\s*'''(.*?)'''",
            r'conn\.execute\s*\(\s*"""(.*?)"""',
            r"conn\.execute\s*\(\s*'([^']+)'",
            r'conn\.execute\s*\(\s*"([^"]+)"',
            r"execute\s*\(\s*'''(.*?)'''",
            r'execute\s*\(\s*"""(.*?)"""',
        ]
        
        for pattern in sql_patterns:
            matches = re.finditer(pattern, content, re.DOTALL | re.IGNORECASE)
            for match in matches:
                query = match.group(1)
                line_num = content[:match.start()].count('\n') + 1
                context = f"{filepath}:{line_num}"
                
                query_issues = self.check_query(query, context)
                issues.extend(query_issues)
        
        return issues
    
    def scan_directory(self, directory: str) -> List[Dict]:
        """Scan all Python files in a directory for SQL compatibility issues."""
        all_issues = []
        
        for root, dirs, files in os.walk(directory):
            dirs[:] = [d for d in dirs if not d.startswith('.') and d != '__pycache__']
            
            for file in files:
                if file.endswith('.py'):
                    filepath = os.path.join(root, file)
                    file_issues = self.scan_file(filepath)
                    all_issues.extend(file_issues)
        
        return all_issues


def validate_query_in_production(query: str, params: tuple = None) -> Dict:
    """
    Test a query against the production database (read-only).
    Returns success status and any errors.
    """
    from models import Database
    
    result = {'success': False, 'error': None, 'row_count': 0}
    
    try:
        db = Database()
        conn = db.get_connection()
        
        if query.strip().upper().startswith('SELECT'):
            cursor = conn.execute(query, params or ())
            rows = cursor.fetchall()
            result['success'] = True
            result['row_count'] = len(rows)
        else:
            result['error'] = 'Only SELECT queries can be validated against production'
        
        conn.close()
    except Exception as e:
        result['error'] = str(e)
    
    return result


def run_compatibility_scan():
    """Run a full compatibility scan on the routes directory."""
    checker = QueryCompatibilityChecker()
    
    directories_to_scan = ['routes', 'mrp_logic.py']
    all_issues = []
    
    for path in directories_to_scan:
        if os.path.isdir(path):
            issues = checker.scan_directory(path)
        elif os.path.isfile(path):
            issues = checker.scan_file(path)
        else:
            continue
        all_issues.extend(issues)
    
    errors = [i for i in all_issues if i.get('severity') == 'error']
    warnings = [i for i in all_issues if i.get('severity') == 'warning']
    
    print(f"\n{'='*60}")
    print("SQL Compatibility Scan Results")
    print(f"{'='*60}")
    print(f"Errors: {len(errors)}")
    print(f"Warnings: {len(warnings)}")
    
    if errors:
        print(f"\n{'='*60}")
        print("ERRORS (must fix before deploying):")
        print(f"{'='*60}")
        for issue in errors:
            print(f"\n  Location: {issue.get('context', 'Unknown')}")
            print(f"  Match: {issue.get('match', '')[:50]}...")
            print(f"  Issue: {issue.get('description', '')}")
            print(f"  Fix: {issue.get('suggestion', '')}")
    
    if warnings:
        print(f"\n{'='*60}")
        print("WARNINGS (review for potential issues):")
        print(f"{'='*60}")
        for issue in warnings[:20]:
            print(f"\n  Location: {issue.get('context', 'Unknown')}")
            print(f"  Match: {issue.get('match', '')[:50]}...")
            print(f"  Issue: {issue.get('description', '')}")
    
    return {'errors': errors, 'warnings': warnings}


if __name__ == '__main__':
    run_compatibility_scan()
