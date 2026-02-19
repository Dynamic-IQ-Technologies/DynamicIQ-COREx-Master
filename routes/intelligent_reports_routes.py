import json
import csv
import io
import logging
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify, session, Response
from auth import login_required
from models import Database
from services.reporting_engine import ReportingEngine, AVAILABLE_DATA_SOURCES

logger = logging.getLogger('intelligent_reports')

intelligent_reports_bp = Blueprint('intelligent_reports', __name__)

import os

def get_openai_client():
    from openai import OpenAI
    api_key = os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY', '') or os.environ.get('OPENAI_API_KEY', '')
    base_url = os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL', '') or None
    if not api_key:
        raise ValueError("OpenAI API key not configured")
    return OpenAI(
        api_key=api_key,
        base_url=base_url if base_url else None
    )

engine = ReportingEngine()


@intelligent_reports_bp.route('/intelligent-reports')
@login_required
def reports_dashboard():
    user_id = session.get('user_id')
    user_role = session.get('role', 'user')
    saved_reports = engine.get_saved_reports(user_id, user_role)
    return render_template('intelligent_reports/dashboard.html',
                           data_sources=AVAILABLE_DATA_SOURCES,
                           saved_reports=saved_reports)


@intelligent_reports_bp.route('/intelligent-reports/api/sources')
@login_required
def get_sources():
    sources = {}
    for key, src in AVAILABLE_DATA_SOURCES.items():
        sources[key] = {
            'label': src['label'],
            'fields': {f: info for f, info in src['fields'].items()}
        }
    return jsonify(sources)


@intelligent_reports_bp.route('/intelligent-reports/api/generate', methods=['POST'])
@login_required
def generate_report():
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'No configuration provided'}), 400

    config = data.get('config', {})
    if not config.get('data_source'):
        return jsonify({'success': False, 'error': 'Please select a data source'}), 400

    result = engine.execute_report(config)
    _log_action(None, 'executed', f"Builder report on {config.get('data_source')}")
    return jsonify(result)


@intelligent_reports_bp.route('/intelligent-reports/api/nl-generate', methods=['POST'])
@login_required
def nl_generate_report():
    data = request.get_json()
    prompt = data.get('prompt', '').strip()
    if not prompt:
        return jsonify({'success': False, 'error': 'Please enter a report description'}), 400

    try:
        client = get_openai_client()
        schema_desc = engine.get_schema_description()

        system_prompt = f"""You are an intelligent SQL report generator for an enterprise MRP/ERP system using PostgreSQL.
Given a natural language report request, generate a safe, read-only SQL SELECT query.

AVAILABLE SCHEMA (use ONLY these exact table and column names):
{schema_desc}

CRITICAL RULES:
1. Only generate SELECT queries. Never generate INSERT, UPDATE, DELETE, DROP, ALTER, or any data-modifying statements.
2. STRICTLY use ONLY the tables and columns listed above. Do NOT invent, guess, or assume any column names. If a column is not listed above, it does not exist.
3. Use proper PostgreSQL syntax.
4. For date filtering use CURRENT_DATE, INTERVAL syntax (e.g., CURRENT_DATE - INTERVAL '90 days').
5. Always include a LIMIT clause (max 500 rows unless specified).
6. For text searches use ILIKE with % wildcards.
7. Use aliases for readability.
8. Return ONLY the SQL query, no explanation, no markdown, no code fences.
9. For work order costs use material_cost, labor_cost, overhead_cost columns. Total cost = COALESCE(material_cost,0) + COALESCE(labor_cost,0) + COALESCE(overhead_cost,0).
10. Use LEFT JOIN when combining tables.
11. The inventory table uses 'condition' (NOT condition_code), 'warehouse_location' (NOT location), 'last_received_date' (NOT received_date).
12. The products table uses 'cost' (NOT unit_price), 'product_category' (NOT category).
13. The sales_orders table uses 'customer_id' (NOT customer_name). Join to a customers table is not available.
14. Do NOT end the query with a semicolon."""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=1000
        )

        sql_query = response.choices[0].message.content.strip()
        sql_query = sql_query.replace('```sql', '').replace('```', '').strip().rstrip(';').strip()

        validation_error = _validate_sql_safety(sql_query)
        if validation_error:
            return jsonify({'success': False, 'error': validation_error}), 400

        result = engine.execute_nl_query(sql_query)
        result['sql_query'] = sql_query
        result['nl_prompt'] = prompt
        _log_action(None, 'nl_generated', f"NL prompt: {prompt[:100]}")
        return jsonify(result)

    except Exception as e:
        logger.error(f"NL report generation error: {e}")
        return jsonify({'success': False, 'error': f'Could not generate report: {str(e)}'}), 500


@intelligent_reports_bp.route('/intelligent-reports/api/refine', methods=['POST'])
@login_required
def refine_report():
    data = request.get_json()
    original_sql = data.get('sql_query', '')
    refinement = data.get('refinement', '').strip()
    original_prompt = data.get('original_prompt', '')

    if not refinement or not original_sql:
        return jsonify({'success': False, 'error': 'Missing refinement or original query'}), 400

    try:
        client = get_openai_client()
        schema_desc = engine.get_schema_description()

        system_prompt = f"""You are an intelligent SQL report refiner for an enterprise MRP/ERP system using PostgreSQL.
You will be given an existing SQL query and a user refinement request. Modify the query to incorporate the refinement.

AVAILABLE SCHEMA:
{schema_desc}

RULES:
1. Only generate SELECT queries. Never generate data-modifying statements.
2. Only use tables and columns listed in the schema.
3. Preserve the original query structure where possible.
4. Apply the refinement without rebuilding from scratch.
5. Return ONLY the modified SQL query, no explanation, no markdown."""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Original prompt: {original_prompt}\n\nCurrent SQL:\n{original_sql}\n\nRefinement: {refinement}"}
            ],
            temperature=0.1,
            max_tokens=1000
        )

        sql_query = response.choices[0].message.content.strip()
        sql_query = sql_query.replace('```sql', '').replace('```', '').strip().rstrip(';').strip()

        validation_error = _validate_sql_safety(sql_query)
        if validation_error:
            return jsonify({'success': False, 'error': validation_error}), 400

        result = engine.execute_nl_query(sql_query)
        result['sql_query'] = sql_query
        result['nl_prompt'] = f"{original_prompt} | Refined: {refinement}"
        return jsonify(result)

    except Exception as e:
        logger.error(f"Report refinement error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@intelligent_reports_bp.route('/intelligent-reports/api/save', methods=['POST'])
@login_required
def save_report():
    data = request.get_json()
    user_id = session.get('user_id')
    report_id = engine.save_report(data, user_id)
    if report_id:
        return jsonify({'success': True, 'report_id': report_id})
    return jsonify({'success': False, 'error': 'Failed to save report'}), 500


@intelligent_reports_bp.route('/intelligent-reports/api/update/<int:report_id>', methods=['PUT'])
@login_required
def update_report(report_id):
    data = request.get_json()
    user_id = session.get('user_id')
    success = engine.update_report(report_id, data, user_id)
    if success:
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'Failed to update report'}), 500


@intelligent_reports_bp.route('/intelligent-reports/api/report/<int:report_id>')
@login_required
def get_report(report_id):
    report = engine.get_report(report_id)
    if report:
        report['created_at'] = report['created_at'].isoformat() if report.get('created_at') else None
        report['updated_at'] = report['updated_at'].isoformat() if report.get('updated_at') else None
        report['last_run_at'] = report['last_run_at'].isoformat() if report.get('last_run_at') else None
        return jsonify({'success': True, 'report': report})
    return jsonify({'success': False, 'error': 'Report not found'}), 404


@intelligent_reports_bp.route('/intelligent-reports/api/report/<int:report_id>/versions')
@login_required
def get_versions(report_id):
    versions = engine.get_report_versions(report_id)
    for v in versions:
        v['created_at'] = v['created_at'].isoformat() if v.get('created_at') else None
    return jsonify({'success': True, 'versions': versions})


@intelligent_reports_bp.route('/intelligent-reports/api/clone/<int:report_id>', methods=['POST'])
@login_required
def clone_report(report_id):
    user_id = session.get('user_id')
    new_id = engine.clone_report(report_id, user_id)
    if new_id:
        return jsonify({'success': True, 'report_id': new_id})
    return jsonify({'success': False, 'error': 'Failed to clone report'}), 500


@intelligent_reports_bp.route('/intelligent-reports/api/delete/<int:report_id>', methods=['DELETE'])
@login_required
def delete_report(report_id):
    user_id = session.get('user_id')
    report = engine.get_report(report_id)
    if report and report['owner_id'] != user_id and session.get('role') != 'admin':
        return jsonify({'success': False, 'error': 'Not authorized to delete this report'}), 403
    success = engine.delete_report(report_id, user_id)
    if success:
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'Failed to delete report'}), 500


@intelligent_reports_bp.route('/intelligent-reports/api/export/<string:format_type>', methods=['POST'])
@login_required
def export_report(format_type):
    data = request.get_json()
    report_data = data.get('data', [])
    report_name = data.get('name', 'Report')

    if not report_data:
        return jsonify({'success': False, 'error': 'No data to export'}), 400

    if format_type == 'csv':
        output = io.StringIO()
        if report_data:
            writer = csv.DictWriter(output, fieldnames=report_data[0].keys())
            writer.writeheader()
            writer.writerows(report_data)
        response = Response(output.getvalue(), mimetype='text/csv')
        response.headers['Content-Disposition'] = f'attachment; filename="{report_name}.csv"'
        _log_action(None, 'exported', f"CSV export: {report_name}")
        return response

    elif format_type == 'json':
        response = Response(json.dumps(report_data, indent=2, default=str), mimetype='application/json')
        response.headers['Content-Disposition'] = f'attachment; filename="{report_name}.json"'
        _log_action(None, 'exported', f"JSON export: {report_name}")
        return response

    return jsonify({'success': False, 'error': 'Unsupported export format'}), 400


@intelligent_reports_bp.route('/intelligent-reports/api/suggest-viz', methods=['POST'])
@login_required
def suggest_visualization():
    data = request.get_json()
    report_data = data.get('data', [])
    if not report_data:
        return jsonify({'suggestion': 'table', 'reason': 'No data available for visualization'})

    columns = list(report_data[0].keys()) if report_data else []
    num_rows = len(report_data)

    numeric_cols = []
    date_cols = []
    text_cols = []
    for col in columns:
        sample_val = report_data[0].get(col)
        if isinstance(sample_val, (int, float)):
            numeric_cols.append(col)
        elif isinstance(sample_val, str) and any(d in col.lower() for d in ['date', 'created', 'updated', 'time']):
            date_cols.append(col)
        else:
            text_cols.append(col)

    if date_cols and numeric_cols:
        suggestion = 'line'
        reason = 'Time-series data detected. A line chart will show trends over time.'
    elif len(numeric_cols) >= 1 and text_cols and num_rows <= 20:
        suggestion = 'bar'
        reason = 'Categorical data with numeric values detected. A bar chart will highlight comparisons.'
    elif len(numeric_cols) >= 2 and num_rows > 20:
        suggestion = 'line'
        reason = 'Multiple numeric columns with many rows. A line chart will reveal patterns.'
    elif 'status' in [c.lower() for c in columns] and num_rows <= 10:
        suggestion = 'doughnut'
        reason = 'Status distribution detected. A doughnut chart will show proportions.'
    else:
        suggestion = 'table'
        reason = 'Mixed data types. A table provides the clearest view of all fields.'

    return jsonify({'suggestion': suggestion, 'reason': reason})


import re

ALLOWED_TABLES = set(AVAILABLE_DATA_SOURCES[k]['table'] for k in AVAILABLE_DATA_SOURCES)

def _validate_sql_safety(sql_query):
    if not sql_query or not sql_query.strip():
        return 'Empty query'

    cleaned = sql_query.strip().rstrip(';').strip()

    if ';' in cleaned:
        return 'Multiple statements are not allowed. Please rephrase your request as a single query.'

    sql_upper = cleaned.upper()

    if not sql_upper.startswith('SELECT'):
        return 'Only SELECT queries are allowed.'

    dangerous_keywords = [
        'INSERT ', 'UPDATE ', 'DELETE ', 'DROP ', 'ALTER ', 'TRUNCATE ',
        'CREATE ', 'GRANT ', 'REVOKE ', 'EXEC ', 'EXECUTE ', 'COPY ',
        'LOAD ', 'INTO OUTFILE', 'INTO DUMPFILE', 'SET ROLE', 'SET SESSION',
        'CALL '
    ]
    for kw in dangerous_keywords:
        if kw in sql_upper:
            return f'Query contains unsafe operation. Please rephrase your request.'

    if re.search(r'--', cleaned) or re.search(r'/\*', cleaned):
        return 'SQL comments are not allowed for safety reasons.'

    cte_match = re.search(r'\bWITH\b', sql_upper)
    if cte_match:
        for kw in ['INSERT', 'UPDATE', 'DELETE', 'DROP']:
            if kw in sql_upper[cte_match.end():]:
                return f'CTE with data modification is not allowed.'

    referenced_tables = set()
    from_pattern = re.findall(r'\bFROM\s+(\w+)', sql_upper)
    join_pattern = re.findall(r'\bJOIN\s+(\w+)', sql_upper)
    for t in from_pattern + join_pattern:
        referenced_tables.add(t.lower())

    allowed_upper = {t.upper() for t in ALLOWED_TABLES}
    allowed_extra = {'PRODUCTS', 'SUPPLIERS', 'WORK_ORDERS', 'PURCHASE_ORDERS',
                     'SALES_ORDERS', 'INVENTORY', 'RFQS', 'INVOICES',
                     'PURCHASE_ORDER_LINES', 'SALES_ORDER_LINES', 'WORK_ORDER_MATERIALS',
                     'BOMS', 'BOM_LINES', 'USERS'}
    all_allowed = allowed_upper | allowed_extra

    for t in referenced_tables:
        if t.upper() not in all_allowed:
            return f'Table "{t}" is not available for reporting. Available tables: {", ".join(sorted(ALLOWED_TABLES))}'

    if not re.search(r'\bLIMIT\s+\d+', sql_upper):
        sql_query_with_limit = cleaned.rstrip().rstrip(';') + ' LIMIT 500'
    else:
        limit_match = re.search(r'\bLIMIT\s+(\d+)', sql_upper)
        if limit_match and int(limit_match.group(1)) > 5000:
            return 'Query limit cannot exceed 5000 rows.'

    return None


def _log_action(report_id, action, details):
    try:
        user_id = session.get('user_id')
        db = Database()
        conn = db.get_connection()
        conn.execute('''
            INSERT INTO report_audit_log (report_id, action, user_id, details)
            VALUES (%s, %s, %s, %s)
        ''', (report_id, action, user_id, details))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Audit log error: {e}")
