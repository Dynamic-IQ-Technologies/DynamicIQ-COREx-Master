import json
import logging
from datetime import datetime
from models import Database

logger = logging.getLogger('reporting_engine')

AVAILABLE_DATA_SOURCES = {
    'work_orders': {
        'label': 'Work Orders',
        'table': 'work_orders',
        'fields': {
            'id': {'label': 'ID', 'type': 'integer'},
            'wo_number': {'label': 'WO Number', 'type': 'text'},
            'description': {'label': 'Description', 'type': 'text'},
            'status': {'label': 'Status', 'type': 'text'},
            'priority': {'label': 'Priority', 'type': 'text'},
            'customer_name': {'label': 'Customer', 'type': 'text'},
            'product_id': {'label': 'Product ID', 'type': 'integer'},
            'quantity': {'label': 'Quantity', 'type': 'number'},
            'serial_number': {'label': 'Serial Number', 'type': 'text'},
            'created_at': {'label': 'Created Date', 'type': 'date'},
            'planned_start_date': {'label': 'Planned Start Date', 'type': 'date'},
            'planned_end_date': {'label': 'Planned End Date', 'type': 'date'},
            'actual_start_date': {'label': 'Actual Start Date', 'type': 'date'},
            'actual_end_date': {'label': 'Actual End Date', 'type': 'date'},
            'material_cost': {'label': 'Material Cost', 'type': 'number'},
            'labor_cost': {'label': 'Labor Cost', 'type': 'number'},
            'overhead_cost': {'label': 'Overhead Cost', 'type': 'number'},
            'workorder_type': {'label': 'Work Order Type', 'type': 'text'},
            'disposition': {'label': 'Disposition', 'type': 'text'},
            'notes': {'label': 'Notes', 'type': 'text'},
        }
    },
    'inventory': {
        'label': 'Inventory',
        'table': 'inventory',
        'fields': {
            'id': {'label': 'ID', 'type': 'integer'},
            'product_id': {'label': 'Product ID', 'type': 'integer'},
            'quantity': {'label': 'Quantity', 'type': 'number'},
            'warehouse_location': {'label': 'Warehouse Location', 'type': 'text'},
            'bin_location': {'label': 'Bin Location', 'type': 'text'},
            'condition': {'label': 'Condition', 'type': 'text'},
            'unit_cost': {'label': 'Unit Cost', 'type': 'number'},
            'last_received_date': {'label': 'Last Received Date', 'type': 'date'},
            'serial_number': {'label': 'Serial Number', 'type': 'text'},
            'lot_number': {'label': 'Lot Number', 'type': 'text'},
            'status': {'label': 'Status', 'type': 'text'},
            'supplier_id': {'label': 'Supplier ID', 'type': 'integer'},
        }
    },
    'purchase_orders': {
        'label': 'Purchase Orders',
        'table': 'purchase_orders',
        'fields': {
            'id': {'label': 'ID', 'type': 'integer'},
            'po_number': {'label': 'PO Number', 'type': 'text'},
            'supplier_id': {'label': 'Supplier ID', 'type': 'integer'},
            'status': {'label': 'Status', 'type': 'text'},
            'order_date': {'label': 'Order Date', 'type': 'date'},
            'expected_date': {'label': 'Expected Date', 'type': 'date'},
            'expected_delivery_date': {'label': 'Expected Delivery Date', 'type': 'date'},
            'total_amount': {'label': 'Total Amount', 'type': 'number'},
            'grand_total': {'label': 'Grand Total', 'type': 'number'},
            'po_type': {'label': 'PO Type', 'type': 'text'},
            'notes': {'label': 'Notes', 'type': 'text'},
            'created_at': {'label': 'Created Date', 'type': 'date'},
        }
    },
    'sales_orders': {
        'label': 'Sales Orders',
        'table': 'sales_orders',
        'fields': {
            'id': {'label': 'ID', 'type': 'integer'},
            'so_number': {'label': 'SO Number', 'type': 'text'},
            'customer_id': {'label': 'Customer ID', 'type': 'integer'},
            'status': {'label': 'Status', 'type': 'text'},
            'order_date': {'label': 'Order Date', 'type': 'date'},
            'required_date': {'label': 'Required Date', 'type': 'date'},
            'total_amount': {'label': 'Total Amount', 'type': 'number'},
            'sales_type': {'label': 'Sales Type', 'type': 'text'},
            'order_type': {'label': 'Order Type', 'type': 'text'},
            'balance_due': {'label': 'Balance Due', 'type': 'number'},
            'created_at': {'label': 'Created Date', 'type': 'date'},
        }
    },
    'products': {
        'label': 'Products',
        'table': 'products',
        'fields': {
            'id': {'label': 'ID', 'type': 'integer'},
            'code': {'label': 'Part Number', 'type': 'text'},
            'name': {'label': 'Name', 'type': 'text'},
            'description': {'label': 'Description', 'type': 'text'},
            'product_category': {'label': 'Category', 'type': 'text'},
            'product_type': {'label': 'Product Type', 'type': 'text'},
            'cost': {'label': 'Cost', 'type': 'number'},
            'reorder_point': {'label': 'Reorder Point', 'type': 'number'},
            'manufacturer': {'label': 'Manufacturer', 'type': 'text'},
        }
    },
    'suppliers': {
        'label': 'Suppliers',
        'table': 'suppliers',
        'fields': {
            'id': {'label': 'ID', 'type': 'integer'},
            'name': {'label': 'Supplier Name', 'type': 'text'},
            'code': {'label': 'Supplier Code', 'type': 'text'},
            'contact_name': {'label': 'Contact', 'type': 'text'},
            'email': {'label': 'Email', 'type': 'text'},
            'phone': {'label': 'Phone', 'type': 'text'},
            'status': {'label': 'Status', 'type': 'text'},
            'city': {'label': 'City', 'type': 'text'},
            'state': {'label': 'State', 'type': 'text'},
            'country': {'label': 'Country', 'type': 'text'},
        }
    },
    'rfqs': {
        'label': 'RFQs',
        'table': 'rfqs',
        'fields': {
            'id': {'label': 'ID', 'type': 'integer'},
            'rfq_number': {'label': 'RFQ Number', 'type': 'text'},
            'title': {'label': 'Title', 'type': 'text'},
            'status': {'label': 'Status', 'type': 'text'},
            'created_at': {'label': 'Created Date', 'type': 'date'},
            'due_date': {'label': 'Due Date', 'type': 'date'},
        }
    },
    'invoices': {
        'label': 'Invoices',
        'table': 'invoices',
        'fields': {
            'id': {'label': 'ID', 'type': 'integer'},
            'invoice_number': {'label': 'Invoice Number', 'type': 'text'},
            'invoice_type': {'label': 'Invoice Type', 'type': 'text'},
            'status': {'label': 'Status', 'type': 'text'},
            'total_amount': {'label': 'Total Amount', 'type': 'number'},
            'balance_due': {'label': 'Balance Due', 'type': 'number'},
            'invoice_date': {'label': 'Invoice Date', 'type': 'date'},
            'due_date': {'label': 'Due Date', 'type': 'date'},
            'amount_paid': {'label': 'Amount Paid', 'type': 'number'},
        }
    },
}

VALID_JOINS = {
    ('work_orders', 'products'): 'work_orders.product_id = products.id',
    ('inventory', 'products'): 'inventory.product_id = products.id',
    ('purchase_orders', 'suppliers'): 'purchase_orders.supplier_id = suppliers.id',
}

ALLOWED_AGGREGATIONS = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']
ALLOWED_OPERATORS = ['=', '!=', '>', '<', '>=', '<=', 'LIKE', 'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL', 'BETWEEN']


class ReportingEngine:

    def __init__(self):
        self.db = Database()

    def get_data_sources(self):
        return AVAILABLE_DATA_SOURCES

    def build_query_from_config(self, config):
        source = config.get('data_source')
        if source not in AVAILABLE_DATA_SOURCES:
            raise ValueError(f"Invalid data source: {source}")

        source_info = AVAILABLE_DATA_SOURCES[source]
        table = source_info['table']
        valid_fields = set(source_info['fields'].keys())

        selected_fields = config.get('fields', [])
        if not selected_fields:
            selected_fields = list(valid_fields)

        for f in selected_fields:
            base_field = f.split('(')[-1].rstrip(')') if '(' in f else f
            if base_field not in valid_fields and base_field != '*':
                raise ValueError(f"Invalid field: {f}")

        joins = config.get('joins', [])
        join_clauses = []
        for j in joins:
            join_table = j.get('table')
            if join_table not in AVAILABLE_DATA_SOURCES:
                raise ValueError(f"Invalid join table: {join_table}")
            key = (source, join_table)
            rev_key = (join_table, source)
            if key in VALID_JOINS:
                join_clauses.append(f"LEFT JOIN {join_table} ON {VALID_JOINS[key]}")
            elif rev_key in VALID_JOINS:
                join_clauses.append(f"LEFT JOIN {join_table} ON {VALID_JOINS[rev_key]}")

        filters = config.get('filters', [])
        where_clauses = []
        params = []
        for flt in filters:
            field = flt.get('field')
            op = flt.get('operator', '=').upper()
            value = flt.get('value')

            base_field = field
            if '.' not in field:
                field = f"{table}.{field}"

            if op not in ALLOWED_OPERATORS:
                continue

            if op in ('IS NULL', 'IS NOT NULL'):
                where_clauses.append(f"{field} {op}")
            elif op == 'BETWEEN':
                vals = value if isinstance(value, list) else [value, value]
                where_clauses.append(f"{field} BETWEEN %s AND %s")
                params.extend(vals[:2])
            elif op == 'IN':
                vals = value if isinstance(value, list) else [value]
                placeholders = ','.join(['%s'] * len(vals))
                where_clauses.append(f"{field} IN ({placeholders})")
                params.extend(vals)
            elif op == 'LIKE':
                where_clauses.append(f"CAST({field} AS TEXT) ILIKE %s")
                params.append(f"%{value}%")
            else:
                where_clauses.append(f"{field} {op} %s")
                params.append(value)

        aggregations = config.get('aggregations', [])
        group_by = config.get('group_by', [])

        if aggregations:
            select_parts = []
            for gb in group_by:
                if gb in valid_fields:
                    select_parts.append(f"{table}.{gb}")
            for agg in aggregations:
                func = agg.get('function', 'COUNT').upper()
                agg_field = agg.get('field', '*')
                alias = agg.get('alias', f"{func.lower()}_{agg_field}")
                if func not in ALLOWED_AGGREGATIONS:
                    continue
                if agg_field == '*':
                    select_parts.append(f"{func}(*) AS {alias}")
                else:
                    select_parts.append(f"{func}({table}.{agg_field}) AS {alias}")
            select_clause = ', '.join(select_parts) if select_parts else f'COUNT(*) AS count'
        else:
            select_parts = [f"{table}.{f}" for f in selected_fields if f in valid_fields]
            select_clause = ', '.join(select_parts) if select_parts else f"{table}.*"

        query = f"SELECT {select_clause} FROM {table}"
        if join_clauses:
            query += ' ' + ' '.join(join_clauses)
        if where_clauses:
            query += ' WHERE ' + ' AND '.join(where_clauses)
        if group_by and aggregations:
            gb_clause = ', '.join([f"{table}.{g}" for g in group_by if g in valid_fields])
            if gb_clause:
                query += f' GROUP BY {gb_clause}'

        sort_by = config.get('sort_by')
        sort_dir = config.get('sort_direction', 'ASC').upper()
        if sort_dir not in ('ASC', 'DESC'):
            sort_dir = 'ASC'
        if sort_by and sort_by in valid_fields:
            query += f' ORDER BY {table}.{sort_by} {sort_dir}'

        limit = config.get('limit', 500)
        if limit and isinstance(limit, int) and limit > 0:
            query += f' LIMIT {min(limit, 5000)}'

        return query, params

    def execute_report(self, config):
        try:
            query, params = self.build_query_from_config(config)
            conn = self.db.get_connection()
            try:
                results = conn.execute(query, tuple(params)).fetchall()
                data = [dict(r) for r in results]
                return {
                    'success': True,
                    'data': data,
                    'row_count': len(data),
                    'query_config': config
                }
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Report execution error: {e}")
            return {
                'success': False,
                'error': str(e),
                'data': [],
                'row_count': 0
            }

    def execute_nl_query(self, sql_query, params=None):
        import re
        if not re.search(r'\bLIMIT\s+\d+', sql_query, re.IGNORECASE):
            sql_query = sql_query.rstrip().rstrip(';') + ' LIMIT 500'
        conn = self.db.get_connection()
        try:
            results = conn.execute(sql_query, tuple(params or [])).fetchall()
            data = [dict(r) for r in results]
            return {
                'success': True,
                'data': data,
                'row_count': len(data)
            }
        except Exception as e:
            logger.error(f"NL query execution error: {e}")
            return {
                'success': False,
                'error': str(e),
                'data': [],
                'row_count': 0
            }
        finally:
            conn.close()

    def save_report(self, report_data, user_id):
        conn = self.db.get_connection()
        try:
            conn.execute('''
                INSERT INTO saved_reports (name, description, category, owner_id, access_level, 
                    report_type, query_config, nl_prompt, visualization_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                report_data.get('name', 'Untitled Report'),
                report_data.get('description', ''),
                report_data.get('category', 'Operations'),
                user_id,
                report_data.get('access_level', 'Private'),
                report_data.get('report_type', 'custom'),
                json.dumps(report_data.get('query_config', {})),
                report_data.get('nl_prompt', ''),
                report_data.get('visualization_type', 'table')
            ))
            result = conn.execute("SELECT lastval()").fetchone()
            report_id = result['lastval'] if result else None
            conn.commit()

            if report_id:
                conn.execute('''
                    INSERT INTO report_versions (report_id, version, query_config, nl_prompt, changed_by, change_description)
                    VALUES (%s, 1, %s, %s, %s, %s)
                ''', (report_id, json.dumps(report_data.get('query_config', {})),
                      report_data.get('nl_prompt', ''), user_id, 'Initial creation'))
                conn.commit()

                self._log_audit(conn, report_id, 'created', user_id, f"Report '{report_data.get('name')}' created")

            return report_id
        except Exception as e:
            logger.error(f"Save report error: {e}")
            conn.rollback()
            return None
        finally:
            conn.close()

    def update_report(self, report_id, report_data, user_id):
        conn = self.db.get_connection()
        try:
            existing = conn.execute('SELECT * FROM saved_reports WHERE id = %s', (report_id,)).fetchone()
            if not existing:
                return False

            new_version = (existing['version'] or 1) + 1
            conn.execute('''
                UPDATE saved_reports SET name = %s, description = %s, category = %s,
                    access_level = %s, query_config = %s, nl_prompt = %s,
                    visualization_type = %s, version = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            ''', (
                report_data.get('name', existing['name']),
                report_data.get('description', existing['description']),
                report_data.get('category', existing['category']),
                report_data.get('access_level', existing['access_level']),
                json.dumps(report_data.get('query_config', {})),
                report_data.get('nl_prompt', existing['nl_prompt']),
                report_data.get('visualization_type', existing['visualization_type']),
                new_version,
                report_id
            ))

            conn.execute('''
                INSERT INTO report_versions (report_id, version, query_config, nl_prompt, changed_by, change_description)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (report_id, new_version, json.dumps(report_data.get('query_config', {})),
                  report_data.get('nl_prompt', ''), user_id, report_data.get('change_description', 'Updated')))
            conn.commit()

            self._log_audit(conn, report_id, 'updated', user_id, f"Report updated to version {new_version}")
            return True
        except Exception as e:
            logger.error(f"Update report error: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_saved_reports(self, user_id, user_role='user'):
        conn = self.db.get_connection()
        try:
            if user_role == 'admin':
                reports = conn.execute('''
                    SELECT sr.*, u.username as owner_name 
                    FROM saved_reports sr
                    LEFT JOIN users u ON sr.owner_id = u.id
                    WHERE sr.status = 'Active'
                    ORDER BY sr.updated_at DESC
                ''').fetchall()
            else:
                reports = conn.execute('''
                    SELECT sr.*, u.username as owner_name 
                    FROM saved_reports sr
                    LEFT JOIN users u ON sr.owner_id = u.id
                    WHERE sr.status = 'Active' AND (sr.owner_id = %s OR sr.access_level IN ('Team', 'Organization'))
                    ORDER BY sr.updated_at DESC
                ''', (user_id,)).fetchall()
            return [dict(r) for r in reports]
        finally:
            conn.close()

    def get_report(self, report_id):
        conn = self.db.get_connection()
        try:
            report = conn.execute('''
                SELECT sr.*, u.username as owner_name 
                FROM saved_reports sr
                LEFT JOIN users u ON sr.owner_id = u.id
                WHERE sr.id = %s
            ''', (report_id,)).fetchone()
            return dict(report) if report else None
        finally:
            conn.close()

    def get_report_versions(self, report_id):
        conn = self.db.get_connection()
        try:
            versions = conn.execute('''
                SELECT rv.*, u.username as changed_by_name
                FROM report_versions rv
                LEFT JOIN users u ON rv.changed_by = u.id
                WHERE rv.report_id = %s
                ORDER BY rv.version DESC
            ''', (report_id,)).fetchall()
            return [dict(v) for v in versions]
        finally:
            conn.close()

    def clone_report(self, report_id, user_id):
        report = self.get_report(report_id)
        if not report:
            return None
        new_data = {
            'name': f"{report['name']} (Copy)",
            'description': report.get('description', ''),
            'category': report.get('category', 'Operations'),
            'access_level': 'Private',
            'report_type': report.get('report_type', 'custom'),
            'query_config': report.get('query_config', {}),
            'nl_prompt': report.get('nl_prompt', ''),
            'visualization_type': report.get('visualization_type', 'table'),
        }
        return self.save_report(new_data, user_id)

    def delete_report(self, report_id, user_id):
        conn = self.db.get_connection()
        try:
            conn.execute("UPDATE saved_reports SET status = 'Deleted' WHERE id = %s", (report_id,))
            conn.commit()
            self._log_audit(conn, report_id, 'deleted', user_id, 'Report deleted')
            return True
        except Exception as e:
            logger.error(f"Delete report error: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def _log_audit(self, conn, report_id, action, user_id, details):
        try:
            conn.execute('''
                INSERT INTO report_audit_log (report_id, action, user_id, details)
                VALUES (%s, %s, %s, %s)
            ''', (report_id, action, user_id, details))
            conn.commit()
        except Exception as e:
            logger.error(f"Audit log error: {e}")

    def get_schema_description(self):
        lines = []
        for key, source in AVAILABLE_DATA_SOURCES.items():
            fields_desc = ', '.join([f"{f} ({info['type']})" for f, info in source['fields'].items()])
            lines.append(f"Table: {source['table']} - Fields: {fields_desc}")
        joins_desc = '; '.join([f"{k[0]} JOIN {k[1]} ON {v}" for k, v in VALID_JOINS.items()])
        lines.append(f"Valid Joins: {joins_desc}")
        return '\n'.join(lines)
