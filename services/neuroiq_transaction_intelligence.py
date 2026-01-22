"""
COREx NeuroIQ Transactional Intelligence Service
Enterprise-grade autonomous system intelligence layer for transaction analysis
"""
import re
import json
from datetime import datetime, timedelta
from models import Database

TRANSACTION_PATTERNS = {
    'work_order': {
        'patterns': [r'\bWO[-\s]?(\d+)\b', r'\bwork\s*order\s*#?\s*(\d+)\b', r'\bwork\s*order\s+([A-Z0-9-]+)\b'],
        'keywords': ['work order', 'wo', 'production order', 'manufacturing order'],
        'table': 'work_orders',
        'number_field': 'wo_number'
    },
    'sales_order': {
        'patterns': [r'\bSO[-\s]?(\d+)\b', r'\bsales\s*order\s*#?\s*(\d+)\b', r'\bsales\s*order\s+([A-Z0-9-]+)\b'],
        'keywords': ['sales order', 'so', 'customer order', 'order'],
        'table': 'sales_orders',
        'number_field': 'so_number'
    },
    'purchase_order': {
        'patterns': [r'\bPO[-\s]?(\d+)\b', r'\bpurchase\s*order\s*#?\s*(\d+)\b', r'\bpurchase\s*order\s+([A-Z0-9-]+)\b'],
        'keywords': ['purchase order', 'po', 'supplier order', 'vendor order'],
        'table': 'purchase_orders',
        'number_field': 'po_number'
    },
    'part': {
        'patterns': [r'\bP/N\s*:?\s*([A-Z0-9-]+)\b', r'\bpart\s*#?\s*:?\s*([A-Z0-9-]+)\b', r'\bpart\s+number\s+([A-Z0-9-]+)\b'],
        'keywords': ['part', 'product', 'item', 'component', 'material'],
        'table': 'products',
        'number_field': 'part_number'
    },
    'serial': {
        'patterns': [r'\bS/N\s*:?\s*([A-Z0-9-]+)\b', r'\bserial\s*#?\s*:?\s*([A-Z0-9-]+)\b', r'\bserial\s+number\s+([A-Z0-9-]+)\b'],
        'keywords': ['serial', 'serial number', 's/n'],
        'table': 'inventory',
        'number_field': 'serial_number'
    },
    'invoice': {
        'patterns': [r'\bINV[-\s]?(\d+)\b', r'\binvoice\s*#?\s*(\d+)\b'],
        'keywords': ['invoice', 'bill', 'inv'],
        'table': 'invoices',
        'number_field': 'invoice_number'
    }
}

INTENT_TYPES = {
    'status_inquiry': {
        'patterns': ['what is', 'what\'s', 'status of', 'where is', 'show me', 'find', 'get', 'check'],
        'action': 'get_status'
    },
    'root_cause': {
        'patterns': ['why is', 'why are', 'why hasn\'t', 'why can\'t', 'what\'s holding', 'what is blocking', 'reason for', 'cause of'],
        'action': 'explain_cause'
    },
    'exception_detection': {
        'patterns': ['overdue', 'late', 'behind', 'past due', 'delayed', 'stuck', 'problem', 'issue', 'exception'],
        'action': 'find_exceptions'
    },
    'availability_check': {
        'patterns': ['do we have', 'is there', 'enough', 'sufficient', 'available', 'can we', 'stock for'],
        'action': 'check_availability'
    },
    'trend_comparison': {
        'patterns': ['compare', 'trend', 'over time', 'last week', 'this month', 'versus', 'vs', 'difference'],
        'action': 'analyze_trend'
    },
    'list_query': {
        'patterns': ['list', 'show all', 'which', 'how many', 'what are', 'all the'],
        'action': 'list_records'
    }
}

TIME_CONTEXT = {
    'today': lambda: datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
    'yesterday': lambda: datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1),
    'this week': lambda: datetime.now() - timedelta(days=datetime.now().weekday()),
    'last week': lambda: datetime.now() - timedelta(days=datetime.now().weekday() + 7),
    'this month': lambda: datetime.now().replace(day=1),
    'last month': lambda: (datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1),
    'last 7 days': lambda: datetime.now() - timedelta(days=7),
    'last 30 days': lambda: datetime.now() - timedelta(days=30),
    'last 90 days': lambda: datetime.now() - timedelta(days=90)
}

STATUS_KEYWORDS = {
    'open': ['open', 'pending', 'in progress', 'active', 'released', 'confirmed'],
    'closed': ['closed', 'completed', 'finished', 'done', 'shipped', 'invoiced'],
    'on_hold': ['on hold', 'held', 'blocked', 'paused', 'waiting'],
    'overdue': ['overdue', 'past due', 'late', 'delayed']
}


class TransactionIntelligenceService:
    """NL-to-Intent parser and transaction query orchestration"""
    
    def __init__(self):
        self.db = Database()
    
    def parse_intent(self, query):
        """
        Parse natural language query to extract:
        - Transaction type(s)
        - Record identifier(s)
        - Time context
        - Metrics requested
        - Intent classification
        """
        query_lower = query.lower()
        
        result = {
            'original_query': query,
            'transaction_types': [],
            'record_ids': [],
            'time_context': None,
            'status_filter': None,
            'intent': None,
            'metrics': []
        }
        
        for tx_type, config in TRANSACTION_PATTERNS.items():
            for pattern in config['patterns']:
                matches = re.findall(pattern, query, re.IGNORECASE)
                for match in matches:
                    result['transaction_types'].append(tx_type)
                    result['record_ids'].append({
                        'type': tx_type,
                        'id': match,
                        'table': config['table'],
                        'number_field': config['number_field']
                    })
            
            for keyword in config['keywords']:
                if keyword in query_lower and tx_type not in result['transaction_types']:
                    result['transaction_types'].append(tx_type)
        
        for time_phrase, date_fn in TIME_CONTEXT.items():
            if time_phrase in query_lower:
                result['time_context'] = {
                    'phrase': time_phrase,
                    'start_date': date_fn().strftime('%Y-%m-%d')
                }
                break
        
        for status_type, keywords in STATUS_KEYWORDS.items():
            for keyword in keywords:
                if keyword in query_lower:
                    result['status_filter'] = status_type
                    break
            if result['status_filter']:
                break
        
        for intent_type, config in INTENT_TYPES.items():
            for pattern in config['patterns']:
                if pattern in query_lower:
                    result['intent'] = {
                        'type': intent_type,
                        'action': config['action']
                    }
                    break
            if result['intent']:
                break
        
        if not result['intent']:
            result['intent'] = {'type': 'status_inquiry', 'action': 'get_status'}
        
        metrics_patterns = [
            (r'\b(quantity|qty)\b', 'quantity'),
            (r'\b(value|amount|cost|price)\b', 'value'),
            (r'\b(status|state)\b', 'status'),
            (r'\b(date|due|deadline)\b', 'dates'),
            (r'\b(customer|client)\b', 'customer'),
            (r'\b(supplier|vendor)\b', 'supplier'),
            (r'\b(shortage|stock out|missing)\b', 'shortages'),
            (r'\b(labor|hours|time)\b', 'labor'),
            (r'\b(material|component|part)\b', 'materials')
        ]
        
        for pattern, metric in metrics_patterns:
            if re.search(pattern, query_lower):
                result['metrics'].append(metric)
        
        return result
    
    def execute_query(self, parsed_intent, user_role='User'):
        """
        Execute transaction query based on parsed intent
        Returns structured data with all relevant information
        """
        action = parsed_intent['intent']['action']
        
        if action == 'get_status' and parsed_intent['record_ids']:
            return self._get_record_status(parsed_intent)
        elif action == 'explain_cause' and parsed_intent['record_ids']:
            return self._explain_blocking_cause(parsed_intent)
        elif action == 'find_exceptions':
            return self._find_exceptions(parsed_intent)
        elif action == 'check_availability':
            return self._check_availability(parsed_intent)
        elif action == 'list_records':
            return self._list_records(parsed_intent)
        elif action == 'analyze_trend':
            return self._analyze_trend(parsed_intent)
        else:
            return self._general_query(parsed_intent)
    
    def _get_record_status(self, parsed_intent):
        """Get detailed status of specific record(s)"""
        results = []
        conn = self.db.get_connection()
        
        try:
            for record_ref in parsed_intent['record_ids']:
                if record_ref['type'] == 'work_order':
                    results.append(self._get_work_order_status(conn, record_ref['id']))
                elif record_ref['type'] == 'sales_order':
                    results.append(self._get_sales_order_status(conn, record_ref['id']))
                elif record_ref['type'] == 'purchase_order':
                    results.append(self._get_purchase_order_status(conn, record_ref['id']))
                elif record_ref['type'] == 'part':
                    results.append(self._get_part_status(conn, record_ref['id']))
        finally:
            conn.close()
        
        return {
            'query_type': 'record_status',
            'records': results,
            'timestamp': datetime.now().isoformat()
        }
    
    def _get_work_order_status(self, conn, wo_id):
        """Get comprehensive work order status"""
        wo = conn.execute('''
            SELECT wo.*, p.name as product_name, p.part_number,
                   c.name as customer_name
            FROM work_orders wo
            LEFT JOIN products p ON wo.product_id = p.id
            LEFT JOIN customers c ON wo.customer_id = c.id
            WHERE wo.wo_number LIKE ? OR wo.id = ?
        ''', (f'%{wo_id}%', wo_id if str(wo_id).isdigit() else 0)).fetchone()
        
        if not wo:
            return {'found': False, 'message': f'Work order {wo_id} not found'}
        
        tasks = conn.execute('''
            SELECT wot.*, ls.name as skillset_name
            FROM work_order_tasks wot
            LEFT JOIN labor_skillsets ls ON wot.skillset_id = ls.id
            WHERE wot.work_order_id = ?
            ORDER BY wot.sequence
        ''', (wo['id'],)).fetchall()
        
        materials = conn.execute('''
            SELECT wom.*, p.name as material_name, p.part_number
            FROM work_order_materials wom
            LEFT JOIN products p ON wom.product_id = p.id
            WHERE wom.work_order_id = ?
        ''', (wo['id'],)).fetchall()
        
        labor_time = conn.execute('''
            SELECT COALESCE(SUM(
                (julianday(end_time) - julianday(start_time)) * 24
            ), 0) as total_hours
            FROM time_entries
            WHERE work_order_id = ?
        ''', (wo['id'],)).fetchone()
        
        shortages = []
        for mat in materials:
            if mat['quantity_required'] > (mat['quantity_issued'] or 0):
                avail = conn.execute('''
                    SELECT COALESCE(SUM(quantity), 0) as available
                    FROM inventory WHERE product_id = ?
                ''', (mat['product_id'],)).fetchone()
                
                needed = mat['quantity_required'] - (mat['quantity_issued'] or 0)
                if avail['available'] < needed:
                    shortages.append({
                        'part_number': mat['part_number'],
                        'material_name': mat['material_name'],
                        'needed': needed,
                        'available': avail['available'],
                        'shortage': needed - avail['available']
                    })
        
        blocking_reasons = []
        if wo['status'] == 'On Hold':
            blocking_reasons.append('Work order is currently on hold')
        if shortages:
            blocking_reasons.append(f'{len(shortages)} material shortage(s) detected')
        
        pending_tasks = [t for t in tasks if t['status'] != 'Completed']
        if pending_tasks:
            blocking_reasons.append(f'{len(pending_tasks)} task(s) not yet completed')
        
        return {
            'found': True,
            'type': 'work_order',
            'id': wo['id'],
            'number': wo['wo_number'],
            'status': wo['status'],
            'priority': wo['priority'],
            'product': {
                'name': wo['product_name'],
                'part_number': wo['part_number']
            },
            'customer': wo['customer_name'],
            'quantity': wo['quantity'],
            'dates': {
                'start_date': wo.get('planned_start_date'),
                'due_date': wo.get('planned_end_date'),
                'created': wo['created_at']
            },
            'progress': {
                'total_tasks': len(tasks),
                'completed_tasks': len([t for t in tasks if t['status'] == 'Completed']),
                'labor_hours': round(labor_time['total_hours'], 2) if labor_time else 0
            },
            'materials': {
                'total_items': len(materials),
                'shortages': shortages
            },
            'blocking_reasons': blocking_reasons,
            'is_blocked': len(blocking_reasons) > 0
        }
    
    def _get_sales_order_status(self, conn, so_id):
        """Get comprehensive sales order status"""
        so = conn.execute('''
            SELECT so.*, c.name as customer_name
            FROM sales_orders so
            LEFT JOIN customers c ON so.customer_id = c.id
            WHERE so.so_number LIKE ? OR so.id = ?
        ''', (f'%{so_id}%', so_id if so_id.isdigit() else 0)).fetchone()
        
        if not so:
            return {'found': False, 'message': f'Sales order {so_id} not found'}
        
        lines = conn.execute('''
            SELECT sol.*, p.name as product_name, p.part_number
            FROM sales_order_lines sol
            LEFT JOIN products p ON sol.product_id = p.id
            WHERE sol.sales_order_id = ?
        ''', (so['id'],)).fetchall()
        
        work_orders = conn.execute('''
            SELECT wo.id, wo.wo_number, wo.status, wo.quantity
            FROM work_orders wo
            WHERE wo.so_id = ?
        ''', (so['id'],)).fetchall()
        
        invoice = conn.execute('''
            SELECT i.invoice_number, i.status, i.total_amount, i.balance_due
            FROM invoices i
            WHERE i.sales_order_id = ?
        ''', (so['id'],)).fetchone()
        
        blocking_reasons = []
        incomplete_wos = [wo for wo in work_orders if wo['status'] not in ('Completed', 'Shipped')]
        if incomplete_wos:
            blocking_reasons.append(f'{len(incomplete_wos)} work order(s) not completed')
        
        for line in lines:
            if (line['quantity_shipped'] or 0) < line['quantity']:
                blocking_reasons.append('Not all items have been shipped')
                break
        
        exchange_info = None
        if so['sales_type'] == 'Exchange':
            exchange_info = {
                'core_due_days': so.get('core_due_days'),
                'expected_core_return': so.get('expected_core_return_date'),
                'core_received': so.get('core_received_date')
            }
        
        return {
            'found': True,
            'type': 'sales_order',
            'id': so['id'],
            'number': so['so_number'],
            'status': so['status'],
            'sales_type': so['sales_type'],
            'customer': so['customer_name'],
            'dates': {
                'order_date': so['order_date'],
                'required_date': so.get('required_date'),
                'ship_date': so.get('ship_date')
            },
            'financials': {
                'total_amount': float(so['total_amount'] or 0),
                'invoiced': invoice['total_amount'] if invoice else 0,
                'balance_due': invoice['balance_due'] if invoice else 0
            },
            'lines': {
                'total': len(lines),
                'shipped': sum(1 for l in lines if (l['quantity_shipped'] or 0) >= l['quantity'])
            },
            'work_orders': {
                'total': len(work_orders),
                'completed': len([wo for wo in work_orders if wo['status'] == 'Completed'])
            },
            'exchange_info': exchange_info,
            'blocking_reasons': blocking_reasons,
            'is_blocked': len(blocking_reasons) > 0
        }
    
    def _get_purchase_order_status(self, conn, po_id):
        """Get comprehensive purchase order status"""
        po = conn.execute('''
            SELECT po.*, s.name as supplier_name
            FROM purchase_orders po
            LEFT JOIN suppliers s ON po.supplier_id = s.id
            WHERE po.po_number LIKE ? OR po.id = ?
        ''', (f'%{po_id}%', po_id if po_id.isdigit() else 0)).fetchone()
        
        if not po:
            return {'found': False, 'message': f'Purchase order {po_id} not found'}
        
        lines = conn.execute('''
            SELECT pol.*, p.name as product_name, p.part_number
            FROM purchase_order_lines pol
            LEFT JOIN products p ON pol.product_id = p.id
            WHERE pol.purchase_order_id = ?
        ''', (po['id'],)).fetchall()
        
        receipts = conn.execute('''
            SELECT r.*, rl.quantity as received_qty, rl.product_id
            FROM receipts r
            JOIN receipt_lines rl ON r.id = rl.receipt_id
            WHERE r.purchase_order_id = ?
        ''', (po['id'],)).fetchall()
        
        is_overdue = False
        if po.get('expected_date'):
            expected = datetime.strptime(po['expected_date'], '%Y-%m-%d') if isinstance(po['expected_date'], str) else po['expected_date']
            if expected < datetime.now() and po['status'] not in ('Received', 'Closed', 'Cancelled'):
                is_overdue = True
        
        blocking_reasons = []
        if is_overdue:
            blocking_reasons.append('Purchase order is past expected delivery date')
        if po['status'] == 'On Hold':
            blocking_reasons.append('Purchase order is on hold')
        
        return {
            'found': True,
            'type': 'purchase_order',
            'id': po['id'],
            'number': po['po_number'],
            'status': po['status'],
            'supplier': po['supplier_name'],
            'dates': {
                'order_date': po['order_date'],
                'expected_date': po.get('expected_date')
            },
            'financials': {
                'total_amount': float(po['total_amount'] or 0),
                'amount_paid': float(po.get('amount_paid') or 0)
            },
            'lines': {
                'total': len(lines),
                'fully_received': sum(1 for l in lines if (l.get('quantity_received') or 0) >= l['quantity'])
            },
            'receipts': {
                'count': len(set(r['id'] for r in receipts)),
                'total_received': sum(r['received_qty'] for r in receipts)
            },
            'is_overdue': is_overdue,
            'blocking_reasons': blocking_reasons,
            'is_blocked': len(blocking_reasons) > 0
        }
    
    def _get_part_status(self, conn, part_id):
        """Get comprehensive part/inventory status"""
        product = conn.execute('''
            SELECT p.*, 
                   (SELECT COALESCE(SUM(quantity), 0) FROM inventory WHERE product_id = p.id) as total_stock,
                   (SELECT COUNT(*) FROM inventory WHERE product_id = p.id AND quantity > 0) as location_count
            FROM products p
            WHERE p.part_number LIKE ? OR p.name LIKE ? OR p.id = ?
        ''', (f'%{part_id}%', f'%{part_id}%', part_id if part_id.isdigit() else 0)).fetchone()
        
        if not product:
            return {'found': False, 'message': f'Part {part_id} not found'}
        
        inventory = conn.execute('''
            SELECT i.*, l.name as location_name
            FROM inventory i
            LEFT JOIN locations l ON i.location_id = l.id
            WHERE i.product_id = ? AND i.quantity > 0
        ''', (product['id'],)).fetchall()
        
        pending_demand = conn.execute('''
            SELECT COALESCE(SUM(wom.quantity_required - COALESCE(wom.quantity_issued, 0)), 0) as wo_demand
            FROM work_order_materials wom
            JOIN work_orders wo ON wom.work_order_id = wo.id
            WHERE wom.product_id = ? AND wo.status NOT IN ('Completed', 'Cancelled', 'Closed')
        ''', (product['id'],)).fetchone()
        
        pending_supply = conn.execute('''
            SELECT COALESCE(SUM(pol.quantity - COALESCE(pol.quantity_received, 0)), 0) as po_supply
            FROM purchase_order_lines pol
            JOIN purchase_orders po ON pol.purchase_order_id = po.id
            WHERE pol.product_id = ? AND po.status NOT IN ('Received', 'Cancelled', 'Closed')
        ''', (product['id'],)).fetchone()
        
        reorder_point = product.get('reorder_point') or 0
        is_low_stock = product['total_stock'] <= reorder_point
        
        return {
            'found': True,
            'type': 'part',
            'id': product['id'],
            'part_number': product['part_number'],
            'name': product['name'],
            'description': product['description'],
            'inventory': {
                'total_stock': product['total_stock'],
                'location_count': product['location_count'],
                'reorder_point': reorder_point,
                'is_low_stock': is_low_stock,
                'locations': [{'name': i['location_name'], 'quantity': i['quantity'], 
                              'lot_number': i.get('lot_number'), 'serial_number': i.get('serial_number')} 
                             for i in inventory]
            },
            'demand': {
                'pending_wo_demand': pending_demand['wo_demand'],
                'net_available': product['total_stock'] - pending_demand['wo_demand']
            },
            'supply': {
                'pending_po_supply': pending_supply['po_supply'],
                'projected_stock': product['total_stock'] + pending_supply['po_supply'] - pending_demand['wo_demand']
            },
            'cost': float(product.get('cost') or 0),
            'unit_of_measure': product.get('uom')
        }
    
    def _explain_blocking_cause(self, parsed_intent):
        """Explain why a transaction is blocked or delayed"""
        results = []
        conn = self.db.get_connection()
        
        try:
            for record_ref in parsed_intent['record_ids']:
                if record_ref['type'] == 'work_order':
                    status = self._get_work_order_status(conn, record_ref['id'])
                    if status['found']:
                        explanation = self._build_wo_explanation(conn, status)
                        results.append(explanation)
                elif record_ref['type'] == 'sales_order':
                    status = self._get_sales_order_status(conn, record_ref['id'])
                    if status['found']:
                        explanation = self._build_so_explanation(conn, status)
                        results.append(explanation)
        finally:
            conn.close()
        
        return {
            'query_type': 'blocking_cause',
            'explanations': results,
            'timestamp': datetime.now().isoformat()
        }
    
    def _build_wo_explanation(self, conn, wo_status):
        """Build detailed explanation for work order blocking causes"""
        explanations = []
        
        if wo_status['status'] == 'On Hold':
            explanations.append({
                'cause': 'Work order is on administrative hold',
                'type': 'status_hold',
                'impact': 'No work can proceed until hold is released',
                'recommendation': 'Review work order notes and contact supervisor to release hold'
            })
        
        for shortage in wo_status['materials'].get('shortages', []):
            po_check = conn.execute('''
                SELECT po.po_number, po.status, po.expected_date, pol.quantity
                FROM purchase_order_lines pol
                JOIN purchase_orders po ON pol.purchase_order_id = po.id
                JOIN products p ON pol.product_id = p.id
                WHERE p.part_number = ? AND po.status NOT IN ('Received', 'Cancelled', 'Closed')
                ORDER BY po.expected_date ASC LIMIT 1
            ''', (shortage['part_number'],)).fetchone()
            
            exp = {
                'cause': f'Material shortage: {shortage["material_name"]} ({shortage["part_number"]})',
                'type': 'material_shortage',
                'details': {
                    'needed': shortage['needed'],
                    'available': shortage['available'],
                    'shortage_qty': shortage['shortage']
                },
                'impact': 'Work order cannot proceed without required materials'
            }
            
            if po_check:
                exp['pending_supply'] = {
                    'po_number': po_check['po_number'],
                    'status': po_check['status'],
                    'expected_date': po_check['expected_date'],
                    'quantity': po_check['quantity']
                }
                exp['recommendation'] = f'Material on order (PO: {po_check["po_number"]}), expected {po_check["expected_date"]}'
            else:
                exp['recommendation'] = f'Create purchase order for {shortage["shortage"]} units of {shortage["part_number"]}'
            
            explanations.append(exp)
        
        if wo_status['progress']['completed_tasks'] < wo_status['progress']['total_tasks']:
            incomplete = wo_status['progress']['total_tasks'] - wo_status['progress']['completed_tasks']
            explanations.append({
                'cause': f'{incomplete} task(s) not yet completed',
                'type': 'pending_tasks',
                'impact': 'Work order requires task completion before it can be closed',
                'recommendation': 'Review task list and assign resources to complete pending tasks'
            })
        
        return {
            'record_type': 'work_order',
            'record_number': wo_status['number'],
            'current_status': wo_status['status'],
            'explanations': explanations,
            'summary': f'{len(explanations)} blocking condition(s) identified' if explanations else 'No blocking conditions detected'
        }
    
    def _build_so_explanation(self, conn, so_status):
        """Build detailed explanation for sales order blocking causes"""
        explanations = []
        
        for wo in conn.execute('''
            SELECT wo.wo_number, wo.status, p.name as product_name
            FROM work_orders wo
            LEFT JOIN products p ON wo.product_id = p.id
            WHERE wo.so_id = ? AND wo.status NOT IN ('Completed', 'Shipped', 'Cancelled')
        ''', (so_status['id'],)).fetchall():
            explanations.append({
                'cause': f'Work order {wo["wo_number"]} is still {wo["status"]}',
                'type': 'pending_work_order',
                'details': {'product': wo['product_name'], 'wo_status': wo['status']},
                'impact': 'Sales order cannot ship until work order is completed',
                'recommendation': f'Complete work order {wo["wo_number"]} to proceed with shipment'
            })
        
        if so_status['exchange_info'] and not so_status['exchange_info'].get('core_received'):
            core_due = so_status['exchange_info'].get('expected_core_return')
            if core_due:
                explanations.append({
                    'cause': 'Core unit not yet received for exchange order',
                    'type': 'pending_core_return',
                    'details': {'expected_return_date': core_due},
                    'impact': 'Exchange order awaiting core return from customer',
                    'recommendation': 'Follow up with customer regarding core return'
                })
        
        return {
            'record_type': 'sales_order',
            'record_number': so_status['number'],
            'current_status': so_status['status'],
            'explanations': explanations,
            'summary': f'{len(explanations)} blocking condition(s) identified' if explanations else 'No blocking conditions detected'
        }
    
    def _find_exceptions(self, parsed_intent):
        """Find overdue, delayed, or exceptional transactions"""
        conn = self.db.get_connection()
        exceptions = {
            'overdue_work_orders': [],
            'overdue_purchase_orders': [],
            'overdue_sales_orders': [],
            'on_hold_items': [],
            'low_stock_alerts': []
        }
        
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            
            overdue_wos = conn.execute('''
                SELECT wo.wo_number, wo.status, wo.planned_end_date, wo.priority,
                       p.name as product_name, c.name as customer_name
                FROM work_orders wo
                LEFT JOIN products p ON wo.product_id = p.id
                LEFT JOIN customers c ON wo.customer_id = c.id
                WHERE wo.planned_end_date < ? AND wo.status NOT IN ('Completed', 'Shipped', 'Cancelled', 'Closed')
                ORDER BY wo.planned_end_date ASC
            ''', (today,)).fetchall()
            
            for wo in overdue_wos:
                due_date = datetime.strptime(wo['planned_end_date'], '%Y-%m-%d')
                days_overdue = (datetime.now() - due_date).days
                exceptions['overdue_work_orders'].append({
                    'number': wo['wo_number'],
                    'status': wo['status'],
                    'due_date': wo['planned_end_date'],
                    'days_overdue': days_overdue,
                    'product': wo['product_name'],
                    'customer': wo['customer_name'],
                    'priority': wo['priority']
                })
            
            overdue_pos = conn.execute('''
                SELECT po.po_number, po.status, po.expected_date, s.name as supplier_name,
                       po.total_amount
                FROM purchase_orders po
                LEFT JOIN suppliers s ON po.supplier_id = s.id
                WHERE po.expected_date < ? AND po.status NOT IN ('Received', 'Cancelled', 'Closed')
                ORDER BY po.expected_date ASC
            ''', (today,)).fetchall()
            
            for po in overdue_pos:
                expected = datetime.strptime(po['expected_date'], '%Y-%m-%d')
                days_overdue = (datetime.now() - expected).days
                exceptions['overdue_purchase_orders'].append({
                    'number': po['po_number'],
                    'status': po['status'],
                    'expected_date': po['expected_date'],
                    'days_overdue': days_overdue,
                    'supplier': po['supplier_name'],
                    'amount': float(po['total_amount'] or 0)
                })
            
            overdue_sos = conn.execute('''
                SELECT so.so_number, so.status, so.required_date, c.name as customer_name,
                       so.total_amount, so.sales_type
                FROM sales_orders so
                LEFT JOIN customers c ON so.customer_id = c.id
                WHERE so.required_date < ? AND so.status NOT IN ('Shipped', 'Invoiced', 'Cancelled', 'Closed')
                ORDER BY so.required_date ASC
            ''', (today,)).fetchall()
            
            for so in overdue_sos:
                if so['required_date']:
                    required = datetime.strptime(so['required_date'], '%Y-%m-%d')
                    days_overdue = (datetime.now() - required).days
                    exceptions['overdue_sales_orders'].append({
                        'number': so['so_number'],
                        'status': so['status'],
                        'required_date': so['required_date'],
                        'days_overdue': days_overdue,
                        'customer': so['customer_name'],
                        'amount': float(so['total_amount'] or 0),
                        'type': so['sales_type']
                    })
            
            on_hold = conn.execute('''
                SELECT 'work_order' as type, wo_number as number, status, priority
                FROM work_orders WHERE status = 'On Hold'
                UNION ALL
                SELECT 'purchase_order' as type, po_number as number, status, NULL as priority
                FROM purchase_orders WHERE status = 'On Hold'
            ''').fetchall()
            
            for item in on_hold:
                exceptions['on_hold_items'].append({
                    'type': item['type'],
                    'number': item['number'],
                    'status': item['status'],
                    'priority': item['priority']
                })
            
            low_stock = conn.execute('''
                SELECT p.part_number, p.name, 
                       COALESCE(SUM(i.quantity), 0) as current_stock,
                       COALESCE(p.reorder_point, 0) as reorder_point
                FROM products p
                LEFT JOIN inventory i ON p.id = i.product_id
                WHERE p.reorder_point > 0
                GROUP BY p.id
                HAVING current_stock <= reorder_point
            ''').fetchall()
            
            for item in low_stock:
                exceptions['low_stock_alerts'].append({
                    'part_number': item['part_number'],
                    'name': item['name'],
                    'current_stock': item['current_stock'],
                    'reorder_point': item['reorder_point']
                })
        finally:
            conn.close()
        
        total_exceptions = (len(exceptions['overdue_work_orders']) + 
                           len(exceptions['overdue_purchase_orders']) +
                           len(exceptions['overdue_sales_orders']) +
                           len(exceptions['on_hold_items']) +
                           len(exceptions['low_stock_alerts']))
        
        return {
            'query_type': 'exceptions',
            'exceptions': exceptions,
            'total_count': total_exceptions,
            'timestamp': datetime.now().isoformat()
        }
    
    def _check_availability(self, parsed_intent):
        """Check material availability for work orders or general stock"""
        conn = self.db.get_connection()
        result = {'query_type': 'availability_check', 'checks': []}
        
        try:
            if 'work_order' in parsed_intent['transaction_types'] and parsed_intent['record_ids']:
                for record_ref in parsed_intent['record_ids']:
                    if record_ref['type'] == 'work_order':
                        wo = conn.execute('''
                            SELECT wo.id, wo.wo_number, wo.status
                            FROM work_orders wo
                            WHERE wo.wo_number LIKE ?
                        ''', (f'%{record_ref["id"]}%',)).fetchone()
                        
                        if wo:
                            materials = conn.execute('''
                                SELECT wom.*, p.name, p.part_number,
                                       (SELECT COALESCE(SUM(quantity), 0) FROM inventory WHERE product_id = wom.product_id) as available
                                FROM work_order_materials wom
                                JOIN products p ON wom.product_id = p.id
                                WHERE wom.work_order_id = ?
                            ''', (wo['id'],)).fetchall()
                            
                            check_result = {
                                'work_order': wo['wo_number'],
                                'materials': [],
                                'can_release': True
                            }
                            
                            for mat in materials:
                                needed = mat['quantity_required'] - (mat['quantity_issued'] or 0)
                                is_available = mat['available'] >= needed
                                if not is_available:
                                    check_result['can_release'] = False
                                
                                check_result['materials'].append({
                                    'part_number': mat['part_number'],
                                    'name': mat['name'],
                                    'required': mat['quantity_required'],
                                    'issued': mat['quantity_issued'] or 0,
                                    'needed': needed,
                                    'available': mat['available'],
                                    'is_available': is_available
                                })
                            
                            result['checks'].append(check_result)
            else:
                today_wos = conn.execute('''
                    SELECT wo.id, wo.wo_number, wo.status, wo.planned_end_date
                    FROM work_orders wo
                    WHERE wo.status IN ('Draft', 'Planned', 'Released')
                    ORDER BY wo.planned_end_date ASC LIMIT 10
                ''').fetchall()
                
                for wo in today_wos:
                    materials = conn.execute('''
                        SELECT wom.*, p.part_number,
                               (SELECT COALESCE(SUM(quantity), 0) FROM inventory WHERE product_id = wom.product_id) as available
                        FROM work_order_materials wom
                        JOIN products p ON wom.product_id = p.id
                        WHERE wom.work_order_id = ?
                    ''', (wo['id'],)).fetchall()
                    
                    shortages = []
                    for mat in materials:
                        needed = mat['quantity_required'] - (mat['quantity_issued'] or 0)
                        if mat['available'] < needed:
                            shortages.append({
                                'part_number': mat['part_number'],
                                'shortage': needed - mat['available']
                            })
                    
                    if shortages:
                        result['checks'].append({
                            'work_order': wo['wo_number'],
                            'due_date': wo['planned_end_date'],
                            'status': wo['status'],
                            'shortages': shortages,
                            'can_release': False
                        })
        finally:
            conn.close()
        
        result['all_available'] = all(c.get('can_release', True) for c in result['checks'])
        result['timestamp'] = datetime.now().isoformat()
        return result
    
    def _list_records(self, parsed_intent):
        """List records based on filters"""
        conn = self.db.get_connection()
        result = {'query_type': 'list', 'records': []}
        
        try:
            tx_type = parsed_intent['transaction_types'][0] if parsed_intent['transaction_types'] else 'work_order'
            status_filter = parsed_intent.get('status_filter')
            time_context = parsed_intent.get('time_context')
            
            if tx_type == 'work_order':
                query = '''
                    SELECT wo.wo_number, wo.status, wo.priority, wo.planned_end_date,
                           p.name as product_name, c.name as customer_name, wo.quantity
                    FROM work_orders wo
                    LEFT JOIN products p ON wo.product_id = p.id
                    LEFT JOIN customers c ON wo.customer_id = c.id
                    WHERE 1=1
                '''
                params = []
                
                if status_filter == 'open':
                    query += " AND wo.status IN ('Draft', 'Planned', 'Released', 'In Progress')"
                elif status_filter == 'closed':
                    query += " AND wo.status IN ('Completed', 'Shipped', 'Closed')"
                elif status_filter == 'on_hold':
                    query += " AND wo.status = 'On Hold'"
                
                if time_context:
                    query += " AND wo.created_at >= ?"
                    params.append(time_context['start_date'])
                
                query += " ORDER BY wo.planned_end_date ASC LIMIT 50"
                
                records = conn.execute(query, params).fetchall()
                result['record_type'] = 'work_orders'
                result['records'] = [dict(r) for r in records]
            
            elif tx_type == 'sales_order':
                query = '''
                    SELECT so.so_number, so.status, so.sales_type, so.order_date, so.required_date,
                           c.name as customer_name, so.total_amount
                    FROM sales_orders so
                    LEFT JOIN customers c ON so.customer_id = c.id
                    WHERE 1=1
                '''
                params = []
                
                if status_filter == 'open':
                    query += " AND so.status IN ('Draft', 'Pending', 'Confirmed')"
                elif status_filter == 'closed':
                    query += " AND so.status IN ('Shipped', 'Invoiced', 'Closed')"
                
                query += " ORDER BY so.order_date DESC LIMIT 50"
                
                records = conn.execute(query, params).fetchall()
                result['record_type'] = 'sales_orders'
                result['records'] = [dict(r) for r in records]
            
            elif tx_type == 'purchase_order':
                query = '''
                    SELECT po.po_number, po.status, po.order_date, po.expected_date,
                           s.name as supplier_name, po.total_amount
                    FROM purchase_orders po
                    LEFT JOIN suppliers s ON po.supplier_id = s.id
                    WHERE 1=1
                '''
                params = []
                
                if status_filter == 'open':
                    query += " AND po.status IN ('Draft', 'Sent', 'Partial')"
                
                query += " ORDER BY po.order_date DESC LIMIT 50"
                
                records = conn.execute(query, params).fetchall()
                result['record_type'] = 'purchase_orders'
                result['records'] = [dict(r) for r in records]
        finally:
            conn.close()
        
        result['count'] = len(result['records'])
        result['timestamp'] = datetime.now().isoformat()
        return result
    
    def _analyze_trend(self, parsed_intent):
        """Analyze trends and comparisons"""
        conn = self.db.get_connection()
        result = {'query_type': 'trend_analysis', 'trends': {}}
        
        try:
            today = datetime.now()
            this_month_start = today.replace(day=1).strftime('%Y-%m-%d')
            last_month_start = (today.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')
            last_month_end = (today.replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')
            
            this_month_wos = conn.execute('''
                SELECT COUNT(*) as count, 
                       SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed
                FROM work_orders WHERE created_at >= ?
            ''', (this_month_start,)).fetchone()
            
            last_month_wos = conn.execute('''
                SELECT COUNT(*) as count,
                       SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed
                FROM work_orders WHERE created_at >= ? AND created_at < ?
            ''', (last_month_start, this_month_start)).fetchone()
            
            result['trends']['work_orders'] = {
                'this_month': {'total': this_month_wos['count'], 'completed': this_month_wos['completed']},
                'last_month': {'total': last_month_wos['count'], 'completed': last_month_wos['completed']},
                'change_percent': ((this_month_wos['count'] - last_month_wos['count']) / max(last_month_wos['count'], 1)) * 100
            }
            
            this_month_revenue = conn.execute('''
                SELECT COALESCE(SUM(total_amount), 0) as revenue, COUNT(*) as count
                FROM invoices WHERE invoice_date >= ? AND status IN ('Posted', 'Paid', 'Partial')
            ''', (this_month_start,)).fetchone()
            
            last_month_revenue = conn.execute('''
                SELECT COALESCE(SUM(total_amount), 0) as revenue, COUNT(*) as count
                FROM invoices WHERE invoice_date >= ? AND invoice_date < ? AND status IN ('Posted', 'Paid', 'Partial')
            ''', (last_month_start, this_month_start)).fetchone()
            
            result['trends']['revenue'] = {
                'this_month': float(this_month_revenue['revenue']),
                'last_month': float(last_month_revenue['revenue']),
                'change_percent': ((this_month_revenue['revenue'] - last_month_revenue['revenue']) / max(last_month_revenue['revenue'], 1)) * 100
            }
        finally:
            conn.close()
        
        result['timestamp'] = datetime.now().isoformat()
        return result
    
    def _general_query(self, parsed_intent):
        """Handle general queries that don't fit specific patterns"""
        return {
            'query_type': 'general',
            'parsed_intent': parsed_intent,
            'message': 'Query understood. Please refine your question for specific transaction data.',
            'timestamp': datetime.now().isoformat()
        }
    
    def get_dependency_graph(self, record_type, record_id):
        """Build transaction dependency graph for cross-module reasoning"""
        conn = self.db.get_connection()
        graph = {'nodes': [], 'edges': [], 'root': None}
        
        try:
            if record_type == 'sales_order':
                so = conn.execute('''
                    SELECT so.*, c.name as customer_name
                    FROM sales_orders so
                    LEFT JOIN customers c ON so.customer_id = c.id
                    WHERE so.id = ? OR so.so_number LIKE ?
                ''', (record_id if str(record_id).isdigit() else 0, f'%{record_id}%')).fetchone()
                
                if so:
                    graph['root'] = {'type': 'sales_order', 'id': so['id'], 'number': so['so_number']}
                    graph['nodes'].append({
                        'type': 'sales_order', 
                        'id': so['id'], 
                        'number': so['so_number'],
                        'status': so['status'],
                        'customer': so['customer_name']
                    })
                    
                    work_orders = conn.execute('''
                        SELECT wo.id, wo.wo_number, wo.status, p.name as product_name
                        FROM work_orders wo
                        LEFT JOIN products p ON wo.product_id = p.id
                        WHERE wo.so_id = ?
                    ''', (so['id'],)).fetchall()
                    
                    for wo in work_orders:
                        graph['nodes'].append({
                            'type': 'work_order',
                            'id': wo['id'],
                            'number': wo['wo_number'],
                            'status': wo['status'],
                            'product': wo['product_name']
                        })
                        graph['edges'].append({
                            'from': {'type': 'sales_order', 'id': so['id']},
                            'to': {'type': 'work_order', 'id': wo['id']},
                            'relationship': 'generates'
                        })
                        
                        materials = conn.execute('''
                            SELECT wom.product_id, p.part_number, p.name,
                                   wom.quantity_required, wom.quantity_issued,
                                   (SELECT COALESCE(SUM(quantity), 0) FROM inventory WHERE product_id = wom.product_id) as stock
                            FROM work_order_materials wom
                            JOIN products p ON wom.product_id = p.id
                            WHERE wom.work_order_id = ?
                        ''', (wo['id'],)).fetchall()
                        
                        for mat in materials:
                            mat_node = {
                                'type': 'inventory',
                                'id': mat['product_id'],
                                'part_number': mat['part_number'],
                                'name': mat['name'],
                                'required': mat['quantity_required'],
                                'available': mat['stock'],
                                'is_shortage': mat['stock'] < (mat['quantity_required'] - (mat['quantity_issued'] or 0))
                            }
                            if not any(n['type'] == 'inventory' and n['id'] == mat['product_id'] for n in graph['nodes']):
                                graph['nodes'].append(mat_node)
                            
                            graph['edges'].append({
                                'from': {'type': 'work_order', 'id': wo['id']},
                                'to': {'type': 'inventory', 'id': mat['product_id']},
                                'relationship': 'requires'
                            })
        finally:
            conn.close()
        
        return graph
    
    def format_response_context(self, query_result):
        """Format query result into context for AI response generation"""
        if query_result['query_type'] == 'record_status':
            lines = []
            for record in query_result.get('records', []):
                if not record.get('found'):
                    lines.append(f"Record not found: {record.get('message', 'Unknown')}")
                    continue
                
                if record['type'] == 'work_order':
                    lines.append(f"\nWORK ORDER {record['number']}:")
                    lines.append(f"  Status: {record['status']} | Priority: {record['priority']}")
                    lines.append(f"  Product: {record['product']['name']} ({record['product']['part_number']})")
                    lines.append(f"  Customer: {record['customer']}")
                    lines.append(f"  Quantity: {record['quantity']}")
                    lines.append(f"  Due Date: {record['dates']['due_date']}")
                    lines.append(f"  Progress: {record['progress']['completed_tasks']}/{record['progress']['total_tasks']} tasks completed")
                    lines.append(f"  Labor Hours: {record['progress']['labor_hours']}")
                    
                    if record['materials']['shortages']:
                        lines.append(f"  MATERIAL SHORTAGES:")
                        for shortage in record['materials']['shortages']:
                            lines.append(f"    - {shortage['part_number']}: need {shortage['needed']}, have {shortage['available']} (short {shortage['shortage']})")
                    
                    if record['blocking_reasons']:
                        lines.append(f"  BLOCKING REASONS:")
                        for reason in record['blocking_reasons']:
                            lines.append(f"    - {reason}")
                
                elif record['type'] == 'sales_order':
                    lines.append(f"\nSALES ORDER {record['number']}:")
                    lines.append(f"  Status: {record['status']} | Type: {record['sales_type']}")
                    lines.append(f"  Customer: {record['customer']}")
                    lines.append(f"  Order Date: {record['dates']['order_date']}")
                    lines.append(f"  Required Date: {record['dates']['required_date']}")
                    lines.append(f"  Total Amount: ${record['financials']['total_amount']:,.2f}")
                    lines.append(f"  Lines: {record['lines']['shipped']}/{record['lines']['total']} shipped")
                    lines.append(f"  Work Orders: {record['work_orders']['completed']}/{record['work_orders']['total']} completed")
                    
                    if record.get('exchange_info'):
                        lines.append(f"  EXCHANGE INFO:")
                        lines.append(f"    Core Due Days: {record['exchange_info']['core_due_days']}")
                        lines.append(f"    Expected Core Return: {record['exchange_info']['expected_core_return']}")
                    
                    if record['blocking_reasons']:
                        lines.append(f"  BLOCKING REASONS:")
                        for reason in record['blocking_reasons']:
                            lines.append(f"    - {reason}")
                
                elif record['type'] == 'purchase_order':
                    lines.append(f"\nPURCHASE ORDER {record['number']}:")
                    lines.append(f"  Status: {record['status']}")
                    lines.append(f"  Supplier: {record['supplier']}")
                    lines.append(f"  Order Date: {record['dates']['order_date']}")
                    lines.append(f"  Expected Date: {record['dates']['expected_date']}")
                    lines.append(f"  Total Amount: ${record['financials']['total_amount']:,.2f}")
                    lines.append(f"  Lines Received: {record['lines']['fully_received']}/{record['lines']['total']}")
                    if record['is_overdue']:
                        lines.append(f"  *** OVERDUE ***")
                
                elif record['type'] == 'part':
                    lines.append(f"\nPART {record['part_number']}:")
                    lines.append(f"  Name: {record['name']}")
                    lines.append(f"  Total Stock: {record['inventory']['total_stock']} (across {record['inventory']['location_count']} locations)")
                    lines.append(f"  Reorder Point: {record['inventory']['reorder_point']}")
                    if record['inventory']['is_low_stock']:
                        lines.append(f"  *** LOW STOCK ALERT ***")
                    lines.append(f"  Pending WO Demand: {record['demand']['pending_wo_demand']}")
                    lines.append(f"  Net Available: {record['demand']['net_available']}")
                    lines.append(f"  Pending PO Supply: {record['supply']['pending_po_supply']}")
                    lines.append(f"  Projected Stock: {record['supply']['projected_stock']}")
            
            return '\n'.join(lines)
        
        elif query_result['query_type'] == 'blocking_cause':
            lines = []
            for exp in query_result.get('explanations', []):
                lines.append(f"\n{exp['record_type'].upper()} {exp['record_number']} - Status: {exp['current_status']}")
                lines.append(f"Summary: {exp['summary']}")
                for e in exp.get('explanations', []):
                    lines.append(f"\n  CAUSE: {e['cause']}")
                    lines.append(f"  Type: {e['type']}")
                    lines.append(f"  Impact: {e['impact']}")
                    lines.append(f"  Recommendation: {e['recommendation']}")
                    if e.get('pending_supply'):
                        ps = e['pending_supply']
                        lines.append(f"  Pending Supply: PO {ps['po_number']} ({ps['status']}) - Expected {ps['expected_date']}")
            return '\n'.join(lines)
        
        elif query_result['query_type'] == 'exceptions':
            lines = ["EXCEPTION REPORT:"]
            exc = query_result['exceptions']
            
            if exc['overdue_work_orders']:
                lines.append(f"\nOVERDUE WORK ORDERS ({len(exc['overdue_work_orders'])}):")
                for wo in exc['overdue_work_orders'][:10]:
                    lines.append(f"  - {wo['number']}: {wo['days_overdue']} days overdue, Status: {wo['status']}, Customer: {wo['customer']}")
            
            if exc['overdue_purchase_orders']:
                lines.append(f"\nOVERDUE PURCHASE ORDERS ({len(exc['overdue_purchase_orders'])}):")
                for po in exc['overdue_purchase_orders'][:10]:
                    lines.append(f"  - {po['number']}: {po['days_overdue']} days overdue, Supplier: {po['supplier']}, Amount: ${po['amount']:,.2f}")
            
            if exc['overdue_sales_orders']:
                lines.append(f"\nOVERDUE SALES ORDERS ({len(exc['overdue_sales_orders'])}):")
                for so in exc['overdue_sales_orders'][:10]:
                    lines.append(f"  - {so['number']}: {so['days_overdue']} days overdue, Customer: {so['customer']}, Amount: ${so['amount']:,.2f}")
            
            if exc['on_hold_items']:
                lines.append(f"\nON HOLD ITEMS ({len(exc['on_hold_items'])}):")
                for item in exc['on_hold_items'][:10]:
                    lines.append(f"  - {item['type']}: {item['number']}")
            
            if exc['low_stock_alerts']:
                lines.append(f"\nLOW STOCK ALERTS ({len(exc['low_stock_alerts'])}):")
                for item in exc['low_stock_alerts'][:10]:
                    lines.append(f"  - {item['part_number']}: {item['current_stock']} on hand (reorder point: {item['reorder_point']})")
            
            lines.append(f"\nTotal Exceptions: {query_result['total_count']}")
            return '\n'.join(lines)
        
        elif query_result['query_type'] == 'availability_check':
            lines = ["AVAILABILITY CHECK:"]
            for check in query_result.get('checks', []):
                lines.append(f"\n  Work Order: {check['work_order']}")
                lines.append(f"  Can Release: {'YES' if check.get('can_release') else 'NO'}")
                if check.get('shortages'):
                    lines.append("  Shortages:")
                    for s in check['shortages']:
                        lines.append(f"    - {s['part_number']}: short {s['shortage']}")
            
            lines.append(f"\nAll Available: {'YES' if query_result.get('all_available') else 'NO'}")
            return '\n'.join(lines)
        
        return json.dumps(query_result, indent=2, default=str)
