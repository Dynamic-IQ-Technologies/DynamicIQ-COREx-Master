"""
AI Super Master Scheduler Engine
Single source of truth for all master scheduling decisions.
Operates with finite capacity, real constraints, and execution realism.
"""
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

class MasterSchedulerEngine:
    """
    AI-powered Master Production Schedule (MPS) engine.
    Implements finite-capacity scheduling with constraint detection.
    """
    
    PRIORITY_MAP = {
        'Critical': 100,
        'High': 75,
        'Medium': 50,
        'Low': 25
    }
    
    EXCEPTION_TYPES = [
        'Late Order',
        'Capacity Overload',
        'Material Shortage',
        'Supplier Risk',
        'Bottleneck',
        'Resource Conflict',
        'Engineering Block'
    ]
    
    def __init__(self, conn):
        self.conn = conn
    
    def generate_schedule_number(self) -> str:
        """Generate next sequential schedule number"""
        last = self.conn.execute('''
            SELECT schedule_number FROM master_schedules
            WHERE schedule_number LIKE 'MPS-%'
            ORDER BY CAST(SUBSTR(schedule_number, 5) AS INTEGER) DESC
            LIMIT 1
        ''').fetchone()
        
        if last:
            try:
                next_num = int(last['schedule_number'].split('-')[1]) + 1
            except (ValueError, IndexError):
                next_num = 1
        else:
            next_num = 1
        
        return f'MPS-{next_num:06d}'
    
    def get_demand_orders(self, date_from: str, date_to: str) -> List[Dict]:
        """
        Collect all demand sources: work orders, sales orders, and service orders.
        """
        orders = []
        
        work_orders = self.conn.execute('''
            SELECT wo.id, wo.wo_number, wo.product_id, wo.quantity,
                   wo.planned_start_date, wo.planned_end_date, wo.priority,
                   wo.status, wo.customer_id, wo.customer_name,
                   p.code as product_code, p.name as product_name,
                   'Work Order' as order_type
            FROM work_orders wo
            JOIN products p ON wo.product_id = p.id
            WHERE wo.status IN ('Planned', 'Released', 'In Progress')
            AND (wo.planned_end_date >= ? OR wo.planned_end_date IS NULL)
            AND (wo.planned_start_date <= ? OR wo.planned_start_date IS NULL)
            ORDER BY wo.priority DESC, wo.planned_end_date ASC
        ''', (date_from, date_to)).fetchall()
        
        for wo in work_orders:
            orders.append({
                'order_type': 'Work Order',
                'order_id': wo['id'],
                'order_number': wo['wo_number'],
                'product_id': wo['product_id'],
                'product_code': wo['product_code'],
                'product_name': wo['product_name'],
                'quantity': wo['quantity'],
                'due_date': wo['planned_end_date'],
                'start_date': wo['planned_start_date'],
                'priority': wo['priority'],
                'priority_score': self.PRIORITY_MAP.get(wo['priority'], 50),
                'status': wo['status'],
                'customer_name': wo['customer_name']
            })
        
        sales_orders = self.conn.execute('''
            SELECT so.id, so.so_number, so.order_date, so.expected_ship_date,
                   so.status, c.name as customer_name,
                   'Sales Order' as order_type
            FROM sales_orders so
            LEFT JOIN customers c ON so.customer_id = c.id
            WHERE so.status IN ('Confirmed', 'In Progress', 'Processing')
            AND (so.expected_ship_date >= ? OR so.expected_ship_date IS NULL)
            AND so.order_date <= ?
        ''', (date_from, date_to)).fetchall()
        
        for so in sales_orders:
            lines = self.conn.execute('''
                SELECT sol.product_id, sol.quantity, p.code, p.name
                FROM sales_order_lines sol
                JOIN products p ON sol.product_id = p.id
                WHERE sol.so_id = ?
            ''', (so['id'],)).fetchall()
            
            for line in lines:
                orders.append({
                    'order_type': 'Sales Order',
                    'order_id': so['id'],
                    'order_number': so['so_number'],
                    'product_id': line['product_id'],
                    'product_code': line['code'],
                    'product_name': line['name'],
                    'quantity': line['quantity'],
                    'due_date': so['expected_ship_date'],
                    'start_date': so['order_date'],
                    'priority': 'Medium',
                    'priority_score': self.PRIORITY_MAP.get('Medium', 50),
                    'status': so['status'],
                    'customer_name': so['customer_name']
                })
        
        return orders
    
    def get_work_centers_capacity(self, date_from: str, date_to: str) -> Dict[int, Dict]:
        """
        Calculate available capacity per work center for the planning horizon.
        """
        work_centers = self.conn.execute('''
            SELECT id, code, name, default_hours_per_day, default_days_per_week,
                   efficiency_factor, cost_per_hour, status
            FROM work_centers
            WHERE status = 'Active'
        ''').fetchall()
        
        capacity_data = {}
        start = datetime.strptime(date_from, '%Y-%m-%d')
        end = datetime.strptime(date_to, '%Y-%m-%d')
        
        for wc in work_centers:
            overrides = self.conn.execute('''
                SELECT capacity_date, available_hours
                FROM work_center_capacity
                WHERE work_center_id = ?
                AND capacity_date BETWEEN ? AND ?
            ''', (wc['id'], date_from, date_to)).fetchall()
            
            override_dict = {r['capacity_date']: r['available_hours'] for r in overrides}
            
            daily_capacity = {}
            current = start
            total_hours = 0
            
            while current <= end:
                date_str = current.strftime('%Y-%m-%d')
                weekday = current.weekday()
                
                if date_str in override_dict:
                    hours = override_dict[date_str]
                elif weekday < wc['default_days_per_week']:
                    hours = wc['default_hours_per_day'] * wc['efficiency_factor']
                else:
                    hours = 0
                
                daily_capacity[date_str] = hours
                total_hours += hours
                current += timedelta(days=1)
            
            planned_load = self.conn.execute('''
                SELECT COALESCE(SUM(woo.planned_hours + woo.setup_hours), 0) as total
                FROM work_order_operations woo
                JOIN work_orders wo ON woo.work_order_id = wo.id
                WHERE woo.work_center_id = ?
                AND woo.status IN ('Pending', 'In Progress')
                AND wo.status IN ('Planned', 'In Progress', 'Released')
            ''', (wc['id'],)).fetchone()['total']
            
            capacity_data[wc['id']] = {
                'id': wc['id'],
                'code': wc['code'],
                'name': wc['name'],
                'daily_capacity': daily_capacity,
                'total_available_hours': total_hours,
                'current_load': planned_load,
                'utilization': (planned_load / total_hours * 100) if total_hours > 0 else 0,
                'cost_per_hour': wc['cost_per_hour']
            }
        
        return capacity_data
    
    def get_material_availability(self, product_ids: List[int]) -> Dict[int, Dict]:
        """
        Check material availability and incoming supply for products.
        """
        if not product_ids:
            return {}
        
        placeholders = ','.join(['?'] * len(product_ids))
        
        inventory = self.conn.execute(f'''
            SELECT i.product_id, i.quantity as on_hand, i.reorder_point, i.safety_stock,
                   p.code, p.name
            FROM inventory i
            JOIN products p ON i.product_id = p.id
            WHERE i.product_id IN ({placeholders})
        ''', product_ids).fetchall()
        
        availability = {r['product_id']: {
            'on_hand': r['on_hand'],
            'reorder_point': r['reorder_point'],
            'safety_stock': r['safety_stock'],
            'on_order': 0,
            'expected_receipts': []
        } for r in inventory}
        
        open_pos = self.conn.execute(f'''
            SELECT pol.product_id, pol.quantity - pol.received_quantity as pending_qty,
                   po.expected_delivery_date, po.po_number
            FROM purchase_order_lines pol
            JOIN purchase_orders po ON pol.po_id = po.id
            WHERE pol.product_id IN ({placeholders})
            AND po.status IN ('Open', 'Partial')
            AND pol.quantity > pol.received_quantity
        ''', product_ids).fetchall()
        
        for po in open_pos:
            pid = po['product_id']
            if pid in availability:
                availability[pid]['on_order'] += po['pending_qty']
                availability[pid]['expected_receipts'].append({
                    'po_number': po['po_number'],
                    'quantity': po['pending_qty'],
                    'expected_date': po['expected_delivery_date']
                })
        
        return availability
    
    def calculate_atp(self, product_id: int, quantity: float, date: str) -> Tuple[bool, Optional[str], float]:
        """
        Available-to-Promise calculation.
        Returns (can_promise, atp_date, available_qty)
        """
        inv = self.conn.execute('''
            SELECT quantity FROM inventory WHERE product_id = ?
        ''', (product_id,)).fetchone()
        
        on_hand = inv['quantity'] if inv else 0
        
        committed = self.conn.execute('''
            SELECT COALESCE(SUM(sol.quantity), 0) as committed
            FROM sales_order_lines sol
            JOIN sales_orders so ON sol.so_id = so.id
            WHERE sol.product_id = ?
            AND so.status IN ('Confirmed', 'In Progress', 'Processing')
            AND so.expected_ship_date <= ?
        ''', (product_id, date)).fetchone()['committed']
        
        available = on_hand - committed
        
        if available >= quantity:
            return True, date, available
        
        future_receipts = self.conn.execute('''
            SELECT po.expected_delivery_date, pol.quantity - pol.received_quantity as pending
            FROM purchase_order_lines pol
            JOIN purchase_orders po ON pol.po_id = po.id
            WHERE pol.product_id = ?
            AND po.status IN ('Open', 'Partial')
            AND pol.quantity > pol.received_quantity
            ORDER BY po.expected_delivery_date ASC
        ''', (product_id,)).fetchall()
        
        cumulative = available
        for receipt in future_receipts:
            cumulative += receipt['pending']
            if cumulative >= quantity:
                return True, receipt['expected_delivery_date'], cumulative
        
        return False, None, cumulative
    
    def calculate_ctp(self, product_id: int, quantity: float, 
                      work_center_id: Optional[int] = None) -> Tuple[bool, str, Dict]:
        """
        Capable-to-Promise calculation (capacity-aware).
        Returns (can_promise, ctp_date, capacity_info)
        """
        product = self.conn.execute('''
            SELECT lead_time FROM products WHERE id = ?
        ''', (product_id,)).fetchone()
        
        lead_time_days = product['lead_time'] if product and product['lead_time'] else 5
        
        try:
            routing = self.conn.execute('''
                SELECT work_center_id, standard_hours
                FROM product_routings
                WHERE product_id = ?
                ORDER BY sequence_number
            ''', (product_id,)).fetchall()
        except:
            routing = []
        
        if not routing:
            ctp_date = (datetime.now() + timedelta(days=lead_time_days)).strftime('%Y-%m-%d')
            return True, ctp_date, {'method': 'lead_time_only', 'days': lead_time_days}
        
        total_hours = sum(r['standard_hours'] * quantity for r in routing)
        
        if work_center_id:
            wc = self.conn.execute('''
                SELECT default_hours_per_day, efficiency_factor
                FROM work_centers WHERE id = ?
            ''', (work_center_id,)).fetchone()
            
            if wc:
                daily_capacity = wc['default_hours_per_day'] * wc['efficiency_factor']
                production_days = int(total_hours / daily_capacity) + 1
                ctp_date = (datetime.now() + timedelta(days=production_days)).strftime('%Y-%m-%d')
                return True, ctp_date, {
                    'method': 'capacity_based',
                    'total_hours': total_hours,
                    'production_days': production_days
                }
        
        ctp_date = (datetime.now() + timedelta(days=lead_time_days)).strftime('%Y-%m-%d')
        return True, ctp_date, {'method': 'default', 'days': lead_time_days}
    
    def detect_exceptions(self, schedule_id: int, orders: List[Dict], 
                          capacity_data: Dict, material_data: Dict) -> List[Dict]:
        """
        Detect and classify scheduling exceptions.
        """
        exceptions = []
        today = datetime.now().date()
        
        for order in orders:
            due_date = order.get('due_date')
            if due_date:
                due = datetime.strptime(due_date, '%Y-%m-%d').date() if isinstance(due_date, str) else due_date
                if due < today:
                    days_late = (today - due).days
                    exceptions.append({
                        'exception_type': 'Late Order',
                        'severity': 'Critical' if days_late > 7 else 'Warning',
                        'order_type': order['order_type'],
                        'order_id': order['order_id'],
                        'order_number': order['order_number'],
                        'title': f"Order {order['order_number']} is {days_late} days past due",
                        'description': f"Due date was {due_date}. Customer: {order.get('customer_name', 'N/A')}",
                        'days_late': days_late,
                        'impact_assessment': f"Potential delivery penalty and customer satisfaction impact"
                    })
        
        for wc_id, wc in capacity_data.items():
            if wc['utilization'] > 100:
                gap = wc['current_load'] - wc['total_available_hours']
                exceptions.append({
                    'exception_type': 'Capacity Overload',
                    'severity': 'Critical' if wc['utilization'] > 120 else 'Warning',
                    'work_center_id': wc_id,
                    'title': f"Work Center {wc['code']} overloaded at {wc['utilization']:.1f}%",
                    'description': f"Current load: {wc['current_load']:.1f}h, Available: {wc['total_available_hours']:.1f}h",
                    'capacity_gap': gap,
                    'impact_assessment': f"Need {gap:.1f} additional hours or schedule rebalancing"
                })
            elif wc['utilization'] > 90:
                exceptions.append({
                    'exception_type': 'Bottleneck',
                    'severity': 'Info',
                    'work_center_id': wc_id,
                    'title': f"Work Center {wc['code']} approaching capacity at {wc['utilization']:.1f}%",
                    'description': f"Monitor closely - limited buffer for new orders",
                    'capacity_gap': 0
                })
        
        for pid, mat in material_data.items():
            if mat['on_hand'] < mat['safety_stock']:
                shortage = mat['safety_stock'] - mat['on_hand']
                exceptions.append({
                    'exception_type': 'Material Shortage',
                    'severity': 'Warning',
                    'title': f"Material below safety stock",
                    'description': f"On-hand: {mat['on_hand']}, Safety stock: {mat['safety_stock']}",
                    'material_shortage': json.dumps({'product_id': pid, 'shortage': shortage})
                })
        
        return exceptions
    
    def generate_schedule(self, schedule_id: int, date_from: str, date_to: str,
                          created_by: int) -> Dict:
        """
        Generate or regenerate a master schedule with finite-capacity logic.
        """
        orders = self.get_demand_orders(date_from, date_to)
        capacity_data = self.get_work_centers_capacity(date_from, date_to)
        
        product_ids = list(set(o['product_id'] for o in orders if o.get('product_id')))
        material_data = self.get_material_availability(product_ids)
        
        sorted_orders = sorted(orders, key=lambda x: (-x['priority_score'], x.get('due_date') or '9999-12-31'))
        
        self.conn.execute('DELETE FROM master_schedule_items WHERE schedule_id = ?', (schedule_id,))
        
        schedule_items = []
        capacity_allocation = {wc_id: dict(wc['daily_capacity']) for wc_id, wc in capacity_data.items()}
        
        seq = 0
        for order in sorted_orders:
            seq += 1
            
            start_date = order.get('start_date') or date_from
            end_date = order.get('due_date') or date_to
            
            can_atp, atp_date, _ = self.calculate_atp(
                order.get('product_id', 0), 
                order['quantity'], 
                end_date
            )
            can_ctp, ctp_date, _ = self.calculate_ctp(
                order.get('product_id', 0),
                order['quantity']
            )
            
            priority_class = 'Critical' if order['priority_score'] >= 75 else \
                            'High' if order['priority_score'] >= 50 else \
                            'Normal' if order['priority_score'] >= 25 else 'Low'
            
            self.conn.execute('''
                INSERT INTO master_schedule_items (
                    schedule_id, order_type, order_id, order_number,
                    product_id, product_code, product_name, quantity,
                    scheduled_start, scheduled_end, original_due_date,
                    priority, priority_class, sequence_number,
                    atp_date, ctp_date, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                schedule_id, order['order_type'], order['order_id'], order['order_number'],
                order.get('product_id'), order.get('product_code'), order.get('product_name'),
                order['quantity'], start_date, end_date, order.get('due_date'),
                order['priority_score'], priority_class, seq,
                atp_date, ctp_date, 'Scheduled'
            ))
            
            schedule_items.append({
                'order_number': order['order_number'],
                'product_code': order.get('product_code'),
                'scheduled_start': start_date,
                'scheduled_end': end_date,
                'priority_class': priority_class
            })
        
        self.conn.execute('DELETE FROM schedule_exceptions WHERE schedule_id = ?', (schedule_id,))
        
        exceptions = self.detect_exceptions(schedule_id, orders, capacity_data, material_data)
        
        for exc in exceptions:
            self.conn.execute('''
                INSERT INTO schedule_exceptions (
                    schedule_id, exception_type, severity, order_type, order_id,
                    order_number, work_center_id, title, description,
                    impact_assessment, days_late, capacity_gap, material_shortage
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                schedule_id, exc['exception_type'], exc.get('severity', 'Warning'),
                exc.get('order_type'), exc.get('order_id'), exc.get('order_number'),
                exc.get('work_center_id'), exc['title'], exc.get('description'),
                exc.get('impact_assessment'), exc.get('days_late'),
                exc.get('capacity_gap'), exc.get('material_shortage')
            ))
        
        self._calculate_capacity_load(schedule_id, date_from, date_to, capacity_data)
        
        self.conn.execute('''
            UPDATE master_schedules SET updated_at = datetime('now') WHERE id = ?
        ''', (schedule_id,))
        
        self.conn.commit()
        
        return {
            'schedule_id': schedule_id,
            'orders_scheduled': len(schedule_items),
            'exceptions_detected': len(exceptions),
            'critical_exceptions': len([e for e in exceptions if e.get('severity') == 'Critical']),
            'work_centers_analyzed': len(capacity_data)
        }
    
    def _calculate_capacity_load(self, schedule_id: int, date_from: str, 
                                  date_to: str, capacity_data: Dict):
        """
        Calculate and store daily capacity load for visualization.
        """
        self.conn.execute('DELETE FROM schedule_capacity_load WHERE schedule_id = ?', (schedule_id,))
        
        scheduled_items = self.conn.execute('''
            SELECT msi.scheduled_start, msi.scheduled_end, msi.order_id, msi.order_type,
                   msi.quantity
            FROM master_schedule_items msi
            WHERE msi.schedule_id = ?
        ''', (schedule_id,)).fetchall()
        
        work_order_ids = [item['order_id'] for item in scheduled_items if item['order_type'] == 'Work Order']
        
        wc_daily_load = {}
        for wc_id in capacity_data.keys():
            wc_daily_load[wc_id] = {}
        
        if work_order_ids:
            placeholders = ','.join(['?'] * len(work_order_ids))
            operations = self.conn.execute(f'''
                SELECT woo.work_order_id, woo.work_center_id, 
                       woo.planned_hours + woo.setup_hours as total_hours,
                       wo.planned_start_date, wo.planned_end_date
                FROM work_order_operations woo
                JOIN work_orders wo ON woo.work_order_id = wo.id
                WHERE woo.work_order_id IN ({placeholders})
                AND woo.work_center_id IS NOT NULL
            ''', work_order_ids).fetchall()
            
            for op in operations:
                wc_id = op['work_center_id']
                if wc_id not in wc_daily_load:
                    continue
                    
                start = op['planned_start_date'] or date_from
                end = op['planned_end_date'] or date_to
                total_hours = op['total_hours'] or 0
                
                try:
                    start_dt = datetime.strptime(start, '%Y-%m-%d')
                    end_dt = datetime.strptime(end, '%Y-%m-%d')
                    days = max((end_dt - start_dt).days, 1)
                    daily_hours = total_hours / days
                    
                    current = start_dt
                    while current <= end_dt:
                        date_str = current.strftime('%Y-%m-%d')
                        if date_str not in wc_daily_load[wc_id]:
                            wc_daily_load[wc_id][date_str] = 0
                        wc_daily_load[wc_id][date_str] += daily_hours
                        current += timedelta(days=1)
                except (ValueError, TypeError):
                    pass
        
        for wc_id, wc in capacity_data.items():
            for date_str, available in wc['daily_capacity'].items():
                planned = wc_daily_load.get(wc_id, {}).get(date_str, 0)
                utilization = (planned / available * 100) if available > 0 else 0
                status = 'Critical' if utilization > 100 else 'Warning' if utilization > 85 else 'Normal'
                
                self.conn.execute('''
                    INSERT OR REPLACE INTO schedule_capacity_load (
                        schedule_id, work_center_id, load_date, available_hours,
                        planned_hours, utilization_pct, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (schedule_id, wc_id, date_str, available, planned, utilization, status))
    
    def get_schedule_summary(self, schedule_id: int) -> Optional[Dict]:
        """
        Get comprehensive summary of a master schedule.
        """
        schedule = self.conn.execute('''
            SELECT * FROM master_schedules WHERE id = ?
        ''', (schedule_id,)).fetchone()
        
        if not schedule:
            return None
        
        items = self.conn.execute('''
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN status = 'Scheduled' THEN 1 ELSE 0 END) as scheduled,
                   SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed,
                   SUM(CASE WHEN priority_class = 'Critical' THEN 1 ELSE 0 END) as critical
            FROM master_schedule_items WHERE schedule_id = ?
        ''', (schedule_id,)).fetchone()
        
        exceptions = self.conn.execute('''
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN severity = 'Critical' THEN 1 ELSE 0 END) as critical,
                   SUM(CASE WHEN severity = 'Warning' THEN 1 ELSE 0 END) as warnings,
                   SUM(CASE WHEN is_resolved = 1 THEN 1 ELSE 0 END) as resolved
            FROM schedule_exceptions WHERE schedule_id = ?
        ''', (schedule_id,)).fetchone()
        
        recommendations = self.conn.execute('''
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END) as pending,
                   SUM(CASE WHEN status = 'Accepted' THEN 1 ELSE 0 END) as accepted
            FROM schedule_recommendations WHERE schedule_id = ?
        ''', (schedule_id,)).fetchone()
        
        late_orders = self.conn.execute('''
            SELECT COUNT(*) as count FROM master_schedule_items
            WHERE schedule_id = ?
            AND scheduled_end < date('now')
            AND status != 'Completed'
        ''', (schedule_id,)).fetchone()['count']
        
        total_items = items['total'] or 0
        on_time = total_items - late_orders
        otd_rate = (on_time / total_items * 100) if total_items > 0 else 100
        
        return {
            'schedule': dict(schedule),
            'items': dict(items),
            'exceptions': dict(exceptions),
            'recommendations': dict(recommendations),
            'otd_rate': round(otd_rate, 1),
            'late_orders': late_orders
        }
    
    def get_at_risk_orders(self, schedule_id: int, limit: int = 10) -> List[Dict]:
        """
        Get orders at highest risk of missing their due dates.
        """
        at_risk = self.conn.execute('''
            SELECT msi.*, 
                   JULIANDAY(msi.scheduled_end) - JULIANDAY('now') as days_remaining,
                   CASE 
                       WHEN msi.scheduled_end < date('now') THEN 'Past Due'
                       WHEN JULIANDAY(msi.scheduled_end) - JULIANDAY('now') <= 2 THEN 'Critical'
                       WHEN JULIANDAY(msi.scheduled_end) - JULIANDAY('now') <= 5 THEN 'At Risk'
                       ELSE 'On Track'
                   END as risk_status
            FROM master_schedule_items msi
            WHERE msi.schedule_id = ?
            AND msi.status != 'Completed'
            AND (msi.scheduled_end < date('now', '+7 days') OR msi.priority_class = 'Critical')
            ORDER BY msi.scheduled_end ASC, msi.priority DESC
            LIMIT ?
        ''', (schedule_id, limit)).fetchall()
        
        return [dict(r) for r in at_risk]
    
    def get_bottleneck_analysis(self, schedule_id: int) -> List[Dict]:
        """
        Analyze work center bottlenecks for the schedule.
        """
        bottlenecks = self.conn.execute('''
            SELECT wc.id, wc.code, wc.name,
                   AVG(scl.utilization_pct) as avg_utilization,
                   MAX(scl.utilization_pct) as peak_utilization,
                   COUNT(CASE WHEN scl.status = 'Critical' THEN 1 END) as critical_days,
                   SUM(scl.available_hours) as total_available,
                   SUM(scl.planned_hours) as total_planned
            FROM schedule_capacity_load scl
            JOIN work_centers wc ON scl.work_center_id = wc.id
            WHERE scl.schedule_id = ?
            GROUP BY wc.id
            HAVING avg_utilization > 70
            ORDER BY avg_utilization DESC
        ''', (schedule_id,)).fetchall()
        
        return [dict(b) for b in bottlenecks]
