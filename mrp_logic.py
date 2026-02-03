from models import Database
from datetime import datetime

class MRPEngine:
    def __init__(self):
        self.db = Database()
    
    def explode_bom(self, product_id, quantity, level=0):
        conn = self.db.get_connection()
        
        bom_items = conn.execute('''
            SELECT b.*, p.code, p.name, p.unit_of_measure
            FROM boms b
            JOIN products p ON b.child_product_id = p.id
            WHERE b.parent_product_id = ?
        ''', (product_id,)).fetchall()
        
        requirements = []
        
        for item in bom_items:
            required_qty = quantity * item['quantity'] * (1 + item['scrap_percentage'] / 100)
            
            requirements.append({
                'product_id': item['child_product_id'],
                'code': item['code'],
                'name': item['name'],
                'unit_of_measure': item['unit_of_measure'],
                'required_quantity': required_qty,
                'level': level
            })
            
            sub_requirements = self.explode_bom(item['child_product_id'], required_qty, level + 1)
            requirements.extend(sub_requirements)
        
        conn.close()
        return requirements
    
    def calculate_requirements(self, work_order_id):
        conn = self.db.get_connection()
        
        work_order = conn.execute(
            'SELECT * FROM work_orders WHERE id = ?',
            (work_order_id,)
        ).fetchone()
        
        if not work_order:
            conn.close()
            return []
        
        requirements = self.explode_bom(work_order['product_id'], work_order['quantity'])
        
        aggregated = {}
        for req in requirements:
            pid = req['product_id']
            if pid in aggregated:
                aggregated[pid]['required_quantity'] += req['required_quantity']
            else:
                aggregated[pid] = req
        
        result = []
        for pid, req in aggregated.items():
            inventory = conn.execute(
                'SELECT quantity FROM inventory WHERE product_id = ?',
                (pid,)
            ).fetchone()
            
            available = inventory['quantity'] if inventory else 0
            shortage = max(0, req['required_quantity'] - available)
            
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO material_requirements 
                (work_order_id, product_id, required_quantity, available_quantity, shortage_quantity, status)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (work_order_id, pid, req['required_quantity'], available, shortage, 
                  'Satisfied' if shortage == 0 else 'Shortage'))
            
            result.append({
                'product_id': pid,
                'code': req['code'],
                'name': req['name'],
                'required_quantity': req['required_quantity'],
                'available_quantity': available,
                'shortage_quantity': shortage,
                'status': 'Satisfied' if shortage == 0 else 'Shortage'
            })
        
        conn.commit()
        conn.close()
        return result
    
    def get_shortage_items(self):
        conn = self.db.get_connection()
        
        shortages = conn.execute('''
            SELECT i.product_id, p.code, p.name, p.unit_of_measure,
                   i.quantity, i.reorder_point, i.safety_stock
            FROM inventory i
            JOIN products p ON i.product_id = p.id
            WHERE i.quantity <= i.reorder_point
        ''').fetchall()
        
        conn.close()
        return shortages
    
    def suggest_purchase_orders(self):
        shortages = self.get_shortage_items()
        suggestions = []
        
        for item in shortages:
            order_qty = max(item['reorder_point'] + item['safety_stock'] - item['quantity'], 0)
            if order_qty > 0:
                suggestions.append({
                    'product_id': item['product_id'],
                    'code': item['code'],
                    'name': item['name'],
                    'current_stock': item['quantity'],
                    'reorder_point': item['reorder_point'],
                    'suggested_quantity': order_qty
                })
        
        return suggestions
    
    def calculate_work_order_cost(self, work_order_id):
        conn = self.db.get_connection()
        
        work_order = conn.execute(
            'SELECT * FROM work_orders WHERE id = ?',
            (work_order_id,)
        ).fetchone()
        
        if not work_order:
            conn.close()
            return None
        
        # WO-level material requirements
        requirements = conn.execute('''
            SELECT mr.*, p.cost
            FROM material_requirements mr
            JOIN products p ON mr.product_id = p.id
            WHERE mr.work_order_id = ?
        ''', (work_order_id,)).fetchall()
        
        wo_material_cost = sum(req['required_quantity'] * (req['cost'] or 0) for req in requirements)
        
        # Task-level material requirements (use issued_qty * unit_cost for actual cost)
        task_materials = conn.execute('''
            SELECT tm.issued_qty, tm.unit_cost
            FROM work_order_task_materials tm
            JOIN work_order_tasks wot ON tm.task_id = wot.id
            WHERE wot.work_order_id = ?
        ''', (work_order_id,)).fetchall()
        
        task_material_cost = sum((tm['issued_qty'] or 0) * (tm['unit_cost'] or 0) for tm in task_materials)
        
        material_cost = wo_material_cost + task_material_cost
        
        conn.close()
        
        return {
            'work_order_id': work_order_id,
            'material_cost': material_cost,
            'labor_cost': work_order['labor_cost'],
            'overhead_cost': work_order['overhead_cost'],
            'total_cost': material_cost + work_order['labor_cost'] + work_order['overhead_cost']
        }
    
    def get_sales_order_demand(self):
        """Calculate material demand from confirmed/pending sales orders"""
        conn = self.db.get_connection()
        
        demand = conn.execute('''
            SELECT 
                sol.product_id,
                p.code,
                p.name,
                p.unit_of_measure,
                SUM(sol.quantity - COALESCE(sol.shipped_qty, 0)) as demand_qty,
                COUNT(DISTINCT so.id) as order_count,
                MIN(so.expected_ship_date) as earliest_need_date
            FROM sales_order_lines sol
            JOIN sales_orders so ON sol.sales_order_id = so.id
            JOIN products p ON sol.product_id = p.id
            WHERE so.status IN ('Confirmed', 'In Progress', 'Pending')
              AND (sol.quantity - COALESCE(sol.shipped_qty, 0)) > 0
            GROUP BY sol.product_id, p.code, p.name, p.unit_of_measure
            HAVING SUM(sol.quantity - COALESCE(sol.shipped_qty, 0)) > 0
        ''').fetchall()
        
        conn.close()
        return [dict(row) for row in demand]
    
    def get_work_order_demand(self):
        """Calculate material demand from open work orders"""
        conn = self.db.get_connection()
        
        demand = conn.execute('''
            SELECT 
                mr.product_id,
                p.code,
                p.name,
                p.unit_of_measure,
                SUM(mr.required_quantity - COALESCE(
                    (SELECT SUM(mi.quantity) FROM material_issuances mi 
                     WHERE mi.work_order_id = mr.work_order_id AND mi.product_id = mr.product_id), 0
                )) as demand_qty,
                COUNT(DISTINCT mr.work_order_id) as order_count,
                MIN(wo.due_date) as earliest_need_date
            FROM material_requirements mr
            JOIN work_orders wo ON mr.work_order_id = wo.id
            JOIN products p ON mr.product_id = p.id
            WHERE wo.status IN ('Open', 'In Progress', 'Released')
            GROUP BY mr.product_id, p.code, p.name, p.unit_of_measure
            HAVING SUM(mr.required_quantity) > 0
        ''').fetchall()
        
        conn.close()
        return [dict(row) for row in demand]
    
    def get_pending_po_supply(self):
        """Get expected supply from pending purchase orders"""
        conn = self.db.get_connection()
        
        supply = conn.execute('''
            SELECT 
                pol.product_id,
                p.code,
                p.name,
                p.unit_of_measure,
                SUM(pol.quantity - COALESCE(pol.received_qty, 0)) as pending_qty,
                COUNT(DISTINCT po.id) as po_count,
                MIN(po.expected_delivery_date) as earliest_arrival
            FROM purchase_order_lines pol
            JOIN purchase_orders po ON pol.purchase_order_id = po.id
            JOIN products p ON pol.product_id = p.id
            WHERE po.status IN ('Sent', 'Confirmed', 'Pending', 'Approved')
              AND (pol.quantity - COALESCE(pol.received_qty, 0)) > 0
            GROUP BY pol.product_id, p.code, p.name, p.unit_of_measure
        ''').fetchall()
        
        conn.close()
        return [dict(row) for row in supply]
    
    def calculate_net_requirements(self):
        """
        Calculate consolidated net material requirements:
        Net Requirement = (Sales Demand + WO Demand) - (On Hand + Pending PO)
        """
        conn = self.db.get_connection()
        
        sales_demand = {d['product_id']: d for d in self.get_sales_order_demand()}
        wo_demand = {d['product_id']: d for d in self.get_work_order_demand()}
        pending_supply = {s['product_id']: s for s in self.get_pending_po_supply()}
        
        all_products = set(sales_demand.keys()) | set(wo_demand.keys())
        
        requirements = []
        for product_id in all_products:
            inventory = conn.execute(
                'SELECT quantity, reorder_point, safety_stock FROM inventory WHERE product_id = ?',
                (product_id,)
            ).fetchone()
            
            on_hand = inventory['quantity'] if inventory else 0
            reorder_point = inventory['reorder_point'] if inventory else 0
            safety_stock = inventory['safety_stock'] if inventory else 0
            
            so_demand = sales_demand.get(product_id, {})
            wo_demand_item = wo_demand.get(product_id, {})
            po_supply = pending_supply.get(product_id, {})
            
            total_demand = (so_demand.get('demand_qty') or 0) + (wo_demand_item.get('demand_qty') or 0)
            total_supply = on_hand + (po_supply.get('pending_qty') or 0)
            
            net_requirement = max(0, total_demand - total_supply)
            
            suggested_order = max(0, net_requirement + safety_stock)
            
            product_info = so_demand if so_demand else wo_demand_item
            
            requirements.append({
                'product_id': product_id,
                'code': product_info.get('code', ''),
                'name': product_info.get('name', ''),
                'unit_of_measure': product_info.get('unit_of_measure', ''),
                'sales_order_demand': so_demand.get('demand_qty') or 0,
                'sales_order_count': so_demand.get('order_count') or 0,
                'work_order_demand': wo_demand_item.get('demand_qty') or 0,
                'work_order_count': wo_demand_item.get('order_count') or 0,
                'total_demand': total_demand,
                'on_hand': on_hand,
                'pending_po_qty': po_supply.get('pending_qty') or 0,
                'pending_po_count': po_supply.get('po_count') or 0,
                'total_supply': total_supply,
                'net_requirement': net_requirement,
                'safety_stock': safety_stock,
                'suggested_order_qty': suggested_order,
                'earliest_need_date': so_demand.get('earliest_need_date') or wo_demand_item.get('earliest_need_date'),
                'status': 'Shortage' if net_requirement > 0 else 'Adequate'
            })
        
        conn.close()
        
        requirements.sort(key=lambda x: (-x['net_requirement'], x['code']))
        return requirements
