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
