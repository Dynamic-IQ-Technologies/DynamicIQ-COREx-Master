from models import Database

class BOMHierarchy:
    @staticmethod
    def calculate_levels(parent_product_id=None):
        db = Database()
        conn = db.get_connection()
        
        if parent_product_id:
            boms = conn.execute('''
                SELECT b.*, p1.code as parent_code, p1.name as parent_name,
                       p2.code as child_code, p2.name as child_name, p2.product_type,
                       p2.cost as child_cost
                FROM boms b
                JOIN products p1 ON b.parent_product_id = p1.id
                JOIN products p2 ON b.child_product_id = p2.id
                WHERE b.parent_product_id = ?
                ORDER BY b.find_number, b.id
            ''', (parent_product_id,)).fetchall()
        else:
            boms = conn.execute('''
                SELECT b.*, p1.code as parent_code, p1.name as parent_name,
                       p2.code as child_code, p2.name as child_name, p2.product_type,
                       p2.cost as child_cost
                FROM boms b
                JOIN products p1 ON b.parent_product_id = p1.id
                JOIN products p2 ON b.child_product_id = p2.id
                ORDER BY p1.code, b.find_number, b.id
            ''').fetchall()
        
        conn.close()
        return boms
    
    @staticmethod
    def build_hierarchy_tree(parent_product_id, level=0, prefix=''):
        db = Database()
        conn = db.get_connection()
        
        items = []
        boms = conn.execute('''
            SELECT b.*, p1.code as parent_code, p1.name as parent_name,
                   p2.code as child_code, p2.name as child_name, p2.product_type,
                   p2.cost as child_cost, p1.product_type as parent_type
            FROM boms b
            JOIN products p1 ON b.parent_product_id = p1.id
            JOIN products p2 ON b.child_product_id = p2.id
            WHERE b.parent_product_id = ?
            ORDER BY b.find_number, b.id
        ''', (parent_product_id,)).fetchall()
        
        for index, bom in enumerate(boms, start=1):
            find_num = bom['find_number'] if bom['find_number'] else str(index)
            item_number = f"{prefix}{find_num}" if prefix else find_num
            
            extended_cost = (bom['quantity'] * bom['child_cost']) if bom['child_cost'] else 0
            
            item_data = {
                'bom': dict(bom),
                'level': level,
                'item_number': item_number,
                'extended_cost': extended_cost,
                'has_children': False,
                'children': []
            }
            
            child_boms = conn.execute(
                'SELECT id FROM boms WHERE parent_product_id = ?', 
                (bom['child_product_id'],)
            ).fetchall()
            
            if child_boms:
                item_data['has_children'] = True
                item_data['children'] = BOMHierarchy.build_hierarchy_tree(
                    bom['child_product_id'], 
                    level + 1, 
                    f"{item_number}."
                )
            
            items.append(item_data)
        
        conn.close()
        return items
    
    @staticmethod
    def get_next_find_number(parent_product_id):
        db = Database()
        conn = db.get_connection()
        
        result = conn.execute('''
            SELECT find_number FROM boms 
            WHERE parent_product_id = ? AND find_number IS NOT NULL
            ORDER BY CAST(find_number AS INTEGER) DESC LIMIT 1
        ''', (parent_product_id,)).fetchone()
        
        conn.close()
        
        if result and result['find_number']:
            try:
                return str(int(result['find_number']) + 1)
            except ValueError:
                return '1'
        return '1'
    
    @staticmethod
    def get_bom_summary(parent_product_id):
        db = Database()
        conn = db.get_connection()
        
        summary = {
            'total_items': 0,
            'categories': {},
            'total_cost': 0,
            'active_items': 0,
            'obsolete_items': 0
        }
        
        def collect_items(parent_id, level=0):
            items = conn.execute('''
                SELECT b.*, p.cost, p.product_type
                FROM boms b
                JOIN products p ON b.child_product_id = p.id
                WHERE b.parent_product_id = ?
            ''', (parent_id,)).fetchall()
            
            for item in items:
                summary['total_items'] += 1
                
                category = item['category'] if item['category'] else 'Other'
                if category not in summary['categories']:
                    summary['categories'][category] = {
                        'count': 0,
                        'total_cost': 0
                    }
                summary['categories'][category]['count'] += 1
                
                extended_cost = (item['quantity'] * item['cost']) if item['cost'] else 0
                summary['categories'][category]['total_cost'] += extended_cost
                summary['total_cost'] += extended_cost
                
                status = item['status'] if item['status'] else 'Active'
                if status == 'Active':
                    summary['active_items'] += 1
                else:
                    summary['obsolete_items'] += 1
                
                collect_items(item['child_product_id'], level + 1)
        
        collect_items(parent_product_id)
        conn.close()
        
        return summary
    
    @staticmethod
    def clone_bom(source_parent_id, target_parent_id):
        db = Database()
        conn = db.get_connection()
        cursor = conn.cursor()
        
        source_boms = conn.execute('''
            SELECT * FROM boms WHERE parent_product_id = ?
        ''', (source_parent_id,)).fetchall()
        
        for bom in source_boms:
            cursor.execute('''
                INSERT INTO boms (parent_product_id, child_product_id, quantity, scrap_percentage,
                                find_number, category, revision, effectivity_date, status,
                                reference_designator, level, document_link, notes, unit_cost, extended_cost)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                target_parent_id, bom['child_product_id'], bom['quantity'], bom['scrap_percentage'],
                bom['find_number'], bom['category'], bom['revision'], bom['effectivity_date'],
                bom['status'], bom['reference_designator'], bom['level'], bom['document_link'],
                bom['notes'], bom['unit_cost'], bom['extended_cost']
            ))
        
        conn.commit()
        conn.close()
        
        return len(source_boms)
