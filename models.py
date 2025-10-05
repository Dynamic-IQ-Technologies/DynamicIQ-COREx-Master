import sqlite3
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash

class Database:
    def __init__(self, db_name='mrp.db'):
        self.db_name = db_name
        
    def get_connection(self):
        conn = sqlite3.connect(self.db_name)
        conn.row_factory = sqlite3.Row
        return conn
    
    def init_db(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                unit_of_measure TEXT NOT NULL,
                product_type TEXT NOT NULL,
                cost REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS boms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_product_id INTEGER NOT NULL,
                child_product_id INTEGER NOT NULL,
                quantity REAL NOT NULL,
                scrap_percentage REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (parent_product_id) REFERENCES products(id),
                FOREIGN KEY (child_product_id) REFERENCES products(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS suppliers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                contact_person TEXT,
                email TEXT,
                phone TEXT,
                address TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS inventory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                quantity REAL NOT NULL DEFAULT 0,
                reorder_point REAL DEFAULT 0,
                safety_stock REAL DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id),
                UNIQUE(product_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS work_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wo_number TEXT UNIQUE NOT NULL,
                product_id INTEGER NOT NULL,
                quantity REAL NOT NULL,
                status TEXT NOT NULL,
                priority TEXT DEFAULT 'Medium',
                planned_start_date DATE,
                planned_end_date DATE,
                actual_start_date DATE,
                actual_end_date DATE,
                material_cost REAL DEFAULT 0,
                labor_cost REAL DEFAULT 0,
                overhead_cost REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS purchase_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                po_number TEXT UNIQUE NOT NULL,
                supplier_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity REAL NOT NULL,
                unit_price REAL NOT NULL,
                status TEXT NOT NULL,
                order_date DATE,
                expected_delivery_date DATE,
                actual_delivery_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (supplier_id) REFERENCES suppliers(id),
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS material_requirements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                work_order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                required_quantity REAL NOT NULL,
                available_quantity REAL NOT NULL,
                shortage_quantity REAL NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (work_order_id) REFERENCES work_orders(id),
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                permission_key TEXT NOT NULL,
                permission_value INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id),
                UNIQUE(user_id, permission_key)
            )
        ''')
        
        cursor.execute("PRAGMA table_info(boms)")
        existing_columns = [col[1] for col in cursor.fetchall()]
        
        bom_columns = [
            ('find_number', 'TEXT'),
            ('category', 'TEXT DEFAULT "Other"'),
            ('revision', 'TEXT DEFAULT "A"'),
            ('effectivity_date', 'DATE'),
            ('status', 'TEXT DEFAULT "Active"'),
            ('reference_designator', 'TEXT'),
            ('level', 'INTEGER DEFAULT 0'),
            ('document_link', 'TEXT'),
            ('notes', 'TEXT'),
            ('unit_cost', 'REAL DEFAULT 0'),
            ('extended_cost', 'REAL DEFAULT 0')
        ]
        
        for col_name, col_type in bom_columns:
            if col_name not in existing_columns:
                try:
                    cursor.execute(f'ALTER TABLE boms ADD COLUMN {col_name} {col_type}')
                except sqlite3.OperationalError:
                    pass
        
        conn.commit()
        conn.close()

class User:
    @staticmethod
    def create(username, email, password, role):
        db = Database()
        conn = db.get_connection()
        cursor = conn.cursor()
        password_hash = generate_password_hash(password)
        cursor.execute(
            'INSERT INTO users (username, email, password_hash, role) VALUES (?, ?, ?, ?)',
            (username, email, password_hash, role)
        )
        conn.commit()
        user_id = cursor.lastrowid
        conn.close()
        return user_id
    
    @staticmethod
    def get_by_id(user_id):
        db = Database()
        conn = db.get_connection()
        user = conn.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()
        conn.close()
        return user
    
    @staticmethod
    def get_by_username(username):
        db = Database()
        conn = db.get_connection()
        user = conn.execute('SELECT * FROM users WHERE username = ?', (username,)).fetchone()
        conn.close()
        return user
    
    @staticmethod
    def verify_password(user, password):
        return check_password_hash(user['password_hash'], password)
    
    @staticmethod
    def get_all():
        db = Database()
        conn = db.get_connection()
        users = conn.execute('SELECT id, username, email, role, created_at FROM users ORDER BY created_at DESC').fetchall()
        conn.close()
        return users
    
    @staticmethod
    def update_role(user_id, new_role):
        db = Database()
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET role = ? WHERE id = ?', (new_role, user_id))
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_permissions(user_id):
        db = Database()
        conn = db.get_connection()
        permissions = conn.execute(
            'SELECT permission_key, permission_value FROM user_permissions WHERE user_id = ?',
            (user_id,)
        ).fetchall()
        conn.close()
        return {p['permission_key']: p['permission_value'] for p in permissions}
    
    @staticmethod
    def set_permission(user_id, permission_key, permission_value):
        db = Database()
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            '''INSERT INTO user_permissions (user_id, permission_key, permission_value) 
               VALUES (?, ?, ?)
               ON CONFLICT(user_id, permission_key) 
               DO UPDATE SET permission_value = ?''',
            (user_id, permission_key, permission_value, permission_value)
        )
        conn.commit()
        conn.close()
    
    @staticmethod
    def get_all_with_permissions():
        db = Database()
        conn = db.get_connection()
        users = conn.execute(
            'SELECT id, username, email, role, created_at FROM users ORDER BY created_at DESC'
        ).fetchall()
        
        result = []
        for user in users:
            permissions = conn.execute(
                'SELECT permission_key, permission_value FROM user_permissions WHERE user_id = ?',
                (user['id'],)
            ).fetchall()
            
            user_dict = dict(user)
            user_dict['permissions'] = {p['permission_key']: p['permission_value'] for p in permissions}
            result.append(user_dict)
        
        conn.close()
        return result
