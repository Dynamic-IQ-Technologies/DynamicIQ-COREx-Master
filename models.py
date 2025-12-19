import sqlite3
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash

class Database:
    def __init__(self, db_name='mrp.db'):
        self.db_name = db_name
        
    def get_connection(self):
        conn = sqlite3.connect(self.db_name)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys = ON')
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
                last_login TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        try:
            cursor.execute('ALTER TABLE users ADD COLUMN last_login TIMESTAMP')
        except:
            pass
        
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
            CREATE TABLE IF NOT EXISTS product_alternates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                alternate_product_id INTEGER NOT NULL,
                relationship_type TEXT DEFAULT 'Interchangeable',
                priority INTEGER DEFAULT 1,
                notes TEXT,
                approved_by TEXT,
                approved_date DATE,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
                FOREIGN KEY (alternate_product_id) REFERENCES products(id) ON DELETE CASCADE,
                UNIQUE(product_id, alternate_product_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS product_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                file_name TEXT NOT NULL,
                original_name TEXT NOT NULL,
                file_type TEXT,
                file_size INTEGER,
                file_category TEXT DEFAULT 'General',
                description TEXT,
                uploaded_by TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1,
                FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
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
                payment_terms INTEGER DEFAULT 30,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS supplier_contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                supplier_id INTEGER NOT NULL,
                contact_name TEXT NOT NULL,
                title TEXT,
                email TEXT,
                phone TEXT,
                mobile TEXT,
                department TEXT,
                is_primary INTEGER DEFAULT 0,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (supplier_id) REFERENCES suppliers(id) ON DELETE CASCADE
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
            CREATE TABLE IF NOT EXISTS work_order_stages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                description TEXT,
                color TEXT DEFAULT '#6c757d',
                sequence INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                serial_number TEXT,
                description TEXT,
                planned_start_date DATE,
                planned_end_date DATE,
                actual_start_date DATE,
                actual_end_date DATE,
                material_cost REAL DEFAULT 0,
                labor_cost REAL DEFAULT 0,
                overhead_cost REAL DEFAULT 0,
                stage_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (stage_id) REFERENCES work_order_stages(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS purchase_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                po_number TEXT UNIQUE NOT NULL,
                supplier_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                order_date DATE,
                expected_delivery_date DATE,
                actual_delivery_date DATE,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS purchase_order_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                po_id INTEGER NOT NULL,
                line_number INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity REAL NOT NULL,
                unit_price REAL NOT NULL,
                uom_id INTEGER,
                received_quantity REAL DEFAULT 0,
                description TEXT,
                exchange_fee_flag INTEGER DEFAULT 0,
                source_so_line_id INTEGER,
                reference_part_number TEXT,
                reference_serial_number TEXT,
                base_quantity REAL DEFAULT 0,
                base_uom_id INTEGER,
                conversion_factor_used REAL DEFAULT 1.0,
                base_received_quantity REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (po_id) REFERENCES purchase_orders(id) ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (uom_id) REFERENCES uom_master(id),
                FOREIGN KEY (source_so_line_id) REFERENCES sales_order_lines(id),
                UNIQUE(po_id, line_number)
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
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS company_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                company_name TEXT NOT NULL,
                dba TEXT,
                address_line1 TEXT,
                address_line2 TEXT,
                city TEXT,
                state TEXT,
                postal_code TEXT,
                country TEXT,
                phone TEXT,
                email TEXT,
                website TEXT,
                tax_id TEXT,
                duns_number TEXT,
                cage_code TEXT,
                logo_filename TEXT,
                auto_post_invoice_gl INTEGER DEFAULT 0,
                updated_by INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (updated_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS receiving_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                receipt_number TEXT UNIQUE NOT NULL,
                po_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity_received REAL NOT NULL,
                receipt_date DATE NOT NULL,
                packing_slip_number TEXT,
                shipment_tracking TEXT,
                warehouse_location TEXT,
                receiver_name TEXT,
                condition TEXT DEFAULT 'New',
                remarks TEXT,
                received_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (po_id) REFERENCES purchase_orders(id),
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (received_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS material_issues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                issue_number TEXT UNIQUE NOT NULL,
                work_order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity_issued REAL NOT NULL,
                issue_date DATE NOT NULL,
                warehouse_location TEXT,
                bin_location TEXT,
                issued_to TEXT,
                task_reference TEXT,
                unit_cost REAL DEFAULT 0,
                total_cost REAL DEFAULT 0,
                remarks TEXT,
                issued_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (work_order_id) REFERENCES work_orders(id),
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (issued_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS material_returns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                return_number TEXT UNIQUE NOT NULL,
                work_order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity_returned REAL NOT NULL,
                return_date DATE NOT NULL,
                warehouse_location TEXT,
                bin_location TEXT,
                condition TEXT DEFAULT 'Serviceable',
                reason TEXT,
                remarks TEXT,
                returned_by INTEGER,
                unit_cost REAL DEFAULT 0,
                total_cost REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (work_order_id) REFERENCES work_orders(id),
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (returned_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS inventory_adjustments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                adjustment_number TEXT UNIQUE NOT NULL,
                product_id INTEGER NOT NULL,
                adjustment_type TEXT,
                quantity_before REAL NOT NULL,
                quantity_adjusted REAL NOT NULL,
                quantity_after REAL NOT NULL,
                adjustment_date DATE NOT NULL,
                reason TEXT NOT NULL,
                reference TEXT,
                warehouse_location TEXT,
                remarks TEXT,
                adjusted_by INTEGER,
                unit_cost REAL DEFAULT 0,
                cost_impact REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (adjusted_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute("PRAGMA table_info(products)")
        product_columns = [col[1] for col in cursor.fetchall()]
        
        product_new_columns = [
            ('part_category', 'TEXT DEFAULT "Other"'),
            ('lead_time', 'INTEGER DEFAULT 0'),
            ('product_category', 'TEXT'),
            ('manufacturer', 'TEXT'),
            ('applicability', 'TEXT'),
            ('shelf_life_cycle', 'TEXT'),
            ('eccn', 'TEXT'),
            ('part_notes', 'TEXT'),
            ('is_serialized', 'INTEGER DEFAULT 0')
        ]
        
        for col_name, col_type in product_new_columns:
            if col_name not in product_columns:
                try:
                    cursor.execute(f'ALTER TABLE products ADD COLUMN {col_name} {col_type}')
                except sqlite3.OperationalError:
                    pass
        
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
        
        cursor.execute("PRAGMA table_info(inventory)")
        inv_columns = [col[1] for col in cursor.fetchall()]
        
        inventory_new_columns = [
            ('condition', 'TEXT DEFAULT "New"'),
            ('warehouse_location', 'TEXT DEFAULT "Main"'),
            ('bin_location', 'TEXT'),
            ('status', 'TEXT DEFAULT "Available"'),
            ('reserved_quantity', 'REAL DEFAULT 0'),
            ('last_received_date', 'DATE'),
            ('is_serialized', 'INTEGER DEFAULT 0'),
            ('serial_number', 'TEXT'),
            ('expiration_date', 'DATE'),
            ('last_inspection_date', 'DATE'),
            ('next_inspection_date', 'DATE'),
            ('inspected_by', 'TEXT'),
            ('inspection_notes', 'TEXT'),
            ('trace_tag', 'TEXT'),
            ('trace', 'TEXT'),
            ('trace_type', 'TEXT'),
            ('msn_esn', 'TEXT'),
            ('mfr_code', 'TEXT'),
            ('lot_number', 'TEXT')
        ]
        
        for col_name, col_type in inventory_new_columns:
            if col_name not in inv_columns:
                try:
                    cursor.execute(f'ALTER TABLE inventory ADD COLUMN {col_name} {col_type}')
                except sqlite3.OperationalError:
                    pass
        
        cursor.execute("PRAGMA table_info(purchase_orders)")
        po_columns = [col[1] for col in cursor.fetchall()]
        
        po_new_columns = [
            ('received_quantity', 'REAL DEFAULT 0'),
            ('packing_slip_number', 'TEXT'),
            ('shipment_tracking', 'TEXT'),
            ('receiver_name', 'TEXT'),
            ('uom_id', 'INTEGER')
        ]
        
        for col_name, col_type in po_new_columns:
            if col_name not in po_columns:
                try:
                    cursor.execute(f'ALTER TABLE purchase_orders ADD COLUMN {col_name} {col_type}')
                except sqlite3.OperationalError:
                    pass
        
        # Add UOM conversion tracking to purchase_order_lines
        cursor.execute("PRAGMA table_info(purchase_order_lines)")
        pol_columns = [col[1] for col in cursor.fetchall()]
        
        pol_conversion_columns = [
            ('base_quantity', 'REAL DEFAULT 0'),
            ('base_uom_id', 'INTEGER'),
            ('conversion_factor_used', 'REAL DEFAULT 1.0'),
            ('base_received_quantity', 'REAL DEFAULT 0'),
            ('description', 'TEXT'),
            ('exchange_fee_flag', 'INTEGER DEFAULT 0'),
            ('source_so_line_id', 'INTEGER'),
            ('reference_part_number', 'TEXT'),
            ('reference_serial_number', 'TEXT')
        ]
        
        for col_name, col_type in pol_conversion_columns:
            if col_name not in pol_columns:
                try:
                    cursor.execute(f'ALTER TABLE purchase_order_lines ADD COLUMN {col_name} {col_type}')
                except sqlite3.OperationalError:
                    pass
        
        # Add cost tracking to material_returns
        cursor.execute("PRAGMA table_info(material_returns)")
        mr_columns = [col[1] for col in cursor.fetchall()]
        
        mr_cost_columns = [
            ('unit_cost', 'REAL DEFAULT 0'),
            ('total_cost', 'REAL DEFAULT 0')
        ]
        
        for col_name, col_type in mr_cost_columns:
            if col_name not in mr_columns:
                try:
                    cursor.execute(f'ALTER TABLE material_returns ADD COLUMN {col_name} {col_type}')
                except sqlite3.OperationalError:
                    pass
        
        # Add cost tracking to inventory_adjustments
        cursor.execute("PRAGMA table_info(inventory_adjustments)")
        ia_columns = [col[1] for col in cursor.fetchall()]
        
        ia_cost_columns = [
            ('adjustment_type', 'TEXT'),
            ('reference', 'TEXT'),
            ('unit_cost', 'REAL DEFAULT 0'),
            ('cost_impact', 'REAL DEFAULT 0')
        ]
        
        for col_name, col_type in ia_cost_columns:
            if col_name not in ia_columns:
                try:
                    cursor.execute(f'ALTER TABLE inventory_adjustments ADD COLUMN {col_name} {col_type}')
                except sqlite3.OperationalError:
                    pass
        
        # Add versioning to product_uom_conversions
        cursor.execute("PRAGMA table_info(product_uom_conversions)")
        puc_columns = [col[1] for col in cursor.fetchall()]
        
        puc_version_columns = [
            ('version_number', 'INTEGER DEFAULT 1'),
            ('effective_date', 'DATE'),
            ('is_active', 'INTEGER DEFAULT 1'),
            ('version_notes', 'TEXT')
        ]
        
        for col_name, col_type in puc_version_columns:
            if col_name not in puc_columns:
                try:
                    cursor.execute(f'ALTER TABLE product_uom_conversions ADD COLUMN {col_name} {col_type}')
                except sqlite3.OperationalError:
                    pass
        
        # Create labor_resources table for employees/technicians
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS labor_resources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_code TEXT UNIQUE NOT NULL,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                role TEXT NOT NULL,
                skillset TEXT,
                hourly_rate REAL DEFAULT 0,
                cost_center TEXT,
                email TEXT,
                phone TEXT,
                status TEXT DEFAULT 'Active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Add user_id column to labor_resources if it doesn't exist
        lr_columns = [row[1] for row in cursor.execute('PRAGMA table_info(labor_resources)').fetchall()]
        if 'user_id' not in lr_columns:
            try:
                cursor.execute('ALTER TABLE labor_resources ADD COLUMN user_id INTEGER')
            except sqlite3.OperationalError:
                pass
        
        # Add clock_pin column to labor_resources for simple clock station authentication
        if 'clock_pin' not in lr_columns:
            try:
                cursor.execute('ALTER TABLE labor_resources ADD COLUMN clock_pin TEXT')
            except sqlite3.OperationalError:
                pass
        
        # Create skillsets table for defining available skillsets
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS skillsets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                skillset_name TEXT NOT NULL UNIQUE,
                description TEXT,
                category TEXT,
                status TEXT DEFAULT 'Active',
                required_level TEXT DEFAULT 'Intermediate',
                target_headcount INTEGER DEFAULT 0,
                criticality TEXT DEFAULT 'Medium',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Add capacity planning columns to skillsets if they don't exist
        skillset_columns = [row[1] for row in cursor.execute('PRAGMA table_info(skillsets)').fetchall()]
        if 'required_level' not in skillset_columns:
            try:
                cursor.execute('ALTER TABLE skillsets ADD COLUMN required_level TEXT DEFAULT "Intermediate"')
            except sqlite3.OperationalError:
                pass
        if 'target_headcount' not in skillset_columns:
            try:
                cursor.execute('ALTER TABLE skillsets ADD COLUMN target_headcount INTEGER DEFAULT 0')
            except sqlite3.OperationalError:
                pass
        if 'criticality' not in skillset_columns:
            try:
                cursor.execute('ALTER TABLE skillsets ADD COLUMN criticality TEXT DEFAULT "Medium"')
            except sqlite3.OperationalError:
                pass
        
        # Create labor_resource_skills junction table for multi-skillset assignment
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS labor_resource_skills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                labor_resource_id INTEGER NOT NULL,
                skillset_id INTEGER NOT NULL,
                skill_level TEXT NOT NULL,
                certified INTEGER DEFAULT 0,
                last_verified_date TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (labor_resource_id) REFERENCES labor_resources(id) ON DELETE CASCADE,
                FOREIGN KEY (skillset_id) REFERENCES skillsets(id) ON DELETE CASCADE,
                UNIQUE(labor_resource_id, skillset_id)
            )
        ''')
        
        # Create indexes for performance
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_lrs_labor_resource ON labor_resource_skills(labor_resource_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_lrs_skillset ON labor_resource_skills(skillset_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_skillsets_name ON skillsets(skillset_name)')
        except sqlite3.OperationalError:
            pass
        
        # Migrate existing skillset data to new structure (one-time migration)
        migrate_existing_skillsets = cursor.execute('''
            SELECT COUNT(*) FROM labor_resources WHERE skillset IS NOT NULL AND skillset != ''
        ''').fetchone()[0]
        
        if migrate_existing_skillsets > 0:
            # Check if migration already done
            already_migrated = cursor.execute('SELECT COUNT(*) FROM labor_resource_skills').fetchone()[0]
            
            if already_migrated == 0:
                # Get all unique non-empty skillsets from labor_resources
                existing_skillsets = cursor.execute('''
                    SELECT DISTINCT skillset FROM labor_resources 
                    WHERE skillset IS NOT NULL AND skillset != ''
                ''').fetchall()
                
                # Create skillset records for each unique skillset
                for skillset_row in existing_skillsets:
                    skillset_text = skillset_row[0].strip()
                    if skillset_text:
                        # Handle comma-separated skillsets
                        skillset_parts = [s.strip() for s in skillset_text.split(',') if s.strip()]
                        for skillset_name in skillset_parts:
                            try:
                                cursor.execute('''
                                    INSERT OR IGNORE INTO skillsets (skillset_name, category, status)
                                    VALUES (?, 'Migrated', 'Active')
                                ''', (skillset_name,))
                            except sqlite3.IntegrityError:
                                pass
                
                # Now populate the junction table
                labor_resources_with_skills = cursor.execute('''
                    SELECT id, skillset FROM labor_resources 
                    WHERE skillset IS NOT NULL AND skillset != ''
                ''').fetchall()
                
                for lr in labor_resources_with_skills:
                    lr_id = lr[0]
                    skillset_text = lr[1].strip()
                    if skillset_text:
                        # Handle comma-separated skillsets
                        skillset_parts = [s.strip() for s in skillset_text.split(',') if s.strip()]
                        for skillset_name in skillset_parts:
                            # Get skillset_id
                            skillset_id = cursor.execute('''
                                SELECT id FROM skillsets WHERE skillset_name = ?
                            ''', (skillset_name,)).fetchone()
                            
                            if skillset_id:
                                try:
                                    # Default migrated skills to 'Intermediate' level
                                    cursor.execute('''
                                        INSERT OR IGNORE INTO labor_resource_skills 
                                        (labor_resource_id, skillset_id, skill_level)
                                        VALUES (?, ?, 'Intermediate')
                                    ''', (lr_id, skillset_id[0]))
                                except sqlite3.IntegrityError:
                                    pass
        
        # Add bin_location column to receiving_transactions if it doesn't exist
        rt_columns = [row[1] for row in cursor.execute('PRAGMA table_info(receiving_transactions)').fetchall()]
        if 'bin_location' not in rt_columns:
            try:
                cursor.execute('ALTER TABLE receiving_transactions ADD COLUMN bin_location TEXT')
            except sqlite3.OperationalError:
                pass
        
        # Add auto_post_invoice_gl column to company_settings if it doesn't exist
        cs_columns = [row[1] for row in cursor.execute('PRAGMA table_info(company_settings)').fetchall()]
        if 'auto_post_invoice_gl' not in cs_columns:
            try:
                cursor.execute('ALTER TABLE company_settings ADD COLUMN auto_post_invoice_gl INTEGER DEFAULT 0')
            except sqlite3.OperationalError:
                pass
        
        # Add Marketing Presentation Generator columns to company_settings
        if 'marketing_tagline' not in cs_columns:
            try:
                cursor.execute('ALTER TABLE company_settings ADD COLUMN marketing_tagline TEXT')
            except sqlite3.OperationalError:
                pass
        if 'brand_primary_color' not in cs_columns:
            try:
                cursor.execute("ALTER TABLE company_settings ADD COLUMN brand_primary_color TEXT DEFAULT '#1e40af'")
            except sqlite3.OperationalError:
                pass
        if 'brand_secondary_color' not in cs_columns:
            try:
                cursor.execute("ALTER TABLE company_settings ADD COLUMN brand_secondary_color TEXT DEFAULT '#f97316'")
            except sqlite3.OperationalError:
                pass
        if 'brand_accent_color' not in cs_columns:
            try:
                cursor.execute("ALTER TABLE company_settings ADD COLUMN brand_accent_color TEXT DEFAULT '#10b981'")
            except sqlite3.OperationalError:
                pass
        if 'brand_tone' not in cs_columns:
            try:
                cursor.execute("ALTER TABLE company_settings ADD COLUMN brand_tone TEXT DEFAULT 'Enterprise'")
            except sqlite3.OperationalError:
                pass
        if 'marketing_description' not in cs_columns:
            try:
                cursor.execute('ALTER TABLE company_settings ADD COLUMN marketing_description TEXT')
            except sqlite3.OperationalError:
                pass
        if 'target_industries' not in cs_columns:
            try:
                cursor.execute('ALTER TABLE company_settings ADD COLUMN target_industries TEXT')
            except sqlite3.OperationalError:
                pass
        if 'key_differentiators' not in cs_columns:
            try:
                cursor.execute('ALTER TABLE company_settings ADD COLUMN key_differentiators TEXT')
            except sqlite3.OperationalError:
                pass
        
        # Add unit_cost column to work_order_task_materials if it doesn't exist
        wotm_columns = [row[1] for row in cursor.execute('PRAGMA table_info(work_order_task_materials)').fetchall()]
        if 'unit_cost' not in wotm_columns:
            try:
                cursor.execute('ALTER TABLE work_order_task_materials ADD COLUMN unit_cost REAL DEFAULT 0')
                cursor.execute('''
                    UPDATE work_order_task_materials 
                    SET unit_cost = COALESCE(
                        (SELECT COALESCE(p.cost, i.unit_cost, 0) 
                         FROM products p 
                         LEFT JOIN inventory i ON i.product_id = p.id 
                         WHERE p.id = work_order_task_materials.product_id),
                        0
                    )
                    WHERE unit_cost = 0 OR unit_cost IS NULL
                ''')
            except sqlite3.OperationalError:
                pass
        
        # Add work_order_id, task_id, and ndt_work_order_id columns to time_clock_punches if they don't exist
        tcp_columns = [row[1] for row in cursor.execute('PRAGMA table_info(time_clock_punches)').fetchall()]
        if 'work_order_id' not in tcp_columns:
            try:
                cursor.execute('ALTER TABLE time_clock_punches ADD COLUMN work_order_id INTEGER')
            except sqlite3.OperationalError:
                pass
        if 'task_id' not in tcp_columns:
            try:
                cursor.execute('ALTER TABLE time_clock_punches ADD COLUMN task_id INTEGER')
            except sqlite3.OperationalError:
                pass
        if 'ndt_work_order_id' not in tcp_columns:
            try:
                cursor.execute('ALTER TABLE time_clock_punches ADD COLUMN ndt_work_order_id INTEGER')
            except sqlite3.OperationalError:
                pass
        
        # Migrate single-line POs to multi-line structure
        po_columns = [row[1] for row in cursor.execute('PRAGMA table_info(purchase_orders)').fetchall()]
        if 'product_id' in po_columns:
            # Old structure exists, migrate data
            # Build SELECT query based on available columns
            select_cols = ['id', 'product_id', 'quantity', 'unit_price']
            if 'uom_id' in po_columns:
                select_cols.append('uom_id')
            if 'received_quantity' in po_columns:
                select_cols.append('received_quantity')
            
            old_pos = cursor.execute(f'''
                SELECT {', '.join(select_cols)}
                FROM purchase_orders
                WHERE product_id IS NOT NULL
            ''').fetchall()
            
            for row in old_pos:
                # Convert Row to dict for safe access
                po = dict(row)
                
                # Check if line already exists
                existing = cursor.execute('''
                    SELECT id FROM purchase_order_lines WHERE po_id = ? AND line_number = 1
                ''', (po['id'],)).fetchone()
                
                if not existing and po.get('product_id'):
                    # Safely get optional columns using dict.get()
                    uom_id = po.get('uom_id') if 'uom_id' in po_columns else None
                    received_qty = po.get('received_quantity', 0) if 'received_quantity' in po_columns else 0
                    
                    cursor.execute('''
                        INSERT INTO purchase_order_lines 
                        (po_id, line_number, product_id, quantity, unit_price, uom_id, received_quantity)
                        VALUES (?, 1, ?, ?, ?, ?, ?)
                    ''', (po['id'], po['product_id'], po['quantity'], po['unit_price'], 
                          uom_id, received_qty if received_qty else 0))
            
            conn.commit()
            
            # Remove old columns (SQLite doesn't support DROP COLUMN directly, so we'll keep them for backward compatibility)
            # They will be ignored in new code
        
        # Add notes column to purchase_orders if it doesn't exist
        if 'notes' not in po_columns:
            try:
                cursor.execute('ALTER TABLE purchase_orders ADD COLUMN notes TEXT')
            except sqlite3.OperationalError:
                pass
        
        # Create work_order_tasks table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS work_order_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_number TEXT UNIQUE NOT NULL,
                work_order_id INTEGER NOT NULL,
                task_name TEXT NOT NULL,
                description TEXT,
                category TEXT DEFAULT 'General',
                sequence_number INTEGER DEFAULT 0,
                priority TEXT DEFAULT 'Medium',
                planned_start_date TIMESTAMP,
                planned_end_date TIMESTAMP,
                actual_start_date TIMESTAMP,
                actual_end_date TIMESTAMP,
                planned_hours REAL DEFAULT 0,
                actual_hours REAL DEFAULT 0,
                planned_labor_cost REAL DEFAULT 0,
                actual_labor_cost REAL DEFAULT 0,
                assigned_resource_id INTEGER,
                status TEXT DEFAULT 'Not Started',
                remarks TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (work_order_id) REFERENCES work_orders(id),
                FOREIGN KEY (assigned_resource_id) REFERENCES labor_resources(id)
            )
        ''')
        
        # Add work_center_id column to work_order_tasks for capacity planning
        wot_columns = [row[1] for row in cursor.execute('PRAGMA table_info(work_order_tasks)').fetchall()]
        if 'work_center_id' not in wot_columns:
            try:
                cursor.execute('ALTER TABLE work_order_tasks ADD COLUMN work_center_id INTEGER REFERENCES work_centers(id)')
            except sqlite3.OperationalError:
                pass
        
        # Create task_templates table for reusable task templates
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS task_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_code TEXT UNIQUE NOT NULL,
                template_name TEXT NOT NULL,
                description TEXT,
                category TEXT DEFAULT 'General',
                status TEXT DEFAULT 'Active',
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                modified_at TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES users(id)
            )
        ''')
        
        # Create task_template_items table for individual tasks within a template
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS task_template_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_id INTEGER NOT NULL,
                task_name TEXT NOT NULL,
                description TEXT,
                category TEXT DEFAULT 'General',
                sequence_number INTEGER DEFAULT 0,
                priority TEXT DEFAULT 'Medium',
                planned_hours REAL DEFAULT 0,
                remarks TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (template_id) REFERENCES task_templates(id) ON DELETE CASCADE
            )
        ''')
        
        # Create order_stage_tracking table for customer service module
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS order_stage_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sales_order_id INTEGER,
                work_order_id INTEGER,
                stage_name TEXT NOT NULL,
                stage_order INTEGER DEFAULT 1,
                stage_status TEXT DEFAULT 'Not Started',
                percent_complete INTEGER DEFAULT 0,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                assigned_to INTEGER,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP,
                FOREIGN KEY (sales_order_id) REFERENCES sales_orders(id) ON DELETE CASCADE,
                FOREIGN KEY (work_order_id) REFERENCES work_orders(id) ON DELETE CASCADE,
                FOREIGN KEY (assigned_to) REFERENCES users(id)
            )
        ''')
        
        # Create work_order_confirmations table for CS module confirmation workflow
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS work_order_confirmations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                work_order_id INTEGER NOT NULL,
                confirmed_by INTEGER NOT NULL,
                confirmation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                quote_approved INTEGER DEFAULT 0,
                materials_available INTEGER DEFAULT 0,
                capacity_available INTEGER DEFAULT 0,
                confirmation_notes TEXT,
                previous_status TEXT,
                new_status TEXT DEFAULT 'Released',
                FOREIGN KEY (work_order_id) REFERENCES work_orders(id) ON DELETE CASCADE,
                FOREIGN KEY (confirmed_by) REFERENCES users(id)
            )
        ''')
        
        # Create indexes for order stage tracking
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_order_stage_so ON order_stage_tracking(sales_order_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_order_stage_wo ON order_stage_tracking(work_order_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_order_stage_status ON order_stage_tracking(stage_status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_wo_confirmations_wo ON work_order_confirmations(work_order_id)')
        
        # Create customer_communications table for Phase 3 CS module
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customer_communications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER NOT NULL,
                sales_order_id INTEGER,
                communication_type TEXT NOT NULL CHECK(communication_type IN ('Call', 'Email', 'Meeting', 'Note', 'Other')),
                subject TEXT,
                description TEXT,
                communication_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                follow_up_required INTEGER DEFAULT 0,
                follow_up_date DATE,
                follow_up_completed INTEGER DEFAULT 0,
                outcome TEXT,
                created_by INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
                FOREIGN KEY (sales_order_id) REFERENCES sales_orders(id) ON DELETE SET NULL,
                FOREIGN KEY (created_by) REFERENCES users(id)
            )
        ''')
        
        # Create order_notes table for quick notes on orders
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS order_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sales_order_id INTEGER NOT NULL,
                note_type TEXT DEFAULT 'General' CHECK(note_type IN ('General', 'Internal', 'Customer', 'Urgent', 'Follow-up')),
                note_text TEXT NOT NULL,
                is_pinned INTEGER DEFAULT 0,
                created_by INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_by INTEGER,
                updated_at TIMESTAMP,
                FOREIGN KEY (sales_order_id) REFERENCES sales_orders(id) ON DELETE CASCADE,
                FOREIGN KEY (created_by) REFERENCES users(id),
                FOREIGN KEY (updated_by) REFERENCES users(id)
            )
        ''')
        
        # Create order_activity_log table for activity timeline
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS order_activity_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sales_order_id INTEGER NOT NULL,
                activity_type TEXT NOT NULL,
                activity_description TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sales_order_id) REFERENCES sales_orders(id) ON DELETE CASCADE,
                FOREIGN KEY (created_by) REFERENCES users(id)
            )
        ''')
        
        # Create indexes for customer communications and order notes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_customer_comm_customer ON customer_communications(customer_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_customer_comm_so ON customer_communications(sales_order_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_customer_comm_followup ON customer_communications(follow_up_required, follow_up_completed)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_order_notes_so ON order_notes(sales_order_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_order_activity_so ON order_activity_log(sales_order_id)')
        
        # Phase 4: Escalation Management tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS order_escalations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sales_order_id INTEGER NOT NULL,
                escalation_level INTEGER DEFAULT 1,
                escalation_reason TEXT NOT NULL,
                priority TEXT DEFAULT 'High',
                assigned_to INTEGER,
                escalated_by INTEGER NOT NULL,
                escalated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                target_resolution_date DATE,
                resolved_at TIMESTAMP,
                resolved_by INTEGER,
                resolution_notes TEXT,
                status TEXT DEFAULT 'Open',
                FOREIGN KEY (sales_order_id) REFERENCES sales_orders(id) ON DELETE CASCADE,
                FOREIGN KEY (assigned_to) REFERENCES users(id),
                FOREIGN KEY (escalated_by) REFERENCES users(id),
                FOREIGN KEY (resolved_by) REFERENCES users(id)
            )
        ''')
        
        # SLA Configuration table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sla_configurations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sla_name TEXT NOT NULL,
                order_type TEXT,
                customer_tier TEXT,
                response_time_hours INTEGER DEFAULT 24,
                resolution_time_hours INTEGER DEFAULT 72,
                escalation_time_hours INTEGER DEFAULT 48,
                is_active INTEGER DEFAULT 1,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES users(id)
            )
        ''')
        
        # Customer Feedback/Satisfaction table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customer_feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sales_order_id INTEGER,
                work_order_id INTEGER,
                customer_id INTEGER NOT NULL,
                rating INTEGER NOT NULL,
                feedback_type TEXT DEFAULT 'Order Completion',
                comments TEXT,
                would_recommend INTEGER DEFAULT 1,
                submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                follow_up_required INTEGER DEFAULT 0,
                follow_up_notes TEXT,
                FOREIGN KEY (sales_order_id) REFERENCES sales_orders(id),
                FOREIGN KEY (work_order_id) REFERENCES work_orders(id),
                FOREIGN KEY (customer_id) REFERENCES customers(id)
            )
        ''')
        
        # Create indexes for Phase 4 tables
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_escalation_so ON order_escalations(sales_order_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_escalation_status ON order_escalations(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_feedback_customer ON customer_feedback(customer_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_feedback_rating ON customer_feedback(rating)')
        
        # Create labor_issuance table for time tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS labor_issuance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                issuance_number TEXT UNIQUE NOT NULL,
                task_id INTEGER NOT NULL,
                work_order_id INTEGER NOT NULL,
                resource_id INTEGER NOT NULL,
                work_date DATE NOT NULL,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                hours_worked REAL NOT NULL,
                hourly_rate REAL DEFAULT 0,
                labor_cost REAL DEFAULT 0,
                remarks TEXT,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (task_id) REFERENCES work_order_tasks(id),
                FOREIGN KEY (work_order_id) REFERENCES work_orders(id),
                FOREIGN KEY (resource_id) REFERENCES labor_resources(id),
                FOREIGN KEY (created_by) REFERENCES users(id)
            )
        ''')
        
        # Create work_order_time_tracking table for clock in/out
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS work_order_time_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_number TEXT UNIQUE NOT NULL,
                employee_id INTEGER NOT NULL,
                work_order_id INTEGER NOT NULL,
                task_id INTEGER,
                clock_in_time TIMESTAMP NOT NULL,
                clock_out_time TIMESTAMP,
                hours_worked REAL DEFAULT 0,
                labor_cost REAL DEFAULT 0,
                hourly_rate REAL DEFAULT 0,
                status TEXT DEFAULT 'In Progress',
                notes TEXT,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                modified_by INTEGER,
                modified_at TIMESTAMP,
                FOREIGN KEY (employee_id) REFERENCES labor_resources(id),
                FOREIGN KEY (work_order_id) REFERENCES work_orders(id),
                FOREIGN KEY (task_id) REFERENCES work_order_tasks(id),
                FOREIGN KEY (created_by) REFERENCES users(id),
                FOREIGN KEY (modified_by) REFERENCES users(id)
            )
        ''')
        
        # Create simplified time_clock_punches table for standalone clock station
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS time_clock_punches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                punch_number TEXT UNIQUE NOT NULL,
                employee_id INTEGER NOT NULL,
                punch_type TEXT NOT NULL,
                punch_time TIMESTAMP NOT NULL,
                location TEXT,
                ip_address TEXT,
                device_info TEXT,
                project_name TEXT,
                work_order_id INTEGER,
                task_id INTEGER,
                notes TEXT,
                status TEXT DEFAULT 'Approved',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (employee_id) REFERENCES labor_resources(id),
                FOREIGN KEY (work_order_id) REFERENCES work_orders(id),
                FOREIGN KEY (task_id) REFERENCES work_order_tasks(id)
            )
        ''')
        
        # Create timesheet_approvals table for manager approvals
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS timesheet_approvals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_id INTEGER NOT NULL,
                period_start DATE NOT NULL,
                period_end DATE NOT NULL,
                total_hours REAL DEFAULT 0,
                status TEXT DEFAULT 'Pending',
                submitted_at TIMESTAMP,
                approved_by INTEGER,
                approved_at TIMESTAMP,
                rejection_reason TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (employee_id) REFERENCES labor_resources(id),
                FOREIGN KEY (approved_by) REFERENCES users(id)
            )
        ''')
        
        # Create clock_login_attempts table for server-side brute-force protection
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS clock_login_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_code TEXT NOT NULL,
                ip_address TEXT NOT NULL,
                attempt_time TIMESTAMP NOT NULL,
                success INTEGER DEFAULT 0
            )
        ''')
        
        # Create task_material_requirements table for materials per task
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS task_material_requirements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                material_id INTEGER NOT NULL,
                description TEXT,
                quantity_required REAL NOT NULL,
                quantity_reserved REAL DEFAULT 0,
                quantity_issued REAL DEFAULT 0,
                unit_of_measure TEXT,
                status TEXT DEFAULT 'Pending' CHECK(status IN ('Pending', 'Partially Issued', 'Issued', 'Received')),
                is_optional INTEGER DEFAULT 0,
                issued_date TIMESTAMP,
                last_issued_at TIMESTAMP,
                issued_by INTEGER,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_by INTEGER,
                updated_at TIMESTAMP,
                FOREIGN KEY (task_id) REFERENCES work_order_tasks(id) ON DELETE CASCADE,
                FOREIGN KEY (material_id) REFERENCES products(id),
                FOREIGN KEY (issued_by) REFERENCES users(id),
                FOREIGN KEY (created_by) REFERENCES users(id),
                FOREIGN KEY (updated_by) REFERENCES users(id)
            )
        ''')
        
        # Create indexes for task_material_requirements
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_task_materials_task_status 
            ON task_material_requirements(task_id, status)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_task_materials_material 
            ON task_material_requirements(material_id)
        ''')
        
        # Create task_required_skills table for skillsets per task
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS task_required_skills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                skillset_id INTEGER NOT NULL,
                skill_level TEXT NOT NULL CHECK(length(skill_level) > 0),
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (task_id) REFERENCES work_order_tasks(id) ON DELETE CASCADE,
                FOREIGN KEY (skillset_id) REFERENCES skillsets(id),
                FOREIGN KEY (created_by) REFERENCES users(id),
                UNIQUE(task_id, skillset_id)
            )
        ''')
        
        # Create indexes for task_required_skills
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_task_skills_task 
            ON task_required_skills(task_id)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_task_skills_skillset 
            ON task_required_skills(skillset_id)
        ''')
        
        # Create uom_master table (UOM Master)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS uom_master (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uom_code TEXT UNIQUE NOT NULL,
                uom_name TEXT NOT NULL,
                uom_type TEXT,
                conversion_factor REAL DEFAULT 1.0,
                base_uom_id INTEGER,
                rounding_precision INTEGER DEFAULT 2,
                is_active INTEGER DEFAULT 1,
                description TEXT,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                modified_by INTEGER,
                modified_at TIMESTAMP,
                FOREIGN KEY (base_uom_id) REFERENCES uom_master(id),
                FOREIGN KEY (created_by) REFERENCES users(id),
                FOREIGN KEY (modified_by) REFERENCES users(id)
            )
        ''')
        
        # Create product_uom_conversions table (Part-UOM associations with versioning)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS product_uom_conversions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                uom_id INTEGER NOT NULL,
                conversion_factor REAL NOT NULL DEFAULT 1.0,
                is_base_uom INTEGER DEFAULT 0,
                is_purchase_uom INTEGER DEFAULT 0,
                is_issue_uom INTEGER DEFAULT 0,
                version_number INTEGER DEFAULT 1,
                effective_date DATE,
                is_active INTEGER DEFAULT 1,
                version_notes TEXT,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (uom_id) REFERENCES uom_master(id),
                FOREIGN KEY (created_by) REFERENCES users(id)
            )
        ''')
        
        # Create chart_of_accounts table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chart_of_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_code TEXT UNIQUE NOT NULL,
                account_name TEXT NOT NULL,
                account_type TEXT NOT NULL,
                parent_account_id INTEGER,
                is_active INTEGER DEFAULT 1,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (parent_account_id) REFERENCES chart_of_accounts(id)
            )
        ''')
        
        # Create fiscal_periods table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fiscal_periods (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period_name TEXT NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                fiscal_year INTEGER NOT NULL,
                status TEXT DEFAULT 'Open',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create gl_entries table (journal entries header)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gl_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_number TEXT UNIQUE NOT NULL,
                entry_date DATE NOT NULL,
                description TEXT NOT NULL,
                transaction_source TEXT NOT NULL,
                reference_type TEXT,
                reference_id INTEGER,
                status TEXT DEFAULT 'Draft',
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                posted_by INTEGER,
                posted_at TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES users(id),
                FOREIGN KEY (posted_by) REFERENCES users(id)
            )
        ''')
        
        # Create gl_entry_lines table (journal entry detail lines)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gl_entry_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                gl_entry_id INTEGER NOT NULL,
                account_id INTEGER NOT NULL,
                debit REAL DEFAULT 0,
                credit REAL DEFAULT 0,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (gl_entry_id) REFERENCES gl_entries(id) ON DELETE CASCADE,
                FOREIGN KEY (account_id) REFERENCES chart_of_accounts(id)
            )
        ''')
        
        # Create vendor_invoices table (AP)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vendor_invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_number TEXT UNIQUE NOT NULL,
                vendor_id INTEGER NOT NULL,
                po_id INTEGER,
                invoice_date DATE NOT NULL,
                due_date DATE NOT NULL,
                amount REAL NOT NULL,
                tax_amount REAL DEFAULT 0,
                total_amount REAL NOT NULL,
                amount_paid REAL DEFAULT 0,
                status TEXT DEFAULT 'Open',
                gl_entry_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (vendor_id) REFERENCES suppliers(id),
                FOREIGN KEY (po_id) REFERENCES purchase_orders(id),
                FOREIGN KEY (gl_entry_id) REFERENCES gl_entries(id)
            )
        ''')
        
        # Create customer_invoices table (AR)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customer_invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_number TEXT UNIQUE NOT NULL,
                customer_name TEXT NOT NULL,
                customer_email TEXT,
                wo_id INTEGER,
                invoice_date DATE NOT NULL,
                due_date DATE NOT NULL,
                amount REAL NOT NULL,
                tax_amount REAL DEFAULT 0,
                total_amount REAL NOT NULL,
                amount_paid REAL DEFAULT 0,
                status TEXT DEFAULT 'Open',
                gl_entry_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (wo_id) REFERENCES work_orders(id),
                FOREIGN KEY (gl_entry_id) REFERENCES gl_entries(id)
            )
        ''')
        
        # Create comprehensive invoices table for Invoice Management Module
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_number TEXT UNIQUE NOT NULL,
                invoice_type TEXT NOT NULL,
                customer_id INTEGER NOT NULL,
                so_id INTEGER,
                wo_id INTEGER,
                invoice_date DATE NOT NULL,
                due_date DATE NOT NULL,
                payment_terms INTEGER DEFAULT 30,
                status TEXT DEFAULT 'Draft',
                subtotal REAL DEFAULT 0,
                tax_rate REAL DEFAULT 0,
                tax_amount REAL DEFAULT 0,
                discount_amount REAL DEFAULT 0,
                total_amount REAL NOT NULL,
                amount_paid REAL DEFAULT 0,
                balance_due REAL DEFAULT 0,
                notes TEXT,
                terms_conditions TEXT,
                gl_entry_id INTEGER,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                approved_by INTEGER,
                approved_at TIMESTAMP,
                posted_by INTEGER,
                posted_at TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers(id),
                FOREIGN KEY (so_id) REFERENCES sales_orders(id),
                FOREIGN KEY (wo_id) REFERENCES work_orders(id),
                FOREIGN KEY (gl_entry_id) REFERENCES gl_entries(id),
                FOREIGN KEY (created_by) REFERENCES users(id),
                FOREIGN KEY (approved_by) REFERENCES users(id),
                FOREIGN KEY (posted_by) REFERENCES users(id)
            )
        ''')
        
        # Create invoice_lines table for line item details
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS invoice_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_id INTEGER NOT NULL,
                line_number INTEGER NOT NULL,
                product_id INTEGER,
                description TEXT NOT NULL,
                quantity REAL NOT NULL,
                unit_price REAL NOT NULL,
                discount_percent REAL DEFAULT 0,
                tax_rate REAL DEFAULT 0,
                line_total REAL NOT NULL,
                reference_type TEXT,
                reference_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (invoice_id) REFERENCES invoices(id) ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        ''')
        
        # Create payments table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payment_number TEXT UNIQUE NOT NULL,
                payment_date DATE NOT NULL,
                payment_type TEXT NOT NULL,
                reference_type TEXT NOT NULL,
                reference_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                payment_method TEXT NOT NULL,
                check_number TEXT,
                remarks TEXT,
                gl_entry_id INTEGER,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (gl_entry_id) REFERENCES gl_entries(id),
                FOREIGN KEY (created_by) REFERENCES users(id)
            )
        ''')
        
        # Create customers table for sales module
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_number TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                contact_person TEXT,
                email TEXT,
                phone TEXT,
                billing_address TEXT,
                shipping_address TEXT,
                payment_terms INTEGER DEFAULT 30,
                credit_limit REAL DEFAULT 0,
                tax_exempt INTEGER DEFAULT 0,
                notes TEXT,
                status TEXT DEFAULT 'Active',
                portal_token TEXT UNIQUE,
                portal_enabled INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customer_contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER NOT NULL,
                contact_name TEXT NOT NULL,
                title TEXT,
                email TEXT,
                phone TEXT,
                mobile TEXT,
                department TEXT,
                is_primary INTEGER DEFAULT 0,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
            )
        ''')
        
        # Create sales_orders table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sales_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                so_number TEXT UNIQUE NOT NULL,
                customer_id INTEGER NOT NULL,
                sales_type TEXT NOT NULL,
                order_date DATE NOT NULL,
                expected_ship_date DATE,
                actual_ship_date DATE,
                status TEXT DEFAULT 'Draft',
                subtotal REAL DEFAULT 0,
                discount_amount REAL DEFAULT 0,
                tax_amount REAL DEFAULT 0,
                total_amount REAL DEFAULT 0,
                amount_paid REAL DEFAULT 0,
                balance_due REAL DEFAULT 0,
                shipping_method TEXT,
                tracking_number TEXT,
                notes TEXT,
                core_charge REAL DEFAULT 0,
                repair_charge REAL DEFAULT 0,
                expected_return_date DATE,
                service_notes TEXT,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers(id),
                FOREIGN KEY (created_by) REFERENCES users(id)
            )
        ''')
        
        # Create sales_order_lines table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sales_order_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                so_id INTEGER NOT NULL,
                line_number INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                description TEXT,
                quantity REAL NOT NULL,
                unit_price REAL NOT NULL,
                discount_percent REAL DEFAULT 0,
                line_total REAL NOT NULL,
                is_core INTEGER DEFAULT 0,
                is_replacement INTEGER DEFAULT 0,
                serial_number TEXT,
                expected_return_date DATE,
                returned INTEGER DEFAULT 0,
                returned_date DATE,
                line_notes TEXT,
                attachment_path TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (so_id) REFERENCES sales_orders(id),
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        ''')
        
        # Create audit_trail table for change tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS audit_trail (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                record_type TEXT NOT NULL,
                record_id TEXT NOT NULL,
                action_type TEXT NOT NULL,
                modified_by INTEGER NOT NULL,
                modified_by_name TEXT,
                modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                changed_fields TEXT,
                ip_address TEXT,
                user_agent TEXT,
                FOREIGN KEY (modified_by) REFERENCES users(id)
            )
        ''')
        
        # Create index for faster audit trail queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_audit_record 
            ON audit_trail(record_type, record_id, modified_at DESC)
        ''')
        
        # Create core_due_tracking table for exchange transactions
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS core_due_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                so_line_id INTEGER NOT NULL,
                so_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                core_charge REAL NOT NULL,
                expected_condition TEXT,
                core_due_date DATE,
                core_received INTEGER DEFAULT 0,
                core_received_date DATE,
                actual_condition TEXT,
                core_disposition TEXT,
                stock_disposition TEXT,
                refund_issued INTEGER DEFAULT 0,
                refund_amount REAL,
                refund_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (so_line_id) REFERENCES sales_order_lines(id),
                FOREIGN KEY (so_id) REFERENCES sales_orders(id),
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        ''')
        
        # Create Service Work Orders tables for service management
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS service_work_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                swo_number TEXT UNIQUE NOT NULL,
                service_type TEXT NOT NULL,
                customer_id INTEGER,
                customer_name TEXT,
                equipment_description TEXT,
                equipment_serial TEXT,
                equipment_model TEXT,
                priority TEXT DEFAULT 'Medium',
                status TEXT DEFAULT 'Open',
                due_date DATE,
                assigned_to INTEGER,
                location TEXT,
                description TEXT,
                service_notes TEXT,
                labor_subtotal REAL DEFAULT 0,
                materials_subtotal REAL DEFAULT 0,
                expenses_subtotal REAL DEFAULT 0,
                total_cost REAL DEFAULT 0,
                invoiced INTEGER DEFAULT 0,
                invoice_id INTEGER,
                approved_by INTEGER,
                approved_at TIMESTAMP,
                completed_at TIMESTAMP,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                modified_by INTEGER,
                modified_at TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers(id),
                FOREIGN KEY (assigned_to) REFERENCES labor_resources(id),
                FOREIGN KEY (invoice_id) REFERENCES invoices(id)
            )
        ''')
        
        # Create service work order labor tracking table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS service_wo_labor (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                swo_id INTEGER NOT NULL,
                employee_id INTEGER NOT NULL,
                labor_type TEXT NOT NULL,
                hours_worked REAL NOT NULL,
                hourly_rate REAL NOT NULL,
                labor_cost REAL NOT NULL,
                work_date DATE,
                description TEXT,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (swo_id) REFERENCES service_work_orders(id) ON DELETE CASCADE,
                FOREIGN KEY (employee_id) REFERENCES labor_resources(id)
            )
        ''')
        
        # Create service work order materials tracking table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS service_wo_materials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                swo_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity REAL NOT NULL,
                unit_price REAL NOT NULL,
                total_cost REAL NOT NULL,
                allocated_from_inventory INTEGER DEFAULT 0,
                description TEXT,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (swo_id) REFERENCES service_work_orders(id) ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        ''')
        
        # Create service work order expenses tracking table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS service_wo_expenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                swo_id INTEGER NOT NULL,
                expense_type TEXT NOT NULL,
                description TEXT,
                amount REAL NOT NULL,
                vendor_name TEXT,
                receipt_reference TEXT,
                expense_date DATE,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (swo_id) REFERENCES service_work_orders(id) ON DELETE CASCADE
            )
        ''')
        
        # Create FAA Form 8130-3 certificates table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS faa_8130_certificates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                certificate_number TEXT UNIQUE NOT NULL,
                work_order_id INTEGER NOT NULL,
                issue_date DATE NOT NULL,
                
                -- Block 1: Approving Civil Aviation Authority
                issuing_authority TEXT DEFAULT 'FAA',
                
                -- Block 2: Authorized Release Certificate
                form_tracking_number TEXT,
                
                -- Block 4: Organization Name and Address
                organization_name TEXT,
                organization_address TEXT,
                
                -- Block 5: Work Order/Contract/Invoice
                work_order_reference TEXT,
                
                -- Block 6: Item (part details)
                part_name TEXT,
                part_number TEXT,
                part_description TEXT,
                
                -- Block 7: Quantity
                quantity INTEGER,
                
                -- Block 8: Serial/Batch Number
                serial_number TEXT,
                batch_number TEXT,
                
                -- Block 9: Status/Work
                status_work TEXT,
                
                -- Block 11: Approval/Authorization Number
                approval_number TEXT,
                
                -- Block 12: Remarks
                remarks TEXT,
                
                -- Block 13: Certifying Staff
                certifier_name TEXT,
                certifier_certificate_number TEXT,
                certifier_signature_date DATE,
                
                -- Block 14: Authorized Signature
                authorized_signature_name TEXT,
                authorized_signature_number TEXT,
                authorized_signature_date DATE,
                
                -- Block 19: Receiving Organization (optional)
                receiving_organization TEXT,
                receiving_address TEXT,
                
                -- File storage
                pdf_file_path TEXT,
                pdf_file_hash TEXT,
                
                -- Metadata
                status TEXT DEFAULT 'Issued',
                voided_at TIMESTAMP,
                voided_by INTEGER,
                void_reason TEXT,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (work_order_id) REFERENCES work_orders(id),
                FOREIGN KEY (created_by) REFERENCES users(id),
                FOREIGN KEY (voided_by) REFERENCES users(id)
            )
        ''')
        
        # Create MRO capabilities tracking table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mro_capabilities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                capability_code TEXT UNIQUE NOT NULL,
                part_number TEXT NOT NULL,
                product_id INTEGER,
                capability_name TEXT NOT NULL,
                applicability TEXT,
                part_class TEXT,
                description TEXT,
                category TEXT,
                manufacturer TEXT,
                tolerance TEXT,
                compliance TEXT,
                certification_required INTEGER DEFAULT 0,
                status TEXT DEFAULT 'Active',
                notes TEXT,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                modified_by INTEGER,
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (created_by) REFERENCES users(id),
                FOREIGN KEY (modified_by) REFERENCES users(id)
            )
        ''')
        
        # Create capability specifications table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS capability_specifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                capability_id INTEGER NOT NULL,
                spec_name TEXT NOT NULL,
                spec_value TEXT,
                spec_type TEXT,
                unit_of_measure TEXT,
                min_value REAL,
                max_value REAL,
                is_critical INTEGER DEFAULT 0,
                display_order INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                modified_by INTEGER,
                FOREIGN KEY (capability_id) REFERENCES mro_capabilities(id) ON DELETE CASCADE,
                FOREIGN KEY (modified_by) REFERENCES users(id),
                UNIQUE(capability_id, spec_name)
            )
        ''')
        
        # Create airline fleet sources table (for tracking uploaded data sources)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS airline_fleet_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT NOT NULL,
                source_type TEXT DEFAULT 'CSV Upload',
                file_name TEXT,
                uploaded_by INTEGER,
                upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                record_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'Active',
                notes TEXT,
                FOREIGN KEY (uploaded_by) REFERENCES users(id)
            )
        ''')
        
        # Create airline fleet aircraft table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS airline_fleet_aircraft (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER NOT NULL,
                airline_name TEXT NOT NULL,
                region TEXT,
                tail_number TEXT,
                aircraft_model TEXT NOT NULL,
                aircraft_variant TEXT,
                config_date TEXT,
                status TEXT DEFAULT 'Active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (source_id) REFERENCES airline_fleet_sources(id) ON DELETE CASCADE
            )
        ''')
        
        # Create airline fleet parts table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS airline_fleet_parts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                aircraft_id INTEGER NOT NULL,
                ata_chapter TEXT,
                part_number TEXT NOT NULL,
                description TEXT,
                quantity_in_service INTEGER DEFAULT 1,
                criticality TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (aircraft_id) REFERENCES airline_fleet_aircraft(id) ON DELETE CASCADE
            )
        ''')
        
        # Create capability matches table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS capability_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fleet_part_id INTEGER NOT NULL,
                capability_id INTEGER,
                match_score TEXT,
                score_breakdown TEXT,
                match_reason TEXT,
                recommended_action TEXT,
                analyst_notes TEXT,
                is_active INTEGER DEFAULT 1,
                is_latest INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (fleet_part_id) REFERENCES airline_fleet_parts(id) ON DELETE CASCADE,
                FOREIGN KEY (capability_id) REFERENCES mro_capabilities(id)
            )
        ''')
        
        # Create match runs table (for tracking analysis runs)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS match_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER,
                triggered_by INTEGER,
                run_type TEXT DEFAULT 'adhoc',
                status TEXT DEFAULT 'Pending',
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                metrics TEXT,
                notes TEXT,
                FOREIGN KEY (source_id) REFERENCES airline_fleet_sources(id),
                FOREIGN KEY (triggered_by) REFERENCES users(id)
            )
        ''')
        
        # Create indexes for market analysis tables
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_fleet_parts_part_number ON airline_fleet_parts(part_number)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_fleet_aircraft_airline ON airline_fleet_aircraft(airline_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_fleet_aircraft_region ON airline_fleet_aircraft(region)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_capability_matches_score ON capability_matches(match_score)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_capability_matches_active ON capability_matches(is_active, is_latest)')
        
        # Create supplier discovery requests table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS supplier_discovery_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_number TEXT UNIQUE NOT NULL,
                product_id INTEGER,
                part_number TEXT NOT NULL,
                description TEXT,
                specifications TEXT,
                quantity REAL,
                uom TEXT,
                need_by_date DATE,
                urgency TEXT DEFAULT 'Normal',
                plant_location TEXT,
                industry TEXT,
                preferred_regions TEXT,
                status TEXT DEFAULT 'Pending',
                ai_search_queries TEXT,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (created_by) REFERENCES users(id)
            )
        ''')
        
        # Create discovered suppliers table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS discovered_suppliers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER NOT NULL,
                supplier_name TEXT NOT NULL,
                website TEXT,
                material_match TEXT,
                certifications TEXT,
                region TEXT,
                estimated_lead_time TEXT,
                confidence_score INTEGER DEFAULT 0,
                notes TEXT,
                approval_status TEXT DEFAULT 'Unapproved',
                approved_by INTEGER,
                approved_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (request_id) REFERENCES supplier_discovery_requests(id) ON DELETE CASCADE,
                FOREIGN KEY (approved_by) REFERENCES users(id)
            )
        ''')
        
        # Create indexes for supplier discovery tables
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_supplier_discovery_status ON supplier_discovery_requests(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_supplier_discovery_part ON supplier_discovery_requests(part_number)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_discovered_suppliers_request ON discovered_suppliers(request_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_discovered_suppliers_score ON discovered_suppliers(confidence_score)')
        
        # Capacity Planning Module Tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS work_centers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                default_hours_per_day REAL DEFAULT 8.0,
                default_days_per_week INTEGER DEFAULT 5,
                efficiency_factor REAL DEFAULT 1.0,
                cost_per_hour REAL DEFAULT 0,
                status TEXT DEFAULT 'Active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS work_center_resources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                work_center_id INTEGER NOT NULL,
                labor_resource_id INTEGER NOT NULL,
                effective_start_date DATE,
                effective_end_date DATE,
                utilization_percent REAL DEFAULT 100.0,
                is_primary INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (work_center_id) REFERENCES work_centers(id) ON DELETE CASCADE,
                FOREIGN KEY (labor_resource_id) REFERENCES labor_resources(id),
                UNIQUE(work_center_id, labor_resource_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS work_center_capacity (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                work_center_id INTEGER NOT NULL,
                capacity_date DATE NOT NULL,
                available_hours REAL NOT NULL,
                override_reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (work_center_id) REFERENCES work_centers(id) ON DELETE CASCADE,
                UNIQUE(work_center_id, capacity_date)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS work_order_operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                work_order_id INTEGER NOT NULL,
                operation_seq INTEGER DEFAULT 10,
                work_center_id INTEGER,
                operation_name TEXT NOT NULL,
                planned_hours REAL NOT NULL DEFAULT 0,
                setup_hours REAL DEFAULT 0,
                planned_start_date DATE,
                planned_end_date DATE,
                actual_start_date DATE,
                actual_end_date DATE,
                actual_hours REAL DEFAULT 0,
                status TEXT DEFAULT 'Pending',
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP,
                FOREIGN KEY (work_order_id) REFERENCES work_orders(id) ON DELETE CASCADE,
                FOREIGN KEY (work_center_id) REFERENCES work_centers(id)
            )
        ''')
        
        # Create indexes for capacity planning tables
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_work_center_status ON work_centers(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_work_center_resources_wc ON work_center_resources(work_center_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_work_center_resources_lr ON work_center_resources(labor_resource_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_work_center_capacity_date ON work_center_capacity(capacity_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_work_order_operations_wo ON work_order_operations(work_order_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_work_order_operations_wc ON work_order_operations(work_center_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_work_order_operations_status ON work_order_operations(status)')
        
        # Tools Management Module Tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tools (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tool_number TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                category TEXT,
                manufacturer TEXT,
                model_number TEXT,
                serial_number TEXT,
                location TEXT,
                status TEXT DEFAULT 'Available',
                condition TEXT DEFAULT 'Good',
                purchase_date DATE,
                purchase_cost REAL DEFAULT 0,
                last_calibration_date DATE,
                next_calibration_date DATE,
                calibration_interval_days INTEGER,
                assigned_to INTEGER,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP,
                FOREIGN KEY (assigned_to) REFERENCES labor_resources(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tool_checkouts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tool_id INTEGER NOT NULL,
                checked_out_by INTEGER NOT NULL,
                work_order_id INTEGER,
                checkout_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expected_return_date DATE,
                return_date TIMESTAMP,
                condition_on_checkout TEXT,
                condition_on_return TEXT,
                notes TEXT,
                FOREIGN KEY (tool_id) REFERENCES tools(id) ON DELETE CASCADE,
                FOREIGN KEY (checked_out_by) REFERENCES labor_resources(id),
                FOREIGN KEY (work_order_id) REFERENCES work_orders(id)
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tools_status ON tools(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tools_category ON tools(category)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tool_checkouts_tool ON tool_checkouts(tool_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tool_checkouts_return ON tool_checkouts(return_date)')
        
        # RFQ (Request for Quotation) Module Tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rfqs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rfq_number TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                status TEXT DEFAULT 'Draft',
                issue_date DATE,
                due_date DATE,
                currency TEXT DEFAULT 'USD',
                terms_conditions TEXT,
                notes TEXT,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rfq_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rfq_id INTEGER NOT NULL,
                line_number INTEGER NOT NULL,
                product_id INTEGER,
                description TEXT NOT NULL,
                quantity REAL NOT NULL DEFAULT 1,
                uom_id INTEGER,
                target_price REAL,
                required_date DATE,
                notes TEXT,
                FOREIGN KEY (rfq_id) REFERENCES rfqs(id) ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (uom_id) REFERENCES uom_master(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rfq_suppliers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rfq_id INTEGER NOT NULL,
                supplier_id INTEGER NOT NULL,
                sent_date TIMESTAMP,
                response_date TIMESTAMP,
                response_status TEXT DEFAULT 'Pending',
                notes TEXT,
                FOREIGN KEY (rfq_id) REFERENCES rfqs(id) ON DELETE CASCADE,
                FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rfq_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rfq_id INTEGER NOT NULL,
                rfq_line_id INTEGER NOT NULL,
                supplier_id INTEGER NOT NULL,
                quoted_price REAL NOT NULL,
                quoted_quantity REAL,
                lead_time_days INTEGER,
                valid_until DATE,
                notes TEXT,
                is_selected INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (rfq_id) REFERENCES rfqs(id) ON DELETE CASCADE,
                FOREIGN KEY (rfq_line_id) REFERENCES rfq_lines(id) ON DELETE CASCADE,
                FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rfqs_status ON rfqs(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rfq_lines_rfq ON rfq_lines(rfq_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rfq_suppliers_rfq ON rfq_suppliers(rfq_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rfq_quotes_rfq ON rfq_quotes(rfq_id)')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rfq_supplier_tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rfq_id INTEGER NOT NULL,
                supplier_id INTEGER NOT NULL,
                token TEXT UNIQUE NOT NULL,
                expires_at TIMESTAMP NOT NULL,
                is_used INTEGER DEFAULT 0,
                allow_multiple_submissions INTEGER DEFAULT 0,
                submission_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_accessed_at TIMESTAMP,
                FOREIGN KEY (rfq_id) REFERENCES rfqs(id) ON DELETE CASCADE,
                FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rfq_supplier_responses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rfq_id INTEGER NOT NULL,
                supplier_id INTEGER NOT NULL,
                token_id INTEGER,
                status TEXT DEFAULT 'Submitted',
                submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                valid_until DATE,
                notes TEXT,
                total_amount REAL DEFAULT 0,
                ip_address TEXT,
                user_agent TEXT,
                FOREIGN KEY (rfq_id) REFERENCES rfqs(id) ON DELETE CASCADE,
                FOREIGN KEY (supplier_id) REFERENCES suppliers(id),
                FOREIGN KEY (token_id) REFERENCES rfq_supplier_tokens(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rfq_response_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                response_id INTEGER NOT NULL,
                rfq_line_id INTEGER NOT NULL,
                unit_price REAL NOT NULL,
                lead_time_days INTEGER NOT NULL,
                notes TEXT,
                FOREIGN KEY (response_id) REFERENCES rfq_supplier_responses(id) ON DELETE CASCADE,
                FOREIGN KEY (rfq_line_id) REFERENCES rfq_lines(id) ON DELETE CASCADE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rfq_response_attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                response_id INTEGER NOT NULL,
                file_name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_type TEXT,
                file_size INTEGER,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (response_id) REFERENCES rfq_supplier_responses(id) ON DELETE CASCADE
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rfq_tokens_token ON rfq_supplier_tokens(token)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rfq_tokens_rfq ON rfq_supplier_tokens(rfq_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_rfq_responses_rfq ON rfq_supplier_responses(rfq_id)')
        
        # Organizational Analyzer tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS org_kpi_definitions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kpi_code TEXT UNIQUE NOT NULL,
                kpi_name TEXT NOT NULL,
                category TEXT NOT NULL,
                description TEXT,
                calculation_method TEXT,
                target_value REAL,
                warning_threshold REAL,
                critical_threshold REAL,
                unit TEXT,
                is_active INTEGER DEFAULT 1,
                display_order INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS org_kpi_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kpi_id INTEGER NOT NULL,
                recorded_date DATE NOT NULL,
                value REAL NOT NULL,
                trend TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (kpi_id) REFERENCES org_kpi_definitions(id) ON DELETE CASCADE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS org_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                title TEXT NOT NULL,
                message TEXT,
                kpi_id INTEGER,
                current_value REAL,
                threshold_value REAL,
                is_acknowledged INTEGER DEFAULT 0,
                acknowledged_by INTEGER,
                acknowledged_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (kpi_id) REFERENCES org_kpi_definitions(id),
                FOREIGN KEY (acknowledged_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS org_recommendations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recommendation_type TEXT NOT NULL,
                priority TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                business_impact TEXT,
                risk_level TEXT,
                time_to_value TEXT,
                confidence_score REAL,
                status TEXT DEFAULT 'Pending',
                generated_by TEXT,
                reviewed_by INTEGER,
                reviewed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (reviewed_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS org_forecasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                forecast_type TEXT NOT NULL,
                scenario TEXT NOT NULL,
                horizon_days INTEGER NOT NULL,
                forecast_date DATE NOT NULL,
                metric_name TEXT NOT NULL,
                predicted_value REAL,
                lower_bound REAL,
                upper_bound REAL,
                confidence_level REAL,
                assumptions TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS org_health_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                score_date DATE NOT NULL,
                overall_score REAL NOT NULL,
                financial_score REAL,
                operational_score REAL,
                workforce_score REAL,
                strategic_score REAL,
                customer_score REAL,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_org_kpi_history_date ON org_kpi_history(recorded_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_org_alerts_severity ON org_alerts(severity, is_acknowledged)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_org_forecasts_type ON org_forecasts(forecast_type, scenario)')
        
        # NDT Module Tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ndt_technicians (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                technician_number TEXT UNIQUE NOT NULL,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                employer TEXT,
                contract_status TEXT DEFAULT 'Active',
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ndt_certifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                technician_id INTEGER NOT NULL,
                method TEXT NOT NULL,
                level TEXT NOT NULL,
                certification_number TEXT,
                issued_date DATE,
                expiration_date DATE NOT NULL,
                issuing_body TEXT,
                document_path TEXT,
                status TEXT DEFAULT 'Active',
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (technician_id) REFERENCES ndt_technicians(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ndt_work_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ndt_wo_number TEXT UNIQUE NOT NULL,
                order_type TEXT DEFAULT 'Standalone',
                customer_id INTEGER,
                sales_order_id INTEGER,
                work_order_id INTEGER,
                product_id INTEGER,
                serial_number TEXT,
                heat_number TEXT,
                part_description TEXT,
                ndt_methods TEXT NOT NULL,
                applicable_code TEXT,
                acceptance_criteria TEXT,
                inspection_location TEXT,
                priority TEXT DEFAULT 'Normal',
                status TEXT DEFAULT 'Draft',
                planned_start_date DATE,
                planned_end_date DATE,
                actual_start_date DATE,
                actual_end_date DATE,
                assigned_technician_id INTEGER,
                reviewer_id INTEGER,
                notes TEXT,
                rejection_reason TEXT,
                disposition TEXT,
                rework_wo_id INTEGER,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers(id),
                FOREIGN KEY (sales_order_id) REFERENCES sales_orders(id),
                FOREIGN KEY (work_order_id) REFERENCES work_orders(id),
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (assigned_technician_id) REFERENCES ndt_technicians(id),
                FOREIGN KEY (reviewer_id) REFERENCES ndt_technicians(id),
                FOREIGN KEY (created_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ndt_inspection_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ndt_wo_id INTEGER NOT NULL,
                method TEXT NOT NULL,
                inspection_date DATE NOT NULL,
                technician_id INTEGER NOT NULL,
                equipment_used TEXT,
                calibration_reference TEXT,
                procedure_reference TEXT,
                area_inspected TEXT,
                defect_type TEXT,
                defect_size TEXT,
                defect_location TEXT,
                indication_details TEXT,
                result TEXT NOT NULL,
                remarks TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ndt_wo_id) REFERENCES ndt_work_orders(id),
                FOREIGN KEY (technician_id) REFERENCES ndt_technicians(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ndt_attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ndt_wo_id INTEGER NOT NULL,
                result_id INTEGER,
                attachment_type TEXT NOT NULL,
                file_name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_size INTEGER,
                description TEXT,
                uploaded_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ndt_wo_id) REFERENCES ndt_work_orders(id),
                FOREIGN KEY (result_id) REFERENCES ndt_inspection_results(id),
                FOREIGN KEY (uploaded_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ndt_status_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ndt_wo_id INTEGER NOT NULL,
                old_status TEXT,
                new_status TEXT NOT NULL,
                changed_by INTEGER NOT NULL,
                change_reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ndt_wo_id) REFERENCES ndt_work_orders(id),
                FOREIGN KEY (changed_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ndt_certifications_expiry ON ndt_certifications(expiration_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ndt_wo_status ON ndt_work_orders(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ndt_wo_customer ON ndt_work_orders(customer_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ndt_results_wo ON ndt_inspection_results(ndt_wo_id)')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ndt_invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_number TEXT UNIQUE NOT NULL,
                ndt_wo_id INTEGER,
                customer_id INTEGER,
                invoice_date DATE NOT NULL,
                due_date DATE NOT NULL,
                payment_terms INTEGER DEFAULT 30,
                status TEXT DEFAULT 'Draft',
                ndt_methods TEXT,
                part_description TEXT,
                serial_number TEXT,
                inspection_type TEXT,
                subtotal REAL DEFAULT 0,
                tax_rate REAL DEFAULT 0,
                tax_amount REAL DEFAULT 0,
                discount_amount REAL DEFAULT 0,
                total_amount REAL NOT NULL,
                amount_paid REAL DEFAULT 0,
                balance_due REAL DEFAULT 0,
                notes TEXT,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                approved_by INTEGER,
                approved_at TIMESTAMP,
                FOREIGN KEY (ndt_wo_id) REFERENCES ndt_work_orders(id),
                FOREIGN KEY (customer_id) REFERENCES customers(id),
                FOREIGN KEY (created_by) REFERENCES users(id),
                FOREIGN KEY (approved_by) REFERENCES users(id)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ndt_invoices_status ON ndt_invoices(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ndt_invoices_customer ON ndt_invoices(customer_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ndt_invoices_ndt_wo ON ndt_invoices(ndt_wo_id)')
        
        # Migrate sales_orders table - add exchange_type column
        self._migrate_sales_orders_exchange_type(cursor)
        
        # Migrate sales_order_lines table - add new columns if they don't exist
        self._migrate_sales_order_lines(cursor)
        
        # Migrate service_wo_materials table - add serial_number column if it doesn't exist
        self._migrate_service_wo_materials(cursor)
        
        # Migrate mro_capabilities table - add applicability and part_class columns if they don't exist
        self._migrate_mro_capabilities(cursor)
        
        # Migrate work_orders table - add so_id and customer_id columns for sales order linking
        self._migrate_work_orders_so_link(cursor)
        
        # Migrate customers table - add portal columns
        self._migrate_customers_portal(cursor)
        
        # Migrate work_orders table - add stage_id column for work order stages
        self._migrate_work_orders_stages(cursor)
        
        # Migrate purchase_orders table - add work_order_id for linking to work orders
        self._migrate_purchase_orders_wo_link(cursor)
        
        self._migrate_rfq_enhancements(cursor)
        
        # Purchase Order Service Lines - for miscellaneous charges and services linked to work orders
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS purchase_order_service_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                po_id INTEGER NOT NULL,
                work_order_id INTEGER,
                line_number INTEGER NOT NULL,
                service_category TEXT NOT NULL,
                description TEXT NOT NULL,
                quantity REAL DEFAULT 1,
                unit_of_measure TEXT DEFAULT 'EA',
                unit_cost REAL NOT NULL,
                total_cost REAL NOT NULL,
                tax_rate REAL DEFAULT 0,
                status TEXT DEFAULT 'Pending',
                received_date DATE,
                received_by INTEGER,
                invoice_id INTEGER,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (po_id) REFERENCES purchase_orders(id) ON DELETE CASCADE,
                FOREIGN KEY (work_order_id) REFERENCES work_orders(id),
                FOREIGN KEY (received_by) REFERENCES users(id)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_po_service_lines_po ON purchase_order_service_lines(po_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_po_service_lines_wo ON purchase_order_service_lines(work_order_id)')
        
        # AI Super Master Scheduler tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS master_schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_number TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                schedule_type TEXT DEFAULT 'MPS',
                horizon_start DATE NOT NULL,
                horizon_end DATE NOT NULL,
                time_bucket TEXT DEFAULT 'Daily',
                status TEXT DEFAULT 'Draft',
                created_by INTEGER,
                approved_by INTEGER,
                approved_at TIMESTAMP,
                is_active INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES users(id),
                FOREIGN KEY (approved_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS master_schedule_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER NOT NULL,
                order_type TEXT NOT NULL,
                order_id INTEGER NOT NULL,
                order_number TEXT NOT NULL,
                product_id INTEGER,
                product_code TEXT,
                product_name TEXT,
                quantity REAL NOT NULL,
                scheduled_start DATE NOT NULL,
                scheduled_end DATE NOT NULL,
                original_due_date DATE,
                priority INTEGER DEFAULT 50,
                priority_class TEXT DEFAULT 'Normal',
                work_center_id INTEGER,
                assigned_hours REAL DEFAULT 0,
                sequence_number INTEGER DEFAULT 0,
                status TEXT DEFAULT 'Scheduled',
                is_locked INTEGER DEFAULT 0,
                lock_reason TEXT,
                atp_date DATE,
                ctp_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (schedule_id) REFERENCES master_schedules(id) ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (work_center_id) REFERENCES work_centers(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS schedule_exceptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER,
                exception_type TEXT NOT NULL,
                severity TEXT DEFAULT 'Warning',
                order_type TEXT,
                order_id INTEGER,
                order_number TEXT,
                work_center_id INTEGER,
                exception_date DATE,
                title TEXT NOT NULL,
                description TEXT,
                impact_assessment TEXT,
                days_late INTEGER,
                capacity_gap REAL,
                material_shortage TEXT,
                is_resolved INTEGER DEFAULT 0,
                resolved_by INTEGER,
                resolved_at TIMESTAMP,
                resolution_notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (schedule_id) REFERENCES master_schedules(id) ON DELETE CASCADE,
                FOREIGN KEY (work_center_id) REFERENCES work_centers(id),
                FOREIGN KEY (resolved_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS schedule_recommendations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER,
                exception_id INTEGER,
                recommendation_type TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                action_required TEXT,
                impacted_orders TEXT,
                cost_impact REAL,
                time_impact_days REAL,
                risk_level TEXT DEFAULT 'Medium',
                priority_score INTEGER DEFAULT 50,
                ai_confidence REAL,
                ai_reasoning TEXT,
                status TEXT DEFAULT 'Pending',
                reviewed_by INTEGER,
                reviewed_at TIMESTAMP,
                decision TEXT,
                decision_notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (schedule_id) REFERENCES master_schedules(id) ON DELETE CASCADE,
                FOREIGN KEY (exception_id) REFERENCES schedule_exceptions(id),
                FOREIGN KEY (reviewed_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS schedule_scenarios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER NOT NULL,
                scenario_name TEXT NOT NULL,
                scenario_type TEXT DEFAULT 'Optimization',
                description TEXT,
                baseline_otd REAL,
                projected_otd REAL,
                baseline_utilization REAL,
                projected_utilization REAL,
                orders_affected INTEGER,
                overtime_hours REAL,
                cost_delta REAL,
                risk_score REAL,
                ai_analysis TEXT,
                scenario_data TEXT,
                is_selected INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (schedule_id) REFERENCES master_schedules(id) ON DELETE CASCADE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS schedule_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER NOT NULL,
                schedule_item_id INTEGER,
                override_type TEXT NOT NULL,
                original_value TEXT,
                new_value TEXT,
                justification TEXT NOT NULL,
                risk_acknowledged INTEGER DEFAULT 0,
                risk_description TEXT,
                overridden_by INTEGER NOT NULL,
                approved_by INTEGER,
                approved_at TIMESTAMP,
                status TEXT DEFAULT 'Pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (schedule_id) REFERENCES master_schedules(id) ON DELETE CASCADE,
                FOREIGN KEY (schedule_item_id) REFERENCES master_schedule_items(id),
                FOREIGN KEY (overridden_by) REFERENCES users(id),
                FOREIGN KEY (approved_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS schedule_capacity_load (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER NOT NULL,
                work_center_id INTEGER NOT NULL,
                load_date DATE NOT NULL,
                available_hours REAL DEFAULT 0,
                planned_hours REAL DEFAULT 0,
                overtime_hours REAL DEFAULT 0,
                utilization_pct REAL DEFAULT 0,
                status TEXT DEFAULT 'Normal',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (schedule_id) REFERENCES master_schedules(id) ON DELETE CASCADE,
                FOREIGN KEY (work_center_id) REFERENCES work_centers(id),
                UNIQUE(schedule_id, work_center_id, load_date)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS schedule_material_requirements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER NOT NULL,
                schedule_item_id INTEGER,
                product_id INTEGER NOT NULL,
                product_code TEXT,
                product_name TEXT,
                required_qty REAL NOT NULL,
                available_qty REAL DEFAULT 0,
                shortage_qty REAL DEFAULT 0,
                required_date DATE NOT NULL,
                po_id INTEGER,
                po_expected_date DATE,
                status TEXT DEFAULT 'Open',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (schedule_id) REFERENCES master_schedules(id) ON DELETE CASCADE,
                FOREIGN KEY (schedule_item_id) REFERENCES master_schedule_items(id),
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (po_id) REFERENCES purchase_orders(id)
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_msi_schedule ON master_schedule_items(schedule_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_msi_order ON master_schedule_items(order_type, order_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_msi_dates ON master_schedule_items(scheduled_start, scheduled_end)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_se_schedule ON schedule_exceptions(schedule_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sr_schedule ON schedule_recommendations(schedule_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_scl_schedule_wc ON schedule_capacity_load(schedule_id, work_center_id)')
        
        # Salesforce Data Migration Agent tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sf_connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_name TEXT NOT NULL,
                instance_url TEXT NOT NULL,
                client_id TEXT NOT NULL,
                client_secret_encrypted TEXT,
                access_token_encrypted TEXT,
                refresh_token_encrypted TEXT,
                token_expiry TIMESTAMP,
                api_version TEXT DEFAULT 'v59.0',
                sandbox INTEGER DEFAULT 0,
                status TEXT DEFAULT 'Disconnected',
                last_connected_at TIMESTAMP,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sf_object_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                connection_id INTEGER NOT NULL,
                object_name TEXT NOT NULL,
                object_label TEXT,
                object_type TEXT DEFAULT 'Standard',
                is_custom INTEGER DEFAULT 0,
                is_queryable INTEGER DEFAULT 1,
                record_count INTEGER DEFAULT 0,
                key_prefix TEXT,
                erp_table_name TEXT,
                erp_table_exists INTEGER DEFAULT 0,
                migration_priority INTEGER DEFAULT 100,
                migration_status TEXT DEFAULT 'Pending',
                discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (connection_id) REFERENCES sf_connections(id) ON DELETE CASCADE,
                UNIQUE(connection_id, object_name)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sf_field_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                object_metadata_id INTEGER NOT NULL,
                field_name TEXT NOT NULL,
                field_label TEXT,
                field_type TEXT NOT NULL,
                sf_data_type TEXT,
                length INTEGER,
                precision_val INTEGER,
                scale INTEGER,
                is_required INTEGER DEFAULT 0,
                is_unique INTEGER DEFAULT 0,
                is_reference INTEGER DEFAULT 0,
                reference_to TEXT,
                picklist_values TEXT,
                erp_column_name TEXT,
                erp_column_type TEXT,
                transformation_rule TEXT,
                mapping_status TEXT DEFAULT 'Auto',
                FOREIGN KEY (object_metadata_id) REFERENCES sf_object_metadata(id) ON DELETE CASCADE,
                UNIQUE(object_metadata_id, field_name)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sf_migrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration_name TEXT NOT NULL,
                connection_id INTEGER NOT NULL,
                migration_type TEXT DEFAULT 'Full',
                status TEXT DEFAULT 'Draft',
                total_objects INTEGER DEFAULT 0,
                completed_objects INTEGER DEFAULT 0,
                total_records INTEGER DEFAULT 0,
                migrated_records INTEGER DEFAULT 0,
                failed_records INTEGER DEFAULT 0,
                validation_status TEXT DEFAULT 'Pending',
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                approved_by INTEGER,
                approved_at TIMESTAMP,
                FOREIGN KEY (connection_id) REFERENCES sf_connections(id),
                FOREIGN KEY (created_by) REFERENCES users(id),
                FOREIGN KEY (approved_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sf_migration_objects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration_id INTEGER NOT NULL,
                object_metadata_id INTEGER NOT NULL,
                sequence_order INTEGER DEFAULT 0,
                source_count INTEGER DEFAULT 0,
                target_count INTEGER DEFAULT 0,
                inserted_count INTEGER DEFAULT 0,
                updated_count INTEGER DEFAULT 0,
                skipped_count INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'Pending',
                schema_created INTEGER DEFAULT 0,
                schema_approved INTEGER DEFAULT 0,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                error_message TEXT,
                FOREIGN KEY (migration_id) REFERENCES sf_migrations(id) ON DELETE CASCADE,
                FOREIGN KEY (object_metadata_id) REFERENCES sf_object_metadata(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sf_migration_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration_object_id INTEGER NOT NULL,
                batch_number INTEGER NOT NULL,
                batch_size INTEGER NOT NULL,
                offset_val INTEGER DEFAULT 0,
                records_processed INTEGER DEFAULT 0,
                records_success INTEGER DEFAULT 0,
                records_failed INTEGER DEFAULT 0,
                status TEXT DEFAULT 'Pending',
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                error_message TEXT,
                FOREIGN KEY (migration_object_id) REFERENCES sf_migration_objects(id) ON DELETE CASCADE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sf_migration_errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration_id INTEGER NOT NULL,
                migration_object_id INTEGER,
                batch_id INTEGER,
                sf_record_id TEXT,
                error_type TEXT NOT NULL,
                error_code TEXT,
                error_message TEXT NOT NULL,
                field_name TEXT,
                record_data TEXT,
                resolution_status TEXT DEFAULT 'Open',
                resolution_notes TEXT,
                resolved_by INTEGER,
                resolved_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (migration_id) REFERENCES sf_migrations(id) ON DELETE CASCADE,
                FOREIGN KEY (migration_object_id) REFERENCES sf_migration_objects(id),
                FOREIGN KEY (batch_id) REFERENCES sf_migration_batches(id),
                FOREIGN KEY (resolved_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sf_reconciliation_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration_id INTEGER NOT NULL,
                migration_object_id INTEGER,
                object_name TEXT NOT NULL,
                sf_record_count INTEGER DEFAULT 0,
                erp_record_count INTEGER DEFAULT 0,
                count_match INTEGER DEFAULT 0,
                checksum_sf TEXT,
                checksum_erp TEXT,
                checksum_match INTEGER DEFAULT 0,
                sample_verified INTEGER DEFAULT 0,
                discrepancy_count INTEGER DEFAULT 0,
                discrepancy_details TEXT,
                reconciliation_status TEXT DEFAULT 'Pending',
                validated_at TIMESTAMP,
                validated_by INTEGER,
                FOREIGN KEY (migration_id) REFERENCES sf_migrations(id) ON DELETE CASCADE,
                FOREIGN KEY (migration_object_id) REFERENCES sf_migration_objects(id),
                FOREIGN KEY (validated_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sf_audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration_id INTEGER,
                connection_id INTEGER,
                event_type TEXT NOT NULL,
                event_category TEXT NOT NULL,
                event_description TEXT NOT NULL,
                object_name TEXT,
                record_count INTEGER,
                user_id INTEGER,
                ip_address TEXT,
                event_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (migration_id) REFERENCES sf_migrations(id),
                FOREIGN KEY (connection_id) REFERENCES sf_connections(id),
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sf_id_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration_id INTEGER NOT NULL,
                object_name TEXT NOT NULL,
                sf_id TEXT NOT NULL,
                erp_id INTEGER NOT NULL,
                erp_table TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (migration_id) REFERENCES sf_migrations(id) ON DELETE CASCADE,
                UNIQUE(migration_id, object_name, sf_id)
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sf_obj_meta_conn ON sf_object_metadata(connection_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sf_field_meta_obj ON sf_field_metadata(object_metadata_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sf_mig_obj ON sf_migration_objects(migration_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sf_mig_batch ON sf_migration_batches(migration_object_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sf_mig_err ON sf_migration_errors(migration_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sf_recon ON sf_reconciliation_results(migration_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sf_audit ON sf_audit_events(migration_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sf_id_map ON sf_id_mappings(migration_id, object_name)')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS it_security_incidents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                incident_number TEXT UNIQUE NOT NULL,
                incident_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                status TEXT DEFAULT 'Open',
                title TEXT NOT NULL,
                description TEXT,
                affected_system TEXT,
                affected_user_id INTEGER,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                detected_by TEXT,
                containment_status TEXT,
                eradication_status TEXT,
                recovery_status TEXT,
                root_cause TEXT,
                remediation_actions TEXT,
                lessons_learned TEXT,
                assigned_to INTEGER,
                resolved_at TIMESTAMP,
                closed_at TIMESTAMP,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (affected_user_id) REFERENCES users(id),
                FOREIGN KEY (assigned_to) REFERENCES users(id),
                FOREIGN KEY (created_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS it_access_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                action_type TEXT NOT NULL,
                resource_type TEXT,
                resource_id TEXT,
                ip_address TEXT,
                user_agent TEXT,
                session_id TEXT,
                success INTEGER DEFAULT 1,
                failure_reason TEXT,
                risk_score INTEGER DEFAULT 0,
                risk_factors TEXT,
                geo_location TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS it_compliance_assessments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                framework TEXT NOT NULL,
                assessment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                overall_score REAL DEFAULT 0,
                status TEXT DEFAULT 'In Progress',
                findings_count INTEGER DEFAULT 0,
                critical_findings INTEGER DEFAULT 0,
                high_findings INTEGER DEFAULT 0,
                medium_findings INTEGER DEFAULT 0,
                low_findings INTEGER DEFAULT 0,
                controls_assessed INTEGER DEFAULT 0,
                controls_passed INTEGER DEFAULT 0,
                controls_failed INTEGER DEFAULT 0,
                next_assessment_date TIMESTAMP,
                assessor TEXT,
                notes TEXT,
                evidence_links TEXT,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS it_ai_agent_monitoring (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_name TEXT NOT NULL,
                agent_type TEXT,
                status TEXT DEFAULT 'Active',
                trust_score REAL DEFAULT 100,
                total_actions INTEGER DEFAULT 0,
                approved_actions INTEGER DEFAULT 0,
                blocked_actions INTEGER DEFAULT 0,
                escalated_actions INTEGER DEFAULT 0,
                last_action_at TIMESTAMP,
                last_action_type TEXT,
                scope_violations INTEGER DEFAULT 0,
                risk_level TEXT DEFAULT 'Low',
                throttle_status TEXT DEFAULT 'Normal',
                suspension_reason TEXT,
                suspended_at TIMESTAMP,
                suspended_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (suspended_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS it_security_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                status TEXT DEFAULT 'Active',
                title TEXT NOT NULL,
                description TEXT,
                source TEXT,
                affected_resource TEXT,
                affected_user_id INTEGER,
                risk_score INTEGER DEFAULT 0,
                detection_method TEXT,
                recommended_action TEXT,
                auto_remediated INTEGER DEFAULT 0,
                remediation_action TEXT,
                acknowledged_by INTEGER,
                acknowledged_at TIMESTAMP,
                resolved_by INTEGER,
                resolved_at TIMESTAMP,
                false_positive INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (affected_user_id) REFERENCES users(id),
                FOREIGN KEY (acknowledged_by) REFERENCES users(id),
                FOREIGN KEY (resolved_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS it_user_risk_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                overall_risk_score INTEGER DEFAULT 0,
                access_risk INTEGER DEFAULT 0,
                behavior_risk INTEGER DEFAULT 0,
                compliance_risk INTEGER DEFAULT 0,
                dormant_account INTEGER DEFAULT 0,
                excessive_privileges INTEGER DEFAULT 0,
                sod_conflicts INTEGER DEFAULT 0,
                failed_logins_24h INTEGER DEFAULT 0,
                unusual_activity_count INTEGER DEFAULT 0,
                last_login TIMESTAMP,
                last_activity TIMESTAMP,
                risk_factors TEXT,
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id),
                UNIQUE(user_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS it_compliance_controls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                framework TEXT NOT NULL,
                control_id TEXT NOT NULL,
                control_name TEXT NOT NULL,
                control_description TEXT,
                control_category TEXT,
                status TEXT DEFAULT 'Not Assessed',
                implementation_status TEXT,
                evidence TEXT,
                last_tested TIMESTAMP,
                next_test_due TIMESTAMP,
                owner_id INTEGER,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (owner_id) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS it_change_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                change_number TEXT UNIQUE NOT NULL,
                change_type TEXT NOT NULL,
                status TEXT DEFAULT 'Pending',
                priority TEXT DEFAULT 'Medium',
                title TEXT NOT NULL,
                description TEXT,
                business_justification TEXT,
                affected_systems TEXT,
                risk_assessment TEXT,
                risk_score INTEGER DEFAULT 0,
                impact_analysis TEXT,
                rollback_plan TEXT,
                requested_by INTEGER,
                approved_by INTEGER,
                implemented_by INTEGER,
                requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                scheduled_at TIMESTAMP,
                implemented_at TIMESTAMP,
                completed_at TIMESTAMP,
                FOREIGN KEY (requested_by) REFERENCES users(id),
                FOREIGN KEY (approved_by) REFERENCES users(id),
                FOREIGN KEY (implemented_by) REFERENCES users(id)
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_it_incidents_status ON it_security_incidents(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_it_access_user ON it_access_audit(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_it_access_time ON it_access_audit(created_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_it_alerts_status ON it_security_alerts(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_it_user_risk ON it_user_risk_scores(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_it_compliance ON it_compliance_assessments(framework)')
        
        # Initialize QMS tables
        init_qms_tables(cursor)
        
        conn.commit()
        conn.close()
    
    def _migrate_sales_orders_exchange_type(self, cursor):
        """Add exchange_type column to sales_orders table"""
        cursor.execute("PRAGMA table_info(sales_orders)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        
        if 'exchange_type' not in existing_columns:
            try:
                cursor.execute("ALTER TABLE sales_orders ADD COLUMN exchange_type TEXT")
            except sqlite3.OperationalError:
                pass
    
    def _migrate_customers_portal(self, cursor):
        """Add portal columns to customers table"""
        cursor.execute("PRAGMA table_info(customers)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        
        if 'portal_token' not in existing_columns:
            try:
                cursor.execute("ALTER TABLE customers ADD COLUMN portal_token TEXT UNIQUE")
            except sqlite3.OperationalError:
                pass
        
        if 'portal_enabled' not in existing_columns:
            try:
                cursor.execute("ALTER TABLE customers ADD COLUMN portal_enabled INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass
    
    def _migrate_sales_order_lines(self, cursor):
        """Add new columns to sales_order_lines table for enhanced functionality"""
        
        # Get existing columns
        cursor.execute("PRAGMA table_info(sales_order_lines)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        
        # Define new columns to add
        new_columns = {
            'line_type': "ALTER TABLE sales_order_lines ADD COLUMN line_type TEXT DEFAULT 'Outright'",
            'line_status': "ALTER TABLE sales_order_lines ADD COLUMN line_status TEXT DEFAULT 'Draft'",
            # Exchange-specific fields
            'core_charge': "ALTER TABLE sales_order_lines ADD COLUMN core_charge REAL DEFAULT 0",
            'core_due_days': "ALTER TABLE sales_order_lines ADD COLUMN core_due_days INTEGER",
            'expected_core_condition': "ALTER TABLE sales_order_lines ADD COLUMN expected_core_condition TEXT",
            'core_disposition': "ALTER TABLE sales_order_lines ADD COLUMN core_disposition TEXT",
            'stock_disposition': "ALTER TABLE sales_order_lines ADD COLUMN stock_disposition TEXT",
            # Managed Repair fields
            'quoted_tat': "ALTER TABLE sales_order_lines ADD COLUMN quoted_tat INTEGER",
            'repair_nte': "ALTER TABLE sales_order_lines ADD COLUMN repair_nte REAL",
            'vendor_repair_source': "ALTER TABLE sales_order_lines ADD COLUMN vendor_repair_source TEXT",
            'repair_status': "ALTER TABLE sales_order_lines ADD COLUMN repair_status TEXT",
            'return_to_address': "ALTER TABLE sales_order_lines ADD COLUMN return_to_address TEXT",
            # Inventory allocation fields
            'allocated_quantity': "ALTER TABLE sales_order_lines ADD COLUMN allocated_quantity REAL DEFAULT 0",
            'allocation_status': "ALTER TABLE sales_order_lines ADD COLUMN allocation_status TEXT DEFAULT 'Pending'",
            'allocation_notes': "ALTER TABLE sales_order_lines ADD COLUMN allocation_notes TEXT",
            'inventory_id': "ALTER TABLE sales_order_lines ADD COLUMN inventory_id INTEGER REFERENCES inventory(id)",
            'released_to_shipping_at': "ALTER TABLE sales_order_lines ADD COLUMN released_to_shipping_at TIMESTAMP",
            'released_by': "ALTER TABLE sales_order_lines ADD COLUMN released_by INTEGER REFERENCES users(id)",
            'shipped_quantity': "ALTER TABLE sales_order_lines ADD COLUMN shipped_quantity REAL DEFAULT 0",
            # Audit trail fields
            'created_by': "ALTER TABLE sales_order_lines ADD COLUMN created_by INTEGER",
            'modified_by': "ALTER TABLE sales_order_lines ADD COLUMN modified_by INTEGER",
            'modified_at': "ALTER TABLE sales_order_lines ADD COLUMN modified_at TIMESTAMP"
        }
        
        # Add missing columns
        for column_name, alter_sql in new_columns.items():
            if column_name not in existing_columns:
                cursor.execute(alter_sql)
    
    def _migrate_service_wo_materials(self, cursor):
        """Add serial_number column to service_wo_materials table if it doesn't exist"""
        
        # Get existing columns
        cursor.execute("PRAGMA table_info(service_wo_materials)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        
        # Add serial_number column if it doesn't exist
        if 'serial_number' not in existing_columns:
            cursor.execute("ALTER TABLE service_wo_materials ADD COLUMN serial_number TEXT")
    
    def _migrate_mro_capabilities(self, cursor):
        """Add applicability and part_class columns to mro_capabilities table if they don't exist"""
        
        # Get existing columns
        cursor.execute("PRAGMA table_info(mro_capabilities)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        
        # Add applicability column if it doesn't exist
        if 'applicability' not in existing_columns:
            cursor.execute("ALTER TABLE mro_capabilities ADD COLUMN applicability TEXT")
        
        # Add part_class column if it doesn't exist
        if 'part_class' not in existing_columns:
            cursor.execute("ALTER TABLE mro_capabilities ADD COLUMN part_class TEXT")
    
    def _migrate_work_orders_so_link(self, cursor):
        """Add so_id column to work_orders table to link to sales orders"""
        
        # Get existing columns
        cursor.execute("PRAGMA table_info(work_orders)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        
        # Add so_id column if it doesn't exist
        if 'so_id' not in existing_columns:
            cursor.execute("ALTER TABLE work_orders ADD COLUMN so_id INTEGER REFERENCES sales_orders(id)")
        
        # Add customer_id column if it doesn't exist
        if 'customer_id' not in existing_columns:
            cursor.execute("ALTER TABLE work_orders ADD COLUMN customer_id INTEGER REFERENCES customers(id)")
        
        # Refresh columns list after potential additions
        cursor.execute("PRAGMA table_info(work_orders)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        
        # Add notes column if it doesn't exist
        if 'notes' not in existing_columns:
            cursor.execute("ALTER TABLE work_orders ADD COLUMN notes TEXT")
        
        # Add disposition column if it doesn't exist
        if 'disposition' not in existing_columns:
            cursor.execute("ALTER TABLE work_orders ADD COLUMN disposition TEXT")
        
        # Add created_by column if it doesn't exist
        if 'created_by' not in existing_columns:
            cursor.execute("ALTER TABLE work_orders ADD COLUMN created_by INTEGER REFERENCES users(id)")
        
        # Add inventory_id column if it doesn't exist (to reference created inventory)
        if 'inventory_id' not in existing_columns:
            cursor.execute("ALTER TABLE work_orders ADD COLUMN inventory_id INTEGER REFERENCES inventory(id)")
    
    def _migrate_purchase_orders_wo_link(self, cursor):
        """Add work_order_id column to purchase_orders table for linking to work orders"""
        
        # Get existing columns
        cursor.execute("PRAGMA table_info(purchase_orders)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        
        # Add work_order_id column if it doesn't exist
        if 'work_order_id' not in existing_columns:
            cursor.execute("ALTER TABLE purchase_orders ADD COLUMN work_order_id INTEGER REFERENCES work_orders(id)")
        
        # Add po_type column if it doesn't exist (to distinguish service/misc POs)
        if 'po_type' not in existing_columns:
            cursor.execute("ALTER TABLE purchase_orders ADD COLUMN po_type TEXT DEFAULT 'Material'")
        
        # Exchange PO fields for Dual Exchange workflow
        if 'is_exchange' not in existing_columns:
            cursor.execute("ALTER TABLE purchase_orders ADD COLUMN is_exchange INTEGER DEFAULT 0")
        
        if 'exchange_owner_type' not in existing_columns:
            cursor.execute("ALTER TABLE purchase_orders ADD COLUMN exchange_owner_type TEXT")
        
        if 'exchange_owner_id' not in existing_columns:
            cursor.execute("ALTER TABLE purchase_orders ADD COLUMN exchange_owner_id INTEGER")
        
        if 'exchange_reference_id' not in existing_columns:
            cursor.execute("ALTER TABLE purchase_orders ADD COLUMN exchange_reference_id TEXT")
        
        if 'source_sales_order_id' not in existing_columns:
            cursor.execute("ALTER TABLE purchase_orders ADD COLUMN source_sales_order_id INTEGER REFERENCES sales_orders(id)")
        
        if 'exchange_status' not in existing_columns:
            cursor.execute("ALTER TABLE purchase_orders ADD COLUMN exchange_status TEXT")
    
    def _migrate_work_orders_stages(self, cursor):
        """Add stage_id column to work_orders table for tracking work order stages"""
        
        # Get existing columns
        cursor.execute("PRAGMA table_info(work_orders)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        
        # Add stage_id column if it doesn't exist
        if 'stage_id' not in existing_columns:
            cursor.execute("ALTER TABLE work_orders ADD COLUMN stage_id INTEGER REFERENCES work_order_stages(id)")
        
        # Seed default stages if none exist
        cursor.execute("SELECT COUNT(*) FROM work_order_stages")
        if cursor.fetchone()[0] == 0:
            default_stages = [
                ('Received', 'Work order received and logged', '#17a2b8', 1),
                ('Inspection', 'Initial inspection in progress', '#ffc107', 2),
                ('Awaiting Parts', 'Waiting for parts/materials', '#fd7e14', 3),
                ('In Work', 'Active work in progress', '#007bff', 4),
                ('Quality Check', 'Quality assurance review', '#6f42c1', 5),
                ('Ready to Ship', 'Completed and ready for shipping', '#28a745', 6)
            ]
            for name, desc, color, seq in default_stages:
                cursor.execute('''
                    INSERT INTO work_order_stages (name, description, color, sequence, is_active)
                    VALUES (?, ?, ?, ?, 1)
                ''', (name, desc, color, seq))
    
    def _migrate_rfq_enhancements(self, cursor):
        """Add buyer contact and condition columns to RFQ tables for supplier portal"""
        
        cursor.execute("PRAGMA table_info(rfqs)")
        rfq_columns = {row[1] for row in cursor.fetchall()}
        
        rfq_new_columns = [
            ('buyer_name', 'TEXT'),
            ('buyer_email', 'TEXT'),
            ('buyer_phone', 'TEXT')
        ]
        
        for col_name, col_type in rfq_new_columns:
            if col_name not in rfq_columns:
                try:
                    cursor.execute(f'ALTER TABLE rfqs ADD COLUMN {col_name} {col_type}')
                except:
                    pass
        
        cursor.execute("PRAGMA table_info(rfq_lines)")
        line_columns = {row[1] for row in cursor.fetchall()}
        
        line_new_columns = [
            ('required_condition', 'TEXT DEFAULT "New"'),
            ('target_delivery_date', 'DATE')
        ]
        
        for col_name, col_type in line_new_columns:
            if col_name not in line_columns:
                try:
                    cursor.execute(f'ALTER TABLE rfq_lines ADD COLUMN {col_name} {col_type}')
                except:
                    pass
    
    def seed_chart_of_accounts(self):
        conn = self.get_connection()
        
        existing = conn.execute('SELECT COUNT(*) as count FROM chart_of_accounts').fetchone()
        if existing['count'] > 0:
            conn.close()
            return
        
        # Insert parent accounts first
        parent_accounts = [
            ('1000', 'Assets', 'Asset'),
            ('2000', 'Liabilities', 'Liability'),
            ('3000', 'Equity', 'Equity'),
            ('4000', 'Revenue', 'Revenue'),
            ('5000', 'Cost of Goods Sold', 'Expense'),
            ('6000', 'Operating Expenses', 'Expense')
        ]
        
        account_map = {}
        for code, name, acc_type in parent_accounts:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO chart_of_accounts (account_code, account_name, account_type, parent_account_id, is_active)
                VALUES (?, ?, ?, NULL, 1)
            ''', (code, name, acc_type))
            account_map[code] = cursor.lastrowid
        
        # Insert child accounts with proper parent references
        child_accounts = [
            ('1100', 'Current Assets', 'Asset', '1000'),
            ('1110', 'Cash', 'Asset', '1100'),
            ('1120', 'Accounts Receivable', 'Asset', '1100'),
            ('1130', 'Inventory', 'Asset', '1100'),
            ('1140', 'WIP - Work in Process', 'Asset', '1100'),
            ('1200', 'Fixed Assets', 'Asset', '1000'),
            ('1210', 'Equipment', 'Asset', '1200'),
            ('1220', 'Accumulated Depreciation', 'Asset', '1200'),
            
            ('2100', 'Current Liabilities', 'Liability', '2000'),
            ('2110', 'Accounts Payable', 'Liability', '2100'),
            ('2120', 'Accrued Expenses', 'Liability', '2100'),
            ('2130', 'Tax Payable', 'Liability', '2100'),
            ('2200', 'Long-term Liabilities', 'Liability', '2000'),
            ('2210', 'Notes Payable', 'Liability', '2200'),
            
            ('3100', 'Owner\'s Equity', 'Equity', '3000'),
            ('3200', 'Retained Earnings', 'Equity', '3000'),
            
            ('4100', 'Sales Revenue', 'Revenue', '4000'),
            ('4200', 'Service Revenue', 'Revenue', '4000'),
            ('4300', 'Other Income', 'Revenue', '4000'),
            
            ('5100', 'Material Cost', 'Expense', '5000'),
            ('5200', 'Direct Labor', 'Expense', '5000'),
            ('5300', 'Manufacturing Overhead', 'Expense', '5000'),
            
            ('6100', 'Salaries & Wages', 'Expense', '6000'),
            ('6200', 'Rent Expense', 'Expense', '6000'),
            ('6300', 'Utilities Expense', 'Expense', '6000'),
            ('6400', 'Depreciation Expense', 'Expense', '6000'),
            ('6500', 'Administrative Expenses', 'Expense', '6000')
        ]
        
        for code, name, acc_type, parent_code in child_accounts:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO chart_of_accounts (account_code, account_name, account_type, parent_account_id, is_active)
                VALUES (?, ?, ?, ?, 1)
            ''', (code, name, acc_type, account_map.get(parent_code)))
            account_map[code] = cursor.lastrowid
        
        conn.commit()
        conn.close()
    
    def seed_unit_of_measure(self):
        conn = self.get_connection()
        
        existing = conn.execute('SELECT COUNT(*) as count FROM uom_master').fetchone()
        if existing['count'] > 0:
            conn.close()
            return
        
        # Insert base UOMs (standalone units with conversion factor 1.0)
        base_uoms = [
            ('EA', 'Each', 'Count', 1.0, None, 0, 1, 'Basic counting unit'),
            ('KG', 'Kilogram', 'Weight', 1.0, None, 3, 1, 'Base weight unit (metric)'),
            ('LB', 'Pound', 'Weight', 1.0, None, 3, 1, 'Base weight unit (imperial)'),
            ('LTR', 'Liter', 'Volume', 1.0, None, 3, 1, 'Base volume unit (metric)'),
            ('GAL', 'Gallon', 'Volume', 1.0, None, 3, 1, 'Base volume unit (imperial)'),
            ('M', 'Meter', 'Length', 1.0, None, 3, 1, 'Base length unit (metric)'),
            ('FT', 'Foot', 'Length', 1.0, None, 3, 1, 'Base length unit (imperial)'),
            ('HR', 'Hour', 'Time', 1.0, None, 2, 1, 'Base time unit'),
        ]
        
        uom_map = {}
        for code, name, uom_type, conv_factor, base_id, precision, is_active, desc in base_uoms:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO uom_master (uom_code, uom_name, uom_type, conversion_factor, base_uom_id, rounding_precision, is_active, description)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (code, name, uom_type, conv_factor, base_id, precision, is_active, desc))
            uom_map[code] = cursor.lastrowid
        
        # Insert derived UOMs with conversion factors
        derived_uoms = [
            # Count-based
            ('PCS', 'Pieces', 'Count', 1.0, 'EA', 0, 1, 'Same as Each'),
            ('BOX', 'Box', 'Count', 1.0, 'EA', 0, 1, 'Container unit (define conversion per product)'),
            ('CASE', 'Case', 'Count', 1.0, 'EA', 0, 1, 'Larger container unit'),
            ('PALLET', 'Pallet', 'Count', 1.0, 'EA', 0, 1, 'Pallet unit'),
            ('DOZEN', 'Dozen', 'Count', 12.0, 'EA', 0, 1, '12 pieces'),
            
            # Weight-based (metric)
            ('G', 'Gram', 'Weight', 0.001, 'KG', 3, 1, '1/1000 of a kilogram'),
            ('MT', 'Metric Ton', 'Weight', 1000.0, 'KG', 3, 1, '1000 kilograms'),
            
            # Weight-based (imperial)
            ('OZ', 'Ounce', 'Weight', 0.0625, 'LB', 3, 1, '1/16 of a pound'),
            ('TON', 'Ton', 'Weight', 2000.0, 'LB', 3, 1, '2000 pounds'),
            
            # Volume-based (metric)
            ('ML', 'Milliliter', 'Volume', 0.001, 'LTR', 3, 1, '1/1000 of a liter'),
            
            # Volume-based (imperial)
            ('QT', 'Quart', 'Volume', 0.25, 'GAL', 3, 1, '1/4 of a gallon'),
            ('PT', 'Pint', 'Volume', 0.125, 'GAL', 3, 1, '1/8 of a gallon'),
            ('FLOZ', 'Fluid Ounce', 'Volume', 0.0078125, 'GAL', 4, 1, '1/128 of a gallon'),
            
            # Length-based (metric)
            ('CM', 'Centimeter', 'Length', 0.01, 'M', 3, 1, '1/100 of a meter'),
            ('MM', 'Millimeter', 'Length', 0.001, 'M', 3, 1, '1/1000 of a meter'),
            ('KM', 'Kilometer', 'Length', 1000.0, 'M', 3, 1, '1000 meters'),
            
            # Length-based (imperial)
            ('IN', 'Inch', 'Length', 0.0833333, 'FT', 4, 1, '1/12 of a foot'),
            ('YD', 'Yard', 'Length', 3.0, 'FT', 3, 1, '3 feet'),
            ('MI', 'Mile', 'Length', 5280.0, 'FT', 3, 1, '5280 feet'),
            
            # Time-based
            ('MIN', 'Minute', 'Time', 0.0166667, 'HR', 4, 1, '1/60 of an hour'),
            ('DAY', 'Day', 'Time', 24.0, 'HR', 2, 1, '24 hours'),
        ]
        
        for code, name, uom_type, conv_factor, base_code, precision, is_active, desc in derived_uoms:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO uom_master (uom_code, uom_name, uom_type, conversion_factor, base_uom_id, rounding_precision, is_active, description)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (code, name, uom_type, conv_factor, uom_map.get(base_code), precision, is_active, desc))
        
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
    def get_by_email(email):
        db = Database()
        conn = db.get_connection()
        user = conn.execute('SELECT * FROM users WHERE email = ?', (email,)).fetchone()
        conn.close()
        return user
    
    @staticmethod
    def verify_password(user, password):
        return check_password_hash(user['password_hash'], password)
    
    @staticmethod
    def get_all():
        db = Database()
        conn = db.get_connection()
        users = conn.execute('SELECT id, username, email, role, last_login, created_at FROM users ORDER BY created_at DESC').fetchall()
        conn.close()
        return users
    
    @staticmethod
    def update_last_login(user_id):
        db = Database()
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = ?', (user_id,))
        conn.commit()
        conn.close()
    
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

class CompanySettings:
    @staticmethod
    def get():
        db = Database()
        conn = db.get_connection()
        settings = conn.execute('SELECT * FROM company_settings WHERE id = 1').fetchone()
        conn.close()
        return settings
    
    @staticmethod
    def create_or_update(data, user_id):
        db = Database()
        conn = db.get_connection()
        cursor = conn.cursor()
        
        existing = conn.execute('SELECT * FROM company_settings WHERE id = 1').fetchone()
        
        if existing:
            cursor.execute('''
                UPDATE company_settings SET
                    company_name = ?, dba = ?, address_line1 = ?, address_line2 = ?,
                    city = ?, state = ?, postal_code = ?, country = ?,
                    phone = ?, email = ?, website = ?, tax_id = ?,
                    duns_number = ?, cage_code = ?, logo_filename = ?, auto_post_invoice_gl = ?,
                    marketing_tagline = ?, brand_primary_color = ?, brand_secondary_color = ?,
                    brand_accent_color = ?, brand_tone = ?, marketing_description = ?,
                    target_industries = ?, key_differentiators = ?,
                    updated_by = ?, last_updated = CURRENT_TIMESTAMP
                WHERE id = 1
            ''', (
                data.get('company_name'), data.get('dba'), data.get('address_line1'),
                data.get('address_line2'), data.get('city'), data.get('state'),
                data.get('postal_code'), data.get('country'), data.get('phone'),
                data.get('email'), data.get('website'), data.get('tax_id'),
                data.get('duns_number'), data.get('cage_code'), data.get('logo_filename'),
                data.get('auto_post_invoice_gl', 0),
                data.get('marketing_tagline'), data.get('brand_primary_color', '#1e40af'),
                data.get('brand_secondary_color', '#f97316'), data.get('brand_accent_color', '#10b981'),
                data.get('brand_tone', 'Enterprise'), data.get('marketing_description'),
                data.get('target_industries'), data.get('key_differentiators'),
                user_id
            ))
        else:
            cursor.execute('''
                INSERT INTO company_settings (
                    id, company_name, dba, address_line1, address_line2,
                    city, state, postal_code, country, phone, email, website,
                    tax_id, duns_number, cage_code, logo_filename, auto_post_invoice_gl,
                    marketing_tagline, brand_primary_color, brand_secondary_color,
                    brand_accent_color, brand_tone, marketing_description,
                    target_industries, key_differentiators, updated_by
                ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data.get('company_name'), data.get('dba'), data.get('address_line1'),
                data.get('address_line2'), data.get('city'), data.get('state'),
                data.get('postal_code'), data.get('country'), data.get('phone'),
                data.get('email'), data.get('website'), data.get('tax_id'),
                data.get('duns_number'), data.get('cage_code'), data.get('logo_filename'),
                data.get('auto_post_invoice_gl', 0),
                data.get('marketing_tagline'), data.get('brand_primary_color', '#1e40af'),
                data.get('brand_secondary_color', '#f97316'), data.get('brand_accent_color', '#10b981'),
                data.get('brand_tone', 'Enterprise'), data.get('marketing_description'),
                data.get('target_industries'), data.get('key_differentiators'),
                user_id
            ))
        
        conn.commit()
        conn.close()
        return True
    
    @staticmethod
    def get_or_create_default():
        settings = CompanySettings.get()
        if not settings:
            default_data = {
                'company_name': 'Your Company Name',
                'dba': '',
                'address_line1': '',
                'address_line2': '',
                'city': '',
                'state': '',
                'postal_code': '',
                'country': '',
                'phone': '',
                'email': '',
                'website': '',
                'tax_id': '',
                'duns_number': '',
                'cage_code': '',
                'logo_filename': None,
                'auto_post_invoice_gl': 0,
                'marketing_tagline': None,
                'brand_primary_color': '#1e40af',
                'brand_secondary_color': '#f97316',
                'brand_accent_color': '#10b981',
                'brand_tone': 'Enterprise',
                'marketing_description': None,
                'target_industries': None,
                'key_differentiators': None
            }
            db = Database()
            conn = db.get_connection()
            conn.execute('''
                INSERT INTO company_settings (
                    id, company_name, dba, address_line1, address_line2,
                    city, state, postal_code, country, phone, email, website,
                    tax_id, duns_number, cage_code, logo_filename, auto_post_invoice_gl,
                    marketing_tagline, brand_primary_color, brand_secondary_color,
                    brand_accent_color, brand_tone, marketing_description,
                    target_industries, key_differentiators
                ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                default_data['company_name'], default_data['dba'],
                default_data['address_line1'], default_data['address_line2'],
                default_data['city'], default_data['state'], default_data['postal_code'],
                default_data['country'], default_data['phone'], default_data['email'],
                default_data['website'], default_data['tax_id'], default_data['duns_number'],
                default_data['cage_code'], default_data['logo_filename'],
                default_data['auto_post_invoice_gl'],
                default_data['marketing_tagline'], default_data['brand_primary_color'],
                default_data['brand_secondary_color'], default_data['brand_accent_color'],
                default_data['brand_tone'], default_data['marketing_description'],
                default_data['target_industries'], default_data['key_differentiators']
            ))
            conn.commit()
            conn.close()
            settings = CompanySettings.get()
        return settings


class AuditLogger:
    """Helper class for automatic audit trail logging"""
    
    @staticmethod
    def log(conn, record_type, record_id, action_type, modified_by, changed_fields=None, ip_address=None, user_agent=None):
        """Alias for log_change for backward compatibility"""
        return AuditLogger.log_change(conn, record_type, record_id, action_type, modified_by, changed_fields, ip_address, user_agent)
    
    @staticmethod
    def log_change(conn, record_type, record_id, action_type, modified_by, changed_fields=None, ip_address=None, user_agent=None):
        """
        Log a change to the audit trail.
        
        Args:
            conn: Database connection
            record_type: Type of record (e.g., 'work_order', 'purchase_order')
            record_id: ID of the record (as string)
            action_type: 'Created', 'Updated', or 'Deleted'
            modified_by: User ID who made the change
            changed_fields: Dictionary of changed fields with old and new values
            ip_address: IP address of the user (optional)
            user_agent: User agent string (optional)
        """
        try:
            import json
            
            # Get user name
            user = conn.execute('SELECT username FROM users WHERE id = ?', (modified_by,)).fetchone()
            user_name = user['username'] if user else 'Unknown'
            
            # Convert changed_fields dict to JSON string
            changed_fields_json = json.dumps(changed_fields) if changed_fields else None
            
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO audit_trail (
                    record_type, record_id, action_type, modified_by, modified_by_name,
                    changed_fields, ip_address, user_agent
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (record_type, str(record_id), action_type, modified_by, user_name,
                  changed_fields_json, ip_address, user_agent))
            
            return cursor.lastrowid
            
        except Exception as e:
            # Log error but don't fail the transaction
            print(f"Audit logging error: {str(e)}")
            return None
    
    @staticmethod
    def get_audit_trail(conn, record_type, record_id, limit=100):
        """Get audit trail for a specific record"""
        return conn.execute('''
            SELECT * FROM audit_trail
            WHERE record_type = ? AND record_id = ?
            ORDER BY modified_at DESC
            LIMIT ?
        ''', (record_type, str(record_id), limit)).fetchall()
    
    @staticmethod
    def compare_records(old_record, new_record, exclude_fields=None):
        """
        Compare two records and return changed fields.
        
        Args:
            old_record: Dict of old values
            new_record: Dict of new values
            exclude_fields: List of fields to exclude from comparison
        
        Returns:
            Dict of changed fields with old and new values
        """
        if exclude_fields is None:
            exclude_fields = ['id', 'created_at', 'last_updated', 'modified_at']
        
        changes = {}
        
        if old_record and new_record:
            for key in new_record.keys():
                if key not in exclude_fields:
                    old_val = old_record.get(key) if old_record else None
                    new_val = new_record.get(key)
                    
                    if old_val != new_val:
                        changes[key] = {
                            'old': str(old_val) if old_val is not None else None,
                            'new': str(new_val) if new_val is not None else None
                        }
        
        return changes if changes else None


class GLAutoPost:
    """Helper class for automatic GL posting from inventory transactions"""
    
    @staticmethod
    def create_auto_journal_entry(conn, entry_date, description, transaction_source, 
                                   reference_type, reference_id, lines, created_by):
        """
        Create and automatically post a journal entry.
        
        Args:
            conn: Database connection
            entry_date: Date of the entry
            description: Entry description
            transaction_source: Source of transaction (e.g., 'Material Receiving')
            reference_type: Type of source record (e.g., 'receiving_transaction')
            reference_id: ID of source record
            lines: List of dicts with keys: account_code, debit, credit, description
            created_by: User ID creating the entry
        
        Returns:
            entry_id: ID of created journal entry, or None if failed
        """
        try:
            cursor = conn.cursor()
            
            # Generate entry number
            last_entry = conn.execute('''
                SELECT entry_number FROM gl_entries 
                WHERE entry_number LIKE 'JE-%'
                ORDER BY CAST(SUBSTR(entry_number, 4) AS INTEGER) DESC 
                LIMIT 1
            ''').fetchone()
            
            if last_entry:
                try:
                    last_number = int(last_entry['entry_number'].split('-')[1])
                    next_number = last_number + 1
                except (ValueError, IndexError):
                    next_number = 1
            else:
                next_number = 1
            
            entry_number = f'JE-{next_number:06d}'
            
            # Validate debits = credits
            total_debit = sum(line.get('debit', 0) for line in lines)
            total_credit = sum(line.get('credit', 0) for line in lines)
            
            if abs(total_debit - total_credit) > 0.01:
                raise ValueError(f'Debits ({total_debit}) must equal credits ({total_credit})')
            
            # Insert journal entry header - automatically posted
            cursor.execute('''
                INSERT INTO gl_entries (
                    entry_number, entry_date, description, transaction_source,
                    reference_type, reference_id, status, created_by, created_at,
                    posted_by, posted_at
                )
                VALUES (?, ?, ?, ?, ?, ?, 'Posted', ?, datetime('now'), ?, datetime('now'))
            ''', (entry_number, entry_date, description, transaction_source,
                  reference_type, reference_id, created_by, created_by))
            
            entry_id = cursor.lastrowid
            
            # Insert journal entry lines
            for line in lines:
                # Get account_id from account_code
                account = conn.execute('''
                    SELECT id FROM chart_of_accounts WHERE account_code = ?
                ''', (line['account_code'],)).fetchone()
                
                if not account:
                    raise ValueError(f"Account code {line['account_code']} not found")
                
                cursor.execute('''
                    INSERT INTO gl_entry_lines (
                        gl_entry_id, account_id, debit, credit, description
                    )
                    VALUES (?, ?, ?, ?, ?)
                ''', (entry_id, account['id'], line.get('debit', 0), 
                      line.get('credit', 0), line.get('description', '')))
            
            return entry_id
            
        except Exception as e:
            # Log error but don't fail the transaction
            print(f"GL Auto-posting error: {str(e)}")
            return None


def init_qms_tables(cursor):
    """Initialize Quality Management System (QMS) tables"""
    
    # SOP Categories for organizing procedures
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS qms_sop_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            parent_id INTEGER,
            sort_order INTEGER DEFAULT 0,
            status TEXT DEFAULT 'Active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (parent_id) REFERENCES qms_sop_categories(id)
        )
    ''')
    
    # Standard Operating Procedures (SOPs)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS qms_sops (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sop_number TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            category_id INTEGER,
            revision TEXT DEFAULT 'A',
            revision_date DATE,
            effective_date DATE,
            review_date DATE,
            purpose TEXT,
            scope TEXT,
            responsibilities TEXT,
            procedure_content TEXT,
            references_text TEXT,
            definitions TEXT,
            attachments TEXT,
            applicable_roles TEXT,
            applicable_modules TEXT,
            compliance_standards TEXT,
            status TEXT DEFAULT 'Draft',
            approval_status TEXT DEFAULT 'Pending',
            prepared_by INTEGER,
            reviewed_by INTEGER,
            approved_by INTEGER,
            approved_date TIMESTAMP,
            supersedes_sop_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            FOREIGN KEY (category_id) REFERENCES qms_sop_categories(id),
            FOREIGN KEY (prepared_by) REFERENCES users(id),
            FOREIGN KEY (reviewed_by) REFERENCES users(id),
            FOREIGN KEY (approved_by) REFERENCES users(id),
            FOREIGN KEY (supersedes_sop_id) REFERENCES qms_sops(id)
        )
    ''')
    
    # SOP Version History
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS qms_sop_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sop_id INTEGER NOT NULL,
            revision TEXT NOT NULL,
            revision_date DATE,
            change_summary TEXT,
            procedure_content TEXT,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (sop_id) REFERENCES qms_sops(id) ON DELETE CASCADE,
            FOREIGN KEY (created_by) REFERENCES users(id)
        )
    ''')
    
    # Work Instructions (linked to SOPs)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS qms_work_instructions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wi_number TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            sop_id INTEGER,
            revision TEXT DEFAULT 'A',
            revision_date DATE,
            effective_date DATE,
            description TEXT,
            prerequisites TEXT,
            safety_requirements TEXT,
            tools_required TEXT,
            materials_required TEXT,
            erp_module TEXT,
            erp_transaction TEXT,
            applicable_roles TEXT,
            verification_checkpoints TEXT,
            troubleshooting TEXT,
            related_transactions TEXT,
            estimated_time_minutes INTEGER,
            difficulty_level TEXT DEFAULT 'Intermediate',
            status TEXT DEFAULT 'Draft',
            approval_status TEXT DEFAULT 'Pending',
            prepared_by INTEGER,
            approved_by INTEGER,
            approved_date TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            FOREIGN KEY (sop_id) REFERENCES qms_sops(id),
            FOREIGN KEY (prepared_by) REFERENCES users(id),
            FOREIGN KEY (approved_by) REFERENCES users(id)
        )
    ''')
    
    # Work Instruction Steps
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS qms_wi_steps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            work_instruction_id INTEGER NOT NULL,
            step_number INTEGER NOT NULL,
            title TEXT NOT NULL,
            instructions TEXT,
            expected_result TEXT,
            verification_required INTEGER DEFAULT 0,
            verification_type TEXT,
            warning_text TEXT,
            caution_text TEXT,
            note_text TEXT,
            image_path TEXT,
            estimated_seconds INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (work_instruction_id) REFERENCES qms_work_instructions(id) ON DELETE CASCADE
        )
    ''')
    
    # User Acknowledgments (for SOPs and Work Instructions)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS qms_acknowledgments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            document_type TEXT NOT NULL,
            document_id INTEGER NOT NULL,
            document_revision TEXT,
            acknowledged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            acknowledgment_method TEXT DEFAULT 'Electronic',
            ip_address TEXT,
            notes TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    ''')
    
    # Compliance Records / Process Deviations
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS qms_deviations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deviation_number TEXT UNIQUE NOT NULL,
            deviation_type TEXT NOT NULL,
            severity TEXT DEFAULT 'Minor',
            sop_id INTEGER,
            work_instruction_id INTEGER,
            erp_module TEXT,
            erp_transaction_id TEXT,
            reported_by INTEGER,
            reported_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            description TEXT NOT NULL,
            root_cause TEXT,
            immediate_action TEXT,
            status TEXT DEFAULT 'Open',
            assigned_to INTEGER,
            due_date DATE,
            closed_date TIMESTAMP,
            closed_by INTEGER,
            closure_notes TEXT,
            capa_required INTEGER DEFAULT 0,
            capa_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            FOREIGN KEY (sop_id) REFERENCES qms_sops(id),
            FOREIGN KEY (work_instruction_id) REFERENCES qms_work_instructions(id),
            FOREIGN KEY (reported_by) REFERENCES users(id),
            FOREIGN KEY (assigned_to) REFERENCES users(id),
            FOREIGN KEY (closed_by) REFERENCES users(id)
        )
    ''')
    
    # Corrective and Preventive Actions (CAPA)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS qms_capa (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            capa_number TEXT UNIQUE NOT NULL,
            capa_type TEXT NOT NULL,
            priority TEXT DEFAULT 'Medium',
            source_type TEXT,
            source_id INTEGER,
            title TEXT NOT NULL,
            description TEXT,
            root_cause_analysis TEXT,
            corrective_action TEXT,
            preventive_action TEXT,
            verification_method TEXT,
            effectiveness_criteria TEXT,
            assigned_to INTEGER,
            owner_id INTEGER,
            status TEXT DEFAULT 'Open',
            target_date DATE,
            completion_date TIMESTAMP,
            verified_date TIMESTAMP,
            verified_by INTEGER,
            effectiveness_verified INTEGER DEFAULT 0,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            FOREIGN KEY (assigned_to) REFERENCES users(id),
            FOREIGN KEY (owner_id) REFERENCES users(id),
            FOREIGN KEY (verified_by) REFERENCES users(id),
            FOREIGN KEY (created_by) REFERENCES users(id)
        )
    ''')
    
    # QMS Audit Trail
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS qms_audit_trail (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_type TEXT NOT NULL,
            document_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            field_changed TEXT,
            old_value TEXT,
            new_value TEXT,
            user_id INTEGER,
            user_name TEXT,
            ip_address TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notes TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    ''')
    
    # QMS AI Analysis Records
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS qms_ai_analyses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            analysis_type TEXT NOT NULL,
            context TEXT,
            request_data TEXT,
            response_data TEXT,
            recommendations TEXT,
            user_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    ''')
    
    # QMS Training Records
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS qms_training_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            document_type TEXT NOT NULL,
            document_id INTEGER NOT NULL,
            training_type TEXT DEFAULT 'Initial',
            training_date DATE,
            trainer_id INTEGER,
            score REAL,
            passed INTEGER DEFAULT 1,
            certificate_number TEXT,
            expiry_date DATE,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (trainer_id) REFERENCES users(id)
        )
    ''')
    
    # QMS Compliance Metrics
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS qms_compliance_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_date DATE NOT NULL,
            metric_type TEXT NOT NULL,
            module TEXT,
            total_transactions INTEGER DEFAULT 0,
            compliant_transactions INTEGER DEFAULT 0,
            deviations_count INTEGER DEFAULT 0,
            capa_open INTEGER DEFAULT 0,
            capa_closed INTEGER DEFAULT 0,
            acknowledgment_rate REAL DEFAULT 0,
            training_completion_rate REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # ============== Exchange Management Tables ==============
    
    # Exchange Master Records
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS exchange_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange_id TEXT UNIQUE NOT NULL,
            sales_order_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            shipped_serial_number TEXT,
            expected_core_serial TEXT,
            exchange_type TEXT DEFAULT 'Standard',
            core_due_date DATE,
            core_value REAL DEFAULT 0,
            exchange_fee REAL DEFAULT 0,
            deposit_amount REAL DEFAULT 0,
            penalty_amount REAL DEFAULT 0,
            status TEXT DEFAULT 'Open',
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            closed_at TIMESTAMP,
            closed_by INTEGER,
            closure_notes TEXT,
            FOREIGN KEY (sales_order_id) REFERENCES sales_orders(id),
            FOREIGN KEY (customer_id) REFERENCES customers(id),
            FOREIGN KEY (product_id) REFERENCES products(id),
            FOREIGN KEY (created_by) REFERENCES users(id),
            FOREIGN KEY (closed_by) REFERENCES users(id)
        )
    ''')
    
    # Exchange Core Tracking
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS exchange_cores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange_id INTEGER NOT NULL,
            core_status TEXT DEFAULT 'Awaiting Core',
            core_serial_number TEXT,
            shipped_by_customer_date DATE,
            received_date DATE,
            received_by INTEGER,
            condition_on_receipt TEXT,
            inspection_notes TEXT,
            days_outstanding INTEGER DEFAULT 0,
            ownership_responsibility TEXT DEFAULT 'Customer',
            financial_exposure REAL DEFAULT 0,
            dispute_reason TEXT,
            dispute_date DATE,
            resolution_notes TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (exchange_id) REFERENCES exchange_master(id) ON DELETE CASCADE,
            FOREIGN KEY (received_by) REFERENCES users(id)
        )
    ''')
    
    # Exchange Linked Purchase Orders
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS exchange_purchase_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange_id INTEGER NOT NULL,
            purchase_order_id INTEGER NOT NULL,
            po_exchange_fee REAL DEFAULT 0,
            po_core_charge REAL DEFAULT 0,
            po_penalty REAL DEFAULT 0,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (exchange_id) REFERENCES exchange_master(id) ON DELETE CASCADE,
            FOREIGN KEY (purchase_order_id) REFERENCES purchase_orders(id)
        )
    ''')
    
    # Exchange Agreements
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS exchange_agreements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange_id INTEGER NOT NULL,
            agreement_number TEXT UNIQUE NOT NULL,
            version INTEGER DEFAULT 1,
            customer_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            part_number TEXT,
            serial_number TEXT,
            core_due_date DATE,
            exchange_terms TEXT,
            penalty_terms TEXT,
            legal_clauses TEXT,
            status TEXT DEFAULT 'Draft',
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            generated_by INTEGER,
            sent_to_customer INTEGER DEFAULT 0,
            sent_date TIMESTAMP,
            signed_date DATE,
            document_filename TEXT,
            FOREIGN KEY (exchange_id) REFERENCES exchange_master(id) ON DELETE CASCADE,
            FOREIGN KEY (customer_id) REFERENCES customers(id),
            FOREIGN KEY (product_id) REFERENCES products(id),
            FOREIGN KEY (generated_by) REFERENCES users(id)
        )
    ''')
    
    # Exchange Audit Log (immutable)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS exchange_audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange_id INTEGER NOT NULL,
            action_type TEXT NOT NULL,
            previous_status TEXT,
            new_status TEXT,
            action_details TEXT,
            performed_by INTEGER,
            performed_by_name TEXT,
            performed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ip_address TEXT,
            requires_justification INTEGER DEFAULT 0,
            justification TEXT,
            approved_by INTEGER,
            FOREIGN KEY (exchange_id) REFERENCES exchange_master(id),
            FOREIGN KEY (performed_by) REFERENCES users(id),
            FOREIGN KEY (approved_by) REFERENCES users(id)
        )
    ''')
    
    # Exchange AI Coordinator Analyses
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS exchange_ai_analyses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            analysis_type TEXT NOT NULL,
            exchange_id INTEGER,
            customer_id INTEGER,
            risk_level TEXT,
            risk_score REAL,
            findings TEXT,
            recommendations TEXT,
            predicted_outcome TEXT,
            confidence_score REAL,
            analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (exchange_id) REFERENCES exchange_master(id),
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        )
    ''')
    
    # Exchange Alerts
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS exchange_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange_id INTEGER NOT NULL,
            alert_type TEXT NOT NULL,
            severity TEXT DEFAULT 'Medium',
            title TEXT NOT NULL,
            message TEXT,
            is_read INTEGER DEFAULT 0,
            is_resolved INTEGER DEFAULT 0,
            resolved_by INTEGER,
            resolved_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (exchange_id) REFERENCES exchange_master(id),
            FOREIGN KEY (resolved_by) REFERENCES users(id)
        )
    ''')
    
    # Migrations for exchange tables
    try:
        cursor.execute('ALTER TABLE exchange_master ADD COLUMN repair_work_order_id INTEGER REFERENCES work_orders(id)')
    except:
        pass
    
    try:
        cursor.execute('ALTER TABLE exchange_cores ADD COLUMN work_order_id INTEGER REFERENCES work_orders(id)')
    except:
        pass
    
    try:
        cursor.execute('ALTER TABLE exchange_cores ADD COLUMN receiving_location TEXT')
    except:
        pass
    
    try:
        cursor.execute('ALTER TABLE exchange_cores ADD COLUMN quantity_received INTEGER DEFAULT 1')
    except:
        pass
    
    # Part Intake System - Supplier Web Part Capture
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS part_intake_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            intake_number TEXT UNIQUE NOT NULL,
            source_type TEXT NOT NULL,
            source_url TEXT,
            source_file_path TEXT,
            raw_content TEXT,
            status TEXT DEFAULT 'Pending',
            supplier_name TEXT,
            supplier_part_number TEXT,
            oem_name TEXT,
            manufacturer_part_number TEXT,
            short_description TEXT,
            long_description TEXT,
            category TEXT,
            base_uom TEXT,
            purchase_uom TEXT,
            packaging_quantity REAL,
            sourcing_price REAL,
            technical_attributes TEXT,
            compliance_indicators TEXT,
            image_urls TEXT,
            confidence_scores TEXT,
            duplicate_check_status TEXT,
            matched_product_ids TEXT,
            match_type TEXT,
            normalized_data TEXT,
            conversion_status TEXT,
            converted_product_id INTEGER,
            converted_by INTEGER,
            converted_at TIMESTAMP,
            captured_by INTEGER NOT NULL,
            captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            approved_by INTEGER,
            approved_at TIMESTAMP,
            rejection_reason TEXT,
            notes TEXT,
            FOREIGN KEY (converted_product_id) REFERENCES products(id),
            FOREIGN KEY (converted_by) REFERENCES users(id),
            FOREIGN KEY (captured_by) REFERENCES users(id),
            FOREIGN KEY (approved_by) REFERENCES users(id)
        )
    ''')
    
    # Part Intake Audit Trail
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS part_intake_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            intake_id INTEGER NOT NULL,
            action_type TEXT NOT NULL,
            action_details TEXT,
            raw_data_snapshot TEXT,
            changes_made TEXT,
            performed_by INTEGER,
            performed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ip_address TEXT,
            user_agent TEXT,
            FOREIGN KEY (intake_id) REFERENCES part_intake_records(id),
            FOREIGN KEY (performed_by) REFERENCES users(id)
        )
    ''')
    
    # Part Intake Supplier Cross-Reference
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS part_intake_supplier_xref (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            supplier_id INTEGER,
            supplier_name TEXT,
            supplier_part_number TEXT,
            manufacturer_name TEXT,
            manufacturer_part_number TEXT,
            source_url TEXT,
            source_document TEXT,
            is_primary INTEGER DEFAULT 0,
            is_verified INTEGER DEFAULT 0,
            verified_by INTEGER,
            verified_at TIMESTAMP,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products(id),
            FOREIGN KEY (supplier_id) REFERENCES suppliers(id),
            FOREIGN KEY (verified_by) REFERENCES users(id),
            FOREIGN KEY (created_by) REFERENCES users(id)
        )
    ''')
    
    # Add sourcing_price column to part_intake_records if missing
    try:
        cursor.execute('ALTER TABLE part_intake_records ADD COLUMN sourcing_price REAL')
    except:
        pass
    
    # Part Intake indexes
    try:
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_intake_status ON part_intake_records(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_intake_mpn ON part_intake_records(manufacturer_part_number)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_intake_supplier_pn ON part_intake_records(supplier_part_number)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_intake_xref_product ON part_intake_supplier_xref(product_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_intake_xref_mpn ON part_intake_supplier_xref(manufacturer_part_number)')
    except:
        pass
    
    # Create indexes for QMS tables
    try:
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_qms_sops_status ON qms_sops(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_qms_sops_category ON qms_sops(category_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_qms_wi_sop ON qms_work_instructions(sop_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_qms_deviations_status ON qms_deviations(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_qms_capa_status ON qms_capa(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_qms_ack_user ON qms_acknowledgments(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_qms_audit_doc ON qms_audit_trail(document_type, document_id)')
        # Exchange indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_exchange_status ON exchange_master(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_exchange_customer ON exchange_master(customer_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_exchange_core_status ON exchange_cores(core_status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_exchange_alerts_unread ON exchange_alerts(is_read, is_resolved)')
    except:
        pass
