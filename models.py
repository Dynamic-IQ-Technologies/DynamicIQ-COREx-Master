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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (po_id) REFERENCES purchase_orders(id) ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (uom_id) REFERENCES uom_master(id),
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
            ('part_category', 'TEXT DEFAULT "Other"')
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
            ('serial_number', 'TEXT')
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
            ('base_received_quantity', 'REAL DEFAULT 0')
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
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
        
        # Add work_order_id and task_id columns to time_clock_punches if they don't exist
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
        
        # Migrate sales_order_lines table - add new columns if they don't exist
        self._migrate_sales_order_lines(cursor)
        
        # Migrate service_wo_materials table - add serial_number column if it doesn't exist
        self._migrate_service_wo_materials(cursor)
        
        # Migrate mro_capabilities table - add applicability and part_class columns if they don't exist
        self._migrate_mro_capabilities(cursor)
        
        # Migrate work_orders table - add so_id and customer_id columns for sales order linking
        self._migrate_work_orders_so_link(cursor)
        
        conn.commit()
        conn.close()
    
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
            'released_to_shipping_at': "ALTER TABLE sales_order_lines ADD COLUMN released_to_shipping_at TIMESTAMP",
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
                    updated_by = ?, last_updated = CURRENT_TIMESTAMP
                WHERE id = 1
            ''', (
                data.get('company_name'), data.get('dba'), data.get('address_line1'),
                data.get('address_line2'), data.get('city'), data.get('state'),
                data.get('postal_code'), data.get('country'), data.get('phone'),
                data.get('email'), data.get('website'), data.get('tax_id'),
                data.get('duns_number'), data.get('cage_code'), data.get('logo_filename'),
                data.get('auto_post_invoice_gl', 0),
                user_id
            ))
        else:
            cursor.execute('''
                INSERT INTO company_settings (
                    id, company_name, dba, address_line1, address_line2,
                    city, state, postal_code, country, phone, email, website,
                    tax_id, duns_number, cage_code, logo_filename, auto_post_invoice_gl, updated_by
                ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data.get('company_name'), data.get('dba'), data.get('address_line1'),
                data.get('address_line2'), data.get('city'), data.get('state'),
                data.get('postal_code'), data.get('country'), data.get('phone'),
                data.get('email'), data.get('website'), data.get('tax_id'),
                data.get('duns_number'), data.get('cage_code'), data.get('logo_filename'),
                data.get('auto_post_invoice_gl', 0),
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
                'auto_post_invoice_gl': 0
            }
            db = Database()
            conn = db.get_connection()
            conn.execute('''
                INSERT INTO company_settings (
                    id, company_name, dba, address_line1, address_line2,
                    city, state, postal_code, country, phone, email, website,
                    tax_id, duns_number, cage_code, logo_filename, auto_post_invoice_gl
                ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                default_data['company_name'], default_data['dba'],
                default_data['address_line1'], default_data['address_line2'],
                default_data['city'], default_data['state'], default_data['postal_code'],
                default_data['country'], default_data['phone'], default_data['email'],
                default_data['website'], default_data['tax_id'], default_data['duns_number'],
                default_data['cage_code'], default_data['logo_filename'],
                default_data['auto_post_invoice_gl']
            ))
            conn.commit()
            conn.close()
            settings = CompanySettings.get()
        return settings


class AuditLogger:
    """Helper class for automatic audit trail logging"""
    
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
