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
                payment_terms INTEGER DEFAULT 30,
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
        
        # Add bin_location column to receiving_transactions if it doesn't exist
        rt_columns = [row[1] for row in cursor.execute('PRAGMA table_info(receiving_transactions)').fetchall()]
        if 'bin_location' not in rt_columns:
            try:
                cursor.execute('ALTER TABLE receiving_transactions ADD COLUMN bin_location TEXT')
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
                notes TEXT,
                status TEXT DEFAULT 'Approved',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (employee_id) REFERENCES labor_resources(id)
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
        
        # Create product_uom_conversions table (Part-UOM associations)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS product_uom_conversions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                uom_id INTEGER NOT NULL,
                conversion_factor REAL NOT NULL DEFAULT 1.0,
                is_base_uom INTEGER DEFAULT 0,
                is_purchase_uom INTEGER DEFAULT 0,
                is_issue_uom INTEGER DEFAULT 0,
                created_by INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (uom_id) REFERENCES uom_master(id),
                FOREIGN KEY (created_by) REFERENCES users(id),
                UNIQUE(product_id, uom_id)
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
        
        # Migrate sales_order_lines table - add new columns if they don't exist
        self._migrate_sales_order_lines(cursor)
        
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
            # Audit trail fields
            'created_by': "ALTER TABLE sales_order_lines ADD COLUMN created_by INTEGER",
            'modified_by': "ALTER TABLE sales_order_lines ADD COLUMN modified_by INTEGER",
            'modified_at': "ALTER TABLE sales_order_lines ADD COLUMN modified_at TIMESTAMP"
        }
        
        # Add missing columns
        for column_name, alter_sql in new_columns.items():
            if column_name not in existing_columns:
                cursor.execute(alter_sql)
    
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
                    duns_number = ?, cage_code = ?, logo_filename = ?,
                    updated_by = ?, last_updated = CURRENT_TIMESTAMP
                WHERE id = 1
            ''', (
                data.get('company_name'), data.get('dba'), data.get('address_line1'),
                data.get('address_line2'), data.get('city'), data.get('state'),
                data.get('postal_code'), data.get('country'), data.get('phone'),
                data.get('email'), data.get('website'), data.get('tax_id'),
                data.get('duns_number'), data.get('cage_code'), data.get('logo_filename'),
                user_id
            ))
        else:
            cursor.execute('''
                INSERT INTO company_settings (
                    id, company_name, dba, address_line1, address_line2,
                    city, state, postal_code, country, phone, email, website,
                    tax_id, duns_number, cage_code, logo_filename, updated_by
                ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data.get('company_name'), data.get('dba'), data.get('address_line1'),
                data.get('address_line2'), data.get('city'), data.get('state'),
                data.get('postal_code'), data.get('country'), data.get('phone'),
                data.get('email'), data.get('website'), data.get('tax_id'),
                data.get('duns_number'), data.get('cage_code'), data.get('logo_filename'),
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
                'logo_filename': None
            }
            db = Database()
            conn = db.get_connection()
            conn.execute('''
                INSERT INTO company_settings (
                    id, company_name, dba, address_line1, address_line2,
                    city, state, postal_code, country, phone, email, website,
                    tax_id, duns_number, cage_code, logo_filename
                ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                default_data['company_name'], default_data['dba'],
                default_data['address_line1'], default_data['address_line2'],
                default_data['city'], default_data['state'], default_data['postal_code'],
                default_data['country'], default_data['phone'], default_data['email'],
                default_data['website'], default_data['tax_id'], default_data['duns_number'],
                default_data['cage_code'], default_data['logo_filename']
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
