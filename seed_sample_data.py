#!/usr/bin/env python3
"""
Seed Sample Data Script for Dynamic.IQ-MRPx
Creates comprehensive sample data to demonstrate all system functionalities
Maximum: 10 clients, 10 suppliers, 10 capabilities
"""

import sqlite3
from datetime import datetime, timedelta
from werkzeug.security import generate_password_hash
import random
import uuid

def seed_data():
    conn = sqlite3.connect('mrp.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    print("Starting sample data seed...")
    
    # ============================================
    # 1. COMPANY SETTINGS
    # ============================================
    cursor.execute('''
        INSERT OR REPLACE INTO company_settings 
        (id, company_name, dba, address_line1, city, state, postal_code, country, 
         phone, email, website, tax_id, cage_code, auto_post_invoice_gl)
        VALUES (1, 'Precision Aerospace MRO', 'PA-MRO', '1500 Industrial Parkway', 
                'Phoenix', 'AZ', '85001', 'USA', '(602) 555-1000', 
                'info@pa-mro.com', 'www.pa-mro.com', '86-1234567', 'A1B2C', 1)
    ''')
    print("  - Company settings created")
    
    # ============================================
    # 2. USERS (Admin, Planner, Sales, Tech)
    # ============================================
    users = [
        ('admin', 'admin@pa-mro.com', 'admin123', 'Admin'),
        ('jsmith', 'jsmith@pa-mro.com', 'planner123', 'Planner'),
        ('mwilson', 'mwilson@pa-mro.com', 'sales123', 'Sales'),
        ('tgarcia', 'tgarcia@pa-mro.com', 'tech123', 'Technician'),
        ('lchen', 'lchen@pa-mro.com', 'cs123', 'Customer Service'),
    ]
    user_ids = {}
    for username, email, password, role in users:
        cursor.execute('SELECT id FROM users WHERE username = ?', (username,))
        existing = cursor.fetchone()
        if not existing:
            cursor.execute('''
                INSERT INTO users (username, email, password_hash, role)
                VALUES (?, ?, ?, ?)
            ''', (username, email, generate_password_hash(password), role))
            user_ids[username] = cursor.lastrowid
        else:
            user_ids[username] = existing['id']
    print(f"  - {len(users)} users created/verified")
    
    # ============================================
    # 3. CUSTOMERS (10)
    # ============================================
    customers = [
        ('CUST001', 'Delta Air Lines', 'Michael Johnson', 'mjohnson@delta.com', '(404) 555-2000', '1030 Delta Blvd, Atlanta, GA 30320', 30, 500000, 0),
        ('CUST002', 'United Airlines', 'Sarah Williams', 'swilliams@united.com', '(312) 555-3000', '233 S Wacker Dr, Chicago, IL 60606', 30, 750000, 0),
        ('CUST003', 'Southwest Airlines', 'Robert Martinez', 'rmartinez@southwest.com', '(214) 555-4000', '2702 Love Field Dr, Dallas, TX 75235', 45, 400000, 0),
        ('CUST004', 'American Airlines', 'Jennifer Brown', 'jbrown@aa.com', '(817) 555-5000', '4333 Amon Carter Blvd, Fort Worth, TX 76155', 30, 600000, 0),
        ('CUST005', 'JetBlue Airways', 'David Lee', 'dlee@jetblue.com', '(718) 555-6000', '27-01 Queens Plaza N, Long Island City, NY 11101', 30, 350000, 0),
        ('CUST006', 'Alaska Airlines', 'Amanda Taylor', 'ataylor@alaskaair.com', '(206) 555-7000', '19300 International Blvd, Seattle, WA 98188', 45, 300000, 1),
        ('CUST007', 'FedEx Express', 'Christopher Moore', 'cmoore@fedex.com', '(901) 555-8000', '3600 Hacks Cross Rd, Memphis, TN 38125', 30, 800000, 0),
        ('CUST008', 'UPS Airlines', 'Michelle Davis', 'mdavis@ups.com', '(502) 555-9000', '1400 N Hurstbourne Pkwy, Louisville, KY 40223', 30, 700000, 0),
        ('CUST009', 'Spirit Airlines', 'James Anderson', 'janderson@spirit.com', '(954) 555-1100', '2800 Executive Way, Miramar, FL 33025', 30, 250000, 0),
        ('CUST010', 'Frontier Airlines', 'Patricia White', 'pwhite@frontier.com', '(720) 555-1200', '4545 Airport Way, Denver, CO 80239', 45, 200000, 0),
    ]
    customer_ids = {}
    for customer in customers:
        cursor.execute('SELECT id FROM customers WHERE customer_number = ?', (customer[0],))
        existing = cursor.fetchone()
        if not existing:
            portal_token = str(uuid.uuid4())
            cursor.execute('''
                INSERT INTO customers (customer_number, name, contact_person, email, phone, 
                                       billing_address, payment_terms, credit_limit, tax_exempt,
                                       portal_token, portal_enabled, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'Active')
            ''', (*customer, portal_token))
            customer_ids[customer[0]] = cursor.lastrowid
        else:
            customer_ids[customer[0]] = existing['id']
    print(f"  - {len(customers)} customers created")
    
    # Customer contacts
    contacts = [
        (customer_ids['CUST001'], 'Michael Johnson', 'Procurement Manager', 'mjohnson@delta.com', '(404) 555-2001', '(404) 555-2002', 'Procurement', 1),
        (customer_ids['CUST001'], 'Lisa Clark', 'MRO Coordinator', 'lclark@delta.com', '(404) 555-2003', '', 'Maintenance', 0),
        (customer_ids['CUST002'], 'Sarah Williams', 'Supply Chain Director', 'swilliams@united.com', '(312) 555-3001', '', 'Supply Chain', 1),
        (customer_ids['CUST003'], 'Robert Martinez', 'Procurement Lead', 'rmartinez@southwest.com', '(214) 555-4001', '', 'Procurement', 1),
    ]
    for contact in contacts:
        cursor.execute('''
            INSERT OR IGNORE INTO customer_contacts 
            (customer_id, contact_name, title, email, phone, mobile, department, is_primary)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', contact)
    print(f"  - Customer contacts created")
    
    # ============================================
    # 4. SUPPLIERS (10)
    # ============================================
    suppliers = [
        ('SUP001', 'Boeing Commercial Parts', 'John Reynolds', 'jreynolds@boeing.com', '(206) 555-0001', '100 N Riverside Plaza, Chicago, IL 60606'),
        ('SUP002', 'Airbus Americas', 'Marie Dupont', 'mdupont@airbus.com', '(571) 555-0002', '2550 Wasser Terrace, Herndon, VA 20171'),
        ('SUP003', 'Pratt & Whitney', 'Thomas Edison', 'tedison@pw.com', '(860) 555-0003', '400 Main Street, East Hartford, CT 06118'),
        ('SUP004', 'GE Aerospace', 'Susan Miller', 'smiller@ge.com', '(513) 555-0004', '1 Neumann Way, Cincinnati, OH 45215'),
        ('SUP005', 'Honeywell Aerospace', 'Richard Chen', 'rchen@honeywell.com', '(602) 555-0005', '1944 E Sky Harbor Cir, Phoenix, AZ 85034'),
        ('SUP006', 'Collins Aerospace', 'Laura Garcia', 'lgarcia@collins.com', '(319) 555-0006', '400 Collins Rd NE, Cedar Rapids, IA 52498'),
        ('SUP007', 'Spirit AeroSystems', 'Mark Thompson', 'mthompson@spiritaero.com', '(316) 555-0007', '3801 S Oliver St, Wichita, KS 67210'),
        ('SUP008', 'Safran Aircraft Engines', 'Pierre Martin', 'pmartin@safran.com', '(513) 555-0008', '1 Rue Safran, Cincinnati, OH 45215'),
        ('SUP009', 'Parker Hannifin Aerospace', 'Emily Ross', 'eross@parker.com', '(949) 555-0009', '14300 Alton Pkwy, Irvine, CA 92618'),
        ('SUP010', 'Moog Aircraft Group', 'Daniel Wright', 'dwright@moog.com', '(716) 555-0010', '400 Jamison Rd, East Aurora, NY 14052'),
    ]
    supplier_ids = {}
    for supplier in suppliers:
        cursor.execute('SELECT id FROM suppliers WHERE code = ?', (supplier[0],))
        existing = cursor.fetchone()
        if not existing:
            cursor.execute('''
                INSERT INTO suppliers (code, name, contact_person, email, phone, address)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', supplier)
            supplier_ids[supplier[0]] = cursor.lastrowid
        else:
            supplier_ids[supplier[0]] = existing['id']
    print(f"  - {len(suppliers)} suppliers created")
    
    # Supplier contacts
    for supplier_code, supplier_id in list(supplier_ids.items())[:5]:
        cursor.execute('''
            INSERT OR IGNORE INTO supplier_contacts 
            (supplier_id, contact_name, title, email, phone, department, is_primary)
            VALUES (?, ?, ?, ?, ?, ?, 1)
        ''', (supplier_id, f'Sales Rep - {supplier_code}', 'Account Manager', 
              f'sales@{supplier_code.lower()}.com', '(555) 123-4567', 'Sales'))
    
    # ============================================
    # 5. UOM MASTER
    # ============================================
    uoms = [
        ('EA', 'Each', 'Count', 1.0, 0),
        ('PK', 'Pack', 'Count', 1.0, 0),
        ('BX', 'Box', 'Count', 1.0, 0),
        ('FT', 'Foot', 'Length', 1.0, 2),
        ('IN', 'Inch', 'Length', 0.0833, 2),
        ('LB', 'Pound', 'Weight', 1.0, 2),
        ('OZ', 'Ounce', 'Weight', 0.0625, 2),
        ('GAL', 'Gallon', 'Volume', 1.0, 2),
        ('QT', 'Quart', 'Volume', 0.25, 2),
        ('HR', 'Hour', 'Time', 1.0, 2),
    ]
    uom_ids = {}
    for uom in uoms:
        cursor.execute('SELECT id FROM uom_master WHERE uom_code = ?', (uom[0],))
        existing = cursor.fetchone()
        if not existing:
            cursor.execute('''
                INSERT INTO uom_master (uom_code, uom_name, uom_type, conversion_factor, rounding_precision, is_active)
                VALUES (?, ?, ?, ?, ?, 1)
            ''', uom)
            uom_ids[uom[0]] = cursor.lastrowid
        else:
            uom_ids[uom[0]] = existing['id']
    print(f"  - {len(uoms)} UOMs created")
    
    # ============================================
    # 6. PRODUCTS
    # ============================================
    products = [
        ('PN-B737-001', 'Boeing 737 Main Landing Gear Actuator', 'Hydraulic actuator for B737 MLG', 'EA', 'Part', 12500.00, 'Landing Gear'),
        ('PN-B737-002', 'Boeing 737 Engine Mount Assembly', 'CFM56 engine mount assy', 'EA', 'Assembly', 45000.00, 'Engine'),
        ('PN-A320-001', 'Airbus A320 Fuel Pump', 'Main fuel boost pump', 'EA', 'Part', 8750.00, 'Fuel System'),
        ('PN-A320-002', 'Airbus A320 Flap Actuator', 'Trailing edge flap actuator', 'EA', 'Part', 15200.00, 'Flight Controls'),
        ('PN-GEN-001', 'O-Ring Seal Kit', 'Assorted O-rings for hydraulic systems', 'PK', 'Consumable', 125.00, 'Seals'),
        ('PN-GEN-002', 'Hydraulic Fluid MIL-PRF-5606', 'Red hydraulic fluid', 'GAL', 'Consumable', 85.00, 'Fluids'),
        ('PN-GEN-003', 'Aircraft Grade Fastener Kit', 'AN hardware assortment', 'BX', 'Consumable', 450.00, 'Fasteners'),
        ('PN-B777-001', 'Boeing 777 APU Controller', 'Auxiliary power unit controller', 'EA', 'Part', 28500.00, 'APU'),
        ('PN-A350-001', 'Airbus A350 Bleed Air Valve', 'Engine bleed air valve', 'EA', 'Part', 18900.00, 'Pneumatic'),
        ('PN-EMB-001', 'Embraer E175 Nose Wheel Steering', 'NWS actuator assembly', 'EA', 'Assembly', 22000.00, 'Landing Gear'),
        ('RAW-AL-2024', 'Aluminum 2024-T3 Sheet 0.063"', 'Aircraft aluminum sheet', 'EA', 'Raw Material', 125.00, 'Raw Materials'),
        ('RAW-SS-316', 'Stainless Steel 316 Bar 1"', 'Corrosion resistant steel bar', 'FT', 'Raw Material', 45.00, 'Raw Materials'),
        ('RAW-TI-6AL4V', 'Titanium 6AL-4V Rod 0.5"', 'Aerospace titanium rod', 'FT', 'Raw Material', 285.00, 'Raw Materials'),
        ('PN-BEARING-001', 'Spherical Bearing MS21240', 'High load spherical bearing', 'EA', 'Part', 350.00, 'Bearings'),
        ('PN-SEAL-001', 'Hydraulic Piston Seal', 'High pressure piston seal', 'EA', 'Part', 85.00, 'Seals'),
    ]
    product_ids = {}
    for product in products:
        cursor.execute('SELECT id FROM products WHERE code = ?', (product[0],))
        existing = cursor.fetchone()
        if not existing:
            cursor.execute('''
                INSERT INTO products (code, name, description, unit_of_measure, product_type, cost, part_category)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', product)
            product_ids[product[0]] = cursor.lastrowid
        else:
            product_ids[product[0]] = existing['id']
    print(f"  - {len(products)} products created")
    
    # ============================================
    # 7. BILL OF MATERIALS (BOMs)
    # ============================================
    boms = [
        (product_ids['PN-B737-001'], product_ids['PN-GEN-001'], 2, 5, 'F1', 'A', 0),
        (product_ids['PN-B737-001'], product_ids['PN-SEAL-001'], 4, 2, 'F2', 'A', 0),
        (product_ids['PN-B737-001'], product_ids['PN-BEARING-001'], 2, 0, 'F3', 'A', 0),
        (product_ids['PN-B737-002'], product_ids['RAW-TI-6AL4V'], 5, 10, 'F1', 'A', 0),
        (product_ids['PN-B737-002'], product_ids['PN-GEN-003'], 1, 0, 'F2', 'A', 0),
        (product_ids['PN-A320-001'], product_ids['PN-GEN-001'], 1, 5, 'F1', 'A', 0),
        (product_ids['PN-A320-001'], product_ids['PN-SEAL-001'], 2, 2, 'F2', 'A', 0),
        (product_ids['PN-EMB-001'], product_ids['PN-BEARING-001'], 4, 0, 'F1', 'A', 0),
        (product_ids['PN-EMB-001'], product_ids['PN-GEN-001'], 2, 5, 'F2', 'A', 0),
    ]
    for bom in boms:
        cursor.execute('''
            INSERT OR IGNORE INTO boms 
            (parent_product_id, child_product_id, quantity, scrap_percentage, find_number, revision, level)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', bom)
    print(f"  - {len(boms)} BOM records created")
    
    # ============================================
    # 8. INVENTORY
    # ============================================
    for product_code, product_id in product_ids.items():
        qty = random.randint(5, 50) if 'RAW' in product_code or 'GEN' in product_code else random.randint(1, 10)
        reorder_pt = qty // 2
        safety_stock = reorder_pt // 2
        cursor.execute('''
            INSERT OR REPLACE INTO inventory 
            (product_id, quantity, reorder_point, safety_stock, warehouse_location, bin_location, condition, status)
            VALUES (?, ?, ?, ?, ?, ?, 'New', 'Available')
        ''', (product_id, qty, reorder_pt, safety_stock, 'Main Warehouse', f'A-{random.randint(1,10)}-{random.randint(1,5)}'))
    print(f"  - Inventory records created")
    
    # ============================================
    # 9. WORK ORDER STAGES
    # ============================================
    stages = [
        ('Receiving', 'Parts received for processing', '#17a2b8', 1),
        ('Teardown', 'Component teardown and inspection', '#ffc107', 2),
        ('Repair', 'Active repair work', '#fd7e14', 3),
        ('Assembly', 'Component assembly', '#6f42c1', 4),
        ('Testing', 'Functional testing and QC', '#20c997', 5),
        ('Final QC', 'Final quality control inspection', '#0dcaf0', 6),
        ('Shipping', 'Ready for shipment', '#198754', 7),
    ]
    stage_ids = {}
    for stage in stages:
        cursor.execute('SELECT id FROM work_order_stages WHERE name = ?', (stage[0],))
        existing = cursor.fetchone()
        if not existing:
            cursor.execute('''
                INSERT INTO work_order_stages (name, description, color, sequence, is_active)
                VALUES (?, ?, ?, ?, 1)
            ''', stage)
            stage_ids[stage[0]] = cursor.lastrowid
        else:
            stage_ids[stage[0]] = existing['id']
    print(f"  - {len(stages)} work order stages created")
    
    # ============================================
    # 10. SKILLSETS
    # ============================================
    skillsets = [
        ('Hydraulics', 'Hydraulic systems repair and overhaul', 'Mechanical'),
        ('Avionics', 'Avionics systems and electronics', 'Electrical'),
        ('Composites', 'Composite material repair', 'Structural'),
        ('NDT', 'Non-destructive testing', 'Inspection'),
        ('Welding', 'Aerospace welding certification', 'Fabrication'),
        ('Painting', 'Aircraft painting and coating', 'Finishing'),
        ('Sheet Metal', 'Sheet metal fabrication', 'Fabrication'),
        ('Fuel Systems', 'Fuel system maintenance', 'Mechanical'),
    ]
    skillset_ids = {}
    for skillset in skillsets:
        cursor.execute('SELECT id FROM skillsets WHERE skillset_name = ?', (skillset[0],))
        existing = cursor.fetchone()
        if not existing:
            cursor.execute('''
                INSERT INTO skillsets (skillset_name, description, category, status)
                VALUES (?, ?, ?, 'Active')
            ''', skillset)
            skillset_ids[skillset[0]] = cursor.lastrowid
        else:
            skillset_ids[skillset[0]] = existing['id']
    print(f"  - {len(skillsets)} skillsets created")
    
    # ============================================
    # 11. LABOR RESOURCES
    # ============================================
    labor_resources = [
        ('EMP001', 'John', 'Martinez', 'Lead Technician', 65.00, 'Production', 'jmartinez@pa-mro.com'),
        ('EMP002', 'Sarah', 'Johnson', 'Senior Technician', 55.00, 'Production', 'sjohnson@pa-mro.com'),
        ('EMP003', 'Michael', 'Chen', 'NDT Inspector', 60.00, 'Quality', 'mchen@pa-mro.com'),
        ('EMP004', 'Emily', 'Williams', 'Avionics Tech', 58.00, 'Production', 'ewilliams@pa-mro.com'),
        ('EMP005', 'David', 'Brown', 'Composite Tech', 52.00, 'Production', 'dbrown@pa-mro.com'),
        ('EMP006', 'Lisa', 'Davis', 'QC Inspector', 48.00, 'Quality', 'ldavis@pa-mro.com'),
        ('EMP007', 'Robert', 'Wilson', 'Welder', 55.00, 'Fabrication', 'rwilson@pa-mro.com'),
        ('EMP008', 'Jennifer', 'Taylor', 'Painter', 45.00, 'Finishing', 'jtaylor@pa-mro.com'),
    ]
    labor_ids = {}
    for labor in labor_resources:
        cursor.execute('SELECT id FROM labor_resources WHERE employee_code = ?', (labor[0],))
        existing = cursor.fetchone()
        if not existing:
            cursor.execute('''
                INSERT INTO labor_resources 
                (employee_code, first_name, last_name, role, hourly_rate, cost_center, email, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'Active')
            ''', labor)
            labor_ids[labor[0]] = cursor.lastrowid
        else:
            labor_ids[labor[0]] = existing['id']
    print(f"  - {len(labor_resources)} labor resources created")
    
    # Assign skills to labor resources
    skill_assignments = [
        ('EMP001', ['Hydraulics', 'Fuel Systems'], 'Expert'),
        ('EMP002', ['Hydraulics', 'Sheet Metal'], 'Advanced'),
        ('EMP003', ['NDT'], 'Expert'),
        ('EMP004', ['Avionics'], 'Expert'),
        ('EMP005', ['Composites'], 'Advanced'),
        ('EMP006', ['NDT'], 'Intermediate'),
        ('EMP007', ['Welding', 'Sheet Metal'], 'Expert'),
        ('EMP008', ['Painting'], 'Advanced'),
    ]
    for emp_code, skills, level in skill_assignments:
        for skill in skills:
            if emp_code in labor_ids and skill in skillset_ids:
                cursor.execute('''
                    INSERT OR IGNORE INTO labor_resource_skills 
                    (labor_resource_id, skillset_id, skill_level, certified)
                    VALUES (?, ?, ?, 1)
                ''', (labor_ids[emp_code], skillset_ids[skill], level))
    
    # ============================================
    # 12. WORK CENTERS
    # ============================================
    work_centers = [
        ('WC-HYD', 'Hydraulics Bay', 'Hydraulic component repair bay', 8.0, 5, 0.95, 75.00),
        ('WC-AVI', 'Avionics Shop', 'Avionics repair and testing', 8.0, 5, 0.90, 85.00),
        ('WC-NDT', 'NDT Lab', 'Non-destructive testing laboratory', 8.0, 5, 0.92, 80.00),
        ('WC-ASY', 'Assembly Area', 'Component assembly area', 10.0, 5, 0.88, 65.00),
        ('WC-TST', 'Test Cell', 'Functional testing facility', 8.0, 5, 0.85, 90.00),
    ]
    wc_ids = {}
    for wc in work_centers:
        cursor.execute('SELECT id FROM work_centers WHERE code = ?', (wc[0],))
        existing = cursor.fetchone()
        if not existing:
            cursor.execute('''
                INSERT INTO work_centers 
                (code, name, description, default_hours_per_day, default_days_per_week, efficiency_factor, cost_per_hour, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'Active')
            ''', wc)
            wc_ids[wc[0]] = cursor.lastrowid
        else:
            wc_ids[wc[0]] = existing['id']
    print(f"  - {len(work_centers)} work centers created")
    
    # ============================================
    # 13. WORK ORDERS
    # ============================================
    today = datetime.now().date()
    work_orders = [
        ('WO-2024-0001', product_ids['PN-B737-001'], 2, 'In Progress', 'High', stage_ids['Repair'], customer_ids['CUST001'], today - timedelta(days=5), today + timedelta(days=10)),
        ('WO-2024-0002', product_ids['PN-A320-001'], 1, 'Released', 'Medium', stage_ids['Teardown'], customer_ids['CUST002'], today - timedelta(days=2), today + timedelta(days=15)),
        ('WO-2024-0003', product_ids['PN-B737-002'], 1, 'In Progress', 'Critical', stage_ids['Assembly'], customer_ids['CUST003'], today - timedelta(days=10), today + timedelta(days=5)),
        ('WO-2024-0004', product_ids['PN-A320-002'], 3, 'Planned', 'Medium', stage_ids['Receiving'], customer_ids['CUST004'], today + timedelta(days=2), today + timedelta(days=20)),
        ('WO-2024-0005', product_ids['PN-B777-001'], 1, 'In Progress', 'High', stage_ids['Testing'], customer_ids['CUST007'], today - timedelta(days=8), today + timedelta(days=3)),
        ('WO-2024-0006', product_ids['PN-A350-001'], 2, 'Quality Control', 'Medium', stage_ids['Final QC'], customer_ids['CUST005'], today - timedelta(days=12), today + timedelta(days=1)),
        ('WO-2024-0007', product_ids['PN-EMB-001'], 1, 'Completed', 'Low', stage_ids['Shipping'], customer_ids['CUST009'], today - timedelta(days=20), today - timedelta(days=2)),
        ('WO-2024-0008', product_ids['PN-B737-001'], 1, 'Planned', 'Medium', stage_ids['Receiving'], customer_ids['CUST006'], today + timedelta(days=5), today + timedelta(days=25)),
    ]
    wo_ids = {}
    for wo in work_orders:
        cursor.execute('SELECT id FROM work_orders WHERE wo_number = ?', (wo[0],))
        existing = cursor.fetchone()
        if not existing:
            cursor.execute('''
                INSERT INTO work_orders 
                (wo_number, product_id, quantity, status, priority, stage_id, customer_id, 
                 planned_start_date, planned_end_date, material_cost, labor_cost, overhead_cost)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (*wo, random.randint(500, 5000), random.randint(1000, 8000), random.randint(200, 1000)))
            wo_ids[wo[0]] = cursor.lastrowid
        else:
            wo_ids[wo[0]] = existing['id']
    print(f"  - {len(work_orders)} work orders created")
    
    # Work Order Tasks
    task_num = 1
    for wo_num, wo_id in list(wo_ids.items())[:5]:
        tasks = [
            (f'TSK-{task_num:04d}', wo_id, 'Incoming Inspection', 'Inspect parts upon receipt', 2.0, labor_ids['EMP006']),
            (f'TSK-{task_num+1:04d}', wo_id, 'Disassembly', 'Disassemble component for repair', 4.0, labor_ids['EMP001']),
            (f'TSK-{task_num+2:04d}', wo_id, 'NDT Inspection', 'Perform NDT checks', 3.0, labor_ids['EMP003']),
            (f'TSK-{task_num+3:04d}', wo_id, 'Repair/Replace', 'Perform repairs as required', 8.0, labor_ids['EMP002']),
            (f'TSK-{task_num+4:04d}', wo_id, 'Reassembly', 'Reassemble component', 4.0, labor_ids['EMP001']),
            (f'TSK-{task_num+5:04d}', wo_id, 'Final Test', 'Functional testing', 2.0, labor_ids['EMP006']),
        ]
        for i, task in enumerate(tasks):
            cursor.execute('''
                INSERT OR IGNORE INTO work_order_tasks 
                (task_number, work_order_id, task_name, description, planned_hours, assigned_resource_id, sequence_number, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'Not Started')
            ''', (*task, i+1))
        task_num += 6
    print(f"  - Work order tasks created")
    
    # ============================================
    # 14. SALES ORDERS
    # ============================================
    sales_orders = [
        ('SO-2024-0001', customer_ids['CUST001'], 'Standard Sale', today - timedelta(days=10), today + timedelta(days=5), 'Confirmed', 45000.00),
        ('SO-2024-0002', customer_ids['CUST002'], 'Repair Exchange', today - timedelta(days=5), today + timedelta(days=15), 'In Production', 28500.00),
        ('SO-2024-0003', customer_ids['CUST003'], 'Outright Sale', today - timedelta(days=3), today + timedelta(days=20), 'Confirmed', 67200.00),
        ('SO-2024-0004', customer_ids['CUST004'], 'Standard Sale', today, today + timedelta(days=30), 'Draft', 15800.00),
        ('SO-2024-0005', customer_ids['CUST005'], 'Repair', today - timedelta(days=15), today - timedelta(days=2), 'Shipped', 22000.00),
        ('SO-2024-0006', customer_ids['CUST007'], 'Standard Sale', today - timedelta(days=8), today + timedelta(days=7), 'Confirmed', 85000.00),
    ]
    so_ids = {}
    for so in sales_orders:
        cursor.execute('SELECT id FROM sales_orders WHERE so_number = ?', (so[0],))
        existing = cursor.fetchone()
        if not existing:
            cursor.execute('''
                INSERT INTO sales_orders 
                (so_number, customer_id, sales_type, order_date, expected_ship_date, status, 
                 subtotal, total_amount, balance_due, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (*so, so[6], so[6], user_ids['mwilson']))
            so_ids[so[0]] = cursor.lastrowid
        else:
            so_ids[so[0]] = existing['id']
    print(f"  - {len(sales_orders)} sales orders created")
    
    # Sales Order Lines
    so_lines = [
        (so_ids['SO-2024-0001'], 1, product_ids['PN-B737-001'], 2, 12500.00),
        (so_ids['SO-2024-0001'], 2, product_ids['PN-GEN-001'], 5, 125.00),
        (so_ids['SO-2024-0002'], 1, product_ids['PN-A320-001'], 1, 8750.00),
        (so_ids['SO-2024-0003'], 1, product_ids['PN-B737-002'], 1, 45000.00),
        (so_ids['SO-2024-0003'], 2, product_ids['PN-GEN-002'], 10, 85.00),
        (so_ids['SO-2024-0004'], 1, product_ids['PN-A320-002'], 1, 15200.00),
        (so_ids['SO-2024-0005'], 1, product_ids['PN-EMB-001'], 1, 22000.00),
        (so_ids['SO-2024-0006'], 1, product_ids['PN-B777-001'], 2, 28500.00),
    ]
    for line in so_lines:
        cursor.execute('''
            INSERT OR IGNORE INTO sales_order_lines 
            (so_id, line_number, product_id, quantity, unit_price, line_total)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (*line, line[3] * line[4]))
    
    # ============================================
    # 15. PURCHASE ORDERS
    # ============================================
    purchase_orders = [
        ('PO-2024-0001', supplier_ids['SUP001'], 'Approved', today - timedelta(days=5), today + timedelta(days=10)),
        ('PO-2024-0002', supplier_ids['SUP003'], 'Sent', today - timedelta(days=2), today + timedelta(days=20)),
        ('PO-2024-0003', supplier_ids['SUP005'], 'Received', today - timedelta(days=15), today - timedelta(days=3)),
        ('PO-2024-0004', supplier_ids['SUP002'], 'Draft', today, today + timedelta(days=30)),
        ('PO-2024-0005', supplier_ids['SUP009'], 'Approved', today - timedelta(days=3), today + timedelta(days=15)),
    ]
    po_ids = {}
    for po in purchase_orders:
        cursor.execute('SELECT id FROM purchase_orders WHERE po_number = ?', (po[0],))
        existing = cursor.fetchone()
        if not existing:
            cursor.execute('''
                INSERT INTO purchase_orders 
                (po_number, supplier_id, status, order_date, expected_delivery_date)
                VALUES (?, ?, ?, ?, ?)
            ''', po)
            po_ids[po[0]] = cursor.lastrowid
        else:
            po_ids[po[0]] = existing['id']
    print(f"  - {len(purchase_orders)} purchase orders created")
    
    # Purchase Order Lines
    po_lines = [
        (po_ids['PO-2024-0001'], 1, product_ids['PN-BEARING-001'], 20, 350.00),
        (po_ids['PO-2024-0001'], 2, product_ids['PN-SEAL-001'], 50, 85.00),
        (po_ids['PO-2024-0002'], 1, product_ids['PN-GEN-002'], 25, 85.00),
        (po_ids['PO-2024-0003'], 1, product_ids['RAW-AL-2024'], 100, 125.00),
        (po_ids['PO-2024-0003'], 2, product_ids['RAW-TI-6AL4V'], 30, 285.00),
        (po_ids['PO-2024-0004'], 1, product_ids['PN-GEN-001'], 40, 125.00),
        (po_ids['PO-2024-0005'], 1, product_ids['PN-GEN-003'], 10, 450.00),
    ]
    for line in po_lines:
        cursor.execute('''
            INSERT OR IGNORE INTO purchase_order_lines 
            (po_id, line_number, product_id, quantity, unit_price)
            VALUES (?, ?, ?, ?, ?)
        ''', line)
    
    # ============================================
    # 16. MRO CAPABILITIES (10)
    # ============================================
    capabilities = [
        ('CAP-HYD-001', 'PN-B737-001', 'B737 MLG Actuator Overhaul', 'Boeing 737-600/700/800/900', 'Component', 'Hydraulic actuator complete overhaul', 'Landing Gear', 'Boeing', 'AS9100D', 1),
        ('CAP-HYD-002', 'PN-A320-001', 'A320 Fuel Pump Repair', 'Airbus A318/A319/A320/A321', 'Component', 'Fuel boost pump repair and testing', 'Fuel System', 'Airbus', 'AS9100D, FAA 145', 1),
        ('CAP-FLT-001', 'PN-A320-002', 'A320 Flap Actuator Overhaul', 'Airbus A320 Family', 'Component', 'Trailing edge flap actuator OH', 'Flight Controls', 'Airbus', 'EASA Part 145', 1),
        ('CAP-ENG-001', 'PN-B737-002', 'CFM56 Engine Mount Repair', 'Boeing 737NG, Airbus A320ceo', 'Assembly', 'Engine mount inspection and repair', 'Engine', 'CFM International', 'AS9100D', 1),
        ('CAP-APU-001', 'PN-B777-001', 'B777 APU Controller Repair', 'Boeing 777-200/300', 'Component', 'APU controller repair and testing', 'APU', 'Honeywell', 'FAA 145', 1),
        ('CAP-PNE-001', 'PN-A350-001', 'A350 Bleed Valve Overhaul', 'Airbus A350-900/1000', 'Component', 'Engine bleed air valve overhaul', 'Pneumatic', 'Airbus', 'EASA Part 145', 1),
        ('CAP-LDG-001', 'PN-EMB-001', 'E175 NWS Actuator Repair', 'Embraer E170/E175', 'Assembly', 'Nose wheel steering system repair', 'Landing Gear', 'Embraer', 'AS9100D', 1),
        ('CAP-NDT-001', 'GENERAL', 'NDT Services - Level III', 'All Aircraft Types', 'Service', 'UT, MT, PT, RT, VT, ET inspections', 'Inspection', 'Multiple', 'ASNT TC-1A', 0),
        ('CAP-CMP-001', 'GENERAL', 'Composite Repair', 'All Aircraft Types', 'Service', 'Composite structure repair and bonding', 'Structural', 'Multiple', 'AS9100D', 0),
        ('CAP-AVI-001', 'GENERAL', 'Avionics Repair', 'All Aircraft Types', 'Service', 'Avionics component repair and testing', 'Avionics', 'Multiple', 'FAA 145', 0),
    ]
    cap_ids = {}
    for cap in capabilities:
        cursor.execute('SELECT id FROM mro_capabilities WHERE capability_code = ?', (cap[0],))
        existing = cursor.fetchone()
        if not existing:
            cursor.execute('''
                INSERT INTO mro_capabilities 
                (capability_code, part_number, capability_name, applicability, part_class, 
                 description, category, manufacturer, compliance, certification_required, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Active')
            ''', cap)
            cap_ids[cap[0]] = cursor.lastrowid
        else:
            cap_ids[cap[0]] = existing['id']
    print(f"  - {len(capabilities)} MRO capabilities created")
    
    # ============================================
    # 17. NDT TECHNICIANS & CERTIFICATIONS
    # ============================================
    ndt_techs = [
        ('NDT001', 'Mark', 'Thompson', 'mthompson@pa-mro.com', '(602) 555-2001', 'PA-MRO', 'Active'),
        ('NDT002', 'Susan', 'Garcia', 'sgarcia@pa-mro.com', '(602) 555-2002', 'PA-MRO', 'Active'),
        ('NDT003', 'Kevin', 'Lee', 'klee@contractor.com', '(602) 555-2003', 'ABC NDT Services', 'Active'),
    ]
    ndt_tech_ids = {}
    for tech in ndt_techs:
        cursor.execute('SELECT id FROM ndt_technicians WHERE technician_number = ?', (tech[0],))
        existing = cursor.fetchone()
        if not existing:
            cursor.execute('''
                INSERT INTO ndt_technicians 
                (technician_number, first_name, last_name, email, phone, employer, contract_status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', tech)
            ndt_tech_ids[tech[0]] = cursor.lastrowid
        else:
            ndt_tech_ids[tech[0]] = existing['id']
    print(f"  - {len(ndt_techs)} NDT technicians created")
    
    # NDT Certifications
    certs = [
        (ndt_tech_ids['NDT001'], 'UT', 'Level III', 'UT-III-2024-001', today - timedelta(days=365), today + timedelta(days=730), 'ASNT'),
        (ndt_tech_ids['NDT001'], 'MT', 'Level II', 'MT-II-2024-001', today - timedelta(days=180), today + timedelta(days=545), 'ASNT'),
        (ndt_tech_ids['NDT001'], 'PT', 'Level II', 'PT-II-2024-001', today - timedelta(days=180), today + timedelta(days=545), 'ASNT'),
        (ndt_tech_ids['NDT002'], 'UT', 'Level II', 'UT-II-2024-002', today - timedelta(days=90), today + timedelta(days=640), 'ASNT'),
        (ndt_tech_ids['NDT002'], 'RT', 'Level II', 'RT-II-2024-002', today - timedelta(days=90), today + timedelta(days=640), 'ASNT'),
        (ndt_tech_ids['NDT003'], 'VT', 'Level II', 'VT-II-2024-003', today - timedelta(days=60), today + timedelta(days=670), 'ASNT'),
        (ndt_tech_ids['NDT003'], 'ET', 'Level I', 'ET-I-2024-003', today - timedelta(days=30), today + timedelta(days=335), 'ASNT'),
    ]
    for cert in certs:
        cursor.execute('''
            INSERT OR IGNORE INTO ndt_certifications 
            (technician_id, method, level, certification_number, issued_date, expiration_date, issuing_body, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'Active')
        ''', cert)
    print(f"  - NDT certifications created")
    
    # NDT Work Orders
    ndt_wos = [
        ('NDT-2024-0001', 'Manufacturing', customer_ids['CUST001'], None, wo_ids.get('WO-2024-0001'), product_ids['PN-B737-001'], 'SN-12345', 'UT,MT', 'In Inspection', ndt_tech_ids['NDT001']),
        ('NDT-2024-0002', 'Standalone', customer_ids['CUST003'], None, None, product_ids['PN-A320-001'], 'SN-67890', 'PT', 'Scheduled', ndt_tech_ids['NDT002']),
        ('NDT-2024-0003', 'Manufacturing', customer_ids['CUST007'], None, wo_ids.get('WO-2024-0005'), product_ids['PN-B777-001'], 'SN-11111', 'UT,RT', 'Approved', ndt_tech_ids['NDT001']),
    ]
    for ndt_wo in ndt_wos:
        cursor.execute('SELECT id FROM ndt_work_orders WHERE ndt_wo_number = ?', (ndt_wo[0],))
        if not cursor.fetchone():
            cursor.execute('''
                INSERT INTO ndt_work_orders 
                (ndt_wo_number, order_type, customer_id, sales_order_id, work_order_id, product_id, 
                 serial_number, ndt_methods, status, assigned_technician_id, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (*ndt_wo, user_ids['admin']))
    print(f"  - NDT work orders created")
    
    # ============================================
    # 18. TOOLS
    # ============================================
    tools = [
        ('TL-001', 'Digital Torque Wrench', 'Precision digital torque wrench 0-100 ft-lb', 'Hand Tools', 'Snap-On', 'TECHANGLE', 'SN-TW-001', 'Main Tool Crib', 'Available', 'Good', today - timedelta(days=180), 850.00, today - timedelta(days=30), today + timedelta(days=335), 365),
        ('TL-002', 'Ultrasonic Flaw Detector', 'Portable UT inspection unit', 'NDT Equipment', 'Olympus', 'EPOCH 650', 'SN-UT-001', 'NDT Lab', 'Available', 'Good', today - timedelta(days=365), 18500.00, today - timedelta(days=15), today + timedelta(days=350), 365),
        ('TL-003', 'Hydraulic Test Stand', 'Component test stand 3000 PSI', 'Test Equipment', 'Custom', 'HTS-3000', 'SN-HTS-001', 'Hydraulics Bay', 'In Use', 'Good', today - timedelta(days=730), 45000.00, today - timedelta(days=60), today + timedelta(days=305), 365),
        ('TL-004', 'Bore Scope', 'Flexible video borescope', 'Inspection', 'Olympus', 'IPLEX RX', 'SN-BS-001', 'Main Tool Crib', 'Available', 'Good', today - timedelta(days=90), 12000.00, None, None, None),
        ('TL-005', 'Calibrated Pressure Gauge Set', 'Precision pressure gauge 0-5000 PSI', 'Measurement', 'Ashcroft', 'PG-5000', 'SN-PG-001', 'Calibration Lab', 'Available', 'Good', today - timedelta(days=60), 2500.00, today - timedelta(days=10), today + timedelta(days=80), 90),
    ]
    for tool in tools:
        cursor.execute('SELECT id FROM tools WHERE tool_number = ?', (tool[0],))
        if not cursor.fetchone():
            cursor.execute('''
                INSERT INTO tools 
                (tool_number, name, description, category, manufacturer, model_number, serial_number,
                 location, status, condition, purchase_date, purchase_cost, last_calibration_date,
                 next_calibration_date, calibration_interval_days)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', tool)
    print(f"  - {len(tools)} tools created")
    
    # ============================================
    # 19. RFQs
    # ============================================
    rfqs = [
        ('RFQ-2024-0001', 'Q1 Raw Material Procurement', 'Aluminum and titanium stock', 'Issued', today - timedelta(days=10), today + timedelta(days=5)),
        ('RFQ-2024-0002', 'Bearing Replacement Parts', 'Spherical bearings for landing gear', 'Draft', today, today + timedelta(days=14)),
        ('RFQ-2024-0003', 'Seal Kit Bulk Order', 'O-ring and seal kits', 'Closed', today - timedelta(days=30), today - timedelta(days=15)),
    ]
    rfq_ids = {}
    for rfq in rfqs:
        cursor.execute('SELECT id FROM rfqs WHERE rfq_number = ?', (rfq[0],))
        existing = cursor.fetchone()
        if not existing:
            cursor.execute('''
                INSERT INTO rfqs 
                (rfq_number, title, description, status, issue_date, due_date, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (*rfq, user_ids['jsmith']))
            rfq_ids[rfq[0]] = cursor.lastrowid
        else:
            rfq_ids[rfq[0]] = existing['id']
    print(f"  - {len(rfqs)} RFQs created")
    
    # RFQ Lines and Suppliers
    if 'RFQ-2024-0001' in rfq_ids:
        cursor.execute('INSERT OR IGNORE INTO rfq_lines (rfq_id, line_number, product_id, description, quantity) VALUES (?, 1, ?, ?, 100)',
                       (rfq_ids['RFQ-2024-0001'], product_ids['RAW-AL-2024'], 'Aluminum 2024-T3 Sheet'))
        cursor.execute('INSERT OR IGNORE INTO rfq_lines (rfq_id, line_number, product_id, description, quantity) VALUES (?, 2, ?, ?, 50)',
                       (rfq_ids['RFQ-2024-0001'], product_ids['RAW-TI-6AL4V'], 'Titanium Rod'))
        cursor.execute('INSERT OR IGNORE INTO rfq_suppliers (rfq_id, supplier_id, response_status) VALUES (?, ?, ?)',
                       (rfq_ids['RFQ-2024-0001'], supplier_ids['SUP001'], 'Responded'))
        cursor.execute('INSERT OR IGNORE INTO rfq_suppliers (rfq_id, supplier_id, response_status) VALUES (?, ?, ?)',
                       (rfq_ids['RFQ-2024-0001'], supplier_ids['SUP007'], 'Pending'))
    
    # ============================================
    # 20. INVOICES
    # ============================================
    invoices = [
        ('INV-2024-0001', 'Sales', customer_ids['CUST005'], so_ids.get('SO-2024-0005'), None, today - timedelta(days=5), today + timedelta(days=25), 'Sent', 22000.00, 0, 22000.00),
        ('INV-2024-0002', 'Work Order', customer_ids['CUST001'], None, wo_ids.get('WO-2024-0007'), today - timedelta(days=3), today + timedelta(days=27), 'Draft', 35000.00, 0, 35000.00),
        ('INV-2024-0003', 'Sales', customer_ids['CUST003'], so_ids.get('SO-2024-0003'), None, today - timedelta(days=1), today + timedelta(days=29), 'Draft', 67200.00, 0, 67200.00),
    ]
    for inv in invoices:
        cursor.execute('SELECT id FROM invoices WHERE invoice_number = ?', (inv[0],))
        if not cursor.fetchone():
            cursor.execute('''
                INSERT INTO invoices 
                (invoice_number, invoice_type, customer_id, so_id, wo_id, invoice_date, due_date,
                 status, subtotal, tax_amount, total_amount, balance_due, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (*inv, inv[10], user_ids['mwilson']))
    print(f"  - {len(invoices)} invoices created")
    
    # ============================================
    # 21. SERVICE WORK ORDERS
    # ============================================
    service_wos = [
        ('SWO-2024-0001', 'On-Site Repair', customer_ids['CUST001'], 'Delta hangar hydraulic leak', 'Hydraulic line repair at customer facility', 'In Progress', 'High', labor_ids['EMP001']),
        ('SWO-2024-0002', 'Calibration', customer_ids['CUST007'], 'Annual tool calibration service', 'Calibrate customer measurement equipment', 'Open', 'Medium', labor_ids['EMP003']),
        ('SWO-2024-0003', 'Emergency Repair', customer_ids['CUST003'], 'AOG actuator replacement', 'Aircraft on ground - emergency actuator swap', 'Completed', 'Critical', labor_ids['EMP001']),
    ]
    for swo in service_wos:
        cursor.execute('SELECT id FROM service_work_orders WHERE swo_number = ?', (swo[0],))
        if not cursor.fetchone():
            cursor.execute('''
                INSERT INTO service_work_orders 
                (swo_number, service_type, customer_id, equipment_description, description, status, priority, assigned_to, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (*swo, user_ids['admin']))
    print(f"  - {len(service_wos)} service work orders created")
    
    # ============================================
    # 22. CHART OF ACCOUNTS
    # ============================================
    accounts = [
        ('1000', 'Cash', 'Asset', None),
        ('1100', 'Accounts Receivable', 'Asset', None),
        ('1200', 'Inventory', 'Asset', None),
        ('1500', 'Fixed Assets', 'Asset', None),
        ('2000', 'Accounts Payable', 'Liability', None),
        ('2100', 'Accrued Expenses', 'Liability', None),
        ('3000', 'Retained Earnings', 'Equity', None),
        ('4000', 'Sales Revenue', 'Revenue', None),
        ('4100', 'Service Revenue', 'Revenue', None),
        ('5000', 'Cost of Goods Sold', 'Expense', None),
        ('5100', 'Direct Labor', 'Expense', None),
        ('5200', 'Materials', 'Expense', None),
        ('6000', 'Operating Expenses', 'Expense', None),
    ]
    for account in accounts:
        cursor.execute('SELECT id FROM chart_of_accounts WHERE account_code = ?', (account[0],))
        if not cursor.fetchone():
            cursor.execute('''
                INSERT INTO chart_of_accounts (account_code, account_name, account_type, parent_account_id, is_active)
                VALUES (?, ?, ?, ?, 1)
            ''', account)
    print(f"  - {len(accounts)} chart of accounts created")
    
    # ============================================
    # 23. SLA CONFIGURATIONS
    # ============================================
    slas = [
        ('Standard SLA', 'Standard Sale', None, 24, 72, 48),
        ('Premium SLA', None, 'Premium', 4, 24, 12),
        ('AOG SLA', 'Emergency', None, 1, 8, 4),
    ]
    for sla in slas:
        cursor.execute('SELECT id FROM sla_configurations WHERE sla_name = ?', (sla[0],))
        if not cursor.fetchone():
            cursor.execute('''
                INSERT INTO sla_configurations 
                (sla_name, order_type, customer_tier, response_time_hours, resolution_time_hours, escalation_time_hours, is_active, created_by)
                VALUES (?, ?, ?, ?, ?, ?, 1, ?)
            ''', (*sla, user_ids['admin']))
    print(f"  - SLA configurations created")
    
    # ============================================
    # 24. TASK TEMPLATES
    # ============================================
    templates = [
        ('TPL-HYD-001', 'Hydraulic Actuator Overhaul', 'Standard task template for hydraulic actuator overhaul', 'Hydraulics'),
        ('TPL-LDG-001', 'Landing Gear Inspection', 'Landing gear component inspection template', 'Landing Gear'),
    ]
    template_ids = {}
    for tpl in templates:
        cursor.execute('SELECT id FROM task_templates WHERE template_code = ?', (tpl[0],))
        existing = cursor.fetchone()
        if not existing:
            cursor.execute('''
                INSERT INTO task_templates (template_code, template_name, description, category, status, created_by)
                VALUES (?, ?, ?, ?, 'Active', ?)
            ''', (*tpl, user_ids['jsmith']))
            template_ids[tpl[0]] = cursor.lastrowid
        else:
            template_ids[tpl[0]] = existing['id']
    
    # Template items
    if 'TPL-HYD-001' in template_ids:
        template_items = [
            (template_ids['TPL-HYD-001'], 'Incoming Inspection', 'Visual inspection and documentation', 1, 2.0),
            (template_ids['TPL-HYD-001'], 'Disassembly', 'Complete disassembly per CMM', 2, 4.0),
            (template_ids['TPL-HYD-001'], 'Cleaning', 'Clean all components', 3, 2.0),
            (template_ids['TPL-HYD-001'], 'NDT Inspection', 'Perform required NDT', 4, 3.0),
            (template_ids['TPL-HYD-001'], 'Parts Replacement', 'Replace worn components', 5, 4.0),
            (template_ids['TPL-HYD-001'], 'Reassembly', 'Reassemble per CMM', 6, 4.0),
            (template_ids['TPL-HYD-001'], 'Functional Test', 'Perform functional test', 7, 2.0),
        ]
        for item in template_items:
            cursor.execute('''
                INSERT OR IGNORE INTO task_template_items 
                (template_id, task_name, description, sequence_number, planned_hours)
                VALUES (?, ?, ?, ?, ?)
            ''', item)
    print(f"  - Task templates created")
    
    # ============================================
    # 25. SHIPMENTS
    # ============================================
    shipments = [
        ('SHP-2024-0001', 'Outbound', 'Sales Order', so_ids.get('SO-2024-0005'), 'Shipped', 'FedEx', '794644790123', today - timedelta(days=2), 45.5),
        ('SHP-2024-0002', 'Outbound', 'Work Order', wo_ids.get('WO-2024-0007'), 'Delivered', 'UPS', '1Z999AA10123456784', today - timedelta(days=5), 120.0),
    ]
    for ship in shipments:
        cursor.execute('SELECT id FROM shipments WHERE shipment_number = ?', (ship[0],))
        if not cursor.fetchone():
            cursor.execute('''
                INSERT INTO shipments 
                (shipment_number, shipment_type, reference_type, reference_id, status, 
                 carrier, tracking_number, ship_date, weight, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (*ship, user_ids['admin']))
    print(f"  - Shipments created")
    
    # Commit all changes
    conn.commit()
    conn.close()
    
    print("\n" + "="*50)
    print("Sample data seeding completed successfully!")
    print("="*50)
    print("\nSummary:")
    print("  - 10 Customers with portal access enabled")
    print("  - 10 Suppliers with contacts")
    print("  - 10 MRO Capabilities")
    print("  - 15 Products with BOMs")
    print("  - 8 Work Orders with tasks")
    print("  - 6 Sales Orders with lines")
    print("  - 5 Purchase Orders with lines")
    print("  - 3 RFQs with suppliers")
    print("  - 3 Invoices")
    print("  - 3 NDT Work Orders")
    print("  - 3 Service Work Orders")
    print("  - 5 Work Centers")
    print("  - 8 Labor Resources with skills")
    print("  - 5 Tools with calibration tracking")
    print("  - Chart of Accounts")
    print("  - SLA Configurations")
    print("  - And more...")
    print("\nLogin credentials:")
    print("  - Admin: admin / admin123")
    print("  - Planner: jsmith / planner123")
    print("  - Sales: mwilson / sales123")
    print("  - Tech: tgarcia / tech123")
    print("  - CS: lchen / cs123")

if __name__ == '__main__':
    seed_data()
