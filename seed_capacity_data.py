#!/usr/bin/env python3
"""
Seed Capacity Planning Sample Data for Dynamic.IQ-COREx
Creates comprehensive capacity data to demonstrate strategic capacity planning features:
- Skillsets with required levels, target headcounts, and criticality
- Work Centers with capacity settings
- Labor Resources with skill assignments at various proficiency levels
- Work Center capacity overrides
- Work Order assignments to show utilization
"""

import sqlite3
from datetime import datetime, timedelta
import random

def seed_capacity_data():
    conn = sqlite3.connect('mrp.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    print("=" * 60)
    print("SEEDING CAPACITY PLANNING DATA")
    print("=" * 60)
    
    # ============================================
    # 1. SKILLSETS WITH CAPACITY PLANNING FIELDS
    # ============================================
    print("\n1. Creating Skillsets with Capacity Planning...")
    
    skillsets = [
        # (name, description, category, status, required_level, target_headcount, criticality)
        ('Ultrasonic Testing (UT)', 'Non-destructive testing using ultrasonic waves to detect internal flaws', 'NDT', 'Active', 'Advanced', 4, 'Critical'),
        ('Magnetic Particle Testing (MT)', 'NDT method for detecting surface and near-surface discontinuities', 'NDT', 'Active', 'Intermediate', 3, 'High'),
        ('Liquid Penetrant Testing (PT)', 'NDT method for detecting surface-breaking defects', 'NDT', 'Active', 'Intermediate', 3, 'Medium'),
        ('Radiographic Testing (RT)', 'X-ray and gamma-ray inspection of components', 'NDT', 'Active', 'Expert', 2, 'Critical'),
        ('Visual Inspection (VT)', 'Visual examination of components and assemblies', 'NDT', 'Active', 'Apprentice', 6, 'Medium'),
        ('Hydraulic Systems', 'Maintenance and repair of aircraft hydraulic systems', 'Technical', 'Active', 'Advanced', 5, 'Critical'),
        ('Pneumatic Systems', 'Maintenance of pneumatic and bleed air systems', 'Technical', 'Active', 'Intermediate', 4, 'High'),
        ('Avionics Systems', 'Electronic flight instrument and navigation systems', 'Technical', 'Active', 'Advanced', 4, 'Critical'),
        ('Composite Repair', 'Repair of carbon fiber and composite structures', 'Technical', 'Active', 'Expert', 3, 'High'),
        ('Engine Overhaul', 'Complete engine disassembly, inspection, and reassembly', 'Technical', 'Active', 'Expert', 2, 'Critical'),
        ('Landing Gear Overhaul', 'Complete landing gear system overhaul', 'Technical', 'Active', 'Advanced', 3, 'Critical'),
        ('Fuel System Repair', 'Fuel tank sealing, component replacement', 'Technical', 'Active', 'Advanced', 3, 'High'),
        ('Sheet Metal Repair', 'Structural sheet metal repairs and fabrication', 'Technical', 'Active', 'Intermediate', 5, 'Medium'),
        ('Welding - TIG', 'Precision TIG welding for aerospace applications', 'Fabrication', 'Active', 'Advanced', 2, 'High'),
        ('Welding - Electron Beam', 'Electron beam welding for critical components', 'Fabrication', 'Active', 'Expert', 1, 'Critical'),
        ('CNC Machining', 'Computer numerical control machining operations', 'Fabrication', 'Active', 'Intermediate', 4, 'Medium'),
        ('Manual Machining', 'Lathe, mill, and grinder operations', 'Fabrication', 'Active', 'Apprentice', 3, 'Low'),
        ('Quality Assurance', 'Inspection and quality control procedures', 'Quality', 'Active', 'Advanced', 4, 'High'),
        ('CMM Operation', 'Coordinate measuring machine programming and operation', 'Quality', 'Active', 'Advanced', 2, 'High'),
        ('Documentation & Records', 'Maintenance record keeping and compliance documentation', 'Administrative', 'Active', 'Intermediate', 3, 'Medium'),
        ('Hazmat Handling', 'Hazardous materials handling and disposal', 'Safety', 'Active', 'Intermediate', 4, 'High'),
        ('Confined Space Entry', 'Safe work in confined spaces (fuel tanks, etc.)', 'Safety', 'Active', 'Intermediate', 5, 'High'),
        ('Fall Protection', 'Working at heights and fall protection systems', 'Safety', 'Active', 'Apprentice', 8, 'Medium'),
        ('Forklift Operation', 'Industrial forklift and material handling', 'Safety', 'Active', 'Apprentice', 4, 'Low'),
    ]
    
    skillset_ids = {}
    for skillset in skillsets:
        name, description, category, status, required_level, target_headcount, criticality = skillset
        cursor.execute('SELECT id FROM skillsets WHERE skillset_name = ?', (name,))
        existing = cursor.fetchone()
        if existing:
            cursor.execute('''
                UPDATE skillsets 
                SET description = ?, category = ?, status = ?, 
                    required_level = ?, target_headcount = ?, criticality = ?
                WHERE id = ?
            ''', (description, category, status, required_level, target_headcount, criticality, existing['id']))
            skillset_ids[name] = existing['id']
        else:
            cursor.execute('''
                INSERT INTO skillsets (skillset_name, description, category, status, 
                                       required_level, target_headcount, criticality)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', skillset)
            skillset_ids[name] = cursor.lastrowid
    
    print(f"   Created/Updated {len(skillsets)} skillsets with capacity planning fields")
    
    # ============================================
    # 2. WORK CENTERS
    # ============================================
    print("\n2. Creating Work Centers...")
    
    work_centers = [
        # (code, name, description, hours_per_day, days_per_week, efficiency, cost_per_hour)
        ('WC-HYD', 'Hydraulics Bay', 'Hydraulic component overhaul and testing facility', 10.0, 5, 0.85, 125.00),
        ('WC-AVI', 'Avionics Shop', 'Electronic systems repair and calibration', 8.0, 5, 0.90, 150.00),
        ('WC-NDT', 'NDT Laboratory', 'Non-destructive testing and inspection center', 10.0, 6, 0.95, 175.00),
        ('WC-ASM', 'Assembly Bay', 'Component assembly and sub-assembly area', 10.0, 5, 0.80, 95.00),
        ('WC-TST', 'Test Cell', 'Component and system testing facility', 8.0, 5, 0.85, 200.00),
        ('WC-MCH', 'Machine Shop', 'CNC and manual machining operations', 10.0, 5, 0.88, 110.00),
        ('WC-WLD', 'Welding Shop', 'Specialized aerospace welding operations', 8.0, 5, 0.82, 135.00),
        ('WC-CMP', 'Composites Bay', 'Composite structure repair and layup', 10.0, 5, 0.75, 145.00),
        ('WC-ENG', 'Engine Shop', 'Engine overhaul and repair facility', 10.0, 6, 0.80, 225.00),
        ('WC-LDG', 'Landing Gear Bay', 'Landing gear overhaul facility', 10.0, 5, 0.85, 160.00),
        ('WC-PNT', 'Paint Booth', 'Painting and surface treatment facility', 8.0, 5, 0.70, 85.00),
        ('WC-CLN', 'Cleaning Station', 'Parts cleaning and preparation', 10.0, 6, 0.90, 65.00),
    ]
    
    work_center_ids = {}
    for wc in work_centers:
        code, name, description, hours_per_day, days_per_week, efficiency, cost_per_hour = wc
        cursor.execute('SELECT id FROM work_centers WHERE code = ?', (code,))
        existing = cursor.fetchone()
        if existing:
            cursor.execute('''
                UPDATE work_centers 
                SET name = ?, description = ?, default_hours_per_day = ?, 
                    default_days_per_week = ?, efficiency_factor = ?, cost_per_hour = ?, status = 'Active'
                WHERE id = ?
            ''', (name, description, hours_per_day, days_per_week, efficiency, cost_per_hour, existing['id']))
            work_center_ids[code] = existing['id']
        else:
            cursor.execute('''
                INSERT INTO work_centers (code, name, description, default_hours_per_day, 
                                          default_days_per_week, efficiency_factor, cost_per_hour, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'Active')
            ''', wc)
            work_center_ids[code] = cursor.lastrowid
    
    print(f"   Created/Updated {len(work_centers)} work centers")
    
    # ============================================
    # 3. LABOR RESOURCES WITH SKILL ASSIGNMENTS
    # ============================================
    print("\n3. Creating Labor Resources with Skill Assignments...")
    
    def get_employee_code(cursor):
        cursor.execute('SELECT MAX(id) as max_id FROM labor_resources')
        result = cursor.fetchone()
        next_id = (result['max_id'] or 0) + 1
        return f'EMP-{next_id:06d}'
    
    labor_resources = [
        # (first_name, last_name, role, hourly_rate, cost_center, email, status, skills)
        # skills = list of (skillset_name, skill_level)
        ('Marcus', 'Thompson', 'Senior NDT Technician', 85.00, 'NDT-001', 'mthompson@pa-mro.com', 'Active', [
            ('Ultrasonic Testing (UT)', 'Expert'),
            ('Magnetic Particle Testing (MT)', 'Expert'),
            ('Liquid Penetrant Testing (PT)', 'Advanced'),
            ('Radiographic Testing (RT)', 'Advanced'),
            ('Visual Inspection (VT)', 'Expert'),
            ('Quality Assurance', 'Advanced'),
        ]),
        ('Jennifer', 'Chen', 'NDT Technician II', 65.00, 'NDT-001', 'jchen@pa-mro.com', 'Active', [
            ('Ultrasonic Testing (UT)', 'Advanced'),
            ('Magnetic Particle Testing (MT)', 'Advanced'),
            ('Liquid Penetrant Testing (PT)', 'Intermediate'),
            ('Visual Inspection (VT)', 'Advanced'),
        ]),
        ('David', 'Williams', 'NDT Technician I', 48.00, 'NDT-001', 'dwilliams@pa-mro.com', 'Active', [
            ('Magnetic Particle Testing (MT)', 'Intermediate'),
            ('Liquid Penetrant Testing (PT)', 'Intermediate'),
            ('Visual Inspection (VT)', 'Intermediate'),
        ]),
        ('Robert', 'Garcia', 'Lead Hydraulics Tech', 78.00, 'HYD-001', 'rgarcia@pa-mro.com', 'Active', [
            ('Hydraulic Systems', 'Expert'),
            ('Pneumatic Systems', 'Advanced'),
            ('Quality Assurance', 'Intermediate'),
            ('Hazmat Handling', 'Advanced'),
        ]),
        ('Sarah', 'Martinez', 'Hydraulics Technician', 58.00, 'HYD-001', 'smartinez@pa-mro.com', 'Active', [
            ('Hydraulic Systems', 'Advanced'),
            ('Pneumatic Systems', 'Intermediate'),
            ('Fuel System Repair', 'Intermediate'),
        ]),
        ('Michael', 'Johnson', 'Avionics Lead', 82.00, 'AVI-001', 'mjohnson@pa-mro.com', 'Active', [
            ('Avionics Systems', 'Expert'),
            ('Quality Assurance', 'Advanced'),
            ('Documentation & Records', 'Advanced'),
        ]),
        ('Emily', 'Brown', 'Avionics Technician', 62.00, 'AVI-001', 'ebrown@pa-mro.com', 'Active', [
            ('Avionics Systems', 'Advanced'),
            ('Documentation & Records', 'Intermediate'),
        ]),
        ('James', 'Wilson', 'Senior Machinist', 72.00, 'MCH-001', 'jwilson@pa-mro.com', 'Active', [
            ('CNC Machining', 'Expert'),
            ('Manual Machining', 'Expert'),
            ('CMM Operation', 'Advanced'),
            ('Quality Assurance', 'Intermediate'),
        ]),
        ('Lisa', 'Anderson', 'Machinist', 52.00, 'MCH-001', 'landerson@pa-mro.com', 'Active', [
            ('CNC Machining', 'Intermediate'),
            ('Manual Machining', 'Advanced'),
        ]),
        ('Christopher', 'Taylor', 'Apprentice Machinist', 35.00, 'MCH-001', 'ctaylor@pa-mro.com', 'Active', [
            ('Manual Machining', 'Apprentice'),
            ('CNC Machining', 'Apprentice'),
        ]),
        ('Amanda', 'Thomas', 'Lead Welder', 75.00, 'WLD-001', 'athomas@pa-mro.com', 'Active', [
            ('Welding - TIG', 'Expert'),
            ('Welding - Electron Beam', 'Advanced'),
            ('Sheet Metal Repair', 'Advanced'),
        ]),
        ('Daniel', 'Jackson', 'Welder', 55.00, 'WLD-001', 'djackson@pa-mro.com', 'Active', [
            ('Welding - TIG', 'Advanced'),
            ('Sheet Metal Repair', 'Intermediate'),
        ]),
        ('Patricia', 'White', 'Composites Specialist', 70.00, 'CMP-001', 'pwhite@pa-mro.com', 'Active', [
            ('Composite Repair', 'Expert'),
            ('Quality Assurance', 'Advanced'),
            ('Hazmat Handling', 'Intermediate'),
        ]),
        ('Kevin', 'Harris', 'Composites Technician', 50.00, 'CMP-001', 'kharris@pa-mro.com', 'Active', [
            ('Composite Repair', 'Intermediate'),
            ('Sheet Metal Repair', 'Intermediate'),
        ]),
        ('Jessica', 'Martin', 'Engine Overhaul Lead', 95.00, 'ENG-001', 'jmartin@pa-mro.com', 'Active', [
            ('Engine Overhaul', 'Expert'),
            ('Quality Assurance', 'Expert'),
            ('Hazmat Handling', 'Advanced'),
            ('Confined Space Entry', 'Advanced'),
        ]),
        ('William', 'Lee', 'Engine Technician', 68.00, 'ENG-001', 'wlee@pa-mro.com', 'Active', [
            ('Engine Overhaul', 'Advanced'),
            ('Fuel System Repair', 'Advanced'),
            ('Hazmat Handling', 'Intermediate'),
        ]),
        ('Nancy', 'Robinson', 'Landing Gear Lead', 80.00, 'LDG-001', 'nrobinson@pa-mro.com', 'Active', [
            ('Landing Gear Overhaul', 'Expert'),
            ('Hydraulic Systems', 'Advanced'),
            ('Quality Assurance', 'Advanced'),
        ]),
        ('Steven', 'Clark', 'Landing Gear Tech', 58.00, 'LDG-001', 'sclark@pa-mro.com', 'Active', [
            ('Landing Gear Overhaul', 'Advanced'),
            ('Hydraulic Systems', 'Intermediate'),
        ]),
        ('Michelle', 'Lewis', 'QA Inspector', 65.00, 'QA-001', 'mlewis@pa-mro.com', 'Active', [
            ('Quality Assurance', 'Expert'),
            ('CMM Operation', 'Expert'),
            ('Visual Inspection (VT)', 'Advanced'),
            ('Documentation & Records', 'Advanced'),
        ]),
        ('Andrew', 'Walker', 'Assembly Technician', 48.00, 'ASM-001', 'awalker@pa-mro.com', 'Active', [
            ('Sheet Metal Repair', 'Intermediate'),
            ('Visual Inspection (VT)', 'Intermediate'),
            ('Fall Protection', 'Intermediate'),
            ('Forklift Operation', 'Advanced'),
        ]),
        ('Rebecca', 'Hall', 'Assembly Technician', 45.00, 'ASM-001', 'rhall@pa-mro.com', 'Active', [
            ('Sheet Metal Repair', 'Apprentice'),
            ('Visual Inspection (VT)', 'Apprentice'),
            ('Fall Protection', 'Intermediate'),
        ]),
        ('Brian', 'Allen', 'Material Handler', 38.00, 'MAT-001', 'ballen@pa-mro.com', 'Active', [
            ('Forklift Operation', 'Expert'),
            ('Hazmat Handling', 'Intermediate'),
            ('Fall Protection', 'Intermediate'),
        ]),
    ]
    
    labor_ids = {}
    for lr in labor_resources:
        first_name, last_name, role, hourly_rate, cost_center, email, status, skills = lr
        
        cursor.execute('SELECT id FROM labor_resources WHERE email = ?', (email,))
        existing = cursor.fetchone()
        
        if existing:
            labor_id = existing['id']
            cursor.execute('''
                UPDATE labor_resources 
                SET first_name = ?, last_name = ?, role = ?, hourly_rate = ?, 
                    cost_center = ?, status = ?
                WHERE id = ?
            ''', (first_name, last_name, role, hourly_rate, cost_center, status, labor_id))
        else:
            employee_code = get_employee_code(cursor)
            cursor.execute('''
                INSERT INTO labor_resources (employee_code, first_name, last_name, role, 
                                             hourly_rate, cost_center, email, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (employee_code, first_name, last_name, role, hourly_rate, cost_center, email, status))
            labor_id = cursor.lastrowid
        
        labor_ids[email] = labor_id
        
        cursor.execute('DELETE FROM labor_resource_skills WHERE labor_resource_id = ?', (labor_id,))
        for skillset_name, skill_level in skills:
            if skillset_name in skillset_ids:
                cursor.execute('''
                    INSERT INTO labor_resource_skills (labor_resource_id, skillset_id, skill_level, certified)
                    VALUES (?, ?, ?, 1)
                ''', (labor_id, skillset_ids[skillset_name], skill_level))
    
    print(f"   Created/Updated {len(labor_resources)} labor resources with skill assignments")
    
    # ============================================
    # 4. WORK CENTER CAPACITY OVERRIDES
    # ============================================
    print("\n4. Creating Work Center Capacity Overrides...")
    
    today = datetime.now().date()
    capacity_overrides = []
    
    for i in range(30):
        date = today + timedelta(days=i)
        day_of_week = date.weekday()
        
        if day_of_week == 5:
            capacity_overrides.append((work_center_ids['WC-NDT'], date, 6.0, 'Saturday reduced shift'))
            capacity_overrides.append((work_center_ids['WC-ENG'], date, 6.0, 'Saturday reduced shift'))
        elif day_of_week == 6:
            pass
        else:
            if i == 10:
                capacity_overrides.append((work_center_ids['WC-HYD'], date, 0.0, 'Scheduled maintenance'))
            if i == 15:
                capacity_overrides.append((work_center_ids['WC-MCH'], date, 4.0, 'Equipment calibration'))
            if i == 20:
                capacity_overrides.append((work_center_ids['WC-TST'], date, 12.0, 'Extended shift - AOG support'))
    
    for override in capacity_overrides:
        wc_id, cap_date, hours, reason = override
        cursor.execute('SELECT id FROM work_center_capacity WHERE work_center_id = ? AND capacity_date = ?', 
                      (wc_id, cap_date.strftime('%Y-%m-%d')))
        existing = cursor.fetchone()
        if existing:
            cursor.execute('''
                UPDATE work_center_capacity SET available_hours = ?, override_reason = ? WHERE id = ?
            ''', (hours, reason, existing['id']))
        else:
            cursor.execute('''
                INSERT INTO work_center_capacity (work_center_id, capacity_date, available_hours, override_reason)
                VALUES (?, ?, ?, ?)
            ''', (wc_id, cap_date.strftime('%Y-%m-%d'), hours, reason))
    
    print(f"   Created {len(capacity_overrides)} capacity overrides")
    
    # ============================================
    # 5. WORK ORDER OPERATIONS FOR CAPACITY LOAD
    # ============================================
    print("\n5. Creating Work Order Operations for Capacity Load...")
    
    cursor.execute('SELECT id FROM work_orders LIMIT 10')
    existing_wos = cursor.fetchall()
    wo_ids = [wo['id'] for wo in existing_wos] if existing_wos else []
    
    operations = [
        # (operation_seq, work_center_code, operation_name, planned_hours, status)
        (10, 'WC-HYD', 'Disassembly and Cleaning', 8.0, 'In Progress'),
        (20, 'WC-NDT', 'NDT Inspection', 4.0, 'Pending'),
        (30, 'WC-MCH', 'Component Machining', 12.0, 'Pending'),
        (40, 'WC-WLD', 'Welding Repair', 6.0, 'Pending'),
        (50, 'WC-CMP', 'Composite Layup', 16.0, 'In Progress'),
        (60, 'WC-ASM', 'Sub-Assembly', 8.0, 'Pending'),
        (70, 'WC-TST', 'Functional Testing', 4.0, 'Pending'),
        (80, 'WC-AVI', 'Avionics Integration', 10.0, 'In Progress'),
        (90, 'WC-LDG', 'Landing Gear Work', 20.0, 'In Progress'),
        (100, 'WC-ENG', 'Engine Assembly', 32.0, 'Pending'),
    ]
    
    ops_created = 0
    for i, wo_id in enumerate(wo_ids):
        op = operations[i % len(operations)]
        op_seq, wc_code, op_name, planned_hours, status = op
        wc_id = work_center_ids.get(wc_code)
        
        if wc_id:
            start_date = (today + timedelta(days=random.randint(1, 14))).strftime('%Y-%m-%d')
            end_date = (today + timedelta(days=random.randint(15, 28))).strftime('%Y-%m-%d')
            
            cursor.execute('''
                SELECT id FROM work_order_operations 
                WHERE work_order_id = ? AND operation_seq = ?
            ''', (wo_id, op_seq))
            existing = cursor.fetchone()
            
            if not existing:
                cursor.execute('''
                    INSERT INTO work_order_operations 
                    (work_order_id, operation_seq, work_center_id, operation_name, 
                     planned_hours, planned_start_date, planned_end_date, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (wo_id, op_seq, wc_id, op_name, planned_hours, start_date, end_date, status))
                ops_created += 1
    
    cursor.execute('''
        SELECT wc.code, COUNT(woo.id) as op_count, COALESCE(SUM(woo.planned_hours), 0) as total_hours
        FROM work_centers wc
        LEFT JOIN work_order_operations woo ON wc.id = woo.work_center_id
        GROUP BY wc.id
        ORDER BY total_hours DESC
    ''')
    wc_load = cursor.fetchall()
    
    print(f"   Created {ops_created} work order operations")
    print("   Work Center Load Summary:")
    for wc in wc_load[:5]:
        print(f"     - {wc['code']}: {wc['op_count']} ops, {wc['total_hours']:.1f} planned hours")
    
    # ============================================
    # COMMIT AND SUMMARY
    # ============================================
    conn.commit()
    conn.close()
    
    print("\n" + "=" * 60)
    print("CAPACITY DATA SEED COMPLETE!")
    print("=" * 60)
    print(f"""
Summary:
  - {len(skillsets)} Skillsets with capacity planning settings
    * Various required levels (Apprentice to Expert)
    * Target headcounts ranging from 1-8
    * Criticality levels (Low to Critical)
    
  - {len(work_centers)} Work Centers with capacity settings
    * Different operating hours (8-10 hrs/day)
    * Efficiency factors (70-95%)
    * Hourly cost rates ($65-$225/hr)
    
  - {len(labor_resources)} Labor Resources with skill assignments
    * Multiple skills per worker (2-6 skills each)
    * Various proficiency levels
    * Realistic role/rate combinations
    
  - {len(capacity_overrides)} Capacity overrides for scheduling
    * Weekend reduced shifts
    * Maintenance downtime
    * Extended shift scenarios
    
  - {ops_created} Work Order Operations for capacity load
    * Distributed across work centers
    * Various planned hours (4-32 hrs)
    * Multiple statuses for utilization display

To view capacity data:
  1. Login as admin (admin/admin123)
  2. Navigate to Operations > Capacity Planning
  3. View Work Centers for capacity settings
  4. View Labor > Skillsets for skill capacity
  5. View Labor > Labor Resources for skill matrix
""")

if __name__ == '__main__':
    seed_capacity_data()
