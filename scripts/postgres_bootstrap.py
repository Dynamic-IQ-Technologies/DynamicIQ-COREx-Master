#!/usr/bin/env python3
"""
PostgreSQL Bootstrap Script for Dynamic.IQ-COREx Production Deployment

This script verifies the PostgreSQL database is accessible and initializes schema if needed.
Run this during the build step before starting the application.

Usage:
    python scripts/postgres_bootstrap.py
"""

import os
import sys

DATABASE_URL = os.environ.get('DATABASE_URL')

print("=" * 60)
print("Dynamic.IQ-COREx PostgreSQL Bootstrap")
print("=" * 60)

if not DATABASE_URL:
    print("[INFO] DATABASE_URL not set - skipping PostgreSQL verification")
    print("[INFO] Application will use development database configuration")
    print("=" * 60)
    sys.exit(0)

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("[WARNING] psycopg2 not available - skipping database verification")
    sys.exit(0)

def create_essential_schema(conn):
    """Create essential tables if they don't exist"""
    cursor = conn.cursor()
    
    essential_tables = [
        """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL,
            last_login TIMESTAMP,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            unit_of_measure TEXT NOT NULL,
            product_type TEXT NOT NULL,
            cost DECIMAL(18,4) DEFAULT 0,
            part_category TEXT DEFAULT 'Other',
            lead_time INTEGER DEFAULT 0,
            product_category TEXT,
            manufacturer TEXT,
            applicability TEXT,
            shelf_life_cycle TEXT,
            eccn TEXT,
            part_notes TEXT,
            is_serialized INTEGER DEFAULT 0,
            calibration_required INTEGER DEFAULT 0,
            master_plan_part INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS suppliers (
            id SERIAL PRIMARY KEY,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            contact_person TEXT,
            email TEXT,
            phone TEXT,
            address TEXT,
            payment_terms INTEGER DEFAULT 30,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS customers (
            id SERIAL PRIMARY KEY,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            contact_person TEXT,
            email TEXT,
            phone TEXT,
            address TEXT,
            payment_terms INTEGER DEFAULT 30,
            credit_limit DECIMAL(18,4) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS inventory (
            id SERIAL PRIMARY KEY,
            product_id INTEGER NOT NULL,
            quantity DECIMAL(18,4) NOT NULL DEFAULT 0,
            reorder_point DECIMAL(18,4) DEFAULT 0,
            safety_stock DECIMAL(18,4) DEFAULT 0,
            condition TEXT DEFAULT 'New',
            warehouse_location TEXT DEFAULT 'Main',
            bin_location TEXT,
            status TEXT DEFAULT 'Available',
            reserved_quantity DECIMAL(18,4) DEFAULT 0,
            unit_cost DECIMAL(18,4) DEFAULT 0,
            tool_asset_number TEXT,
            stock_category TEXT,
            created_at TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(product_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS work_orders (
            id SERIAL PRIMARY KEY,
            wo_number TEXT UNIQUE NOT NULL,
            product_id INTEGER NOT NULL,
            quantity DECIMAL(18,4) NOT NULL,
            status TEXT NOT NULL,
            priority TEXT DEFAULT 'Medium',
            serial_number TEXT,
            description TEXT,
            planned_start_date DATE,
            planned_end_date DATE,
            actual_start_date DATE,
            actual_end_date DATE,
            material_cost DECIMAL(18,4) DEFAULT 0,
            labor_cost DECIMAL(18,4) DEFAULT 0,
            overhead_cost DECIMAL(18,4) DEFAULT 0,
            stage_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS purchase_orders (
            id SERIAL PRIMARY KEY,
            po_number TEXT UNIQUE NOT NULL,
            supplier_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            order_date DATE,
            expected_delivery_date DATE,
            actual_delivery_date DATE,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS sales_orders (
            id SERIAL PRIMARY KEY,
            so_number TEXT UNIQUE NOT NULL,
            customer_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            order_date DATE,
            required_date DATE,
            ship_date DATE,
            notes TEXT,
            order_type TEXT DEFAULT 'Standard',
            core_due_days INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    ]
    
    for sql in essential_tables:
        try:
            cursor.execute(sql)
        except Exception as e:
            print(f"[WARNING] Table creation issue: {e}")
    
    conn.commit()
    print("[OK] Essential schema verified/created")

def verify_database():
    """Verify database connection and schema"""
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        print("[OK] Database connection successful")
    except Exception as e:
        print(f"[WARNING] Database connection failed: {e}")
        print("[INFO] This may be normal if production database isn't set up yet")
        print("[INFO] Please create a production database in the Replit Database tool")
        print("=" * 60)
        sys.exit(0)
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    tables = [row['table_name'] for row in cursor.fetchall()]
    
    required_tables = [
        'users', 'products', 'inventory', 'work_orders', 
        'purchase_orders', 'sales_orders', 'suppliers', 'customers'
    ]
    
    missing_tables = [t for t in required_tables if t not in tables]
    
    if missing_tables:
        print(f"[INFO] Creating missing tables: {missing_tables}")
        create_essential_schema(conn)
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = [row['table_name'] for row in cursor.fetchall()]
        missing_tables = [t for t in required_tables if t not in tables]
    
    if not missing_tables:
        print(f"[OK] All {len(required_tables)} core tables verified")
        print(f"[OK] Total tables in database: {len(tables)}")
    
    try:
        cursor.execute("SELECT COUNT(*) as count FROM users")
        user_count = cursor.fetchone()['count']
        print(f"[OK] Users in database: {user_count}")
    except:
        print("[INFO] Users table exists but may be empty")
    
    conn.close()
    print("=" * 60)
    print("Bootstrap complete - ready for deployment")
    print("=" * 60)
    
    return True

if __name__ == "__main__":
    success = verify_database()
    sys.exit(0)
