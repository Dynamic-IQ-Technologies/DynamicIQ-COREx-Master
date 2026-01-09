#!/usr/bin/env python3
"""
PostgreSQL Bootstrap Script for Dynamic.IQ-COREx Production Deployment

This script verifies the PostgreSQL database is accessible and has the required schema.
Run this during the build step before starting the application.

Usage:
    python scripts/postgres_bootstrap.py
"""

import os
import sys

DATABASE_URL = os.environ.get('DATABASE_URL')

if not DATABASE_URL:
    print("ERROR: DATABASE_URL environment variable not set")
    print("Please ensure PostgreSQL database is provisioned")
    sys.exit(1)

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("ERROR: psycopg2 not installed")
    sys.exit(1)

def verify_database():
    """Verify database connection and schema"""
    print("=" * 60)
    print("Dynamic.IQ-COREx PostgreSQL Bootstrap")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        print("[OK] Database connection successful")
    except Exception as e:
        print(f"[ERROR] Database connection failed: {e}")
        sys.exit(1)
    
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
        print(f"[WARNING] Missing core tables: {missing_tables}")
        print("Run 'python scripts/init_postgres.py' to initialize schema")
    else:
        print(f"[OK] All {len(required_tables)} core tables verified")
        print(f"[OK] Total tables in database: {len(tables)}")
    
    cursor.execute("SELECT COUNT(*) as count FROM users")
    user_count = cursor.fetchone()['count']
    print(f"[OK] Users in database: {user_count}")
    
    conn.close()
    print("=" * 60)
    print("Bootstrap complete - database ready for deployment")
    print("=" * 60)
    
    return len(missing_tables) == 0

if __name__ == "__main__":
    success = verify_database()
    sys.exit(0 if success else 1)
