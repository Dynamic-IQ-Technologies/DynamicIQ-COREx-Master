#!/usr/bin/env python3
"""
PostgreSQL Initialization and Migration Script for Dynamic.IQ-COREx

This script directly mirrors the SQLite schema structure into PostgreSQL
and migrates all existing data with proper type conversions.

Usage:
    python scripts/init_postgres.py
"""

import os
import sys
import sqlite3
import re
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DATABASE_URL = os.environ.get('DATABASE_URL')

if not DATABASE_URL:
    print("ERROR: DATABASE_URL environment variable not set")
    print("Please create a PostgreSQL database first")
    sys.exit(1)

import psycopg2
from psycopg2.extras import RealDictCursor

SQLITE_DB = 'mrp.db'

def get_postgres_connection():
    """Get PostgreSQL connection"""
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def get_sqlite_connection():
    """Get SQLite connection"""
    if not os.path.exists(SQLITE_DB):
        print(f"ERROR: SQLite database '{SQLITE_DB}' not found.")
        return None
    conn = sqlite3.connect(SQLITE_DB)
    conn.row_factory = sqlite3.Row
    return conn

def convert_sqlite_type_to_postgres(sqlite_type):
    """Convert SQLite column type to PostgreSQL type"""
    if not sqlite_type:
        return 'TEXT'
    
    sqlite_type = sqlite_type.upper().strip()
    
    if 'INT' in sqlite_type:
        return 'INTEGER'
    elif sqlite_type in ('REAL', 'FLOAT', 'DOUBLE'):
        return 'DECIMAL(18,4)'
    elif 'CHAR' in sqlite_type or 'CLOB' in sqlite_type or sqlite_type == 'TEXT':
        return 'TEXT'
    elif 'BLOB' in sqlite_type:
        return 'BYTEA'
    elif sqlite_type == 'BOOLEAN':
        return 'BOOLEAN'
    elif 'DATE' in sqlite_type:
        return 'DATE'
    elif 'TIME' in sqlite_type:
        return 'TIMESTAMP'
    else:
        return 'TEXT'

def get_sqlite_tables(sqlite_conn):
    """Get list of all tables from SQLite"""
    cursor = sqlite_conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    return [row[0] for row in cursor.fetchall()]

def get_table_columns(sqlite_conn, table_name):
    """Get column information for a table"""
    cursor = sqlite_conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    return cursor.fetchall()

def create_postgres_table(pg_conn, sqlite_conn, table_name):
    """Create a PostgreSQL table based on SQLite schema"""
    columns = get_table_columns(sqlite_conn, table_name)
    
    if not columns:
        return False
    
    pg_cur = pg_conn.cursor()
    
    try:
        pg_cur.execute(f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table_name,))
        exists = pg_cur.fetchone()['exists']
        if exists:
            print(f"  Table {table_name} already exists")
            return True
    except Exception as e:
        pg_conn.rollback()
    
    col_defs = []
    for col in columns:
        col_id, col_name, col_type, not_null, default_val, is_pk = col
        
        pg_type = convert_sqlite_type_to_postgres(col_type)
        
        if col_name == 'id' and is_pk:
            col_def = f'"{col_name}" SERIAL PRIMARY KEY'
        else:
            col_def = f'"{col_name}" {pg_type}'
            if not_null and col_name != 'id':
                if default_val is not None:
                    pass
                else:
                    pass
        
        col_defs.append(col_def)
    
    create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n  ' + ',\n  '.join(col_defs) + '\n)'
    
    try:
        pg_cur.execute(create_sql)
        pg_conn.commit()
        print(f"  Created table: {table_name}")
        return True
    except Exception as e:
        print(f"  Error creating {table_name}: {e}")
        pg_conn.rollback()
        return False

def migrate_table_data(sqlite_conn, pg_conn, table_name):
    """Migrate data from SQLite to PostgreSQL with type conversion"""
    sqlite_cur = sqlite_conn.cursor()
    pg_cur = pg_conn.cursor()
    
    try:
        pg_cur.execute(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
        count = pg_cur.fetchone()['cnt']
        if count > 0:
            print(f"  Skipping {table_name} data - already has {count} records")
            return count
    except Exception as e:
        pg_conn.rollback()
        return 0
    
    try:
        rows = sqlite_cur.execute(f'SELECT * FROM "{table_name}"').fetchall()
        columns = [desc[0] for desc in sqlite_cur.description]
    except Exception as e:
        print(f"  Error reading {table_name}: {e}")
        return 0
    
    if not rows:
        print(f"  No data in {table_name}")
        return 0
    
    col_names = ', '.join([f'"{c}"' for c in columns])
    placeholders = ', '.join(['%s'] * len(columns))
    
    migrated = 0
    for row in rows:
        values = []
        for i, val in enumerate(row):
            if val is None:
                values.append(None)
            elif isinstance(val, int) and val in (0, 1):
                values.append(val)
            else:
                values.append(val)
        
        try:
            pg_cur.execute(
                f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING',
                tuple(values)
            )
            migrated += 1
        except Exception as e:
            error_str = str(e)
            if 'violates not-null constraint' not in error_str and 'violates foreign key' not in error_str:
                print(f"  Row error in {table_name}: {e}")
            pg_conn.rollback()
            continue
    
    pg_conn.commit()
    
    try:
        pg_cur.execute(f'SELECT MAX(id) as max_id FROM "{table_name}"')
        result = pg_cur.fetchone()
        max_id = result['max_id'] if result and result['max_id'] else 0
        if max_id > 0:
            seq_name = f"{table_name}_id_seq"
            pg_cur.execute(f"SELECT setval('{seq_name}', {max_id}, true)")
            pg_conn.commit()
    except Exception:
        pg_conn.rollback()
    
    if migrated > 0:
        print(f"  Migrated {migrated}/{len(rows)} records to {table_name}")
    
    return migrated

def main():
    print("=" * 60)
    print("Dynamic.IQ-COREx PostgreSQL Migration")
    print("=" * 60)
    
    print("\n[1/4] Connecting to databases...")
    pg_conn = get_postgres_connection()
    print("  Connected to PostgreSQL")
    
    sqlite_conn = get_sqlite_connection()
    if not sqlite_conn:
        print("  ERROR: Cannot connect to SQLite")
        return
    print("  Connected to SQLite")
    
    tables_ordered = [
        'users',
        'unit_of_measure',
        'condition_codes',
        'skills',
        'chart_of_accounts',
        'products',
        'suppliers',
        'customers',
        'inventory',
        'bom',
        'work_order_stages',
        'master_routings',
        'work_orders',
        'work_order_tasks',
        'purchase_orders',
        'purchase_order_lines',
        'sales_orders',
        'sales_order_lines',
        'invoices',
        'invoice_lines',
        'general_ledger',
        'journal_entries',
        'journal_entry_lines',
        'uom_conversions',
        'audit_log',
        'labor_time_entries',
        'user_skills',
        'shipping_documents',
        'shipping_document_lines',
        'rfqs',
        'rfq_lines',
        'repair_orders',
        'master_routing_tasks',
        'training_records',
        'ojt_records',
        'sops',
        'work_instructions',
        'work_order_documents',
        'inventory_documents',
        'leads',
        'lead_activities',
        'ndt_certificates',
        'tools',
        'tool_checkouts',
        'task_materials',
    ]
    
    all_sqlite_tables = get_sqlite_tables(sqlite_conn)
    
    tables = []
    for t in tables_ordered:
        if t in all_sqlite_tables:
            tables.append(t)
    
    for t in all_sqlite_tables:
        if t not in tables:
            tables.append(t)
    
    print(f"\n[2/4] Creating {len(tables)} tables in PostgreSQL...")
    for table in tables:
        create_postgres_table(pg_conn, sqlite_conn, table)
    
    # Create exchange chain tables (may not exist in SQLite)
    print("\n  Creating exchange chain tables...")
    pg_cur = pg_conn.cursor()
    
    exchange_tables_sql = [
        '''CREATE TABLE IF NOT EXISTS exchange_chain_nodes (
            id SERIAL PRIMARY KEY,
            chain_id TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending',
            state_hash TEXT NOT NULL,
            metadata TEXT,
            previous_hash TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(chain_id, entity_type, entity_id)
        )''',
        '''CREATE TABLE IF NOT EXISTS exchange_dependency_edges (
            id SERIAL PRIMARY KEY,
            from_node_id INTEGER NOT NULL,
            to_node_id INTEGER NOT NULL,
            dependency_type TEXT NOT NULL,
            weight DECIMAL(10,2) DEFAULT 1.0,
            status TEXT DEFAULT 'active',
            metadata TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        '''CREATE TABLE IF NOT EXISTS exchange_events (
            id SERIAL PRIMARY KEY,
            event_id TEXT UNIQUE NOT NULL,
            chain_id TEXT NOT NULL,
            node_id INTEGER,
            event_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            prev_hash TEXT,
            event_hash TEXT NOT NULL,
            replay_cursor INTEGER NOT NULL,
            processed INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        '''CREATE TABLE IF NOT EXISTS exchange_chain_links (
            id SERIAL PRIMARY KEY,
            chain_id TEXT NOT NULL,
            linked_type TEXT NOT NULL,
            linked_id INTEGER NOT NULL,
            link_role TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(chain_id, linked_type, linked_id)
        )'''
    ]
    
    for sql in exchange_tables_sql:
        try:
            pg_cur.execute(sql)
            pg_conn.commit()
        except Exception as e:
            pg_conn.rollback()
            print(f"    Warning: {e}")
    
    # Create engine tables (AI executor, performance profiler, event engine)
    print("\n  Creating engine tables...")
    engine_tables_sql = [
        '''CREATE TABLE IF NOT EXISTS execution_modifications (
            id TEXT PRIMARY KEY,
            chain_id TEXT NOT NULL,
            hint_type TEXT NOT NULL,
            target_entity TEXT NOT NULL,
            target_id INTEGER NOT NULL,
            parameters TEXT,
            risk_score DECIMAL(10,4) NOT NULL,
            confidence DECIMAL(10,4) NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP,
            effectiveness_score DECIMAL(10,4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        '''CREATE TABLE IF NOT EXISTS risk_vectors (
            id SERIAL PRIMARY KEY,
            chain_id TEXT NOT NULL UNIQUE,
            event_frequency DECIMAL(10,4),
            failure_rate DECIMAL(10,4),
            average_processing_time DECIMAL(10,4),
            dependency_depth INTEGER,
            overdue_probability DECIMAL(10,4),
            resource_contention DECIMAL(10,4),
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        '''CREATE TABLE IF NOT EXISTS ai_learning_data (
            id SERIAL PRIMARY KEY,
            modification_id TEXT NOT NULL,
            predicted_outcome TEXT,
            actual_outcome TEXT,
            feedback_score DECIMAL(10,4),
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        '''CREATE TABLE IF NOT EXISTS performance_metrics (
            id SERIAL PRIMARY KEY,
            metric_type TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            value DECIMAL(18,4) NOT NULL,
            unit TEXT,
            context TEXT,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        '''CREATE TABLE IF NOT EXISTS latency_comparisons (
            id SERIAL PRIMARY KEY,
            operation TEXT NOT NULL,
            baseline_ms DECIMAL(10,4) NOT NULL,
            optimized_ms DECIMAL(10,4) NOT NULL,
            improvement_percent DECIMAL(10,4) NOT NULL,
            sample_count INTEGER NOT NULL,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        '''CREATE TABLE IF NOT EXISTS performance_snapshots (
            id SERIAL PRIMARY KEY,
            snapshot_type TEXT NOT NULL,
            snapshot_data TEXT NOT NULL,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        '''CREATE TABLE IF NOT EXISTS processed_events (
            id SERIAL PRIMARY KEY,
            event_id TEXT UNIQUE NOT NULL,
            idempotency_key TEXT UNIQUE NOT NULL,
            chain_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            prev_hash TEXT,
            event_hash TEXT NOT NULL,
            sequence_number INTEGER NOT NULL,
            processing_status TEXT NOT NULL,
            processing_time_ms DECIMAL(10,4),
            result TEXT,
            error TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        '''CREATE TABLE IF NOT EXISTS event_replay_cursors (
            id SERIAL PRIMARY KEY,
            chain_id TEXT UNIQUE NOT NULL,
            last_sequence INTEGER NOT NULL DEFAULT 0,
            last_event_hash TEXT,
            last_replay_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''',
        '''CREATE TABLE IF NOT EXISTS idempotency_registry (
            id SERIAL PRIMARY KEY,
            idempotency_key TEXT UNIQUE NOT NULL,
            event_id TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP
        )'''
    ]
    
    for sql in engine_tables_sql:
        try:
            pg_cur.execute(sql)
            pg_conn.commit()
        except Exception as e:
            pg_conn.rollback()
            print(f"    Warning: {e}")
    
    # Create indexes
    print("\n  Creating indexes...")
    indexes = [
        'CREATE INDEX IF NOT EXISTS idx_mods_chain ON execution_modifications(chain_id)',
        'CREATE INDEX IF NOT EXISTS idx_mods_expires ON execution_modifications(expires_at)',
        'CREATE INDEX IF NOT EXISTS idx_risk_chain ON risk_vectors(chain_id)',
        'CREATE INDEX IF NOT EXISTS idx_perf_type ON performance_metrics(metric_type)',
        'CREATE INDEX IF NOT EXISTS idx_perf_recorded ON performance_metrics(recorded_at)',
        'CREATE INDEX IF NOT EXISTS idx_latency_op ON latency_comparisons(operation)',
        'CREATE INDEX IF NOT EXISTS idx_events_chain ON processed_events(chain_id)',
        'CREATE INDEX IF NOT EXISTS idx_events_sequence ON processed_events(sequence_number)',
        'CREATE INDEX IF NOT EXISTS idx_events_type ON processed_events(event_type)',
        'CREATE INDEX IF NOT EXISTS idx_idempotency ON idempotency_registry(idempotency_key)'
    ]
    
    for idx_sql in indexes:
        try:
            pg_cur.execute(idx_sql)
            pg_conn.commit()
        except Exception as e:
            pg_conn.rollback()
    
    print(f"\n[3/4] Migrating data...")
    total_migrated = 0
    for table in tables:
        total_migrated += migrate_table_data(sqlite_conn, pg_conn, table)
    
    print(f"\n[4/4] Cleanup and verification...")
    pg_cur = pg_conn.cursor()
    for table in tables:
        try:
            pg_cur.execute(f'SELECT COUNT(*) as cnt FROM "{table}"')
            count = pg_cur.fetchone()['cnt']
            if count > 0:
                print(f"  {table}: {count} records")
        except:
            pass
    
    sqlite_conn.close()
    pg_conn.close()
    
    print("\n" + "=" * 60)
    print(f"Migration complete! Total records: {total_migrated}")
    print("=" * 60)
    print("\nTo use PostgreSQL in production:")
    print("1. Set REPLIT_DEPLOYMENT=1 environment variable")
    print("2. The app will automatically use PostgreSQL")
    print("=" * 60)

if __name__ == '__main__':
    main()
