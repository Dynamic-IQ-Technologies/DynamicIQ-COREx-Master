#!/usr/bin/env python3
"""
Schema Synchronization Script for Dynamic.IQ-COREx
Detects and fixes missing columns between SQLite (development) and PostgreSQL (production)
"""

import sqlite3
import psycopg2
import os
import sys
from datetime import datetime

def get_sqlite_schema():
    """Extract complete schema from SQLite development database"""
    if not os.path.exists('mrp.db'):
        print("[ERROR] SQLite database 'mrp.db' not found")
        return {}
    
    conn = sqlite3.connect('mrp.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = [row[0] for row in cursor.fetchall()]
    
    schema = {}
    for table in tables:
        cursor.execute(f"PRAGMA table_info({table})")
        columns = {}
        for row in cursor.fetchall():
            col_name = row[1]
            col_type = row[2].upper()
            not_null = row[3]
            default_value = row[4]
            columns[col_name] = {
                'type': col_type,
                'not_null': not_null,
                'default': default_value
            }
        schema[table] = columns
    
    conn.close()
    return schema

def get_postgres_schema(conn_string):
    """Extract complete schema from PostgreSQL production database"""
    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        schema = {}
        for table in tables:
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
            """, (table,))
            columns = {}
            for row in cursor.fetchall():
                columns[row[0]] = {
                    'type': row[1].upper(),
                    'not_null': row[2] == 'NO',
                    'default': row[3]
                }
            schema[table] = columns
        
        conn.close()
        return schema
    except Exception as e:
        print(f"[ERROR] Failed to connect to PostgreSQL: {e}")
        return {}

def map_sqlite_to_postgres_type(sqlite_type):
    """Map SQLite types to PostgreSQL equivalents"""
    type_map = {
        'INTEGER': 'INTEGER',
        'INT': 'INTEGER',
        'BIGINT': 'BIGINT',
        'SMALLINT': 'SMALLINT',
        'REAL': 'DOUBLE PRECISION',
        'FLOAT': 'DOUBLE PRECISION',
        'DOUBLE': 'DOUBLE PRECISION',
        'TEXT': 'TEXT',
        'VARCHAR': 'VARCHAR(255)',
        'CHAR': 'CHAR(255)',
        'BLOB': 'BYTEA',
        'BOOLEAN': 'BOOLEAN',
        'BOOL': 'BOOLEAN',
        'DATE': 'DATE',
        'DATETIME': 'TIMESTAMP',
        'TIMESTAMP': 'TIMESTAMP',
        'NUMERIC': 'NUMERIC',
        'DECIMAL': 'DECIMAL(18,4)',
    }
    
    sqlite_type = sqlite_type.upper().strip()
    
    if '(' in sqlite_type:
        base_type = sqlite_type.split('(')[0]
        if base_type in ['VARCHAR', 'CHAR', 'DECIMAL', 'NUMERIC']:
            return sqlite_type
        return type_map.get(base_type, 'TEXT')
    
    return type_map.get(sqlite_type, 'TEXT')

def generate_migration(sqlite_schema, postgres_schema):
    """Generate SQL statements to sync PostgreSQL with SQLite schema"""
    migrations = []
    missing_tables = []
    missing_columns = []
    
    for table, columns in sqlite_schema.items():
        if table not in postgres_schema:
            missing_tables.append(table)
            col_defs = []
            for col_name, col_info in columns.items():
                pg_type = map_sqlite_to_postgres_type(col_info['type'])
                if col_name == 'id':
                    col_defs.append(f"id SERIAL PRIMARY KEY")
                else:
                    col_defs.append(f"{col_name} {pg_type}")
            
            create_sql = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(col_defs)})"
            migrations.append(create_sql)
        else:
            for col_name, col_info in columns.items():
                if col_name not in postgres_schema[table]:
                    missing_columns.append(f"{table}.{col_name}")
                    pg_type = map_sqlite_to_postgres_type(col_info['type'])
                    
                    default_clause = ""
                    if col_info['default'] is not None:
                        default_val = col_info['default']
                        if isinstance(default_val, str) and default_val.startswith("'"):
                            default_clause = f" DEFAULT {default_val}"
                        elif default_val == 'CURRENT_TIMESTAMP':
                            default_clause = " DEFAULT CURRENT_TIMESTAMP"
                        else:
                            default_clause = f" DEFAULT {default_val}"
                    
                    alter_sql = f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_name} {pg_type}{default_clause}"
                    migrations.append(alter_sql)
    
    return migrations, missing_tables, missing_columns

def apply_migrations(conn_string, migrations):
    """Apply migration statements to PostgreSQL"""
    if not migrations:
        print("[INFO] No migrations needed - schema is in sync")
        return True
    
    success_count = 0
    error_count = 0
    
    for sql in migrations:
        if 'pg_constraint' in sql or 'pg_' in sql.split()[4] if len(sql.split()) > 4 else False:
            continue
        
        try:
            conn = psycopg2.connect(conn_string)
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
            conn.close()
            success_count += 1
            print(f"[OK] {sql[:80]}...")
        except Exception as e:
            error_count += 1
            error_msg = str(e).split('\n')[0]
            if 'already exists' in error_msg.lower():
                print(f"[SKIP] Column already exists: {sql[:50]}...")
                success_count += 1
                error_count -= 1
            else:
                print(f"[ERROR] {sql[:60]}... - {error_msg}")
            try:
                conn.close()
            except:
                pass
    
    print(f"\n[SUMMARY] Applied {success_count} migrations, {error_count} errors")
    return error_count == 0

def run_schema_sync():
    """Main entry point for schema synchronization"""
    print("=" * 60)
    print("Dynamic.IQ-COREx Schema Synchronization")
    print(f"Started at: {datetime.now().isoformat()}")
    print("=" * 60)
    
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        print("[ERROR] DATABASE_URL environment variable not set")
        return False
    
    print("\n[STEP 1] Extracting SQLite schema...")
    sqlite_schema = get_sqlite_schema()
    if not sqlite_schema:
        print("[ERROR] Could not extract SQLite schema")
        return False
    print(f"[INFO] Found {len(sqlite_schema)} tables in SQLite")
    
    print("\n[STEP 2] Extracting PostgreSQL schema...")
    postgres_schema = get_postgres_schema(database_url)
    if not postgres_schema:
        print("[ERROR] Could not extract PostgreSQL schema")
        return False
    print(f"[INFO] Found {len(postgres_schema)} tables in PostgreSQL")
    
    print("\n[STEP 3] Generating migrations...")
    migrations, missing_tables, missing_columns = generate_migration(sqlite_schema, postgres_schema)
    
    if missing_tables:
        print(f"\n[MISSING TABLES] {len(missing_tables)}:")
        for t in missing_tables[:20]:
            print(f"  - {t}")
        if len(missing_tables) > 20:
            print(f"  ... and {len(missing_tables) - 20} more")
    
    if missing_columns:
        print(f"\n[MISSING COLUMNS] {len(missing_columns)}:")
        for c in missing_columns[:30]:
            print(f"  - {c}")
        if len(missing_columns) > 30:
            print(f"  ... and {len(missing_columns) - 30} more")
    
    print(f"\n[INFO] Total migrations to apply: {len(migrations)}")
    
    print("\n[STEP 4] Applying migrations...")
    success = apply_migrations(database_url, migrations)
    
    print("\n" + "=" * 60)
    if success:
        print("[SUCCESS] Schema synchronization completed!")
    else:
        print("[WARNING] Schema synchronization completed with errors")
    print("=" * 60)
    
    return success

if __name__ == '__main__':
    success = run_schema_sync()
    sys.exit(0 if success else 1)
