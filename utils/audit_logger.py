"""Audit Logger utility for tracking changes across the application."""
from datetime import datetime


class AuditLogger:
    """Provides audit logging functionality for database changes."""
    
    @staticmethod
    def log(conn, record_type, record_id, action_type, changes=None, modified_by=None):
        """
        Simplified audit log method (alias for log_change with different param order).
        
        Args:
            conn: Database connection
            record_type: Type of record (e.g., 'inventory', 'work_orders')
            record_id: ID of the record being modified
            action_type: Type of action (e.g., 'CREATE', 'UPDATE', 'DELETE')
            changes: Optional dict of field changes
            modified_by: User ID who made the change
        """
        try:
            conn.execute('''
                INSERT INTO activity_log (
                    record_type, record_id, action_type, user_id, 
                    changes, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                record_type,
                record_id,
                action_type,
                modified_by,
                str(changes) if changes else None,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
        except Exception:
            pass
    
    @staticmethod
    def log_change(conn, record_type, record_id, action_type, modified_by, 
                   changes=None, description=None):
        """
        Log an audit trail entry.
        
        Args:
            conn: Database connection
            record_type: Type of record (e.g., 'shipments', 'sales_orders')
            record_id: ID of the record being modified
            action_type: Type of action (e.g., 'Created', 'Updated', 'Deleted')
            modified_by: User ID who made the change
            changes: Optional dict of field changes
            description: Optional description of the change
        """
        try:
            conn.execute('''
                INSERT INTO activity_log (
                    record_type, record_id, action_type, user_id, 
                    changes, description, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                record_type,
                record_id,
                action_type,
                modified_by,
                str(changes) if changes else None,
                description,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
        except Exception:
            pass
    
    @staticmethod
    def get_history(conn, record_type, record_id, limit=50):
        """
        Get audit history for a specific record.
        
        Args:
            conn: Database connection
            record_type: Type of record
            record_id: ID of the record
            limit: Maximum number of entries to return
            
        Returns:
            List of audit log entries
        """
        try:
            return conn.execute('''
                SELECT al.*, u.username
                FROM activity_log al
                LEFT JOIN users u ON al.user_id = u.id
                WHERE al.record_type = ? AND al.record_id = ?
                ORDER BY al.created_at DESC
                LIMIT ?
            ''', (record_type, record_id, limit)).fetchall()
        except Exception:
            return []
