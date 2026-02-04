"""
ASC-AI Admin Console Routes
============================
Admin interface for monitoring and managing the Autonomous System Correction engine.
"""

from flask import Blueprint, render_template, jsonify, request, session
from auth import login_required, role_required
from models import Database
from engines.asc_ai import asc_engine
import json

asc_admin_bp = Blueprint('asc_admin_routes', __name__)


@asc_admin_bp.route('/asc-ai/dashboard')
@login_required
@role_required('Admin')
def asc_dashboard():
    stats = asc_engine.get_dashboard_stats()
    ledger_status = asc_engine.verify_ledger_balance()
    
    return render_template('asc_ai/dashboard.html',
                         stats=stats,
                         ledger_status=ledger_status)


@asc_admin_bp.route('/asc-ai/anomalies')
@login_required
@role_required('Admin')
def list_anomalies():
    db = Database()
    conn = db.get_connection()
    
    anomalies = conn.execute('''
        SELECT a.*, e.event_type, e.entity_type, e.entity_id, e.error_message
        FROM asc_anomalies a
        LEFT JOIN asc_events e ON a.event_id = e.id
        ORDER BY a.detected_at DESC
        LIMIT 100
    ''').fetchall()
    
    conn.close()
    
    return render_template('asc_ai/anomalies.html', anomalies=anomalies)


@asc_admin_bp.route('/asc-ai/corrections')
@login_required
@role_required('Admin')
def list_corrections():
    db = Database()
    conn = db.get_connection()
    
    corrections = conn.execute('''
        SELECT c.*, a.anomaly_type, a.severity, a.confidence as anomaly_confidence
        FROM asc_corrections c
        LEFT JOIN asc_anomalies a ON c.anomaly_id = a.id
        ORDER BY c.created_at DESC
        LIMIT 100
    ''').fetchall()
    
    conn.close()
    
    return render_template('asc_ai/corrections.html', corrections=corrections)


@asc_admin_bp.route('/asc-ai/quarantine')
@login_required
@role_required('Admin')
def list_quarantine():
    db = Database()
    conn = db.get_connection()
    
    quarantined = conn.execute('''
        SELECT q.*, c.correction_type, c.plan, c.confidence,
               a.anomaly_type, a.severity, e.entity_type, e.entity_id, e.error_message
        FROM asc_quarantine q
        JOIN asc_corrections c ON q.correction_id = c.id
        LEFT JOIN asc_anomalies a ON c.anomaly_id = a.id
        LEFT JOIN asc_events e ON a.event_id = e.id
        WHERE q.status = 'pending'
        ORDER BY q.created_at DESC
    ''').fetchall()
    
    conn.close()
    
    return render_template('asc_ai/quarantine.html', quarantined=quarantined)


@asc_admin_bp.route('/asc-ai/audit-log')
@login_required
@role_required('Admin')
def audit_log():
    db = Database()
    conn = db.get_connection()
    
    logs = conn.execute('''
        SELECT * FROM asc_audit_log
        ORDER BY created_at DESC
        LIMIT 200
    ''').fetchall()
    
    conn.close()
    
    return render_template('asc_ai/audit_log.html', logs=logs)


@asc_admin_bp.route('/asc-ai/api/stats')
@login_required
@role_required('Admin')
def api_stats():
    stats = asc_engine.get_dashboard_stats()
    ledger_status = asc_engine.verify_ledger_balance()
    return jsonify({
        'stats': stats,
        'ledger_status': ledger_status
    })


@asc_admin_bp.route('/asc-ai/api/approve/<correction_id>', methods=['POST'])
@login_required
@role_required('Admin')
def approve_correction(correction_id):
    user_id = session.get('user_id')
    success = asc_engine.approve_correction(correction_id, user_id)
    
    if success:
        db = Database()
        conn = db.get_connection()
        conn.execute('''
            UPDATE asc_quarantine 
            SET status = 'approved', reviewed_by = ?, reviewed_at = CURRENT_TIMESTAMP
            WHERE correction_id = ?
        ''', (user_id, correction_id))
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Correction approved and applied'})
    
    return jsonify({'success': False, 'message': 'Failed to apply correction'}), 400


@asc_admin_bp.route('/asc-ai/api/reject/<correction_id>', methods=['POST'])
@login_required
@role_required('Admin')
def reject_correction(correction_id):
    user_id = session.get('user_id')
    reason = request.json.get('reason', 'Rejected by admin')
    
    success = asc_engine.reject_correction(correction_id, user_id, reason)
    
    if success:
        db = Database()
        conn = db.get_connection()
        conn.execute('''
            UPDATE asc_quarantine 
            SET status = 'rejected', reviewed_by = ?, reviewed_at = CURRENT_TIMESTAMP
            WHERE correction_id = ?
        ''', (user_id, correction_id))
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Correction rejected'})
    
    return jsonify({'success': False, 'message': 'Correction not found'}), 404


@asc_admin_bp.route('/asc-ai/api/events')
@login_required
@role_required('Admin')
def api_events():
    db = Database()
    conn = db.get_connection()
    
    limit = request.args.get('limit', 50, type=int)
    
    events = conn.execute('''
        SELECT * FROM asc_events
        ORDER BY created_at DESC
        LIMIT ?
    ''', (limit,)).fetchall()
    
    conn.close()
    
    return jsonify({'events': [dict(e) for e in events]})


@asc_admin_bp.route('/asc-ai/api/verify-ledger')
@login_required
@role_required('Admin')
def api_verify_ledger():
    result = asc_engine.verify_ledger_balance()
    return jsonify(result)


@asc_admin_bp.route('/asc-ai/api/health')
@login_required
@role_required('Admin')
def api_health():
    try:
        stats = asc_engine.get_dashboard_stats()
        return jsonify({
            'status': 'healthy',
            'engine_initialized': asc_engine._initialized,
            'events_count': stats['total_events'],
            'anomalies_count': stats['total_anomalies'],
            'quarantine_count': stats['quarantined']
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500
