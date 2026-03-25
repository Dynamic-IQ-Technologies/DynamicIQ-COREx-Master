from flask import Blueprint, jsonify, render_template, session
from models import Database
from auth import login_required
import logging
import threading
import uuid
import json


def _j(value, default=None):
    """Safely return a parsed JSON value — handles both str and already-parsed dict/list."""
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default

logger = logging.getLogger(__name__)

risk_radar_bp = Blueprint('risk_radar', __name__)

_jobs = {}
_jobs_lock = threading.Lock()


def _level_color(level):
    return {
        'Critical': 'danger',
        'High': 'warning',
        'Medium': 'info',
        'Low': 'success',
    }.get(level, 'secondary')


def _trend_icon(trend):
    return {
        'degrading': '<i class="bi bi-arrow-up-right text-danger"></i>',
        'improving': '<i class="bi bi-arrow-down-right text-success"></i>',
        'stable':    '<i class="bi bi-dash text-muted"></i>',
    }.get(trend or 'stable', '<i class="bi bi-dash text-muted"></i>')


# ─────────────────────────────────────────────
# Dashboard page
# ─────────────────────────────────────────────
@risk_radar_bp.route('/risk-radar')
@login_required
def dashboard():
    db = Database()
    conn = db.get_connection()
    try:
        supplier_profiles = conn.execute('''
            SELECT rp.*, s.country
            FROM supply_risk_profiles rp
            LEFT JOIN suppliers s ON rp.entity_id = s.id
            WHERE rp.entity_type = 'supplier'
            ORDER BY rp.risk_score DESC
        ''').fetchall()

        part_profiles = conn.execute('''
            SELECT rp.*, p.name AS product_name, p.lead_time_days,
                   COALESCE(p.cost, 0) AS unit_cost
            FROM supply_risk_profiles rp
            JOIN products p ON rp.entity_id = p.id
            WHERE rp.entity_type = 'part'
            ORDER BY rp.risk_score DESC
        ''').fetchall()

        recent_events = conn.execute('''
            SELECT * FROM risk_events
            ORDER BY created_at DESC LIMIT 20
        ''').fetchall()

        # Summary metrics
        high_risk_suppliers = sum(1 for s in supplier_profiles if s['risk_level'] in ('High', 'Critical'))
        high_risk_parts = sum(1 for p in part_profiles if p['risk_level'] in ('High', 'Critical'))
        total_exposure = sum(
            _j(p['score_breakdown'], {}).get('shortage', {}).get('shortage_qty', 0) *
            float(p['unit_cost'] or 0)
            for p in part_profiles
        )
        last_updated = None
        if supplier_profiles:
            last_updated = supplier_profiles[0]['last_calculated']
        elif part_profiles:
            last_updated = part_profiles[0]['last_calculated']

        suppliers_enriched = []
        for s in supplier_profiles:
            bd = _j(s['score_breakdown'], {})
            row = dict(s)
            row['breakdown'] = bd
            row['mitigations'] = _j(s['mitigation_recommendations'], [])
            row['level_color'] = _level_color(s['risk_level'])
            row['trend_icon'] = _trend_icon(s['trend'])
            suppliers_enriched.append(row)

        parts_enriched = []
        for p in part_profiles:
            bd = _j(p['score_breakdown'], {})
            row = dict(p)
            row['breakdown'] = bd
            row['mitigations'] = _j(p['mitigation_recommendations'], [])
            row['level_color'] = _level_color(p['risk_level'])
            row['trend_icon'] = _trend_icon(p['trend'])
            row['shortage_qty'] = bd.get('shortage', {}).get('shortage_qty', 0)
            parts_enriched.append(row)

        return render_template(
            'risk_radar/dashboard.html',
            supplier_profiles=suppliers_enriched,
            part_profiles=parts_enriched,
            recent_events=[dict(e) for e in recent_events],
            high_risk_suppliers=high_risk_suppliers,
            high_risk_parts=high_risk_parts,
            total_exposure=total_exposure,
            last_updated=last_updated,
            has_data=bool(supplier_profiles or part_profiles),
        )
    finally:
        conn.close()


# ─────────────────────────────────────────────
# Async recalculation
# ─────────────────────────────────────────────
def _run_recalc(job_id, user_id):
    try:
        from engines.risk_radar import run_full_recalculation
        db = Database()
        conn = db.get_connection()
        results = run_full_recalculation(conn, created_by=user_id)
        conn.close()
        with _jobs_lock:
            _jobs[job_id] = {
                'status': 'done',
                'supplier_count': len(results['suppliers']),
                'part_count': len(results['parts']),
                'alert_count': len(results['alerts']),
                'alerts': results['alerts'],
            }
    except Exception as e:
        logger.error(f"Risk recalculation failed: {e}", exc_info=True)
        with _jobs_lock:
            _jobs[job_id] = {'status': 'error', 'error': str(e)}


@risk_radar_bp.route('/api/risk-radar/recalculate', methods=['POST'])
@login_required
def recalculate():
    job_id = str(uuid.uuid4())[:16]
    user_id = session.get('user_id')
    with _jobs_lock:
        _jobs[job_id] = {'status': 'running'}
    t = threading.Thread(target=_run_recalc, args=(job_id, user_id), daemon=True)
    t.start()
    return jsonify({'status': 'running', 'job_id': job_id})


@risk_radar_bp.route('/api/risk-radar/poll/<job_id>')
@login_required
def poll_recalc(job_id):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        return jsonify({'status': 'not_found'}), 404
    if job['status'] == 'done':
        with _jobs_lock:
            _jobs.pop(job_id, None)
    return jsonify(job)


# ─────────────────────────────────────────────
# Live score lookup (for embedding)
# ─────────────────────────────────────────────
@risk_radar_bp.route('/api/risk-radar/scores')
@login_required
def get_all_scores():
    db = Database()
    conn = db.get_connection()
    try:
        rows = conn.execute(
            'SELECT entity_type, entity_id, risk_score, risk_level, trend FROM supply_risk_profiles'
        ).fetchall()
        return jsonify({
            f"{r['entity_type']}:{r['entity_id']}": {
                'score': float(r['risk_score'] or 0),
                'level': r['risk_level'],
                'trend': r['trend'],
            }
            for r in rows
        })
    finally:
        conn.close()


@risk_radar_bp.route('/api/risk-radar/supplier/<int:supplier_id>')
@login_required
def supplier_risk_detail(supplier_id):
    db = Database()
    conn = db.get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM supply_risk_profiles WHERE entity_type='supplier' AND entity_id=%s",
            (supplier_id,)
        ).fetchone()
        if not row:
            return jsonify({'error': 'Not calculated yet'}), 404
        history = conn.execute('''
            SELECT risk_score, risk_level, calculated_at FROM risk_score_history
            WHERE entity_type='supplier' AND entity_id=%s
            ORDER BY calculated_at DESC LIMIT 10
        ''', (supplier_id,)).fetchall()
        data = dict(row)
        data['score_breakdown'] = _j(data.get('score_breakdown'), {})
        data['mitigation_recommendations'] = _j(data.get('mitigation_recommendations'), [])
        data['history'] = [dict(h) for h in history]
        return jsonify(data)
    finally:
        conn.close()


@risk_radar_bp.route('/api/risk-radar/part/<int:product_id>')
@login_required
def part_risk_detail(product_id):
    db = Database()
    conn = db.get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM supply_risk_profiles WHERE entity_type='part' AND entity_id=%s",
            (product_id,)
        ).fetchone()
        if not row:
            return jsonify({'error': 'Not calculated yet'}), 404
        data = dict(row)
        data['score_breakdown'] = _j(data.get('score_breakdown'), {})
        data['mitigation_recommendations'] = _j(data.get('mitigation_recommendations'), [])
        return jsonify(data)
    finally:
        conn.close()


@risk_radar_bp.route('/api/risk-radar/events')
@login_required
def event_log():
    db = Database()
    conn = db.get_connection()
    try:
        rows = conn.execute(
            'SELECT * FROM risk_events ORDER BY created_at DESC LIMIT 100'
        ).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()
