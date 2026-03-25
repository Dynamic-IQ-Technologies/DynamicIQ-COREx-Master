from flask import Blueprint, jsonify, render_template, request, session
from models import Database
from auth import login_required
import logging
import threading
import uuid
import json

logger = logging.getLogger(__name__)
twin_bp = Blueprint('digital_twin', __name__)

_jobs: dict = {}
_jobs_lock = threading.Lock()


def _j(v, d=None):
    if v is None:
        return d
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(v)
    except (TypeError, ValueError):
        return d


def _fmt_dt(ts):
    return str(ts)[:16] if ts else None


# ─────────────────────────────────────────────
# Dashboard
# ─────────────────────────────────────────────
@twin_bp.route('/digital-twin')
@login_required
def dashboard():
    db = Database()
    conn = db.get_connection()
    try:
        twin_counts = conn.execute('''
            SELECT twin_type, COUNT(*) AS cnt, MAX(last_synced) AS last_sync
            FROM digital_twins GROUP BY twin_type
        ''').fetchall()

        simulations = conn.execute('''
            SELECT id, name, scenario_type, status, confidence_level,
                   impact_kpis, executive_summary, created_at, completed_at
            FROM twin_simulations
            ORDER BY created_at DESC LIMIT 20
        ''').fetchall()

        suppliers = conn.execute(
            "SELECT id, name FROM suppliers WHERE status != 'Inactive' OR status IS NULL ORDER BY name"
        ).fetchall()

        sims_enriched = []
        for s in simulations:
            row = dict(s)
            row['impact_kpis'] = _j(s['impact_kpis'], {})
            row['created_at_fmt'] = _fmt_dt(s['created_at'])
            sims_enriched.append(row)

        twin_map = {r['twin_type']: {'count': int(r['cnt']), 'last_sync': _fmt_dt(r['last_sync'])}
                   for r in twin_counts}
        has_twins = bool(twin_counts)

        return render_template(
            'digital_twin/dashboard.html',
            twin_map=twin_map,
            simulations=sims_enriched,
            suppliers=[dict(s) for s in suppliers],
            has_twins=has_twins,
        )
    finally:
        conn.close()


# ─────────────────────────────────────────────
# Sync twins (async)
# ─────────────────────────────────────────────
def _run_sync(job_id, user_id):
    try:
        from engines.twin_engine import sync_digital_twins
        db = Database()
        conn = db.get_connection()
        results = sync_digital_twins(conn)
        conn.close()
        with _jobs_lock:
            _jobs[job_id] = {'status': 'done', 'results': results}
    except Exception as e:
        logger.error(f"Twin sync error: {e}", exc_info=True)
        with _jobs_lock:
            _jobs[job_id] = {'status': 'error', 'error': str(e)}


@twin_bp.route('/api/twin/sync', methods=['POST'])
@login_required
def sync_twins():
    job_id = str(uuid.uuid4())[:16]
    with _jobs_lock:
        _jobs[job_id] = {'status': 'running'}
    t = threading.Thread(target=_run_sync, args=(job_id, session.get('user_id')), daemon=True)
    t.start()
    return jsonify({'status': 'running', 'job_id': job_id})


# ─────────────────────────────────────────────
# Run simulation (async)
# ─────────────────────────────────────────────
def _run_simulation(job_id, scenario_type, parameters, sim_name, user_id):
    try:
        from engines.twin_engine import run_simulation
        db = Database()
        conn = db.get_connection()
        sim_id = run_simulation(conn, scenario_type, parameters, sim_name=sim_name, created_by=user_id)
        conn.close()
        with _jobs_lock:
            _jobs[job_id] = {'status': 'done', 'sim_id': sim_id}
    except Exception as e:
        logger.error(f"Simulation error: {e}", exc_info=True)
        with _jobs_lock:
            _jobs[job_id] = {'status': 'error', 'error': str(e)}


@twin_bp.route('/api/twin/simulate', methods=['POST'])
@login_required
def run_sim():
    data = request.get_json() or {}
    scenario_type = data.get('scenario_type', '').strip()
    parameters = data.get('parameters', {})
    sim_name = data.get('name', '').strip() or None
    if not scenario_type:
        return jsonify({'error': 'scenario_type required'}), 400

    job_id = str(uuid.uuid4())[:16]
    with _jobs_lock:
        _jobs[job_id] = {'status': 'running'}
    t = threading.Thread(
        target=_run_simulation,
        args=(job_id, scenario_type, parameters, sim_name, session.get('user_id')),
        daemon=True,
    )
    t.start()
    return jsonify({'status': 'running', 'job_id': job_id})


@twin_bp.route('/api/twin/poll/<job_id>')
@login_required
def poll(job_id):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        return jsonify({'status': 'not_found'}), 404
    if job['status'] in ('done', 'error'):
        with _jobs_lock:
            _jobs.pop(job_id, None)
    return jsonify(job)


# ─────────────────────────────────────────────
# Get simulation result
# ─────────────────────────────────────────────
@twin_bp.route('/api/twin/simulation/<int:sim_id>')
@login_required
def get_simulation(sim_id):
    db = Database()
    conn = db.get_connection()
    try:
        row = conn.execute(
            'SELECT * FROM twin_simulations WHERE id = %s', (sim_id,)
        ).fetchone()
        if not row:
            return jsonify({'error': 'Not found'}), 404
        data = dict(row)
        data['parameters'] = _j(data.get('parameters'), {})
        data['current_state'] = _j(data.get('current_state'), {})
        data['simulated_state'] = _j(data.get('simulated_state'), {})
        data['impact_kpis'] = _j(data.get('impact_kpis'), {})
        data['mitigations'] = _j(data.get('mitigations'), [])
        data['created_at'] = _fmt_dt(data.get('created_at'))
        data['completed_at'] = _fmt_dt(data.get('completed_at'))
        return jsonify(data)
    finally:
        conn.close()


@twin_bp.route('/api/twin/simulations')
@login_required
def list_simulations():
    db = Database()
    conn = db.get_connection()
    try:
        rows = conn.execute(
            'SELECT id, name, scenario_type, status, confidence_level, impact_kpis, created_at FROM twin_simulations ORDER BY created_at DESC LIMIT 50'
        ).fetchall()
        return jsonify([{
            'id': r['id'],
            'name': r['name'],
            'scenario_type': r['scenario_type'],
            'status': r['status'],
            'confidence_level': r['confidence_level'],
            'impact_kpis': _j(r['impact_kpis'], {}),
            'created_at': _fmt_dt(r['created_at']),
        } for r in rows])
    finally:
        conn.close()
