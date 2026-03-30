from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database, AuditLogger
from auth import login_required, role_required
from datetime import datetime, timedelta, date
import math

epm_bp = Blueprint('epm_routes', __name__)

CRITICALITY_COLORS = {
    'Low': 'success',
    'Medium': 'warning',
    'High': 'danger',
    'Safety-Critical': 'dark'
}

STATUS_COLORS = {
    'Planned': 'secondary',
    'Released': 'primary',
    'In Progress': 'warning',
    'Waiting': 'info',
    'Completed': 'success',
    'Verified': 'success',
    'Closed': 'dark'
}

def _next_mwo_number(conn):
    row = conn.execute("SELECT COUNT(*) as cnt FROM epm_work_orders").fetchone()
    n = (row['cnt'] if row else 0) + 1
    return f"MWO-{n:05d}"

def _next_asset_tag(conn):
    row = conn.execute("SELECT COUNT(*) as cnt FROM epm_equipment").fetchone()
    n = (row['cnt'] if row else 0) + 1
    return f"EQ-{n:04d}"

def _calculate_risk_score(conn, equipment_id):
    equip = conn.execute('SELECT * FROM epm_equipment WHERE id = ?', (equipment_id,)).fetchone()
    if not equip:
        return 0, 'Unknown', None

    risk = 0
    reasons = []

    plans = conn.execute(
        'SELECT * FROM epm_maintenance_plans WHERE equipment_id = ? AND active = 1', (equipment_id,)
    ).fetchall()

    total_rul_days = None
    for plan in plans:
        last_mwo = conn.execute('''
            SELECT actual_finish FROM epm_work_orders
            WHERE equipment_id = ? AND plan_id = ? AND status IN ('Completed','Verified','Closed')
            ORDER BY actual_finish DESC LIMIT 1
        ''', (equipment_id, plan['id'])).fetchone()

        if plan['interval_unit'] == 'Days' and plan['interval_value']:
            interval_days = plan['interval_value']
            if last_mwo and last_mwo['actual_finish']:
                try:
                    if isinstance(last_mwo['actual_finish'], str):
                        last_dt = datetime.fromisoformat(last_mwo['actual_finish'][:19])
                    else:
                        last_dt = last_mwo['actual_finish']
                    days_since = (datetime.now() - last_dt).days
                    pct_used = days_since / interval_days if interval_days else 0
                    rul = interval_days - days_since
                    if total_rul_days is None or rul < total_rul_days:
                        total_rul_days = rul
                    if pct_used >= 1.0:
                        risk += 30
                        reasons.append('Overdue PM')
                    elif pct_used >= 0.9:
                        risk += 20
                        reasons.append('PM due soon')
                    elif pct_used >= 0.75:
                        risk += 10
                        reasons.append('PM approaching')
                except Exception:
                    pass
            else:
                risk += 15
                reasons.append('No PM history')

    open_mwos = conn.execute(
        "SELECT COUNT(*) as cnt FROM epm_work_orders WHERE equipment_id = ? AND status NOT IN ('Closed','Completed','Verified')",
        (equipment_id,)
    ).fetchone()
    if open_mwos and open_mwos['cnt'] > 2:
        risk += 10
        reasons.append('Multiple open MWOs')

    if equip['criticality'] == 'Safety-Critical':
        risk = min(100, risk + 15)
        reasons.append('Safety-critical asset')
    elif equip['criticality'] == 'High':
        risk = min(100, risk + 8)

    latest_signal = conn.execute(
        'SELECT risk_score FROM epm_predictive_signals WHERE equipment_id = ? ORDER BY timestamp DESC LIMIT 1',
        (equipment_id,)
    ).fetchone()
    if latest_signal:
        signal_contrib = latest_signal['risk_score'] * 0.4
        risk = min(100, risk + signal_contrib)
        reasons.append('Sensor data included')

    risk = min(100, max(0, risk))
    if risk >= 75:
        level = 'Critical'
    elif risk >= 50:
        level = 'High'
    elif risk >= 25:
        level = 'Medium'
    else:
        level = 'Low'

    return round(risk, 1), level, total_rul_days


def _get_next_due(conn, equipment_id):
    plans = conn.execute(
        'SELECT * FROM epm_maintenance_plans WHERE equipment_id = ? AND active = 1', (equipment_id,)
    ).fetchall()
    earliest = None
    for plan in plans:
        if plan['interval_unit'] != 'Days':
            continue
        last_mwo = conn.execute('''
            SELECT actual_finish FROM epm_work_orders
            WHERE equipment_id = ? AND plan_id = ? AND status IN ('Completed','Verified','Closed')
            ORDER BY actual_finish DESC LIMIT 1
        ''', (equipment_id, plan['id'])).fetchone()
        if last_mwo and last_mwo['actual_finish']:
            try:
                if isinstance(last_mwo['actual_finish'], str):
                    last_dt = datetime.fromisoformat(last_mwo['actual_finish'][:19]).date()
                else:
                    last_dt = last_mwo['actual_finish'].date() if hasattr(last_mwo['actual_finish'], 'date') else last_mwo['actual_finish']
                due = last_dt + timedelta(days=int(plan['interval_value']))
            except Exception:
                due = date.today() + timedelta(days=int(plan['interval_value']))
        else:
            due = date.today()
        if earliest is None or due < earliest:
            earliest = due
    return earliest


# ─────────────────────────────────────────────────────────────────────────────
# EQUIPMENT LIST
# ─────────────────────────────────────────────────────────────────────────────
@epm_bp.route('/maintenance/epm')
@login_required
def list_equipment():
    db = Database()
    conn = db.get_connection()

    location_filter = request.args.get('location', '')
    status_filter = request.args.get('status', '')
    criticality_filter = request.args.get('criticality', '')
    search = request.args.get('search', '')

    query = 'SELECT e.*, wc.name as wc_name FROM epm_equipment e LEFT JOIN work_centers wc ON e.work_center_id = wc.id WHERE 1=1'
    params = []

    if location_filter:
        query += ' AND e.location = ?'
        params.append(location_filter)
    if status_filter:
        query += ' AND e.status = ?'
        params.append(status_filter)
    if criticality_filter:
        query += ' AND e.criticality = ?'
        params.append(criticality_filter)
    if search:
        query += ' AND (e.name LIKE ? OR e.asset_tag LIKE ? OR e.manufacturer LIKE ?)'
        params += [f'%{search}%', f'%{search}%', f'%{search}%']

    query += ' ORDER BY e.criticality DESC, e.name'
    rows = conn.execute(query, params).fetchall()
    equipment = [dict(r) for r in rows]

    for eq in equipment:
        score, level, rul = _calculate_risk_score(conn, eq['id'])
        eq['risk_score'] = score
        eq['risk_level'] = level
        eq['rul_days'] = rul
        eq['next_due'] = _get_next_due(conn, eq['id'])
        plan_count = conn.execute(
            'SELECT COUNT(*) as cnt FROM epm_maintenance_plans WHERE equipment_id = ? AND active = 1',
            (eq['id'],)
        ).fetchone()
        eq['plan_count'] = plan_count['cnt'] if plan_count else 0
        open_mwo = conn.execute(
            "SELECT COUNT(*) as cnt FROM epm_work_orders WHERE equipment_id = ? AND status NOT IN ('Closed','Completed','Verified')",
            (eq['id'],)
        ).fetchone()
        eq['open_mwos'] = open_mwo['cnt'] if open_mwo else 0

    locations = [r['location'] for r in conn.execute(
        'SELECT DISTINCT location FROM epm_equipment WHERE location IS NOT NULL ORDER BY location'
    ).fetchall()]

    overdue_count = sum(1 for e in equipment if e['next_due'] and e['next_due'] < date.today())
    total_active = sum(1 for e in equipment if e['status'] == 'Active')
    high_risk_count = sum(1 for e in equipment if e['risk_level'] in ('High', 'Critical'))

    conn.close()
    return render_template('epm/list.html',
        equipment=equipment,
        locations=locations,
        location_filter=location_filter,
        status_filter=status_filter,
        criticality_filter=criticality_filter,
        search=search,
        overdue_count=overdue_count,
        total_active=total_active,
        high_risk_count=high_risk_count,
        criticality_colors=CRITICALITY_COLORS,
        today=date.today()
    )


# ─────────────────────────────────────────────────────────────────────────────
# CREATE EQUIPMENT
# ─────────────────────────────────────────────────────────────────────────────
@epm_bp.route('/maintenance/epm/new', methods=['GET', 'POST'])
@role_required('Admin', 'Maintenance Engineer', 'Maintenance Manager')
def create_equipment():
    db = Database()
    conn = db.get_connection()

    if request.method == 'POST':
        try:
            asset_tag = request.form.get('asset_tag', '').strip() or _next_asset_tag(conn)
            name = request.form.get('name', '').strip()
            if not name:
                flash('Equipment name is required.', 'danger')
                conn.close()
                return redirect(url_for('epm_routes.create_equipment'))

            conn.execute('''
                INSERT INTO epm_equipment (asset_tag, name, type, manufacturer, model, serial_number,
                    location, work_center_id, criticality, commission_date, operating_hours, status, notes, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                asset_tag,
                name,
                request.form.get('type', '').strip(),
                request.form.get('manufacturer', '').strip(),
                request.form.get('model', '').strip(),
                request.form.get('serial_number', '').strip(),
                request.form.get('location', '').strip(),
                request.form.get('work_center_id') or None,
                request.form.get('criticality', 'Medium'),
                request.form.get('commission_date') or None,
                float(request.form.get('operating_hours', 0) or 0),
                request.form.get('status', 'Active'),
                request.form.get('notes', '').strip(),
                session.get('user_id')
            ))
            row = conn.execute("SELECT last_insert_rowid() as id").fetchone()
            eq_id = row['id']
            conn.execute('''INSERT INTO epm_audit_log (entity_type, entity_id, action, changed_by, notes)
                VALUES ('equipment', ?, 'Created', ?, ?)''',
                (eq_id, session.get('user_id'), f'Equipment {asset_tag} created'))
            conn.commit()
            conn.close()
            flash(f'Equipment {asset_tag} – {name} created successfully.', 'success')
            action_after = request.form.get('action_after', 'view')
            if action_after == 'create_another':
                return redirect(url_for('epm_routes.create_equipment'))
            return redirect(url_for('epm_routes.view_equipment', id=eq_id))
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error creating equipment: {str(e)}', 'danger')
            return redirect(url_for('epm_routes.create_equipment'))

    work_centers = [dict(r) for r in conn.execute(
        "SELECT id, name FROM work_centers WHERE status='Active' ORDER BY name"
    ).fetchall()]
    next_tag = _next_asset_tag(conn)
    conn.close()
    return render_template('epm/equipment_form.html',
        action='create', work_centers=work_centers, next_tag=next_tag, equipment={})


# ─────────────────────────────────────────────────────────────────────────────
# VIEW EQUIPMENT DETAIL
# ─────────────────────────────────────────────────────────────────────────────
@epm_bp.route('/maintenance/epm/<int:id>')
@login_required
def view_equipment(id):
    db = Database()
    conn = db.get_connection()

    equip = conn.execute('''
        SELECT e.*, wc.name as wc_name
        FROM epm_equipment e
        LEFT JOIN work_centers wc ON e.work_center_id = wc.id
        WHERE e.id = ?
    ''', (id,)).fetchone()
    if not equip:
        flash('Equipment not found.', 'danger')
        conn.close()
        return redirect(url_for('epm_routes.list_equipment'))
    equip = dict(equip)

    plans = conn.execute('''
        SELECT p.*, u.username as owner_name,
               (SELECT COUNT(*) FROM epm_maintenance_tasks WHERE plan_id = p.id) as task_count
        FROM epm_maintenance_plans p
        LEFT JOIN users u ON p.owner_user_id = u.id
        WHERE p.equipment_id = ?
        ORDER BY p.active DESC, p.name
    ''', (id,)).fetchall()
    plans = [dict(p) for p in plans]

    for plan in plans:
        tasks = conn.execute(
            'SELECT * FROM epm_maintenance_tasks WHERE plan_id = ? ORDER BY sequence', (plan['id'],)
        ).fetchall()
        plan['tasks'] = [dict(t) for t in tasks]

        last_mwo = conn.execute('''
            SELECT actual_finish FROM epm_work_orders
            WHERE equipment_id = ? AND plan_id = ? AND status IN ('Completed','Verified','Closed')
            ORDER BY actual_finish DESC LIMIT 1
        ''', (id, plan['id'])).fetchone()
        plan['last_pm'] = last_mwo['actual_finish'] if last_mwo else None

        if plan['interval_unit'] == 'Days' and plan['interval_value']:
            if plan['last_pm']:
                try:
                    if isinstance(plan['last_pm'], str):
                        last_dt = datetime.fromisoformat(str(plan['last_pm'])[:19]).date()
                    else:
                        last_dt = plan['last_pm'].date() if hasattr(plan['last_pm'], 'date') else plan['last_pm']
                    plan['next_due'] = last_dt + timedelta(days=int(plan['interval_value']))
                except Exception:
                    plan['next_due'] = None
            else:
                plan['next_due'] = None
        else:
            plan['next_due'] = None

    work_orders = conn.execute('''
        SELECT w.*, u.username as tech_name, p.name as plan_name
        FROM epm_work_orders w
        LEFT JOIN users u ON w.technician_id = u.id
        LEFT JOIN epm_maintenance_plans p ON w.plan_id = p.id
        WHERE w.equipment_id = ?
        ORDER BY w.scheduled_date DESC
        LIMIT 50
    ''', (id,)).fetchall()
    work_orders = [dict(w) for w in work_orders]

    signals = conn.execute('''
        SELECT * FROM epm_predictive_signals WHERE equipment_id = ?
        ORDER BY timestamp DESC LIMIT 20
    ''', (id,)).fetchall()
    signals = [dict(s) for s in signals]

    audit_log = conn.execute('''
        SELECT al.*, u.username as user_name
        FROM epm_audit_log al
        LEFT JOIN users u ON al.changed_by = u.id
        WHERE al.entity_type IN ('equipment','plan','mwo') AND al.entity_id = ?
        ORDER BY al.created_at DESC LIMIT 30
    ''', (id,)).fetchall()
    audit_log = [dict(a) for a in audit_log]

    risk_score, risk_level, rul_days = _calculate_risk_score(conn, id)
    next_due = _get_next_due(conn, id)

    technicians = [dict(r) for r in conn.execute(
        "SELECT id, username FROM users ORDER BY username"
    ).fetchall()]

    conn.close()
    return render_template('epm/detail.html',
        equip=equip, plans=plans, work_orders=work_orders,
        signals=signals, audit_log=audit_log,
        risk_score=risk_score, risk_level=risk_level, rul_days=rul_days,
        next_due=next_due, technicians=technicians,
        criticality_colors=CRITICALITY_COLORS,
        status_colors=STATUS_COLORS,
        today=date.today()
    )


# ─────────────────────────────────────────────────────────────────────────────
# EDIT EQUIPMENT
# ─────────────────────────────────────────────────────────────────────────────
@epm_bp.route('/maintenance/epm/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Maintenance Engineer', 'Maintenance Manager')
def edit_equipment(id):
    db = Database()
    conn = db.get_connection()

    equip = conn.execute('SELECT * FROM epm_equipment WHERE id = ?', (id,)).fetchone()
    if not equip:
        conn.close()
        flash('Equipment not found.', 'danger')
        return redirect(url_for('epm_routes.list_equipment'))

    if request.method == 'POST':
        try:
            conn.execute('''
                UPDATE epm_equipment SET name=?, type=?, manufacturer=?, model=?, serial_number=?,
                    location=?, work_center_id=?, criticality=?, commission_date=?,
                    operating_hours=?, status=?, notes=?, updated_at=CURRENT_TIMESTAMP
                WHERE id=?
            ''', (
                request.form.get('name', '').strip(),
                request.form.get('type', '').strip(),
                request.form.get('manufacturer', '').strip(),
                request.form.get('model', '').strip(),
                request.form.get('serial_number', '').strip(),
                request.form.get('location', '').strip(),
                request.form.get('work_center_id') or None,
                request.form.get('criticality', 'Medium'),
                request.form.get('commission_date') or None,
                float(request.form.get('operating_hours', 0) or 0),
                request.form.get('status', 'Active'),
                request.form.get('notes', '').strip(),
                id
            ))
            conn.execute('''INSERT INTO epm_audit_log (entity_type, entity_id, action, changed_by, notes)
                VALUES ('equipment', ?, 'Updated', ?, 'Equipment details updated')''',
                (id, session.get('user_id')))
            conn.commit()
            conn.close()
            flash('Equipment updated successfully.', 'success')
            return redirect(url_for('epm_routes.view_equipment', id=id))
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error updating equipment: {str(e)}', 'danger')
            return redirect(url_for('epm_routes.edit_equipment', id=id))

    work_centers = [dict(r) for r in conn.execute(
        "SELECT id, name FROM work_centers WHERE status='Active' ORDER BY name"
    ).fetchall()]
    conn.close()
    return render_template('epm/equipment_form.html',
        action='edit', equipment=dict(equip), work_centers=work_centers, next_tag='')


# ─────────────────────────────────────────────────────────────────────────────
# MAINTENANCE PLANS
# ─────────────────────────────────────────────────────────────────────────────
@epm_bp.route('/maintenance/epm/<int:id>/plans/new', methods=['GET', 'POST'])
@role_required('Admin', 'Maintenance Engineer', 'Maintenance Manager')
def create_plan(id):
    db = Database()
    conn = db.get_connection()

    equip = conn.execute('SELECT * FROM epm_equipment WHERE id = ?', (id,)).fetchone()
    if not equip:
        conn.close()
        flash('Equipment not found.', 'danger')
        return redirect(url_for('epm_routes.list_equipment'))

    if request.method == 'POST':
        try:
            name = request.form.get('name', '').strip()
            if not name:
                flash('Plan name is required.', 'danger')
                conn.close()
                return redirect(url_for('epm_routes.create_plan', id=id))

            conn.execute('''
                INSERT INTO epm_maintenance_plans
                    (equipment_id, name, strategy, interval_value, interval_unit,
                     effective_date, owner_user_id, auto_generate, notes, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ''', (
                id,
                name,
                request.form.get('strategy', 'Time-Based'),
                float(request.form.get('interval_value', 90) or 90),
                request.form.get('interval_unit', 'Days'),
                request.form.get('effective_date') or None,
                request.form.get('owner_user_id') or session.get('user_id'),
                1 if request.form.get('auto_generate') else 0,
                request.form.get('notes', '').strip()
            ))
            row = conn.execute("SELECT last_insert_rowid() as id").fetchone()
            plan_id = row['id']
            conn.execute('''INSERT INTO epm_audit_log (entity_type, entity_id, action, changed_by, notes)
                VALUES ('plan', ?, 'Created', ?, ?)''',
                (plan_id, session.get('user_id'), f'Plan {name} created for equipment {id}'))
            conn.commit()
            conn.close()
            flash(f'Maintenance plan "{name}" created.', 'success')
            return redirect(url_for('epm_routes.view_equipment', id=id) + '#plans')
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error creating plan: {str(e)}', 'danger')
            return redirect(url_for('epm_routes.create_plan', id=id))

    users = [dict(r) for r in conn.execute("SELECT id, username FROM users ORDER BY username").fetchall()]
    conn.close()
    return render_template('epm/plan_form.html', equip=dict(equip), action='create', plan={}, users=users)


@epm_bp.route('/maintenance/epm/<int:id>/plans/<int:pid>/edit', methods=['GET', 'POST'])
@role_required('Admin', 'Maintenance Engineer', 'Maintenance Manager')
def edit_plan(id, pid):
    db = Database()
    conn = db.get_connection()

    equip = conn.execute('SELECT * FROM epm_equipment WHERE id = ?', (id,)).fetchone()
    plan = conn.execute('SELECT * FROM epm_maintenance_plans WHERE id = ? AND equipment_id = ?', (pid, id)).fetchone()
    if not equip or not plan:
        conn.close()
        flash('Not found.', 'danger')
        return redirect(url_for('epm_routes.list_equipment'))

    if request.method == 'POST':
        try:
            conn.execute('''
                UPDATE epm_maintenance_plans SET name=?, strategy=?, interval_value=?, interval_unit=?,
                    effective_date=?, owner_user_id=?, auto_generate=?, notes=?, active=?
                WHERE id=?
            ''', (
                request.form.get('name', '').strip(),
                request.form.get('strategy', 'Time-Based'),
                float(request.form.get('interval_value', 90) or 90),
                request.form.get('interval_unit', 'Days'),
                request.form.get('effective_date') or None,
                request.form.get('owner_user_id') or session.get('user_id'),
                1 if request.form.get('auto_generate') else 0,
                request.form.get('notes', '').strip(),
                1 if request.form.get('active') else 0,
                pid
            ))
            conn.execute('''INSERT INTO epm_audit_log (entity_type, entity_id, action, changed_by, notes)
                VALUES ('plan', ?, 'Updated', ?, 'Plan details updated')''',
                (pid, session.get('user_id')))
            conn.commit()
            conn.close()
            flash('Maintenance plan updated.', 'success')
            return redirect(url_for('epm_routes.view_equipment', id=id) + '#plans')
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error updating plan: {str(e)}', 'danger')
            return redirect(url_for('epm_routes.edit_plan', id=id, pid=pid))

    users = [dict(r) for r in conn.execute("SELECT id, username FROM users ORDER BY username").fetchall()]
    tasks = [dict(r) for r in conn.execute(
        'SELECT * FROM epm_maintenance_tasks WHERE plan_id = ? ORDER BY sequence', (pid,)
    ).fetchall()]
    conn.close()
    return render_template('epm/plan_form.html',
        equip=dict(equip), action='edit', plan=dict(plan), users=users, tasks=tasks)


@epm_bp.route('/maintenance/epm/<int:id>/plans/<int:pid>/tasks/add', methods=['POST'])
@role_required('Admin', 'Maintenance Engineer', 'Maintenance Manager')
def add_task(id, pid):
    db = Database()
    conn = db.get_connection()
    try:
        desc = request.form.get('description', '').strip()
        if not desc:
            flash('Task description is required.', 'danger')
        else:
            max_seq = conn.execute('SELECT MAX(sequence) as ms FROM epm_maintenance_tasks WHERE plan_id = ?', (pid,)).fetchone()
            seq = (max_seq['ms'] or 0) + 1
            conn.execute('''
                INSERT INTO epm_maintenance_tasks
                    (plan_id, sequence, task_type, description, required_tools, required_parts,
                     estimated_duration, measurement_required, min_value, max_value, unit_of_measure)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                pid, seq,
                request.form.get('task_type', 'Inspection'),
                desc,
                request.form.get('required_tools', '').strip(),
                request.form.get('required_parts', '').strip(),
                float(request.form.get('estimated_duration', 1.0) or 1.0),
                1 if request.form.get('measurement_required') else 0,
                float(request.form.get('min_value', 0) or 0) or None,
                float(request.form.get('max_value', 0) or 0) or None,
                request.form.get('unit_of_measure', '').strip()
            ))
            conn.commit()
            flash('Task added.', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error adding task: {str(e)}', 'danger')
    conn.close()
    return redirect(url_for('epm_routes.edit_plan', id=id, pid=pid))


@epm_bp.route('/maintenance/epm/tasks/<int:tid>/delete', methods=['POST'])
@role_required('Admin', 'Maintenance Engineer', 'Maintenance Manager')
def delete_task(tid):
    db = Database()
    conn = db.get_connection()
    try:
        task = conn.execute(
            'SELECT t.*, p.equipment_id FROM epm_maintenance_tasks t JOIN epm_maintenance_plans p ON t.plan_id = p.id WHERE t.id = ?',
            (tid,)
        ).fetchone()
        if task:
            conn.execute('DELETE FROM epm_maintenance_tasks WHERE id = ?', (tid,))
            conn.commit()
            flash('Task removed.', 'success')
            conn.close()
            return redirect(url_for('epm_routes.edit_plan', id=task['equipment_id'], pid=task['plan_id']))
    except Exception as e:
        conn.rollback()
        flash(f'Error: {str(e)}', 'danger')
    conn.close()
    return redirect(url_for('epm_routes.list_equipment'))


# ─────────────────────────────────────────────────────────────────────────────
# GENERATE MWO
# ─────────────────────────────────────────────────────────────────────────────
@epm_bp.route('/maintenance/epm/<int:id>/plans/<int:pid>/generate-mwo', methods=['POST'])
@role_required('Admin', 'Maintenance Engineer', 'Maintenance Manager')
def generate_mwo(id, pid):
    db = Database()
    conn = db.get_connection()
    try:
        plan = conn.execute('SELECT * FROM epm_maintenance_plans WHERE id = ? AND equipment_id = ?', (pid, id)).fetchone()
        if not plan:
            flash('Plan not found.', 'danger')
            conn.close()
            return redirect(url_for('epm_routes.view_equipment', id=id))

        scheduled_date = request.form.get('scheduled_date') or str(date.today())
        technician_id = request.form.get('technician_id') or None
        mwo_number = _next_mwo_number(conn)

        conn.execute('''
            INSERT INTO epm_work_orders (mwo_number, equipment_id, plan_id, status, scheduled_date, technician_id, created_by)
            VALUES (?, ?, ?, 'Planned', ?, ?, ?)
        ''', (mwo_number, id, pid, scheduled_date, technician_id, session.get('user_id')))
        row = conn.execute("SELECT last_insert_rowid() as id").fetchone()
        mwo_id = row['id']

        tasks = conn.execute('SELECT * FROM epm_maintenance_tasks WHERE plan_id = ? ORDER BY sequence', (pid,)).fetchall()
        for task in tasks:
            conn.execute('''
                INSERT INTO epm_task_executions (mwo_id, task_id)
                VALUES (?, ?)
            ''', (mwo_id, task['id']))

        conn.execute('''INSERT INTO epm_audit_log (entity_type, entity_id, action, changed_by, notes)
            VALUES ('mwo', ?, 'Generated', ?, ?)''',
            (mwo_id, session.get('user_id'), f'MWO {mwo_number} generated from plan {plan["name"]}'))
        conn.commit()
        conn.close()
        flash(f'Work Order {mwo_number} generated successfully.', 'success')
        return redirect(url_for('epm_routes.view_mwo', id=mwo_id))
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error generating MWO: {str(e)}', 'danger')
        return redirect(url_for('epm_routes.view_equipment', id=id))


# ─────────────────────────────────────────────────────────────────────────────
# MWO VIEW & EXECUTE
# ─────────────────────────────────────────────────────────────────────────────
@epm_bp.route('/maintenance/epm/mwo/<int:id>', methods=['GET', 'POST'])
@login_required
def view_mwo(id):
    db = Database()
    conn = db.get_connection()

    mwo = conn.execute('''
        SELECT w.*, e.name as equip_name, e.asset_tag, e.criticality,
               p.name as plan_name, p.strategy,
               u.username as tech_name
        FROM epm_work_orders w
        JOIN epm_equipment e ON w.equipment_id = e.id
        LEFT JOIN epm_maintenance_plans p ON w.plan_id = p.id
        LEFT JOIN users u ON w.technician_id = u.id
        WHERE w.id = ?
    ''', (id,)).fetchone()

    if not mwo:
        flash('Work order not found.', 'danger')
        conn.close()
        return redirect(url_for('epm_routes.list_equipment'))
    mwo = dict(mwo)

    if request.method == 'POST':
        action = request.form.get('action')
        try:
            if action == 'start':
                conn.execute('''UPDATE epm_work_orders SET status='In Progress', actual_start=CURRENT_TIMESTAMP,
                    technician_id=?, updated_at=CURRENT_TIMESTAMP WHERE id=?''',
                    (request.form.get('technician_id') or mwo['technician_id'], id))
                conn.execute('''INSERT INTO epm_audit_log (entity_type, entity_id, action, changed_by)
                    VALUES ('mwo', ?, 'Started', ?)''', (id, session.get('user_id')))
                conn.commit()
                flash('Work order started.', 'success')

            elif action == 'save_tasks':
                task_ids = request.form.getlist('task_id')
                for tid in task_ids:
                    completed = 1 if request.form.get(f'completed_{tid}') else 0
                    measured_val = request.form.get(f'measured_{tid}') or None
                    notes = request.form.get(f'notes_{tid}', '').strip()
                    pass_fail = None
                    if measured_val:
                        exec_row = conn.execute('''
                            SELECT te.*, t.min_value, t.max_value, t.measurement_required
                            FROM epm_task_executions te
                            JOIN epm_maintenance_tasks t ON te.task_id = t.id
                            WHERE te.mwo_id=? AND te.task_id=?
                        ''', (id, tid)).fetchone()
                        if exec_row and exec_row['measurement_required']:
                            mv = float(measured_val)
                            mn = exec_row['min_value']
                            mx = exec_row['max_value']
                            if mn is not None and mx is not None:
                                pass_fail = 'Pass' if mn <= mv <= mx else 'Fail'

                    conn.execute('''UPDATE epm_task_executions
                        SET completed=?, measured_value=?, pass_fail=?, notes=?,
                            completed_by=?, completed_at=CASE WHEN ?=1 THEN CURRENT_TIMESTAMP ELSE completed_at END
                        WHERE mwo_id=? AND task_id=?''',
                        (completed, measured_val, pass_fail, notes, session.get('user_id'), completed, id, tid))
                conn.execute('''INSERT INTO epm_audit_log (entity_type, entity_id, action, changed_by)
                    VALUES ('mwo', ?, 'Tasks Updated', ?)''', (id, session.get('user_id')))
                conn.commit()
                flash('Task progress saved.', 'success')

            elif action == 'complete':
                findings = request.form.get('findings', '').strip()
                follow_up = 1 if request.form.get('follow_up_required') else 0
                follow_up_notes = request.form.get('follow_up_notes', '').strip()
                all_tasks = conn.execute(
                    'SELECT COUNT(*) as cnt FROM epm_task_executions WHERE mwo_id=?', (id,)
                ).fetchone()
                done_tasks = conn.execute(
                    'SELECT COUNT(*) as cnt FROM epm_task_executions WHERE mwo_id=? AND completed=1', (id,)
                ).fetchone()
                if all_tasks and done_tasks and all_tasks['cnt'] > 0 and done_tasks['cnt'] < all_tasks['cnt']:
                    flash(f'Cannot complete: {done_tasks["cnt"]}/{all_tasks["cnt"]} tasks completed. Finish all tasks first.', 'danger')
                else:
                    conn.execute('''UPDATE epm_work_orders
                        SET status='Completed', actual_finish=CURRENT_TIMESTAMP,
                            findings=?, follow_up_required=?, follow_up_notes=?,
                            updated_at=CURRENT_TIMESTAMP
                        WHERE id=?''',
                        (findings, follow_up, follow_up_notes, id))
                    conn.execute('''INSERT INTO epm_audit_log (entity_type, entity_id, action, changed_by, notes)
                        VALUES ('mwo', ?, 'Completed', ?, ?)''',
                        (id, session.get('user_id'), findings[:200] if findings else ''))
                    conn.commit()
                    flash('Work order completed and signed off.', 'success')
                    conn.close()
                    return redirect(url_for('epm_routes.view_mwo', id=id))

            elif action == 'close':
                conn.execute('''UPDATE epm_work_orders SET status='Closed', updated_at=CURRENT_TIMESTAMP WHERE id=? AND status='Completed' ''', (id,))
                conn.execute('''INSERT INTO epm_audit_log (entity_type, entity_id, action, changed_by)
                    VALUES ('mwo', ?, 'Closed', ?)''', (id, session.get('user_id')))
                conn.commit()
                flash('Work order closed.', 'success')
        except Exception as e:
            conn.rollback()
            flash(f'Error: {str(e)}', 'danger')

        conn.close()
        return redirect(url_for('epm_routes.view_mwo', id=id))

    task_executions = conn.execute('''
        SELECT te.*, t.description, t.task_type, t.estimated_duration, t.measurement_required,
               t.min_value, t.max_value, t.unit_of_measure, t.required_tools, t.required_parts
        FROM epm_task_executions te
        JOIN epm_maintenance_tasks t ON te.task_id = t.id
        WHERE te.mwo_id = ?
        ORDER BY t.sequence
    ''', (id,)).fetchall()
    task_executions = [dict(t) for t in task_executions]

    technicians = [dict(r) for r in conn.execute("SELECT id, username FROM users ORDER BY username").fetchall()]

    conn.close()
    return render_template('epm/mwo_detail.html',
        mwo=mwo, task_executions=task_executions,
        technicians=technicians,
        status_colors=STATUS_COLORS,
        criticality_colors=CRITICALITY_COLORS
    )


# ─────────────────────────────────────────────────────────────────────────────
# RECORD PREDICTIVE SIGNAL
# ─────────────────────────────────────────────────────────────────────────────
@epm_bp.route('/maintenance/epm/<int:id>/signal', methods=['POST'])
@login_required
def record_signal(id):
    db = Database()
    conn = db.get_connection()
    try:
        value = float(request.form.get('value', 0) or 0)
        signal_type = request.form.get('signal_type', 'General')
        risk_score, _, rul = _calculate_risk_score(conn, id)

        conn.execute('''
            INSERT INTO epm_predictive_signals
                (equipment_id, signal_type, value, risk_score, remaining_useful_life, source, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (id, signal_type, value, risk_score, rul, 'Manual', request.form.get('notes', '').strip()))
        conn.commit()
        flash('Predictive signal recorded.', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error: {str(e)}', 'danger')
    conn.close()
    return redirect(url_for('epm_routes.view_equipment', id=id) + '#predictive')


# ─────────────────────────────────────────────────────────────────────────────
# EPM DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────
@epm_bp.route('/maintenance/epm/dashboard')
@login_required
def epm_dashboard():
    db = Database()
    conn = db.get_connection()

    total_equip = conn.execute("SELECT COUNT(*) as cnt FROM epm_equipment WHERE status='Active'").fetchone()['cnt']
    total_plans = conn.execute("SELECT COUNT(*) as cnt FROM epm_maintenance_plans WHERE active=1").fetchone()['cnt']
    open_mwos = conn.execute(
        "SELECT COUNT(*) as cnt FROM epm_work_orders WHERE status NOT IN ('Closed','Completed','Verified')"
    ).fetchone()['cnt']
    completed_mwos = conn.execute(
        "SELECT COUNT(*) as cnt FROM epm_work_orders WHERE status IN ('Completed','Verified','Closed')"
    ).fetchone()['cnt']

    all_equip = conn.execute('SELECT id FROM epm_equipment WHERE status=?', ('Active',)).fetchall()
    overdue = []
    high_risk = []
    for eq in all_equip:
        eid = eq['id'] if isinstance(eq, dict) else eq[0]
        nd = _get_next_due(conn, eid)
        if nd and nd < date.today():
            row = conn.execute('SELECT asset_tag, name, criticality FROM epm_equipment WHERE id=?', (eid,)).fetchone()
            if row:
                overdue.append({'id': eid, 'asset_tag': row['asset_tag'], 'name': row['name'],
                                'criticality': row['criticality'], 'overdue_since': nd})
        score, level, _ = _calculate_risk_score(conn, eid)
        if level in ('High', 'Critical'):
            row = conn.execute('SELECT asset_tag, name, criticality FROM epm_equipment WHERE id=?', (eid,)).fetchone()
            if row:
                high_risk.append({'id': eid, 'asset_tag': row['asset_tag'], 'name': row['name'],
                                  'risk_score': score, 'risk_level': level})

    compliance_pct = 0
    if total_plans > 0:
        on_time = len([e for e in [_get_next_due(conn, r[0] if not isinstance(r, dict) else r['id'])
                                    for r in all_equip]
                       if e is None or e >= date.today()])
        compliance_pct = round(on_time / max(total_plans, 1) * 100, 1)

    recent_mwos = conn.execute('''
        SELECT w.id, w.mwo_number, w.status, w.scheduled_date, e.name as equip_name, e.asset_tag
        FROM epm_work_orders w
        JOIN epm_equipment e ON w.equipment_id = e.id
        ORDER BY w.created_at DESC LIMIT 10
    ''').fetchall()
    recent_mwos = [dict(r) for r in recent_mwos]

    conn.close()
    return render_template('epm/dashboard.html',
        total_equip=total_equip, total_plans=total_plans,
        open_mwos=open_mwos, completed_mwos=completed_mwos,
        overdue=overdue, high_risk=high_risk,
        compliance_pct=compliance_pct,
        recent_mwos=recent_mwos,
        status_colors=STATUS_COLORS,
        criticality_colors=CRITICALITY_COLORS
    )
