from flask import Blueprint, jsonify, request, session
from models import Database
from auth import login_required
from datetime import datetime, date
import logging

log = logging.getLogger(__name__)
wo_priority_bp = Blueprint('wo_priority_routes', __name__)


# ─── Lazy Table Creation ──────────────────────────────────────────────────────

def ensure_priority_tables(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS wo_priority_profiles (
            id              SERIAL PRIMARY KEY,
            wo_id           INTEGER NOT NULL UNIQUE,
            priority_score  NUMERIC(5,2) DEFAULT 0,
            priority_class  TEXT DEFAULT 'Low',
            trend           TEXT DEFAULT 'stable',
            override_active BOOLEAN DEFAULT FALSE,
            override_score  NUMERIC(5,2),
            override_class  TEXT,
            override_by     TEXT,
            override_reason TEXT,
            override_at     TIMESTAMP,
            last_computed   TIMESTAMP DEFAULT NOW(),
            created_at      TIMESTAMP DEFAULT NOW()
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS wo_priority_history (
            id              SERIAL PRIMARY KEY,
            wo_id           INTEGER NOT NULL,
            priority_score  NUMERIC(5,2),
            priority_class  TEXT,
            computed_at     TIMESTAMP DEFAULT NOW(),
            trigger_event   TEXT
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS wo_priority_drivers (
            id              SERIAL PRIMARY KEY,
            wo_id           INTEGER NOT NULL,
            factor_key      TEXT NOT NULL,
            factor_label    TEXT,
            points          NUMERIC(5,2),
            max_points      INTEGER,
            detail          TEXT,
            computed_at     TIMESTAMP DEFAULT NOW()
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS wo_priority_overrides (
            id              SERIAL PRIMARY KEY,
            wo_id           INTEGER NOT NULL,
            previous_score  NUMERIC(5,2),
            previous_class  TEXT,
            new_score       NUMERIC(5,2),
            new_class       TEXT,
            override_by     TEXT,
            override_reason TEXT,
            override_type   TEXT DEFAULT 'manual',
            created_at      TIMESTAMP DEFAULT NOW()
        )
    ''')
    conn.commit()


# ─── Scoring Engine ───────────────────────────────────────────────────────────

def compute_wo_priority(conn, wo_id, trigger_event='manual'):
    ensure_priority_tables(conn)

    wo = conn.execute('''
        SELECT wo.*, p.name as product_name, p.code as product_code,
               so.total_amount as so_value, so.expected_ship_date,
               c.credit_limit
        FROM work_orders wo
        JOIN products p ON wo.product_id = p.id
        LEFT JOIN sales_orders so ON wo.so_id = so.id
        LEFT JOIN customers c ON wo.customer_id = c.id
        WHERE wo.id = %s
    ''', (wo_id,)).fetchone()

    if not wo:
        return None

    drivers = {}
    total = 0.0

    # ── 1. AOG & Urgency  (0-25 pts) ─────────────────────────────────────────
    aog_pts = 0
    aog_detail = []
    if wo['is_aog']:
        aog_pts += 25
        aog_detail.append('AOG flag active (+25)')
    if wo['is_warranty']:
        aog_pts = min(aog_pts + 6, 25)
        aog_detail.append('Warranty obligation (+6)')
    if wo['workorder_type'] == 'External':
        aog_pts = min(aog_pts + 4, 25)
        aog_detail.append('External customer WO (+4)')
    drivers['aog_urgency'] = {
        'label': 'AOG / Urgency Flags',
        'points': aog_pts,
        'max': 25,
        'detail': ', '.join(aog_detail) if aog_detail else 'No urgent flags',
    }
    total += aog_pts

    # ── 2. Schedule / Due Date (0-20 pts) ────────────────────────────────────
    sched_pts = 0
    sched_detail = 'No due date set'
    due_raw = wo['planned_end_date'] or wo['expected_ship_date']
    if due_raw:
        try:
            if isinstance(due_raw, str):
                due = datetime.strptime(due_raw[:10], '%Y-%m-%d').date()
            else:
                due = due_raw if isinstance(due_raw, date) else due_raw.date()
            days_left = (due - date.today()).days
            if days_left < 0:
                sched_pts = 20
                sched_detail = f'Overdue by {abs(days_left)} days'
            elif days_left <= 3:
                sched_pts = 18
                sched_detail = f'Due in {days_left} day(s) — critical window'
            elif days_left <= 7:
                sched_pts = 14
                sched_detail = f'Due in {days_left} days — urgent'
            elif days_left <= 14:
                sched_pts = 9
                sched_detail = f'Due in {days_left} days — approaching'
            elif days_left <= 30:
                sched_pts = 4
                sched_detail = f'Due in {days_left} days'
            else:
                sched_pts = 0
                sched_detail = f'Due in {days_left} days — adequate lead time'
        except Exception:
            sched_detail = 'Due date parse error'
    drivers['schedule'] = {
        'label': 'Schedule / Due Date',
        'points': sched_pts,
        'max': 20,
        'detail': sched_detail,
    }
    total += sched_pts

    # ── 3. Financial & Downtime Impact (0-15 pts) ─────────────────────────────
    fin_pts = 0
    so_value = float(wo['so_value'] or 0)
    labor    = float(wo['labor_cost'] or 0)
    overhead = float(wo['overhead_cost'] or 0)
    total_exposure = so_value + labor + overhead
    if total_exposure >= 500_000:
        fin_pts = 15
    elif total_exposure >= 200_000:
        fin_pts = 11
    elif total_exposure >= 50_000:
        fin_pts = 7
    elif total_exposure >= 10_000:
        fin_pts = 3
    drivers['financial'] = {
        'label': 'Financial & Downtime Impact',
        'points': fin_pts,
        'max': 15,
        'detail': f'Total exposure: ${total_exposure:,.0f} (SO: ${so_value:,.0f} + Labour/OH: ${labor+overhead:,.0f})',
    }
    total += fin_pts

    # ── 4. Supplier / Material Risk (0-15 pts) ────────────────────────────────
    supp_pts = 0
    supp_detail = 'No material requirements'
    try:
        materials = conn.execute('''
            SELECT DISTINCT mr.product_id
            FROM material_requirements mr
            WHERE mr.work_order_id = %s
        ''', (wo_id,)).fetchall()

        if materials:
            product_ids = [m['product_id'] for m in materials]
            risk_rows = []
            for pid in product_ids:
                r = conn.execute('''
                    SELECT rp.risk_score
                    FROM supply_risk_profiles rp
                    JOIN products p ON p.id = rp.entity_id
                    WHERE rp.entity_type = 'supplier'
                      AND p.id = %s
                    ORDER BY rp.risk_score DESC LIMIT 1
                ''', (pid,)).fetchone()
                if r:
                    risk_rows.append(float(r['risk_score'] or 0))

            if risk_rows:
                avg_risk = sum(risk_rows) / len(risk_rows)
                max_risk = max(risk_rows)
                if max_risk >= 70 or avg_risk >= 60:
                    supp_pts = 15
                elif max_risk >= 50 or avg_risk >= 40:
                    supp_pts = 10
                elif max_risk >= 30:
                    supp_pts = 5
                supp_detail = f'Avg supplier risk: {avg_risk:.0f}/100, Peak: {max_risk:.0f}/100 across {len(materials)} material(s)'
            else:
                supp_detail = f'{len(materials)} material(s) — no supplier risk profiles found'
    except Exception as e:
        conn.rollback()
        supp_detail = 'Supplier risk data unavailable'

    drivers['supplier_risk'] = {
        'label': 'Supplier / Material Risk',
        'points': supp_pts,
        'max': 15,
        'detail': supp_detail,
    }
    total += supp_pts

    # ── 5. Inventory & Compliance Readiness (0-15 pts) ────────────────────────
    comp_pts = 0
    comp_detail = 'No material requirements to check'
    try:
        mat_rows = conn.execute('''
            SELECT mr.product_id, mr.required_quantity,
                   COALESCE(
                       (SELECT SUM(mi.quantity_issued) FROM material_issues mi
                        WHERE mi.work_order_id = mr.work_order_id AND mi.product_id = mr.product_id), 0
                   ) as issued_qty
            FROM material_requirements mr
            WHERE mr.work_order_id = %s
        ''', (wo_id,)).fetchall()

        if mat_rows:
            short_count = 0
            blocked_count = 0
            for m in mat_rows:
                if float(m['issued_qty'] or 0) < float(m['required_quantity'] or 0):
                    short_count += 1
                inv = conn.execute('''
                    SELECT icp.compliance_status
                    FROM inventory i
                    LEFT JOIN inv_compliance_profiles icp ON icp.inventory_id = i.id
                    WHERE i.product_id = %s LIMIT 1
                ''', (m['product_id'],)).fetchone()
                if inv and inv['compliance_status'] in ('Non-Compliant', 'Suspended'):
                    blocked_count += 1
            if blocked_count > 0:
                comp_pts = 15
                comp_detail = f'{blocked_count} material line(s) blocked by compliance; {short_count} short'
            elif short_count > 0:
                comp_pts = 8
                comp_detail = f'{short_count} of {len(mat_rows)} material line(s) not yet fully issued'
            else:
                comp_pts = 0
                comp_detail = f'All {len(mat_rows)} material line(s) issued and compliant'
    except Exception:
        conn.rollback()
        comp_detail = 'Compliance data unavailable'

    drivers['compliance_readiness'] = {
        'label': 'Inventory & Compliance Readiness',
        'points': comp_pts,
        'max': 15,
        'detail': comp_detail,
    }
    total += comp_pts

    # ── 6. Maintenance Strategy / Repair Type (0-10 pts) ─────────────────────
    maint_pts = 0
    maint_detail = []
    rc = wo['repair_category'] or ''
    disp = wo['disposition'] or ''
    if rc in ('Major Component', 'Engine', 'Avionics'):
        maint_pts += 7
        maint_detail.append(f'Critical repair category: {rc}')
    elif rc in ('Small Component',):
        maint_pts += 3
        maint_detail.append(f'Standard repair category: {rc}')
    if disp == 'Repair':
        maint_pts = min(maint_pts + 3, 10)
        maint_detail.append('Corrective repair disposition (+3)')

    drivers['maintenance_strategy'] = {
        'label': 'Maintenance Strategy & Repair Type',
        'points': maint_pts,
        'max': 10,
        'detail': ', '.join(maint_detail) if maint_detail else 'Standard manufacture / no elevated repair risk',
    }
    total += maint_pts

    # ── Clamp and classify ────────────────────────────────────────────────────
    score = min(round(total, 2), 100.0)
    if score >= 75:
        cls = 'Critical'
    elif score >= 50:
        cls = 'High'
    elif score >= 25:
        cls = 'Medium'
    else:
        cls = 'Low'

    # ── Trend (compare to last stored score) ─────────────────────────────────
    prev = conn.execute(
        'SELECT priority_score FROM wo_priority_profiles WHERE wo_id = %s', (wo_id,)
    ).fetchone()
    prev_score = float(prev['priority_score']) if prev else None
    if prev_score is None:
        trend = 'stable'
    elif score > prev_score + 2:
        trend = 'rising'
    elif score < prev_score - 2:
        trend = 'falling'
    else:
        trend = 'stable'

    # ── Persist profile ───────────────────────────────────────────────────────
    conn.execute('''
        INSERT INTO wo_priority_profiles (wo_id, priority_score, priority_class, trend, last_computed)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (wo_id) DO UPDATE SET
            priority_score = EXCLUDED.priority_score,
            priority_class = EXCLUDED.priority_class,
            trend          = EXCLUDED.trend,
            last_computed  = EXCLUDED.last_computed
    ''', (wo_id, score, cls, trend))

    # ── History snapshot ──────────────────────────────────────────────────────
    conn.execute('''
        INSERT INTO wo_priority_history (wo_id, priority_score, priority_class, trigger_event)
        VALUES (%s, %s, %s, %s)
    ''', (wo_id, score, cls, trigger_event))

    # ── Persist drivers (replace current) ────────────────────────────────────
    conn.execute('DELETE FROM wo_priority_drivers WHERE wo_id = %s', (wo_id,))
    for key, d in drivers.items():
        conn.execute('''
            INSERT INTO wo_priority_drivers (wo_id, factor_key, factor_label, points, max_points, detail)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (wo_id, key, d['label'], d['points'], d['max'], d['detail']))

    conn.commit()

    # ── Build impact-if-delayed narrative ────────────────────────────────────
    impact_lines = []
    if wo['is_aog']:
        impact_lines.append('Aircraft on Ground — every hour of delay risks flight schedule disruption and potential regulatory non-compliance.')
    if sched_pts >= 14:
        impact_lines.append(f'Due date pressure ({sched_detail}) — delay increases late-delivery penalty exposure.')
    if fin_pts >= 11:
        impact_lines.append(f'High financial exposure (${total_exposure:,.0f}) — inaction risks revenue loss and SLA breach.')
    if supp_pts >= 10:
        impact_lines.append('Elevated supplier risk — parts may become unavailable; delaying increases procurement volatility.')
    if comp_pts >= 8:
        impact_lines.append('Material shortfall or compliance blocks — delay extends the wait for compliant stock, compounding schedule risk.')
    if not impact_lines:
        impact_lines.append('No immediate critical delay impact identified. Monitor as conditions evolve.')

    return {
        'wo_id': wo_id,
        'wo_number': wo['wo_number'],
        'priority_score': score,
        'priority_class': cls,
        'trend': trend,
        'drivers': drivers,
        'impact_if_delayed': ' '.join(impact_lines),
        'computed_at': datetime.now().isoformat(),
    }


def _class_color(cls):
    return {'Critical': 'danger', 'High': 'warning', 'Medium': 'info', 'Low': 'success'}.get(cls, 'secondary')


# ─── API Endpoints ────────────────────────────────────────────────────────────

@wo_priority_bp.route('/api/wo/priority/<int:wo_id>')
@login_required
def get_wo_priority(wo_id):
    db = Database()
    conn = db.get_connection()
    try:
        result = compute_wo_priority(conn, wo_id, trigger_event='page_load')
        if not result:
            return jsonify({'error': 'Work order not found'}), 404

        profile = conn.execute(
            'SELECT * FROM wo_priority_profiles WHERE wo_id = %s', (wo_id,)
        ).fetchone()

        override_active = profile and profile['override_active']
        effective_score = float(profile['override_score']) if override_active and profile['override_score'] else result['priority_score']
        effective_class = profile['override_class'] if override_active and profile['override_class'] else result['priority_class']

        history = conn.execute('''
            SELECT priority_score, priority_class, computed_at
            FROM wo_priority_history
            WHERE wo_id = %s
            ORDER BY computed_at DESC LIMIT 10
        ''', (wo_id,)).fetchall()

        return jsonify({
            'wo_id': wo_id,
            'wo_number': result['wo_number'],
            'ai_score': result['priority_score'],
            'ai_class': result['priority_class'],
            'effective_score': effective_score,
            'effective_class': effective_class,
            'effective_color': _class_color(effective_class),
            'trend': result['trend'],
            'override_active': bool(override_active),
            'override_by': profile['override_by'] if profile else None,
            'override_reason': profile['override_reason'] if profile else None,
            'drivers': result['drivers'],
            'impact_if_delayed': result['impact_if_delayed'],
            'history': [
                {'score': float(h['priority_score']), 'class': h['priority_class'], 'at': str(h['computed_at'])}
                for h in history
            ],
            'computed_at': result['computed_at'],
        })
    finally:
        conn.close()


@wo_priority_bp.route('/api/wo/priority/<int:wo_id>/override', methods=['POST'])
@login_required
def override_wo_priority(wo_id):
    data = request.get_json() or {}
    new_score = data.get('score')
    new_class  = data.get('class')
    reason     = data.get('reason', '').strip()
    clear      = data.get('clear', False)

    if not clear and (new_score is None or not reason):
        return jsonify({'error': 'score and reason are required'}), 400

    db = Database()
    conn = db.get_connection()
    try:
        ensure_priority_tables(conn)

        profile = conn.execute(
            'SELECT * FROM wo_priority_profiles WHERE wo_id = %s', (wo_id,)
        ).fetchone()

        if clear:
            conn.execute('''
                UPDATE wo_priority_profiles
                SET override_active=FALSE, override_score=NULL, override_class=NULL,
                    override_by=NULL, override_reason=NULL, override_at=NULL
                WHERE wo_id = %s
            ''', (wo_id,))
            conn.execute('''
                INSERT INTO wo_priority_overrides
                    (wo_id, previous_score, previous_class, new_score, new_class,
                     override_by, override_reason, override_type)
                VALUES (%s, %s, %s, NULL, NULL, %s, %s, 'cleared')
            ''', (
                wo_id,
                float(profile['override_score']) if profile and profile['override_score'] else None,
                profile['override_class'] if profile else None,
                session.get('username', 'system'),
                'Override cleared',
            ))
            conn.commit()
            return jsonify({'message': 'Override cleared — AI-recommended priority restored.'})

        score  = float(new_score)
        if not new_class:
            if score >= 75:   new_class = 'Critical'
            elif score >= 50: new_class = 'High'
            elif score >= 25: new_class = 'Medium'
            else:             new_class = 'Low'

        prev_score = float(profile['priority_score']) if profile else None
        prev_class = profile['priority_class'] if profile else None

        conn.execute('''
            INSERT INTO wo_priority_profiles
                (wo_id, priority_score, priority_class, override_active, override_score,
                 override_class, override_by, override_reason, override_at)
            VALUES (%s, COALESCE((SELECT priority_score FROM wo_priority_profiles WHERE wo_id=%s), 0),
                    COALESCE((SELECT priority_class FROM wo_priority_profiles WHERE wo_id=%s), 'Low'),
                    TRUE, %s, %s, %s, %s, NOW())
            ON CONFLICT (wo_id) DO UPDATE SET
                override_active = TRUE,
                override_score  = EXCLUDED.override_score,
                override_class  = EXCLUDED.override_class,
                override_by     = EXCLUDED.override_by,
                override_reason = EXCLUDED.override_reason,
                override_at     = EXCLUDED.override_at
        ''', (wo_id, wo_id, wo_id, score, new_class,
              session.get('username', 'system'), reason))

        conn.execute('''
            INSERT INTO wo_priority_overrides
                (wo_id, previous_score, previous_class, new_score, new_class,
                 override_by, override_reason, override_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'manual')
        ''', (wo_id, prev_score, prev_class, score, new_class,
              session.get('username', 'system'), reason))

        conn.commit()
        return jsonify({'message': f'Priority overridden to {new_class} ({score}/100). Override logged.', 'class': new_class, 'score': score})
    finally:
        conn.close()


@wo_priority_bp.route('/api/wo/priority/queue')
@login_required
def priority_queue():
    db = Database()
    conn = db.get_connection()
    try:
        ensure_priority_tables(conn)
        wos = conn.execute('''
            SELECT wo.id, wo.wo_number, wo.status, wo.is_aog,
                   p.code as product_code, p.name as product_name,
                   COALESCE(pp.override_score, pp.priority_score, 0) as eff_score,
                   COALESCE(pp.override_class, pp.priority_class, 'Low') as eff_class,
                   pp.trend, pp.override_active, pp.last_computed
            FROM work_orders wo
            JOIN products p ON p.id = wo.product_id
            LEFT JOIN wo_priority_profiles pp ON pp.wo_id = wo.id
            WHERE wo.status NOT IN ('Completed', 'Cancelled', 'Closed')
            ORDER BY eff_score DESC, wo.id ASC
        ''').fetchall()
        return jsonify([dict(w) for w in wos])
    finally:
        conn.close()


@wo_priority_bp.route('/api/wo/priority/recalculate-all', methods=['POST'])
@login_required
def recalculate_all():
    if session.get('role') not in ('Admin', 'Planner'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    db = Database()
    conn = db.get_connection()
    try:
        ensure_priority_tables(conn)
        wos = conn.execute('''
            SELECT id FROM work_orders
            WHERE status NOT IN ('Completed', 'Cancelled', 'Closed')
        ''').fetchall()
        updated = 0
        for w in wos:
            try:
                compute_wo_priority(conn, w['id'], trigger_event='bulk_recalculate')
                updated += 1
            except Exception as ex:
                conn.rollback()
                log.warning(f'Priority recalc failed for WO {w["id"]}: {ex}')
        return jsonify({'message': f'Recalculated {updated} work orders.'})
    finally:
        conn.close()
