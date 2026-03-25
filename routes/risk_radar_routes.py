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


@risk_radar_bp.route('/api/risk-radar/customer/<int:customer_id>')
@login_required
def customer_risk_detail(customer_id):
    """Compute and return a live customer risk score from transactional data."""
    db = Database()
    conn = db.get_connection()
    try:
        customer = conn.execute(
            'SELECT * FROM customers WHERE id = %s', (customer_id,)
        ).fetchone()
        if not customer:
            return jsonify({'error': 'Customer not found'}), 404

        credit_limit = float(customer['credit_limit'] or 0)
        factors = {}
        total_risk = 0

        # ── 1. Credit utilisation risk (0-30 pts) ────────────────────────────
        open_order_value = conn.execute('''
            SELECT COALESCE(SUM(total_amount), 0) as val
            FROM sales_orders
            WHERE customer_id = %s AND status IN ('Pending', 'Confirmed', 'In Progress', 'Open')
        ''', (customer_id,)).fetchone()['val'] or 0

        if credit_limit > 0:
            utilisation = (float(open_order_value) / credit_limit) * 100
        else:
            utilisation = 0

        if utilisation >= 90:
            credit_pts = 30
        elif utilisation >= 70:
            credit_pts = 18
        elif utilisation >= 50:
            credit_pts = 8
        else:
            credit_pts = 0

        factors['credit_exposure'] = {
            'label': 'Credit Exposure',
            'risk_contribution': credit_pts,
            'note': f'Open order value: ${float(open_order_value):,.0f} / Credit limit: ${credit_limit:,.0f} ({utilisation:.1f}% utilised)',
        }
        total_risk += credit_pts

        # ── 2. Overdue invoices risk (0-25 pts) ──────────────────────────────
        try:
            overdue = conn.execute('''
                SELECT COALESCE(SUM(total_amount - amount_paid), 0) as overdue_amt,
                       COUNT(*) as overdue_count
                FROM vendor_invoices
                WHERE customer_id = %s
                  AND due_date < CURRENT_DATE
                  AND status NOT IN ('Paid', 'Voided', 'Cancelled')
            ''', (customer_id,)).fetchone()
            overdue_amt = float(overdue['overdue_amt'] or 0)
            overdue_count = int(overdue['overdue_count'] or 0)
        except Exception:
            overdue_amt = 0
            overdue_count = 0

        if overdue_amt > 0 and credit_limit > 0:
            overdue_pct = (overdue_amt / credit_limit) * 100
            if overdue_pct >= 30 or overdue_count >= 3:
                inv_pts = 25
            elif overdue_pct >= 15 or overdue_count >= 2:
                inv_pts = 15
            else:
                inv_pts = 8
        elif overdue_count > 0:
            inv_pts = 10
        else:
            inv_pts = 0

        factors['overdue_invoices'] = {
            'label': 'Overdue Invoices',
            'risk_contribution': inv_pts,
            'count': overdue_count,
            'note': f'${overdue_amt:,.0f} overdue across {overdue_count} invoice(s)',
        }
        total_risk += inv_pts

        # ── 3. Activity / recency risk (0-20 pts) ────────────────────────────
        last_order = conn.execute('''
            SELECT order_date FROM sales_orders
            WHERE customer_id = %s AND status NOT IN ('Cancelled', 'Draft')
            ORDER BY order_date DESC LIMIT 1
        ''', (customer_id,)).fetchone()

        if last_order and last_order['order_date']:
            from datetime import date as _date
            try:
                from datetime import datetime as _dt
                lo = last_order['order_date']
                if isinstance(lo, str):
                    lo = _dt.strptime(lo[:10], '%Y-%m-%d').date()
                days_since = (_date.today() - lo).days
            except Exception:
                days_since = 0
        else:
            days_since = 9999

        if days_since > 365:
            activity_pts = 20
        elif days_since > 180:
            activity_pts = 12
        elif days_since > 90:
            activity_pts = 5
        else:
            activity_pts = 0

        factors['activity'] = {
            'label': 'Order Activity',
            'risk_contribution': activity_pts,
            'note': f'Last active order: {"Never" if days_since == 9999 else f"{days_since} days ago"}',
        }
        total_risk += activity_pts

        # ── 4. Cancellation rate risk (0-15 pts) ─────────────────────────────
        order_stats = conn.execute('''
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled
            FROM sales_orders WHERE customer_id = %s
        ''', (customer_id,)).fetchone()

        total_orders = int(order_stats['total'] or 0)
        cancelled = int(order_stats['cancelled'] or 0)
        cancel_rate = (cancelled / total_orders * 100) if total_orders > 0 else 0

        if cancel_rate >= 30:
            cancel_pts = 15
        elif cancel_rate >= 15:
            cancel_pts = 8
        elif cancel_rate >= 5:
            cancel_pts = 3
        else:
            cancel_pts = 0

        factors['cancellation_rate'] = {
            'label': 'Cancellation Rate',
            'risk_contribution': cancel_pts,
            'rate_pct': round(cancel_rate, 1),
            'data_points': total_orders,
            'note': f'{cancelled} cancelled of {total_orders} total orders ({cancel_rate:.1f}%)',
        }
        total_risk += cancel_pts

        # ── 5. Volume trend risk (0-10 pts) ──────────────────────────────────
        from datetime import datetime as _dts
        ytd_start = _dts.now().replace(month=1, day=1, hour=0, minute=0, second=0).date().isoformat()
        ly_start = str(int(ytd_start[:4]) - 1) + ytd_start[4:]
        ly_end   = str(int(ytd_start[:4]) - 1) + '-12-31'

        ytd_sales = conn.execute('''
            SELECT COALESCE(SUM(total_amount), 0) as val FROM sales_orders
            WHERE customer_id = %s AND status NOT IN ('Cancelled') AND order_date >= %s
        ''', (customer_id, ytd_start)).fetchone()['val'] or 0

        ly_sales = conn.execute('''
            SELECT COALESCE(SUM(total_amount), 0) as val FROM sales_orders
            WHERE customer_id = %s AND status NOT IN ('Cancelled')
              AND order_date >= %s AND order_date <= %s
        ''', (customer_id, ly_start, ly_end)).fetchone()['val'] or 0

        if ly_sales > 0:
            trend_change = ((float(ytd_sales) - float(ly_sales)) / float(ly_sales)) * 100
        else:
            trend_change = 0

        if trend_change < -50:
            trend_pts = 10
            trend_label = 'Significant decline vs LY'
        elif trend_change < -20:
            trend_pts = 6
            trend_label = 'Moderate decline vs LY'
        elif trend_change < 0:
            trend_pts = 2
            trend_label = 'Slight decline vs LY'
        else:
            trend_pts = 0
            trend_label = 'Stable or growing'

        factors['volume_trend'] = {
            'label': 'Sales Volume Trend',
            'risk_contribution': trend_pts,
            'note': f'YTD: ${float(ytd_sales):,.0f} vs LY: ${float(ly_sales):,.0f} — {trend_label}',
        }
        total_risk += trend_pts

        # ── 6. Customer status risk (0 or 10 pts) ────────────────────────────
        if customer.get('status') == 'Inactive':
            status_pts = 10
        else:
            status_pts = 0
        factors['status'] = {
            'label': 'Account Status',
            'risk_contribution': status_pts,
            'note': customer.get('status', 'Active'),
        }
        total_risk += status_pts

        # ── Risk level bucketing ──────────────────────────────────────────────
        total_risk = min(total_risk, 100)
        if total_risk <= 20:
            risk_level = 'Low'
            trend = 'stable'
        elif total_risk <= 45:
            risk_level = 'Medium'
            trend = 'stable'
        elif total_risk <= 70:
            risk_level = 'High'
            trend = 'degrading'
        else:
            risk_level = 'Critical'
            trend = 'degrading'

        # ── Mitigation recommendations ────────────────────────────────────────
        mitigations = []
        if credit_pts >= 18:
            mitigations.append({'urgency': 'High', 'action': 'Review credit limit',
                'detail': 'Open order value is consuming most of the credit limit. Consider requiring payment on account.'})
        if inv_pts >= 15:
            mitigations.append({'urgency': 'Critical', 'action': 'Chase overdue invoices',
                'detail': f'{overdue_count} overdue invoice(s) totalling ${overdue_amt:,.0f}. Escalate to collections.'})
        if activity_pts >= 12:
            mitigations.append({'urgency': 'Medium', 'action': 'Re-engagement outreach',
                'detail': f'Customer has not placed an active order in {days_since} days. Schedule account review.'})
        if cancel_pts >= 8:
            mitigations.append({'urgency': 'Medium', 'action': 'Investigate cancellations',
                'detail': f'Cancellation rate of {cancel_rate:.0f}% is above threshold. Identify root causes.'})
        if trend_pts >= 6:
            mitigations.append({'urgency': 'Medium', 'action': 'Account growth review',
                'detail': 'Year-over-year sales are declining. Schedule a business review meeting.'})

        return jsonify({
            'customer_id': customer_id,
            'risk_score': round(total_risk, 1),
            'risk_level': risk_level,
            'trend': trend,
            'score_breakdown': factors,
            'mitigation_recommendations': mitigations,
        })
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
