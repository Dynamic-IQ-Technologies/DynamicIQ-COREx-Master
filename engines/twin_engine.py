"""
Digital Twin Engine — sync state from live ERP data, run what-if simulations.
"""
import json
import logging
import os
from datetime import date, datetime, timedelta

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def _f(v, d=0.0):
    try:
        return float(v) if v is not None else d
    except (TypeError, ValueError):
        return d


def _estimate_daily_demand(conn, product_id, product_row=None):
    """Estimate average daily demand for a product from SO history, then WO, then reorder params."""
    row = conn.execute('''
        SELECT COALESCE(SUM(sol.quantity), 0) / 180.0 AS daily
        FROM sales_order_lines sol
        JOIN sales_orders so ON sol.so_id = so.id
        WHERE sol.product_id = %s
          AND so.created_at >= CURRENT_DATE - INTERVAL '180 days'
    ''', (product_id,)).fetchone()
    d = _f(row['daily'])

    if d <= 0:
        # Try work-order task materials
        row2 = conn.execute('''
            SELECT COALESCE(SUM(quantity), 0) / 90.0 AS daily
            FROM work_order_task_materials
            WHERE product_id = %s
              AND created_at >= CURRENT_DATE - INTERVAL '90 days'
        ''', (product_id,)).fetchone()
        d = _f(row2['daily'])

    if d <= 0 and product_row:
        rq = max(_f(product_row.get('reorder_qty')), _f(product_row.get('safety_stock')))
        d = max(0.05, rq / 90.0)

    return max(0.05, d)


def _estimate_hourly_rate(conn):
    """Estimate revenue per operational hour from recent SO history."""
    row = conn.execute('''
        SELECT COALESCE(SUM(sol.line_total), 0) AS rev
        FROM sales_order_lines sol
        JOIN sales_orders so ON sol.so_id = so.id
        WHERE so.created_at >= CURRENT_DATE - INTERVAL '90 days'
    ''').fetchone()
    rev_90d = _f(row['rev'])
    if rev_90d > 0:
        return rev_90d / (90 * 8)
    return 200.0  # aerospace MRO default: $200/hour


def _generate_ai_summary(scenario_type, entity_name, params, impact):
    """Generate a plain-language executive summary using GPT-4o-mini."""
    try:
        from openai import OpenAI
        client = OpenAI(
            api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
            base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL'),
            timeout=10.0,
        )
        labels = {
            'supplier_failure': f"supplier {entity_name} fails for {params.get('failure_duration_days', 30)} days",
            'lead_time_increase': f"lead times increase by {params.get('increase_pct', 50)}%",
            'demand_spike': f"demand spikes by {params.get('increase_pct', 30)}%",
            'maintenance_deferral': f"maintenance is deferred by {params.get('deferral_weeks', 4)} weeks",
        }
        scenario_desc = labels.get(scenario_type, scenario_type)
        prompt = (
            f"Simulation scenario: if {scenario_desc}.\n"
            f"Results: downtime={impact.get('downtime_hours',0):.0f}h, "
            f"revenue impact=${impact.get('revenue_impact_usd',0):,.0f}, "
            f"parts at risk={impact.get('parts_at_risk',0)}, "
            f"blocked work orders={impact.get('blocked_wos',0)}.\n"
            f"Write ONE concise executive sentence (max 30 words) starting with 'If'. "
            f"No markdown, no special characters."
        )
        resp = client.chat.completions.create(
            model='gpt-4o-mini',
            messages=[{'role': 'user', 'content': prompt}],
            temperature=0.15,
            max_tokens=60,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        logger.warning(f"AI summary failed: {e}")
        return None


# ─────────────────────────────────────────────
# TWIN SYNC
# ─────────────────────────────────────────────
def sync_digital_twins(conn):
    """Rebuild digital twin snapshots from live ERP data."""
    results = {'suppliers': 0, 'inventory': 0, 'schedule': 0}

    # Supplier twins
    suppliers = conn.execute(
        "SELECT id, name, country FROM suppliers WHERE status != 'Inactive' OR status IS NULL"
    ).fetchall()
    for s in suppliers:
        state = _build_supplier_state(conn, s['id'])
        conn.execute('''
            INSERT INTO digital_twins (twin_type, entity_id, entity_name, state_snapshot)
            VALUES ('supplier', %s, %s, %s)
            ON CONFLICT (twin_type, entity_id) DO UPDATE SET
                entity_name = EXCLUDED.entity_name,
                state_snapshot = EXCLUDED.state_snapshot,
                last_synced = CURRENT_TIMESTAMP,
                version = digital_twins.version + 1
        ''', (s['id'], s['name'], json.dumps(state)))
        results['suppliers'] += 1

    # Inventory twins (one per product that has inventory or PO history)
    products = conn.execute('''
        SELECT DISTINCT p.id, p.code, p.name, p.lead_time_days, p.lead_time,
               COALESCE(p.cost, 0) AS cost, p.safety_stock, p.reorder_point, p.reorder_qty
        FROM products p
        WHERE p.id IN (
            SELECT product_id FROM inventory
            UNION SELECT product_id FROM purchase_order_lines
            UNION SELECT product_id FROM work_order_task_materials
        )
        ORDER BY p.code
    ''').fetchall()
    for p in products:
        state = _build_inventory_state(conn, p)
        conn.execute('''
            INSERT INTO digital_twins (twin_type, entity_id, entity_name, state_snapshot)
            VALUES ('inventory', %s, %s, %s)
            ON CONFLICT (twin_type, entity_id) DO UPDATE SET
                entity_name = EXCLUDED.entity_name,
                state_snapshot = EXCLUDED.state_snapshot,
                last_synced = CURRENT_TIMESTAMP,
                version = digital_twins.version + 1
        ''', (p['id'], p['code'], json.dumps(state)))
        results['inventory'] += 1

    # Operations / maintenance schedule twin (entity_id = 0)
    sched = _build_schedule_state(conn)
    conn.execute('''
        INSERT INTO digital_twins (twin_type, entity_id, entity_name, state_snapshot)
        VALUES ('schedule', 0, 'Operations Schedule', %s)
        ON CONFLICT (twin_type, entity_id) DO UPDATE SET
            state_snapshot = EXCLUDED.state_snapshot,
            last_synced = CURRENT_TIMESTAMP,
            version = digital_twins.version + 1
    ''', (json.dumps(sched),))
    results['schedule'] = 1

    conn.commit()
    return results


def _build_supplier_state(conn, supplier_id):
    pos = conn.execute('''
        SELECT expected_delivery_date, actual_delivery_date, order_date, status,
               COALESCE(grand_total, 0) AS total
        FROM purchase_orders WHERE supplier_id = %s ORDER BY order_date DESC LIMIT 20
    ''', (supplier_id,)).fetchall()
    total_pos = len(pos)
    on_time = sum(1 for p in pos if p['actual_delivery_date'] and
                  p['actual_delivery_date'] <= p['expected_delivery_date'])
    otif = round(on_time / total_pos * 100, 1) if total_pos else None

    parts = conn.execute('''
        SELECT DISTINCT pol.product_id, p.code, p.name
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        JOIN products p ON pol.product_id = p.id
        WHERE po.supplier_id = %s
    ''', (supplier_id,)).fetchall()

    open_po = conn.execute('''
        SELECT COUNT(*) AS cnt, COALESCE(SUM(grand_total), 0) AS val
        FROM purchase_orders WHERE supplier_id = %s
          AND status IN ('Ordered', 'Partially Received')
    ''', (supplier_id,)).fetchone()

    return {
        'otif_rate': otif,
        'total_pos_history': total_pos,
        'parts_supplied': len(parts),
        'parts': [{'id': p['product_id'], 'code': p['code'], 'name': p['name']} for p in parts],
        'open_pos_count': int(open_po['cnt']),
        'open_pos_value': _f(open_po['val']),
    }


def _build_inventory_state(conn, product):
    pid = product['id']
    inv = conn.execute('''
        SELECT COALESCE(SUM(quantity), 0) AS qty,
               COALESCE(SUM(reserved_quantity), 0) AS reserved
        FROM inventory WHERE product_id = %s AND (status IS NULL OR status = 'Serviceable')
    ''', (pid,)).fetchone()
    on_hand = _f(inv['qty'])
    reserved = _f(inv['reserved'])
    available = max(0.0, on_hand - reserved)

    daily = _estimate_daily_demand(conn, pid, product)
    days_cov = round(available / daily, 1) if daily > 0 else 999.0

    in_transit = conn.execute('''
        SELECT COALESCE(SUM(pol.quantity - COALESCE(pol.received_quantity, 0)), 0) AS qty
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        WHERE pol.product_id = %s AND po.status IN ('Ordered', 'Partially Received')
    ''', (pid,)).fetchone()
    lead_days = int(product['lead_time_days'] or product['lead_time'] or 30)

    return {
        'on_hand': on_hand,
        'available': available,
        'reserved': reserved,
        'in_transit': _f(in_transit['qty']),
        'daily_demand': round(daily, 4),
        'days_coverage': days_cov,
        'safety_stock': _f(product['safety_stock']),
        'reorder_point': _f(product['reorder_point']),
        'reorder_qty': _f(product['reorder_qty']),
        'lead_time_days': lead_days,
        'unit_cost': _f(product['cost']),
    }


def _build_schedule_state(conn):
    wo_row = conn.execute('''
        SELECT COUNT(*) AS cnt, COALESCE(SUM(labor_cost), 0) AS labor
        FROM work_orders WHERE status NOT IN ('Closed', 'Cancelled')
    ''').fetchone()
    overdue = conn.execute('''
        SELECT COUNT(*) AS cnt FROM work_orders
        WHERE status NOT IN ('Closed', 'Cancelled')
          AND planned_end_date < CURRENT_DATE AND planned_end_date IS NOT NULL
    ''').fetchone()
    tasks = conn.execute('''
        SELECT COALESCE(SUM(planned_hours), 0) AS ph, COALESCE(SUM(actual_hours), 0) AS ah
        FROM work_order_tasks wot
        JOIN work_orders wo ON wot.work_order_id = wo.id
        WHERE wo.status NOT IN ('Closed', 'Cancelled')
    ''').fetchone()
    return {
        'open_wo_count': int(wo_row['cnt']),
        'open_labor_cost': _f(wo_row['labor']),
        'overdue_wo_count': int(overdue['cnt']),
        'planned_task_hours': _f(tasks['ph']),
        'actual_task_hours': _f(tasks['ah']),
    }


# ─────────────────────────────────────────────
# SCENARIO SIMULATIONS
# ─────────────────────────────────────────────
def run_simulation(conn, scenario_type, parameters, sim_name=None, created_by=None):
    """
    Runs a what-if simulation and saves results to twin_simulations.
    Returns the simulation record id.
    """
    hourly_rate = _estimate_hourly_rate(conn)

    dispatch = {
        'supplier_failure':    _sim_supplier_failure,
        'lead_time_increase':  _sim_lead_time_increase,
        'demand_spike':        _sim_demand_spike,
        'maintenance_deferral': _sim_maintenance_deferral,
    }
    fn = dispatch.get(scenario_type)
    if not fn:
        raise ValueError(f"Unknown scenario type: {scenario_type}")

    current_state, simulated_state, impact, mitigations = fn(conn, parameters, hourly_rate)

    entity_name = (
        current_state.get('supplier_name') or
        current_state.get('label') or
        scenario_type
    )
    summary = _generate_ai_summary(scenario_type, entity_name, parameters, impact)

    # Persist
    row = conn.execute('''
        INSERT INTO twin_simulations
            (name, scenario_type, parameters, status, current_state, simulated_state,
             impact_kpis, executive_summary, mitigations, confidence_level,
             created_by, completed_at)
        VALUES (%s, %s, %s, 'complete', %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        RETURNING id
    ''', (
        sim_name or f"{scenario_type.replace('_', ' ').title()}",
        scenario_type,
        json.dumps(parameters),
        json.dumps(current_state),
        json.dumps(simulated_state),
        json.dumps(impact),
        summary,
        json.dumps(mitigations),
        impact.get('confidence', 'Medium'),
        created_by,
    )).fetchone()
    conn.commit()
    return row['id']


# ── Supplier failure ──────────────────────────
def _sim_supplier_failure(conn, params, hourly_rate):
    supplier_id = int(params['supplier_id'])
    failure_days = int(params.get('failure_duration_days', 30))

    sup = conn.execute('SELECT name, country FROM suppliers WHERE id = %s', (supplier_id,)).fetchone()
    if not sup:
        raise ValueError(f"Supplier {supplier_id} not found")

    parts = conn.execute('''
        SELECT DISTINCT pol.product_id, p.code, p.name,
               COALESCE(p.lead_time_days, p.lead_time, 30) AS lead_days,
               COALESCE(p.cost, 0) AS cost,
               p.safety_stock, p.reorder_qty
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        JOIN products p ON pol.product_id = p.id
        WHERE po.supplier_id = %s
    ''', (supplier_id,)).fetchall()

    parts_analysis = []
    blocked_wo_total = 0
    revenue_risk = 0.0
    buffer_cost = 0.0
    downtime_hours = 0.0

    for pt in parts:
        pid = pt['product_id']
        daily = _estimate_daily_demand(conn, pid, pt)

        inv_row = conn.execute('''
            SELECT COALESCE(SUM(quantity), 0) AS qty FROM inventory
            WHERE product_id = %s AND (status IS NULL OR status = 'Serviceable')
        ''', (pid,)).fetchone()
        on_hand = _f(inv_row['qty'])
        in_transit = _f(conn.execute('''
            SELECT COALESCE(SUM(pol2.quantity - COALESCE(pol2.received_quantity,0)),0) AS qty
            FROM purchase_order_lines pol2
            JOIN purchase_orders po2 ON pol2.po_id=po2.id
            WHERE pol2.product_id=%s AND po2.status IN ('Ordered','Partially Received')
        ''', (pid,)).fetchone()['qty'])

        effective_stock = on_hand + in_transit
        days_cov = effective_stock / daily if daily > 0 else 999.0
        at_risk = days_cov < failure_days

        shortage_qty = 0
        shortage_date = None
        if at_risk:
            shortage_date = (date.today() + timedelta(days=max(0, int(days_cov)))).isoformat()
            shortage_qty = max(0, round((failure_days - days_cov) * daily))
            buffer_cost += shortage_qty * _f(pt['cost'])

            # WOs needing this part
            wo_rows = conn.execute('''
                SELECT DISTINCT wo.id FROM work_orders wo
                WHERE wo.status NOT IN ('Closed','Cancelled')
                  AND (
                    wo.id IN (SELECT work_order_id FROM material_requirements WHERE product_id=%s)
                    OR wo.id IN (SELECT work_order_id FROM work_order_task_materials WHERE product_id=%s)
                    OR wo.product_id = %s
                  )
            ''', (pid, pid, pid)).fetchall()
            blocked_wo_total += len(wo_rows)

            # Task hours for those WOs
            if wo_rows:
                wo_ids = [r['id'] for r in wo_rows]
                ph = _f(conn.execute('''
                    SELECT COALESCE(SUM(planned_hours),0) AS h FROM work_order_tasks
                    WHERE work_order_id = ANY(%s)
                ''', (wo_ids,)).fetchone()['h'])
                downtime_hours += ph if ph > 0 else len(wo_rows) * 8.0

            # Revenue at risk from open SO lines
            so_val = _f(conn.execute('''
                SELECT COALESCE(SUM(sol.line_total),0) AS val
                FROM sales_order_lines sol
                JOIN sales_orders so ON sol.so_id=so.id
                WHERE sol.product_id=%s AND so.status NOT IN ('Cancelled','Closed')
            ''', (pid,)).fetchone()['val'])
            revenue_risk += so_val

        parts_analysis.append({
            'product_id': pid,
            'code': pt['code'],
            'name': pt['name'],
            'on_hand': on_hand,
            'in_transit': in_transit,
            'daily_demand': round(daily, 3),
            'days_coverage': round(days_cov, 1),
            'at_risk': at_risk,
            'shortage_date': shortage_date,
            'shortage_qty': shortage_qty,
            'unit_cost': _f(pt['cost']),
        })

    downtime_cost = round(downtime_hours * hourly_rate)
    total_impact = revenue_risk + downtime_cost
    parts_at_risk = sum(1 for p in parts_analysis if p['at_risk'])

    current_state = {
        'supplier_name': sup['name'],
        'supplier_country': sup['country'] or 'Unknown',
        'parts_supplied': len(parts),
        'all_parts': parts_analysis,
    }
    simulated_state = {
        'failure_duration_days': failure_days,
        'parts_at_risk': parts_at_risk,
        'parts_detail': [p for p in parts_analysis if p['at_risk']],
        'blocked_wos': blocked_wo_total,
        'downtime_hours': round(downtime_hours, 1),
        'revenue_at_risk_usd': round(revenue_risk),
        'downtime_cost_usd': downtime_cost,
        'total_impact_usd': round(total_impact),
        'buffer_cost_usd': round(buffer_cost),
    }
    impact = {
        'downtime_hours': round(downtime_hours, 1),
        'revenue_impact_usd': round(total_impact),
        'parts_at_risk': parts_at_risk,
        'blocked_wos': blocked_wo_total,
        'confidence': 'High' if len(parts) >= 2 else 'Medium',
    }
    mitigations = []
    for p in [x for x in parts_analysis if x['at_risk']]:
        mitigations.append({
            'action': f"Pre-order {max(1, p['shortage_qty'])} units of {p['code']}",
            'detail': f"Current stock covers {p['days_coverage']} days; {failure_days}-day gap creates shortage on {p['shortage_date']}.",
            'urgency': 'Critical',
            'risk_reduction_pct': 40,
            'cost_impact': p['shortage_qty'] * p['unit_cost'],
            'time_impact_days': -failure_days,
        })
    mitigations.append({
        'action': f"Qualify an alternate supplier for {sup['name']} parts",
        'detail': 'Dual-sourcing reduces single-supplier dependency.',
        'urgency': 'High',
        'risk_reduction_pct': 55,
        'cost_impact': 5000,
        'time_impact_days': 30,
    })
    if blocked_wo_total > 0:
        mitigations.append({
            'action': f"Prioritise {blocked_wo_total} work order(s) at risk",
            'detail': 'Accelerate completion before potential shortage date.',
            'urgency': 'High',
            'risk_reduction_pct': 20,
            'cost_impact': 0,
            'time_impact_days': -7,
        })
    return current_state, simulated_state, impact, mitigations


# ── Lead time increase ────────────────────────
def _sim_lead_time_increase(conn, params, hourly_rate):
    increase_pct = _f(params.get('increase_pct', 50))
    multiplier = 1.0 + increase_pct / 100.0

    products = conn.execute('''
        SELECT p.id, p.code, p.name,
               COALESCE(p.lead_time_days, p.lead_time, 30) AS lead_days,
               COALESCE(p.cost, 0) AS cost,
               p.safety_stock, p.reorder_point, p.reorder_qty
        FROM products p
        WHERE p.id IN (SELECT DISTINCT product_id FROM inventory)
        ORDER BY p.code
    ''').fetchall()

    affected = []
    total_stockout_risk = 0.0

    for p in products:
        pid = p['id']
        daily = _estimate_daily_demand(conn, pid, p)
        inv = _f(conn.execute('''
            SELECT COALESCE(SUM(quantity),0) AS q FROM inventory
            WHERE product_id=%s AND (status IS NULL OR status='Serviceable')
        ''', (pid,)).fetchone()['q'])

        orig_lt = int(p['lead_days'])
        new_lt = int(orig_lt * multiplier)
        ss = _f(p['safety_stock'])
        rp = _f(p['reorder_point'])

        # Stock needed to cover new lead time
        stock_needed_now = daily * new_lt + ss
        stock_available = inv
        shortfall = max(0, stock_needed_now - stock_available)

        if shortfall > 0:
            stockout_days = stock_available / daily if daily > 0 else 999.0
            cost = shortfall * _f(p['cost'])
            total_stockout_risk += cost
            affected.append({
                'product_id': pid,
                'code': p['code'],
                'name': p['name'],
                'orig_lead_days': orig_lt,
                'new_lead_days': new_lt,
                'on_hand': inv,
                'stock_needed': round(stock_needed_now, 1),
                'shortfall_qty': round(shortfall, 1),
                'coverage_days': round(stockout_days, 1),
                'risk_cost': round(cost),
            })

    current_state = {
        'label': f'Lead Time +{increase_pct:.0f}%',
        'products_evaluated': len(products),
        'avg_current_lead_days': round(sum(_f(p['lead_days']) for p in products) / len(products), 1) if products else 0,
        'current_at_risk_count': 0,
    }
    simulated_state = {
        'increase_pct': increase_pct,
        'parts_newly_at_risk': len(affected),
        'total_stockout_risk_usd': round(total_stockout_risk),
        'affected_parts': affected,
        'avg_new_lead_days': round(sum(a['new_lead_days'] for a in affected) / len(affected), 1) if affected else 0,
    }
    impact = {
        'downtime_hours': len(affected) * 16.0,
        'revenue_impact_usd': round(total_stockout_risk * 1.5),
        'parts_at_risk': len(affected),
        'blocked_wos': 0,
        'confidence': 'Medium',
    }
    mitigations = [
        {
            'action': f"Increase safety stock levels for {len(affected)} affected part(s)",
            'detail': f"Extended lead times leave {len(affected)} parts under-buffered.",
            'urgency': 'High',
            'risk_reduction_pct': 45,
            'cost_impact': round(total_stockout_risk),
            'time_impact_days': 0,
        },
        {
            'action': "Place early purchase orders before lead time changes take effect",
            'detail': 'Order now while lead times are still shorter.',
            'urgency': 'High',
            'risk_reduction_pct': 35,
            'cost_impact': round(total_stockout_risk * 0.8),
            'time_impact_days': -14,
        },
        {
            'action': "Negotiate expedite clauses with all suppliers",
            'detail': 'Lock in expedite pricing and priority access before disruption.',
            'urgency': 'Medium',
            'risk_reduction_pct': 20,
            'cost_impact': 2500,
            'time_impact_days': 0,
        },
    ]
    return current_state, simulated_state, impact, mitigations


# ── Demand spike ──────────────────────────────
def _sim_demand_spike(conn, params, hourly_rate):
    increase_pct = _f(params.get('increase_pct', 30))
    duration_days = int(params.get('duration_days', 30))
    multiplier = 1.0 + increase_pct / 100.0

    products = conn.execute('''
        SELECT p.id, p.code, p.name,
               COALESCE(p.lead_time_days, p.lead_time, 30) AS lead_days,
               COALESCE(p.cost, 0) AS cost, p.safety_stock, p.reorder_qty
        FROM products p
        WHERE p.id IN (SELECT DISTINCT product_id FROM inventory)
        ORDER BY p.code
    ''').fetchall()

    affected = []
    total_procurement_cost = 0.0
    earliest_stockout = None

    for p in products:
        pid = p['id']
        base_daily = _estimate_daily_demand(conn, pid, p)
        new_daily = base_daily * multiplier
        inv = _f(conn.execute('''
            SELECT COALESCE(SUM(quantity),0) AS q FROM inventory
            WHERE product_id=%s AND (status IS NULL OR status='Serviceable')
        ''', (pid,)).fetchone()['q'])

        base_days_cov = inv / base_daily if base_daily > 0 else 999.0
        new_days_cov = inv / new_daily if new_daily > 0 else 999.0

        if new_days_cov < duration_days or new_days_cov < base_days_cov - 10:
            extra_demand = (new_daily - base_daily) * duration_days
            cost_to_cover = extra_demand * _f(p['cost'])
            total_procurement_cost += cost_to_cover
            stockout_date = (date.today() + timedelta(days=max(0, int(new_days_cov)))).isoformat()
            if earliest_stockout is None or new_days_cov < _f(earliest_stockout):
                earliest_stockout = new_days_cov
            affected.append({
                'product_id': pid,
                'code': p['code'],
                'name': p['name'],
                'base_daily_demand': round(base_daily, 3),
                'new_daily_demand': round(new_daily, 3),
                'on_hand': inv,
                'base_days_coverage': round(base_days_cov, 1),
                'new_days_coverage': round(new_days_cov, 1),
                'stockout_date': stockout_date,
                'extra_procurement_qty': round(extra_demand, 1),
                'procurement_cost': round(cost_to_cover),
            })

    current_state = {
        'label': f'Demand Spike +{increase_pct:.0f}%',
        'products_evaluated': len(products),
        'current_at_risk_count': 0,
    }
    simulated_state = {
        'demand_increase_pct': increase_pct,
        'duration_days': duration_days,
        'parts_at_risk': len(affected),
        'total_procurement_cost_usd': round(total_procurement_cost),
        'affected_parts': affected,
        'earliest_stockout_days': round(earliest_stockout, 1) if earliest_stockout else None,
    }
    impact = {
        'downtime_hours': len(affected) * 12.0,
        'revenue_impact_usd': round(total_procurement_cost * 2.0),
        'parts_at_risk': len(affected),
        'blocked_wos': 0,
        'confidence': 'Medium',
    }
    mitigations = [
        {
            'action': f"Pre-position emergency stock for {len(affected)} part(s)",
            'detail': f"Demand spike of {increase_pct:.0f}% depletes stock {duration_days} days faster.",
            'urgency': 'Critical',
            'risk_reduction_pct': 50,
            'cost_impact': round(total_procurement_cost),
            'time_impact_days': -7,
        },
        {
            'action': "Activate contingency procurement channel",
            'detail': 'Engage broker/spot-buy for fast-turn inventory to cover spike duration.',
            'urgency': 'High',
            'risk_reduction_pct': 30,
            'cost_impact': round(total_procurement_cost * 0.15),
            'time_impact_days': -5,
        },
        {
            'action': "Review and adjust reorder points upward",
            'detail': f"Reorder points calibrated for current demand, not +{increase_pct:.0f}% spike.",
            'urgency': 'Medium',
            'risk_reduction_pct': 20,
            'cost_impact': 0,
            'time_impact_days': 0,
        },
    ]
    return current_state, simulated_state, impact, mitigations


# ── Maintenance deferral ──────────────────────
def _sim_maintenance_deferral(conn, params, hourly_rate):
    deferral_weeks = int(params.get('deferral_weeks', 4))
    deferral_days = deferral_weeks * 7

    schedule = _build_schedule_state(conn)

    # How many tasks would become overdue?
    newly_overdue = conn.execute('''
        SELECT COUNT(*) AS cnt FROM work_order_tasks wot
        JOIN work_orders wo ON wot.work_order_id = wo.id
        WHERE wo.status NOT IN ('Closed','Cancelled')
          AND wot.planned_end_date IS NOT NULL
          AND wot.planned_end_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '%s days'
    ''', (deferral_days,)).fetchone()

    # WOs that would slip past their promised delivery
    slipped_wos = conn.execute('''
        SELECT COUNT(*) AS cnt FROM work_orders wo
        WHERE wo.status NOT IN ('Closed','Cancelled')
          AND wo.planned_end_date IS NOT NULL
          AND wo.planned_end_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '%s days'
    ''', (deferral_days,)).fetchone()

    open_wos = schedule['open_wo_count']
    planned_hours = schedule['planned_task_hours']
    labor_cost = schedule['open_labor_cost']

    # Failure probability increase: each deferred week adds ~5% risk per WO
    failure_risk_increase = deferral_weeks * 5
    estimated_unplanned = round(open_wos * (failure_risk_increase / 100))
    unplanned_cost = estimated_unplanned * labor_cost / max(1, open_wos) * 2.5  # unplanned 2.5x cost

    # Compliance exposure
    compliance_risk = 'High' if deferral_weeks >= 6 else 'Medium' if deferral_weeks >= 3 else 'Low'

    current_state = {
        'label': f'Maintenance Deferral {deferral_weeks} weeks',
        'open_wo_count': open_wos,
        'planned_task_hours': planned_hours,
        'open_labor_cost': labor_cost,
        'overdue_wo_count': schedule['overdue_wo_count'],
    }
    simulated_state = {
        'deferral_weeks': deferral_weeks,
        'newly_overdue_tasks': int(newly_overdue['cnt']),
        'wos_slipping_delivery': int(slipped_wos['cnt']),
        'failure_risk_increase_pct': failure_risk_increase,
        'estimated_unplanned_events': estimated_unplanned,
        'unplanned_maintenance_cost_usd': round(unplanned_cost),
        'compliance_risk': compliance_risk,
        'backlog_hours': round(planned_hours + deferral_weeks * planned_hours * 0.1),
    }
    extra_downtime = estimated_unplanned * 16.0 + deferral_days * 0.5
    impact = {
        'downtime_hours': round(extra_downtime, 1),
        'revenue_impact_usd': round(extra_downtime * hourly_rate + unplanned_cost),
        'parts_at_risk': 0,
        'blocked_wos': int(slipped_wos['cnt']),
        'confidence': 'Medium',
    }
    mitigations = [
        {
            'action': f"Maintain original PM schedule — defer non-critical tasks only",
            'detail': f"Deferring all maintenance by {deferral_weeks} weeks increases unplanned failure risk by {failure_risk_increase}%.",
            'urgency': 'High',
            'risk_reduction_pct': 40,
            'cost_impact': 0,
            'time_impact_days': 0,
        },
        {
            'action': "Triage and prioritise AOG and safety-critical work orders",
            'detail': 'Complete highest-priority WOs before deferral window begins.',
            'urgency': 'Critical',
            'risk_reduction_pct': 30,
            'cost_impact': 0,
            'time_impact_days': -deferral_days,
        },
        {
            'action': "Pre-stage parts and tools for deferred work",
            'detail': 'Ensure rapid restart when deferral window ends.',
            'urgency': 'Medium',
            'risk_reduction_pct': 15,
            'cost_impact': round(labor_cost * 0.05),
            'time_impact_days': -3,
        },
    ]
    return current_state, simulated_state, impact, mitigations
