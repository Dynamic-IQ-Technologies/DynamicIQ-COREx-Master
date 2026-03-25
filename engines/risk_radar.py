"""
Supply Chain Risk Radar — Scoring Engine
Calculates risk scores for suppliers and parts from internal ERP data + AI narrative.
"""
import json
import logging
import os
from datetime import date, datetime

logger = logging.getLogger(__name__)

RISK_LEVELS = [
    (76, 'Critical'),
    (51, 'High'),
    (26, 'Medium'),
    (0,  'Low'),
]

def risk_level(score):
    for threshold, label in RISK_LEVELS:
        if score >= threshold:
            return label
    return 'Low'


def _safe_float(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


# ─────────────────────────────────────────────
# SUPPLIER RISK SCORE
# ─────────────────────────────────────────────
def calculate_supplier_risk(conn, supplier_id):
    """
    Returns (score 0-100, breakdown_dict, mitigation_list)
    Weights: OTIF 35 | Lead-time variance 20 | Quality incidents 15
             Active shortage exposure 20 | Overdue POs 10
    """
    breakdown = {}
    mitigations = []

    # 1. OTIF (35 pts)
    po_rows = conn.execute('''
        SELECT order_date, expected_delivery_date, actual_delivery_date, status
        FROM purchase_orders
        WHERE supplier_id = %s
          AND status IN ('Received', 'Partially Received')
          AND expected_delivery_date IS NOT NULL
        ORDER BY order_date DESC
        LIMIT 30
    ''', (supplier_id,)).fetchall()

    if po_rows:
        on_time = sum(
            1 for p in po_rows
            if p['actual_delivery_date'] and
               p['actual_delivery_date'] <= p['expected_delivery_date']
        )
        otif_rate = on_time / len(po_rows)
        otif_risk = round((1 - otif_rate) * 35)
        breakdown['otif'] = {
            'label': 'On-Time In-Full Delivery',
            'rate_pct': round(otif_rate * 100),
            'risk_contribution': otif_risk,
            'data_points': len(po_rows),
        }
        if otif_rate < 0.8:
            mitigations.append({
                'action': 'Initiate supplier performance review',
                'detail': f'OTIF is {round(otif_rate*100)}% — below 80% threshold.',
                'urgency': 'High' if otif_rate < 0.6 else 'Medium',
                'risk_reduction_pct': 15,
                'cost_impact': 0,
            })
    else:
        otif_risk = 15
        breakdown['otif'] = {
            'label': 'On-Time In-Full Delivery',
            'rate_pct': None,
            'risk_contribution': otif_risk,
            'data_points': 0,
            'note': 'No delivery history — uncertainty added',
        }

    # 2. Lead-time variance (20 pts)
    lt_rows = [
        p for p in po_rows
        if p['actual_delivery_date'] and p['order_date'] and p['expected_delivery_date']
    ]
    if len(lt_rows) >= 2:
        def days_diff(a, b):
            if isinstance(a, str):
                a = date.fromisoformat(a)
            if isinstance(b, str):
                b = date.fromisoformat(b)
            return abs((a - b).days)

        variances = [
            days_diff(p['actual_delivery_date'], p['expected_delivery_date'])
            for p in lt_rows
        ]
        avg_var = sum(variances) / len(variances)
        exp_lts = [
            days_diff(p['expected_delivery_date'], p['order_date'])
            for p in lt_rows
            if p['order_date']
        ]
        avg_exp = (sum(exp_lts) / len(exp_lts)) if exp_lts else 30
        cv = avg_var / max(1, avg_exp)
        lt_risk = min(20, round(cv * 20))
        breakdown['lead_time_variance'] = {
            'label': 'Lead Time Variance',
            'avg_variance_days': round(avg_var, 1),
            'cv_pct': round(cv * 100),
            'risk_contribution': lt_risk,
        }
        if avg_var > 7:
            mitigations.append({
                'action': f'Order {round(avg_var)} days earlier than planned',
                'detail': 'Average delivery is late; build lead-time buffer.',
                'urgency': 'Medium',
                'risk_reduction_pct': 10,
                'cost_impact': 0,
            })
    else:
        lt_risk = 5
        breakdown['lead_time_variance'] = {
            'label': 'Lead Time Variance',
            'risk_contribution': lt_risk,
            'note': 'Insufficient data',
        }

    # 3. Open quality incidents (15 pts)
    capa_row = conn.execute('''
        SELECT COUNT(*) AS cnt FROM qms_capa
        WHERE source_type = 'Supplier' AND CAST(source_id AS INTEGER) = %s
          AND status != 'Closed'
    ''', (supplier_id,)).fetchone()
    capa_count = int(capa_row['cnt'] or 0)
    capa_risk = min(15, capa_count * 5)
    breakdown['quality_incidents'] = {
        'label': 'Open Quality Incidents',
        'count': capa_count,
        'risk_contribution': capa_risk,
    }
    if capa_count > 0:
        mitigations.append({
            'action': f'Resolve {capa_count} open quality incident(s)',
            'detail': 'Open CAPA records indicate unresolved quality issues.',
            'urgency': 'High',
            'risk_reduction_pct': capa_count * 5,
            'cost_impact': 0,
        })

    # 4. Active shortage exposure (20 pts)  — parts this supplier covers that are short
    shortage_row = conn.execute('''
        SELECT COALESCE(SUM(mr.shortage_quantity * COALESCE(p.cost, 0)), 0) AS exposure
        FROM material_requirements mr
        JOIN products p ON mr.product_id = p.id
        WHERE mr.status != 'Satisfied'
          AND mr.shortage_quantity > 0
          AND p.id IN (
              SELECT DISTINCT pol.product_id
              FROM purchase_order_lines pol
              JOIN purchase_orders po2 ON pol.po_id = po2.id
              WHERE po2.supplier_id = %s
          )
    ''', (supplier_id,)).fetchone()
    exposure = _safe_float(shortage_row['exposure'])
    shortage_risk = min(20, round(exposure / 500))
    breakdown['shortage_exposure'] = {
        'label': 'Active Shortage Exposure',
        'value_usd': round(exposure, 2),
        'risk_contribution': shortage_risk,
    }
    if exposure > 0:
        mitigations.append({
            'action': 'Expedite open shortage items',
            'detail': f'${exposure:,.0f} exposure from parts currently short.',
            'urgency': 'High',
            'risk_reduction_pct': 20,
            'cost_impact': round(exposure * 0.05),
        })

    # 5. Overdue open POs (10 pts)
    overdue_row = conn.execute('''
        SELECT COUNT(*) AS cnt FROM purchase_orders
        WHERE supplier_id = %s
          AND status IN ('Ordered', 'Partially Received')
          AND expected_delivery_date < CURRENT_DATE
    ''', (supplier_id,)).fetchone()
    overdue = int(overdue_row['cnt'] or 0)
    overdue_risk = min(10, overdue * 4)
    breakdown['overdue_pos'] = {
        'label': 'Overdue Purchase Orders',
        'count': overdue,
        'risk_contribution': overdue_risk,
    }
    if overdue > 0:
        mitigations.append({
            'action': f'Follow up on {overdue} overdue PO(s)',
            'detail': 'Open POs past expected delivery; update ETA or escalate.',
            'urgency': 'High',
            'risk_reduction_pct': 8,
            'cost_impact': 0,
        })

    total = min(100, max(0,
        otif_risk + lt_risk + capa_risk + shortage_risk + overdue_risk
    ))

    if not mitigations:
        mitigations.append({
            'action': 'Continue monitoring supplier performance',
            'detail': 'No immediate risks detected based on current ERP data.',
            'urgency': 'Low',
            'risk_reduction_pct': 0,
            'cost_impact': 0,
        })

    return total, breakdown, mitigations


# ─────────────────────────────────────────────
# PART RISK SCORE
# ─────────────────────────────────────────────
def calculate_part_risk(conn, product_id, supplier_risk_map):
    """
    Returns (score 0-100, breakdown_dict, mitigation_list)
    Weights: Current shortage 40 | Supplier risk 30 | Urgency 20 | Single-source 10
    """
    breakdown = {}
    mitigations = []

    product = conn.execute(
        'SELECT * FROM products WHERE id = %s', (product_id,)
    ).fetchone()
    if not product:
        return 0, {}, []

    # 1. Current shortage severity (40 pts)
    shortage = _safe_float(conn.execute('''
        SELECT COALESCE(SUM(shortage_quantity), 0) AS s
        FROM material_requirements
        WHERE product_id = %s AND status != 'Satisfied' AND shortage_quantity > 0
    ''', (product_id,)).fetchone()['s'])

    reorder_base = max(1, _safe_float(
        product['reorder_qty'] or product['safety_stock'] or 10
    ))
    shortage_risk = min(40, round(shortage / reorder_base * 40)) if shortage > 0 else 0
    breakdown['shortage'] = {
        'label': 'Current Shortage Severity',
        'shortage_qty': shortage,
        'reorder_qty': reorder_base,
        'risk_contribution': shortage_risk,
    }
    if shortage > 0:
        mitigations.append({
            'action': f'Create PO for {int(shortage)} units',
            'detail': 'Part is currently short — immediate procurement required.',
            'urgency': 'Critical',
            'risk_reduction_pct': 35,
            'cost_impact': round(shortage * _safe_float(product['cost'])),
        })

    # 2. Primary supplier risk (30 pts)
    sup_row = conn.execute('''
        SELECT po.supplier_id, COUNT(*) AS cnt
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        WHERE pol.product_id = %s
        GROUP BY po.supplier_id
        ORDER BY cnt DESC LIMIT 1
    ''', (product_id,)).fetchone()

    if sup_row:
        sup_score = _safe_float(supplier_risk_map.get(sup_row['supplier_id'], 25))
    else:
        sup_score = 40.0  # no supplier history = elevated risk

    sup_contribution = round(sup_score / 100 * 30)
    breakdown['supplier_risk'] = {
        'label': 'Primary Supplier Risk',
        'supplier_score': sup_score,
        'risk_contribution': sup_contribution,
    }

    # 3. Lead time vs open demand urgency (20 pts)
    lead_days = int(product['lead_time_days'] or product['lead_time'] or 30)
    wo_demand = int(conn.execute('''
        SELECT COUNT(*) AS cnt FROM material_requirements
        WHERE product_id = %s AND status != 'Satisfied'
    ''', (product_id,)).fetchone()['cnt'])

    urgency_risk = min(20, round((lead_days / 30) * max(0, wo_demand) * 4))
    breakdown['urgency'] = {
        'label': 'Lead Time vs Demand Urgency',
        'lead_time_days': lead_days,
        'open_demand_lines': wo_demand,
        'risk_contribution': urgency_risk,
    }
    if lead_days > 21 and wo_demand > 0:
        mitigations.append({
            'action': f'Increase safety stock — {lead_days}-day lead time',
            'detail': 'Long lead time with active demand; stock buffer recommended.',
            'urgency': 'Medium',
            'risk_reduction_pct': 12,
            'cost_impact': round(reorder_base * _safe_float(product['cost']) * 0.5),
        })

    # 4. Single-source concentration (10 pts)
    sup_count = int(conn.execute('''
        SELECT COUNT(DISTINCT po.supplier_id) AS cnt
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        WHERE pol.product_id = %s
    ''', (product_id,)).fetchone()['cnt'])

    single_risk = 10 if sup_count == 0 else max(0, 10 - (sup_count - 1) * 3)
    breakdown['supplier_concentration'] = {
        'label': 'Supplier Concentration Risk',
        'supplier_count': sup_count,
        'risk_contribution': single_risk,
    }
    if sup_count <= 1:
        mitigations.append({
            'action': 'Qualify an alternate supplier',
            'detail': 'Single-source dependency creates supply chain vulnerability.',
            'urgency': 'Medium',
            'risk_reduction_pct': 10,
            'cost_impact': 0,
        })

    total = min(100, max(0,
        shortage_risk + sup_contribution + urgency_risk + single_risk
    ))

    if not mitigations:
        mitigations.append({
            'action': 'Monitor stock levels and lead times',
            'detail': 'No immediate risk signals detected.',
            'urgency': 'Low',
            'risk_reduction_pct': 0,
            'cost_impact': 0,
        })

    return total, breakdown, mitigations


# ─────────────────────────────────────────────
# AI NARRATIVE GENERATION
# ─────────────────────────────────────────────
def generate_ai_narrative(supplier_name, country, score, breakdown, mitigations):
    try:
        from openai import OpenAI
        client = OpenAI(
            api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
            base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL'),
            timeout=10.0
        )
        top_factors = ', '.join(
            f"{v['label']} ({v['risk_contribution']} pts)"
            for v in breakdown.values()
            if v.get('risk_contribution', 0) > 0
        )
        prompt = (
            f"Supplier: {supplier_name} | Country: {country or 'Unknown'} | "
            f"Risk score: {score}/100 | Key factors: {top_factors}.\n"
            f"Write 2 plain-English sentences: (1) summarise the risk, "
            f"(2) mention any known geopolitical or trade risks for {country or 'this region'}. "
            f"No markdown, no bullet points, no special characters."
        )
        resp = client.chat.completions.create(
            model='gpt-4o-mini',
            messages=[{'role': 'user', 'content': prompt}],
            temperature=0.2,
            max_tokens=120,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        logger.warning(f"AI narrative failed for {supplier_name}: {e}")
        return None


# ─────────────────────────────────────────────
# TREND CALCULATION
# ─────────────────────────────────────────────
def calculate_trend(conn, entity_type, entity_id, new_score):
    row = conn.execute('''
        SELECT risk_score FROM risk_score_history
        WHERE entity_type = %s AND entity_id = %s
        ORDER BY calculated_at DESC LIMIT 1
    ''', (entity_type, entity_id)).fetchone()
    if not row:
        return 'stable'
    prev = _safe_float(row['risk_score'])
    diff = new_score - prev
    if diff >= 5:
        return 'degrading'
    if diff <= -5:
        return 'improving'
    return 'stable'


# ─────────────────────────────────────────────
# FULL RECALCULATION
# ─────────────────────────────────────────────
def run_full_recalculation(conn, created_by=None):
    """
    Recalculates risk scores for all active suppliers and parts with PO history.
    Returns summary dict.
    """
    results = {'suppliers': [], 'parts': [], 'alerts': []}
    supplier_risk_map = {}

    # --- Suppliers ---
    suppliers = conn.execute(
        "SELECT id, name, country FROM suppliers WHERE status != 'Inactive' OR status IS NULL"
    ).fetchall()

    for sup in suppliers:
        sid = sup['id']
        try:
            score, breakdown, mitigations = calculate_supplier_risk(conn, sid)
            level = risk_level(score)
            trend = calculate_trend(conn, 'supplier', sid, score)
            narrative = generate_ai_narrative(
                sup['name'], sup['country'], score, breakdown, mitigations
            )

            # Get previous score for event detection
            prev_row = conn.execute(
                "SELECT risk_score, risk_level FROM supply_risk_profiles WHERE entity_type='supplier' AND entity_id=%s",
                (sid,)
            ).fetchone()
            prev_score = _safe_float(prev_row['risk_score']) if prev_row else None
            prev_level = prev_row['risk_level'] if prev_row else None

            # Upsert profile
            conn.execute('''
                INSERT INTO supply_risk_profiles
                    (entity_type, entity_id, entity_name, risk_score, risk_level,
                     score_breakdown, trend, confidence, mitigation_recommendations,
                     ai_narrative, last_calculated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (entity_type, entity_id) DO UPDATE SET
                    entity_name = EXCLUDED.entity_name,
                    risk_score = EXCLUDED.risk_score,
                    risk_level = EXCLUDED.risk_level,
                    score_breakdown = EXCLUDED.score_breakdown,
                    trend = EXCLUDED.trend,
                    confidence = EXCLUDED.confidence,
                    mitigation_recommendations = EXCLUDED.mitigation_recommendations,
                    ai_narrative = EXCLUDED.ai_narrative,
                    last_calculated = EXCLUDED.last_calculated
            ''', (
                'supplier', sid, sup['name'], score, level,
                json.dumps(breakdown), trend, 'Medium',
                json.dumps(mitigations), narrative
            ))

            # History snapshot
            conn.execute('''
                INSERT INTO risk_score_history (entity_type, entity_id, entity_name, risk_score, risk_level)
                VALUES (%s, %s, %s, %s, %s)
            ''', ('supplier', sid, sup['name'], score, level))

            # Risk event if threshold breached or level changed
            if prev_level and prev_level != level and level in ('High', 'Critical'):
                conn.execute('''
                    INSERT INTO risk_events
                        (entity_type, entity_id, entity_name, event_type,
                         old_score, new_score, risk_level, description,
                         recommendations, created_by)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ''', (
                    'supplier', sid, sup['name'], 'threshold_breach',
                    prev_score, score, level,
                    f"Supplier {sup['name']} risk escalated to {level} ({score:.0f}/100)",
                    json.dumps(mitigations), created_by
                ))
                results['alerts'].append({
                    'entity': sup['name'],
                    'type': 'supplier',
                    'level': level,
                    'score': score,
                    'message': f"Risk escalated to {level}",
                })

            supplier_risk_map[sid] = score
            results['suppliers'].append({
                'id': sid, 'name': sup['name'], 'score': score, 'level': level,
            })
        except Exception as e:
            logger.error(f"Supplier {sid} scoring failed: {e}", exc_info=True)

    # --- Parts ---
    products = conn.execute('''
        SELECT DISTINCT p.id, p.code, p.name
        FROM products p
        WHERE p.id IN (
            SELECT DISTINCT product_id FROM purchase_order_lines
            UNION
            SELECT DISTINCT product_id FROM material_requirements
            UNION
            SELECT DISTINCT product_id FROM inventory
        )
        ORDER BY p.code
    ''').fetchall()

    for prod in products:
        pid = prod['id']
        try:
            score, breakdown, mitigations = calculate_part_risk(conn, pid, supplier_risk_map)
            if score == 0 and not any(v.get('risk_contribution', 0) > 0 for v in breakdown.values()):
                continue

            level = risk_level(score)
            trend = calculate_trend(conn, 'part', pid, score)

            conn.execute('''
                INSERT INTO supply_risk_profiles
                    (entity_type, entity_id, entity_name, risk_score, risk_level,
                     score_breakdown, trend, confidence, mitigation_recommendations,
                     last_calculated)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
                ON CONFLICT (entity_type, entity_id) DO UPDATE SET
                    entity_name = EXCLUDED.entity_name,
                    risk_score = EXCLUDED.risk_score,
                    risk_level = EXCLUDED.risk_level,
                    score_breakdown = EXCLUDED.score_breakdown,
                    trend = EXCLUDED.trend,
                    confidence = EXCLUDED.confidence,
                    mitigation_recommendations = EXCLUDED.mitigation_recommendations,
                    last_calculated = EXCLUDED.last_calculated
            ''', (
                'part', pid, prod['code'],
                score, level, json.dumps(breakdown), trend, 'Medium',
                json.dumps(mitigations)
            ))

            conn.execute('''
                INSERT INTO risk_score_history (entity_type, entity_id, entity_name, risk_score, risk_level)
                VALUES (%s,%s,%s,%s,%s)
            ''', ('part', pid, prod['code'], score, level))

            results['parts'].append({
                'id': pid, 'code': prod['code'], 'name': prod['name'],
                'score': score, 'level': level,
            })
        except Exception as e:
            logger.error(f"Part {pid} scoring failed: {e}", exc_info=True)

    conn.commit()
    return results
