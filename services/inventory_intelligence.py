"""
Inventory Intelligence Service
Predictive analytics, quality scoring, cycle count scheduling, and AI forecasting.
"""
import os
import json
import logging
from datetime import datetime, date, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL')


def _conn():
    return psycopg2.connect(DATABASE_URL)


# ─── Quality Scoring ─────────────────────────────────────────────────────────

CONDITION_SCORES = {
    'New': 100,
    'Serviceable': 78,
    'Overhauled': 60,
    'Repaired': 45,
}

def _inspection_score(last_inspection_date):
    if not last_inspection_date:
        return 50
    if isinstance(last_inspection_date, str):
        try:
            last_inspection_date = datetime.strptime(last_inspection_date[:10], '%Y-%m-%d').date()
        except Exception:
            return 50
    days = (date.today() - last_inspection_date).days
    if days <= 30:
        return 100
    if days <= 90:
        return 85
    if days <= 180:
        return 70
    if days <= 365:
        return 55
    return 25


def compute_quality_score(item):
    cond_score = CONDITION_SCORES.get(item.get('condition') or '', 55)
    insp_score = _inspection_score(item.get('last_inspection_date'))
    qty = float(item.get('quantity') or 0)
    reorder = float(item.get('reorder_point') or 0)
    safety = float(item.get('safety_stock') or 0)

    stock_score = 100
    if reorder > 0 and qty <= 0:
        stock_score = 0
    elif reorder > 0 and qty <= reorder:
        stock_score = 35
    elif safety > 0 and qty <= safety:
        stock_score = 60

    score = round(cond_score * 0.40 + insp_score * 0.35 + stock_score * 0.25)
    return max(0, min(100, score))


def compute_risk_level(item, quality_score):
    qty = float(item.get('quantity') or 0)
    reorder = float(item.get('reorder_point') or 0)
    safety = float(item.get('safety_stock') or 0)
    if quality_score < 45 or (reorder > 0 and qty <= reorder):
        return 'Critical'
    if quality_score < 68 or (safety > 0 and qty <= safety * 1.2):
        return 'Warning'
    return 'Good'


# ─── Demand & Turnover ────────────────────────────────────────────────────────

def _receiving_history(cur, product_id, days=365):
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    cur.execute("""
        SELECT quantity_received, receipt_date
        FROM receiving_transactions
        WHERE product_id = %s AND receipt_date >= %s
        ORDER BY receipt_date
    """, (product_id, cutoff))
    return cur.fetchall()


def compute_turnover_rate(cur, product_id, current_qty):
    rows = _receiving_history(cur, product_id, 365)
    if not rows:
        return 0.0
    total_received = sum(float(r['quantity_received'] or 0) for r in rows)
    avg_stock = max(current_qty, total_received / 2) or 1
    return round(total_received / avg_stock, 2)


def compute_avg_daily_demand(cur, product_id):
    rows = _receiving_history(cur, product_id, 365)
    if not rows:
        return 0.0
    total = sum(float(r['quantity_received'] or 0) for r in rows)
    return round(total / 365, 4)


def compute_demand_volatility(cur, product_id):
    rows = _receiving_history(cur, product_id, 180)
    if len(rows) < 2:
        return 0.0
    quantities = [float(r['quantity_received'] or 0) for r in rows]
    mean = sum(quantities) / len(quantities)
    variance = sum((q - mean) ** 2 for q in quantities) / len(quantities)
    return round(variance ** 0.5, 2)


def compute_stock_age_days(item):
    last_updated = item.get('last_updated') or item.get('last_received_date')
    if not last_updated:
        return None
    if isinstance(last_updated, str):
        try:
            last_updated = datetime.strptime(last_updated[:19], '%Y-%m-%d %H:%M:%S')
        except Exception:
            return None
    if isinstance(last_updated, datetime):
        return (datetime.now() - last_updated).days
    if isinstance(last_updated, date):
        return (date.today() - last_updated).days
    return None


# ─── Forecasting ─────────────────────────────────────────────────────────────

def compute_reorder_forecast(cur, item, product):
    qty = float(item.get('quantity') or 0)
    safety = float(item.get('safety_stock') or product.get('safety_stock') or 0)
    reorder_pt = float(item.get('reorder_point') or product.get('reorder_point') or 0)
    reorder_qty = float(product.get('reorder_qty') or 0)
    lead_time = int(product.get('lead_time_days') or product.get('lead_time') or 14)

    daily_demand = compute_avg_daily_demand(cur, item['product_id'])
    volatility = compute_demand_volatility(cur, item['product_id'])

    demand_during_lead = daily_demand * lead_time
    safety_adjusted = max(safety, volatility * 1.5)
    optimal_reorder_qty = max(reorder_qty, demand_during_lead + safety_adjusted)

    needs_reorder = qty <= reorder_pt or (daily_demand > 0 and qty / daily_demand < lead_time)
    days_until_stockout = int(qty / daily_demand) if daily_demand > 0 else None
    reorder_date = None
    if needs_reorder:
        reorder_date = date.today().isoformat()
    elif days_until_stockout is not None:
        trigger_day = days_until_stockout - lead_time
        if trigger_day <= 30:
            reorder_date = (date.today() + timedelta(days=max(0, trigger_day))).isoformat()

    confidence = 85 if daily_demand > 0 else 40
    if volatility > daily_demand * 2 and daily_demand > 0:
        confidence = 55

    return {
        'recommended_qty': round(optimal_reorder_qty, 2),
        'daily_demand': daily_demand,
        'demand_during_lead': round(demand_during_lead, 2),
        'safety_stock_adjusted': round(safety_adjusted, 2),
        'days_until_stockout': days_until_stockout,
        'reorder_date': reorder_date,
        'lead_time_days': lead_time,
        'volatility': volatility,
        'confidence': confidence,
        'needs_reorder': needs_reorder,
    }


def simulate_what_if(forecast, demand_multiplier=1.0, supply_delay_days=0):
    adjusted_demand = forecast['daily_demand'] * demand_multiplier
    effective_lead = forecast['lead_time_days'] + supply_delay_days
    demand_lead = adjusted_demand * effective_lead
    rec_qty = max(demand_lead + forecast['safety_stock_adjusted'], 1)
    days_out = int(1 / adjusted_demand) if adjusted_demand > 0 else None
    return {
        'adjusted_daily_demand': round(adjusted_demand, 4),
        'effective_lead_time': effective_lead,
        'recommended_qty': round(rec_qty, 2),
        'days_until_stockout': days_out,
    }


# ─── Cycle Count Scheduling ───────────────────────────────────────────────────

def _next_weekday(target_weekday=0):
    today = date.today()
    days_ahead = target_weekday - today.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    return today + timedelta(days=days_ahead)


def compute_item_priority(item, unit_cost):
    value = float(item.get('quantity') or 0) * float(unit_cost or 0)
    condition = item.get('condition') or 'Serviceable'
    if value >= 10000 or condition in ('New',):
        return 'High'
    if value >= 2000 or condition in ('Overhauled',):
        return 'Medium'
    return 'Low'


def next_count_date(priority):
    if priority == 'High':
        return _next_weekday(0)
    if priority == 'Medium':
        today = date.today()
        first_next = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
        return first_next
    today = date.today()
    quarter_month = ((today.month - 1) // 3 + 1) * 3 + 1
    if quarter_month > 12:
        return date(today.year + 1, 1, 1)
    return date(today.year, quarter_month, 1)


def auto_schedule_cycle_counts(inventory_items):
    """Returns list of (inventory_id, product_id, scheduled_date, priority) tuples."""
    scheduled = []
    for item in inventory_items:
        priority = item.get('priority', 'Medium')
        scheduled_date = next_count_date(priority)
        scheduled.append({
            'inventory_id': item['id'],
            'product_id': item['product_id'],
            'scheduled_date': scheduled_date,
            'priority': priority,
            'expected_qty': float(item.get('quantity') or 0),
        })
    return scheduled


# ─── Part Class Grouping ──────────────────────────────────────────────────────

def get_part_class(product):
    cat = product.get('part_category') or product.get('product_category') or product.get('product_type') or 'General'
    return cat.strip() or 'General'


# ─── AI Narrative (OpenAI) ────────────────────────────────────────────────────

def get_ai_summary(items_data):
    try:
        from openai import OpenAI
        api_key = os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY')
        if not api_key:
            return None
        client = OpenAI(api_key=api_key)

        critical = [i for i in items_data if i['risk_level'] == 'Critical']
        warning = [i for i in items_data if i['risk_level'] == 'Warning']
        needs_reorder = [i for i in items_data if i.get('forecast', {}).get('needs_reorder')]

        summary_lines = []
        for i in items_data:
            f = i.get('forecast', {})
            summary_lines.append(
                f"Part {i['part_code']} ({i['part_name']}): qty={i['quantity']}, "
                f"risk={i['risk_level']}, quality={i['quality_score']}, "
                f"daily_demand={f.get('daily_demand', 0):.4f}, "
                f"days_to_stockout={f.get('days_until_stockout', 'N/A')}, "
                f"rec_reorder_qty={f.get('recommended_qty', 0)}"
            )

        prompt = (
            "You are an inventory intelligence analyst for a manufacturing ERP system. "
            "Analyze the following inventory data and provide a concise executive summary "
            "with 3 to 5 actionable recommendations. Use plain English, no special characters, "
            "no bullet symbols or markdown. Separate recommendations with numbered lines only.\n\n"
            "Inventory Status:\n" + "\n".join(summary_lines) + "\n\n"
            f"Critical items: {len(critical)}, Warning items: {len(warning)}, "
            f"Items needing reorder: {len(needs_reorder)} out of {len(items_data)} total."
        )

        response = client.chat.completions.create(
            model='gpt-4o',
            messages=[{'role': 'user', 'content': prompt}],
            max_tokens=400,
            temperature=0.4
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.warning(f"AI summary failed: {e}")
        return None


# ─── Main Dashboard Data Builder ──────────────────────────────────────────────

def build_dashboard_data():
    with _conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            cur.execute("""
                SELECT i.*,
                       p.code as part_code, p.name as part_name, p.cost as product_cost,
                       p.part_category, p.product_category, p.product_type,
                       p.lead_time_days, p.lead_time, p.safety_stock as p_safety_stock,
                       p.reorder_point as p_reorder_point, p.reorder_qty,
                       p.unit_of_measure,
                       s.name as supplier_name, s.status as supplier_status
                FROM inventory i
                JOIN products p ON i.product_id = p.id
                LEFT JOIN suppliers s ON i.supplier_id = s.id
                ORDER BY p.code
            """)
            rows = cur.fetchall()

            items = []
            part_class_map = {}

            for row in rows:
                item = dict(row)
                product = {
                    'id': row['product_id'],
                    'code': row['part_code'],
                    'name': row['part_name'],
                    'cost': row['product_cost'],
                    'lead_time_days': row['lead_time_days'],
                    'lead_time': row['lead_time'],
                    'safety_stock': row['p_safety_stock'],
                    'reorder_point': row['p_reorder_point'],
                    'reorder_qty': row['reorder_qty'],
                    'part_category': row['part_category'],
                    'product_category': row['product_category'],
                    'product_type': row['product_type'],
                }

                unit_cost = float(row.get('unit_cost') or row.get('product_cost') or 0)
                qty = float(row.get('quantity') or 0)
                quality_score = compute_quality_score(item)
                risk_level = compute_risk_level(item, quality_score)
                turnover = compute_turnover_rate(cur, row['product_id'], qty)
                stock_age = compute_stock_age_days(item)
                forecast = compute_reorder_forecast(cur, item, product)
                priority = compute_item_priority(item, unit_cost)
                part_class = get_part_class(product)

                enriched = {
                    'id': row['id'],
                    'product_id': row['product_id'],
                    'part_code': row['part_code'],
                    'part_name': row['part_name'],
                    'part_class': part_class,
                    'condition': row.get('condition') or 'Unknown',
                    'status': row.get('status') or 'Available',
                    'quantity': qty,
                    'reorder_point': float(row.get('reorder_point') or row.get('p_reorder_point') or 0),
                    'safety_stock': float(row.get('safety_stock') or row.get('p_safety_stock') or 0),
                    'unit_cost': unit_cost,
                    'inventory_value': round(qty * unit_cost, 2),
                    'unit_of_measure': row.get('unit_of_measure') or 'EA',
                    'warehouse_location': row.get('warehouse_location') or '',
                    'bin_location': row.get('bin_location') or '',
                    'last_inspection_date': str(row.get('last_inspection_date') or ''),
                    'last_updated': str(row.get('last_updated') or ''),
                    'supplier_name': row.get('supplier_name') or 'Unknown',
                    'quality_score': quality_score,
                    'risk_level': risk_level,
                    'turnover_rate': turnover,
                    'stock_age_days': stock_age,
                    'priority': priority,
                    'forecast': forecast,
                    'needs_reorder': forecast['needs_reorder'],
                }
                items.append(enriched)

                pc = part_class_map.setdefault(part_class, {
                    'name': part_class,
                    'count': 0,
                    'total_value': 0,
                    'critical': 0,
                    'warning': 0,
                    'good': 0,
                    'quality_sum': 0,
                    'needs_reorder': 0,
                })
                pc['count'] += 1
                pc['total_value'] += enriched['inventory_value']
                pc['quality_sum'] += quality_score
                pc['needs_reorder'] += (1 if forecast['needs_reorder'] else 0)
                if risk_level == 'Critical':
                    pc['critical'] += 1
                elif risk_level == 'Warning':
                    pc['warning'] += 1
                else:
                    pc['good'] += 1

            for pc in part_class_map.values():
                pc['avg_quality'] = round(pc['quality_sum'] / pc['count'], 1) if pc['count'] else 0
                del pc['quality_sum']

            cur.execute("""
                SELECT cc.*, p.code as part_code, p.name as part_name
                FROM inventory_cycle_counts cc
                LEFT JOIN products p ON cc.product_id = p.id
                ORDER BY cc.scheduled_date ASC
                LIMIT 50
            """)
            cycle_counts = [dict(r) for r in cur.fetchall()]
            for cc in cycle_counts:
                for k, v in cc.items():
                    if isinstance(v, (date, datetime)):
                        cc[k] = str(v)

            cur.execute("""
                SELECT r.*, p.code as part_code, p.name as part_name
                FROM inventory_ai_recommendations r
                LEFT JOIN products p ON r.product_id = p.id
                WHERE r.status = 'Pending'
                ORDER BY r.created_at DESC
                LIMIT 20
            """)
            recommendations = [dict(r) for r in cur.fetchall()]
            for rec in recommendations:
                for k, v in rec.items():
                    if isinstance(v, (date, datetime)):
                        rec[k] = str(v)

            kpis = _build_kpis(items)

            for item in items:
                for k, v in item.items():
                    if isinstance(v, (date, datetime)):
                        item[k] = str(v)
                fc = item.get('forecast', {})
                for k, v in fc.items():
                    if isinstance(v, (date, datetime)):
                        fc[k] = str(v)

            return {
                'items': items,
                'part_classes': list(part_class_map.values()),
                'cycle_counts': cycle_counts,
                'recommendations': recommendations,
                'kpis': kpis,
            }


def _build_kpis(items):
    total = len(items)
    critical = sum(1 for i in items if i['risk_level'] == 'Critical')
    warning = sum(1 for i in items if i['risk_level'] == 'Warning')
    good = sum(1 for i in items if i['risk_level'] == 'Good')
    needs_reorder = sum(1 for i in items if i['needs_reorder'])
    total_value = sum(i['inventory_value'] for i in items)
    avg_quality = round(sum(i['quality_score'] for i in items) / total, 1) if total else 0
    quality_pass = sum(1 for i in items if i['quality_score'] >= 70)
    quality_fail = total - quality_pass
    avg_turnover = round(sum(i['turnover_rate'] for i in items) / total, 2) if total else 0
    return {
        'total_items': total,
        'critical_items': critical,
        'warning_items': warning,
        'good_items': good,
        'needs_reorder': needs_reorder,
        'total_inventory_value': round(total_value, 2),
        'avg_quality_score': avg_quality,
        'quality_pass': quality_pass,
        'quality_fail': quality_fail,
        'avg_turnover_rate': avg_turnover,
    }


def save_cycle_count_schedule(items_data):
    scheduled = auto_schedule_cycle_counts(items_data)
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM inventory_cycle_counts WHERE status = 'Scheduled'")
            for s in scheduled:
                cur.execute("""
                    INSERT INTO inventory_cycle_counts
                    (inventory_id, product_id, scheduled_date, priority, status, expected_qty)
                    VALUES (%s, %s, %s, %s, 'Scheduled', %s)
                """, (s['inventory_id'], s['product_id'],
                      s['scheduled_date'], s['priority'], s['expected_qty']))
        conn.commit()
    return len(scheduled)


def save_ai_recommendations(items_data):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM inventory_ai_recommendations WHERE status = 'Pending'")
            for item in items_data:
                if not item.get('needs_reorder'):
                    continue
                fc = item.get('forecast', {})
                cur.execute("""
                    INSERT INTO inventory_ai_recommendations
                    (inventory_id, product_id, recommendation_type, recommended_qty,
                     reorder_date, confidence_score, risk_level, quality_score, status)
                    VALUES (%s, %s, 'Reorder', %s, %s, %s, %s, %s, 'Pending')
                """, (
                    item['id'], item['product_id'],
                    fc.get('recommended_qty', 0),
                    fc.get('reorder_date'),
                    fc.get('confidence', 50),
                    item['risk_level'],
                    item['quality_score'],
                ))
        conn.commit()


def mark_count_complete(count_id, counted_qty, notes=''):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE inventory_cycle_counts
                SET status = 'Completed', completed_date = %s,
                    counted_qty = %s, variance = counted_qty - expected_qty,
                    notes = %s
                WHERE id = %s
            """, (date.today(), counted_qty, notes or None, count_id))
        conn.commit()


def dismiss_recommendation(rec_id, user_id):
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE inventory_ai_recommendations
                SET status = 'Dismissed', actioned_at = NOW(), actioned_by = %s
                WHERE id = %s
            """, (user_id, rec_id))
        conn.commit()
