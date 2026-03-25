from flask import Blueprint, jsonify, session
from models import Database
from auth import login_required
import logging
import json
import os
import uuid
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

mrr_ai_bp = Blueprint('mrr_ai', __name__, url_prefix='/api/mrr')

_jobs = {}
_jobs_lock = threading.Lock()


def _fetch_mrr_data(conn):
    """Mirror the full MRR report logic across all four shortage sources."""
    po_rows = conn.execute('''
        SELECT pol.product_id,
               SUM(pol.quantity - COALESCE(pol.received_quantity, 0)) as qty_on_order
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        WHERE po.status IN ('Ordered', 'Partially Received')
        GROUP BY pol.product_id
    ''').fetchall()
    po_dict = {r['product_id']: float(r['qty_on_order'] or 0) for r in po_rows}

    inv_rows = conn.execute('''
        SELECT product_id, SUM(quantity) as total_qty
        FROM inventory GROUP BY product_id
    ''').fetchall()
    inv_dict = {r['product_id']: float(r['total_qty'] or 0) for r in inv_rows}
    net_inventory = dict(inv_dict)

    product_shortages = {}

    # --- Source 1: Production work order material requirements ---
    prod_rows = conn.execute('''
        SELECT mr.product_id, p.code, p.name, p.unit_of_measure,
               COALESCE(p.cost, 0) as unit_cost, p.lead_time_days,
               mr.required_quantity, mr.shortage_quantity
        FROM material_requirements mr
        JOIN products p ON mr.product_id = p.id
        WHERE mr.status != 'Satisfied'
    ''').fetchall()
    for r in prod_rows:
        pid = r['product_id']
        shortage = float(r['shortage_quantity'] or 0)
        net_inventory[pid] = net_inventory.get(pid, 0) - float(r['required_quantity'] or 0)
        if shortage > 0:
            _accumulate(product_shortages, pid, r, shortage)

    # --- Source 2: Service work order materials ---
    svc_rows = conn.execute('''
        SELECT swm.product_id, p.code, p.name, p.unit_of_measure,
               COALESCE(p.cost, 0) as unit_cost, p.lead_time_days,
               swm.quantity as required_quantity
        FROM service_wo_materials swm
        JOIN products p ON swm.product_id = p.id
        JOIN service_work_orders swo ON swm.swo_id = swo.id
        WHERE swo.status NOT IN ('Completed', 'Cancelled', 'Invoiced')
          AND swm.allocated_from_inventory = 0
    ''').fetchall()
    for r in svc_rows:
        pid = r['product_id']
        required = float(r['required_quantity'] or 0)
        available = max(0, net_inventory.get(pid, 0))
        shortage = max(0, required - available)
        net_inventory[pid] = net_inventory.get(pid, 0) - required
        if shortage > 0:
            _accumulate(product_shortages, pid, r, shortage)

    # --- Source 3: Work order task materials ---
    task_rows = conn.execute('''
        SELECT tm.product_id, p.code, p.name, p.unit_of_measure,
               COALESCE(p.cost, 0) as unit_cost, p.lead_time_days,
               tm.required_qty as required_quantity,
               COALESCE(tm.issued_qty, 0) as issued_qty
        FROM work_order_task_materials tm
        JOIN products p ON tm.product_id = p.id
        JOIN work_order_tasks wot ON tm.task_id = wot.id
        JOIN work_orders wo ON wot.work_order_id = wo.id
        WHERE wo.status NOT IN ('Completed', 'Closed', 'Cancelled')
          AND COALESCE(tm.issued_qty, 0) < tm.required_qty
          AND NOT EXISTS (
              SELECT 1 FROM material_requirements mr
              WHERE mr.work_order_id = wo.id AND mr.product_id = tm.product_id
          )
    ''').fetchall()
    for r in task_rows:
        pid = r['product_id']
        shortage = float(r['required_quantity'] or 0) - float(r['issued_qty'] or 0)
        if shortage > 0:
            _accumulate(product_shortages, pid, r, shortage)

    # --- Source 4: Sales order lines not fully allocated ---
    so_rows = conn.execute('''
        SELECT sol.product_id, p.code, p.name, p.unit_of_measure,
               COALESCE(p.cost, 0) as unit_cost, p.lead_time_days,
               sol.quantity as required_quantity,
               COALESCE(sol.allocated_quantity, 0) as allocated_quantity
        FROM sales_order_lines sol
        JOIN products p ON sol.product_id = p.id
        JOIN sales_orders so ON sol.so_id = so.id
        WHERE so.status NOT IN ('Cancelled', 'Closed', 'Shipped', 'Invoiced')
          AND COALESCE(sol.is_core, 0) = 0
          AND sol.quantity > COALESCE(sol.allocated_quantity, 0)
    ''').fetchall()
    for r in so_rows:
        pid = r['product_id']
        required = float(r['required_quantity'] or 0)
        available = max(0, net_inventory.get(pid, 0))
        shortage = max(0, required - available)
        net_inventory[pid] = net_inventory.get(pid, 0) - required
        if shortage > 0:
            _accumulate(product_shortages, pid, r, shortage)

    # Build final list, filter out items fully covered by POs
    result = []
    for pid, data in product_shortages.items():
        on_order = po_dict.get(pid, 0)
        total_shortage = data['total_shortage']
        net_shortage = max(0, total_shortage - on_order)
        if net_shortage <= 0:
            continue
        result.append({
            'product_id': pid,
            'code': data['code'],
            'name': data['name'],
            'uom': data['uom'],
            'unit_cost': data['unit_cost'],
            'lead_time_days': data['lead_time_days'],
            'shortage_qty': total_shortage,
            'on_order_qty': on_order,
            'net_shortage': net_shortage,
            'current_stock': inv_dict.get(pid, 0),
            'wo_count': data['source_count'],
            'suppliers': []
        })

    result.sort(key=lambda x: x['net_shortage'] * x['unit_cost'], reverse=True)
    result = result[:20]

    # Enrich with supplier history
    if result:
        pids = [r['product_id'] for r in result]
        placeholders = ','.join(['%s'] * len(pids))
        sup_rows = conn.execute(f'''
            SELECT pol.product_id, s.name as supplier_name, s.id as supplier_id,
                   AVG(pol.unit_price) as avg_price,
                   COUNT(pol.id) as order_count,
                   MAX(po.order_date) as last_order_date
            FROM purchase_order_lines pol
            JOIN purchase_orders po ON pol.po_id = po.id
            JOIN suppliers s ON po.supplier_id = s.id
            WHERE pol.product_id IN ({placeholders})
            GROUP BY pol.product_id, s.name, s.id
            ORDER BY pol.product_id, COUNT(pol.id) DESC
        ''', pids).fetchall()
        sup_map = {}
        for r in sup_rows:
            pid = r['product_id']
            if pid not in sup_map:
                sup_map[pid] = []
            sup_map[pid].append({
                'name': r['supplier_name'],
                'supplier_id': r['supplier_id'],
                'avg_price': float(r['avg_price'] or 0),
                'order_count': r['order_count'],
                'last_order': str(r['last_order_date']) if r['last_order_date'] else None
            })
        for item in result:
            item['suppliers'] = sup_map.get(item['product_id'], [])

    return result


def _accumulate(product_shortages, pid, row, shortage):
    if pid not in product_shortages:
        product_shortages[pid] = {
            'code': row['code'],
            'name': row['name'],
            'uom': row['unit_of_measure'],
            'unit_cost': float(row['unit_cost'] or 0),
            'lead_time_days': row['lead_time_days'],
            'total_shortage': 0,
            'source_count': 0
        }
    product_shortages[pid]['total_shortage'] += shortage
    product_shortages[pid]['source_count'] += 1


def _build_mrr_prompt(items):
    lines = []
    for i, item in enumerate(items[:12], 1):
        suppliers_text = ', '.join(
            f"{s['name']} (avg ${s['avg_price']:.2f}, {s['order_count']} orders)"
            for s in item['suppliers'][:3]
        ) if item['suppliers'] else 'No supplier history'

        lines.append(
            f"{i}. {item['code']} - {item['name']}\n"
            f"   Net shortage: {item['net_shortage']} {item['uom']} | "
            f"Stock: {item['current_stock']} | On order: {item['on_order_qty']}\n"
            f"   Unit cost: ${item['unit_cost']:.2f} | Lead time: {item['lead_time_days'] or 'Unknown'} days\n"
            f"   Covering {item['wo_count']} work order(s)\n"
            f"   Known suppliers: {suppliers_text}"
        )

    total_exposure = sum(i['net_shortage'] * i['unit_cost'] for i in items[:12])

    prompt = f"""You are an aerospace MRO procurement intelligence analyst. Analyze the following material shortage data and generate procurement and maintenance decision recommendations.

MATERIAL REQUIREMENTS SHORTAGE SUMMARY
Total items with net shortage: {len(items[:12])}
Total financial exposure: ${total_exposure:,.2f}

SHORTAGE DETAILS:
{chr(10).join(lines)}

Return ONLY a valid JSON object — no markdown, no extra text:
{{
  "summary": "<2-3 sentence executive summary of the procurement situation>",
  "total_risk_exposure": <float, total USD risk if shortages are not resolved>,
  "overall_urgency": <"Low"|"Medium"|"High"|"Critical">,
  "overall_confidence": <"Low"|"Medium"|"High">,
  "recommendations": [
    {{
      "product_code": "<code>",
      "product_name": "<name>",
      "product_id": <integer>,
      "shortage_qty": <float>,
      "suggested_supplier": "<supplier name or 'Seek new supplier'>",
      "suggested_quantity": <float, recommended order qty including buffer>,
      "suggested_timing": "<e.g. 'Order within 48 hours'>",
      "timing_urgency": <"Low"|"Medium"|"High"|"Critical">,
      "why_supplier": "<plain English, 1 sentence>",
      "why_quantity": "<plain English, 1 sentence>",
      "why_now": "<plain English, 1 sentence>",
      "if_ignored": "<plain English, 1 sentence consequence>",
      "downtime_reduction_pct": <integer 0-100>,
      "net_cash_impact": <float, negative=cost, positive=saving>,
      "confidence": <"Low"|"Medium"|"High">,
      "risk_exposure": <float, USD cost of inaction>,
      "recommended_actions": [<"create_po"|"create_wo"|"flag_expedite">, ...]
    }}
  ],
  "cfo_summary": "<3-4 sentence summary suitable for CFO approval, including total exposure, recommended spend, and expected risk reduction>"
}}"""
    return prompt


def _call_ai(prompt):
    try:
        from openai import OpenAI
        client = OpenAI(
            api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
            base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
        )
        response = client.chat.completions.create(
            model='gpt-4o',
            messages=[
                {'role': 'system', 'content': 'You are a procurement intelligence expert. Always respond with valid JSON only.'},
                {'role': 'user', 'content': prompt}
            ],
            temperature=0.3,
            max_tokens=2500
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith('```'):
            raw = raw.split('```')[1]
            if raw.startswith('json'):
                raw = raw[4:]
        return json.loads(raw)
    except Exception as e:
        logger.error(f"MRR AI call failed: {e}", exc_info=True)
        return None


def _fallback_recommendations(items):
    recs = []
    for item in items[:10]:
        best_supplier = item['suppliers'][0]['name'] if item['suppliers'] else 'Seek new supplier'
        buffer_qty = round(item['net_shortage'] * 1.2)
        recs.append({
            'product_code': item['code'],
            'product_name': item['name'],
            'product_id': item['product_id'],
            'shortage_qty': item['net_shortage'],
            'suggested_supplier': best_supplier,
            'suggested_quantity': buffer_qty,
            'suggested_timing': 'Order as soon as possible',
            'timing_urgency': 'High' if item['net_shortage'] * item['unit_cost'] > 5000 else 'Medium',
            'why_supplier': 'Previously used supplier with order history.' if item['suppliers'] else 'No supplier history — sourcing required.',
            'why_quantity': f"Shortage of {item['net_shortage']} units plus 20% buffer to reduce reorder risk.",
            'why_now': f"Covering {item['wo_count']} active work order(s) currently unable to proceed.",
            'if_ignored': 'Work orders will remain blocked, increasing downtime exposure.',
            'downtime_reduction_pct': 60,
            'net_cash_impact': -(item['net_shortage'] * item['unit_cost']),
            'confidence': 'Low',
            'risk_exposure': item['net_shortage'] * item['unit_cost'] * 1.5,
            'recommended_actions': ['create_po', 'flag_expedite']
        })

    total_exposure = sum(i['net_shortage'] * i['unit_cost'] for i in items[:10])
    return {
        'summary': f"{len(items)} materials have unresolved shortages. AI analysis unavailable — showing statistical estimates based on shortage data.",
        'total_risk_exposure': total_exposure * 1.5,
        'overall_urgency': 'High' if total_exposure > 10000 else 'Medium',
        'overall_confidence': 'Low',
        'recommendations': recs,
        'cfo_summary': f"Total of {len(items)} materials have net shortages with an estimated financial exposure of ${total_exposure:,.2f}. Immediate procurement action is recommended to unblock active work orders."
    }


def _save_decision(conn, job_id, user_id, result, prompt, items):
    conn.execute(
        '''INSERT INTO mrr_ai_decisions
           (job_id, generated_by, status, input_summary, recommendations, raw_prompt, ai_version)
           VALUES (%s, %s, 'done', %s, %s, %s, 'gpt-4o-v1')
           ON CONFLICT (job_id) DO UPDATE SET
               status = 'done',
               recommendations = EXCLUDED.recommendations,
               generated_at = CURRENT_TIMESTAMP''',
        (
            job_id, user_id,
            json.dumps({'item_count': len(items), 'total_exposure': sum(i['net_shortage'] * i['unit_cost'] for i in items)}),
            json.dumps(result),
            prompt[:1000]
        )
    )
    conn.commit()


def _run_analysis_thread(job_id, user_id):
    try:
        db = Database()
        conn = db.get_connection()
        items = _fetch_mrr_data(conn)

        if not items:
            with _jobs_lock:
                _jobs[job_id] = {'status': 'done', 'result': {
                    'success': True,
                    'empty': True,
                    'summary': 'No material shortages found. All requirements are currently satisfied.',
                    'recommendations': [],
                    'total_risk_exposure': 0,
                    'overall_urgency': 'Low',
                    'overall_confidence': 'High',
                    'cfo_summary': 'All material requirements are currently satisfied. No procurement action required.'
                }}
            conn.close()
            return

        prompt = _build_mrr_prompt(items)
        result = _call_ai(prompt)
        used_fallback = False

        if not result:
            result = _fallback_recommendations(items)
            used_fallback = True

        _save_decision(conn, job_id, user_id, result, prompt, items)
        conn.close()

        with _jobs_lock:
            _jobs[job_id] = {
                'status': 'done',
                'result': {
                    'success': True,
                    'fallback': used_fallback,
                    'generated_at': datetime.utcnow().isoformat(),
                    'job_id': job_id,
                    **result
                }
            }
    except Exception as e:
        logger.error(f"MRR AI analysis error for job {job_id}: {e}", exc_info=True)
        with _jobs_lock:
            _jobs[job_id] = {'status': 'error', 'error': str(e)}

        try:
            db2 = Database()
            conn2 = db2.get_connection()
            conn2.execute(
                "UPDATE mrr_ai_decisions SET status='error', error_message=%s WHERE job_id=%s",
                (str(e), job_id)
            )
            conn2.commit()
            conn2.close()
        except Exception:
            pass


@mrr_ai_bp.route('/ai-decisions', methods=['POST'])
@login_required
def start_analysis():
    job_id = str(uuid.uuid4())[:16]
    user_id = session.get('user_id')

    db = Database()
    conn = db.get_connection()
    try:
        conn.execute(
            "INSERT INTO mrr_ai_decisions (job_id, generated_by, status) VALUES (%s, %s, 'generating')",
            (job_id, user_id)
        )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()

    with _jobs_lock:
        _jobs[job_id] = {'status': 'generating'}

    t = threading.Thread(target=_run_analysis_thread, args=(job_id, user_id), daemon=True)
    t.start()

    return jsonify({'status': 'generating', 'job_id': job_id})


@mrr_ai_bp.route('/ai-decisions/poll/<job_id>', methods=['GET'])
@login_required
def poll_analysis(job_id):
    with _jobs_lock:
        job = _jobs.get(job_id)

    if not job:
        db = Database()
        conn = db.get_connection()
        try:
            row = conn.execute(
                "SELECT * FROM mrr_ai_decisions WHERE job_id = %s", (job_id,)
            ).fetchone()
            if row and row['status'] == 'done' and row['recommendations']:
                return jsonify({'success': True, 'job_id': job_id, **row['recommendations']})
            if row and row['status'] == 'error':
                return jsonify({'success': False, 'error': row['error_message']}), 500
        finally:
            conn.close()
        return jsonify({'status': 'not_found'}), 404

    if job['status'] == 'generating':
        return jsonify({'status': 'generating', 'job_id': job_id})
    if job['status'] == 'done':
        with _jobs_lock:
            _jobs.pop(job_id, None)
        return jsonify(job['result'])
    if job['status'] == 'error':
        with _jobs_lock:
            _jobs.pop(job_id, None)
        return jsonify({'success': False, 'error': job['error']}), 500

    return jsonify({'status': 'unknown'})


@mrr_ai_bp.route('/ai-decisions/log', methods=['GET'])
@login_required
def decision_log():
    db = Database()
    conn = db.get_connection()
    try:
        rows = conn.execute(
            '''SELECT id, job_id, generated_at, generated_by, status, input_summary, ai_version
               FROM mrr_ai_decisions ORDER BY generated_at DESC LIMIT 50'''
        ).fetchall()
        return jsonify({'success': True, 'decisions': [dict(r) for r in rows]})
    finally:
        conn.close()
