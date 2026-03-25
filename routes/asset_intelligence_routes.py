from flask import Blueprint, jsonify, session
from models import Database
from auth import login_required
import logging
import json
import os
from datetime import datetime, timedelta, date

logger = logging.getLogger(__name__)

asset_intel_bp = Blueprint('asset_intel', __name__, url_prefix='/api/asset-intelligence')


def _gather_asset_data(product_id, work_order_id, conn):
    """Gather all available data about this asset/product from the database."""
    data = {}

    product = conn.execute(
        'SELECT * FROM products WHERE id = %s', (product_id,)
    ).fetchone()
    data['product'] = dict(product) if product else {}

    inventory_rows = conn.execute(
        'SELECT * FROM inventory WHERE product_id = %s ORDER BY last_updated DESC LIMIT 10',
        (product_id,)
    ).fetchall()
    data['inventory'] = [dict(r) for r in inventory_rows]

    historical_wos = conn.execute(
        '''SELECT wo_number, status, priority, planned_start_date, planned_end_date,
                  actual_start_date, actual_end_date, material_cost, labor_cost,
                  overhead_cost, description, repair_category, workorder_type,
                  created_at
           FROM work_orders
           WHERE product_id = %s AND id != %s
           ORDER BY created_at DESC LIMIT 20''',
        (product_id, work_order_id)
    ).fetchall()
    data['historical_wos'] = [dict(r) for r in historical_wos]

    current_wo = conn.execute(
        'SELECT * FROM work_orders WHERE id = %s', (work_order_id,)
    ).fetchone()
    data['current_wo'] = dict(current_wo) if current_wo else {}

    time_records = conn.execute(
        '''SELECT SUM(hours_worked) as total_hours, COUNT(*) as entry_count
           FROM work_order_time_tracking
           WHERE work_order_id IN (
               SELECT id FROM work_orders WHERE product_id = %s
           )''',
        (product_id,)
    ).fetchone()
    data['total_labor_hours'] = float(time_records['total_hours'] or 0) if time_records else 0

    po_data = conn.execute(
        '''SELECT pol.unit_price, pol.quantity, po.order_date, po.status,
                  s.name as supplier_name, s.lead_time_days
           FROM purchase_order_lines pol
           JOIN purchase_orders po ON pol.po_id = po.id
           LEFT JOIN suppliers s ON po.supplier_id = s.id
           WHERE pol.product_id = %s
           ORDER BY po.order_date DESC LIMIT 15''',
        (product_id,)
    ).fetchall()
    data['procurement_history'] = [dict(r) for r in po_data]

    ndt_records = conn.execute(
        '''SELECT status, created_at, description
           FROM ndt_work_orders
           WHERE work_order_id IN (
               SELECT id FROM work_orders WHERE product_id = %s
           ) LIMIT 10''',
        (product_id,)
    ).fetchall()
    data['ndt_records'] = [dict(r) for r in ndt_records]

    capa_records = conn.execute(
        '''SELECT title, status, severity, created_at
           FROM qms_capa
           ORDER BY created_at DESC LIMIT 5'''
    ).fetchall()
    data['qms_capa'] = [dict(r) for r in capa_records]

    faa_certs = conn.execute(
        '''SELECT cert_number, expiry_date, status
           FROM faa_8130_certificates
           WHERE work_order_id = %s LIMIT 5''',
        (work_order_id,)
    ).fetchall()
    data['faa_certs'] = [dict(r) for r in faa_certs]

    inv_items = data['inventory']
    total_qty = sum(float(i.get('quantity') or 0) for i in inv_items)
    safety_stock = float((inv_items[0].get('safety_stock') or 0)) if inv_items else 0
    data['stock_summary'] = {
        'total_quantity': total_qty,
        'safety_stock': safety_stock,
        'below_safety': total_qty < safety_stock
    }

    completed_wos = [w for w in historical_wos if w.get('status') in ('Completed', 'Closed')]
    data['repair_count'] = len(historical_wos)
    data['completed_repair_count'] = len(completed_wos)

    mtbf_days = None
    if len(completed_wos) >= 2:
        dates = []
        for w in completed_wos:
            d = w.get('actual_end_date') or w.get('planned_end_date')
            if d:
                dates.append(d if isinstance(d, date) else d)
        if len(dates) >= 2:
            sorted_dates = sorted(dates)
            gaps = [(sorted_dates[i+1] - sorted_dates[i]).days
                    for i in range(len(sorted_dates)-1)
                    if hasattr(sorted_dates[i+1] - sorted_dates[i], 'days')]
            if gaps:
                mtbf_days = sum(gaps) / len(gaps)
    data['mtbf_days'] = mtbf_days

    cost_history = [float(w.get('material_cost') or 0) + float(w.get('labor_cost') or 0)
                    for w in completed_wos if w.get('material_cost') or w.get('labor_cost')]
    data['avg_repair_cost'] = (sum(cost_history) / len(cost_history)) if cost_history else 0

    return data


def _build_ai_prompt(data):
    product = data.get('product', {})
    current_wo = data.get('current_wo', {})
    repair_count = data.get('repair_count', 0)
    mtbf = data.get('mtbf_days')
    avg_cost = data.get('avg_repair_cost', 0)
    stock = data.get('stock_summary', {})
    procurement = data.get('procurement_history', [])
    ndt = data.get('ndt_records', [])
    faa = data.get('faa_certs', [])
    labor_hours = data.get('total_labor_hours', 0)

    supplier_names = list(set(p.get('supplier_name') for p in procurement if p.get('supplier_name')))
    price_variance = 0
    prices = [float(p.get('unit_price') or 0) for p in procurement if p.get('unit_price')]
    if len(prices) > 1:
        price_variance = round((max(prices) - min(prices)) / max(prices) * 100, 1)

    faa_status = 'Certificates present' if faa else 'No FAA 8130 certificates on file'
    ndt_count = len(ndt)

    prompt = f"""You are an expert aerospace MRO asset intelligence analyst. Analyze the following asset data and return a comprehensive JSON intelligence report.

ASSET: {product.get('name', 'Unknown')} (Part: {product.get('code', 'N/A')})
Category: {product.get('product_category', 'N/A')} | Type: {product.get('product_type', 'N/A')}
Manufacturer: {product.get('manufacturer', 'N/A')} | Lead Time: {product.get('lead_time_days', 'N/A')} days
ECCN: {product.get('eccn', 'N/A')}

CURRENT WORK ORDER: {current_wo.get('wo_number', 'N/A')}
Status: {current_wo.get('status', 'N/A')} | Priority: {current_wo.get('priority', 'N/A')}
Type: {current_wo.get('workorder_type', 'N/A')} | Repair Category: {current_wo.get('repair_category', 'N/A')}
Description: {current_wo.get('description', 'None')}
AOG: {bool(current_wo.get('is_aog'))} | Warranty: {bool(current_wo.get('is_warranty'))}

HISTORICAL DATA:
- Total repair history: {repair_count} work orders
- Mean Time Between Failure (MTBF): {f'{mtbf:.0f} days' if mtbf else 'Insufficient data'}
- Average repair cost: ${avg_cost:,.2f}
- Total labor hours across all repairs: {labor_hours:.1f} hours
- Suppliers used: {', '.join(supplier_names[:5]) if supplier_names else 'None on record'}
- Supplier price variance: {price_variance}%
- NDT inspections: {ndt_count}
- FAA Compliance: {faa_status}

INVENTORY STATUS:
- Current stock: {stock.get('total_quantity', 0)} units
- Safety stock level: {stock.get('safety_stock', 0)} units
- Below safety stock: {stock.get('below_safety', False)}

Return ONLY a valid JSON object with this exact structure (no markdown, no extra text):
{{
  "criticality_score": <integer 1-100>,
  "health_score": <integer 1-100>,
  "health_label": <"Critical"|"Poor"|"Fair"|"Good"|"Excellent">,
  "predicted_failure_window": {{
    "start_days": <integer>,
    "end_days": <integer>,
    "label": <human-readable string>
  }},
  "downtime_cost_estimate": <float, estimated USD cost if failure unaddressed>,
  "compliance_status": <"Pass"|"At Risk"|"Blocked">,
  "dna_profile": {{
    "maintenance_frequency": <"Low"|"Moderate"|"High"|"Critical">,
    "mtbf_assessment": <string>,
    "failure_mode_patterns": [<string>, ...],
    "environmental_exposure": <string>,
    "vendor_quality_risk": <"Low"|"Moderate"|"High">,
    "lead_time_risk": <"Low"|"Moderate"|"High">,
    "summary": <1-2 sentence DNA summary>
  }},
  "failure_predictions": {{
    "7_day": {{"probability": <0-100 integer>, "confidence": <"Low"|"Medium"|"High">, "drivers": [<string>, ...]}},
    "30_day": {{"probability": <0-100 integer>, "confidence": <"Low"|"Medium"|"High">, "drivers": [<string>, ...]}},
    "90_day": {{"probability": <0-100 integer>, "confidence": <"Low"|"Medium"|"High">, "drivers": [<string>, ...]}}
  }},
  "likely_failure_components": [<string>, ...],
  "prediction_explanation": <1-2 sentence plain English explanation>,
  "financial_impact": {{
    "downtime_cost_per_hour": <float>,
    "total_downtime_exposure": <float>,
    "repair_vs_replace": <"Repair"|"Replace"|"Monitor">,
    "repair_vs_replace_rationale": <string>,
    "cost_of_delay_per_day": <float>,
    "inventory_availability_risk": <"Low"|"Moderate"|"High"|"Critical">
  }},
  "recommendations": [
    {{
      "rank": <1-6>,
      "type": <"Preventive Maintenance"|"Inventory Reservation"|"Supplier Action"|"Expedite"|"Compliance"|"Other">,
      "action": <string>,
      "risk_reduced_pct": <integer>,
      "cost_impact": <float>,
      "urgency": <"Low"|"Medium"|"High">,
      "rationale": <string>
    }}
  ],
  "compliance_evaluation": {{
    "faa_status": <string>,
    "as9100_status": <"Pass"|"At Risk"|"Blocked">,
    "itar_flag": <boolean>,
    "missing_certs": [<string>, ...],
    "blocking_issues": [<string>, ...],
    "notes": <string>
  }},
  "explainability": {{
    "data_sources": [<string>, ...],
    "key_drivers": [<string>, ...],
    "overall_confidence": <"Low"|"Medium"|"High">,
    "if_ignored": <1-2 sentence consequence scenario>
  }}
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
                {'role': 'system', 'content': 'You are an aerospace MRO asset intelligence expert. Always respond with valid JSON only.'},
                {'role': 'user', 'content': prompt}
            ],
            temperature=0.3,
            max_tokens=2000
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith('```'):
            raw = raw.split('```')[1]
            if raw.startswith('json'):
                raw = raw[4:]
        return json.loads(raw)
    except Exception as e:
        logger.error(f"AI call failed: {e}", exc_info=True)
        return None


def _fallback_intelligence(data):
    """Return a basic intelligence snapshot when AI is unavailable."""
    repair_count = data.get('repair_count', 0)
    mtbf = data.get('mtbf_days')
    stock = data.get('stock_summary', {})

    criticality = min(100, 30 + repair_count * 5)
    health = max(10, 80 - repair_count * 4)
    if health >= 70:
        health_label = 'Good'
    elif health >= 50:
        health_label = 'Fair'
    elif health >= 30:
        health_label = 'Poor'
    else:
        health_label = 'Critical'

    return {
        'criticality_score': criticality,
        'health_score': health,
        'health_label': health_label,
        'predicted_failure_window': {
            'start_days': 30,
            'end_days': 90,
            'label': 'Within 30-90 days (estimated)'
        },
        'downtime_cost_estimate': data.get('avg_repair_cost', 0) * 1.5,
        'compliance_status': 'At Risk',
        'dna_profile': {
            'maintenance_frequency': 'High' if repair_count > 5 else 'Moderate',
            'mtbf_assessment': f'{mtbf:.0f} days average between repairs' if mtbf else 'Insufficient history',
            'failure_mode_patterns': ['Wear-related failure', 'Inspection-triggered maintenance'],
            'environmental_exposure': 'Standard operational conditions assumed',
            'vendor_quality_risk': 'Moderate',
            'lead_time_risk': 'Moderate',
            'summary': f'Asset has {repair_count} repair events on record. AI analysis unavailable - showing statistical estimates.'
        },
        'failure_predictions': {
            '7_day': {'probability': 15, 'confidence': 'Low', 'drivers': ['Statistical estimate only']},
            '30_day': {'probability': 35, 'confidence': 'Low', 'drivers': ['Repair frequency', 'Age of last repair']},
            '90_day': {'probability': 55, 'confidence': 'Low', 'drivers': ['Historical MTBF', 'Stock levels']}
        },
        'likely_failure_components': ['Component wear', 'Seal degradation'],
        'prediction_explanation': 'Estimates based on repair history only. Enable AI for full predictive analysis.',
        'financial_impact': {
            'downtime_cost_per_hour': 2500,
            'total_downtime_exposure': data.get('avg_repair_cost', 0) * 2,
            'repair_vs_replace': 'Monitor',
            'repair_vs_replace_rationale': 'Insufficient data for full recommendation.',
            'cost_of_delay_per_day': 1200,
            'inventory_availability_risk': 'High' if stock.get('below_safety') else 'Moderate'
        },
        'recommendations': [
            {
                'rank': 1,
                'type': 'Preventive Maintenance',
                'action': 'Schedule full inspection within 30 days',
                'risk_reduced_pct': 25,
                'cost_impact': -500,
                'urgency': 'Medium',
                'rationale': 'Based on repair frequency pattern'
            },
            {
                'rank': 2,
                'type': 'Inventory Reservation',
                'action': 'Reserve critical replacement parts',
                'risk_reduced_pct': 15,
                'cost_impact': -200,
                'urgency': 'Medium' if not stock.get('below_safety') else 'High',
                'rationale': 'Stock is below safety level' if stock.get('below_safety') else 'Maintain buffer stock'
            }
        ],
        'compliance_evaluation': {
            'faa_status': 'Review required',
            'as9100_status': 'At Risk',
            'itar_flag': False,
            'missing_certs': ['Review FAA 8130 status'],
            'blocking_issues': [],
            'notes': 'Manual compliance review recommended'
        },
        'explainability': {
            'data_sources': ['Work order history', 'Inventory records'],
            'key_drivers': ['Repair frequency', 'Stock levels'],
            'overall_confidence': 'Low',
            'if_ignored': 'Risk of unplanned downtime increases over time without preventive action.'
        }
    }


@asset_intel_bp.route('/workorder/<int:work_order_id>', methods=['GET'])
@login_required
def get_intelligence(work_order_id):
    db = Database()
    conn = db.get_connection()
    try:
        cached = conn.execute(
            '''SELECT * FROM asset_intelligence_snapshots
               WHERE work_order_id = %s AND is_current = TRUE
               ORDER BY generated_at DESC LIMIT 1''',
            (work_order_id,)
        ).fetchone()

        if cached:
            age = (datetime.utcnow() - cached['generated_at']).total_seconds() / 3600
            if age < 24:
                return jsonify({
                    'success': True,
                    'cached': True,
                    'generated_at': cached['generated_at'].isoformat(),
                    'criticality_score': cached['criticality_score'],
                    'health_score': cached['health_score'],
                    'health_label': cached['health_label'],
                    'compliance_status': cached['compliance_status'],
                    'downtime_cost_estimate': float(cached['downtime_cost_estimate'] or 0),
                    'dna_profile': cached['dna_profile'],
                    'failure_predictions': cached['failure_predictions'],
                    'financial_impact': cached['financial_impact'],
                    'recommendations': cached['recommendations'],
                    'compliance_evaluation': cached['compliance_evaluation'],
                    'explainability': cached['explainability'],
                    'predicted_failure_window': cached['dna_profile'].get('predicted_failure_window') if cached['dna_profile'] else None
                })

        wo = conn.execute('SELECT * FROM work_orders WHERE id = %s', (work_order_id,)).fetchone()
        if not wo:
            return jsonify({'success': False, 'error': 'Work order not found'}), 404

        product_id = wo['product_id']
        if not product_id:
            return jsonify({'success': False, 'error': 'No product linked to this work order'}), 400

        asset_data = _gather_asset_data(product_id, work_order_id, conn)
        prompt = _build_ai_prompt(asset_data)
        intelligence = _call_ai(prompt)

        if not intelligence:
            intelligence = _fallback_intelligence(asset_data)
            used_fallback = True
        else:
            used_fallback = False

        conn.execute(
            '''UPDATE asset_intelligence_snapshots SET is_current = FALSE
               WHERE work_order_id = %s''',
            (work_order_id,)
        )

        pfw = intelligence.get('predicted_failure_window', {})
        start_days = pfw.get('start_days', 30)
        end_days = pfw.get('end_days', 90)
        today = date.today()

        conn.execute(
            '''INSERT INTO asset_intelligence_snapshots
               (work_order_id, product_id, generated_by, criticality_score, health_score,
                health_label, predicted_failure_start, predicted_failure_end,
                downtime_cost_estimate, compliance_status,
                dna_profile, failure_predictions, financial_impact,
                recommendations, compliance_evaluation, explainability,
                raw_ai_response, data_sources, is_current)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)''',
            (
                work_order_id, product_id, session.get('user_id'),
                intelligence.get('criticality_score'),
                intelligence.get('health_score'),
                intelligence.get('health_label'),
                today + timedelta(days=start_days),
                today + timedelta(days=end_days),
                intelligence.get('downtime_cost_estimate'),
                intelligence.get('compliance_status'),
                json.dumps(intelligence.get('dna_profile', {})),
                json.dumps(intelligence.get('failure_predictions', {})),
                json.dumps(intelligence.get('financial_impact', {})),
                json.dumps(intelligence.get('recommendations', [])),
                json.dumps(intelligence.get('compliance_evaluation', {})),
                json.dumps(intelligence.get('explainability', {})),
                prompt[:500],
                json.dumps(intelligence.get('explainability', {}).get('data_sources', [])),
            )
        )
        conn.commit()

        return jsonify({
            'success': True,
            'cached': False,
            'fallback': used_fallback,
            'generated_at': datetime.utcnow().isoformat(),
            **intelligence
        })

    except Exception as e:
        logger.error(f"Asset intelligence error: {e}", exc_info=True)
        conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        conn.close()


@asset_intel_bp.route('/workorder/<int:work_order_id>/refresh', methods=['POST'])
@login_required
def refresh_intelligence(work_order_id):
    db = Database()
    conn = db.get_connection()
    try:
        conn.execute(
            'UPDATE asset_intelligence_snapshots SET is_current = FALSE WHERE work_order_id = %s',
            (work_order_id,)
        )
        conn.commit()
        conn.close()
        return get_intelligence(work_order_id)
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)}), 500
