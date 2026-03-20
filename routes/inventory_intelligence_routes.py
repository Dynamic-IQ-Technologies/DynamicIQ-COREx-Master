from flask import Blueprint, render_template, request, jsonify, session
from auth import login_required
import logging

logger = logging.getLogger(__name__)

inv_intel_bp = Blueprint('inv_intel', __name__, url_prefix='/inventory/intelligence')


@inv_intel_bp.route('/')
@login_required
def dashboard():
    return render_template('inventory/intelligence.html')


@inv_intel_bp.route('/api/data')
@login_required
def api_data():
    try:
        from services.inventory_intelligence import build_dashboard_data
        data = build_dashboard_data()
        return jsonify({'success': True, **data})
    except Exception as e:
        logger.error(f"Intelligence data error: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@inv_intel_bp.route('/api/ai-summary', methods=['POST'])
@login_required
def api_ai_summary():
    try:
        from services.inventory_intelligence import build_dashboard_data, get_ai_summary
        data = build_dashboard_data()
        narrative = get_ai_summary(data['items'])
        if not narrative:
            narrative = (
                "AI analysis is currently unavailable. Based on the inventory data, "
                "review items flagged as Critical or Warning and prioritize reorders "
                "for parts with stock below reorder point."
            )
        return jsonify({'success': True, 'narrative': narrative})
    except Exception as e:
        logger.error(f"AI summary error: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@inv_intel_bp.route('/api/generate-forecast', methods=['POST'])
@login_required
def api_generate_forecast():
    try:
        from services.inventory_intelligence import (
            build_dashboard_data, save_cycle_count_schedule, save_ai_recommendations
        )
        data = build_dashboard_data()
        count_n = save_cycle_count_schedule(data['items'])
        save_ai_recommendations(data['items'])
        return jsonify({'success': True, 'scheduled': count_n})
    except Exception as e:
        logger.error(f"Forecast generation error: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@inv_intel_bp.route('/api/what-if', methods=['POST'])
@login_required
def api_what_if():
    try:
        from services.inventory_intelligence import (
            build_dashboard_data, simulate_what_if
        )
        body = request.get_json() or {}
        demand_mult = float(body.get('demand_multiplier', 1.0))
        supply_delay = int(body.get('supply_delay_days', 0))

        data = build_dashboard_data()
        results = []
        for item in data['items']:
            sim = simulate_what_if(item['forecast'], demand_mult, supply_delay)
            results.append({
                'part_code': item['part_code'],
                'part_name': item['part_name'],
                'current_qty': item['quantity'],
                'risk_level': item['risk_level'],
                'simulation': sim,
            })
        return jsonify({'success': True, 'results': results,
                        'params': {'demand_multiplier': demand_mult, 'supply_delay_days': supply_delay}})
    except Exception as e:
        logger.error(f"What-if error: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@inv_intel_bp.route('/api/complete-count/<int:count_id>', methods=['POST'])
@login_required
def api_complete_count(count_id):
    try:
        from services.inventory_intelligence import mark_count_complete
        body = request.get_json() or {}
        counted_qty = float(body.get('counted_qty', 0))
        notes = body.get('notes', '')
        mark_count_complete(count_id, counted_qty, notes)
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Complete count error: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@inv_intel_bp.route('/api/dismiss-recommendation/<int:rec_id>', methods=['POST'])
@login_required
def api_dismiss_recommendation(rec_id):
    try:
        from services.inventory_intelligence import dismiss_recommendation
        dismiss_recommendation(rec_id, session.get('user_id'))
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Dismiss recommendation error: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500
