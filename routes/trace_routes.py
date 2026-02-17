from flask import Blueprint, render_template, request, jsonify, session
from models import Database
from functools import wraps
import logging

logger = logging.getLogger(__name__)

trace_bp = Blueprint('trace', __name__, url_prefix='/trace')

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            from flask import redirect, url_for
            return redirect(url_for('auth_routes.login'))
        return f(*args, **kwargs)
    return decorated_function

@trace_bp.route('/')
@login_required
def dashboard():
    return render_template('trace/dashboard.html')

@trace_bp.route('/search', methods=['POST'])
@login_required
def search():
    try:
        from services.traceability_engine import TraceabilityEngine

        data = request.get_json() or {}
        query_type = data.get('query_type', 'part_number')
        query_value = data.get('query_value', '').strip()

        if not query_value:
            return jsonify({'success': False, 'error': 'Search value is required'}), 400

        events = TraceabilityEngine.search(query_type, query_value)
        summary = TraceabilityEngine.build_summary(events)
        graph_data = TraceabilityEngine.build_graph_data(events)
        cost_data = TraceabilityEngine.build_cost_data(events)
        risk_analysis = TraceabilityEngine.get_ai_risk_analysis(events, summary)

        return jsonify({
            'success': True,
            'events': events,
            'summary': summary,
            'graph_data': graph_data,
            'cost_data': cost_data,
            'risk_analysis': risk_analysis,
            'query': {'type': query_type, 'value': query_value}
        })
    except Exception as e:
        logger.error(f"Trace search error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@trace_bp.route('/quick/<query_type>/<query_value>')
@login_required
def quick_trace(query_type, query_value):
    return render_template('trace/dashboard.html',
                           auto_search=True,
                           query_type=query_type,
                           query_value=query_value)
