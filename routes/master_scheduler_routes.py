"""
AI Super Master Scheduler Routes
Master Production Schedule management with AI-powered optimization.
"""
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session
from functools import wraps
from datetime import datetime, timedelta
import json
import os

from models import Database
from engines.master_scheduler import MasterSchedulerEngine

try:
    from openai import OpenAI
    client = OpenAI(
        api_key=os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY"),
        base_url=os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")
    )
    OPENAI_AVAILABLE = True
except Exception:
    OPENAI_AVAILABLE = False

master_scheduler_bp = Blueprint('master_scheduler_routes', __name__)

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.', 'danger')
            return redirect(url_for('auth_routes.login'))
        return f(*args, **kwargs)
    return decorated_function

def role_required(*roles):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if 'role' not in session or session['role'] not in roles:
                flash('Access denied. Insufficient permissions.', 'danger')
                return redirect(url_for('main_routes.dashboard'))
            return f(*args, **kwargs)
        return wrapper
    return decorator


@master_scheduler_bp.route('/master-scheduler')
@login_required
@role_required('Admin', 'Planner')
def dashboard():
    db = Database()
    conn = db.get_connection()
    engine = MasterSchedulerEngine(conn)
    
    active_schedule = conn.execute('''
        SELECT * FROM master_schedules WHERE is_active = 1 LIMIT 1
    ''').fetchone()
    
    recent_schedules = conn.execute('''
        SELECT ms.*, u.username as created_by_name
        FROM master_schedules ms
        LEFT JOIN users u ON ms.created_by = u.id
        ORDER BY ms.created_at DESC
        LIMIT 5
    ''').fetchall()
    
    summary = None
    at_risk_orders = []
    bottlenecks = []
    exceptions = []
    recommendations = []
    
    if active_schedule:
        summary = engine.get_schedule_summary(active_schedule['id'])
        at_risk_orders = engine.get_at_risk_orders(active_schedule['id'], 8)
        bottlenecks = engine.get_bottleneck_analysis(active_schedule['id'])
        
        exceptions = conn.execute('''
            SELECT * FROM schedule_exceptions
            WHERE schedule_id = ? AND is_resolved = 0
            ORDER BY 
                CASE severity WHEN 'Critical' THEN 1 WHEN 'Warning' THEN 2 ELSE 3 END,
                created_at DESC
            LIMIT 10
        ''', (active_schedule['id'],)).fetchall()
        
        recommendations = conn.execute('''
            SELECT * FROM schedule_recommendations
            WHERE schedule_id = ? AND status = 'Pending'
            ORDER BY priority_score DESC
            LIMIT 5
        ''', (active_schedule['id'],)).fetchall()
    
    work_centers = conn.execute('''
        SELECT wc.*, 
               (SELECT COALESCE(SUM(woo.planned_hours), 0) 
                FROM work_order_operations woo
                JOIN work_orders wo ON woo.work_order_id = wo.id
                WHERE woo.work_center_id = wc.id
                AND woo.status IN ('Pending', 'In Progress')
                AND wo.status IN ('Planned', 'In Progress', 'Released')) as current_load
        FROM work_centers wc
        WHERE wc.status = 'Active'
        ORDER BY wc.code
    ''').fetchall()
    
    total_orders = conn.execute('''
        SELECT COUNT(*) as count FROM work_orders 
        WHERE status IN ('Planned', 'Released', 'In Progress')
    ''').fetchone()['count']
    
    late_orders = conn.execute('''
        SELECT COUNT(*) as count FROM work_orders
        WHERE status IN ('Planned', 'Released', 'In Progress')
        AND planned_end_date < date('now')
    ''').fetchone()['count']
    
    otd_rate = ((total_orders - late_orders) / total_orders * 100) if total_orders > 0 else 100
    
    conn.close()
    
    return render_template('master_scheduler/dashboard.html',
                          active_schedule=active_schedule,
                          recent_schedules=recent_schedules,
                          summary=summary,
                          at_risk_orders=at_risk_orders,
                          bottlenecks=bottlenecks,
                          exceptions=exceptions,
                          recommendations=recommendations,
                          work_centers=work_centers,
                          total_orders=total_orders,
                          late_orders=late_orders,
                          otd_rate=round(otd_rate, 1))


@master_scheduler_bp.route('/master-scheduler/create', methods=['GET', 'POST'])
@login_required
@role_required('Admin', 'Planner')
def create_schedule():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            engine = MasterSchedulerEngine(conn)
            schedule_number = engine.generate_schedule_number()
            
            name = request.form.get('name', f'Master Schedule {schedule_number}')
            description = request.form.get('description', '')
            horizon_start = request.form.get('horizon_start', datetime.now().strftime('%Y-%m-%d'))
            horizon_end = request.form.get('horizon_end', (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d'))
            time_bucket = request.form.get('time_bucket', 'Daily')
            
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO master_schedules (
                    schedule_number, name, description, horizon_start, horizon_end,
                    time_bucket, status, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, 'Draft', ?)
            ''', (schedule_number, name, description, horizon_start, horizon_end,
                  time_bucket, session.get('user_id')))
            
            schedule_id = cursor.lastrowid
            conn.commit()
            
            result = engine.generate_schedule(schedule_id, horizon_start, horizon_end, session.get('user_id'))
            
            flash(f'Schedule {schedule_number} created with {result["orders_scheduled"]} orders and {result["exceptions_detected"]} exceptions detected.', 'success')
            conn.close()
            return redirect(url_for('master_scheduler_routes.view_schedule', id=schedule_id))
            
        except Exception as e:
            conn.rollback()
            conn.close()
            flash(f'Error creating schedule: {str(e)}', 'danger')
            return redirect(url_for('master_scheduler_routes.dashboard'))
    
    conn.close()
    return render_template('master_scheduler/schedule_form.html', action='create')


@master_scheduler_bp.route('/master-scheduler/schedules/<int:id>')
@login_required
@role_required('Admin', 'Planner')
def view_schedule(id):
    db = Database()
    conn = db.get_connection()
    engine = MasterSchedulerEngine(conn)
    
    schedule = conn.execute('''
        SELECT ms.*, u.username as created_by_name, a.username as approved_by_name
        FROM master_schedules ms
        LEFT JOIN users u ON ms.created_by = u.id
        LEFT JOIN users a ON ms.approved_by = a.id
        WHERE ms.id = ?
    ''', (id,)).fetchone()
    
    if not schedule:
        conn.close()
        flash('Schedule not found', 'danger')
        return redirect(url_for('master_scheduler_routes.dashboard'))
    
    summary = engine.get_schedule_summary(id)
    at_risk_orders = engine.get_at_risk_orders(id, 10)
    bottlenecks = engine.get_bottleneck_analysis(id)
    
    items = conn.execute('''
        SELECT * FROM master_schedule_items
        WHERE schedule_id = ?
        ORDER BY priority DESC, scheduled_end ASC
    ''', (id,)).fetchall()
    
    exceptions = conn.execute('''
        SELECT se.*, wc.code as work_center_code, u.username as resolved_by_name
        FROM schedule_exceptions se
        LEFT JOIN work_centers wc ON se.work_center_id = wc.id
        LEFT JOIN users u ON se.resolved_by = u.id
        WHERE se.schedule_id = ?
        ORDER BY 
            CASE se.severity WHEN 'Critical' THEN 1 WHEN 'Warning' THEN 2 ELSE 3 END,
            se.is_resolved ASC,
            se.created_at DESC
    ''', (id,)).fetchall()
    
    recommendations = conn.execute('''
        SELECT sr.*, u.username as reviewed_by_name
        FROM schedule_recommendations sr
        LEFT JOIN users u ON sr.reviewed_by = u.id
        WHERE sr.schedule_id = ?
        ORDER BY sr.priority_score DESC, sr.created_at DESC
    ''', (id,)).fetchall()
    
    scenarios = conn.execute('''
        SELECT * FROM schedule_scenarios
        WHERE schedule_id = ?
        ORDER BY created_at DESC
    ''', (id,)).fetchall()
    
    capacity_load = conn.execute('''
        SELECT scl.*, wc.code, wc.name
        FROM schedule_capacity_load scl
        JOIN work_centers wc ON scl.work_center_id = wc.id
        WHERE scl.schedule_id = ?
        ORDER BY wc.code, scl.load_date
    ''', (id,)).fetchall()
    
    conn.close()
    
    return render_template('master_scheduler/schedule_detail.html',
                          schedule=schedule,
                          summary=summary,
                          items=items,
                          exceptions=exceptions,
                          recommendations=recommendations,
                          scenarios=scenarios,
                          at_risk_orders=at_risk_orders,
                          bottlenecks=bottlenecks,
                          capacity_load=capacity_load)


@master_scheduler_bp.route('/master-scheduler/schedules/<int:id>/regenerate', methods=['POST'])
@login_required
@role_required('Admin', 'Planner')
def regenerate_schedule(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        schedule = conn.execute('SELECT * FROM master_schedules WHERE id = ?', (id,)).fetchone()
        
        if not schedule:
            conn.close()
            return jsonify({'success': False, 'error': 'Schedule not found'})
        
        engine = MasterSchedulerEngine(conn)
        result = engine.generate_schedule(id, schedule['horizon_start'], schedule['horizon_end'], session.get('user_id'))
        
        conn.close()
        return jsonify({
            'success': True,
            'data': result
        })
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)})


@master_scheduler_bp.route('/master-scheduler/schedules/<int:id>/activate', methods=['POST'])
@login_required
@role_required('Admin', 'Planner')
def activate_schedule(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('UPDATE master_schedules SET is_active = 0')
        conn.execute('UPDATE master_schedules SET is_active = 1, status = ? WHERE id = ?', ('Active', id))
        conn.commit()
        conn.close()
        
        flash('Schedule activated successfully', 'success')
        return redirect(url_for('master_scheduler_routes.view_schedule', id=id))
    except Exception as e:
        conn.rollback()
        conn.close()
        flash(f'Error activating schedule: {str(e)}', 'danger')
        return redirect(url_for('master_scheduler_routes.view_schedule', id=id))


@master_scheduler_bp.route('/master-scheduler/exceptions/<int:id>/resolve', methods=['POST'])
@login_required
@role_required('Admin', 'Planner')
def resolve_exception(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        resolution_notes = request.form.get('resolution_notes', '')
        
        conn.execute('''
            UPDATE schedule_exceptions
            SET is_resolved = 1, resolved_by = ?, resolved_at = datetime('now'),
                resolution_notes = ?
            WHERE id = ?
        ''', (session.get('user_id'), resolution_notes, id))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True})
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)})


@master_scheduler_bp.route('/master-scheduler/recommendations/<int:id>/decision', methods=['POST'])
@login_required
@role_required('Admin', 'Planner')
def recommendation_decision(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        data = request.get_json()
        decision = data.get('decision', 'Rejected')
        notes = data.get('notes', '')
        
        conn.execute('''
            UPDATE schedule_recommendations
            SET status = ?, decision = ?, decision_notes = ?,
                reviewed_by = ?, reviewed_at = datetime('now')
            WHERE id = ?
        ''', (decision, decision, notes, session.get('user_id'), id))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True})
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)})


@master_scheduler_bp.route('/master-scheduler/schedules/<int:id>/ai-analyze', methods=['POST'])
@login_required
@role_required('Admin', 'Planner')
def ai_analyze_schedule(id):
    """Generate AI-powered analysis and recommendations for the schedule"""
    if not OPENAI_AVAILABLE:
        return jsonify({'success': False, 'error': 'AI integration not available'})
    
    db = Database()
    conn = db.get_connection()
    engine = MasterSchedulerEngine(conn)
    
    try:
        schedule = conn.execute('SELECT * FROM master_schedules WHERE id = ?', (id,)).fetchone()
        if not schedule:
            conn.close()
            return jsonify({'success': False, 'error': 'Schedule not found'})
        
        summary = engine.get_schedule_summary(id)
        at_risk_orders = engine.get_at_risk_orders(id, 15)
        bottlenecks = engine.get_bottleneck_analysis(id)
        
        exceptions = conn.execute('''
            SELECT exception_type, severity, title, description, days_late, capacity_gap
            FROM schedule_exceptions WHERE schedule_id = ? AND is_resolved = 0
        ''', (id,)).fetchall()
        
        context = {
            'schedule_name': schedule['name'],
            'horizon': f"{schedule['horizon_start']} to {schedule['horizon_end']}",
            'status': schedule['status'],
            'otd_rate': summary.get('otd_rate', 0) if summary else 0,
            'total_items': summary['items']['total'] if summary else 0,
            'late_orders': summary.get('late_orders', 0) if summary else 0,
            'critical_exceptions': summary['exceptions']['critical'] if summary else 0,
            'exceptions': [dict(e) for e in exceptions[:10]],
            'at_risk_orders': at_risk_orders[:10],
            'bottlenecks': bottlenecks[:5]
        }
        
        prompt = f"""You are an AI Super Master Scheduler - the single source of truth for all master scheduling decisions.
You operate with finite capacity, real constraints, and execution realism. You do not accept impossible schedules.

Analyze this Master Production Schedule and provide strategic recommendations:

SCHEDULE CONTEXT:
{json.dumps(context, indent=2, default=str)}

Provide your analysis in the following JSON format:
{{
    "executive_summary": "Brief 2-3 sentence overall assessment",
    "health_score": 0-100,
    "critical_issues": [
        {{"issue": "description", "severity": "Critical/Warning", "impact": "business impact"}}
    ],
    "recommendations": [
        {{
            "type": "Resequencing/Overtime/Resource Shift/Expedite/Material/Other",
            "title": "Brief title",
            "description": "Detailed recommendation",
            "action": "Specific action to take",
            "impacted_orders": "List of affected orders or 'Multiple'",
            "cost_impact": estimated dollar impact or null,
            "time_impact_days": number of days saved or null,
            "risk_level": "Low/Medium/High",
            "priority_score": 1-100
        }}
    ],
    "capacity_insights": "Analysis of capacity utilization and bottlenecks",
    "otd_forecast": "Forecast of on-time delivery performance",
    "next_actions": ["Ordered list of immediate actions to take"]
}}

Be specific, quantitative, and actionable. No generic responses."""
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            response_format={"type": "json_object"}
        )
        
        analysis = json.loads(response.choices[0].message.content)
        
        if 'recommendations' in analysis:
            for rec in analysis['recommendations']:
                conn.execute('''
                    INSERT INTO schedule_recommendations (
                        schedule_id, recommendation_type, title, description,
                        action_required, impacted_orders, cost_impact, time_impact_days,
                        risk_level, priority_score, ai_confidence, ai_reasoning, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Pending')
                ''', (
                    id, rec.get('type', 'Other'), rec.get('title', 'AI Recommendation'),
                    rec.get('description', ''), rec.get('action', ''),
                    rec.get('impacted_orders', ''), rec.get('cost_impact'),
                    rec.get('time_impact_days'), rec.get('risk_level', 'Medium'),
                    rec.get('priority_score', 50), 0.85, json.dumps(rec)
                ))
            conn.commit()
        
        conn.close()
        return jsonify({'success': True, 'data': analysis})
        
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)})


@master_scheduler_bp.route('/master-scheduler/schedules/<int:id>/scenarios', methods=['POST'])
@login_required
@role_required('Admin', 'Planner')
def create_scenario(id):
    """Create what-if scenario for schedule comparison"""
    db = Database()
    conn = db.get_connection()
    
    try:
        data = request.get_json()
        scenario_name = data.get('name', 'New Scenario')
        scenario_type = data.get('type', 'Optimization')
        description = data.get('description', '')
        scenario_data = json.dumps(data.get('parameters', {}))
        
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO schedule_scenarios (
                schedule_id, scenario_name, scenario_type, description, scenario_data
            ) VALUES (?, ?, ?, ?, ?)
        ''', (id, scenario_name, scenario_type, description, scenario_data))
        
        scenario_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'scenario_id': scenario_id})
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)})


@master_scheduler_bp.route('/master-scheduler/schedules/<int:id>/scenarios/<int:scenario_id>/analyze', methods=['POST'])
@login_required
@role_required('Admin', 'Planner')
def analyze_scenario(id, scenario_id):
    """AI-powered scenario analysis"""
    if not OPENAI_AVAILABLE:
        return jsonify({'success': False, 'error': 'AI integration not available'})
    
    db = Database()
    conn = db.get_connection()
    
    try:
        scenario = conn.execute('SELECT * FROM schedule_scenarios WHERE id = ?', (scenario_id,)).fetchone()
        schedule = conn.execute('SELECT * FROM master_schedules WHERE id = ?', (id,)).fetchone()
        
        if not scenario or not schedule:
            conn.close()
            return jsonify({'success': False, 'error': 'Scenario or schedule not found'})
        
        engine = MasterSchedulerEngine(conn)
        summary = engine.get_schedule_summary(id)
        
        scenario_params = json.loads(scenario['scenario_data']) if scenario['scenario_data'] else {}
        
        prompt = f"""Analyze this scheduling scenario and project its impact:

CURRENT STATE:
- Schedule: {schedule['name']}
- Current OTD Rate: {summary.get('otd_rate', 0)}%
- Total Orders: {summary['items']['total'] if summary else 0}
- Current Exceptions: {summary['exceptions']['total'] if summary else 0}

SCENARIO: {scenario['scenario_name']}
Type: {scenario['scenario_type']}
Description: {scenario['description']}
Parameters: {json.dumps(scenario_params)}

Provide analysis in JSON format:
{{
    "projected_otd": 0-100,
    "projected_utilization": 0-100,
    "orders_affected": number,
    "overtime_hours": number,
    "cost_delta": dollar amount (positive = cost, negative = savings),
    "risk_score": 0-100,
    "analysis": "Detailed analysis of scenario impact",
    "recommendation": "Accept/Reject/Modify",
    "confidence": 0-100
}}"""
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            response_format={"type": "json_object"}
        )
        
        analysis = json.loads(response.choices[0].message.content)
        
        conn.execute('''
            UPDATE schedule_scenarios
            SET baseline_otd = ?, projected_otd = ?, baseline_utilization = ?,
                projected_utilization = ?, orders_affected = ?, overtime_hours = ?,
                cost_delta = ?, risk_score = ?, ai_analysis = ?
            WHERE id = ?
        ''', (
            summary.get('otd_rate', 0) if summary else 0,
            analysis.get('projected_otd', 0),
            50,
            analysis.get('projected_utilization', 0),
            analysis.get('orders_affected', 0),
            analysis.get('overtime_hours', 0),
            analysis.get('cost_delta', 0),
            analysis.get('risk_score', 50),
            json.dumps(analysis),
            scenario_id
        ))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'data': analysis})
        
    except Exception as e:
        conn.close()
        return jsonify({'success': False, 'error': str(e)})


@master_scheduler_bp.route('/master-scheduler/items/<int:id>/override', methods=['POST'])
@login_required
@role_required('Admin', 'Planner')
def create_override(id):
    """Create a schedule override with justification"""
    db = Database()
    conn = db.get_connection()
    
    try:
        data = request.get_json()
        
        item = conn.execute('SELECT * FROM master_schedule_items WHERE id = ?', (id,)).fetchone()
        if not item:
            conn.close()
            return jsonify({'success': False, 'error': 'Schedule item not found'})
        
        override_type = data.get('override_type', 'Date Change')
        original_value = data.get('original_value', '')
        new_value = data.get('new_value', '')
        justification = data.get('justification', '')
        risk_acknowledged = 1 if data.get('risk_acknowledged', False) else 0
        
        if not justification:
            conn.close()
            return jsonify({'success': False, 'error': 'Justification is required for overrides'})
        
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO schedule_overrides (
                schedule_id, schedule_item_id, override_type, original_value,
                new_value, justification, risk_acknowledged, overridden_by, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'Pending')
        ''', (
            item['schedule_id'], id, override_type, original_value,
            new_value, justification, risk_acknowledged, session.get('user_id')
        ))
        
        override_id = cursor.lastrowid
        
        if override_type == 'Date Change':
            conn.execute('''
                UPDATE master_schedule_items SET scheduled_end = ?, is_locked = 1,
                    lock_reason = ? WHERE id = ?
            ''', (new_value, f'Override #{override_id}: {justification[:50]}', id))
        elif override_type == 'Priority Change':
            conn.execute('''
                UPDATE master_schedule_items SET priority = ?, is_locked = 1,
                    lock_reason = ? WHERE id = ?
            ''', (int(new_value), f'Override #{override_id}: {justification[:50]}', id))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'override_id': override_id})
        
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'success': False, 'error': str(e)})


@master_scheduler_bp.route('/master-scheduler/overrides')
@login_required
@role_required('Admin', 'Planner')
def list_overrides():
    """View all schedule overrides for governance"""
    db = Database()
    conn = db.get_connection()
    
    overrides = conn.execute('''
        SELECT so.*, msi.order_number, msi.product_code, ms.schedule_number,
               u1.username as overridden_by_name, u2.username as approved_by_name
        FROM schedule_overrides so
        JOIN master_schedule_items msi ON so.schedule_item_id = msi.id
        JOIN master_schedules ms ON so.schedule_id = ms.id
        LEFT JOIN users u1 ON so.overridden_by = u1.id
        LEFT JOIN users u2 ON so.approved_by = u2.id
        ORDER BY so.created_at DESC
    ''').fetchall()
    
    conn.close()
    
    return render_template('master_scheduler/overrides.html', overrides=overrides)


@master_scheduler_bp.route('/master-scheduler/api/capacity-chart/<int:schedule_id>')
@login_required
def capacity_chart_data(schedule_id):
    """Get capacity load data for Chart.js visualization"""
    db = Database()
    conn = db.get_connection()
    
    data = conn.execute('''
        SELECT scl.load_date, wc.code, wc.name, scl.available_hours, 
               scl.planned_hours, scl.utilization_pct
        FROM schedule_capacity_load scl
        JOIN work_centers wc ON scl.work_center_id = wc.id
        WHERE scl.schedule_id = ?
        ORDER BY scl.load_date, wc.code
    ''', (schedule_id,)).fetchall()
    
    conn.close()
    
    result = {}
    for row in data:
        wc_code = row['code']
        if wc_code not in result:
            result[wc_code] = {
                'name': row['name'],
                'dates': [],
                'available': [],
                'planned': [],
                'utilization': []
            }
        result[wc_code]['dates'].append(row['load_date'])
        result[wc_code]['available'].append(row['available_hours'])
        result[wc_code]['planned'].append(row['planned_hours'])
        result[wc_code]['utilization'].append(row['utilization_pct'])
    
    return jsonify(result)
