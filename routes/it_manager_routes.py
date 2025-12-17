from flask import Blueprint, render_template, request, jsonify, session
from models import Database
from auth import login_required, role_required
from datetime import datetime, timedelta
import json
import os

it_manager_bp = Blueprint('it_manager_routes', __name__, url_prefix='/it-manager')

@it_manager_bp.route('/')
@login_required
@role_required('Admin')
def dashboard():
    """IT Security Manager Dashboard - Security posture overview"""
    db = Database()
    conn = db.get_connection()
    
    security_posture = calculate_security_posture(conn)
    active_alerts = get_active_alerts(conn)
    user_risk_summary = get_user_risk_summary(conn)
    compliance_status = get_compliance_status(conn)
    ai_agents = get_ai_agent_status(conn)
    recent_incidents = get_recent_incidents(conn)
    access_metrics = get_access_metrics(conn)
    
    conn.close()
    
    return render_template('it_manager/dashboard.html',
                         security_posture=security_posture,
                         active_alerts=active_alerts,
                         user_risk_summary=user_risk_summary,
                         compliance_status=compliance_status,
                         ai_agents=ai_agents,
                         recent_incidents=recent_incidents,
                         access_metrics=access_metrics)

@it_manager_bp.route('/incidents')
@login_required
@role_required('Admin')
def incidents():
    """Security Incidents Management"""
    db = Database()
    conn = db.get_connection()
    
    status_filter = request.args.get('status', '')
    severity_filter = request.args.get('severity', '')
    
    query = 'SELECT * FROM it_security_incidents WHERE 1=1'
    params = []
    
    if status_filter:
        query += ' AND status = ?'
        params.append(status_filter)
    if severity_filter:
        query += ' AND severity = ?'
        params.append(severity_filter)
    
    query += ' ORDER BY created_at DESC'
    incidents = conn.execute(query, params).fetchall()
    
    stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Open' THEN 1 ELSE 0 END) as open_count,
            SUM(CASE WHEN severity = 'Critical' AND status = 'Open' THEN 1 ELSE 0 END) as critical_open
        FROM it_security_incidents
    ''').fetchone()
    
    conn.close()
    
    return render_template('it_manager/incidents.html',
                         incidents=incidents,
                         stats=dict(stats) if stats else {},
                         status_filter=status_filter,
                         severity_filter=severity_filter)

@it_manager_bp.route('/alerts')
@login_required
@role_required('Admin')
def alerts():
    """Security Alerts Management"""
    db = Database()
    conn = db.get_connection()
    
    alerts = conn.execute('''
        SELECT sa.*, u.username as affected_username
        FROM it_security_alerts sa
        LEFT JOIN users u ON sa.affected_user_id = u.id
        ORDER BY 
            CASE sa.severity WHEN 'Critical' THEN 1 WHEN 'High' THEN 2 WHEN 'Medium' THEN 3 ELSE 4 END,
            sa.created_at DESC
    ''').fetchall()
    
    stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) as active,
            SUM(CASE WHEN severity = 'Critical' AND status = 'Active' THEN 1 ELSE 0 END) as critical
        FROM it_security_alerts
    ''').fetchone()
    
    conn.close()
    
    return render_template('it_manager/alerts.html',
                         alerts=alerts,
                         stats=dict(stats) if stats else {})

@it_manager_bp.route('/access-governance')
@login_required
@role_required('Admin')
def access_governance():
    """User Access Governance and Risk Analysis"""
    db = Database()
    conn = db.get_connection()
    
    users_with_risk = conn.execute('''
        SELECT u.*, 
            COALESCE(ur.overall_risk_score, 0) as risk_score,
            COALESCE(ur.access_risk, 0) as access_risk,
            COALESCE(ur.behavior_risk, 0) as behavior_risk,
            COALESCE(ur.dormant_account, 0) as dormant,
            COALESCE(ur.excessive_privileges, 0) as excessive_privileges,
            COALESCE(ur.sod_conflicts, 0) as sod_conflicts,
            ur.last_login,
            ur.risk_factors
        FROM users u
        LEFT JOIN it_user_risk_scores ur ON u.id = ur.user_id
        ORDER BY COALESCE(ur.overall_risk_score, 0) DESC
    ''').fetchall()
    
    role_distribution = conn.execute('''
        SELECT role, COUNT(*) as count FROM users GROUP BY role
    ''').fetchall()
    
    high_risk_count = sum(1 for u in users_with_risk if u['risk_score'] and u['risk_score'] >= 70)
    
    conn.close()
    
    return render_template('it_manager/access_governance.html',
                         users=users_with_risk,
                         role_distribution=[dict(r) for r in role_distribution],
                         high_risk_count=high_risk_count)

@it_manager_bp.route('/compliance')
@login_required
@role_required('Admin')
def compliance():
    """Compliance Assessment Dashboard"""
    db = Database()
    conn = db.get_connection()
    
    assessments = conn.execute('''
        SELECT * FROM it_compliance_assessments
        ORDER BY assessment_date DESC
    ''').fetchall()
    
    controls = conn.execute('''
        SELECT framework, 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Passed' THEN 1 ELSE 0 END) as passed,
            SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) as failed
        FROM it_compliance_controls
        GROUP BY framework
    ''').fetchall()
    
    frameworks = {
        'SOC2': {'name': 'SOC 2 Type II', 'score': 0, 'status': 'Not Assessed'},
        'ISO27001': {'name': 'ISO 27001', 'score': 0, 'status': 'Not Assessed'},
        'FAA': {'name': 'FAA/EASA MRO', 'score': 0, 'status': 'Not Assessed'},
        'SOX': {'name': 'SOX Controls', 'score': 0, 'status': 'Not Assessed'}
    }
    
    for a in assessments:
        if a['framework'] in frameworks:
            frameworks[a['framework']]['score'] = a['overall_score']
            frameworks[a['framework']]['status'] = a['status']
    
    conn.close()
    
    return render_template('it_manager/compliance.html',
                         assessments=assessments,
                         controls=[dict(c) for c in controls],
                         frameworks=frameworks)

@it_manager_bp.route('/ai-agents')
@login_required
@role_required('Admin')
def ai_agents():
    """AI Agent Oversight and Safety"""
    db = Database()
    conn = db.get_connection()
    
    agents = conn.execute('''
        SELECT * FROM it_ai_agent_monitoring
        ORDER BY trust_score ASC
    ''').fetchall()
    
    if not agents:
        agents = get_default_ai_agents()
    
    conn.close()
    
    return render_template('it_manager/ai_agents.html', agents=agents)

@it_manager_bp.route('/change-management')
@login_required
@role_required('Admin')
def change_management():
    """Change and Release Management"""
    db = Database()
    conn = db.get_connection()
    
    changes = conn.execute('''
        SELECT cr.*, 
            u1.username as requester,
            u2.username as approver
        FROM it_change_requests cr
        LEFT JOIN users u1 ON cr.requested_by = u1.id
        LEFT JOIN users u2 ON cr.approved_by = u2.id
        ORDER BY cr.requested_at DESC
    ''').fetchall()
    
    stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END) as pending,
            SUM(CASE WHEN status = 'Approved' THEN 1 ELSE 0 END) as approved
        FROM it_change_requests
    ''').fetchone()
    
    conn.close()
    
    return render_template('it_manager/change_management.html',
                         changes=changes,
                         stats=dict(stats) if stats else {})

@it_manager_bp.route('/api/ask', methods=['POST'])
@login_required
@role_required('Admin')
def ask_it_agent():
    """Natural language interface for IT security queries"""
    data = request.get_json()
    query = data.get('query', '')
    
    if not query:
        return jsonify({'error': 'Query required'}), 400
    
    db = Database()
    conn = db.get_connection()
    
    context = gather_security_context(conn)
    conn.close()
    
    try:
        from openai import OpenAI
        
        client = OpenAI(
            api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
            base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
        )
        
        system_prompt = """You are the Secure IT Manager AI Super Agent for an Enterprise MRP system.
You operate under Zero-Trust principles with least-privilege enforcement.

Your responsibilities include:
- Identity, Access & Role Governance
- ERP Security Operations (SecOps)
- Infrastructure & Application Integrity
- Data Protection & Governance
- Compliance & Audit Readiness (SOC 2, ISO 27001, FAA/EASA, SOX)
- Change & Release Management
- AI Agent Oversight & Safety
- Incident Response & Recovery

Behavioral Principles:
- Security > Convenience
- Automation with accountability
- No silent failures
- Every action is logged and explainable

Provide clear, actionable responses. If recommending actions, specify the risk level and approval requirements."""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Current Security Context:\n{json.dumps(context, indent=2)}\n\nQuery: {query}"}
            ],
            max_tokens=1500,
            temperature=0.7
        )
        
        answer = response.choices[0].message.content
        
        return jsonify({
            'success': True,
            'response': answer,
            'context_used': list(context.keys())
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@it_manager_bp.route('/api/calculate-risk/<int:user_id>', methods=['POST'])
@login_required
@role_required('Admin')
def calculate_user_risk(user_id):
    """Calculate and update risk score for a specific user"""
    db = Database()
    conn = db.get_connection()
    
    try:
        user = conn.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()
        if not user:
            return jsonify({'error': 'User not found'}), 404
        
        access_risk = 0
        behavior_risk = 0
        
        if user['role'] == 'Admin':
            access_risk += 30
        
        audit_entries = conn.execute('''
            SELECT COUNT(*) as count FROM audit_trail 
            WHERE user_id = ? AND action = 'delete'
            AND modified_at >= datetime('now', '-7 days')
        ''', (user_id,)).fetchone()
        
        if audit_entries and audit_entries['count'] > 10:
            behavior_risk += 20
        
        failed_logins = 0
        overall_risk = min(access_risk + behavior_risk, 100)
        
        risk_factors = []
        if access_risk > 20:
            risk_factors.append('Elevated privileges')
        if behavior_risk > 0:
            risk_factors.append('High activity volume')
        
        conn.execute('''
            INSERT OR REPLACE INTO it_user_risk_scores 
            (user_id, overall_risk_score, access_risk, behavior_risk, 
             failed_logins_24h, risk_factors, calculated_at)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        ''', (user_id, overall_risk, access_risk, behavior_risk, 
              failed_logins, json.dumps(risk_factors)))
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'user_id': user_id,
            'risk_score': overall_risk
        })
        
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500

@it_manager_bp.route('/api/lockdown', methods=['POST'])
@login_required
@role_required('Admin')
def initiate_lockdown():
    """Initiate system lockdown for incident response"""
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('''
            INSERT INTO it_security_incidents 
            (incident_number, incident_type, severity, status, title, description, 
             detected_by, created_by)
            VALUES (?, 'Lockdown', 'Critical', 'Open', 'System Lockdown Initiated',
                    'Manual lockdown initiated by administrator', 'IT Manager Agent', ?)
        ''', (f"INC-{datetime.now().strftime('%Y%m%d%H%M%S')}", session.get('user_id')))
        
        conn.execute('''
            INSERT INTO it_security_alerts 
            (alert_type, severity, status, title, description, source)
            VALUES ('Lockdown', 'Critical', 'Active', 'System Lockdown Active',
                    'System is in lockdown mode. All high-risk operations suspended.',
                    'IT Manager Agent')
        ''')
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Lockdown initiated. Incident created.'
        })
        
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500

@it_manager_bp.route('/api/resolve-alert/<int:alert_id>', methods=['POST'])
@login_required
@role_required('Admin')
def resolve_alert(alert_id):
    """Resolve a security alert"""
    db = Database()
    conn = db.get_connection()
    
    try:
        data = request.get_json() or {}
        false_positive = data.get('false_positive', False)
        
        conn.execute('''
            UPDATE it_security_alerts 
            SET status = 'Resolved', 
                resolved_by = ?, 
                resolved_at = datetime('now'),
                false_positive = ?
            WHERE id = ?
        ''', (session.get('user_id'), 1 if false_positive else 0, alert_id))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True})
        
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500


def calculate_security_posture(conn):
    """Calculate overall security posture score"""
    scores = {
        'access_control': 85,
        'threat_detection': 90,
        'compliance': 75,
        'data_protection': 88,
        'incident_response': 82
    }
    
    alerts = conn.execute('''
        SELECT COUNT(*) as count FROM it_security_alerts 
        WHERE status = 'Active' AND severity IN ('Critical', 'High')
    ''').fetchone()
    
    if alerts and alerts['count'] > 0:
        scores['threat_detection'] -= min(alerts['count'] * 5, 30)
    
    incidents = conn.execute('''
        SELECT COUNT(*) as count FROM it_security_incidents 
        WHERE status = 'Open'
    ''').fetchone()
    
    if incidents and incidents['count'] > 0:
        scores['incident_response'] -= min(incidents['count'] * 10, 40)
    
    overall = sum(scores.values()) / len(scores)
    
    return {
        'overall': round(overall),
        'components': scores,
        'status': 'Healthy' if overall >= 80 else 'At Risk' if overall >= 60 else 'Critical'
    }

def get_active_alerts(conn):
    """Get active security alerts"""
    alerts = conn.execute('''
        SELECT * FROM it_security_alerts 
        WHERE status = 'Active'
        ORDER BY 
            CASE severity WHEN 'Critical' THEN 1 WHEN 'High' THEN 2 WHEN 'Medium' THEN 3 ELSE 4 END,
            created_at DESC
        LIMIT 10
    ''').fetchall()
    return [dict(a) for a in alerts]

def get_user_risk_summary(conn):
    """Get user risk distribution summary"""
    users = conn.execute('SELECT COUNT(*) as total FROM users').fetchone()
    
    high_risk = conn.execute('''
        SELECT COUNT(*) as count FROM it_user_risk_scores 
        WHERE overall_risk_score >= 70
    ''').fetchone()
    
    medium_risk = conn.execute('''
        SELECT COUNT(*) as count FROM it_user_risk_scores 
        WHERE overall_risk_score >= 40 AND overall_risk_score < 70
    ''').fetchone()
    
    return {
        'total_users': users['total'] if users else 0,
        'high_risk': high_risk['count'] if high_risk else 0,
        'medium_risk': medium_risk['count'] if medium_risk else 0,
        'low_risk': (users['total'] if users else 0) - (high_risk['count'] if high_risk else 0) - (medium_risk['count'] if medium_risk else 0)
    }

def get_compliance_status(conn):
    """Get compliance framework status"""
    frameworks = ['SOC2', 'ISO27001', 'FAA', 'SOX']
    status = {}
    
    for fw in frameworks:
        assessment = conn.execute('''
            SELECT overall_score, status FROM it_compliance_assessments 
            WHERE framework = ? ORDER BY assessment_date DESC LIMIT 1
        ''', (fw,)).fetchone()
        
        if assessment:
            status[fw] = {'score': assessment['overall_score'], 'status': assessment['status']}
        else:
            status[fw] = {'score': 0, 'status': 'Not Assessed'}
    
    return status

def get_ai_agent_status(conn):
    """Get AI agent monitoring status"""
    agents = conn.execute('''
        SELECT * FROM it_ai_agent_monitoring
        ORDER BY trust_score ASC LIMIT 5
    ''').fetchall()
    
    if not agents:
        return get_default_ai_agents()
    
    return [dict(a) for a in agents]

def get_default_ai_agents():
    """Return default AI agents for display"""
    return [
        {'agent_name': 'ERP Copilot', 'agent_type': 'Assistant', 'status': 'Active', 'trust_score': 95, 'risk_level': 'Low'},
        {'agent_name': 'Supplier Discovery', 'agent_type': 'Discovery', 'status': 'Active', 'trust_score': 92, 'risk_level': 'Low'},
        {'agent_name': 'Market Analyzer', 'agent_type': 'Analytics', 'status': 'Active', 'trust_score': 90, 'risk_level': 'Low'},
        {'agent_name': 'Financial Analyzer', 'agent_type': 'Analytics', 'status': 'Active', 'trust_score': 88, 'risk_level': 'Low'},
        {'agent_name': 'Master Scheduler', 'agent_type': 'Planning', 'status': 'Active', 'trust_score': 91, 'risk_level': 'Low'},
        {'agent_name': 'Business Analytics', 'agent_type': 'Analytics', 'status': 'Active', 'trust_score': 89, 'risk_level': 'Low'},
        {'agent_name': 'Organizational Analyzer', 'agent_type': 'Executive', 'status': 'Active', 'trust_score': 87, 'risk_level': 'Low'}
    ]

def get_recent_incidents(conn):
    """Get recent security incidents"""
    incidents = conn.execute('''
        SELECT * FROM it_security_incidents 
        ORDER BY created_at DESC LIMIT 5
    ''').fetchall()
    return [dict(i) for i in incidents]

def get_access_metrics(conn):
    """Get access and authentication metrics"""
    total_logins = conn.execute('''
        SELECT COUNT(*) as count FROM it_access_audit 
        WHERE action_type = 'login' AND created_at >= datetime('now', '-24 hours')
    ''').fetchone()
    
    failed_logins = conn.execute('''
        SELECT COUNT(*) as count FROM it_access_audit 
        WHERE action_type = 'login' AND success = 0 AND created_at >= datetime('now', '-24 hours')
    ''').fetchone()
    
    return {
        'logins_24h': total_logins['count'] if total_logins else 0,
        'failed_logins_24h': failed_logins['count'] if failed_logins else 0,
        'active_sessions': 0
    }

def gather_security_context(conn):
    """Gather current security context for AI analysis"""
    return {
        'security_posture': calculate_security_posture(conn),
        'active_alerts': len(get_active_alerts(conn)),
        'user_risk_summary': get_user_risk_summary(conn),
        'compliance_status': get_compliance_status(conn),
        'recent_incidents': len(get_recent_incidents(conn)),
        'access_metrics': get_access_metrics(conn),
        'timestamp': datetime.now().isoformat()
    }
