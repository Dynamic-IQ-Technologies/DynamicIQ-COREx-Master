from flask import Blueprint, request, jsonify, flash, redirect, url_for, session, render_template
from models import Database
from auth import login_required, role_required
from datetime import datetime, date
import json

inv_compliance_bp = Blueprint('inv_compliance_routes', __name__)

# ── DB bootstrap ─────────────────────────────────────────────────────────────

def ensure_compliance_tables(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS inv_compliance_profiles (
            id SERIAL PRIMARY KEY,
            inventory_id INTEGER NOT NULL UNIQUE,
            compliance_status TEXT DEFAULT 'Pending',
            risk_score INTEGER DEFAULT 0,
            coc_required BOOLEAN DEFAULT FALSE,
            coc_received BOOLEAN DEFAULT FALSE,
            coc_expiry_date DATE,
            test_report_required BOOLEAN DEFAULT FALSE,
            test_report_received BOOLEAN DEFAULT FALSE,
            material_cert_required BOOLEAN DEFAULT FALSE,
            material_cert_received BOOLEAN DEFAULT FALSE,
            country_origin_doc_required BOOLEAN DEFAULT FALSE,
            country_origin_doc_received BOOLEAN DEFAULT FALSE,
            inspection_record_required BOOLEAN DEFAULT FALSE,
            inspection_record_received BOOLEAN DEFAULT FALSE,
            faa_easa_applicable BOOLEAN DEFAULT FALSE,
            dod_far_dfars_applicable BOOLEAN DEFAULT FALSE,
            itar_ear_applicable BOOLEAN DEFAULT FALSE,
            as9100_applicable BOOLEAN DEFAULT FALSE,
            customer_specific_applicable BOOLEAN DEFAULT FALSE,
            customer_requirement_details TEXT,
            use_blocked BOOLEAN DEFAULT FALSE,
            ship_blocked BOOLEAN DEFAULT FALSE,
            transfer_blocked BOOLEAN DEFAULT FALSE,
            block_reason TEXT,
            compliance_review_date DATE,
            next_review_date DATE,
            supplier_approved BOOLEAN DEFAULT TRUE,
            supplier_approval_date DATE,
            compliance_notes TEXT,
            last_validated_at TIMESTAMP,
            validated_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS compliance_documents (
            id SERIAL PRIMARY KEY,
            inventory_id INTEGER NOT NULL,
            doc_type TEXT NOT NULL,
            doc_number TEXT,
            doc_name TEXT NOT NULL,
            issued_by TEXT,
            issued_date DATE,
            expiry_date DATE,
            is_approved BOOLEAN DEFAULT FALSE,
            approved_by INTEGER,
            approved_at TIMESTAMP,
            notes TEXT,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS compliance_validation_events (
            id SERIAL PRIMARY KEY,
            inventory_id INTEGER NOT NULL,
            triggered_by TEXT,
            triggered_by_user INTEGER,
            status_before TEXT,
            status_after TEXT,
            checks_performed JSONB,
            issues_found TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS compliance_override_logs (
            id SERIAL PRIMARY KEY,
            inventory_id INTEGER NOT NULL,
            override_type TEXT,
            justification TEXT NOT NULL,
            authorized_by INTEGER,
            authorized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            transaction_ref TEXT
        )
    ''')
    conn.commit()


def _get_or_create_profile(conn, inventory_id):
    profile = conn.execute(
        'SELECT * FROM inv_compliance_profiles WHERE inventory_id = %s', (inventory_id,)
    ).fetchone()
    if not profile:
        conn.execute(
            'INSERT INTO inv_compliance_profiles (inventory_id) VALUES (%s)', (inventory_id,)
        )
        conn.commit()
        profile = conn.execute(
            'SELECT * FROM inv_compliance_profiles WHERE inventory_id = %s', (inventory_id,)
        ).fetchone()
    return profile


def _compute_status_and_risk(inv, profile, documents):
    """Re-compute compliance_status and risk_score from current data."""
    issues = []
    risk = 0

    # Check expiration date
    if inv.get('expiration_date'):
        exp = inv['expiration_date']
        if isinstance(exp, str):
            try:
                exp = date.fromisoformat(exp)
            except Exception:
                exp = None
        if exp:
            days_left = (exp - date.today()).days
            if days_left < 0:
                issues.append('Part is expired')
                risk += 40
            elif days_left <= 30:
                issues.append(f'Part expires in {days_left} days')
                risk += 20

    # Documentation completeness
    doc_checks = [
        ('coc_required', 'coc_received', 'Certificate of Conformance'),
        ('test_report_required', 'test_report_received', 'Test Report'),
        ('material_cert_required', 'material_cert_received', 'Material Certification'),
        ('country_origin_doc_required', 'country_origin_doc_received', 'Country of Origin Document'),
        ('inspection_record_required', 'inspection_record_received', 'Inspection Record'),
    ]
    for req_col, recv_col, label in doc_checks:
        if profile.get(req_col) and not profile.get(recv_col):
            issues.append(f'{label} missing')
            risk += 15

    # Supplier approval
    if profile.get('faa_easa_applicable') or profile.get('as9100_applicable'):
        if not profile.get('supplier_approved'):
            issues.append('Supplier not approved')
            risk += 25

    # Blocking flags
    if profile.get('use_blocked') or profile.get('ship_blocked') or profile.get('transfer_blocked'):
        issues.append('Manually blocked by compliance officer')
        risk += 30

    # Check document expiries
    today = date.today()
    for doc in documents:
        if doc.get('expiry_date'):
            exp = doc['expiry_date']
            if isinstance(exp, str):
                try:
                    exp = date.fromisoformat(exp)
                except Exception:
                    continue
            days_left = (exp - today).days
            if days_left < 0:
                issues.append(f'Document "{doc["doc_name"]}" expired')
                risk += 20
            elif days_left <= 30:
                issues.append(f'Document "{doc["doc_name"]}" expires in {days_left} days')
                risk += 10

    risk = min(risk, 100)

    if risk == 0 and not issues:
        status = 'Compliant'
    elif risk <= 30:
        status = 'At Risk'
    elif any('expired' in i.lower() or 'expire' in i.lower() for i in issues):
        status = 'Expired'
    else:
        status = 'Non-Compliant'

    return status, risk, issues


# ── JSON status endpoint (for tab badge / polling) ───────────────────────────

@inv_compliance_bp.route('/inventory/<int:inv_id>/compliance/status')
@login_required
def compliance_status(inv_id):
    db = Database()
    conn = db.get_connection()
    try:
        ensure_compliance_tables(conn)
        profile = _get_or_create_profile(conn, inv_id)
        return jsonify({
            'compliance_status': profile['compliance_status'],
            'risk_score': profile['risk_score'],
            'use_blocked': bool(profile['use_blocked']),
            'ship_blocked': bool(profile['ship_blocked']),
            'transfer_blocked': bool(profile['transfer_blocked']),
        })
    finally:
        conn.close()


# ── Update compliance profile ────────────────────────────────────────────────

@inv_compliance_bp.route('/inventory/<int:inv_id>/compliance/update', methods=['POST'])
@login_required
@role_required('Admin', 'QMS Manager', 'Planner', 'Production Staff', 'Procurement')
def update_compliance(inv_id):
    db = Database()
    conn = db.get_connection()
    try:
        ensure_compliance_tables(conn)
        inv = conn.execute(
            'SELECT i.*, p.code, p.name FROM inventory i JOIN products p ON i.product_id = p.id WHERE i.id = %s',
            (inv_id,)
        ).fetchone()
        if not inv:
            flash('Inventory record not found', 'danger')
            return redirect(url_for('inventory_routes.list_inventory'))

        f = request.form

        def chk(field):
            return field in f and f[field] == 'on'

        def dt(field):
            v = f.get(field, '').strip()
            return v or None

        profile = _get_or_create_profile(conn, inv_id)
        status_before = profile['compliance_status']

        conn.execute('''
            UPDATE inv_compliance_profiles SET
                coc_required=%s, coc_received=%s, coc_expiry_date=%s,
                test_report_required=%s, test_report_received=%s,
                material_cert_required=%s, material_cert_received=%s,
                country_origin_doc_required=%s, country_origin_doc_received=%s,
                inspection_record_required=%s, inspection_record_received=%s,
                faa_easa_applicable=%s, dod_far_dfars_applicable=%s,
                itar_ear_applicable=%s, as9100_applicable=%s,
                customer_specific_applicable=%s, customer_requirement_details=%s,
                use_blocked=%s, ship_blocked=%s, transfer_blocked=%s,
                block_reason=%s, compliance_review_date=%s, next_review_date=%s,
                supplier_approved=%s, supplier_approval_date=%s,
                compliance_notes=%s, updated_at=CURRENT_TIMESTAMP
            WHERE inventory_id=%s
        ''', (
            chk('coc_required'), chk('coc_received'), dt('coc_expiry_date'),
            chk('test_report_required'), chk('test_report_received'),
            chk('material_cert_required'), chk('material_cert_received'),
            chk('country_origin_doc_required'), chk('country_origin_doc_received'),
            chk('inspection_record_required'), chk('inspection_record_received'),
            chk('faa_easa_applicable'), chk('dod_far_dfars_applicable'),
            chk('itar_ear_applicable'), chk('as9100_applicable'),
            chk('customer_specific_applicable'), f.get('customer_requirement_details', '').strip(),
            chk('use_blocked'), chk('ship_blocked'), chk('transfer_blocked'),
            f.get('block_reason', '').strip(), dt('compliance_review_date'), dt('next_review_date'),
            chk('supplier_approved'), dt('supplier_approval_date'),
            f.get('compliance_notes', '').strip(),
            inv_id
        ))
        conn.commit()

        # Re-validate
        profile = conn.execute(
            'SELECT * FROM inv_compliance_profiles WHERE inventory_id = %s', (inv_id,)
        ).fetchone()
        documents = conn.execute(
            'SELECT * FROM compliance_documents WHERE inventory_id = %s', (inv_id,)
        ).fetchall()
        status, risk, issues = _compute_status_and_risk(dict(inv), dict(profile), [dict(d) for d in documents])

        conn.execute('''
            UPDATE inv_compliance_profiles
            SET compliance_status=%s, risk_score=%s, last_validated_at=CURRENT_TIMESTAMP, validated_by=%s
            WHERE inventory_id=%s
        ''', (status, risk, session.get('user_id'), inv_id))

        conn.execute('''
            INSERT INTO compliance_validation_events
            (inventory_id, triggered_by, triggered_by_user, status_before, status_after,
             checks_performed, issues_found)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (inv_id, 'Manual Update', session.get('user_id'), status_before, status,
              json.dumps({'doc_checks': True, 'expiry_check': True, 'supplier_check': True}),
              '; '.join(issues) if issues else 'None'))
        conn.commit()

        flash(f'Compliance profile updated. Status: {status}', 'success' if status == 'Compliant' else 'warning')
    except Exception as e:
        conn.rollback()
        flash(f'Error updating compliance: {str(e)}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('inventory_routes.view_inventory', id=inv_id) + '#compliance-tab')


# ── Add compliance document ──────────────────────────────────────────────────

@inv_compliance_bp.route('/inventory/<int:inv_id>/compliance/documents/add', methods=['POST'])
@login_required
@role_required('Admin', 'QMS Manager', 'Planner', 'Production Staff', 'Procurement')
def add_compliance_document(inv_id):
    db = Database()
    conn = db.get_connection()
    try:
        ensure_compliance_tables(conn)
        f = request.form
        doc_type = f.get('doc_type', '').strip()
        doc_name = f.get('doc_name', '').strip()
        if not doc_type or not doc_name:
            flash('Document type and name are required', 'warning')
            return redirect(url_for('inventory_routes.view_inventory', id=inv_id) + '#compliance-tab')

        def dt(field):
            v = f.get(field, '').strip()
            return v or None

        conn.execute('''
            INSERT INTO compliance_documents
            (inventory_id, doc_type, doc_number, doc_name, issued_by, issued_date,
             expiry_date, notes, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (inv_id, doc_type, f.get('doc_number', '').strip(), doc_name,
              f.get('issued_by', '').strip(), dt('issued_date'), dt('expiry_date'),
              f.get('notes', '').strip(), session.get('user_id')))
        conn.commit()

        # Mark the corresponding received flag
        flag_map = {
            'Certificate of Conformance': 'coc_received',
            'Test Report': 'test_report_received',
            'Material Certification': 'material_cert_received',
            'Country of Origin': 'country_origin_doc_received',
            'Inspection Record': 'inspection_record_received',
        }
        if doc_type in flag_map:
            conn.execute(
                f'UPDATE inv_compliance_profiles SET {flag_map[doc_type]} = TRUE, updated_at = CURRENT_TIMESTAMP WHERE inventory_id = %s',
                (inv_id,)
            )
            conn.commit()

        # Re-validate
        inv = conn.execute(
            'SELECT i.*, p.code, p.name FROM inventory i JOIN products p ON i.product_id = p.id WHERE i.id = %s',
            (inv_id,)
        ).fetchone()
        profile = _get_or_create_profile(conn, inv_id)
        documents = conn.execute('SELECT * FROM compliance_documents WHERE inventory_id = %s', (inv_id,)).fetchall()
        status, risk, _ = _compute_status_and_risk(dict(inv), dict(profile), [dict(d) for d in documents])
        conn.execute(
            'UPDATE inv_compliance_profiles SET compliance_status=%s, risk_score=%s, last_validated_at=CURRENT_TIMESTAMP WHERE inventory_id=%s',
            (status, risk, inv_id)
        )
        conn.commit()
        flash(f'Document added. Compliance re-evaluated: {status}', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error adding document: {str(e)}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('inventory_routes.view_inventory', id=inv_id) + '#compliance-tab')


# ── Approve document ─────────────────────────────────────────────────────────

@inv_compliance_bp.route('/inventory/<int:inv_id>/compliance/documents/<int:doc_id>/approve', methods=['POST'])
@login_required
@role_required('Admin', 'QMS Manager')
def approve_compliance_document(inv_id, doc_id):
    db = Database()
    conn = db.get_connection()
    try:
        ensure_compliance_tables(conn)
        conn.execute('''
            UPDATE compliance_documents
            SET is_approved=TRUE, approved_by=%s, approved_at=CURRENT_TIMESTAMP
            WHERE id=%s AND inventory_id=%s AND is_approved=FALSE
        ''', (session.get('user_id'), doc_id, inv_id))
        conn.commit()
        flash('Document approved and locked', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error: {str(e)}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('inventory_routes.view_inventory', id=inv_id) + '#compliance-tab')


# ── Manual re-validate ───────────────────────────────────────────────────────

@inv_compliance_bp.route('/inventory/<int:inv_id>/compliance/validate', methods=['POST'])
@login_required
def run_compliance_validation(inv_id):
    db = Database()
    conn = db.get_connection()
    try:
        ensure_compliance_tables(conn)
        inv = conn.execute(
            'SELECT i.*, p.code, p.name FROM inventory i JOIN products p ON i.product_id = p.id WHERE i.id = %s',
            (inv_id,)
        ).fetchone()
        profile = _get_or_create_profile(conn, inv_id)
        documents = conn.execute('SELECT * FROM compliance_documents WHERE inventory_id = %s', (inv_id,)).fetchall()
        status_before = profile['compliance_status']
        status, risk, issues = _compute_status_and_risk(dict(inv), dict(profile), [dict(d) for d in documents])

        conn.execute('''
            UPDATE inv_compliance_profiles
            SET compliance_status=%s, risk_score=%s,
                last_validated_at=CURRENT_TIMESTAMP, validated_by=%s
            WHERE inventory_id=%s
        ''', (status, risk, session.get('user_id'), inv_id))
        conn.execute('''
            INSERT INTO compliance_validation_events
            (inventory_id, triggered_by, triggered_by_user, status_before, status_after,
             checks_performed, issues_found)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (inv_id, 'Manual', session.get('user_id'), status_before, status,
              json.dumps({'doc_checks': True, 'expiry_check': True, 'supplier_check': True,
                         'blocking_flags': True, 'document_expiry': True}),
              '; '.join(issues) if issues else 'None'))
        conn.commit()
        flash(f'Validation complete. Status: {status}' + (f' — Issues: {"; ".join(issues)}' if issues else ''),
              'success' if status == 'Compliant' else 'warning')
    except Exception as e:
        conn.rollback()
        flash(f'Validation error: {str(e)}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('inventory_routes.view_inventory', id=inv_id) + '#compliance-tab')


# ── Compliance override log ──────────────────────────────────────────────────

@inv_compliance_bp.route('/inventory/<int:inv_id>/compliance/override', methods=['POST'])
@login_required
@role_required('Admin', 'QMS Manager')
def log_compliance_override(inv_id):
    db = Database()
    conn = db.get_connection()
    try:
        ensure_compliance_tables(conn)
        override_type = request.form.get('override_type', '').strip()
        justification = request.form.get('justification', '').strip()
        transaction_ref = request.form.get('transaction_ref', '').strip()
        if not justification:
            flash('Justification is required for override', 'warning')
            return redirect(url_for('inventory_routes.view_inventory', id=inv_id) + '#compliance-tab')

        conn.execute('''
            INSERT INTO compliance_override_logs
            (inventory_id, override_type, justification, authorized_by, transaction_ref)
            VALUES (%s, %s, %s, %s, %s)
        ''', (inv_id, override_type, justification, session.get('user_id'), transaction_ref))
        conn.commit()
        flash('Override logged with justification', 'warning')
    except Exception as e:
        conn.rollback()
        flash(f'Error: {str(e)}', 'danger')
    finally:
        conn.close()
    return redirect(url_for('inventory_routes.view_inventory', id=inv_id) + '#compliance-tab')


# ── Helper used by issuance to check blocking ────────────────────────────────

def check_inventory_compliance_block(conn, inventory_id):
    """Returns (blocked: bool, reason: str). Called from issuance routes."""
    try:
        ensure_compliance_tables(conn)
        profile = conn.execute(
            'SELECT use_blocked, block_reason, compliance_status FROM inv_compliance_profiles WHERE inventory_id = %s',
            (inventory_id,)
        ).fetchone()
        if profile and profile['use_blocked']:
            reason = profile['block_reason'] or f'Compliance status: {profile["compliance_status"]}'
            return True, reason
        return False, ''
    except Exception:
        return False, ''
