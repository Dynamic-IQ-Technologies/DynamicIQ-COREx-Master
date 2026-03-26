"""
QuickBooks Sync Integration — Dynamic.IQ-COREx
Supports QuickBooks Online (OAuth 2.0 REST API) and QuickBooks Desktop (polling mode).
"""
from flask import Blueprint, jsonify, request, session, redirect, url_for, flash, render_template
from models import Database
from auth import login_required
from datetime import datetime, timedelta
import os, json, logging, requests as http

log = logging.getLogger(__name__)
qb_sync_bp = Blueprint('qb_sync_routes', __name__)

# ── QB Online constants ───────────────────────────────────────────────────────
QB_OAUTH_URL       = 'https://appcenter.intuit.com/connect/oauth2'
QB_TOKEN_URL       = 'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer'
QB_REVOKE_URL      = 'https://developer.api.intuit.com/v2/oauth2/tokens/revoke'
QB_SANDBOX_BASE    = 'https://sandbox-quickbooks.api.intuit.com'
QB_PRODUCTION_BASE = 'https://quickbooks.api.intuit.com'
QB_SCOPE           = 'com.intuit.quickbooks.accounting'
QB_API_VERSION     = 'v3'

CONFLICT_RULES = ('erp_wins', 'qb_wins', 'manual_review')


def _auto_redirect_uri(req):
    """Build the correct public-facing callback URL regardless of environment."""
    domain = os.environ.get('REPLIT_DOMAINS') or os.environ.get('REPLIT_DEV_DOMAIN')
    if domain:
        return f'https://{domain}/qb/callback'
    # Fall back to Flask's host_url (works when behind a proper reverse proxy)
    return req.host_url.rstrip('/') + '/qb/callback'


# ─── Lazy Table Creation ──────────────────────────────────────────────────────

def ensure_qb_tables(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS qb_sync_config (
            id               SERIAL PRIMARY KEY,
            tenant_id        TEXT DEFAULT 'default',
            qb_mode          TEXT DEFAULT 'online',
            sandbox_mode     BOOLEAN DEFAULT TRUE,
            realm_id         TEXT,
            client_id        TEXT,
            client_secret    TEXT,
            redirect_uri     TEXT,
            access_token     TEXT,
            refresh_token    TEXT,
            token_expiry     TIMESTAMP,
            connected_at     TIMESTAMP,
            connected_by     TEXT,
            conflict_rule    TEXT DEFAULT 'manual_review',
            auto_sync_wo     BOOLEAN DEFAULT FALSE,
            auto_sync_inv    BOOLEAN DEFAULT FALSE,
            auto_sync_pay    BOOLEAN DEFAULT FALSE,
            webhook_secret   TEXT,
            desktop_poll_url TEXT,
            is_active        BOOLEAN DEFAULT FALSE,
            updated_at       TIMESTAMP DEFAULT NOW()
        )
    ''')
    for col, definition in [
        ('client_id',     'TEXT'),
        ('client_secret', 'TEXT'),
        ('redirect_uri',  'TEXT'),
    ]:
        try:
            conn.execute(f'ALTER TABLE qb_sync_config ADD COLUMN IF NOT EXISTS {col} {definition}')
            conn.commit()
        except Exception:
            conn.rollback()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS qb_sync_event_log (
            id            SERIAL PRIMARY KEY,
            entity_type   TEXT NOT NULL,
            entity_id     INTEGER,
            event_type    TEXT NOT NULL,
            direction     TEXT DEFAULT 'erp_to_qb',
            qb_entity_id  TEXT,
            qb_doc_number TEXT,
            status        TEXT DEFAULT 'pending',
            error_message TEXT,
            payload_sent  TEXT,
            payload_recv  TEXT,
            synced_by     TEXT,
            retry_count   INTEGER DEFAULT 0,
            created_at    TIMESTAMP DEFAULT NOW()
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS qb_wo_invoice_map (
            id               SERIAL PRIMARY KEY,
            wo_id            INTEGER,
            invoice_id       INTEGER,
            qb_invoice_id    TEXT,
            qb_invoice_number TEXT,
            qb_txn_date      DATE,
            qb_total_amount  NUMERIC(12,2),
            last_synced_at   TIMESTAMP,
            sync_status      TEXT DEFAULT 'pending',
            created_at       TIMESTAMP DEFAULT NOW()
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS qb_payment_sync (
            id               SERIAL PRIMARY KEY,
            qb_payment_id    TEXT,
            qb_invoice_id    TEXT,
            erp_invoice_id   INTEGER,
            erp_wo_id        INTEGER,
            amount           NUMERIC(12,2),
            payment_method   TEXT,
            payment_date     DATE,
            memo             TEXT,
            sync_status      TEXT DEFAULT 'pending',
            applied_to_erp   BOOLEAN DEFAULT FALSE,
            applied_at       TIMESTAMP,
            created_at       TIMESTAMP DEFAULT NOW()
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS qb_conflict_log (
            id               SERIAL PRIMARY KEY,
            entity_type      TEXT,
            entity_id        INTEGER,
            qb_entity_id     TEXT,
            conflict_type    TEXT,
            erp_value        TEXT,
            qb_value         TEXT,
            resolution       TEXT DEFAULT 'pending',
            resolved_by      TEXT,
            resolved_at      TIMESTAMP,
            resolution_notes TEXT,
            created_at       TIMESTAMP DEFAULT NOW()
        )
    ''')
    conn.commit()


# ─── Token & Config Helpers ───────────────────────────────────────────────────

def _get_config(conn):
    ensure_qb_tables(conn)
    return conn.execute(
        "SELECT * FROM qb_sync_config WHERE tenant_id='default' ORDER BY id DESC LIMIT 1"
    ).fetchone()


def _qb_base(config):
    if config and config['sandbox_mode']:
        return QB_SANDBOX_BASE
    return QB_PRODUCTION_BASE


def _api_url(config, path):
    return f"{_qb_base(config)}/{QB_API_VERSION}/company/{config['realm_id']}/{path}"


def _token_expired(config):
    if not config or not config['token_expiry']:
        return True
    expiry = config['token_expiry']
    if isinstance(expiry, str):
        expiry = datetime.fromisoformat(expiry)
    return datetime.now() >= expiry - timedelta(minutes=5)


def _get_credentials(config):
    """Return (client_id, client_secret) — DB values take priority over env vars."""
    client_id     = (config.get('client_id') if config else None) or os.environ.get('QB_CLIENT_ID', '')
    client_secret = (config.get('client_secret') if config else None) or os.environ.get('QB_CLIENT_SECRET', '')
    return client_id, client_secret


def _credentials_configured(config):
    cid, csec = _get_credentials(config)
    return bool(cid and csec)


class QBAuthExpiredError(Exception):
    """Raised when the QB access token cannot be refreshed and the session must be reconnected."""


def _refresh_token(conn, config):
    client_id, client_secret = _get_credentials(config)
    if not client_id or not client_secret or not config.get('refresh_token'):
        return None
    try:
        resp = http.post(
            QB_TOKEN_URL,
            auth=(client_id, client_secret),
            data={'grant_type': 'refresh_token', 'refresh_token': config['refresh_token']},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            expiry = datetime.now() + timedelta(seconds=data.get('expires_in', 3600))
            conn.execute('''
                UPDATE qb_sync_config
                SET access_token=%s, refresh_token=%s, token_expiry=%s, updated_at=NOW()
                WHERE tenant_id='default'
            ''', (data['access_token'], data.get('refresh_token', config['refresh_token']),
                  expiry.isoformat()))
            conn.commit()
            log.info('QB token refreshed successfully.')
            return data['access_token']
        else:
            log.error(f'QB token refresh HTTP {resp.status_code}: {resp.text[:200]}')
    except Exception as ex:
        log.error(f'QB token refresh failed: {ex}')
    return None


def _qb_headers(conn, config):
    token = config['access_token']
    if _token_expired(config):
        new_token = _refresh_token(conn, config)
        if new_token:
            token = new_token
        else:
            raise QBAuthExpiredError(
                'QuickBooks session has expired. Please reconnect QuickBooks from the QB Sync dashboard.'
            )
    return {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    }


def _qb_request(conn, config, method, url, **kwargs):
    """Make a QB API request, automatically refreshing the token once on 401."""
    headers = _qb_headers(conn, config)
    resp    = http.request(method, url, headers=headers, **kwargs)
    if resp.status_code == 401:
        log.warning('QB 401 received — attempting token refresh and retry.')
        new_token = _refresh_token(conn, config)
        if not new_token:
            raise QBAuthExpiredError(
                'QuickBooks session has expired. Please reconnect QuickBooks from the QB Sync dashboard.'
            )
        headers['Authorization'] = f'Bearer {new_token}'
        resp = http.request(method, url, headers=headers, **kwargs)
    return resp


def _log_event(conn, entity_type, entity_id, event_type, direction, status,
               qb_entity_id=None, qb_doc_number=None, error=None,
               payload_sent=None, payload_recv=None):
    try:
        conn.execute('''
            INSERT INTO qb_sync_event_log
                (entity_type, entity_id, event_type, direction, qb_entity_id,
                 qb_doc_number, status, error_message, payload_sent, payload_recv, synced_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (entity_type, entity_id, event_type, direction,
              str(qb_entity_id) if qb_entity_id else None,
              qb_doc_number, status, error,
              json.dumps(payload_sent)[:4000] if payload_sent else None,
              json.dumps(payload_recv)[:4000] if payload_recv else None,
              session.get('username', 'system')))
        conn.commit()
    except Exception as ex:
        log.warning(f'QB log_event failed: {ex}')


def _log_conflict(conn, entity_type, entity_id, qb_entity_id,
                  conflict_type, erp_val, qb_val):
    try:
        conn.execute('''
            INSERT INTO qb_conflict_log
                (entity_type, entity_id, qb_entity_id, conflict_type, erp_value, qb_value)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (entity_type, entity_id, str(qb_entity_id), conflict_type,
              str(erp_val)[:1000], str(qb_val)[:1000]))
        conn.commit()
    except Exception as ex:
        log.warning(f'QB log_conflict failed: {ex}')


# ─── Core Sync Functions ──────────────────────────────────────────────────────

def _qb_find_or_create_customer(conn, config, wo):
    """Find or create a QB Customer matching ERP customer, return QB Id."""
    cust_name = (wo.get('customer_full_name') or wo.get('customer_name') or 'Unknown Customer').replace("'", "\\'")

    query_url = _api_url(config, f"query?query=SELECT Id,DisplayName FROM Customer WHERE DisplayName LIKE '{cust_name[:40]}' MAXRESULTS 5&minorversion=65")
    resp = _qb_request(conn, config, 'GET', query_url, timeout=10)
    if resp.status_code == 200:
        items = resp.json().get('QueryResponse', {}).get('Customer', [])
        if items:
            return items[0]['Id']

    payload = {'DisplayName': cust_name[:100], 'CompanyName': cust_name[:100]}
    resp = _qb_request(conn, config, 'POST', _api_url(config, 'customer?minorversion=65'),
                       json=payload, timeout=10)
    if resp.status_code in (200, 201):
        return resp.json().get('Customer', {}).get('Id')
    return None


def _build_invoice_payload(wo, qb_customer_id, existing_sync=None):
    line_items = []
    line_num = 1

    labor  = float(wo.get('labor_cost') or 0)
    oh     = float(wo.get('overhead_cost') or 0)
    amount = float(wo.get('so_value') or labor + oh or 0)

    if labor > 0:
        line_items.append({
            'LineNum': line_num,
            'Description': f'Labour — WO {wo["wo_number"]}',
            'Amount': round(labor, 2),
            'DetailType': 'SalesItemLineDetail',
            'SalesItemLineDetail': {'Qty': 1, 'UnitPrice': round(labor, 2)},
        })
        line_num += 1

    if oh > 0:
        line_items.append({
            'LineNum': line_num,
            'Description': f'Overhead — WO {wo["wo_number"]}',
            'Amount': round(oh, 2),
            'DetailType': 'SalesItemLineDetail',
            'SalesItemLineDetail': {'Qty': 1, 'UnitPrice': round(oh, 2)},
        })
        line_num += 1

    if not line_items:
        line_items.append({
            'LineNum': 1,
            'Description': f'Work Order {wo["wo_number"]} — {wo.get("product_name", "")}',
            'Amount': max(amount, 0.01),
            'DetailType': 'SalesItemLineDetail',
            'SalesItemLineDetail': {'Qty': 1, 'UnitPrice': max(amount, 0.01)},
        })

    payload = {
        'DocNumber':    wo['wo_number'],
        'TxnDate':      str(wo.get('planned_start_date') or datetime.now().date()),
        'CustomerRef':  {'value': qb_customer_id},
        'Line':         line_items,
        'PrivateNote':  f'ERP Work Order {wo["wo_number"]} — {wo.get("product_name", "")}',
    }
    if wo.get('planned_end_date'):
        payload['DueDate'] = str(wo['planned_end_date'])

    if existing_sync and existing_sync.get('qb_invoice_id'):
        payload['Id']        = existing_sync['qb_invoice_id']
        payload['SyncToken'] = existing_sync.get('sync_token', '0')

    return payload


def sync_wo_to_qb(conn, wo_id, trigger='manual', max_retries=3):
    """Sync a single work order to QB as an invoice. Returns dict with status."""
    config = _get_config(conn)
    if not config or not config['is_active']:
        return {'status': 'skipped', 'reason': 'QB integration not configured or inactive'}

    wo = conn.execute('''
        SELECT wo.*, p.name as product_name, p.code as product_code,
               c.name as customer_full_name,
               so.total_amount as so_value
        FROM work_orders wo
        JOIN products p ON p.id = wo.product_id
        LEFT JOIN customers c ON c.id = wo.customer_id
        LEFT JOIN sales_orders so ON so.id = wo.so_id
        WHERE wo.id = %s
    ''', (wo_id,)).fetchone()

    if not wo:
        return {'status': 'error', 'reason': 'Work order not found'}

    existing = conn.execute(
        'SELECT * FROM qb_wo_invoice_map WHERE wo_id = %s ORDER BY id DESC LIMIT 1', (wo_id,)
    ).fetchone()

    attempt = 0
    last_error = None
    while attempt < max_retries:
        attempt += 1
        try:
            qb_cust_id = _qb_find_or_create_customer(conn, config, wo)
            if not qb_cust_id:
                raise ValueError('Could not find or create QB customer')

            # Check for existing QB invoice by DocNumber to detect conflicts
            if existing and existing.get('qb_invoice_id'):
                check_url  = _api_url(config, f"invoice/{existing['qb_invoice_id']}?minorversion=65")
                check_resp = _qb_request(conn, config, 'GET', check_url, timeout=10)
                if check_resp.status_code == 200:
                    qb_inv = check_resp.json().get('Invoice', {})
                    qb_total = float(qb_inv.get('TotalAmt', 0))
                    erp_total = float(wo.get('so_value') or
                                      (float(wo.get('labor_cost') or 0) + float(wo.get('overhead_cost') or 0)))
                    if abs(qb_total - erp_total) > 0.5:
                        conflict_rule = config.get('conflict_rule', 'manual_review')
                        _log_conflict(conn, 'work_order', wo_id, existing['qb_invoice_id'],
                                      'amount_mismatch',
                                      f'ERP total: ${erp_total:,.2f}',
                                      f'QB total: ${qb_total:,.2f}')
                        if conflict_rule == 'manual_review':
                            _log_event(conn, 'work_order', wo_id, 'sync_conflict', 'erp_to_qb',
                                       'conflict', existing['qb_invoice_id'])
                            return {'status': 'conflict', 'reason': 'Amount mismatch — requires manual review'}
                        elif conflict_rule == 'qb_wins':
                            _log_event(conn, 'work_order', wo_id, 'sync_skipped', 'erp_to_qb',
                                       'skipped', existing['qb_invoice_id'])
                            return {'status': 'skipped', 'reason': 'QB wins — ERP not overwritten'}
                    existing_sync = dict(existing)
                    existing_sync['sync_token'] = qb_inv.get('SyncToken', '0')
                else:
                    existing_sync = None
            else:
                existing_sync = None

            payload = _build_invoice_payload(wo, qb_cust_id, existing_sync)
            method  = 'POST'
            url     = _api_url(config, 'invoice?minorversion=65')
            if existing_sync:
                method = 'POST'
                url    = _api_url(config, 'invoice?operation=update&minorversion=65')

            resp = _qb_request(conn, config, method, url, json=payload, timeout=15)

            if resp.status_code in (200, 201):
                inv_data     = resp.json().get('Invoice', {})
                qb_inv_id    = inv_data.get('Id')
                qb_doc_num   = inv_data.get('DocNumber')
                qb_total     = float(inv_data.get('TotalAmt', 0))
                qb_txn_date  = inv_data.get('TxnDate')

                conn.execute('''
                    INSERT INTO qb_wo_invoice_map
                        (wo_id, qb_invoice_id, qb_invoice_number, qb_txn_date, qb_total_amount, last_synced_at, sync_status)
                    VALUES (%s, %s, %s, %s, %s, NOW(), 'synced')
                    ON CONFLICT DO NOTHING
                ''', (wo_id, qb_inv_id, qb_doc_num, qb_txn_date, qb_total))

                if existing:
                    conn.execute('''
                        UPDATE qb_wo_invoice_map
                        SET qb_invoice_id=%s, qb_invoice_number=%s, qb_txn_date=%s,
                            qb_total_amount=%s, last_synced_at=NOW(), sync_status='synced'
                        WHERE wo_id=%s
                    ''', (qb_inv_id, qb_doc_num, qb_txn_date, qb_total, wo_id))

                conn.commit()
                _log_event(conn, 'work_order', wo_id,
                           'updated' if existing_sync else 'created',
                           'erp_to_qb', 'success',
                           qb_entity_id=qb_inv_id, qb_doc_number=qb_doc_num,
                           payload_sent=payload, payload_recv=inv_data)
                return {
                    'status': 'success',
                    'qb_invoice_id': qb_inv_id,
                    'qb_doc_number': qb_doc_num,
                    'qb_total': qb_total,
                }
            else:
                last_error = f'QB API {resp.status_code}: {resp.text[:300]}'
                log.warning(f'QB sync WO {wo_id} attempt {attempt}: {last_error}')

        except QBAuthExpiredError as ex:
            _log_event(conn, 'work_order', wo_id, 'sync_failed', 'erp_to_qb', 'failed', error=str(ex))
            return {'status': 'auth_expired', 'reason': str(ex)}
        except Exception as ex:
            last_error = str(ex)
            log.warning(f'QB sync WO {wo_id} attempt {attempt} exception: {ex}')

    _log_event(conn, 'work_order', wo_id, 'sync_failed', 'erp_to_qb', 'failed',
               error=last_error)
    return {'status': 'failed', 'reason': last_error}


def sync_invoice_to_qb(conn, invoice_id, trigger='manual'):
    """Sync an ERP invoice to QB. Updates the existing QB invoice if already synced,
    otherwise creates a new one. Prevents duplicates on re-sync."""
    config = _get_config(conn)
    if not config or not config['is_active']:
        return {'status': 'skipped', 'reason': 'QB integration not configured'}

    inv = conn.execute('''
        SELECT i.*, c.name as customer_full_name
        FROM invoices i
        LEFT JOIN customers c ON c.id = i.customer_id
        WHERE i.id = %s
    ''', (invoice_id,)).fetchone()
    if not inv:
        return {'status': 'error', 'reason': 'Invoice not found'}

    try:
        # ── 1. Resolve or create the QB Customer ──────────────────────────────
        cust_name  = (inv.get('customer_full_name') or 'Unknown Customer').replace("'", "\\'")
        query_url  = _api_url(config, f"query?query=SELECT Id FROM Customer WHERE DisplayName LIKE '{cust_name[:40]}' MAXRESULTS 1&minorversion=65")
        qb_cust_id = None
        resp = _qb_request(conn, config, 'GET', query_url, timeout=10)
        if resp.status_code == 200:
            items = resp.json().get('QueryResponse', {}).get('Customer', [])
            if items:
                qb_cust_id = items[0]['Id']
        if not qb_cust_id:
            cr = _qb_request(conn, config, 'POST', _api_url(config, 'customer?minorversion=65'),
                              json={'DisplayName': cust_name[:100]}, timeout=10)
            if cr.status_code in (200, 201):
                qb_cust_id = cr.json().get('Customer', {}).get('Id')

        total = float(inv.get('total_amount') or 0)
        payload = {
            'DocNumber':   inv.get('invoice_number', f'INV-{invoice_id}'),
            'TxnDate':     str(inv.get('invoice_date') or datetime.now().date()),
            'CustomerRef': {'value': qb_cust_id},
            'Line': [{
                'Amount': total,
                'DetailType': 'SalesItemLineDetail',
                'Description': f'Invoice {inv.get("invoice_number")} — {inv.get("invoice_type", "Service")}',
                'SalesItemLineDetail': {'Qty': 1, 'UnitPrice': total},
            }],
            'DueDate': str(inv.get('due_date') or (datetime.now() + timedelta(days=30)).date()),
        }

        # ── 2. Check if this invoice was previously synced to QB ──────────────
        existing_map = conn.execute('''
            SELECT qb_invoice_id FROM qb_wo_invoice_map
            WHERE invoice_id = %s AND qb_invoice_id IS NOT NULL
            ORDER BY last_synced_at DESC LIMIT 1
        ''', (invoice_id,)).fetchone()

        if existing_map:
            # ── UPDATE path: fetch current SyncToken then sparse-update ──────
            qb_inv_id = existing_map['qb_invoice_id']
            fetch_resp = _qb_request(conn, config, 'GET',
                                     _api_url(config, f'invoice/{qb_inv_id}?minorversion=65'),
                                     timeout=10)
            if fetch_resp.status_code != 200:
                err = f'Failed to fetch QB invoice for update: {fetch_resp.status_code} {fetch_resp.text[:200]}'
                _log_event(conn, 'invoice', invoice_id, 'sync_failed', 'erp_to_qb', 'failed', error=err)
                return {'status': 'failed', 'reason': err}

            qb_existing = fetch_resp.json().get('Invoice', {})
            sync_token  = qb_existing.get('SyncToken')

            payload['Id']        = qb_inv_id
            payload['SyncToken'] = sync_token
            payload['sparse']    = True

            resp = _qb_request(conn, config, 'POST',
                               _api_url(config, 'invoice?minorversion=65'),
                               json=payload, timeout=15)
            if resp.status_code in (200, 201):
                inv_data = resp.json().get('Invoice', {})
                conn.execute('''
                    UPDATE qb_wo_invoice_map
                    SET qb_total_amount = %s, last_synced_at = NOW(), sync_status = 'synced'
                    WHERE invoice_id = %s AND qb_invoice_id = %s
                ''', (total, invoice_id, qb_inv_id))
                conn.commit()
                _log_event(conn, 'invoice', invoice_id, 'updated', 'erp_to_qb', 'success',
                           qb_entity_id=qb_inv_id, payload_sent=payload, payload_recv=inv_data)
                return {'status': 'success', 'qb_invoice_id': qb_inv_id, 'action': 'updated'}
            else:
                err = f'QB {resp.status_code}: {resp.text[:300]}'
                _log_event(conn, 'invoice', invoice_id, 'sync_failed', 'erp_to_qb', 'failed', error=err)
                return {'status': 'failed', 'reason': err}

        else:
            # ── CREATE path: new invoice, no prior QB mapping ─────────────────
            resp = _qb_request(conn, config, 'POST',
                               _api_url(config, 'invoice?minorversion=65'),
                               json=payload, timeout=15)
            if resp.status_code in (200, 201):
                inv_data  = resp.json().get('Invoice', {})
                qb_inv_id = inv_data.get('Id')
                conn.execute('''
                    INSERT INTO qb_wo_invoice_map
                        (invoice_id, qb_invoice_id, qb_invoice_number, qb_total_amount, last_synced_at, sync_status)
                    VALUES (%s, %s, %s, %s, NOW(), 'synced')
                ''', (invoice_id, qb_inv_id, inv_data.get('DocNumber'), total))
                conn.commit()
                _log_event(conn, 'invoice', invoice_id, 'created', 'erp_to_qb', 'success',
                           qb_entity_id=qb_inv_id, payload_sent=payload, payload_recv=inv_data)
                return {'status': 'success', 'qb_invoice_id': qb_inv_id, 'action': 'created'}
            else:
                err = f'QB {resp.status_code}: {resp.text[:300]}'
                _log_event(conn, 'invoice', invoice_id, 'sync_failed', 'erp_to_qb', 'failed', error=err)
                return {'status': 'failed', 'reason': err}

    except QBAuthExpiredError as ex:
        _log_event(conn, 'invoice', invoice_id, 'sync_failed', 'erp_to_qb', 'failed', error=str(ex))
        return {'status': 'auth_expired', 'reason': str(ex)}
    except Exception as ex:
        err = str(ex)
        _log_event(conn, 'invoice', invoice_id, 'sync_failed', 'erp_to_qb', 'failed', error=err)
        return {'status': 'failed', 'reason': err}


def pull_invoice_from_qb(conn, erp_invoice_id, qb_invoice_id):
    """Fetch a single QB invoice and update ERP invoice status, amount_paid, balance_due."""
    config = _get_config(conn)
    if not config or not config['is_active']:
        return {'status': 'skipped', 'reason': 'QB not connected'}
    try:
        url  = _api_url(config, f"invoice/{qb_invoice_id}?minorversion=65")
        resp = _qb_request(conn, config, 'GET', url, timeout=10)
        if resp.status_code != 200:
            return {'status': 'failed', 'reason': f'QB API {resp.status_code}: {resp.text[:200]}'}

        qb_inv      = resp.json().get('Invoice', {})
        total_amt   = float(qb_inv.get('TotalAmt', 0))
        balance     = float(qb_inv.get('Balance', 0))
        amount_paid = round(total_amt - balance, 2)
        qb_status   = qb_inv.get('EmailStatus') or ''

        if balance <= 0:
            new_status = 'Paid'
        elif amount_paid > 0:
            new_status = 'Partially Paid'
        else:
            new_status = None

        update_parts = ['amount_paid=%s', 'balance_due=%s']
        params       = [amount_paid, balance]
        if new_status:
            update_parts.append('status=%s')
            params.append(new_status)
        params.append(erp_invoice_id)

        conn.execute(
            f"UPDATE invoices SET {', '.join(update_parts)} WHERE id=%s",
            params,
        )
        conn.execute('''
            UPDATE qb_wo_invoice_map
            SET last_synced_at=NOW(), sync_status='synced', qb_total_amount=%s
            WHERE invoice_id=%s AND qb_invoice_id=%s
        ''', (total_amt, erp_invoice_id, qb_invoice_id))
        conn.commit()

        _log_event(conn, 'invoice', erp_invoice_id, 'pulled_from_qb', 'qb_to_erp', 'success',
                   qb_entity_id=qb_invoice_id, payload_recv=qb_inv)
        return {
            'status':      'success',
            'total_amt':   total_amt,
            'balance':     balance,
            'amount_paid': amount_paid,
            'new_status':  new_status,
        }
    except QBAuthExpiredError as ex:
        conn.rollback()
        return {'status': 'auth_expired', 'reason': str(ex)}
    except Exception as ex:
        conn.rollback()
        log.error(f'pull_invoice_from_qb error: {ex}')
        _log_event(conn, 'invoice', erp_invoice_id, 'pull_failed', 'qb_to_erp', 'failed',
                   qb_entity_id=qb_invoice_id, error=str(ex))
        return {'status': 'failed', 'reason': str(ex)}


def pull_all_synced_invoices(conn):
    """Pull updates from QB for every invoice that has been synced. Returns summary dict."""
    config = _get_config(conn)
    if not config or not config['is_active']:
        return {'pulled': 0, 'failed': 0, 'skipped': 1}

    mappings = conn.execute('''
        SELECT invoice_id, qb_invoice_id
        FROM qb_wo_invoice_map
        WHERE invoice_id IS NOT NULL AND qb_invoice_id IS NOT NULL
        ORDER BY last_synced_at DESC
        LIMIT 200
    ''').fetchall()

    results = {'pulled': 0, 'failed': 0, 'skipped': 0, 'auth_expired': False}
    for m in mappings:
        r = pull_invoice_from_qb(conn, m['invoice_id'], m['qb_invoice_id'])
        if r['status'] == 'success':
            results['pulled'] += 1
        elif r['status'] == 'skipped':
            results['skipped'] += 1
        elif r['status'] == 'auth_expired':
            results['auth_expired'] = True
            results['auth_reason']  = r.get('reason', 'QB session expired')
            break
        else:
            results['failed'] += 1

    try:
        conn.execute(
            "UPDATE qb_sync_config SET updated_at=NOW() WHERE tenant_id='default'"
        )
        conn.commit()
    except Exception:
        conn.rollback()

    log.info(f'pull_all_synced_invoices: {results}')
    return results


def pull_qb_payments(conn, days_back=7):
    """Pull recent QB payments and update ERP invoice records."""
    config = _get_config(conn)
    if not config or not config['is_active']:
        return []

    try:
        since = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        url   = _api_url(config, f"query?query=SELECT * FROM Payment WHERE TxnDate >= '{since}' MAXRESULTS 100&minorversion=65")
        resp  = _qb_request(conn, config, 'GET', url, timeout=15)
        if resp.status_code != 200:
            return []

        payments = resp.json().get('QueryResponse', {}).get('Payment', [])
        synced   = []
        for p in payments:
            qb_pay_id = p.get('Id')
            existing  = conn.execute(
                'SELECT id FROM qb_payment_sync WHERE qb_payment_id=%s', (qb_pay_id,)
            ).fetchone()
            if existing:
                continue

            for line in p.get('Line', []):
                for linked in line.get('LinkedTxn', []):
                    if linked.get('TxnType') == 'Invoice':
                        qb_inv_id = linked['TxnId']
                        mapping   = conn.execute(
                            "SELECT * FROM qb_wo_invoice_map WHERE qb_invoice_id=%s LIMIT 1",
                            (qb_inv_id,)
                        ).fetchone()
                        erp_inv_id = mapping['invoice_id'] if mapping else None
                        erp_wo_id  = mapping['wo_id'] if mapping else None
                        amount     = float(p.get('TotalAmt', 0))
                        pay_date   = p.get('TxnDate')
                        memo       = p.get('PrivateNote') or ''
                        method     = (p.get('PaymentMethodRef') or {}).get('name', 'Unknown')

                        conn.execute('''
                            INSERT INTO qb_payment_sync
                                (qb_payment_id, qb_invoice_id, erp_invoice_id, erp_wo_id,
                                 amount, payment_method, payment_date, memo, sync_status)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'pending')
                        ''', (qb_pay_id, qb_inv_id, erp_inv_id, erp_wo_id,
                              amount, method, pay_date, memo[:500]))

                        if erp_inv_id:
                            conn.execute('''
                                UPDATE invoices
                                SET amount_paid = COALESCE(amount_paid, 0) + %s,
                                    status = CASE
                                        WHEN COALESCE(amount_paid, 0) + %s >= total_amount THEN 'Paid'
                                        ELSE status
                                    END
                                WHERE id = %s
                            ''', (amount, amount, erp_inv_id))
                            conn.execute('''
                                UPDATE qb_payment_sync
                                SET applied_to_erp=TRUE, applied_at=NOW(), sync_status='applied'
                                WHERE qb_payment_id=%s
                            ''', (qb_pay_id,))

                        conn.commit()
                        _log_event(conn, 'payment', erp_inv_id, 'payment_received',
                                   'qb_to_erp', 'success', qb_entity_id=qb_pay_id,
                                   payload_recv=p)
                        synced.append({'qb_payment_id': qb_pay_id, 'amount': amount,
                                       'erp_invoice_id': erp_inv_id})
        return synced

    except Exception as ex:
        log.error(f'QB pull_payments error: {ex}')
        return []


# ─── OAuth Flow ───────────────────────────────────────────────────────────────

@qb_sync_bp.route('/qb/connect')
@login_required
def qb_connect():
    if session.get('role') not in ('Admin', 'Accountant'):
        flash('Only Admin or Accountant users can connect QuickBooks.', 'danger')
        return redirect(url_for('qb_sync_routes.qb_dashboard'))

    db   = Database()
    conn = db.get_connection()
    ensure_qb_tables(conn)
    config = _get_config(conn)
    conn.close()
    client_id, _ = _get_credentials(config)
    # Auto-detected public URL takes priority; stored value only used for
    # explicitly custom overrides (env var → auto-detected → DB stored)
    redirect_uri  = os.environ.get('QB_REDIRECT_URI') or \
                    _auto_redirect_uri(request) or \
                    (config.get('redirect_uri') if config else None)
    if not client_id:
        flash('QuickBooks credentials are not configured. Please enter your Client ID and Client Secret on this page first.', 'danger')
        return redirect(url_for('qb_sync_routes.qb_dashboard'))

    import secrets
    state = secrets.token_urlsafe(16)
    session['qb_oauth_state'] = state
    session['qb_redirect_uri'] = redirect_uri  # store so callback uses same value

    log.warning(f'[QB CONNECT] redirect_uri being sent to QB: {redirect_uri}')
    log.warning(f'[QB CONNECT] client_id (first 8): {client_id[:8]}...')

    from urllib.parse import urlencode
    params = {
        'client_id':     client_id,
        'response_type': 'code',
        'scope':         QB_SCOPE,
        'redirect_uri':  redirect_uri,
        'state':         state,
    }
    auth_url = QB_OAUTH_URL + '?' + urlencode(params)
    return redirect(auth_url)


@qb_sync_bp.route('/qb/callback')
@login_required
def qb_callback():
    code      = request.args.get('code')
    realm_id  = request.args.get('realmId')
    state     = request.args.get('state')

    if state != session.get('qb_oauth_state'):
        flash('Invalid OAuth state. Please try connecting again.', 'danger')
        return redirect(url_for('qb_sync_routes.qb_dashboard'))

    db         = Database()
    conn       = db.get_connection()
    ensure_qb_tables(conn)
    config     = _get_config(conn)
    client_id, client_secret = _get_credentials(config)
    # Must use exactly the same redirect_uri that was sent during /qb/connect
    redirect_uri = session.get('qb_redirect_uri') or \
                   os.environ.get('QB_REDIRECT_URI') or \
                   _auto_redirect_uri(request)
    log.warning(f'[QB CALLBACK] redirect_uri used for token exchange: {redirect_uri}')

    try:
        resp = http.post(
            QB_TOKEN_URL,
            auth=(client_id, client_secret),
            data={'grant_type': 'authorization_code', 'code': code, 'redirect_uri': redirect_uri},
            timeout=10,
        )
        if resp.status_code != 200:
            flash(f'QB authentication failed: {resp.text[:200]}', 'danger')
            return redirect(url_for('qb_sync_routes.qb_dashboard'))

        data   = resp.json()
        expiry = datetime.now() + timedelta(seconds=data.get('expires_in', 3600))

        conn.execute('''
            INSERT INTO qb_sync_config
                (tenant_id, realm_id, access_token, refresh_token, token_expiry,
                 connected_at, connected_by, is_active, sandbox_mode)
            VALUES ('default', %s, %s, %s, %s, NOW(), %s, TRUE, %s)
            ON CONFLICT DO NOTHING
        ''', (realm_id, data['access_token'], data.get('refresh_token'),
              expiry.isoformat(), session.get('username', 'system'),
              os.environ.get('QB_SANDBOX', 'true').lower() != 'false'))

        existing = conn.execute("SELECT id FROM qb_sync_config WHERE tenant_id='default' LIMIT 1").fetchone()
        if existing:
            conn.execute('''
                UPDATE qb_sync_config
                SET realm_id=%s, access_token=%s, refresh_token=%s, token_expiry=%s,
                    connected_at=NOW(), connected_by=%s, is_active=TRUE, sandbox_mode=%s, updated_at=NOW()
                WHERE tenant_id='default'
            ''', (realm_id, data['access_token'], data.get('refresh_token'),
                  expiry.isoformat(), session.get('username', 'system'),
                  os.environ.get('QB_SANDBOX', 'true').lower() != 'false'))

        conn.commit()
        conn.close()
        flash('QuickBooks connected successfully!', 'success')

    except Exception as ex:
        flash(f'Error exchanging QB token: {ex}', 'danger')

    return redirect(url_for('qb_sync_routes.qb_dashboard'))


@qb_sync_bp.route('/qb/disconnect', methods=['POST'])
@login_required
def qb_disconnect():
    if session.get('role') not in ('Admin', 'Accountant'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    db   = Database()
    conn = db.get_connection()
    try:
        ensure_qb_tables(conn)
        config = _get_config(conn)
        if config and config.get('access_token'):
            try:
                cid, csec = _get_credentials(config)
                http.post(QB_REVOKE_URL,
                          auth=(cid, csec),
                          data={'token': config['access_token']}, timeout=5)
            except Exception:
                pass
        conn.execute('''
            UPDATE qb_sync_config
            SET is_active=FALSE, access_token=NULL, refresh_token=NULL, updated_at=NOW()
            WHERE tenant_id='default'
        ''')
        conn.commit()
        return jsonify({'message': 'QuickBooks disconnected.'})
    finally:
        conn.close()


# ─── Credentials Save ─────────────────────────────────────────────────────────

@qb_sync_bp.route('/api/qb/credentials', methods=['POST'])
@login_required
def save_qb_credentials():
    if session.get('role') not in ('Admin', 'Accountant'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    data = request.get_json() or {}
    client_id     = (data.get('client_id') or '').strip()
    client_secret = (data.get('client_secret') or '').strip()
    redirect_uri  = (data.get('redirect_uri') or '').strip()
    if not client_id or not client_secret:
        return jsonify({'error': 'Client ID and Client Secret are required.'}), 400
    db   = Database()
    conn = db.get_connection()
    try:
        ensure_qb_tables(conn)
        existing = conn.execute("SELECT id FROM qb_sync_config WHERE tenant_id='default' LIMIT 1").fetchone()
        if existing:
            conn.execute('''
                UPDATE qb_sync_config
                SET client_id=%s, client_secret=%s, redirect_uri=%s, updated_at=NOW()
                WHERE tenant_id='default'
            ''', (client_id, client_secret, redirect_uri or None))
        else:
            conn.execute('''
                INSERT INTO qb_sync_config (tenant_id, client_id, client_secret, redirect_uri)
                VALUES ('default', %s, %s, %s)
            ''', (client_id, client_secret, redirect_uri or None))
        conn.commit()
        return jsonify({'message': 'Credentials saved. You can now connect to QuickBooks.'})
    except Exception as ex:
        conn.rollback()
        return jsonify({'error': str(ex)}), 500
    finally:
        conn.close()


# ─── Sync Settings Save ───────────────────────────────────────────────────────

@qb_sync_bp.route('/api/qb/settings', methods=['POST'])
@login_required
def save_qb_settings():
    if session.get('role') not in ('Admin', 'Accountant'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    data = request.get_json() or {}
    db   = Database()
    conn = db.get_connection()
    try:
        ensure_qb_tables(conn)
        params = [
            data.get('conflict_rule', 'manual_review'),
            bool(data.get('auto_sync_wo', False)),
            bool(data.get('auto_sync_inv', False)),
            bool(data.get('auto_sync_pay', False)),
            bool(data.get('sandbox_mode', True)),
        ]
        extra_set = ''
        if 'webhook_secret' in data and data['webhook_secret']:
            extra_set = ', webhook_secret=%s'
            params.append(data['webhook_secret'].strip())
        conn.execute(
            f'''UPDATE qb_sync_config
               SET conflict_rule=%s, auto_sync_wo=%s, auto_sync_inv=%s,
                   auto_sync_pay=%s, sandbox_mode=%s{extra_set}, updated_at=NOW()
               WHERE tenant_id='default' ''',
            params,
        )
        conn.commit()
        return jsonify({'message': 'Settings saved.'})
    finally:
        conn.close()


# ─── Sync API Endpoints ───────────────────────────────────────────────────────

@qb_sync_bp.route('/api/qb/status')
@login_required
def qb_status():
    db   = Database()
    conn = db.get_connection()
    try:
        ensure_qb_tables(conn)
        config = _get_config(conn)
        auto_uri = _auto_redirect_uri(request)
        if not config:
            return jsonify({
                'connected': False,
                'status': 'not_configured',
                'credentials_configured': bool(os.environ.get('QB_CLIENT_ID')),
                'client_id_hint': None,
                'redirect_uri_auto': auto_uri,
            })

        cid, _   = _get_credentials(config)
        creds_ok = bool(cid)
        hint     = (cid[:6] + '...' + cid[-4:]) if cid and len(cid) > 10 else (cid or None)

        token_ok = not _token_expired(config)
        recent   = conn.execute('''
            SELECT status, COUNT(*) as cnt
            FROM qb_sync_event_log
            WHERE created_at >= NOW() - INTERVAL '24 hours'
            GROUP BY status
        ''').fetchall()
        stats    = {r['status']: int(r['cnt']) for r in recent}
        pending_conflicts = conn.execute(
            "SELECT COUNT(*) as cnt FROM qb_conflict_log WHERE resolution='pending'"
        ).fetchone()

        return jsonify({
            'connected':               bool(config['is_active']),
            'realm_id':                config['realm_id'],
            'sandbox_mode':            bool(config['sandbox_mode']),
            'token_valid':             token_ok,
            'connected_at':            str(config['connected_at']) if config['connected_at'] else None,
            'connected_by':            config['connected_by'],
            'conflict_rule':           config['conflict_rule'],
            'auto_sync_wo':            bool(config['auto_sync_wo']),
            'auto_sync_inv':           bool(config['auto_sync_inv']),
            'auto_sync_pay':           bool(config['auto_sync_pay']),
            'stats_24h':               stats,
            'pending_conflicts':       int(pending_conflicts['cnt'] or 0) if pending_conflicts else 0,
            'credentials_configured':  creds_ok,
            'client_id_hint':          hint,
            'redirect_uri_auto':       (config.get('redirect_uri') or auto_uri),
        })
    finally:
        conn.close()


@qb_sync_bp.route('/api/qb/sync-wo/<int:wo_id>', methods=['POST'])
@login_required
def api_sync_wo(wo_id):
    db   = Database()
    conn = db.get_connection()
    try:
        result = sync_wo_to_qb(conn, wo_id, trigger='manual_ui')
        return jsonify(result)
    finally:
        conn.close()


@qb_sync_bp.route('/api/qb/sync-invoice/<int:invoice_id>', methods=['POST'])
@login_required
def api_sync_invoice(invoice_id):
    db   = Database()
    conn = db.get_connection()
    try:
        result = sync_invoice_to_qb(conn, invoice_id, trigger='manual_ui')
        return jsonify(result)
    finally:
        conn.close()


@qb_sync_bp.route('/api/qb/pull-payments', methods=['POST'])
@login_required
def api_pull_payments():
    if session.get('role') not in ('Admin', 'Accountant'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    db   = Database()
    conn = db.get_connection()
    try:
        days  = int(request.json.get('days_back', 7)) if request.json else 7
        synced = pull_qb_payments(conn, days_back=days)
        return jsonify({'synced': len(synced), 'payments': synced})
    finally:
        conn.close()


@qb_sync_bp.route('/api/qb/pull-invoice/<int:invoice_id>', methods=['POST'])
@login_required
def api_pull_invoice(invoice_id):
    """Pull latest status for one invoice from QB back into ERP."""
    if session.get('role') not in ('Admin', 'Accountant'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    db   = Database()
    conn = db.get_connection()
    try:
        ensure_qb_tables(conn)
        mapping = conn.execute(
            'SELECT qb_invoice_id FROM qb_wo_invoice_map WHERE invoice_id=%s AND qb_invoice_id IS NOT NULL ORDER BY id DESC LIMIT 1',
            (invoice_id,)
        ).fetchone()
        if not mapping:
            return jsonify({'status': 'skipped', 'reason': 'Invoice has not been synced to QB yet'}), 200
        result = pull_invoice_from_qb(conn, invoice_id, mapping['qb_invoice_id'])
        return jsonify(result)
    finally:
        conn.close()


@qb_sync_bp.route('/api/qb/pull-all-invoices', methods=['POST'])
@login_required
def api_pull_all_invoices():
    """Pull QB updates for all synced invoices."""
    if session.get('role') not in ('Admin', 'Accountant'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    db   = Database()
    conn = db.get_connection()
    try:
        ensure_qb_tables(conn)
        results = pull_all_synced_invoices(conn)
        return jsonify(results)
    finally:
        conn.close()


@qb_sync_bp.route('/api/qb/wo-sync-status/<int:wo_id>')
@login_required
def wo_sync_status(wo_id):
    db   = Database()
    conn = db.get_connection()
    try:
        ensure_qb_tables(conn)
        mapping = conn.execute(
            'SELECT * FROM qb_wo_invoice_map WHERE wo_id=%s ORDER BY id DESC LIMIT 1', (wo_id,)
        ).fetchone()
        recent_logs = conn.execute('''
            SELECT event_type, status, error_message, created_at
            FROM qb_sync_event_log
            WHERE entity_type='work_order' AND entity_id=%s
            ORDER BY created_at DESC LIMIT 8
        ''', (wo_id,)).fetchall()
        conflicts = conn.execute('''
            SELECT * FROM qb_conflict_log
            WHERE entity_type='work_order' AND entity_id=%s AND resolution='pending'
        ''', (wo_id,)).fetchall()
        config = _get_config(conn)

        return jsonify({
            'connected':        bool(config and config['is_active']),
            'sandbox_mode':     bool(config['sandbox_mode']) if config else True,
            'mapping':          dict(mapping) if mapping else None,
            'logs':             [
                {'event': l['event_type'], 'status': l['status'],
                 'error': l['error_message'],
                 'at': str(l['created_at'])} for l in recent_logs
            ],
            'pending_conflicts': [dict(c) for c in conflicts],
        })
    finally:
        conn.close()


@qb_sync_bp.route('/api/qb/logs')
@login_required
def qb_logs():
    db   = Database()
    conn = db.get_connection()
    try:
        ensure_qb_tables(conn)
        limit  = min(int(request.args.get('limit', 50)), 200)
        entity = request.args.get('entity_type', '')
        status = request.args.get('status', '')
        sql    = 'SELECT * FROM qb_sync_event_log WHERE 1=1'
        params = []
        if entity:
            sql += ' AND entity_type=%s'; params.append(entity)
        if status:
            sql += ' AND status=%s'; params.append(status)
        sql += ' ORDER BY created_at DESC LIMIT %s'; params.append(limit)
        rows = conn.execute(sql, params).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@qb_sync_bp.route('/api/qb/conflicts')
@login_required
def qb_conflicts():
    db   = Database()
    conn = db.get_connection()
    try:
        ensure_qb_tables(conn)
        rows = conn.execute(
            "SELECT * FROM qb_conflict_log ORDER BY created_at DESC LIMIT 100"
        ).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@qb_sync_bp.route('/api/qb/conflicts/<int:conflict_id>/resolve', methods=['POST'])
@login_required
def resolve_conflict(conflict_id):
    data       = request.get_json() or {}
    resolution = data.get('resolution', 'erp_wins')
    notes      = data.get('notes', '').strip()

    db   = Database()
    conn = db.get_connection()
    try:
        ensure_qb_tables(conn)
        conn.execute('''
            UPDATE qb_conflict_log
            SET resolution=%s, resolved_by=%s, resolved_at=NOW(), resolution_notes=%s
            WHERE id=%s
        ''', (resolution, session.get('username', 'system'), notes, conflict_id))
        conn.commit()
        return jsonify({'message': f'Conflict resolved: {resolution}'})
    finally:
        conn.close()


@qb_sync_bp.route('/api/qb/payments')
@login_required
def qb_payments_list():
    db   = Database()
    conn = db.get_connection()
    try:
        ensure_qb_tables(conn)
        rows = conn.execute(
            'SELECT * FROM qb_payment_sync ORDER BY created_at DESC LIMIT 100'
        ).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


# ─── QB Webhook (QBO pushes events here) ──────────────────────────────────────

@qb_sync_bp.route('/qb/webhook', methods=['POST'])
def qb_webhook():
    import hmac, hashlib
    db   = Database()
    conn = db.get_connection()
    try:
        ensure_qb_tables(conn)
        config = _get_config(conn)
        secret = config['webhook_secret'] if config else ''

        if secret:
            sig     = request.headers.get('intuit-signature', '')
            payload = request.get_data()
            expected = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
            if not hmac.compare_digest(sig, expected):
                return jsonify({'error': 'invalid signature'}), 401

        body    = request.get_json(force=True) or {}
        for notification in body.get('eventNotifications', []):
            for entity_event in notification.get('dataChangeEvent', {}).get('entities', []):
                etype  = entity_event.get('name')
                eid    = entity_event.get('id')
                op     = entity_event.get('operation')
                _log_event(conn, f'qb_{etype}', None, f'webhook_{op}', 'qb_to_erp', 'received',
                           qb_entity_id=eid)
                if etype == 'Payment' and op in ('Create', 'Update'):
                    pull_qb_payments(conn, days_back=1)
                elif etype == 'Invoice' and op in ('Create', 'Update', 'Delete'):
                    mapping = conn.execute(
                        'SELECT invoice_id FROM qb_wo_invoice_map WHERE qb_invoice_id=%s AND invoice_id IS NOT NULL LIMIT 1',
                        (eid,)
                    ).fetchone()
                    if mapping and mapping['invoice_id']:
                        pull_invoice_from_qb(conn, mapping['invoice_id'], eid)

        conn.commit()
        return jsonify({'status': 'ok'})
    finally:
        conn.close()


# ─── QB Dashboard Page ────────────────────────────────────────────────────────

@qb_sync_bp.route('/qb/dashboard')
@login_required
def qb_dashboard():
    db   = Database()
    conn = db.get_connection()
    try:
        ensure_qb_tables(conn)
        config  = _get_config(conn)
        return render_template('qb_sync/dashboard.html', config=config)
    finally:
        conn.close()


# ─── Nightly Invoice Sync ─────────────────────────────────────────────────────

def _sync_unsynced_invoices(conn):
    """Find all standard invoices not yet pushed to QB and sync them one by one.
    Returns summary dict: {synced, failed, skipped, auth_expired}."""
    config = _get_config(conn)
    if not config or not config['is_active']:
        return {'synced': 0, 'failed': 0, 'skipped': 0, 'auth_expired': False}

    try:
        unsynced = conn.execute('''
            SELECT i.id
            FROM invoices i
            WHERE i.invoice_type != 'NDT'
              AND COALESCE(i.status, '') NOT IN ('Cancelled', 'Draft')
              AND COALESCE(i.total_amount, 0) > 0
              AND i.id NOT IN (
                  SELECT invoice_id FROM qb_wo_invoice_map
                  WHERE invoice_id IS NOT NULL
              )
            ORDER BY i.id DESC
            LIMIT 100
        ''').fetchall()
    except Exception as ex:
        log.error(f'[QB NightlySync] Query failed: {ex}')
        return {'synced': 0, 'failed': 0, 'skipped': 0, 'auth_expired': False}

    results = {'synced': 0, 'failed': 0, 'skipped': 0, 'auth_expired': False}
    for row in unsynced:
        r = sync_invoice_to_qb(conn, row['id'], trigger='nightly_auto')
        if r['status'] == 'success':
            results['synced'] += 1
        elif r['status'] == 'auth_expired':
            results['auth_expired'] = True
            results['auth_reason'] = r.get('reason', 'QB session expired')
            break
        elif r['status'] in ('skipped', 'conflict'):
            results['skipped'] += 1
        else:
            results['failed'] += 1

    log.info(f'[QB NightlySync] Complete: {results}')
    return results


# ─── Background Auto-Pull / Nightly Sync Scheduler ───────────────────────────

import threading as _threading
_qb_scheduler_started     = False
_last_nightly_inv_sync_dt = None   # tracks the date of the last nightly push


def _qb_auto_pull_loop():
    """Background thread:
       - Every 30 minutes: pull QB payment/balance updates if auto_sync_pay is on.
       - Once per night (00:00–01:00): push unsynced invoices to QB if auto_sync_inv is on.
    """
    import time
    global _last_nightly_inv_sync_dt
    while True:
        time.sleep(1800)   # wake every 30 minutes
        try:
            db   = Database()
            conn = db.get_connection()
            try:
                config = _get_config(conn)
                if not (config and config['is_active']):
                    continue

                # ── 30-minute: pull QB updates back into ERP ──
                if config.get('auto_sync_pay'):
                    log.info('[QB Auto-Pull] Running scheduled pull from QuickBooks...')
                    results = pull_all_synced_invoices(conn)
                    log.info(f'[QB Auto-Pull] Done: {results}')

                # ── Nightly (midnight window): push unsynced invoices to QB ──
                if config.get('auto_sync_inv'):
                    now   = datetime.now()
                    today = now.date()
                    if 0 <= now.hour < 1 and _last_nightly_inv_sync_dt != today:
                        log.info('[QB NightlySync] Starting nightly push of unsynced invoices...')
                        _last_nightly_inv_sync_dt = today
                        results = _sync_unsynced_invoices(conn)
                        log.info(f'[QB NightlySync] Done: {results}')
            finally:
                conn.close()
        except Exception as ex:
            log.warning(f'[QB Scheduler] Error: {ex}')


def start_qb_scheduler():
    global _qb_scheduler_started
    if not _qb_scheduler_started:
        _qb_scheduler_started = True
        t = _threading.Thread(target=_qb_auto_pull_loop, name='qb-scheduler', daemon=True)
        t.start()
        log.info('[QB] Background scheduler started (auto-pull + nightly sync)')


start_qb_scheduler()


# ─── Invoice Auto-Sync Toggle Endpoints ───────────────────────────────────────

@qb_sync_bp.route('/api/invoices/qb-auto-sync-inv', methods=['GET'])
@login_required
def get_auto_sync_inv():
    """Return current auto_sync_inv flag and last nightly sync date."""
    if session.get('role') not in ('Admin', 'Accountant'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    db   = Database()
    conn = db.get_connection()
    try:
        ensure_qb_tables(conn)
        config = _get_config(conn)
        return jsonify({
            'enabled':    bool(config and config.get('auto_sync_inv')),
            'last_run':   str(_last_nightly_inv_sync_dt) if _last_nightly_inv_sync_dt else None,
            'qb_active':  bool(config and config.get('is_active')),
        })
    finally:
        conn.close()


@qb_sync_bp.route('/api/invoices/qb-auto-sync-inv', methods=['POST'])
@login_required
def set_auto_sync_inv():
    """Toggle or set the auto_sync_inv flag."""
    if session.get('role') not in ('Admin', 'Accountant'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    data    = request.get_json() or {}
    enabled = bool(data.get('enabled', False))
    db   = Database()
    conn = db.get_connection()
    try:
        ensure_qb_tables(conn)
        conn.execute(
            "UPDATE qb_sync_config SET auto_sync_inv=%s, updated_at=NOW() WHERE tenant_id='default'",
            (enabled,),
        )
        conn.commit()
        return jsonify({'enabled': enabled, 'message': f'Nightly QB auto-sync {"enabled" if enabled else "disabled"}.'})
    finally:
        conn.close()


@qb_sync_bp.route('/api/invoices/qb-sync-unsynced-now', methods=['POST'])
@login_required
def sync_unsynced_now():
    """Immediately push all unsynced invoices to QB (manual trigger)."""
    if session.get('role') not in ('Admin', 'Accountant'):
        return jsonify({'error': 'Insufficient permissions'}), 403
    db   = Database()
    conn = db.get_connection()
    try:
        ensure_qb_tables(conn)
        results = _sync_unsynced_invoices(conn)
        return jsonify(results)
    finally:
        conn.close()
