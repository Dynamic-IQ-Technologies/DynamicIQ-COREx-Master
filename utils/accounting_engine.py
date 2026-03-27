"""
accounting_engine.py - Central Accounting Engine
=================================================
Implements GAAP-compliant double-entry bookkeeping for all financial transactions.

Rules enforced:
  - Every financial transaction generates a balanced, atomic journal entry
  - Debits must equal credits (tolerance: $0.01)
  - All lines reference valid Chart of Accounts entries
  - Journal entries are immutable once posted
  - Full traceability: source module, reference ID, user, timestamp
"""

import logging
from datetime import datetime

logger = logging.getLogger('accounting_engine')


# ---------------------------------------------------------------------------
# Chart-of-Accounts codes used by this engine
# ---------------------------------------------------------------------------
ACCOUNTS = {
    'cash':         '1110',
    'ar':           '1120',
    'inventory':    '1130',
    'wip':          '1140',
    'finished':     '1150',
    'equipment':    '1210',
    'ap':           '2110',
    'accrued':      '2120',
    'tax_payable':  '2130',
    'wages_payable':'2150',
    'sales_rev':    '4100',
    'service_rev':  '4200',
    'other_inc':    '4300',
    'cogs':         '5000',
    'material':     '5100',
    'labor':        '5200',
    'overhead':     '5300',
    'salaries':     '6100',
    'depreciation': '6400',
    'admin':        '6500',
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_account_id(conn, code):
    row = conn.execute(
        'SELECT id FROM chart_of_accounts WHERE account_code = ? AND is_active = 1',
        (code,)
    ).fetchone()
    if not row:
        raise ValueError(f'Account code {code} not found or inactive in Chart of Accounts')
    return row['id']


def _next_je_number(conn, prefix='JE'):
    row = conn.execute(
        "SELECT entry_number FROM gl_entries "
        "WHERE entry_number LIKE ? "
        "ORDER BY id DESC LIMIT 1",
        (f'{prefix}-%',)
    ).fetchone()
    if row:
        try:
            last_num = int(row['entry_number'].split('-')[-1])
            return f'{prefix}-{last_num + 1:06d}'
        except (ValueError, IndexError):
            pass
    return f'{prefix}-000001'


def _create_journal_entry(conn, *, entry_number, entry_date, description,
                          transaction_source, reference_type, reference_id,
                          lines, created_by, status='Posted'):
    """
    Core double-entry posting engine.

    lines: list of dicts with keys:
        account_code  str   e.g. '1120'
        debit         float
        credit        float
        description   str

    Returns: gl_entry_id (int)
    Raises:  ValueError if debits != credits or account not found
    """
    total_debit  = round(sum(float(l.get('debit',  0)) for l in lines), 2)
    total_credit = round(sum(float(l.get('credit', 0)) for l in lines), 2)

    if abs(total_debit - total_credit) > 0.01:
        raise ValueError(
            f'Unbalanced journal entry: debits={total_debit}, credits={total_credit} '
            f'(diff={abs(total_debit - total_credit):.4f}). '
            f'Source: {transaction_source} ref {reference_type}:{reference_id}'
        )

    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    cur = conn.execute('''
        INSERT INTO gl_entries (
            entry_number, entry_date, description, transaction_source,
            reference_type, reference_id, status,
            created_by, created_at, posted_by, posted_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        entry_number, entry_date, description, transaction_source,
        reference_type, str(reference_id), status,
        created_by, now,
        created_by if status == 'Posted' else None,
        now        if status == 'Posted' else None,
    ))

    entry_id = cur.lastrowid
    if not entry_id:
        raise RuntimeError('Failed to obtain gl_entries ID after INSERT')

    for line in lines:
        account_id = _get_account_id(conn, line['account_code'])
        conn.execute('''
            INSERT INTO gl_entry_lines (gl_entry_id, account_id, debit, credit, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            entry_id,
            account_id,
            round(float(line.get('debit',  0)), 4),
            round(float(line.get('credit', 0)), 4),
            line.get('description', description),
        ))

    logger.info(
        'JE posted: %s | source=%s | ref=%s:%s | DR=%.2f CR=%.2f | id=%s',
        entry_number, transaction_source, reference_type, reference_id,
        total_debit, total_credit, entry_id
    )
    return entry_id


# ---------------------------------------------------------------------------
# Public transaction-type posting functions
# ---------------------------------------------------------------------------

def post_ar_invoice(conn, invoice_id, user_id):
    """
    AR Invoice Issued:
        DR  Accounts Receivable  (1120)
        CR  Sales Revenue        (4100)

    Returns gl_entry_id, or raises on failure.
    """
    inv = conn.execute('SELECT * FROM invoices WHERE id = ?', (invoice_id,)).fetchone()
    if not inv:
        raise ValueError(f'Invoice {invoice_id} not found')
    if inv.get('gl_entry_id'):
        return inv['gl_entry_id']

    total = round(float(inv['total_amount'] or 0), 2)
    if total <= 0:
        raise ValueError(f'Invoice {invoice_id} has zero/negative total — cannot post')

    entry_num = _next_je_number(conn, 'AR-INV')
    ref       = inv['invoice_number']

    entry_id = _create_journal_entry(
        conn,
        entry_number=entry_num,
        entry_date=str(inv['invoice_date']),
        description=f'Revenue Recognition - {ref}',
        transaction_source='AR Invoice',
        reference_type='invoice',
        reference_id=invoice_id,
        lines=[
            {'account_code': ACCOUNTS['ar'],        'debit': total,  'credit': 0,     'description': f'AR - {ref}'},
            {'account_code': ACCOUNTS['sales_rev'],  'debit': 0,      'credit': total, 'description': f'Revenue - {ref}'},
        ],
        created_by=user_id,
    )

    conn.execute(
        'UPDATE invoices SET gl_entry_id = ?, status = ?, posted_by = ?, posted_at = CURRENT_TIMESTAMP WHERE id = ?',
        (entry_id, 'Posted' if inv['status'] == 'Approved' else inv['status'], user_id, invoice_id)
    )
    return entry_id


def post_ap_invoice(conn, vendor_invoice_id, user_id, expense_account_code=None):
    """
    AP / Vendor Invoice Received:
        DR  Inventory  (1130)  [or expense_account_code if supplied]
        CR  Accounts Payable (2110)

    Returns gl_entry_id, or raises on failure.
    """
    vi = conn.execute('SELECT * FROM vendor_invoices WHERE id = ?', (vendor_invoice_id,)).fetchone()
    if not vi:
        raise ValueError(f'Vendor invoice {vendor_invoice_id} not found')
    if vi.get('gl_entry_id'):
        return vi['gl_entry_id']

    total = round(float(vi['total_amount'] or 0), 2)
    if total <= 0:
        raise ValueError(f'Vendor invoice {vendor_invoice_id} has zero/negative total')

    debit_account = expense_account_code or ACCOUNTS['inventory']
    entry_num = _next_je_number(conn, 'AP-INV')
    ref       = vi['invoice_number']

    entry_id = _create_journal_entry(
        conn,
        entry_number=entry_num,
        entry_date=str(vi['invoice_date']),
        description=f'Vendor Invoice - {ref}',
        transaction_source='AP Invoice',
        reference_type='vendor_invoice',
        reference_id=vendor_invoice_id,
        lines=[
            {'account_code': debit_account,    'debit': total,  'credit': 0,     'description': f'Inventory/Material - {ref}'},
            {'account_code': ACCOUNTS['ap'],   'debit': 0,      'credit': total, 'description': f'AP - {ref}'},
        ],
        created_by=user_id,
    )

    conn.execute(
        'UPDATE vendor_invoices SET gl_entry_id = ? WHERE id = ?',
        (entry_id, vendor_invoice_id)
    )
    return entry_id


def post_ar_payment(conn, invoice_id, amount, payment_date, payment_method,
                    payment_reference, user_id):
    """
    Customer Payment Received:
        DR  Cash (1110)
        CR  Accounts Receivable (1120)

    Returns gl_entry_id.
    """
    inv = conn.execute('SELECT * FROM invoices WHERE id = ?', (invoice_id,)).fetchone()
    if not inv:
        raise ValueError(f'Invoice {invoice_id} not found')

    amount = round(float(amount), 2)
    entry_num = _next_je_number(conn, 'AR-PAY')
    ref       = inv['invoice_number']
    desc      = f'Customer Payment - {ref} - {payment_method}'
    if payment_reference:
        desc += f' ({payment_reference})'

    entry_id = _create_journal_entry(
        conn,
        entry_number=entry_num,
        entry_date=payment_date,
        description=desc,
        transaction_source='Customer Payment',
        reference_type='invoice',
        reference_id=invoice_id,
        lines=[
            {'account_code': ACCOUNTS['cash'], 'debit': amount, 'credit': 0,      'description': f'Cash received - {ref}'},
            {'account_code': ACCOUNTS['ar'],   'debit': 0,      'credit': amount, 'description': f'AR cleared - {ref}'},
        ],
        created_by=user_id,
    )
    return entry_id


def post_ap_payment(conn, vendor_invoice_id, amount, payment_date, payment_method,
                    payment_reference, user_id):
    """
    Vendor Payment Made:
        DR  Accounts Payable (2110)
        CR  Cash (1110)

    Returns gl_entry_id.
    """
    vi = conn.execute('SELECT * FROM vendor_invoices WHERE id = ?', (vendor_invoice_id,)).fetchone()
    if not vi:
        raise ValueError(f'Vendor invoice {vendor_invoice_id} not found')

    amount = round(float(amount), 2)
    entry_num = _next_je_number(conn, 'AP-PAY')
    ref       = vi['invoice_number']
    desc      = f'AP Payment - {ref} - {payment_method}'
    if payment_reference:
        desc += f' ({payment_reference})'

    entry_id = _create_journal_entry(
        conn,
        entry_number=entry_num,
        entry_date=payment_date,
        description=desc,
        transaction_source='AP Payment',
        reference_type='vendor_invoice',
        reference_id=vendor_invoice_id,
        lines=[
            {'account_code': ACCOUNTS['ap'],   'debit': amount, 'credit': 0,      'description': f'AP cleared - {ref}'},
            {'account_code': ACCOUNTS['cash'],  'debit': 0,      'credit': amount, 'description': f'Cash paid - {ref}'},
        ],
        created_by=user_id,
    )
    return entry_id


def post_inventory_receipt(conn, receipt_id, product_name, total_value,
                           receipt_number, receipt_date, user_id):
    """
    Inventory Receipt:
        DR  Inventory (1130)
        CR  Accounts Payable (2110)
    """
    entry_num = _next_je_number(conn, 'INV-RCV')
    return _create_journal_entry(
        conn,
        entry_number=entry_num,
        entry_date=receipt_date,
        description=f'Material Receiving - {receipt_number}',
        transaction_source='Material Receiving',
        reference_type='receiving_transaction',
        reference_id=receipt_id,
        lines=[
            {'account_code': ACCOUNTS['inventory'], 'debit': total_value,  'credit': 0,           'description': f'Inventory received - {product_name} ({receipt_number})'},
            {'account_code': ACCOUNTS['ap'],        'debit': 0,            'credit': total_value, 'description': f'AP for receipt - {product_name} ({receipt_number})'},
        ],
        created_by=user_id,
    )


def post_inventory_adjustment(conn, adjustment_id, product_name, adjustment_value,
                               adjustment_date, adjustment_type, user_id):
    """
    Inventory Write-Up:    DR Inventory (1130)  CR Other Income (4300)
    Inventory Write-Down:  DR COGS (5000)        CR Inventory (1130)
    """
    entry_num = _next_je_number(conn, 'INV-ADJ')
    adj_value = abs(round(float(adjustment_value), 2))

    if adjustment_value >= 0:
        lines = [
            {'account_code': ACCOUNTS['inventory'], 'debit': adj_value, 'credit': 0,          'description': f'Inventory write-up - {product_name}'},
            {'account_code': ACCOUNTS['other_inc'], 'debit': 0,         'credit': adj_value,  'description': f'Inventory write-up gain - {product_name}'},
        ]
    else:
        lines = [
            {'account_code': ACCOUNTS['cogs'],      'debit': adj_value, 'credit': 0,          'description': f'Inventory write-down loss - {product_name}'},
            {'account_code': ACCOUNTS['inventory'], 'debit': 0,         'credit': adj_value,  'description': f'Inventory write-down - {product_name}'},
        ]

    return _create_journal_entry(
        conn,
        entry_number=entry_num,
        entry_date=adjustment_date,
        description=f'Inventory Adjustment - {product_name}',
        transaction_source='Inventory Adjustment',
        reference_type='inventory_adjustment',
        reference_id=adjustment_id,
        lines=lines,
        created_by=user_id,
    )


def post_wip_issuance(conn, issuance_id, product_name, total_value,
                      issuance_date, user_id):
    """
    Material Issued to Production (WIP):
        DR  WIP - Work In Process (1140)
        CR  Inventory (1130)
    """
    entry_num = _next_je_number(conn, 'WIP-ISS')
    return _create_journal_entry(
        conn,
        entry_number=entry_num,
        entry_date=issuance_date,
        description=f'Material Issued to WIP - {product_name}',
        transaction_source='WIP Issuance',
        reference_type='issuance',
        reference_id=issuance_id,
        lines=[
            {'account_code': ACCOUNTS['wip'],       'debit': total_value, 'credit': 0,           'description': f'WIP - {product_name}'},
            {'account_code': ACCOUNTS['inventory'],  'debit': 0,           'credit': total_value, 'description': f'Inventory issued - {product_name}'},
        ],
        created_by=user_id,
    )


def post_work_order_completion(conn, wo_id, wo_number, total_cost, completion_date, user_id):
    """
    Work Order Completion (transfer WIP → Finished Goods):
        DR  Finished Goods Inventory (1150)
        CR  WIP - Work In Process    (1140)
    """
    entry_num = _next_je_number(conn, 'WO-COMP')
    return _create_journal_entry(
        conn,
        entry_number=entry_num,
        entry_date=completion_date,
        description=f'Work Order Completion - {wo_number}',
        transaction_source='Work Order',
        reference_type='work_order',
        reference_id=wo_id,
        lines=[
            {'account_code': ACCOUNTS['finished'], 'debit': total_cost, 'credit': 0,          'description': f'FG from WO {wo_number}'},
            {'account_code': ACCOUNTS['wip'],      'debit': 0,          'credit': total_cost, 'description': f'WIP cleared - WO {wo_number}'},
        ],
        created_by=user_id,
    )


def post_cogs(conn, invoice_id, invoice_number, cogs_amount, invoice_date, user_id):
    """
    Cost of Goods Sold on Shipment:
        DR  COGS (5000)
        CR  Inventory (1130)
    """
    entry_num = _next_je_number(conn, 'COGS')
    return _create_journal_entry(
        conn,
        entry_number=entry_num,
        entry_date=invoice_date,
        description=f'COGS - {invoice_number}',
        transaction_source='COGS',
        reference_type='invoice',
        reference_id=invoice_id,
        lines=[
            {'account_code': ACCOUNTS['cogs'],      'debit': cogs_amount, 'credit': 0,            'description': f'COGS - {invoice_number}'},
            {'account_code': ACCOUNTS['inventory'],  'debit': 0,           'credit': cogs_amount,  'description': f'Inventory consumed - {invoice_number}'},
        ],
        created_by=user_id,
    )


# ---------------------------------------------------------------------------
# Integrity check
# ---------------------------------------------------------------------------

def integrity_check(conn):
    """
    Returns a dict with counts of integrity violations across the system.
    """
    result = {}

    # AR invoices missing GL entries
    rows = conn.execute('''
        SELECT id, invoice_number, total_amount, status, invoice_date
        FROM invoices
        WHERE gl_entry_id IS NULL AND status NOT IN ('Draft', 'Void')
        ORDER BY invoice_date
    ''').fetchall()
    result['ar_invoices_missing_je'] = [dict(r) for r in rows]

    # AP vendor invoices missing GL entries
    rows = conn.execute('''
        SELECT vi.id, vi.invoice_number, vi.total_amount, vi.status, vi.invoice_date,
               s.name as vendor_name
        FROM vendor_invoices vi
        LEFT JOIN suppliers s ON vi.vendor_id = s.id
        WHERE vi.gl_entry_id IS NULL
        ORDER BY vi.invoice_date
    ''').fetchall()
    result['ap_invoices_missing_je'] = [dict(r) for r in rows]

    # Unbalanced journal entries
    rows = conn.execute('''
        SELECT ge.id, ge.entry_number, ge.description, ge.transaction_source,
               SUM(gel.debit) as total_debit,
               SUM(gel.credit) as total_credit,
               ABS(SUM(gel.debit) - SUM(gel.credit)) as imbalance
        FROM gl_entries ge
        JOIN gl_entry_lines gel ON gel.gl_entry_id = ge.id
        GROUP BY ge.id, ge.entry_number, ge.description, ge.transaction_source
        HAVING ABS(SUM(gel.debit) - SUM(gel.credit)) > 0.01
        ORDER BY imbalance DESC
    ''').fetchall()
    result['unbalanced_entries'] = [dict(r) for r in rows]

    # Journal entries with no lines (orphan headers)
    rows = conn.execute('''
        SELECT ge.id, ge.entry_number, ge.description, ge.transaction_source, ge.entry_date
        FROM gl_entries ge
        LEFT JOIN gl_entry_lines gel ON gel.gl_entry_id = ge.id
        WHERE gel.id IS NULL
        ORDER BY ge.id
    ''').fetchall()
    result['empty_journal_entries'] = [dict(r) for r in rows]

    # Summary counts
    result['summary'] = {
        'ar_missing':         len(result['ar_invoices_missing_je']),
        'ap_missing':         len(result['ap_invoices_missing_je']),
        'unbalanced':         len(result['unbalanced_entries']),
        'empty_headers':      len(result['empty_journal_entries']),
        'total_violations':   (
            len(result['ar_invoices_missing_je']) +
            len(result['ap_invoices_missing_je']) +
            len(result['unbalanced_entries']) +
            len(result['empty_journal_entries'])
        ),
    }

    return result


# ---------------------------------------------------------------------------
# Backfill — generate missing JEs for historical data
# ---------------------------------------------------------------------------

def backfill_missing_je(conn, user_id):
    """
    Scan all financial tables for records missing journal entries and create them.
    Returns a summary dict with counts of JEs created and errors.
    """
    created = []
    errors  = []

    # 1. AR invoices (not Draft, not Void) missing JE
    invoices = conn.execute('''
        SELECT * FROM invoices
        WHERE gl_entry_id IS NULL AND status NOT IN ('Draft', 'Void')
    ''').fetchall()

    for inv in invoices:
        try:
            eid = post_ar_invoice(conn, inv['id'], user_id)
            created.append({'type': 'AR Invoice', 'ref': inv['invoice_number'], 'gl_entry_id': eid})
        except Exception as e:
            errors.append({'type': 'AR Invoice', 'ref': inv.get('invoice_number'), 'error': str(e)})
            logger.error('Backfill AR Invoice %s: %s', inv.get('invoice_number'), e)

    # 2. AP vendor invoices missing JE
    vendor_invoices = conn.execute('''
        SELECT * FROM vendor_invoices WHERE gl_entry_id IS NULL
    ''').fetchall()

    for vi in vendor_invoices:
        try:
            eid = post_ap_invoice(conn, vi['id'], user_id)
            created.append({'type': 'AP Invoice', 'ref': vi['invoice_number'], 'gl_entry_id': eid})
        except Exception as e:
            errors.append({'type': 'AP Invoice', 'ref': vi.get('invoice_number'), 'error': str(e)})
            logger.error('Backfill AP Invoice %s: %s', vi.get('invoice_number'), e)

    logger.info('Backfill complete: %d JEs created, %d errors', len(created), len(errors))
    return {'created': created, 'errors': errors}
