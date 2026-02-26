"""
GL Journal Entry Utility Module

Provides automatic journal entry generation for all financial transactions
to ensure proper accounting practices and complete audit trail.

Standard Journal Entry Types:
- Sales Invoice: DR A/R, CR Sales Revenue, DR COGS, CR Inventory
- Payment Received: DR Cash, CR A/R
- Purchase/Vendor Invoice: DR Inventory/Expense, CR A/P
- Payment Made: DR A/P, CR Cash
- Work Order Materials: DR WIP, CR Inventory
- Work Order Labor: DR WIP, CR Wages Payable
- Work Order Completion: DR COGS, CR WIP
"""

from datetime import datetime
import logging

logger = logging.getLogger('gl_journal')

GL_ACCOUNTS = {
    'CASH': 1110,
    'AR': 1120,
    'INVENTORY': 1130,
    'WIP': 1140,
    'EQUIPMENT': 1210,
    'AP': 2110,
    'WAGES_PAYABLE': 2150,
    'SALES_REVENUE': 4100,
    'SERVICE_REVENUE': 4200,
    'MATERIAL_COST': 5100,
    'DIRECT_LABOR': 5200,
    'OVERHEAD': 5300,
}

def get_account_id(conn, account_code):
    """Get account ID from account code"""
    result = conn.execute(
        'SELECT id FROM chart_of_accounts WHERE account_code = ?',
        (str(account_code),)
    ).fetchone()
    return result['id'] if result else None

def generate_entry_number(conn):
    """Generate unique journal entry number"""
    result = conn.execute(
        "SELECT MAX(CAST(SUBSTR(entry_number, 4) AS INTEGER)) as max_num FROM gl_entries WHERE entry_number LIKE 'JE-%'"
    ).fetchone()
    next_num = (result['max_num'] or 0) + 1
    return f"JE-{next_num:06d}"

def create_journal_entry(conn, entry_date, description, transaction_source, reference_type, reference_id, lines, user_id=None, auto_post=True):
    """
    Create a journal entry with multiple lines.
    
    Args:
        conn: Database connection
        entry_date: Date of the entry
        description: Description of the transaction
        transaction_source: Source system (e.g., 'Invoice', 'Work Order', 'Payment')
        reference_type: Type of reference document
        reference_id: ID of the reference document
        lines: List of dicts with keys: account_code, debit, credit, description
        user_id: User creating the entry
        auto_post: Whether to auto-post the entry
    
    Returns:
        gl_entry_id if successful, None if failed
    """
    try:
        total_debits = sum(line.get('debit', 0) or 0 for line in lines)
        total_credits = sum(line.get('credit', 0) or 0 for line in lines)
        
        if abs(total_debits - total_credits) > 0.01:
            logger.error(f"Journal entry out of balance: Debits={total_debits}, Credits={total_credits}")
            return None
        
        entry_number = generate_entry_number(conn)
        status = 'Posted' if auto_post else 'Draft'
        posted_by = user_id if auto_post else None
        posted_at = datetime.now() if auto_post else None
        
        cursor = conn.execute('''
            INSERT INTO gl_entries (entry_number, entry_date, description, transaction_source, reference_type, reference_id, status, created_by, created_at, posted_by, posted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (entry_number, entry_date, description, transaction_source, reference_type, str(reference_id) if reference_id is not None else None, status, user_id, datetime.now(), posted_by, posted_at))
        
        gl_entry_id = cursor.lastrowid
        
        for line in lines:
            account_code = line.get('account_code')
            account_id = get_account_id(conn, account_code)
            
            if not account_id:
                logger.error(f"Account not found: {account_code}")
                continue
            
            conn.execute('''
                INSERT INTO gl_entry_lines (gl_entry_id, account_id, debit, credit, description)
                VALUES (?, ?, ?, ?, ?)
            ''', (gl_entry_id, account_id, line.get('debit', 0) or 0, line.get('credit', 0) or 0, line.get('description', '')))
        
        logger.info(f"Created journal entry {entry_number} for {transaction_source} {reference_type}:{reference_id}")
        return gl_entry_id
        
    except Exception as e:
        logger.error(f"Error creating journal entry: {str(e)}")
        return None


def create_sales_invoice_entry(conn, invoice_id, invoice_number, invoice_date, total_amount, cogs_amount, user_id=None):
    """
    Create journal entry for a sales invoice.
    DR Accounts Receivable (1120)
    CR Sales Revenue (4100)
    DR Cost of Goods Sold (5100)
    CR Inventory (1130)
    """
    lines = [
        {'account_code': GL_ACCOUNTS['AR'], 'debit': total_amount, 'credit': 0, 'description': f'Invoice {invoice_number} - A/R'},
        {'account_code': GL_ACCOUNTS['SALES_REVENUE'], 'debit': 0, 'credit': total_amount, 'description': f'Invoice {invoice_number} - Revenue'},
    ]
    
    if cogs_amount and cogs_amount > 0:
        lines.extend([
            {'account_code': GL_ACCOUNTS['MATERIAL_COST'], 'debit': cogs_amount, 'credit': 0, 'description': f'Invoice {invoice_number} - COGS'},
            {'account_code': GL_ACCOUNTS['INVENTORY'], 'debit': 0, 'credit': cogs_amount, 'description': f'Invoice {invoice_number} - Inventory Relief'},
        ])
    
    return create_journal_entry(
        conn=conn,
        entry_date=invoice_date,
        description=f"Sales Invoice {invoice_number}",
        transaction_source='Sales Invoice',
        reference_type='invoice',
        reference_id=invoice_id,
        lines=lines,
        user_id=user_id
    )


def create_payment_received_entry(conn, payment_id, payment_number, payment_date, amount, invoice_number, user_id=None):
    """
    Create journal entry for payment received (A/R collection).
    DR Cash (1110)
    CR Accounts Receivable (1120)
    """
    lines = [
        {'account_code': GL_ACCOUNTS['CASH'], 'debit': amount, 'credit': 0, 'description': f'Payment {payment_number} for Invoice {invoice_number}'},
        {'account_code': GL_ACCOUNTS['AR'], 'debit': 0, 'credit': amount, 'description': f'Payment {payment_number} - A/R reduction'},
    ]
    
    return create_journal_entry(
        conn=conn,
        entry_date=payment_date,
        description=f"Payment Received {payment_number} for Invoice {invoice_number}",
        transaction_source='Payment Received',
        reference_type='payment',
        reference_id=payment_id,
        lines=lines,
        user_id=user_id
    )


def create_vendor_invoice_entry(conn, vendor_invoice_id, invoice_number, invoice_date, total_amount, is_inventory=True, user_id=None):
    """
    Create journal entry for vendor invoice (A/P).
    DR Inventory (1130) or Expense
    CR Accounts Payable (2110)
    """
    debit_account = GL_ACCOUNTS['INVENTORY'] if is_inventory else GL_ACCOUNTS['OVERHEAD']
    debit_desc = 'Inventory' if is_inventory else 'Expense'
    
    lines = [
        {'account_code': debit_account, 'debit': total_amount, 'credit': 0, 'description': f'Vendor Invoice {invoice_number} - {debit_desc}'},
        {'account_code': GL_ACCOUNTS['AP'], 'debit': 0, 'credit': total_amount, 'description': f'Vendor Invoice {invoice_number} - A/P'},
    ]
    
    return create_journal_entry(
        conn=conn,
        entry_date=invoice_date,
        description=f"Vendor Invoice {invoice_number}",
        transaction_source='Vendor Invoice',
        reference_type='vendor_invoice',
        reference_id=vendor_invoice_id,
        lines=lines,
        user_id=user_id
    )


def create_payment_made_entry(conn, payment_id, payment_number, payment_date, amount, vendor_invoice_number, user_id=None):
    """
    Create journal entry for payment made (A/P disbursement).
    DR Accounts Payable (2110)
    CR Cash (1110)
    """
    lines = [
        {'account_code': GL_ACCOUNTS['AP'], 'debit': amount, 'credit': 0, 'description': f'Payment {payment_number} for Vendor Invoice {vendor_invoice_number}'},
        {'account_code': GL_ACCOUNTS['CASH'], 'debit': 0, 'credit': amount, 'description': f'Payment {payment_number} - Cash disbursement'},
    ]
    
    return create_journal_entry(
        conn=conn,
        entry_date=payment_date,
        description=f"Payment Made {payment_number} for Vendor Invoice {vendor_invoice_number}",
        transaction_source='Payment Made',
        reference_type='payment',
        reference_id=payment_id,
        lines=lines,
        user_id=user_id
    )


def create_wo_material_issue_entry(conn, wo_id, wo_number, issue_date, material_cost, user_id=None):
    """
    Create journal entry for work order material issuance.
    DR WIP (1140)
    CR Inventory (1130)
    """
    if not material_cost or material_cost <= 0:
        return None
    
    lines = [
        {'account_code': GL_ACCOUNTS['WIP'], 'debit': material_cost, 'credit': 0, 'description': f'WO {wo_number} - Material to WIP'},
        {'account_code': GL_ACCOUNTS['INVENTORY'], 'debit': 0, 'credit': material_cost, 'description': f'WO {wo_number} - Inventory relief'},
    ]
    
    return create_journal_entry(
        conn=conn,
        entry_date=issue_date,
        description=f"Material Issue for Work Order {wo_number}",
        transaction_source='Material Issue',
        reference_type='work_order',
        reference_id=wo_id,
        lines=lines,
        user_id=user_id
    )


def create_wo_labor_entry(conn, wo_id, wo_number, entry_date, labor_cost, user_id=None):
    """
    Create journal entry for work order labor.
    DR WIP (1140)
    CR Wages Payable (2150)
    """
    if not labor_cost or labor_cost <= 0:
        return None
    
    lines = [
        {'account_code': GL_ACCOUNTS['WIP'], 'debit': labor_cost, 'credit': 0, 'description': f'WO {wo_number} - Labor to WIP'},
        {'account_code': GL_ACCOUNTS['WAGES_PAYABLE'], 'debit': 0, 'credit': labor_cost, 'description': f'WO {wo_number} - Wages payable'},
    ]
    
    return create_journal_entry(
        conn=conn,
        entry_date=entry_date,
        description=f"Labor for Work Order {wo_number}",
        transaction_source='Labor Entry',
        reference_type='work_order',
        reference_id=wo_id,
        lines=lines,
        user_id=user_id
    )


def create_wo_completion_entry(conn, wo_id, wo_number, completion_date, total_wip_cost, user_id=None):
    """
    Create journal entry for work order completion (WIP to COGS).
    DR Cost of Goods Sold (5100/5200/5300)
    CR WIP (1140)
    """
    if not total_wip_cost or total_wip_cost <= 0:
        return None
    
    lines = [
        {'account_code': GL_ACCOUNTS['MATERIAL_COST'], 'debit': total_wip_cost, 'credit': 0, 'description': f'WO {wo_number} - WIP to COGS'},
        {'account_code': GL_ACCOUNTS['WIP'], 'debit': 0, 'credit': total_wip_cost, 'description': f'WO {wo_number} - WIP relief'},
    ]
    
    return create_journal_entry(
        conn=conn,
        entry_date=completion_date,
        description=f"Work Order {wo_number} Completion",
        transaction_source='Work Order Completion',
        reference_type='work_order',
        reference_id=wo_id,
        lines=lines,
        user_id=user_id
    )


def create_inventory_adjustment_entry(conn, adjustment_id, adjustment_date, product_name, quantity_change, value_change, reason, user_id=None):
    """
    Create journal entry for inventory adjustment.
    Increase: DR Inventory, CR Inventory Adjustment (Other Income)
    Decrease: DR Inventory Adjustment (Expense), CR Inventory
    """
    if value_change > 0:
        lines = [
            {'account_code': GL_ACCOUNTS['INVENTORY'], 'debit': abs(value_change), 'credit': 0, 'description': f'Adjustment - {product_name} (+{quantity_change})'},
            {'account_code': 4300, 'debit': 0, 'credit': abs(value_change), 'description': f'Adjustment - {reason}'},
        ]
    else:
        lines = [
            {'account_code': GL_ACCOUNTS['OVERHEAD'], 'debit': abs(value_change), 'credit': 0, 'description': f'Adjustment - {reason}'},
            {'account_code': GL_ACCOUNTS['INVENTORY'], 'debit': 0, 'credit': abs(value_change), 'description': f'Adjustment - {product_name} ({quantity_change})'},
        ]
    
    return create_journal_entry(
        conn=conn,
        entry_date=adjustment_date,
        description=f"Inventory Adjustment - {product_name}",
        transaction_source='Inventory Adjustment',
        reference_type='inventory_adjustment',
        reference_id=adjustment_id,
        lines=lines,
        user_id=user_id
    )
