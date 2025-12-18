"""
UOM Conversion Utility Module

Provides centralized Unit of Measure conversion logic with:
- High precision (6 decimal places minimum)
- Product-specific conversion factors
- Validation against circular or undefined conversions
- Audit trail support
"""

from decimal import Decimal, ROUND_HALF_UP
from models import Database

PRECISION_DECIMALS = 6
COST_PRECISION_DECIMALS = 6


def get_conversion_factor(product_id, from_uom_id, to_uom_id, conn=None):
    """
    Get the conversion factor to convert from one UOM to another for a specific product.
    
    Returns:
        tuple: (conversion_factor, error_message)
        - If successful: (factor, None)
        - If failed: (None, error_message)
    """
    if from_uom_id == to_uom_id:
        return (Decimal('1.0'), None)
    
    close_conn = False
    if conn is None:
        db = Database()
        conn = db.get_connection()
        close_conn = True
    
    try:
        from_uom = conn.execute(
            'SELECT id, uom_code, base_uom_id, conversion_factor FROM uom_master WHERE id = ?',
            (from_uom_id,)
        ).fetchone()
        
        to_uom = conn.execute(
            'SELECT id, uom_code, base_uom_id, conversion_factor FROM uom_master WHERE id = ?',
            (to_uom_id,)
        ).fetchone()
        
        if not from_uom or not to_uom:
            return (None, f"UOM not found: from={from_uom_id}, to={to_uom_id}")
        
        product_from_conv = conn.execute('''
            SELECT conversion_factor, is_base_uom 
            FROM product_uom_conversions 
            WHERE product_id = ? AND uom_id = ? AND is_active = 1
            ORDER BY version_number DESC LIMIT 1
        ''', (product_id, from_uom_id)).fetchone()
        
        product_to_conv = conn.execute('''
            SELECT conversion_factor, is_base_uom 
            FROM product_uom_conversions 
            WHERE product_id = ? AND uom_id = ? AND is_active = 1
            ORDER BY version_number DESC LIMIT 1
        ''', (product_id, to_uom_id)).fetchone()
        
        product = conn.execute(
            'SELECT unit_of_measure FROM products WHERE id = ?',
            (product_id,)
        ).fetchone()
        
        base_uom = conn.execute(
            'SELECT id FROM uom_master WHERE uom_code = ?',
            (product['unit_of_measure'] if product else 'EA',)
        ).fetchone()
        base_uom_id = base_uom['id'] if base_uom else 1
        
        from_factor = Decimal('1.0')
        to_factor = Decimal('1.0')
        
        if product_from_conv:
            from_factor = Decimal(str(product_from_conv['conversion_factor']))
        elif from_uom['conversion_factor']:
            from_factor = Decimal(str(from_uom['conversion_factor']))
        
        if product_to_conv:
            to_factor = Decimal(str(product_to_conv['conversion_factor']))
        elif to_uom['conversion_factor']:
            to_factor = Decimal(str(to_uom['conversion_factor']))
        
        if to_factor == 0:
            return (None, f"Invalid conversion factor (zero) for UOM {to_uom['uom_code']}")
        
        conversion_factor = from_factor / to_factor
        
        return (round_decimal(conversion_factor, PRECISION_DECIMALS), None)
        
    except Exception as e:
        return (None, f"Conversion error: {str(e)}")
    finally:
        if close_conn:
            conn.close()


def convert_quantity(quantity, from_uom_id, to_uom_id, product_id, conn=None):
    """
    Convert a quantity from one UOM to another for a specific product.
    
    Returns:
        tuple: (converted_quantity, conversion_factor, error_message)
    """
    quantity = Decimal(str(quantity))
    
    factor, error = get_conversion_factor(product_id, from_uom_id, to_uom_id, conn)
    if error:
        return (None, None, error)
    
    converted = quantity * factor
    return (round_decimal(converted, PRECISION_DECIMALS), factor, None)


def calculate_base_unit_cost(extended_cost, base_quantity):
    """
    Calculate unit cost in base UOM from extended cost and base quantity.
    Extended cost must remain invariant - this derives base unit cost.
    
    Formula: Base Unit Cost = Extended Cost / Base Quantity
    """
    if base_quantity is None or base_quantity == 0:
        return Decimal('0')
    
    extended = Decimal(str(extended_cost))
    qty = Decimal(str(base_quantity))
    
    base_unit_cost = extended / qty
    return round_decimal(base_unit_cost, COST_PRECISION_DECIMALS)


def calculate_extended_cost(quantity, unit_price):
    """
    Calculate extended cost from ordered quantity and unit price.
    This is the authoritative cost that should never change due to UOM conversion.
    
    Formula: Extended Cost = Ordered Quantity x Unit Price
    """
    qty = Decimal(str(quantity))
    price = Decimal(str(unit_price))
    
    extended = qty * price
    return round_decimal(extended, COST_PRECISION_DECIMALS)


def validate_receipt_quantity(receipt_qty, receipt_uom_id, po_line, product_id, conn=None):
    """
    Validate that a receipt quantity doesn't exceed the open PO quantity.
    
    Returns:
        tuple: (is_valid, base_receipt_qty, conversion_factor, error_message)
    """
    close_conn = False
    if conn is None:
        db = Database()
        conn = db.get_connection()
        close_conn = True
    
    try:
        base_uom_id = po_line['base_uom_id'] or po_line['uom_id'] or 1
        
        base_receipt_qty, factor, error = convert_quantity(
            receipt_qty, receipt_uom_id, base_uom_id, product_id, conn
        )
        
        if error or base_receipt_qty is None:
            return (False, None, None, error or "Failed to convert receipt quantity")
        
        open_base_qty = Decimal(str(po_line['base_quantity'] or po_line['quantity'])) - \
                        Decimal(str(po_line['base_received_quantity'] or po_line['received_quantity'] or 0))
        
        tolerance = Decimal('0.01')
        
        if base_receipt_qty > (open_base_qty + open_base_qty * tolerance):  # type: ignore
            return (
                False, 
                base_receipt_qty, 
                factor,
                f"Receipt quantity ({base_receipt_qty}) exceeds open PO quantity ({open_base_qty}) in base UOM"
            )
        
        return (True, base_receipt_qty, factor, None)
        
    except Exception as e:
        return (False, None, None, f"Validation error: {str(e)}")
    finally:
        if close_conn:
            conn.close()


def allocate_cost_for_partial_receipt(po_line, base_receipt_qty):
    """
    Allocate cost proportionally for a partial receipt.
    Ensures extended cost is distributed correctly without variance.
    
    Returns:
        tuple: (allocated_cost, unit_cost_at_receipt)
    """
    extended_cost = Decimal(str(po_line['extended_cost'] or 
                                Decimal(str(po_line['quantity'])) * Decimal(str(po_line['unit_price']))))
    base_quantity = Decimal(str(po_line['base_quantity'] or po_line['quantity']))
    receipt_qty = Decimal(str(base_receipt_qty))
    
    if base_quantity == 0:
        return (Decimal('0'), Decimal('0'))
    
    proportion = receipt_qty / base_quantity
    allocated_cost = round_decimal(extended_cost * proportion, COST_PRECISION_DECIMALS)
    unit_cost = round_decimal(extended_cost / base_quantity, COST_PRECISION_DECIMALS)
    
    return (allocated_cost, unit_cost)


def round_decimal(value, decimal_places):
    """Round a Decimal value to specified decimal places."""
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    
    quantize_str = '0.' + '0' * decimal_places
    return value.quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP)


def get_product_uom_options(product_id, conn=None):
    """
    Get all valid UOM options for a product (base + alternate UOMs).
    
    Returns:
        list: List of dicts with uom_id, uom_code, uom_name, conversion_factor, is_base
    """
    close_conn = False
    if conn is None:
        db = Database()
        conn = db.get_connection()
        close_conn = True
    
    try:
        product = conn.execute(
            'SELECT unit_of_measure FROM products WHERE id = ?',
            (product_id,)
        ).fetchone()
        
        if not product:
            return []
        
        base_uom_code = product['unit_of_measure']
        
        base_uom = conn.execute(
            'SELECT id, uom_code, uom_name FROM uom_master WHERE uom_code = ?',
            (base_uom_code,)
        ).fetchone()
        
        uom_options = []
        
        if base_uom:
            uom_options.append({
                'uom_id': base_uom['id'],
                'uom_code': base_uom['uom_code'],
                'uom_name': base_uom['uom_name'],
                'conversion_factor': 1.0,
                'is_base': True
            })
        
        conversions = conn.execute('''
            SELECT puc.*, um.uom_code, um.uom_name
            FROM product_uom_conversions puc
            JOIN uom_master um ON puc.uom_id = um.id
            WHERE puc.product_id = ? AND puc.is_active = 1
            ORDER BY um.uom_code
        ''', (product_id,)).fetchall()
        
        for conv in conversions:
            if not any(u['uom_id'] == conv['uom_id'] for u in uom_options):
                uom_options.append({
                    'uom_id': conv['uom_id'],
                    'uom_code': conv['uom_code'],
                    'uom_name': conv['uom_name'],
                    'conversion_factor': conv['conversion_factor'],
                    'is_base': conv['is_base_uom'] == 1
                })
        
        return uom_options
        
    except Exception as e:
        return []
    finally:
        if close_conn:
            conn.close()


def get_uom_display_info(uom_id, conn=None):
    """Get UOM code and name for display purposes."""
    close_conn = False
    if conn is None:
        db = Database()
        conn = db.get_connection()
        close_conn = True
    
    try:
        uom = conn.execute(
            'SELECT uom_code, uom_name FROM uom_master WHERE id = ?',
            (uom_id,)
        ).fetchone()
        
        if uom:
            return {'uom_code': uom['uom_code'], 'uom_name': uom['uom_name']}
        return {'uom_code': 'EA', 'uom_name': 'Each'}
        
    finally:
        if close_conn:
            conn.close()
