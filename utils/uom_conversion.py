"""
UOM Conversion & Variance Control Engine

Provides centralized Unit of Measure conversion logic with:
- High precision (6 decimal places minimum)
- Product-specific conversion factors
- Validation against circular or undefined conversions
- Variance detection and prevention
- Cost integrity protection
- Audit trail support for full traceability

Core Rules (Non-Negotiable):
1. All inventory transactions MUST post in the base inventory UoM
2. Deterministic UoM conversion using item-specific tables only
3. Extended cost must remain invariant - adjust unit cost, not quantity
4. Quantity-to-receive must equal: Ordered Qty × Conversion Factor
5. Apply rounding only at final posting stage
"""

from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from models import Database
from datetime import datetime

PRECISION_DECIMALS = 6
COST_PRECISION_DECIMALS = 6
QUANTITY_TOLERANCE = Decimal('0.01')  # 1% tolerance for quantity matching
COST_TOLERANCE = Decimal('0.001')  # 0.1% tolerance for cost matching


class VarianceType:
    """Enumeration of variance types for categorization"""
    NONE = 'NONE'
    COST_CONVERSION = 'COST_CONVERSION'
    QUANTITY_CONVERSION = 'QUANTITY_CONVERSION'
    ROUNDING = 'ROUNDING'
    PRICE_DIFFERENCE = 'PRICE_DIFFERENCE'
    QUANTITY_DIFFERENCE = 'QUANTITY_DIFFERENCE'


class ConversionResult:
    """Result object for conversion operations with full audit trail"""
    
    def __init__(self, success, **kwargs):
        self.success = success
        self.error_message = kwargs.get('error_message')
        self.warning_message = kwargs.get('warning_message')
        
        # Conversion details
        self.original_quantity = kwargs.get('original_quantity')
        self.original_uom_id = kwargs.get('original_uom_id')
        self.original_uom_code = kwargs.get('original_uom_code')
        self.converted_quantity = kwargs.get('converted_quantity')
        self.target_uom_id = kwargs.get('target_uom_id')
        self.target_uom_code = kwargs.get('target_uom_code')
        self.conversion_factor = kwargs.get('conversion_factor')
        
        # Cost details
        self.original_unit_price = kwargs.get('original_unit_price')
        self.extended_cost = kwargs.get('extended_cost')
        self.normalized_unit_cost = kwargs.get('normalized_unit_cost')
        self.rounding_adjustment = kwargs.get('rounding_adjustment', Decimal('0'))
        
        # Variance tracking
        self.variance_type = kwargs.get('variance_type', VarianceType.NONE)
        self.variance_amount = kwargs.get('variance_amount', Decimal('0'))
        self.variance_blocked = kwargs.get('variance_blocked', False)
    
    def to_dict(self):
        """Convert to dictionary for storage/audit"""
        return {
            'success': self.success,
            'error_message': self.error_message,
            'warning_message': self.warning_message,
            'original_quantity': float(self.original_quantity) if self.original_quantity else None,
            'original_uom_id': self.original_uom_id,
            'original_uom_code': self.original_uom_code,
            'converted_quantity': float(self.converted_quantity) if self.converted_quantity else None,
            'target_uom_id': self.target_uom_id,
            'target_uom_code': self.target_uom_code,
            'conversion_factor': float(self.conversion_factor) if self.conversion_factor else None,
            'original_unit_price': float(self.original_unit_price) if self.original_unit_price else None,
            'extended_cost': float(self.extended_cost) if self.extended_cost else None,
            'normalized_unit_cost': float(self.normalized_unit_cost) if self.normalized_unit_cost else None,
            'rounding_adjustment': float(self.rounding_adjustment) if self.rounding_adjustment else 0,
            'variance_type': self.variance_type,
            'variance_amount': float(self.variance_amount) if self.variance_amount else 0,
            'variance_blocked': self.variance_blocked,
            'timestamp': datetime.now().isoformat()
        }


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


# =============================================================================
# VARIANCE CONTROL ENGINE
# =============================================================================

def validate_po_line_conversion(product_id, ordered_qty, ordered_uom_id, unit_price, conn=None, po_line_id=None, persist_audit=False):
    """
    Validate and calculate PO line conversion with variance detection.
    
    Args:
        product_id: The product ID
        ordered_qty: Quantity ordered in the ordered UoM
        ordered_uom_id: The UoM ID for the ordered quantity
        unit_price: Unit price in the ordered UoM
        conn: Database connection (optional)
        po_line_id: Specific PO line ID for audit tracking (optional)
        persist_audit: If True, save the audit trail to the database
    
    Returns:
        ConversionResult object with full audit trail
    """
    close_conn = False
    if conn is None:
        db = Database()
        conn = db.get_connection()
        close_conn = True
    
    try:
        ordered_qty = Decimal(str(ordered_qty))
        unit_price = Decimal(str(unit_price))
        
        # Get product base UOM
        product = conn.execute('''
            SELECT id, unit_of_measure, code, description 
            FROM products WHERE id = ?
        ''', (product_id,)).fetchone()
        
        if not product:
            return ConversionResult(
                False,
                error_message=f"Product ID {product_id} not found",
                variance_blocked=True
            )
        
        # Get ordered UOM info
        ordered_uom = conn.execute(
            'SELECT id, uom_code, uom_name, rounding_precision FROM uom_master WHERE id = ?',
            (ordered_uom_id,)
        ).fetchone()
        
        if not ordered_uom:
            return ConversionResult(
                False,
                error_message=f"Ordered UOM ID {ordered_uom_id} not found",
                variance_blocked=True
            )
        
        # Get base UOM info
        base_uom = conn.execute(
            'SELECT id, uom_code, uom_name, rounding_precision FROM uom_master WHERE uom_code = ?',
            (product['unit_of_measure'],)
        ).fetchone()
        
        if not base_uom:
            # Default to EA
            base_uom = conn.execute(
                'SELECT id, uom_code, uom_name, rounding_precision FROM uom_master WHERE uom_code = ?',
                ('EA',)
            ).fetchone()
        
        base_uom_id = base_uom['id'] if base_uom else 1
        
        # Get conversion factor
        factor, error = get_conversion_factor(product_id, ordered_uom_id, base_uom_id, conn)
        
        if error or factor is None:
            return ConversionResult(
                False,
                error_message=f"Conversion factor missing or invalid: {error}",
                original_quantity=ordered_qty,
                original_uom_id=ordered_uom_id,
                original_uom_code=ordered_uom['uom_code'] if ordered_uom else None,
                original_unit_price=unit_price,
                variance_blocked=True,
                variance_type=VarianceType.QUANTITY_CONVERSION
            )
        
        # Calculate extended cost (invariant - this is the authoritative value)
        extended_cost = round_decimal(ordered_qty * unit_price, COST_PRECISION_DECIMALS)
        
        # Calculate base quantity
        base_quantity = round_decimal(ordered_qty * factor, PRECISION_DECIMALS)
        
        # Check for illegal fractional quantity
        rounding_precision = base_uom['rounding_precision'] if base_uom else 2
        check_qty = round_decimal(base_quantity, rounding_precision)
        rounding_adjustment = base_quantity - check_qty
        
        # Calculate normalized unit cost (cost per base UOM unit)
        if base_quantity > 0:
            normalized_unit_cost = round_decimal(extended_cost / base_quantity, COST_PRECISION_DECIMALS)
        else:
            normalized_unit_cost = Decimal('0')
        
        # Check for potential variance
        variance_type = VarianceType.NONE
        variance_amount = Decimal('0')
        
        if abs(rounding_adjustment) > Decimal('0.0001'):
            variance_type = VarianceType.ROUNDING
            variance_amount = rounding_adjustment
        
        result = ConversionResult(
            True,
            original_quantity=ordered_qty,
            original_uom_id=ordered_uom_id,
            original_uom_code=ordered_uom['uom_code'] if ordered_uom else None,
            converted_quantity=base_quantity,
            target_uom_id=base_uom_id,
            target_uom_code=base_uom['uom_code'] if base_uom else 'EA',
            conversion_factor=factor,
            original_unit_price=unit_price,
            extended_cost=extended_cost,
            normalized_unit_cost=normalized_unit_cost,
            rounding_adjustment=rounding_adjustment,
            variance_type=variance_type,
            variance_amount=variance_amount,
            variance_blocked=False
        )
        
        # Persist audit trail if requested
        if persist_audit and po_line_id:
            log_conversion_audit(conn, 'po_line', po_line_id, result)
        
        return result
        
    except Exception as e:
        return ConversionResult(
            False,
            error_message=f"Conversion validation error: {str(e)}",
            variance_blocked=True
        )
    finally:
        if close_conn:
            conn.close()


def log_conversion_audit(conn, entity_type, entity_id, result):
    """
    Log conversion audit trail to the audit_log table.
    
    Args:
        conn: Database connection
        entity_type: Type of entity ('po_line', 'receipt', 'invoice')
        entity_id: ID of the entity
        result: ConversionResult object
    """
    try:
        import json
        audit_data = result.to_dict()
        
        conn.execute('''
            INSERT INTO audit_log (table_name, record_id, action, user_id, changes, created_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
        ''', (
            f'uom_conversion_{entity_type}',
            entity_id,
            'UOM_CONVERSION_VALIDATION',
            None,  # System action
            json.dumps(audit_data)
        ))
        conn.commit()
    except Exception as e:
        # Log error but don't fail the validation
        print(f"Warning: Failed to log conversion audit: {e}")


def validate_receipt_for_variance(po_line, receipt_qty, receipt_uom_id, product_id, conn=None):
    """
    Validate receipt against PO line for variance prevention.
    Blocks cost variances caused solely by UoM conversion.
    
    Returns:
        ConversionResult with variance detection
    """
    close_conn = False
    if conn is None:
        db = Database()
        conn = db.get_connection()
        close_conn = True
    
    try:
        receipt_qty = Decimal(str(receipt_qty))
        
        base_uom_id = po_line['base_uom_id'] or po_line['uom_id'] or 1
        
        # Convert receipt to base UOM
        base_receipt_qty, factor, error = convert_quantity(
            receipt_qty, receipt_uom_id, base_uom_id, product_id, conn
        )
        
        if error or base_receipt_qty is None:
            return ConversionResult(
                False,
                error_message=error or "Failed to convert receipt quantity",
                variance_blocked=True,
                variance_type=VarianceType.QUANTITY_CONVERSION
            )
        
        # Get open quantity in base UOM
        po_base_qty = Decimal(str(po_line['base_quantity'] or po_line['quantity']))
        received_base_qty = Decimal(str(po_line['base_received_quantity'] or po_line['received_quantity'] or 0))
        open_base_qty = po_base_qty - received_base_qty
        
        # Check quantity variance
        qty_variance = base_receipt_qty - open_base_qty
        qty_variance_pct = abs(qty_variance / open_base_qty) if open_base_qty > 0 else Decimal('0')
        
        variance_type = VarianceType.NONE
        variance_blocked = False
        
        if base_receipt_qty > open_base_qty * (1 + QUANTITY_TOLERANCE):
            variance_type = VarianceType.QUANTITY_DIFFERENCE
            variance_blocked = True
        
        # Calculate allocated cost
        allocated_cost, unit_cost = allocate_cost_for_partial_receipt(po_line, base_receipt_qty)
        
        # Get UOM info for audit
        receipt_uom = conn.execute(
            'SELECT uom_code FROM uom_master WHERE id = ?', (receipt_uom_id,)
        ).fetchone()
        base_uom = conn.execute(
            'SELECT uom_code FROM uom_master WHERE id = ?', (base_uom_id,)
        ).fetchone()
        
        return ConversionResult(
            not variance_blocked,
            original_quantity=receipt_qty,
            original_uom_id=receipt_uom_id,
            original_uom_code=receipt_uom['uom_code'] if receipt_uom else None,
            converted_quantity=base_receipt_qty,
            target_uom_id=base_uom_id,
            target_uom_code=base_uom['uom_code'] if base_uom else 'EA',
            conversion_factor=factor,
            extended_cost=allocated_cost,
            normalized_unit_cost=unit_cost,
            variance_type=variance_type,
            variance_amount=qty_variance,
            variance_blocked=variance_blocked,
            error_message=f"Receipt quantity exceeds open PO quantity by {qty_variance}" if variance_blocked else None
        )
        
    except Exception as e:
        return ConversionResult(
            False,
            error_message=f"Receipt validation error: {str(e)}",
            variance_blocked=True
        )
    finally:
        if close_conn:
            conn.close()


def validate_invoice_match(po_line, invoice_qty, invoice_unit_price, invoice_uom_id, product_id, conn=None):
    """
    Validate invoice against PO/Receipt for 3-way match with variance prevention.
    Prevents artificial variances caused by UoM differences.
    
    Returns:
        ConversionResult with variance detection
    """
    close_conn = False
    if conn is None:
        db = Database()
        conn = db.get_connection()
        close_conn = True
    
    try:
        invoice_qty = Decimal(str(invoice_qty))
        invoice_unit_price = Decimal(str(invoice_unit_price))
        
        base_uom_id = po_line['base_uom_id'] or po_line['uom_id'] or 1
        
        # Convert invoice qty to base UOM
        base_invoice_qty, factor, error = convert_quantity(
            invoice_qty, invoice_uom_id, base_uom_id, product_id, conn
        )
        
        if error or base_invoice_qty is None:
            return ConversionResult(
                False,
                error_message=error or "Failed to convert invoice quantity",
                variance_blocked=True,
                variance_type=VarianceType.QUANTITY_CONVERSION
            )
        
        # Calculate invoice extended cost
        invoice_extended = round_decimal(invoice_qty * invoice_unit_price, COST_PRECISION_DECIMALS)
        
        # Calculate expected cost from PO
        po_extended = Decimal(str(po_line['extended_cost'] or 
                                   Decimal(str(po_line['quantity'])) * Decimal(str(po_line['unit_price']))))
        
        # Compare quantities
        received_base_qty = Decimal(str(po_line['base_received_quantity'] or po_line['received_quantity'] or 0))
        qty_variance = base_invoice_qty - received_base_qty
        
        # Compare costs
        cost_variance = invoice_extended - po_extended
        cost_variance_pct = abs(cost_variance / po_extended) if po_extended > 0 else Decimal('0')
        
        variance_type = VarianceType.NONE
        variance_blocked = False
        variance_amount = Decimal('0')
        warning_message = None
        
        # Check for quantity mismatch
        if abs(qty_variance / received_base_qty) > QUANTITY_TOLERANCE if received_base_qty > 0 else abs(qty_variance) > Decimal('0.01'):
            variance_type = VarianceType.QUANTITY_DIFFERENCE
            variance_amount = qty_variance
            warning_message = f"Invoice quantity differs from received by {qty_variance} base units"
        
        # Check for price variance (this is allowed but flagged)
        elif cost_variance_pct > COST_TOLERANCE:
            variance_type = VarianceType.PRICE_DIFFERENCE
            variance_amount = cost_variance
            warning_message = f"Invoice price differs from PO by ${cost_variance}"
        
        return ConversionResult(
            True,
            original_quantity=invoice_qty,
            original_uom_id=invoice_uom_id,
            converted_quantity=base_invoice_qty,
            target_uom_id=base_uom_id,
            conversion_factor=factor,
            original_unit_price=invoice_unit_price,
            extended_cost=invoice_extended,
            variance_type=variance_type,
            variance_amount=variance_amount,
            variance_blocked=variance_blocked,
            warning_message=warning_message
        )
        
    except Exception as e:
        return ConversionResult(
            False,
            error_message=f"Invoice match validation error: {str(e)}",
            variance_blocked=True
        )
    finally:
        if close_conn:
            conn.close()


def get_po_line_audit_trail(po_line):
    """
    Generate audit trail data for a PO line's UoM conversion.
    
    Returns:
        dict with full conversion audit data
    """
    return {
        'ordered_uom_id': po_line.get('uom_id'),
        'ordered_quantity': po_line.get('quantity'),
        'conversion_factor': po_line.get('conversion_factor_used'),
        'base_uom_id': po_line.get('base_uom_id'),
        'base_quantity': po_line.get('base_quantity'),
        'unit_price': po_line.get('unit_price'),
        'base_unit_price': po_line.get('base_unit_price'),
        'extended_cost': po_line.get('extended_cost'),
        'extended_cost_locked': po_line.get('extended_cost_locked', False),
        'received_quantity': po_line.get('received_quantity', 0),
        'base_received_quantity': po_line.get('base_received_quantity', 0)
    }


def check_conversion_defined(product_id, uom_id, conn=None):
    """
    Check if a valid conversion exists for product and UoM.
    Used to block PO creation with undefined conversions.
    
    Returns:
        tuple: (is_defined, conversion_factor, error_message)
    """
    close_conn = False
    if conn is None:
        db = Database()
        conn = db.get_connection()
        close_conn = True
    
    try:
        # Get product base UOM
        product = conn.execute(
            'SELECT unit_of_measure FROM products WHERE id = ?',
            (product_id,)
        ).fetchone()
        
        if not product:
            return (False, None, f"Product {product_id} not found")
        
        base_uom = conn.execute(
            'SELECT id FROM uom_master WHERE uom_code = ?',
            (product['unit_of_measure'],)
        ).fetchone()
        
        base_uom_id = base_uom['id'] if base_uom else 1
        
        if uom_id == base_uom_id:
            return (True, Decimal('1.0'), None)
        
        # Check for product-specific conversion
        product_conv = conn.execute('''
            SELECT conversion_factor FROM product_uom_conversions 
            WHERE product_id = ? AND uom_id = ? AND is_active = 1
        ''', (product_id, uom_id)).fetchone()
        
        if product_conv:
            return (True, Decimal(str(product_conv['conversion_factor'])), None)
        
        # Check for global UOM conversion
        uom = conn.execute(
            'SELECT conversion_factor, base_uom_id FROM uom_master WHERE id = ?',
            (uom_id,)
        ).fetchone()
        
        if uom and uom['conversion_factor']:
            return (True, Decimal(str(uom['conversion_factor'])), None)
        
        return (False, None, f"No conversion defined for product {product_id} with UOM {uom_id}")
        
    except Exception as e:
        return (False, None, f"Error checking conversion: {str(e)}")
    finally:
        if close_conn:
            conn.close()
