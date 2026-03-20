from models import Database
from datetime import datetime, date
import json
import logging

logger = logging.getLogger(__name__)


class TraceabilityEngine:

    @staticmethod
    def search(query_type, query_value):
        if not query_value or not query_value.strip():
            return []
        query_value = query_value.strip()
        db = Database()

        if query_type == 'part_number':
            return TraceabilityEngine._trace_by_part(db, query_value)
        elif query_type == 'serial_number':
            return TraceabilityEngine._trace_by_serial(db, query_value)
        elif query_type == 'lot_number':
            return TraceabilityEngine._trace_by_lot(db, query_value)
        elif query_type == 'work_order':
            return TraceabilityEngine._trace_by_work_order(db, query_value)
        elif query_type == 'purchase_order':
            return TraceabilityEngine._trace_by_purchase_order(db, query_value)
        elif query_type == 'sales_order':
            return TraceabilityEngine._trace_by_sales_order(db, query_value)
        else:
            return TraceabilityEngine._trace_by_part(db, query_value)

    @staticmethod
    def _safe_row(row, keys):
        d = {}
        for i, k in enumerate(keys):
            v = row[i] if i < len(row) else None
            if isinstance(v, (datetime, date)):
                v = v.isoformat()
            elif hasattr(v, '__float__'):
                try:
                    v = float(v)
                except:
                    v = str(v)
            d[k] = v
        return d

    @staticmethod
    def _trace_by_part(db, part_number):
        events = []
        try:
            rows = db.execute_query("""
                SELECT p.id, p.code, p.name, p.manufacturer, p.product_type, p.product_category
                FROM products p WHERE UPPER(p.code) = UPPER(%s)
            """, (part_number,))
            if not rows:
                rows = db.execute_query("""
                    SELECT p.id, p.code, p.name, p.manufacturer, p.product_type, p.product_category
                    FROM products p WHERE UPPER(p.code) LIKE UPPER(%s)
                """, (f'%{part_number}%',))
            if not rows:
                return events
            product_ids = [r[0] for r in rows]
            for r in rows:
                events.append({
                    'module': 'Product Master',
                    'event_type': 'Product Record',
                    'timestamp': None,
                    'description': f'Part {r[1]} - {r[2]}',
                    'details': {'manufacturer': r[3], 'type': r[4], 'category': r[5]},
                    'entity_id': r[0],
                    'entity_ref': r[1],
                    'icon': 'bi-box-seam'
                })
            for pid in product_ids:
                events.extend(TraceabilityEngine._get_inventory_events(db, pid))
                events.extend(TraceabilityEngine._get_receiving_events(db, pid))
                events.extend(TraceabilityEngine._get_issuance_events(db, pid))
                events.extend(TraceabilityEngine._get_return_events(db, pid))
                events.extend(TraceabilityEngine._get_adjustment_events(db, pid))
                events.extend(TraceabilityEngine._get_wo_events_by_product(db, pid))
                events.extend(TraceabilityEngine._get_po_events_by_product(db, pid))
                events.extend(TraceabilityEngine._get_so_events_by_product(db, pid))
                events.extend(TraceabilityEngine._get_shipment_events_by_product(db, pid))
                events.extend(TraceabilityEngine._get_quality_events_by_product(db, pid))
                events.extend(TraceabilityEngine._get_gl_events_by_product(db, pid))
                events.extend(TraceabilityEngine._get_bom_events(db, pid))
        except Exception as e:
            logger.error(f"Trace by part error: {e}")
        events.sort(key=lambda x: x.get('timestamp') or '0000-00-00')
        return events

    @staticmethod
    def _trace_by_serial(db, serial_number):
        events = []
        try:
            rows = db.execute_query("""
                SELECT i.id, i.product_id, i.serial_number, p.code, p.name, i.last_updated
                FROM inventory i JOIN products p ON i.product_id = p.id
                WHERE UPPER(i.serial_number) = UPPER(%s)
            """, (serial_number,))
            if not rows:
                rows = db.execute_query("""
                    SELECT i.id, i.product_id, i.serial_number, p.code, p.name, i.last_updated
                    FROM inventory i JOIN products p ON i.product_id = p.id
                    WHERE UPPER(i.serial_number) LIKE UPPER(%s)
                """, (f'%{serial_number}%',))
            if not rows:
                return events
            product_ids = set()
            for r in rows:
                product_ids.add(r[1])
                events.append({
                    'module': 'Inventory',
                    'event_type': 'Serial Record',
                    'timestamp': r[5].isoformat() if isinstance(r[5], (datetime, date)) else str(r[5]) if r[5] else None,
                    'description': f'Serial {r[2]} for Part {r[3]} - {r[4]}',
                    'details': {'inventory_id': r[0], 'product_id': r[1]},
                    'entity_id': r[0],
                    'entity_ref': r[2],
                    'icon': 'bi-upc-scan'
                })
            wo_rows = db.execute_query("""
                SELECT wo.id, wo.wo_number, wo.product_id, wo.status, wo.created_at, p.code
                FROM work_orders wo JOIN products p ON wo.product_id = p.id
                WHERE UPPER(wo.serial_number) = UPPER(%s)
            """, (serial_number,))
            for r in wo_rows:
                product_ids.add(r[2])
                events.append({
                    'module': 'Manufacturing',
                    'event_type': 'Work Order',
                    'timestamp': r[4].isoformat() if isinstance(r[4], (datetime, date)) else str(r[4]) if r[4] else None,
                    'description': f'WO {r[1]} - Status: {r[3]}',
                    'details': {'wo_id': r[0], 'part': r[5]},
                    'entity_id': r[0],
                    'entity_ref': r[1],
                    'icon': 'bi-gear'
                })
            rt_rows = db.execute_query("""
                SELECT rt.id, rt.receipt_number, rt.product_id, rt.quantity_received, rt.receipt_date,
                       rt.serial_number, p.code
                FROM receiving_transactions rt JOIN products p ON rt.product_id = p.id
                WHERE UPPER(rt.serial_number) = UPPER(%s)
            """, (serial_number,))
            for r in rt_rows:
                product_ids.add(r[2])
                events.append({
                    'module': 'Receiving',
                    'event_type': 'Receipt',
                    'timestamp': r[4].isoformat() if isinstance(r[4], (datetime, date)) else str(r[4]) if r[4] else None,
                    'description': f'Receipt {r[1]} - Qty: {r[3]} for Part {r[6]}',
                    'details': {'receipt_id': r[0]},
                    'entity_id': r[0],
                    'entity_ref': r[1],
                    'icon': 'bi-box-arrow-in-down'
                })
            sol_rows = db.execute_query("""
                SELECT sol.id, sol.so_id, so.so_number, sol.product_id, sol.quantity,
                       sol.serial_number, sol.created_at, p.code
                FROM sales_order_lines sol
                JOIN sales_orders so ON sol.so_id = so.id
                JOIN products p ON sol.product_id = p.id
                WHERE UPPER(sol.serial_number) = UPPER(%s)
            """, (serial_number,))
            for r in sol_rows:
                product_ids.add(r[3])
                events.append({
                    'module': 'Sales',
                    'event_type': 'Sales Order Line',
                    'timestamp': r[6].isoformat() if isinstance(r[6], (datetime, date)) else str(r[6]) if r[6] else None,
                    'description': f'SO {r[2]} - Part {r[7]} Qty: {r[4]}',
                    'details': {'so_id': r[1], 'line_id': r[0]},
                    'entity_id': r[1],
                    'entity_ref': r[2],
                    'icon': 'bi-cart'
                })
            for pid in product_ids:
                events.extend(TraceabilityEngine._get_issuance_events(db, pid))
                events.extend(TraceabilityEngine._get_return_events(db, pid))
                events.extend(TraceabilityEngine._get_quality_events_by_product(db, pid))
                events.extend(TraceabilityEngine._get_gl_events_by_product(db, pid))
        except Exception as e:
            logger.error(f"Trace by serial error: {e}")
        events.sort(key=lambda x: x.get('timestamp') or '0000-00-00')
        return events

    @staticmethod
    def _trace_by_lot(db, lot_number):
        events = []
        try:
            inv_rows = db.execute_query("""
                SELECT i.id, i.product_id, i.lot_number, i.quantity, i.serial_number,
                       p.code, p.name, i.last_updated
                FROM inventory i JOIN products p ON i.product_id = p.id
                WHERE UPPER(i.lot_number) = UPPER(%s)
            """, (lot_number,))
            product_ids = set()
            for r in inv_rows:
                product_ids.add(r[1])
                events.append({
                    'module': 'Inventory',
                    'event_type': 'Lot Record',
                    'timestamp': r[7].isoformat() if isinstance(r[7], (datetime, date)) else str(r[7]) if r[7] else None,
                    'description': f'Lot {r[2]} - Part {r[5]} ({r[6]}) Qty: {r[3]}',
                    'details': {'inventory_id': r[0], 'serial': r[4]},
                    'entity_id': r[0],
                    'entity_ref': r[2],
                    'icon': 'bi-collection'
                })
            rt_rows = db.execute_query("""
                SELECT rt.id, rt.receipt_number, rt.product_id, rt.quantity_received, rt.receipt_date,
                       rt.lot_number, p.code
                FROM receiving_transactions rt JOIN products p ON rt.product_id = p.id
                WHERE UPPER(rt.lot_number) = UPPER(%s)
            """, (lot_number,))
            for r in rt_rows:
                product_ids.add(r[2])
                events.append({
                    'module': 'Receiving',
                    'event_type': 'Lot Receipt',
                    'timestamp': r[4].isoformat() if isinstance(r[4], (datetime, date)) else str(r[4]) if r[4] else None,
                    'description': f'Receipt {r[1]} - Lot {r[5]} Part {r[6]} Qty: {r[3]}',
                    'details': {'receipt_id': r[0]},
                    'entity_id': r[0],
                    'entity_ref': r[1],
                    'icon': 'bi-box-arrow-in-down'
                })
            for pid in product_ids:
                events.extend(TraceabilityEngine._get_issuance_events(db, pid))
                events.extend(TraceabilityEngine._get_wo_events_by_product(db, pid))
                events.extend(TraceabilityEngine._get_quality_events_by_product(db, pid))
        except Exception as e:
            logger.error(f"Trace by lot error: {e}")
        events.sort(key=lambda x: x.get('timestamp') or '0000-00-00')
        return events

    @staticmethod
    def _trace_by_work_order(db, wo_number):
        events = []
        try:
            rows = db.execute_query("""
                SELECT wo.id, wo.wo_number, wo.product_id, wo.quantity, wo.status,
                       wo.serial_number, wo.created_at, wo.planned_start_date, wo.actual_end_date,
                       wo.material_cost, wo.labor_cost, wo.overhead_cost,
                       p.code, p.name, wo.so_id, wo.customer_id
                FROM work_orders wo LEFT JOIN products p ON wo.product_id = p.id
                WHERE UPPER(wo.wo_number) = UPPER(%s)
            """, (wo_number,))
            if not rows:
                rows = db.execute_query("""
                    SELECT wo.id, wo.wo_number, wo.product_id, wo.quantity, wo.status,
                           wo.serial_number, wo.created_at, wo.planned_start_date, wo.actual_end_date,
                           wo.material_cost, wo.labor_cost, wo.overhead_cost,
                           p.code, p.name, wo.so_id, wo.customer_id
                    FROM work_orders wo LEFT JOIN products p ON wo.product_id = p.id
                    WHERE UPPER(wo.wo_number) LIKE UPPER(%s)
                """, (f'%{wo_number}%',))
            if not rows:
                return events
            for r in rows:
                wo_id = r[0]
                events.append({
                    'module': 'Manufacturing',
                    'event_type': 'Work Order Created',
                    'timestamp': r[6].isoformat() if isinstance(r[6], (datetime, date)) else str(r[6]) if r[6] else None,
                    'description': f'WO {r[1]} - Part {r[12] or "N/A"} ({r[13] or "N/A"}) Qty: {r[3]} Status: {r[4]}',
                    'details': {
                        'serial': r[5], 'material_cost': float(r[9] or 0),
                        'labor_cost': float(r[10] or 0), 'overhead_cost': float(r[11] or 0),
                        'so_id': r[14], 'customer_id': r[15]
                    },
                    'entity_id': wo_id,
                    'entity_ref': r[1],
                    'icon': 'bi-gear'
                })
                events.extend(TraceabilityEngine._get_wo_material_issues(db, wo_id))
                events.extend(TraceabilityEngine._get_wo_returns(db, wo_id))
                events.extend(TraceabilityEngine._get_wo_labor(db, wo_id))
                events.extend(TraceabilityEngine._get_wo_stages(db, wo_id))
                if r[2]:
                    events.extend(TraceabilityEngine._get_po_events_by_product(db, r[2]))
                    events.extend(TraceabilityEngine._get_receiving_events(db, r[2]))
                    events.extend(TraceabilityEngine._get_gl_events_by_product(db, r[2]))
        except Exception as e:
            logger.error(f"Trace by WO error: {e}")
        events.sort(key=lambda x: x.get('timestamp') or '0000-00-00')
        return events

    @staticmethod
    def _trace_by_purchase_order(db, po_number):
        events = []
        try:
            rows = db.execute_query("""
                SELECT po.id, po.po_number, po.supplier_id, po.status, po.order_date,
                       po.expected_delivery_date, po.actual_delivery_date, po.work_order_id,
                       s.name as supplier_name
                FROM purchase_orders po LEFT JOIN suppliers s ON po.supplier_id = s.id
                WHERE UPPER(po.po_number) = UPPER(%s)
            """, (po_number,))
            if not rows:
                return events
            for r in rows:
                po_id = r[0]
                events.append({
                    'module': 'Procurement',
                    'event_type': 'Purchase Order',
                    'timestamp': r[4].isoformat() if isinstance(r[4], (datetime, date)) else str(r[4]) if r[4] else None,
                    'description': f'PO {r[1]} - Supplier: {r[8] or "N/A"} Status: {r[3]}',
                    'details': {'supplier_id': r[2], 'expected': str(r[5]) if r[5] else None, 'wo_id': r[7]},
                    'entity_id': po_id,
                    'entity_ref': r[1],
                    'icon': 'bi-receipt'
                })
                pol_rows = db.execute_query("""
                    SELECT pol.id, pol.product_id, pol.quantity, pol.unit_price, pol.received_quantity,
                           p.code, p.name
                    FROM purchase_order_lines pol LEFT JOIN products p ON pol.product_id = p.id
                    WHERE pol.po_id = %s
                """, (po_id,))
                for pl in pol_rows:
                    events.append({
                        'module': 'Procurement',
                        'event_type': 'PO Line',
                        'timestamp': r[4].isoformat() if isinstance(r[4], (datetime, date)) else str(r[4]) if r[4] else None,
                        'description': f'Part {pl[5] or "N/A"} Qty: {pl[2]} @ ${float(pl[3] or 0):.2f} Rcvd: {pl[4] or 0}',
                        'details': {'product_id': pl[1]},
                        'entity_id': pl[0],
                        'entity_ref': r[1],
                        'icon': 'bi-list-check'
                    })
                    if pl[1]:
                        events.extend(TraceabilityEngine._get_receiving_events(db, pl[1], po_id))
                rt_rows = db.execute_query("""
                    SELECT rt.id, rt.receipt_number, rt.product_id, rt.quantity_received,
                           rt.receipt_date, rt.serial_number, rt.lot_number, p.code
                    FROM receiving_transactions rt LEFT JOIN products p ON rt.product_id = p.id
                    WHERE rt.po_id = %s
                """, (po_id,))
                for rt in rt_rows:
                    events.append({
                        'module': 'Receiving',
                        'event_type': 'Receipt',
                        'timestamp': rt[4].isoformat() if isinstance(rt[4], (datetime, date)) else str(rt[4]) if rt[4] else None,
                        'description': f'Receipt {rt[1]} Part {rt[7] or "N/A"} Qty: {rt[3]} SN: {rt[5] or "N/A"}',
                        'details': {'lot': rt[6]},
                        'entity_id': rt[0],
                        'entity_ref': rt[1],
                        'icon': 'bi-box-arrow-in-down'
                    })
        except Exception as e:
            logger.error(f"Trace by PO error: {e}")
        events.sort(key=lambda x: x.get('timestamp') or '0000-00-00')
        return events

    @staticmethod
    def _trace_by_sales_order(db, so_number):
        events = []
        try:
            rows = db.execute_query("""
                SELECT so.id, so.so_number, so.customer_id, so.status, so.order_date,
                       so.total_amount, c.name as customer_name
                FROM sales_orders so LEFT JOIN customers c ON so.customer_id = c.id
                WHERE UPPER(so.so_number) = UPPER(%s)
            """, (so_number,))
            if not rows:
                return events
            for r in rows:
                so_id = r[0]
                events.append({
                    'module': 'Sales',
                    'event_type': 'Sales Order',
                    'timestamp': r[4].isoformat() if isinstance(r[4], (datetime, date)) else str(r[4]) if r[4] else None,
                    'description': f'SO {r[1]} - Customer: {r[6] or "N/A"} Status: {r[3]} Total: ${float(r[5] or 0):,.2f}',
                    'details': {'customer_id': r[2]},
                    'entity_id': so_id,
                    'entity_ref': r[1],
                    'icon': 'bi-cart'
                })
                sol_rows = db.execute_query("""
                    SELECT sol.id, sol.product_id, sol.quantity, sol.unit_price, sol.serial_number,
                           sol.work_order_id, p.code, p.name
                    FROM sales_order_lines sol LEFT JOIN products p ON sol.product_id = p.id
                    WHERE sol.so_id = %s
                """, (so_id,))
                for sl in sol_rows:
                    events.append({
                        'module': 'Sales',
                        'event_type': 'SO Line',
                        'timestamp': r[4].isoformat() if isinstance(r[4], (datetime, date)) else str(r[4]) if r[4] else None,
                        'description': f'Part {sl[6] or "N/A"} ({sl[7] or "N/A"}) Qty: {sl[2]} SN: {sl[4] or "N/A"}',
                        'details': {'work_order_id': sl[5]},
                        'entity_id': sl[0],
                        'entity_ref': r[1],
                        'icon': 'bi-list-check'
                    })
                wo_rows = db.execute_query("""
                    SELECT wo.id, wo.wo_number, wo.status, wo.created_at, p.code
                    FROM work_orders wo LEFT JOIN products p ON wo.product_id = p.id
                    WHERE wo.so_id = %s
                """, (so_id,))
                for wo in wo_rows:
                    events.append({
                        'module': 'Manufacturing',
                        'event_type': 'Work Order',
                        'timestamp': wo[3].isoformat() if isinstance(wo[3], (datetime, date)) else str(wo[3]) if wo[3] else None,
                        'description': f'WO {wo[1]} - Part {wo[4] or "N/A"} Status: {wo[2]}',
                        'details': {},
                        'entity_id': wo[0],
                        'entity_ref': wo[1],
                        'icon': 'bi-gear'
                    })
                ship_rows = db.execute_query("""
                    SELECT s.id, s.shipment_number, s.tracking_number, s.status, s.ship_date, s.carrier
                    FROM shipments s WHERE s.sales_order_id = %s
                """, (so_id,))
                for sh in ship_rows:
                    events.append({
                        'module': 'Shipping',
                        'event_type': 'Shipment',
                        'timestamp': sh[4].isoformat() if isinstance(sh[4], (datetime, date)) else str(sh[4]) if sh[4] else None,
                        'description': f'Shipment {sh[1] or sh[0]} - Carrier: {sh[5] or "N/A"} Tracking: {sh[2] or "N/A"} Status: {sh[3]}',
                        'details': {},
                        'entity_id': sh[0],
                        'entity_ref': sh[1] or str(sh[0]),
                        'icon': 'bi-truck'
                    })
                inv_rows = db.execute_query("""
                    SELECT inv.id, inv.invoice_number, inv.status, inv.invoice_date, inv.total_amount
                    FROM invoices inv WHERE inv.so_id = %s
                """, (so_id,))
                for iv in inv_rows:
                    events.append({
                        'module': 'Finance',
                        'event_type': 'Invoice',
                        'timestamp': iv[3].isoformat() if isinstance(iv[3], (datetime, date)) else str(iv[3]) if iv[3] else None,
                        'description': f'Invoice {iv[1]} Status: {iv[2]} Amount: ${float(iv[4] or 0):,.2f}',
                        'details': {},
                        'entity_id': iv[0],
                        'entity_ref': iv[1],
                        'icon': 'bi-file-earmark-text'
                    })
        except Exception as e:
            logger.error(f"Trace by SO error: {e}")
        events.sort(key=lambda x: x.get('timestamp') or '0000-00-00')
        return events

    @staticmethod
    def _get_inventory_events(db, product_id):
        events = []
        try:
            rows = db.execute_query("""
                SELECT i.id, i.quantity, i.condition, i.warehouse_location, i.bin_location,
                       i.unit_cost, i.serial_number, i.lot_number, i.status, i.last_updated,
                       i.supplier_id, i.po_id, s.name as supplier_name
                FROM inventory i LEFT JOIN suppliers s ON i.supplier_id = s.id
                WHERE i.product_id = %s
            """, (product_id,))
            for r in rows:
                events.append({
                    'module': 'Inventory',
                    'event_type': 'Inventory Record',
                    'timestamp': r[9].isoformat() if isinstance(r[9], (datetime, date)) else str(r[9]) if r[9] else None,
                    'description': f'Qty: {r[1]} Cond: {r[2] or "N/A"} Loc: {r[3] or ""}/{r[4] or ""} Cost: ${float(r[5] or 0):.2f}',
                    'details': {
                        'serial': r[6], 'lot': r[7], 'status': r[8],
                        'supplier': r[12], 'po_id': r[11], 'cost': float(r[5] or 0)
                    },
                    'entity_id': r[0],
                    'entity_ref': r[6] or f'INV-{r[0]}',
                    'icon': 'bi-archive'
                })
        except Exception as e:
            logger.error(f"Inventory events error: {e}")
        return events

    @staticmethod
    def _get_receiving_events(db, product_id, po_id=None):
        events = []
        try:
            if po_id:
                rows = db.execute_query("""
                    SELECT rt.id, rt.receipt_number, rt.quantity_received, rt.receipt_date,
                           rt.serial_number, rt.lot_number, rt.unit_cost_at_receipt, rt.condition
                    FROM receiving_transactions rt
                    WHERE rt.product_id = %s AND rt.po_id = %s
                """, (product_id, po_id))
            else:
                rows = db.execute_query("""
                    SELECT rt.id, rt.receipt_number, rt.quantity_received, rt.receipt_date,
                           rt.serial_number, rt.lot_number, rt.unit_cost_at_receipt, rt.condition
                    FROM receiving_transactions rt
                    WHERE rt.product_id = %s
                """, (product_id,))
            for r in rows:
                events.append({
                    'module': 'Receiving',
                    'event_type': 'Material Receipt',
                    'timestamp': r[3].isoformat() if isinstance(r[3], (datetime, date)) else str(r[3]) if r[3] else None,
                    'description': f'Receipt {r[1]} Qty: {r[2]} SN: {r[4] or "N/A"} Cond: {r[7] or "N/A"}',
                    'details': {'lot': r[5], 'cost': float(r[6] or 0)},
                    'entity_id': r[0],
                    'entity_ref': r[1],
                    'icon': 'bi-box-arrow-in-down'
                })
        except Exception as e:
            logger.error(f"Receiving events error: {e}")
        return events

    @staticmethod
    def _get_issuance_events(db, product_id):
        events = []
        try:
            rows = db.execute_query("""
                SELECT mi.id, mi.issue_number, mi.work_order_id, mi.quantity_issued,
                       mi.issue_date, mi.unit_cost, mi.total_cost, mi.issued_to,
                       wo.wo_number
                FROM material_issues mi LEFT JOIN work_orders wo ON mi.work_order_id = wo.id
                WHERE mi.product_id = %s
            """, (product_id,))
            for r in rows:
                events.append({
                    'module': 'Manufacturing',
                    'event_type': 'Material Issue',
                    'timestamp': r[4].isoformat() if isinstance(r[4], (datetime, date)) else str(r[4]) if r[4] else None,
                    'description': f'Issue {r[1]} to WO {r[8] or "N/A"} Qty: {r[3]} Cost: ${float(r[6] or 0):.2f}',
                    'details': {'wo_id': r[2], 'cost': float(r[6] or 0)},
                    'entity_id': r[0],
                    'entity_ref': r[1],
                    'icon': 'bi-box-arrow-right'
                })
        except Exception as e:
            logger.error(f"Issuance events error: {e}")
        return events

    @staticmethod
    def _get_return_events(db, product_id):
        events = []
        try:
            rows = db.execute_query("""
                SELECT mr.id, mr.return_number, mr.work_order_id, mr.quantity_returned,
                       mr.return_date, mr.unit_cost, mr.total_cost, mr.condition,
                       wo.wo_number
                FROM material_returns mr LEFT JOIN work_orders wo ON mr.work_order_id = wo.id
                WHERE mr.product_id = %s
            """, (product_id,))
            for r in rows:
                events.append({
                    'module': 'Manufacturing',
                    'event_type': 'Material Return',
                    'timestamp': r[4].isoformat() if isinstance(r[4], (datetime, date)) else str(r[4]) if r[4] else None,
                    'description': f'Return {r[1]} from WO {r[8] or "N/A"} Qty: {r[3]} Cond: {r[7] or "N/A"}',
                    'details': {'wo_id': r[2], 'cost': float(r[6] or 0)},
                    'entity_id': r[0],
                    'entity_ref': r[1],
                    'icon': 'bi-box-arrow-in-left'
                })
        except Exception as e:
            logger.error(f"Return events error: {e}")
        return events

    @staticmethod
    def _get_adjustment_events(db, product_id):
        events = []
        try:
            rows = db.execute_query("""
                SELECT ia.id, ia.adjustment_number, ia.adjustment_type, ia.quantity_before,
                       ia.quantity_adjusted, ia.quantity_after, ia.adjustment_date,
                       ia.reason, ia.cost_impact
                FROM inventory_adjustments ia WHERE ia.product_id = %s
            """, (product_id,))
            for r in rows:
                events.append({
                    'module': 'Inventory',
                    'event_type': 'Adjustment',
                    'timestamp': r[6].isoformat() if isinstance(r[6], (datetime, date)) else str(r[6]) if r[6] else None,
                    'description': f'Adj {r[1]} Type: {r[2]} Before: {r[3]} Adj: {r[4]} After: {r[5]}',
                    'details': {'reason': r[7], 'cost_impact': float(r[8] or 0)},
                    'entity_id': r[0],
                    'entity_ref': r[1],
                    'icon': 'bi-pencil-square'
                })
        except Exception as e:
            logger.error(f"Adjustment events error: {e}")
        return events

    @staticmethod
    def _get_wo_events_by_product(db, product_id):
        events = []
        try:
            rows = db.execute_query("""
                SELECT wo.id, wo.wo_number, wo.status, wo.created_at, wo.serial_number,
                       wo.material_cost, wo.labor_cost, wo.so_id
                FROM work_orders wo WHERE wo.product_id = %s
            """, (product_id,))
            for r in rows:
                total_cost = float(r[5] or 0) + float(r[6] or 0)
                events.append({
                    'module': 'Manufacturing',
                    'event_type': 'Work Order',
                    'timestamp': r[3].isoformat() if isinstance(r[3], (datetime, date)) else str(r[3]) if r[3] else None,
                    'description': f'WO {r[1]} Status: {r[2]} SN: {r[4] or "N/A"} Cost: ${total_cost:,.2f}',
                    'details': {'wo_id': r[0], 'so_id': r[7], 'cost': total_cost},
                    'entity_id': r[0],
                    'entity_ref': r[1],
                    'icon': 'bi-gear'
                })
        except Exception as e:
            logger.error(f"WO events error: {e}")
        return events

    @staticmethod
    def _get_po_events_by_product(db, product_id):
        events = []
        try:
            rows = db.execute_query("""
                SELECT po.id, po.po_number, po.status, po.order_date,
                       pol.quantity, pol.unit_price, s.name as supplier_name
                FROM purchase_order_lines pol
                JOIN purchase_orders po ON pol.po_id = po.id
                LEFT JOIN suppliers s ON po.supplier_id = s.id
                WHERE pol.product_id = %s
            """, (product_id,))
            for r in rows:
                events.append({
                    'module': 'Procurement',
                    'event_type': 'Purchase Order',
                    'timestamp': r[3].isoformat() if isinstance(r[3], (datetime, date)) else str(r[3]) if r[3] else None,
                    'description': f'PO {r[1]} Supplier: {r[6] or "N/A"} Qty: {r[4]} @ ${float(r[5] or 0):.2f} Status: {r[2]}',
                    'details': {'cost': float(r[4] or 0) * float(r[5] or 0)},
                    'entity_id': r[0],
                    'entity_ref': r[1],
                    'icon': 'bi-receipt'
                })
        except Exception as e:
            logger.error(f"PO events error: {e}")
        return events

    @staticmethod
    def _get_so_events_by_product(db, product_id):
        events = []
        try:
            rows = db.execute_query("""
                SELECT so.id, so.so_number, so.status, so.order_date,
                       sol.quantity, sol.unit_price, sol.serial_number,
                       c.name as customer_name
                FROM sales_order_lines sol
                JOIN sales_orders so ON sol.so_id = so.id
                LEFT JOIN customers c ON so.customer_id = c.id
                WHERE sol.product_id = %s
            """, (product_id,))
            for r in rows:
                events.append({
                    'module': 'Sales',
                    'event_type': 'Sales Order',
                    'timestamp': r[3].isoformat() if isinstance(r[3], (datetime, date)) else str(r[3]) if r[3] else None,
                    'description': f'SO {r[1]} Customer: {r[7] or "N/A"} Qty: {r[4]} @ ${float(r[5] or 0):.2f} SN: {r[6] or "N/A"}',
                    'details': {'cost': float(r[4] or 0) * float(r[5] or 0)},
                    'entity_id': r[0],
                    'entity_ref': r[1],
                    'icon': 'bi-cart'
                })
        except Exception as e:
            logger.error(f"SO events error: {e}")
        return events

    @staticmethod
    def _get_shipment_events_by_product(db, product_id):
        events = []
        try:
            rows = db.execute_query("""
                SELECT s.id, s.shipment_number, s.tracking_number, s.status, s.ship_date,
                       sl.quantity, s.carrier
                FROM shipment_lines sl
                JOIN shipments s ON sl.shipment_id = s.id
                WHERE sl.product_id = %s
            """, (product_id,))
            for r in rows:
                events.append({
                    'module': 'Shipping',
                    'event_type': 'Shipment',
                    'timestamp': r[4].isoformat() if isinstance(r[4], (datetime, date)) else str(r[4]) if r[4] else None,
                    'description': f'Ship {r[1] or r[0]} Carrier: {r[6] or "N/A"} Tracking: {r[2] or "N/A"} Qty: {r[5]}',
                    'details': {},
                    'entity_id': r[0],
                    'entity_ref': r[1] or str(r[0]),
                    'icon': 'bi-truck'
                })
        except Exception as e:
            logger.error(f"Shipment events error: {e}")
        return events

    @staticmethod
    def _get_quality_events_by_product(db, product_id):
        events = []
        try:
            dev_rows = db.execute_query("""
                SELECT d.id, d.deviation_number, d.deviation_type, d.severity,
                       d.status, d.reported_date, d.description
                FROM qms_deviations d WHERE d.erp_transaction_id = %s::text
            """, (str(product_id),))
            for r in dev_rows:
                events.append({
                    'module': 'Quality',
                    'event_type': 'Deviation',
                    'timestamp': r[5].isoformat() if isinstance(r[5], (datetime, date)) else str(r[5]) if r[5] else None,
                    'description': f'DEV {r[1]} Type: {r[2]} Severity: {r[3]} Status: {r[4]}',
                    'details': {'desc': r[6]},
                    'entity_id': r[0],
                    'entity_ref': r[1],
                    'icon': 'bi-exclamation-triangle'
                })
            capa_rows = db.execute_query("""
                SELECT c.id, c.capa_number, c.capa_type, c.priority, c.status,
                       c.created_at, c.title
                FROM qms_capa c WHERE c.source_id = %s
            """, (product_id,))
            for r in capa_rows:
                events.append({
                    'module': 'Quality',
                    'event_type': 'CAPA',
                    'timestamp': r[5].isoformat() if isinstance(r[5], (datetime, date)) else str(r[5]) if r[5] else None,
                    'description': f'CAPA {r[1]} Type: {r[2]} Priority: {r[3]} Status: {r[4]} - {r[6] or ""}',
                    'details': {},
                    'entity_id': r[0],
                    'entity_ref': r[1],
                    'icon': 'bi-clipboard-check'
                })
        except Exception as e:
            logger.error(f"Quality events error: {e}")
        return events

    @staticmethod
    def _get_gl_events_by_product(db, product_id):
        events = []
        try:
            rows = db.execute_query("""
                SELECT ge.id, ge.entry_number, ge.entry_date, ge.description,
                       ge.transaction_source, ge.reference_type, ge.reference_id
                FROM gl_entries ge
                WHERE ge.reference_id = %s::text
                   OR ge.description LIKE %s
                ORDER BY ge.entry_date
            """, (str(product_id), f'%product {product_id}%'))
            for r in rows:
                events.append({
                    'module': 'Finance',
                    'event_type': 'GL Entry',
                    'timestamp': r[2].isoformat() if isinstance(r[2], (datetime, date)) else str(r[2]) if r[2] else None,
                    'description': f'GL {r[1]} - {r[3] or "N/A"} Source: {r[4] or "N/A"}',
                    'details': {'ref_type': r[5], 'ref_id': r[6]},
                    'entity_id': r[0],
                    'entity_ref': r[1],
                    'icon': 'bi-journal-text'
                })
        except Exception as e:
            logger.error(f"GL events error: {e}")
        return events

    @staticmethod
    def _get_bom_events(db, product_id):
        events = []
        try:
            rows = db.execute_query("""
                SELECT b.id, b.parent_product_id, b.child_product_id, b.quantity,
                       pp.code as parent_code, cp.code as component_code, cp.name as component_name
                FROM boms b
                LEFT JOIN products pp ON b.parent_product_id = pp.id
                LEFT JOIN products cp ON b.child_product_id = cp.id
                WHERE b.parent_product_id = %s OR b.child_product_id = %s
            """, (product_id, product_id))
            for r in rows:
                if r[1] == product_id:
                    events.append({
                        'module': 'Engineering',
                        'event_type': 'BOM Component',
                        'timestamp': None,
                        'description': f'Uses component {r[5]} ({r[6]}) Qty: {r[3]}',
                        'details': {'relationship': 'parent', 'component_id': r[2]},
                        'entity_id': r[0],
                        'entity_ref': f'BOM-{r[0]}',
                        'icon': 'bi-diagram-3'
                    })
                else:
                    events.append({
                        'module': 'Engineering',
                        'event_type': 'BOM Parent',
                        'timestamp': None,
                        'description': f'Used in assembly {r[4]} Qty: {r[3]}',
                        'details': {'relationship': 'child', 'parent_id': r[1]},
                        'entity_id': r[0],
                        'entity_ref': f'BOM-{r[0]}',
                        'icon': 'bi-diagram-3'
                    })
        except Exception as e:
            logger.error(f"BOM events error: {e}")
        return events

    @staticmethod
    def _get_wo_material_issues(db, wo_id):
        events = []
        try:
            rows = db.execute_query("""
                SELECT mi.id, mi.issue_number, mi.product_id, mi.quantity_issued,
                       mi.issue_date, mi.unit_cost, mi.total_cost, p.code, p.name
                FROM material_issues mi LEFT JOIN products p ON mi.product_id = p.id
                WHERE mi.work_order_id = %s
            """, (wo_id,))
            for r in rows:
                events.append({
                    'module': 'Manufacturing',
                    'event_type': 'Material Issue',
                    'timestamp': r[4].isoformat() if isinstance(r[4], (datetime, date)) else str(r[4]) if r[4] else None,
                    'description': f'Issue {r[1]} Part {r[7] or "N/A"} Qty: {r[3]} Cost: ${float(r[6] or 0):.2f}',
                    'details': {'product_id': r[2], 'cost': float(r[6] or 0)},
                    'entity_id': r[0],
                    'entity_ref': r[1],
                    'icon': 'bi-box-arrow-right'
                })
        except Exception as e:
            logger.error(f"WO material issues error: {e}")
        return events

    @staticmethod
    def _get_wo_returns(db, wo_id):
        events = []
        try:
            rows = db.execute_query("""
                SELECT mr.id, mr.return_number, mr.product_id, mr.quantity_returned,
                       mr.return_date, mr.unit_cost, mr.total_cost, p.code
                FROM material_returns mr LEFT JOIN products p ON mr.product_id = p.id
                WHERE mr.work_order_id = %s
            """, (wo_id,))
            for r in rows:
                events.append({
                    'module': 'Manufacturing',
                    'event_type': 'Material Return',
                    'timestamp': r[4].isoformat() if isinstance(r[4], (datetime, date)) else str(r[4]) if r[4] else None,
                    'description': f'Return {r[1]} Part {r[7] or "N/A"} Qty: {r[3]} Cost: ${float(r[6] or 0):.2f}',
                    'details': {'product_id': r[2], 'cost': float(r[6] or 0)},
                    'entity_id': r[0],
                    'entity_ref': r[1],
                    'icon': 'bi-box-arrow-in-left'
                })
        except Exception as e:
            logger.error(f"WO returns error: {e}")
        return events

    @staticmethod
    def _get_wo_labor(db, wo_id):
        events = []
        try:
            rows = db.execute_query("""
                SELECT tt.id, tt.employee_id, tt.clock_in_time, tt.clock_out_time,
                       tt.hours_worked, tt.hourly_rate, tt.labor_cost, tt.entry_number
                FROM work_order_time_tracking tt
                WHERE tt.work_order_id = %s
            """, (wo_id,))
            for r in rows:
                cost = float(r[6] or 0) if r[6] else float(r[4] or 0) * float(r[5] or 0)
                events.append({
                    'module': 'Labor',
                    'event_type': 'Labor Entry',
                    'timestamp': r[2].isoformat() if isinstance(r[2], (datetime, date)) else str(r[2]) if r[2] else None,
                    'description': f'Entry {r[7] or r[0]} Hours: {r[4] or 0} Cost: ${cost:.2f}',
                    'details': {'cost': cost},
                    'entity_id': r[0],
                    'entity_ref': r[7] or f'LABOR-{r[0]}',
                    'icon': 'bi-person-badge'
                })
        except Exception as e:
            logger.error(f"WO labor error: {e}")
        return events

    @staticmethod
    def _get_wo_stages(db, wo_id):
        events = []
        try:
            rows = db.execute_query("""
                SELECT wsh.id, wsh.stage_id, wsh.entered_at, wsh.changed_by,
                       ws.name as stage_name
                FROM work_order_stage_history wsh
                LEFT JOIN work_order_stages ws ON wsh.stage_id = ws.id
                WHERE wsh.work_order_id = %s
                ORDER BY wsh.entered_at
            """, (wo_id,))
            for r in rows:
                events.append({
                    'module': 'Manufacturing',
                    'event_type': 'Stage Change',
                    'timestamp': r[2].isoformat() if isinstance(r[2], (datetime, date)) else str(r[2]) if r[2] else None,
                    'description': f'Stage changed to: {r[4] or "Stage " + str(r[1])} by {r[3] or "System"}',
                    'details': {},
                    'entity_id': r[0],
                    'entity_ref': f'STAGE-{r[0]}',
                    'icon': 'bi-arrow-right-circle'
                })
        except Exception as e:
            logger.error(f"WO stages error: {e}")
        return events

    @staticmethod
    def build_graph_data(events):
        nodes = {}
        edges = []
        module_colors = {
            'Product Master': '#6366f1',
            'Inventory': '#10b981',
            'Receiving': '#3b82f6',
            'Manufacturing': '#f59e0b',
            'Procurement': '#8b5cf6',
            'Sales': '#ef4444',
            'Shipping': '#06b6d4',
            'Finance': '#84cc16',
            'Quality': '#f97316',
            'Engineering': '#ec4899',
            'Labor': '#14b8a6',
        }
        for i, event in enumerate(events):
            node_id = f"{event['module']}_{event.get('entity_id', i)}"
            if node_id not in nodes:
                nodes[node_id] = {
                    'id': node_id,
                    'label': event.get('entity_ref', ''),
                    'module': event['module'],
                    'type': event['event_type'],
                    'color': module_colors.get(event['module'], '#94a3b8'),
                    'description': event['description']
                }
        node_list = list(nodes.values())
        seen_edges = set()
        for i in range(len(events)):
            for j in range(i + 1, min(i + 5, len(events))):
                n1 = f"{events[i]['module']}_{events[i].get('entity_id', i)}"
                n2 = f"{events[j]['module']}_{events[j].get('entity_id', j)}"
                if n1 != n2 and n1 in nodes and n2 in nodes:
                    edge_key = tuple(sorted([n1, n2]))
                    if edge_key not in seen_edges:
                        seen_edges.add(edge_key)
                        edges.append({'from': n1, 'to': n2})
        return {'nodes': node_list, 'edges': edges}

    @staticmethod
    def build_cost_data(events):
        cost_points = []
        cumulative = 0
        for event in events:
            details = event.get('details', {})
            cost = details.get('cost', 0) or 0
            cost_impact = details.get('cost_impact', 0) or 0
            total = cost + cost_impact
            if total != 0:
                cumulative += total
                cost_points.append({
                    'timestamp': event.get('timestamp', ''),
                    'event': event['event_type'],
                    'description': event['description'],
                    'amount': round(total, 2),
                    'cumulative': round(cumulative, 2),
                    'module': event['module']
                })
        return cost_points

    @staticmethod
    def build_summary(events):
        if not events:
            return {
                'total_events': 0,
                'modules_touched': [],
                'first_event': None,
                'last_event': None,
                'total_cost': 0,
                'event_types': {},
                'module_counts': {}
            }
        modules = set()
        event_types = {}
        module_counts = {}
        total_cost = 0
        timestamps = []
        for e in events:
            modules.add(e['module'])
            et = e['event_type']
            event_types[et] = event_types.get(et, 0) + 1
            mc = e['module']
            module_counts[mc] = module_counts.get(mc, 0) + 1
            d = e.get('details', {})
            total_cost += (d.get('cost', 0) or 0) + (d.get('cost_impact', 0) or 0)
            if e.get('timestamp'):
                timestamps.append(e['timestamp'])
        timestamps.sort()
        return {
            'total_events': len(events),
            'modules_touched': sorted(list(modules)),
            'first_event': timestamps[0] if timestamps else None,
            'last_event': timestamps[-1] if timestamps else None,
            'total_cost': round(total_cost, 2),
            'event_types': event_types,
            'module_counts': module_counts
        }

    @staticmethod
    def get_ai_risk_analysis(events, summary):
        risk_score = 0
        risk_factors = []
        recommendations = []
        quality_events = [e for e in events if e['module'] == 'Quality']
        if quality_events:
            risk_score += len(quality_events) * 15
            risk_factors.append(f'{len(quality_events)} quality event(s) detected')
            recommendations.append('Review quality events for pattern analysis')
        return_events = [e for e in events if e['event_type'] == 'Material Return']
        if return_events:
            risk_score += len(return_events) * 10
            risk_factors.append(f'{len(return_events)} material return(s) recorded')
            recommendations.append('Investigate material return causes')
        adjustment_events = [e for e in events if e['event_type'] == 'Adjustment']
        if adjustment_events:
            risk_score += len(adjustment_events) * 8
            risk_factors.append(f'{len(adjustment_events)} inventory adjustment(s)')
            recommendations.append('Audit adjustment reasons for anomalies')
        if summary.get('total_cost', 0) > 50000:
            risk_score += 20
            risk_factors.append(f'High cumulative cost exposure: ${summary["total_cost"]:,.2f}')
            recommendations.append('Review cost trajectory against budget')
        supplier_count = len(set(
            e.get('details', {}).get('supplier', '')
            for e in events if e.get('details', {}).get('supplier')
        ))
        if supplier_count > 3:
            risk_score += 10
            risk_factors.append(f'{supplier_count} different suppliers involved')
            recommendations.append('Evaluate supplier consolidation opportunity')
        if supplier_count == 1:
            risk_score += 15
            risk_factors.append('Single-source supplier dependency')
            recommendations.append('Consider qualifying alternate suppliers')
        if summary.get('total_events', 0) > 50:
            risk_score += 10
            risk_factors.append('High transaction volume detected')
        risk_score = min(risk_score, 100)
        if risk_score < 25:
            risk_level = 'Low'
            risk_color = '#10b981'
        elif risk_score < 50:
            risk_level = 'Moderate'
            risk_color = '#f59e0b'
        elif risk_score < 75:
            risk_level = 'Elevated'
            risk_color = '#f97316'
        else:
            risk_level = 'Critical'
            risk_color = '#ef4444'
        if not recommendations:
            recommendations.append('No immediate action required')
        return {
            'risk_score': risk_score,
            'risk_level': risk_level,
            'risk_color': risk_color,
            'risk_factors': risk_factors,
            'recommendations': recommendations,
            'quality_events': len(quality_events),
            'return_events': len(return_events),
            'adjustment_events': len(adjustment_events),
            'supplier_count': supplier_count
        }
