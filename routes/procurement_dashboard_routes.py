from flask import Blueprint, render_template, jsonify, request
from auth import login_required
from models import Database, safe_float
from datetime import datetime, timedelta
import json
import os

procurement_dashboard_bp = Blueprint('procurement_dashboard', __name__)

def get_date_ranges():
    today = datetime.now()
    
    last_30_start = (today - timedelta(days=30)).strftime('%Y-%m-%d')
    last_90_start = (today - timedelta(days=90)).strftime('%Y-%m-%d')
    last_365_start = (today - timedelta(days=365)).strftime('%Y-%m-%d')
    
    prev_30_start = (today - timedelta(days=60)).strftime('%Y-%m-%d')
    prev_30_end = (today - timedelta(days=30)).strftime('%Y-%m-%d')
    
    today_str = today.strftime('%Y-%m-%d')
    
    return {
        'mtd_start': last_30_start,
        'qtd_start': last_90_start,
        'ytd_start': last_365_start,
        'prev_30_start': prev_30_start,
        'prev_30_end': prev_30_end,
        'today': today_str
    }

@procurement_dashboard_bp.route('/executive-procurement-dashboard')
@login_required
def executive_procurement_dashboard():
    db = Database()
    conn = db.get_connection()
    
    dates = get_date_ranges()
    today = datetime.now()
    
    mtd_spend = conn.execute('''
        SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) as spend
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        WHERE po.order_date >= ? AND po.order_date <= ?
        AND po.status NOT IN ('Draft', 'Cancelled')
    ''', (dates['mtd_start'], dates['today'])).fetchone()
    
    qtd_spend = conn.execute('''
        SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) as spend
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        WHERE po.order_date >= ? AND po.order_date <= ?
        AND po.status NOT IN ('Draft', 'Cancelled')
    ''', (dates['qtd_start'], dates['today'])).fetchone()
    
    ytd_spend = conn.execute('''
        SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) as spend
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        WHERE po.order_date >= ? AND po.order_date <= ?
        AND po.status NOT IN ('Draft', 'Cancelled')
    ''', (dates['ytd_start'], dates['today'])).fetchone()
    
    prev_ytd_spend = conn.execute('''
        SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) as spend
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        WHERE po.order_date >= ? AND po.order_date <= ?
        AND po.status NOT IN ('Draft', 'Cancelled')
    ''', ((today - timedelta(days=730)).strftime('%Y-%m-%d'), 
          (today - timedelta(days=365)).strftime('%Y-%m-%d'))).fetchone()
    
    spend_growth = 0
    if prev_ytd_spend['spend'] > 0:
        spend_growth = ((ytd_spend['spend'] - prev_ytd_spend['spend']) / prev_ytd_spend['spend']) * 100
    
    open_pos = conn.execute('''
        SELECT 
            COUNT(DISTINCT po.id) as count,
            COALESCE(SUM(pol.quantity * pol.unit_price), 0) as value
        FROM purchase_orders po
        LEFT JOIN purchase_order_lines pol ON pol.po_id = po.id
        WHERE po.status IN ('Approved', 'Ordered', 'Sent')
    ''').fetchone()
    
    po_cycle_time = conn.execute('''
        SELECT AVG(JULIANDAY(COALESCE(actual_delivery_date, expected_delivery_date)) - JULIANDAY(order_date)) as avg_days
        FROM purchase_orders
        WHERE status = 'Received'
        AND order_date >= ?
    ''', (dates['ytd_start'],)).fetchone()
    
    otif_stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN actual_delivery_date <= expected_delivery_date THEN 1 ELSE 0 END) as on_time,
            SUM(CASE WHEN received_quantity >= (SELECT SUM(quantity) FROM purchase_order_lines WHERE po_id = po.id) THEN 1 ELSE 0 END) as in_full
        FROM purchase_orders po
        WHERE status = 'Received'
        AND order_date >= ?
    ''', (dates['ytd_start'],)).fetchone()
    
    otif_pct = 0
    if otif_stats['total'] > 0:
        on_time_pct = (otif_stats['on_time'] or 0) / otif_stats['total'] * 100
        in_full_pct = (otif_stats['in_full'] or 0) / otif_stats['total'] * 100
        otif_pct = (on_time_pct + in_full_pct) / 2
    
    low_stock_items = conn.execute('''
        SELECT COUNT(*) as count
        FROM inventory i
        WHERE i.quantity <= i.reorder_point
        AND i.quantity > 0
    ''').fetchone()
    
    stockout_items = conn.execute('''
        SELECT COUNT(*) as count
        FROM inventory i
        WHERE i.quantity <= 0
    ''').fetchone()
    
    inventory_value = conn.execute('''
        SELECT COALESCE(SUM((i.quantity - COALESCE(i.reserved_quantity, 0)) * COALESCE(i.unit_cost, 0)), 0) as total_value
        FROM inventory i
        WHERE (i.quantity - COALESCE(i.reserved_quantity, 0)) > 0
    ''').fetchone()
    
    excess_inventory = conn.execute('''
        SELECT COALESCE(SUM((i.quantity - i.reorder_point * 3) * i.unit_cost), 0) as excess_value
        FROM inventory i
        WHERE i.quantity > i.reorder_point * 3
        AND i.unit_cost > 0
    ''').fetchone()
    
    spend_by_supplier = conn.execute('''
        SELECT 
            s.name as supplier_name,
            s.code as supplier_code,
            COALESCE(SUM(pol.quantity * pol.unit_price), 0) as spend,
            COUNT(DISTINCT po.id) as po_count
        FROM suppliers s
        LEFT JOIN purchase_orders po ON po.supplier_id = s.id
            AND po.order_date >= ?
            AND po.status NOT IN ('Draft', 'Cancelled')
        LEFT JOIN purchase_order_lines pol ON pol.po_id = po.id
        GROUP BY s.id, s.name, s.code
        HAVING COALESCE(SUM(pol.quantity * pol.unit_price), 0) > 0
        ORDER BY 3 DESC
        LIMIT 15
    ''', (dates['ytd_start'],)).fetchall()
    
    spend_by_category = conn.execute('''
        SELECT 
            COALESCE(p.part_category, 'Uncategorized') as category,
            COALESCE(SUM(pol.quantity * pol.unit_price), 0) as spend,
            COUNT(DISTINCT pol.id) as line_count
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        LEFT JOIN products p ON pol.product_id = p.id
        WHERE po.order_date >= ?
        AND po.status NOT IN ('Draft', 'Cancelled')
        GROUP BY p.part_category
        ORDER BY 2 DESC
    ''', (dates['ytd_start'],)).fetchall()
    
    monthly_spend = conn.execute('''
        SELECT 
            strftime('%Y-%m', po.order_date) as month,
            COALESCE(SUM(pol.quantity * pol.unit_price), 0) as spend
        FROM purchase_orders po
        JOIN purchase_order_lines pol ON pol.po_id = po.id
        WHERE po.order_date >= date('now', '-12 months')
        AND po.status NOT IN ('Draft', 'Cancelled')
        GROUP BY 1
        ORDER BY 1
    ''').fetchall()
    
    supplier_performance = conn.execute('''
        SELECT 
            s.name as supplier_name,
            s.code as supplier_code,
            COUNT(po.id) as total_pos,
            SUM(CASE WHEN po.actual_delivery_date <= po.expected_delivery_date THEN 1 ELSE 0 END) as on_time,
            AVG(JULIANDAY(COALESCE(po.actual_delivery_date, po.expected_delivery_date)) - JULIANDAY(po.order_date)) as avg_lead_time,
            COALESCE(SUM(pol.quantity * pol.unit_price), 0) as total_spend
        FROM suppliers s
        JOIN purchase_orders po ON po.supplier_id = s.id
        LEFT JOIN purchase_order_lines pol ON pol.po_id = po.id
        WHERE po.status = 'Received'
        AND po.order_date >= ?
        GROUP BY s.id, s.name, s.code
        HAVING COUNT(po.id) >= 1
        ORDER BY 6 DESC
        LIMIT 10
    ''', (dates['ytd_start'],)).fetchall()
    
    pending_deliveries = conn.execute('''
        SELECT 
            po.po_number,
            s.name as supplier_name,
            po.expected_delivery_date,
            COALESCE(SUM(pol.quantity * pol.unit_price), 0) as value,
            JULIANDAY(po.expected_delivery_date) - JULIANDAY('now') as days_until_due
        FROM purchase_orders po
        JOIN suppliers s ON po.supplier_id = s.id
        LEFT JOIN purchase_order_lines pol ON pol.po_id = po.id
        WHERE po.status IN ('Approved', 'Ordered', 'Sent')
        AND po.expected_delivery_date IS NOT NULL
        GROUP BY po.id, po.po_number, s.name, po.expected_delivery_date
        ORDER BY po.expected_delivery_date
        LIMIT 10
    ''').fetchall()
    
    overdue_pos = conn.execute('''
        SELECT 
            COUNT(*) as count,
            COALESCE(SUM(pol.quantity * pol.unit_price), 0) as value
        FROM purchase_orders po
        LEFT JOIN purchase_order_lines pol ON pol.po_id = po.id
        WHERE po.status IN ('Approved', 'Ordered', 'Sent')
        AND po.expected_delivery_date < date('now')
    ''').fetchone()
    
    wo_material_shortages = conn.execute('''
        SELECT COUNT(*) as count
        FROM work_orders wo
        WHERE wo.status IN ('Planned', 'In Progress')
        AND EXISTS (
            SELECT 1 FROM work_order_tasks wot
            JOIN work_order_task_materials wotm ON wotm.task_id = wot.id
            JOIN inventory i ON wotm.product_id = i.product_id
            WHERE wot.work_order_id = wo.id
            AND (i.quantity - COALESCE(i.reserved_quantity, 0)) < wotm.required_qty
        )
    ''').fetchone()
    
    aog_spend = conn.execute('''
        SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) as spend
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON pol.po_id = po.id
        JOIN work_orders wo ON po.work_order_id = wo.id
        WHERE wo.is_aog = 1
        AND po.order_date >= ?
        AND po.status NOT IN ('Draft', 'Cancelled')
    ''', (dates['ytd_start'],)).fetchone()
    
    aog_pct = 0
    if ytd_spend['spend'] > 0:
        aog_pct = (aog_spend['spend'] / ytd_spend['spend']) * 100
    
    forecast_30 = conn.execute('''
        SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) as forecast
        FROM purchase_orders po
        JOIN purchase_order_lines pol ON pol.po_id = po.id
        WHERE po.status IN ('Approved', 'Ordered', 'Sent')
        AND po.expected_delivery_date BETWEEN date('now') AND date('now', '+30 days')
    ''').fetchone()
    
    forecast_60 = conn.execute('''
        SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) as forecast
        FROM purchase_orders po
        JOIN purchase_order_lines pol ON pol.po_id = po.id
        WHERE po.status IN ('Approved', 'Ordered', 'Sent')
        AND po.expected_delivery_date BETWEEN date('now', '+31 days') AND date('now', '+60 days')
    ''').fetchone()
    
    forecast_90 = conn.execute('''
        SELECT COALESCE(SUM(pol.quantity * pol.unit_price), 0) as forecast
        FROM purchase_orders po
        JOIN purchase_order_lines pol ON pol.po_id = po.id
        WHERE po.status IN ('Approved', 'Ordered', 'Sent')
        AND po.expected_delivery_date BETWEEN date('now', '+61 days') AND date('now', '+90 days')
    ''').fetchone()
    
    alerts = []
    
    if overdue_pos['count'] > 0:
        alerts.append({
            'type': 'danger',
            'icon': 'exclamation-triangle-fill',
            'title': 'Overdue Purchase Orders',
            'message': f'{overdue_pos["count"]} POs overdue (${overdue_pos["value"]:,.0f} at risk)'
        })
    
    if low_stock_items['count'] > 5:
        alerts.append({
            'type': 'warning',
            'icon': 'box-seam',
            'title': 'Low Stock Alert',
            'message': f'{low_stock_items["count"]} items below reorder point'
        })
    
    if stockout_items['count'] > 0:
        alerts.append({
            'type': 'danger',
            'icon': 'x-circle',
            'title': 'Stock-Out Critical',
            'message': f'{stockout_items["count"]} items completely out of stock'
        })
    
    if wo_material_shortages['count'] > 0:
        alerts.append({
            'type': 'warning',
            'icon': 'tools',
            'title': 'Work Order Material Shortage',
            'message': f'{wo_material_shortages["count"]} work orders have material shortages'
        })
    
    if otif_pct < 80:
        alerts.append({
            'type': 'warning',
            'icon': 'truck',
            'title': 'Supplier OTIF Below Target',
            'message': f'OTIF at {otif_pct:.1f}% (target: 80%)'
        })
    
    if aog_pct > 10:
        alerts.append({
            'type': 'info',
            'icon': 'lightning',
            'title': 'High AOG-Driven Spend',
            'message': f'{aog_pct:.1f}% of spend is AOG-related'
        })
    
    conn.close()
    
    kpis = {
        'mtd_spend': mtd_spend['spend'],
        'qtd_spend': qtd_spend['spend'],
        'ytd_spend': ytd_spend['spend'],
        'spend_growth': spend_growth,
        'open_po_count': open_pos['count'],
        'open_po_value': open_pos['value'],
        'po_cycle_time': po_cycle_time['avg_days'] or 0,
        'otif_pct': otif_pct,
        'low_stock_count': low_stock_items['count'],
        'stockout_count': stockout_items['count'],
        'inventory_value': inventory_value['total_value'],
        'excess_inventory': excess_inventory['excess_value'] if excess_inventory['excess_value'] > 0 else 0,
        'overdue_po_count': overdue_pos['count'],
        'overdue_po_value': overdue_pos['value'],
        'wo_shortages': wo_material_shortages['count'],
        'aog_spend': aog_spend['spend'],
        'aog_pct': aog_pct
    }
    
    forecasts = {
        '30_day': forecast_30['forecast'],
        '60_day': forecast_60['forecast'],
        '90_day': forecast_90['forecast']
    }
    
    return render_template('procurement_dashboard/executive.html',
                         kpis=kpis,
                         spend_by_supplier=spend_by_supplier,
                         spend_by_category=spend_by_category,
                         monthly_spend=monthly_spend,
                         supplier_performance=supplier_performance,
                         pending_deliveries=pending_deliveries,
                         forecasts=forecasts,
                         alerts=alerts)


@procurement_dashboard_bp.route('/api/procurement-copilot', methods=['POST'])
@login_required
def procurement_copilot():
    try:
        from openai import OpenAI
        client = OpenAI()
        
        data = request.get_json()
        question = data.get('question', '')
        
        if not question:
            return jsonify({'success': False, 'error': 'No question provided'})
        
        db = Database()
        conn = db.get_connection()
        dates = get_date_ranges()
        
        context_data = {}
        
        context_data['spend_summary'] = conn.execute('''
            SELECT 
                COALESCE(SUM(pol.quantity * pol.unit_price), 0) as total_spend,
                COUNT(DISTINCT po.id) as po_count,
                COUNT(DISTINCT po.supplier_id) as supplier_count
            FROM purchase_orders po
            JOIN purchase_order_lines pol ON pol.po_id = po.id
            WHERE po.order_date >= ?
            AND po.status NOT IN ('Draft', 'Cancelled')
        ''', (dates['ytd_start'],)).fetchone()
        
        context_data['top_suppliers'] = conn.execute('''
            SELECT s.name, COALESCE(SUM(pol.quantity * pol.unit_price), 0) as spend
            FROM suppliers s
            JOIN purchase_orders po ON po.supplier_id = s.id
            JOIN purchase_order_lines pol ON pol.po_id = po.id
            WHERE po.order_date >= ?
            AND po.status NOT IN ('Draft', 'Cancelled')
            GROUP BY s.id, s.name
            ORDER BY 2 DESC
            LIMIT 5
        ''', (dates['ytd_start'],)).fetchall()
        
        context_data['overdue_pos'] = conn.execute('''
            SELECT COUNT(*) as count, COALESCE(SUM(pol.quantity * pol.unit_price), 0) as value
            FROM purchase_orders po
            LEFT JOIN purchase_order_lines pol ON pol.po_id = po.id
            WHERE po.status IN ('Approved', 'Ordered', 'Sent')
            AND po.expected_delivery_date < date('now')
        ''').fetchone()
        
        context_data['low_stock'] = conn.execute('''
            SELECT p.part_number, p.description, i.quantity, i.reorder_point
            FROM inventory i
            JOIN products p ON i.product_id = p.id
            WHERE i.quantity <= i.reorder_point
            ORDER BY (i.reorder_point - i.quantity) DESC
            LIMIT 10
        ''').fetchall()
        
        conn.close()
        
        context = f"""
You are a Procurement AI Copilot for an Aviation MRO company. Answer questions about procurement, suppliers, and inventory.

CURRENT DATA:
- Total Spend (12 months): ${safe_float(context_data['spend_summary']['total_spend']):,.2f}
- Active POs: {context_data['spend_summary']['po_count']}
- Active Suppliers: {context_data['spend_summary']['supplier_count']}

TOP SUPPLIERS BY SPEND:
{chr(10).join([f"- {s['name']}: ${safe_float(s['spend']):,.2f}" for s in context_data['top_suppliers']])}

OVERDUE POs: {context_data['overdue_pos']['count']} orders (${safe_float(context_data['overdue_pos']['value']):,.2f} at risk)

LOW STOCK ITEMS:
{chr(10).join([f"- {item['part_number']}: {item['quantity']} on hand, reorder at {item['reorder_point']}" for item in context_data['low_stock'][:5]])}

Provide concise, actionable insights. Be specific with numbers and recommendations.
"""
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": context},
                {"role": "user", "content": question}
            ],
            max_tokens=500,
            temperature=0.7
        )
        
        answer = response.choices[0].message.content
        
        return jsonify({'success': True, 'response': answer})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})
