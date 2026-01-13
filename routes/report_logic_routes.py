from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify, send_file
from models import Database, AuditLogger
from auth import login_required, role_required
from datetime import datetime, timedelta
import json
import os
import io
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
import pandas as pd

report_logic_bp = Blueprint('report_logic_routes', __name__)

REPORT_TYPES = {
    'transaction': {
        'name': 'Transaction Report',
        'description': 'Sales, purchases, and financial transactions',
        'data_sources': ['sales_orders', 'purchase_orders', 'invoices', 'vendor_invoices']
    },
    'financial': {
        'name': 'Financial Report',
        'description': 'Revenue, expenses, profitability analysis',
        'data_sources': ['invoices', 'vendor_invoices', 'gl_entries', 'chart_of_accounts']
    },
    'operational': {
        'name': 'Operational Report',
        'description': 'Work orders, production metrics, efficiency',
        'data_sources': ['work_orders', 'work_order_tasks', 'labor_resources']
    },
    'inventory': {
        'name': 'Inventory Report',
        'description': 'Stock levels, valuations, movements',
        'data_sources': ['inventory', 'products', 'inventory_adjustments']
    },
    'compliance': {
        'name': 'Compliance/Audit Report',
        'description': 'Audit trails, quality records, certifications',
        'data_sources': ['audit_log', 'qms_sops', 'qms_deviations']
    },
    'custom': {
        'name': 'Custom AI Report',
        'description': 'AI-defined reports based on natural language',
        'data_sources': []
    }
}

def generate_report_number():
    """Generate unique report number"""
    db = Database()
    conn = db.get_connection()
    today = datetime.now().strftime('%Y%m%d')
    
    last_report = conn.execute('''
        SELECT report_number FROM report_repository 
        WHERE report_number LIKE ? 
        ORDER BY id DESC LIMIT 1
    ''', (f'RPT-{today}-%',)).fetchone()
    
    if last_report:
        last_seq = int(last_report['report_number'].split('-')[-1])
        seq = last_seq + 1
    else:
        seq = 1
    
    conn.close()
    return f"RPT-{today}-{seq:04d}"

def log_report_action(report_id, action_type, description, ai_command=None, data_sources=None, parameters=None, result_summary=None, user_id=None):
    """Log report actions for audit trail"""
    db = Database()
    conn = db.get_connection()
    
    conn.execute('''
        INSERT INTO report_audit_log 
        (report_id, action_type, action_description, ai_command, data_sources, parameters_used, result_summary, performed_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (report_id, action_type, description, ai_command, data_sources, parameters, result_summary, user_id))
    
    conn.commit()
    conn.close()

def parse_ai_command(command):
    """Parse natural language command to extract report parameters"""
    import openai
    
    system_prompt = """You are an AI report assistant for Dynamic.IQ-COREx MRP system.
Parse the user's natural language command and extract report parameters.

Available report types: transaction, financial, operational, inventory, compliance, custom

Return a JSON object with:
{
    "report_type": "transaction|financial|operational|inventory|compliance|custom",
    "name": "Generated report name",
    "description": "Brief description",
    "filters": {
        "date_range": {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"} or null,
        "period": "last_30_days|last_90_days|this_month|this_quarter|this_year|ytd|all" or null,
        "entities": ["customer names", "product codes", "regions"] or [],
        "status": ["status values"] or [],
        "categories": ["category values"] or []
    },
    "aggregations": ["sum", "avg", "count", "trend", "comparison"],
    "group_by": ["field names"] or [],
    "output_actions": {
        "save": true/false,
        "download": true/false,
        "email": true/false,
        "email_recipients": ["email addresses"] or []
    },
    "confidence": 0.0-1.0
}

If any parameter is unclear, use reasonable defaults. Always include a confidence score."""

    try:
        client = openai.OpenAI()
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": command}
            ],
            response_format={"type": "json_object"},
            temperature=0.3
        )
        
        result = json.loads(response.choices[0].message.content)
        return result
    except Exception as e:
        return {
            "report_type": "custom",
            "name": "Custom Report",
            "description": command,
            "filters": {},
            "aggregations": [],
            "group_by": [],
            "output_actions": {"save": True, "download": False, "email": False},
            "confidence": 0.5,
            "error": str(e)
        }

def execute_report_query(report_type, filters, aggregations, group_by):
    """Execute the report query based on parameters"""
    db = Database()
    conn = db.get_connection()
    
    results = {
        "columns": [],
        "data": [],
        "summary": {},
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "record_count": 0
        }
    }
    
    date_filter = ""
    params = []
    
    if filters.get('date_range'):
        start = filters['date_range'].get('start')
        end = filters['date_range'].get('end')
        if start and end:
            date_filter = "AND date_field BETWEEN ? AND ?"
            params = [start, end]
    elif filters.get('period'):
        period = filters['period']
        today = datetime.now()
        if period == 'last_30_days':
            start = (today - timedelta(days=30)).strftime('%Y-%m-%d')
        elif period == 'last_90_days':
            start = (today - timedelta(days=90)).strftime('%Y-%m-%d')
        elif period == 'this_month':
            start = today.replace(day=1).strftime('%Y-%m-%d')
        elif period == 'this_quarter':
            quarter_start = ((today.month - 1) // 3) * 3 + 1
            start = today.replace(month=quarter_start, day=1).strftime('%Y-%m-%d')
        elif period == 'this_year' or period == 'ytd':
            start = today.replace(month=1, day=1).strftime('%Y-%m-%d')
        else:
            start = None
        
        if start:
            date_filter = "AND date_field >= ?"
            params = [start]
    
    try:
        if report_type == 'transaction':
            query = '''
                SELECT 
                    'Sales Order' as transaction_type,
                    so.order_number as reference,
                    so.order_date as date,
                    c.name as entity,
                    so.status,
                    so.total_amount as amount
                FROM sales_orders so
                LEFT JOIN customers c ON so.customer_id = c.id
                WHERE 1=1 ''' + date_filter.replace('date_field', 'so.order_date') + '''
                UNION ALL
                SELECT 
                    'Purchase Order' as transaction_type,
                    po.po_number as reference,
                    po.order_date as date,
                    s.name as entity,
                    po.status,
                    COALESCE(po.total_amount, 0) as amount
                FROM purchase_orders po
                LEFT JOIN suppliers s ON po.supplier_id = s.id
                WHERE 1=1 ''' + date_filter.replace('date_field', 'po.order_date') + '''
                ORDER BY date DESC
            '''
            
            rows = conn.execute(query, params + params).fetchall()
            results["columns"] = ["Transaction Type", "Reference", "Date", "Entity", "Status", "Amount"]
            results["data"] = [dict(row) for row in rows]
            
            total_sales = sum(r['amount'] or 0 for r in results["data"] if r['transaction_type'] == 'Sales Order')
            total_purchases = sum(r['amount'] or 0 for r in results["data"] if r['transaction_type'] == 'Purchase Order')
            
            results["summary"] = {
                "total_transactions": len(results["data"]),
                "total_sales": total_sales,
                "total_purchases": total_purchases,
                "net_flow": total_sales - total_purchases
            }
            
        elif report_type == 'financial':
            query = '''
                SELECT 
                    'Revenue' as category,
                    inv.invoice_number as reference,
                    inv.invoice_date as date,
                    c.name as entity,
                    inv.status,
                    inv.total_amount as amount,
                    COALESCE(inv.amount_paid, 0) as paid
                FROM invoices inv
                LEFT JOIN customers c ON inv.customer_id = c.id
                WHERE 1=1 ''' + date_filter.replace('date_field', 'inv.invoice_date') + '''
                UNION ALL
                SELECT 
                    'Expense' as category,
                    vi.invoice_number as reference,
                    vi.invoice_date as date,
                    s.name as entity,
                    vi.status,
                    vi.total_amount as amount,
                    COALESCE(vi.amount_paid, 0) as paid
                FROM vendor_invoices vi
                LEFT JOIN suppliers s ON vi.vendor_id = s.id
                WHERE 1=1 ''' + date_filter.replace('date_field', 'vi.invoice_date') + '''
                ORDER BY date DESC
            '''
            
            rows = conn.execute(query, params + params).fetchall()
            results["columns"] = ["Category", "Reference", "Date", "Entity", "Status", "Amount", "Paid"]
            results["data"] = [dict(row) for row in rows]
            
            total_revenue = sum(r['amount'] or 0 for r in results["data"] if r['category'] == 'Revenue')
            total_expenses = sum(r['amount'] or 0 for r in results["data"] if r['category'] == 'Expense')
            
            results["summary"] = {
                "total_revenue": total_revenue,
                "total_expenses": total_expenses,
                "net_income": total_revenue - total_expenses,
                "profit_margin": round((total_revenue - total_expenses) / total_revenue * 100, 2) if total_revenue > 0 else 0
            }
            
        elif report_type == 'operational':
            query = '''
                SELECT 
                    wo.wo_number as work_order,
                    wo.status,
                    wo.priority,
                    p.code as product_code,
                    p.name as product_name,
                    wo.quantity,
                    wo.start_date,
                    wo.due_date,
                    COALESCE(wo.material_cost, 0) as material_cost,
                    COALESCE(wo.labor_cost, 0) as labor_cost
                FROM work_orders wo
                LEFT JOIN products p ON wo.product_id = p.id
                WHERE 1=1 ''' + date_filter.replace('date_field', 'wo.start_date') + '''
                ORDER BY wo.start_date DESC
            '''
            
            rows = conn.execute(query, params).fetchall()
            results["columns"] = ["Work Order", "Status", "Priority", "Product Code", "Product Name", "Quantity", "Start Date", "Due Date", "Material Cost", "Labor Cost"]
            results["data"] = [dict(row) for row in rows]
            
            completed = len([r for r in results["data"] if r['status'] in ('Completed', 'Closed')])
            total_material = sum(r['material_cost'] or 0 for r in results["data"])
            total_labor = sum(r['labor_cost'] or 0 for r in results["data"])
            
            results["summary"] = {
                "total_work_orders": len(results["data"]),
                "completed": completed,
                "completion_rate": round(completed / len(results["data"]) * 100, 2) if results["data"] else 0,
                "total_material_cost": total_material,
                "total_labor_cost": total_labor,
                "total_cost": total_material + total_labor
            }
            
        elif report_type == 'inventory':
            query = '''
                SELECT 
                    p.code as product_code,
                    p.name as product_name,
                    p.product_type,
                    i.quantity,
                    COALESCE(i.unit_cost, p.cost, 0) as unit_cost,
                    (i.quantity * COALESCE(i.unit_cost, p.cost, 0)) as total_value,
                    i.reorder_point,
                    i.safety_stock,
                    CASE WHEN i.quantity <= i.reorder_point THEN 'Low Stock' ELSE 'OK' END as stock_status
                FROM inventory i
                JOIN products p ON i.product_id = p.id
                WHERE i.quantity > 0
                ORDER BY total_value DESC
            '''
            
            rows = conn.execute(query).fetchall()
            results["columns"] = ["Product Code", "Product Name", "Type", "Quantity", "Unit Cost", "Total Value", "Reorder Point", "Safety Stock", "Status"]
            results["data"] = [dict(row) for row in rows]
            
            total_value = sum(r['total_value'] or 0 for r in results["data"])
            low_stock = len([r for r in results["data"] if r['stock_status'] == 'Low Stock'])
            
            results["summary"] = {
                "total_items": len(results["data"]),
                "total_value": total_value,
                "low_stock_items": low_stock,
                "average_value": round(total_value / len(results["data"]), 2) if results["data"] else 0
            }
            
        elif report_type == 'compliance':
            query = '''
                SELECT 
                    'Audit Entry' as record_type,
                    al.timestamp as date,
                    al.table_name as entity,
                    al.action as action_type,
                    u.username as performed_by,
                    al.record_id as reference
                FROM audit_log al
                LEFT JOIN users u ON al.user_id = u.id
                WHERE 1=1 ''' + date_filter.replace('date_field', 'al.timestamp') + '''
                ORDER BY al.timestamp DESC
                LIMIT 500
            '''
            
            rows = conn.execute(query, params).fetchall()
            results["columns"] = ["Record Type", "Date", "Entity", "Action Type", "Performed By", "Reference"]
            results["data"] = [dict(row) for row in rows]
            
            results["summary"] = {
                "total_records": len(results["data"]),
                "create_actions": len([r for r in results["data"] if r['action_type'] == 'INSERT']),
                "update_actions": len([r for r in results["data"] if r['action_type'] == 'UPDATE']),
                "delete_actions": len([r for r in results["data"] if r['action_type'] == 'DELETE'])
            }
        
        else:
            results["columns"] = ["Info"]
            results["data"] = [{"Info": "Custom report type - data generation pending AI analysis"}]
            results["summary"] = {"status": "Custom report requires specific query definition"}
        
        results["metadata"]["record_count"] = len(results["data"])
        
    except Exception as e:
        results["error"] = str(e)
        results["columns"] = ["Error"]
        results["data"] = [{"Error": str(e)}]
    
    conn.close()
    return results

def generate_pdf_report(report_data, report_info):
    """Generate professional PDF report in landscape format"""
    from reportlab.lib.pagesizes import LETTER
    from reportlab.platypus import PageBreak, HRFlowable
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    
    buffer = io.BytesIO()
    page_width, page_height = landscape(LETTER)
    
    doc = SimpleDocTemplate(
        buffer, 
        pagesize=landscape(LETTER), 
        topMargin=0.75*inch, 
        bottomMargin=0.75*inch,
        leftMargin=0.5*inch,
        rightMargin=0.5*inch
    )
    
    available_width = page_width - 1.0*inch
    elements = []
    styles = getSampleStyleSheet()
    
    company_style = ParagraphStyle(
        'CompanyName',
        parent=styles['Heading1'],
        fontSize=24,
        spaceAfter=5,
        textColor=colors.HexColor('#1e3a5f'),
        alignment=TA_CENTER
    )
    
    title_style = ParagraphStyle(
        'ReportTitle',
        parent=styles['Heading1'],
        fontSize=16,
        spaceAfter=10,
        textColor=colors.HexColor('#2c5282'),
        alignment=TA_CENTER
    )
    
    subtitle_style = ParagraphStyle(
        'ReportSubtitle',
        parent=styles['Normal'],
        fontSize=10,
        spaceAfter=5,
        textColor=colors.HexColor('#4a5568'),
        alignment=TA_CENTER
    )
    
    section_header_style = ParagraphStyle(
        'SectionHeader',
        parent=styles['Heading2'],
        fontSize=12,
        spaceBefore=15,
        spaceAfter=10,
        textColor=colors.HexColor('#1e3a5f'),
        borderPadding=5
    )
    
    summary_style = ParagraphStyle(
        'SummaryText',
        parent=styles['Normal'],
        fontSize=10,
        leading=14,
        textColor=colors.HexColor('#2d3748')
    )
    
    footer_style = ParagraphStyle(
        'FooterText',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#718096'),
        alignment=TA_CENTER
    )
    
    elements.append(Paragraph("Dynamic.IQ-COREx", company_style))
    elements.append(Paragraph("Enterprise Resource Planning System", subtitle_style))
    elements.append(Spacer(1, 10))
    
    elements.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#1e3a5f')))
    elements.append(Spacer(1, 15))
    
    report_name = report_info.get('name', 'Report')
    elements.append(Paragraph(report_name.upper(), title_style))
    
    report_type = report_info.get('report_type', 'General').replace('_', ' ').title()
    elements.append(Paragraph(f"Report Type: {report_type}", subtitle_style))
    
    header_info = [
        [
            Paragraph(f"<b>Report Number:</b> {report_info.get('report_number', 'N/A')}", summary_style),
            Paragraph(f"<b>Generated:</b> {datetime.now().strftime('%B %d, %Y at %I:%M %p')}", summary_style),
            Paragraph(f"<b>Generated By:</b> {report_info.get('generated_by', 'System')}", summary_style)
        ]
    ]
    
    header_table = Table(header_info, colWidths=[available_width/3]*3)
    header_table.setStyle(TableStyle([
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
    ]))
    elements.append(header_table)
    elements.append(Spacer(1, 15))
    
    if report_info.get('description'):
        elements.append(Paragraph(f"<i>{report_info.get('description')}</i>", subtitle_style))
        elements.append(Spacer(1, 10))
    
    elements.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#cbd5e0')))
    elements.append(Spacer(1, 15))
    
    if report_data.get('summary'):
        elements.append(Paragraph("EXECUTIVE SUMMARY", section_header_style))
        
        summary_items = report_data['summary']
        summary_rows = []
        items = list(summary_items.items())
        
        for i in range(0, len(items), 3):
            row = []
            for j in range(3):
                if i + j < len(items):
                    key, val = items[i + j]
                    label = key.replace('_', ' ').title()
                    if isinstance(val, float):
                        formatted_val = f"${val:,.2f}" if 'revenue' in key.lower() or 'value' in key.lower() or 'total' in key.lower() else f"{val:,.2f}"
                    elif isinstance(val, int):
                        formatted_val = f"{val:,}"
                    else:
                        formatted_val = str(val)
                    row.append(Paragraph(f"<b>{label}:</b><br/><font size='12' color='#1e3a5f'>{formatted_val}</font>", summary_style))
                else:
                    row.append("")
            summary_rows.append(row)
        
        if summary_rows:
            summary_table = Table(summary_rows, colWidths=[available_width/3]*3)
            summary_table.setStyle(TableStyle([
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('TOPPADDING', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                ('LEFTPADDING', (0, 0), (-1, -1), 10),
                ('RIGHTPADDING', (0, 0), (-1, -1), 10),
                ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#f7fafc')),
                ('BOX', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
            ]))
            elements.append(summary_table)
        elements.append(Spacer(1, 20))
    
    if report_data.get('data') and report_data.get('columns'):
        elements.append(Paragraph("DETAILED DATA", section_header_style))
        
        header = report_data['columns']
        num_cols = len(header)
        
        if num_cols <= 4:
            col_widths = [available_width / num_cols] * num_cols
        elif num_cols <= 6:
            col_widths = [available_width / num_cols] * num_cols
        elif num_cols <= 8:
            col_widths = [available_width / num_cols] * num_cols
        else:
            col_widths = [available_width / min(num_cols, 10)] * min(num_cols, 10)
            header = header[:10]
        
        wrapped_header = [Paragraph(f"<b>{col}</b>", ParagraphStyle('HeaderCell', fontSize=8, textColor=colors.white, alignment=TA_CENTER)) for col in header]
        table_data = [wrapped_header]
        
        max_rows = 50 if len(report_data['data']) > 50 else len(report_data['data'])
        for row in report_data['data'][:max_rows]:
            row_values = []
            for col in header:
                col_key = col.lower().replace(' ', '_')
                val = row.get(col_key, row.get(col, ''))
                if isinstance(val, float):
                    val = f"{val:,.2f}"
                elif isinstance(val, int):
                    val = f"{val:,}"
                elif val is None:
                    val = '-'
                cell_text = str(val)[:40]
                row_values.append(Paragraph(cell_text, ParagraphStyle('DataCell', fontSize=8, alignment=TA_CENTER)))
            table_data.append(row_values)
        
        table = Table(table_data, colWidths=col_widths, repeatRows=1)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1e3a5f')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
            ('TOPPADDING', (0, 1), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
            ('BACKGROUND', (0, 1), (-1, -1), colors.white),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#cbd5e0')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f7fafc')])
        ]))
        elements.append(table)
        
        if len(report_data['data']) > max_rows:
            elements.append(Spacer(1, 10))
            elements.append(Paragraph(f"<i>Showing {max_rows} of {len(report_data['data'])} records. Download Excel for complete data.</i>", footer_style))
    
    elements.append(Spacer(1, 30))
    elements.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#cbd5e0')))
    elements.append(Spacer(1, 10))
    elements.append(Paragraph(f"Report generated by Dynamic.IQ-COREx Report Logic Engine | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", footer_style))
    elements.append(Paragraph("Confidential - For Internal Use Only", footer_style))
    
    doc.build(elements)
    buffer.seek(0)
    return buffer

def generate_excel_report(report_data, report_info):
    """Generate Excel from report data"""
    buffer = io.BytesIO()
    
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        if report_data.get('data'):
            df = pd.DataFrame(report_data['data'])
            df.to_excel(writer, sheet_name='Data', index=False)
        
        if report_data.get('summary'):
            summary_df = pd.DataFrame([report_data['summary']])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        meta_df = pd.DataFrame([{
            'Report Name': report_info.get('name', 'Report'),
            'Report Number': report_info.get('report_number', 'N/A'),
            'Generated At': datetime.now().isoformat(),
            'Generated By': report_info.get('generated_by', 'System')
        }])
        meta_df.to_excel(writer, sheet_name='Metadata', index=False)
    
    buffer.seek(0)
    return buffer

def send_report_email(report_id, recipients, subject, message, attachment_path=None):
    """Send report via email using Brevo"""
    import sib_api_v3_sdk
    from sib_api_v3_sdk.rest import ApiException
    
    api_key = os.environ.get('BREVO_API_KEY')
    from_email = os.environ.get('BREVO_FROM_EMAIL', 'noreply@dynamiciq.com')
    
    if not api_key:
        return {"success": False, "error": "Email service not configured"}
    
    configuration = sib_api_v3_sdk.Configuration()
    configuration.api_key['api-key'] = api_key
    
    api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))
    
    to_list = [{"email": email.strip(), "name": email.split('@')[0]} for email in recipients if email]
    
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <h2 style="color: #1e3a5f;">Dynamic.IQ-COREx Report</h2>
        <p>{message}</p>
        <p style="color: #666; font-size: 12px;">This report was generated automatically by the COREx Report Logic system.</p>
    </body>
    </html>
    """
    
    send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(
        to=to_list,
        html_content=html_content,
        sender={"email": from_email, "name": "COREx Report Logic"},
        subject=subject
    )
    
    try:
        api_response = api_instance.send_transac_email(send_smtp_email)
        return {"success": True, "message_id": api_response.message_id}
    except ApiException as e:
        return {"success": False, "error": str(e)}


@report_logic_bp.route('/report-logic')
@login_required
def report_logic_dashboard():
    """Report Logic main dashboard"""
    db = Database()
    conn = db.get_connection()
    
    recent_reports = conn.execute('''
        SELECT rr.*, u.username as generated_by_name
        FROM report_repository rr
        LEFT JOIN users u ON rr.generated_by = u.id
        ORDER BY rr.generated_at DESC
        LIMIT 10
    ''').fetchall()
    
    stats = conn.execute('''
        SELECT 
            COUNT(*) as total_reports,
            SUM(CASE WHEN ai_generated = 1 THEN 1 ELSE 0 END) as ai_reports,
            SUM(CASE WHEN DATE(generated_at) = DATE('now') THEN 1 ELSE 0 END) as today_reports
        FROM report_repository
    ''').fetchone()
    
    conn.close()
    
    return render_template('report_logic/dashboard.html',
                          recent_reports=recent_reports,
                          stats=stats,
                          report_types=REPORT_TYPES)


@report_logic_bp.route('/api/report-logic/command', methods=['POST'])
@login_required
def process_ai_command():
    """Process AI natural language command"""
    data = request.get_json()
    command = data.get('command', '')
    
    if not command:
        return jsonify({'success': False, 'error': 'No command provided'})
    
    parsed = parse_ai_command(command)
    
    report_data = execute_report_query(
        parsed.get('report_type', 'custom'),
        parsed.get('filters', {}),
        parsed.get('aggregations', []),
        parsed.get('group_by', [])
    )
    
    response = {
        'success': True,
        'parsed_command': parsed,
        'report_preview': {
            'columns': report_data.get('columns', []),
            'data': report_data.get('data', [])[:10],
            'summary': report_data.get('summary', {}),
            'total_records': report_data.get('metadata', {}).get('record_count', 0)
        },
        'suggested_actions': []
    }
    
    if parsed.get('output_actions', {}).get('save'):
        response['suggested_actions'].append('save')
    if parsed.get('output_actions', {}).get('download'):
        response['suggested_actions'].append('download')
    if parsed.get('output_actions', {}).get('email'):
        response['suggested_actions'].append('email')
    
    if not response['suggested_actions']:
        response['suggested_actions'] = ['save', 'download', 'email']
    
    response['confidence'] = parsed.get('confidence', 0.8)
    
    session['pending_report'] = {
        'parsed': parsed,
        'data': report_data,
        'command': command
    }
    
    return jsonify(response)


@report_logic_bp.route('/api/report-logic/generate', methods=['POST'])
@login_required
def generate_report():
    """Generate and save the report"""
    data = request.get_json()
    action = data.get('action', 'save')
    
    pending = session.get('pending_report')
    if not pending:
        return jsonify({'success': False, 'error': 'No pending report. Please issue a command first.'})
    
    parsed = pending['parsed']
    report_data = pending['data']
    command = pending['command']
    
    db = Database()
    conn = db.get_connection()
    
    report_number = generate_report_number()
    report_name = parsed.get('name', 'AI Generated Report')
    report_type = parsed.get('report_type', 'custom')
    
    conn.execute('''
        INSERT INTO report_repository 
        (report_number, name, description, report_type, parameters, generated_data, 
         ai_generated, ai_command, generated_by, confidence_score, access_roles)
        VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
    ''', (
        report_number,
        report_name,
        parsed.get('description', ''),
        report_type,
        json.dumps(parsed.get('filters', {})),
        json.dumps(report_data),
        command,
        session.get('user_id'),
        parsed.get('confidence', 0.8),
        'Admin,Planner,Manager'
    ))
    conn.commit()
    
    report_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    
    log_report_action(
        report_id, 'CREATE', f'Report {report_number} generated via AI command',
        ai_command=command,
        data_sources=json.dumps(REPORT_TYPES.get(report_type, {}).get('data_sources', [])),
        parameters=json.dumps(parsed.get('filters', {})),
        result_summary=json.dumps(report_data.get('summary', {})),
        user_id=session.get('user_id')
    )
    
    conn.close()
    
    session.pop('pending_report', None)
    
    result = {
        'success': True,
        'report_id': report_id,
        'report_number': report_number,
        'message': f'Report {report_number} has been generated and saved.'
    }
    
    if action == 'download_pdf':
        result['download_url'] = url_for('report_logic_routes.download_report', id=report_id, format='pdf')
    elif action == 'download_excel':
        result['download_url'] = url_for('report_logic_routes.download_report', id=report_id, format='xlsx')
    elif action == 'download_csv':
        result['download_url'] = url_for('report_logic_routes.download_report', id=report_id, format='csv')
    
    return jsonify(result)


@report_logic_bp.route('/report-logic/download/<int:id>')
@login_required
def download_report(id):
    """Download report in specified format"""
    file_format = request.args.get('format', 'pdf')
    
    db = Database()
    conn = db.get_connection()
    
    report = conn.execute('''
        SELECT * FROM report_repository WHERE id = ?
    ''', (id,)).fetchone()
    
    if not report:
        conn.close()
        flash('Report not found', 'danger')
        return redirect(url_for('report_logic_routes.report_logic_dashboard'))
    
    report_data = json.loads(report['generated_data']) if report['generated_data'] else {}
    report_info = {
        'name': report['name'],
        'report_number': report['report_number'],
        'generated_by': 'System'
    }
    
    conn.execute('''
        INSERT INTO report_distribution (report_id, distribution_type, downloaded_at, created_by)
        VALUES (?, 'download', CURRENT_TIMESTAMP, ?)
    ''', (id, session.get('user_id')))
    conn.commit()
    
    log_report_action(report['id'], 'DOWNLOAD', f'Report downloaded as {file_format.upper()}', user_id=session.get('user_id'))
    
    conn.close()
    
    if file_format == 'pdf':
        buffer = generate_pdf_report(report_data, report_info)
        return send_file(
            buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=f"{report['report_number']}.pdf"
        )
    
    elif file_format == 'xlsx':
        buffer = generate_excel_report(report_data, report_info)
        return send_file(
            buffer,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f"{report['report_number']}.xlsx"
        )
    
    elif file_format == 'csv':
        if report_data.get('data'):
            df = pd.DataFrame(report_data['data'])
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            buffer.seek(0)
            return send_file(
                io.BytesIO(buffer.getvalue().encode()),
                mimetype='text/csv',
                as_attachment=True,
                download_name=f"{report['report_number']}.csv"
            )
    
    flash('Invalid format specified', 'danger')
    return redirect(url_for('report_logic_routes.report_logic_dashboard'))


@report_logic_bp.route('/api/report-logic/email', methods=['POST'])
@login_required
def email_report():
    """Email report to recipients"""
    data = request.get_json()
    report_id = data.get('report_id')
    recipients = data.get('recipients', [])
    subject = data.get('subject', 'COREx Report')
    message = data.get('message', 'Please find the attached report.')
    
    if not report_id or not recipients:
        return jsonify({'success': False, 'error': 'Report ID and recipients are required'})
    
    db = Database()
    conn = db.get_connection()
    
    report = conn.execute('SELECT * FROM report_repository WHERE id = ?', (report_id,)).fetchone()
    
    if not report:
        conn.close()
        return jsonify({'success': False, 'error': 'Report not found'})
    
    result = send_report_email(report_id, recipients, subject, message)
    
    for recipient in recipients:
        conn.execute('''
            INSERT INTO report_distribution 
            (report_id, distribution_type, recipient_email, subject, message, 
             delivery_status, sent_at, created_by)
            VALUES (?, 'email', ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
        ''', (
            report_id, recipient.strip(), subject, message,
            'Sent' if result['success'] else 'Failed',
            session.get('user_id')
        ))
    
    conn.commit()
    
    log_report_action(
        report_id, 'EMAIL', 
        f'Report emailed to {len(recipients)} recipient(s)',
        result_summary=json.dumps({'recipients': recipients, 'status': result}),
        user_id=session.get('user_id')
    )
    
    conn.close()
    
    return jsonify(result)


@report_logic_bp.route('/report-logic/repository')
@login_required
def report_repository():
    """View all saved reports"""
    db = Database()
    conn = db.get_connection()
    
    filter_type = request.args.get('type', '')
    search = request.args.get('search', '')
    
    query = '''
        SELECT rr.*, u.username as generated_by_name,
               (SELECT COUNT(*) FROM report_distribution rd WHERE rd.report_id = rr.id) as distribution_count
        FROM report_repository rr
        LEFT JOIN users u ON rr.generated_by = u.id
        WHERE rr.is_archived = 0
    '''
    params = []
    
    if filter_type:
        query += ' AND rr.report_type = ?'
        params.append(filter_type)
    
    if search:
        query += ' AND (rr.name LIKE ? OR rr.report_number LIKE ? OR rr.description LIKE ?)'
        params.extend([f'%{search}%', f'%{search}%', f'%{search}%'])
    
    query += ' ORDER BY rr.generated_at DESC'
    
    reports = conn.execute(query, params).fetchall()
    conn.close()
    
    return render_template('report_logic/repository.html',
                          reports=reports,
                          report_types=REPORT_TYPES,
                          filter_type=filter_type,
                          search=search)


@report_logic_bp.route('/report-logic/view/<int:id>')
@login_required
def view_report(id):
    """View a specific report"""
    db = Database()
    conn = db.get_connection()
    
    report = conn.execute('''
        SELECT rr.*, u.username as generated_by_name
        FROM report_repository rr
        LEFT JOIN users u ON rr.generated_by = u.id
        WHERE rr.id = ?
    ''', (id,)).fetchone()
    
    if not report:
        conn.close()
        flash('Report not found', 'danger')
        return redirect(url_for('report_logic_routes.report_repository'))
    
    distributions = conn.execute('''
        SELECT * FROM report_distribution 
        WHERE report_id = ? 
        ORDER BY created_at DESC
    ''', (id,)).fetchall()
    
    audit_log = conn.execute('''
        SELECT ral.*, u.username 
        FROM report_audit_log ral
        LEFT JOIN users u ON ral.performed_by = u.id
        WHERE ral.report_id = ?
        ORDER BY ral.performed_at DESC
    ''', (id,)).fetchall()
    
    conn.close()
    
    report_data = json.loads(report['generated_data']) if report['generated_data'] else {}
    
    return render_template('report_logic/view.html',
                          report=report,
                          report_data=report_data,
                          distributions=distributions,
                          audit_log=audit_log)


@report_logic_bp.route('/report-logic/delete/<int:id>', methods=['POST'])
@login_required
@role_required(['Admin', 'Planner', 'Manager'])
def delete_report(id):
    """Delete a report from the repository"""
    db = Database()
    conn = db.get_connection()
    
    report = conn.execute('SELECT * FROM report_repository WHERE id = ?', (id,)).fetchone()
    
    if not report:
        conn.close()
        flash('Report not found', 'danger')
        return redirect(url_for('report_logic_routes.report_repository'))
    
    report_number = report['report_number']
    report_name = report['name']
    
    log_report_action(
        id, 'DELETE', 
        f'Report {report_number} deleted',
        user_id=session.get('user_id')
    )
    
    conn.execute('DELETE FROM report_distribution WHERE report_id = ?', (id,))
    conn.execute('DELETE FROM report_audit_log WHERE report_id = ?', (id,))
    conn.execute('DELETE FROM report_subscriptions WHERE report_id = ?', (id,))
    conn.execute('DELETE FROM report_repository WHERE id = ?', (id,))
    
    conn.commit()
    conn.close()
    
    AuditLogger.log('report_repository', id, 'DELETE', session.get('user_id'),
                   {'report_number': report_number, 'name': report_name})
    
    flash(f'Report {report_number} has been deleted successfully', 'success')
    return redirect(url_for('report_logic_routes.report_repository'))


@report_logic_bp.route('/api/report-logic/search', methods=['POST'])
@login_required
def search_reports():
    """Search reports via AI command"""
    data = request.get_json()
    query = data.get('query', '')
    
    db = Database()
    conn = db.get_connection()
    
    reports = conn.execute('''
        SELECT id, report_number, name, report_type, generated_at
        FROM report_repository
        WHERE name LIKE ? OR description LIKE ? OR ai_command LIKE ?
        ORDER BY generated_at DESC
        LIMIT 20
    ''', (f'%{query}%', f'%{query}%', f'%{query}%')).fetchall()
    
    conn.close()
    
    return jsonify({
        'success': True,
        'reports': [dict(r) for r in reports]
    })
