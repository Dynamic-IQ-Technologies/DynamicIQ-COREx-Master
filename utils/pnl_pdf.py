"""
Professional Profit & Loss Statement PDF Generator
Creates elegant, professional P&L statements using ReportLab
"""
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from io import BytesIO
from datetime import datetime


class PnLPDFGenerator:
    """Generate professional Profit & Loss Statement PDFs"""
    
    def __init__(self, revenue_data, efficiency_data, company_name, period_start=None, period_end=None):
        self.revenue = revenue_data
        self.efficiency = efficiency_data
        self.company_name = company_name or 'Company'
        self.period_start = period_start or datetime.now().replace(month=1, day=1).strftime('%B %d, %Y')
        self.period_end = period_end or datetime.now().strftime('%B %d, %Y')
        self.primary_color = colors.Color(0.1, 0.2, 0.4)
        self.accent_color = colors.Color(0.15, 0.35, 0.6)
        
    def _create_styles(self):
        """Create custom paragraph styles"""
        styles = getSampleStyleSheet()
        
        styles.add(ParagraphStyle(
            name='CompanyTitle',
            parent=styles['Heading1'],
            fontSize=18,
            textColor=self.primary_color,
            alignment=TA_CENTER,
            spaceAfter=4,
            fontName='Helvetica-Bold'
        ))
        
        styles.add(ParagraphStyle(
            name='ReportTitle',
            parent=styles['Heading1'],
            fontSize=14,
            textColor=colors.Color(0.3, 0.3, 0.3),
            alignment=TA_CENTER,
            spaceAfter=4,
            fontName='Helvetica-Bold'
        ))
        
        styles.add(ParagraphStyle(
            name='PeriodText',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.Color(0.5, 0.5, 0.5),
            alignment=TA_CENTER,
            spaceAfter=20
        ))
        
        styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=styles['Heading2'],
            fontSize=11,
            textColor=self.primary_color,
            spaceBefore=16,
            spaceAfter=8,
            fontName='Helvetica-Bold',
            leftIndent=0
        ))
        
        styles.add(ParagraphStyle(
            name='LineItem',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.Color(0.2, 0.2, 0.2),
            leftIndent=20
        ))
        
        styles.add(ParagraphStyle(
            name='SubTotal',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.Color(0.2, 0.2, 0.2),
            fontName='Helvetica-Bold',
            leftIndent=10
        ))
        
        styles.add(ParagraphStyle(
            name='GrandTotal',
            parent=styles['Normal'],
            fontSize=12,
            textColor=self.primary_color,
            fontName='Helvetica-Bold',
            leftIndent=0
        ))
        
        styles.add(ParagraphStyle(
            name='FooterText',
            parent=styles['Normal'],
            fontSize=8,
            textColor=colors.Color(0.5, 0.5, 0.5),
            alignment=TA_CENTER
        ))
        
        return styles
    
    def _format_currency(self, value):
        """Format value as currency"""
        if value < 0:
            return f"($ {abs(value):,.2f})"
        return f"$ {value:,.2f}"
    
    def _format_percentage(self, value):
        """Format value as percentage"""
        return f"{value:.1f}%"
    
    def _create_line_item_table(self, items, styles):
        """Create a table for line items with amounts"""
        table_data = []
        for item in items:
            label = item.get('label', '')
            amount = item.get('amount', 0)
            is_subtotal = item.get('is_subtotal', False)
            is_total = item.get('is_total', False)
            indent = item.get('indent', 0)
            
            padding = '    ' * indent
            formatted_amount = self._format_currency(amount)
            
            if is_total:
                table_data.append([
                    Paragraph(f"<b>{padding}{label}</b>", styles['GrandTotal']),
                    Paragraph(f"<b>{formatted_amount}</b>", styles['GrandTotal'])
                ])
            elif is_subtotal:
                table_data.append([
                    Paragraph(f"<b>{padding}{label}</b>", styles['SubTotal']),
                    Paragraph(f"<b>{formatted_amount}</b>", styles['SubTotal'])
                ])
            else:
                table_data.append([
                    Paragraph(f"{padding}{label}", styles['LineItem']),
                    Paragraph(formatted_amount, styles['LineItem'])
                ])
        
        if not table_data:
            return None
            
        table = Table(table_data, colWidths=[4.5*inch, 2*inch])
        table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        
        return table
    
    def generate(self):
        """Generate the P&L PDF and return as bytes"""
        buffer = BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=letter,
            rightMargin=0.75*inch,
            leftMargin=0.75*inch,
            topMargin=0.75*inch,
            bottomMargin=0.75*inch
        )
        
        styles = self._create_styles()
        story = []
        
        story.append(Paragraph(self.company_name, styles['CompanyTitle']))
        story.append(Paragraph("Profit & Loss Statement", styles['ReportTitle']))
        story.append(Paragraph(f"For the Period: {self.period_start} to {self.period_end}", styles['PeriodText']))
        
        story.append(HRFlowable(width="100%", thickness=2, color=self.primary_color))
        story.append(Spacer(1, 10))
        
        story.append(Paragraph("REVENUE", styles['SectionHeader']))
        revenue_items = [
            {'label': 'Sales Revenue', 'amount': self.revenue.get('total_revenue', 0), 'indent': 1},
            {'label': 'Revenue (Month-to-Date)', 'amount': self.revenue.get('revenue_mtd', 0), 'indent': 2},
            {'label': 'Revenue (Year-to-Date)', 'amount': self.revenue.get('revenue_ytd', 0), 'indent': 2},
            {'label': 'Total Revenue', 'amount': self.revenue.get('total_revenue', 0), 'is_subtotal': True},
        ]
        revenue_table = self._create_line_item_table(revenue_items, styles)
        if revenue_table:
            story.append(revenue_table)
        
        story.append(Spacer(1, 10))
        story.append(HRFlowable(width="100%", thickness=0.5, color=colors.Color(0.8, 0.8, 0.8)))
        
        story.append(Paragraph("COST OF GOODS SOLD", styles['SectionHeader']))
        cogs_items = [
            {'label': 'Direct Materials', 'amount': self.revenue.get('total_material_cost', 0), 'indent': 1},
            {'label': 'Direct Labor', 'amount': self.revenue.get('total_labor_cost', 0), 'indent': 1},
            {'label': 'Manufacturing Overhead', 'amount': self.revenue.get('total_overhead', 0), 'indent': 1},
            {'label': 'Total Cost of Goods Sold', 'amount': self.revenue.get('total_cogs', 0), 'is_subtotal': True},
        ]
        cogs_table = self._create_line_item_table(cogs_items, styles)
        if cogs_table:
            story.append(cogs_table)
        
        story.append(Spacer(1, 10))
        story.append(HRFlowable(width="100%", thickness=1.5, color=self.accent_color))
        
        gross_profit_items = [
            {'label': 'GROSS PROFIT', 'amount': self.revenue.get('gross_profit', 0), 'is_total': True},
        ]
        gross_profit_table = self._create_line_item_table(gross_profit_items, styles)
        if gross_profit_table:
            story.append(gross_profit_table)
        
        story.append(Spacer(1, 10))
        story.append(HRFlowable(width="100%", thickness=0.5, color=colors.Color(0.8, 0.8, 0.8)))
        
        monthly_labor = self.efficiency.get('total_monthly_labor_expense', 0)
        story.append(Paragraph("OPERATING EXPENSES", styles['SectionHeader']))
        opex_items = [
            {'label': 'Salaries & Wages', 'amount': monthly_labor, 'indent': 1},
            {'label': 'Total Operating Expenses', 'amount': monthly_labor, 'is_subtotal': True},
        ]
        opex_table = self._create_line_item_table(opex_items, styles)
        if opex_table:
            story.append(opex_table)
        
        story.append(Spacer(1, 10))
        story.append(HRFlowable(width="100%", thickness=2, color=self.primary_color))
        
        operating_income = self.revenue.get('gross_profit', 0) - monthly_labor
        net_income_items = [
            {'label': 'OPERATING INCOME', 'amount': operating_income, 'is_total': True},
        ]
        net_income_table = self._create_line_item_table(net_income_items, styles)
        if net_income_table:
            story.append(net_income_table)
        
        story.append(Spacer(1, 30))
        story.append(HRFlowable(width="100%", thickness=0.5, color=colors.Color(0.8, 0.8, 0.8)))
        story.append(Spacer(1, 10))
        
        story.append(Paragraph("KEY METRICS", styles['SectionHeader']))
        
        metrics_data = [
            ['Metric', 'Value'],
            ['Gross Margin', self._format_percentage(self.revenue.get('gross_margin', 0))],
            ['Average Order Value', self._format_currency(self.revenue.get('avg_order_value', 0))],
            ['Total Orders', str(self.revenue.get('order_count', 0))],
            ['Total Customers', str(self.revenue.get('customer_count', 0))],
            ['Revenue per Customer', self._format_currency(self.revenue.get('revenue_per_customer', 0))],
            ['Revenue Growth (MoM)', self._format_percentage(self.revenue.get('revenue_growth', 0))],
        ]
        
        metrics_table = Table(metrics_data, colWidths=[3.5*inch, 2*inch])
        metrics_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), self.primary_color),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('LEFTPADDING', (0, 0), (-1, -1), 10),
            ('RIGHTPADDING', (0, 0), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.Color(0.8, 0.8, 0.8)),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.Color(0.97, 0.97, 0.97), colors.white]),
        ]))
        story.append(metrics_table)
        
        story.append(Spacer(1, 40))
        story.append(HRFlowable(width="100%", thickness=0.5, color=colors.Color(0.8, 0.8, 0.8)))
        story.append(Spacer(1, 10))
        
        generated_date = datetime.now().strftime('%B %d, %Y at %I:%M %p')
        story.append(Paragraph(f"Generated on {generated_date}", styles['FooterText']))
        story.append(Paragraph("This is a system-generated report from Dynamic.IQ-COREx", styles['FooterText']))
        story.append(Paragraph("For internal use only - Not audited financial statements", styles['FooterText']))
        
        doc.build(story)
        buffer.seek(0)
        return buffer.getvalue()
