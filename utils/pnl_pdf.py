"""
Professional Profit & Loss Statement PDF Generator
Creates GAAP-compliant P&L statements following corporate accounting practices
"""
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable, PageBreak
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from io import BytesIO
from datetime import datetime


class PnLPDFGenerator:
    """Generate GAAP-compliant Profit & Loss Statement PDFs"""
    
    def __init__(self, financial_data, company_name, period_start=None, period_end=None):
        self.data = financial_data
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
            spaceBefore=12,
            spaceAfter=6,
            fontName='Helvetica-Bold',
            leftIndent=0
        ))
        
        styles.add(ParagraphStyle(
            name='LineItem',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.Color(0.2, 0.2, 0.2),
            leftIndent=20
        ))
        
        styles.add(ParagraphStyle(
            name='SubTotal',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.Color(0.2, 0.2, 0.2),
            fontName='Helvetica-Bold',
            leftIndent=10
        ))
        
        styles.add(ParagraphStyle(
            name='GrandTotal',
            parent=styles['Normal'],
            fontSize=11,
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
        
        styles.add(ParagraphStyle(
            name='NoteText',
            parent=styles['Normal'],
            fontSize=8,
            textColor=colors.Color(0.4, 0.4, 0.4),
            leftIndent=20,
            spaceBefore=2
        ))
        
        return styles
    
    def _format_currency(self, value):
        """Format value as currency with parentheses for negatives"""
        if value is None:
            value = 0
        if value < 0:
            return f"({abs(value):,.2f})"
        return f"{value:,.2f}"
    
    def _format_percentage(self, value):
        """Format value as percentage"""
        if value is None:
            value = 0
        return f"{value:.1f}%"
    
    def _create_line_item_table(self, items, styles, show_notes=False):
        """Create a table for line items with amounts"""
        table_data = []
        col_widths = [4.2*inch, 1.8*inch] if not show_notes else [3.5*inch, 1.5*inch, 1.5*inch]
        
        for item in items:
            label = item.get('label', '')
            amount = item.get('amount', 0)
            note = item.get('note', '')
            is_subtotal = item.get('is_subtotal', False)
            is_total = item.get('is_total', False)
            is_header = item.get('is_header', False)
            indent = item.get('indent', 0)
            show_line = item.get('show_line', False)
            double_line = item.get('double_line', False)
            
            padding = '&nbsp;&nbsp;&nbsp;&nbsp;' * indent
            formatted_amount = self._format_currency(amount) if amount is not None else ''
            
            if is_header:
                row = [
                    Paragraph(f"<b>{label}</b>", styles['SectionHeader']),
                    Paragraph('', styles['LineItem'])
                ]
            elif is_total:
                row = [
                    Paragraph(f"<b>{padding}{label}</b>", styles['GrandTotal']),
                    Paragraph(f"<b>{formatted_amount}</b>", styles['GrandTotal'])
                ]
            elif is_subtotal:
                row = [
                    Paragraph(f"<b>{padding}{label}</b>", styles['SubTotal']),
                    Paragraph(f"<b>{formatted_amount}</b>", styles['SubTotal'])
                ]
            else:
                row = [
                    Paragraph(f"{padding}{label}", styles['LineItem']),
                    Paragraph(formatted_amount, styles['LineItem'])
                ]
            
            if show_notes:
                row.append(Paragraph(note, styles['NoteText']))
                
            table_data.append(row)
        
        if not table_data:
            return None
        
        table = Table(table_data, colWidths=col_widths)
        
        table_style = [
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]
        
        for idx, item in enumerate(items):
            if item.get('show_line'):
                table_style.append(('LINEABOVE', (1, idx), (1, idx), 0.5, colors.black))
            if item.get('double_line'):
                table_style.append(('LINEABOVE', (1, idx), (1, idx), 1, colors.black))
                table_style.append(('LINEBELOW', (1, idx), (1, idx), 1, colors.black))
        
        table.setStyle(TableStyle(table_style))
        return table
    
    def generate(self):
        """Generate the P&L PDF following GAAP format"""
        buffer = BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=letter,
            rightMargin=0.75*inch,
            leftMargin=0.75*inch,
            topMargin=0.6*inch,
            bottomMargin=0.6*inch
        )
        
        styles = self._create_styles()
        story = []
        
        story.append(Paragraph(self.company_name, styles['CompanyTitle']))
        story.append(Paragraph("Statement of Income", styles['ReportTitle']))
        story.append(Paragraph("(Profit and Loss Statement)", styles['PeriodText']))
        story.append(Paragraph(f"For the Period Ended {self.period_end}", styles['PeriodText']))
        
        story.append(HRFlowable(width="100%", thickness=2, color=self.primary_color))
        story.append(Spacer(1, 8))
        
        net_sales = self.data.get('net_sales', 0)
        service_revenue = self.data.get('service_revenue', 0)
        other_revenue = self.data.get('other_revenue', 0)
        total_revenue = net_sales + service_revenue + other_revenue
        
        revenue_items = [
            {'label': 'REVENUE', 'is_header': True},
            {'label': 'Net Sales', 'amount': net_sales, 'indent': 1},
            {'label': 'Service Revenue', 'amount': service_revenue, 'indent': 1},
            {'label': 'Other Revenue', 'amount': other_revenue, 'indent': 1},
            {'label': 'Total Revenue', 'amount': total_revenue, 'is_subtotal': True, 'show_line': True},
        ]
        revenue_table = self._create_line_item_table(revenue_items, styles)
        if revenue_table:
            story.append(revenue_table)
        
        story.append(Spacer(1, 6))
        
        beginning_inventory = self.data.get('beginning_inventory', 0)
        purchases = self.data.get('purchases', 0)
        direct_labor = self.data.get('direct_labor', 0)
        manufacturing_overhead = self.data.get('manufacturing_overhead', 0)
        ending_inventory = self.data.get('ending_inventory', 0)
        total_cogs = self.data.get('total_cogs', 0)
        
        cogs_items = [
            {'label': 'COST OF GOODS SOLD', 'is_header': True},
            {'label': 'Beginning Inventory', 'amount': beginning_inventory, 'indent': 1},
            {'label': 'Add: Purchases/Raw Materials', 'amount': purchases, 'indent': 1},
            {'label': 'Add: Direct Labor', 'amount': direct_labor, 'indent': 1},
            {'label': 'Add: Manufacturing Overhead', 'amount': manufacturing_overhead, 'indent': 1},
            {'label': 'Less: Ending Inventory', 'amount': -ending_inventory if ending_inventory > 0 else 0, 'indent': 1},
            {'label': 'Total Cost of Goods Sold', 'amount': total_cogs, 'is_subtotal': True, 'show_line': True},
        ]
        cogs_table = self._create_line_item_table(cogs_items, styles)
        if cogs_table:
            story.append(cogs_table)
        
        story.append(Spacer(1, 6))
        
        gross_profit = total_revenue - total_cogs
        gross_profit_items = [
            {'label': 'GROSS PROFIT', 'amount': gross_profit, 'is_total': True, 'show_line': True},
        ]
        gross_table = self._create_line_item_table(gross_profit_items, styles)
        if gross_table:
            story.append(gross_table)
        
        story.append(Spacer(1, 6))
        
        selling_expenses = self.data.get('selling_expenses', 0)
        salaries_wages = self.data.get('salaries_wages', 0)
        rent_expense = self.data.get('rent_expense', 0)
        utilities_expense = self.data.get('utilities_expense', 0)
        insurance_expense = self.data.get('insurance_expense', 0)
        depreciation = self.data.get('depreciation', 0)
        amortization = self.data.get('amortization', 0)
        office_supplies = self.data.get('office_supplies', 0)
        professional_fees = self.data.get('professional_fees', 0)
        marketing_advertising = self.data.get('marketing_advertising', 0)
        travel_entertainment = self.data.get('travel_entertainment', 0)
        repairs_maintenance = self.data.get('repairs_maintenance', 0)
        other_operating = self.data.get('other_operating_expenses', 0)
        
        total_operating_expenses = (selling_expenses + salaries_wages + rent_expense + 
                                   utilities_expense + insurance_expense + depreciation + 
                                   amortization + office_supplies + professional_fees +
                                   marketing_advertising + travel_entertainment + 
                                   repairs_maintenance + other_operating)
        
        opex_items = [
            {'label': 'OPERATING EXPENSES', 'is_header': True},
            {'label': 'Selling Expenses', 'amount': selling_expenses, 'indent': 1},
            {'label': 'Salaries and Wages', 'amount': salaries_wages, 'indent': 1},
            {'label': 'Rent Expense', 'amount': rent_expense, 'indent': 1},
            {'label': 'Utilities', 'amount': utilities_expense, 'indent': 1},
            {'label': 'Insurance', 'amount': insurance_expense, 'indent': 1},
            {'label': 'Depreciation Expense', 'amount': depreciation, 'indent': 1},
            {'label': 'Amortization Expense', 'amount': amortization, 'indent': 1},
            {'label': 'Office Supplies', 'amount': office_supplies, 'indent': 1},
            {'label': 'Professional Fees', 'amount': professional_fees, 'indent': 1},
            {'label': 'Marketing and Advertising', 'amount': marketing_advertising, 'indent': 1},
            {'label': 'Travel and Entertainment', 'amount': travel_entertainment, 'indent': 1},
            {'label': 'Repairs and Maintenance', 'amount': repairs_maintenance, 'indent': 1},
            {'label': 'Other Operating Expenses', 'amount': other_operating, 'indent': 1},
            {'label': 'Total Operating Expenses', 'amount': total_operating_expenses, 'is_subtotal': True, 'show_line': True},
        ]
        opex_table = self._create_line_item_table(opex_items, styles)
        if opex_table:
            story.append(opex_table)
        
        story.append(Spacer(1, 6))
        
        operating_income = gross_profit - total_operating_expenses
        ebit_items = [
            {'label': 'OPERATING INCOME (EBIT)', 'amount': operating_income, 'is_total': True, 'show_line': True},
        ]
        ebit_table = self._create_line_item_table(ebit_items, styles)
        if ebit_table:
            story.append(ebit_table)
        
        story.append(Spacer(1, 6))
        
        interest_income = self.data.get('interest_income', 0)
        interest_expense = self.data.get('interest_expense', 0)
        gain_loss_assets = self.data.get('gain_loss_assets', 0)
        other_income = self.data.get('other_income_expense', 0)
        total_other = interest_income - interest_expense + gain_loss_assets + other_income
        
        other_items = [
            {'label': 'OTHER INCOME (EXPENSES)', 'is_header': True},
            {'label': 'Interest Income', 'amount': interest_income, 'indent': 1},
            {'label': 'Interest Expense', 'amount': -interest_expense if interest_expense > 0 else 0, 'indent': 1},
            {'label': 'Gain (Loss) on Sale of Assets', 'amount': gain_loss_assets, 'indent': 1},
            {'label': 'Other Income (Expense)', 'amount': other_income, 'indent': 1},
            {'label': 'Total Other Income (Expenses)', 'amount': total_other, 'is_subtotal': True, 'show_line': True},
        ]
        other_table = self._create_line_item_table(other_items, styles)
        if other_table:
            story.append(other_table)
        
        story.append(Spacer(1, 6))
        
        income_before_tax = operating_income + total_other
        ebt_items = [
            {'label': 'INCOME BEFORE INCOME TAXES', 'amount': income_before_tax, 'is_total': True, 'show_line': True},
        ]
        ebt_table = self._create_line_item_table(ebt_items, styles)
        if ebt_table:
            story.append(ebt_table)
        
        story.append(Spacer(1, 6))
        
        income_tax_expense = self.data.get('income_tax_expense', 0)
        tax_items = [
            {'label': 'Income Tax Expense', 'amount': income_tax_expense, 'indent': 1},
        ]
        tax_table = self._create_line_item_table(tax_items, styles)
        if tax_table:
            story.append(tax_table)
        
        story.append(Spacer(1, 8))
        story.append(HRFlowable(width="100%", thickness=2, color=self.primary_color))
        story.append(Spacer(1, 4))
        
        net_income = income_before_tax - income_tax_expense
        net_income_items = [
            {'label': 'NET INCOME', 'amount': net_income, 'is_total': True, 'double_line': True},
        ]
        net_table = self._create_line_item_table(net_income_items, styles)
        if net_table:
            story.append(net_table)
        
        story.append(Spacer(1, 20))
        story.append(HRFlowable(width="100%", thickness=0.5, color=colors.Color(0.7, 0.7, 0.7)))
        story.append(Spacer(1, 8))
        
        story.append(Paragraph("KEY FINANCIAL RATIOS", styles['SectionHeader']))
        
        gross_margin = (gross_profit / total_revenue * 100) if total_revenue > 0 else 0
        operating_margin = (operating_income / total_revenue * 100) if total_revenue > 0 else 0
        net_margin = (net_income / total_revenue * 100) if total_revenue > 0 else 0
        cogs_ratio = (total_cogs / total_revenue * 100) if total_revenue > 0 else 0
        opex_ratio = (total_operating_expenses / total_revenue * 100) if total_revenue > 0 else 0
        
        ratios_data = [
            ['Ratio', 'Value', 'Benchmark'],
            ['Gross Profit Margin', f'{gross_margin:.1f}%', '30-50%'],
            ['Operating Profit Margin (EBIT Margin)', f'{operating_margin:.1f}%', '10-20%'],
            ['Net Profit Margin', f'{net_margin:.1f}%', '5-15%'],
            ['Cost of Goods Sold %', f'{cogs_ratio:.1f}%', '50-70%'],
            ['Operating Expenses %', f'{opex_ratio:.1f}%', '20-35%'],
        ]
        
        ratios_table = Table(ratios_data, colWidths=[3*inch, 1.5*inch, 1.5*inch])
        ratios_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), self.primary_color),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('ALIGN', (1, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('LEFTPADDING', (0, 0), (-1, -1), 8),
            ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.Color(0.8, 0.8, 0.8)),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.Color(0.97, 0.97, 0.97), colors.white]),
        ]))
        story.append(ratios_table)
        
        story.append(Spacer(1, 20))
        
        story.append(Paragraph("COMPARATIVE SUMMARY", styles['SectionHeader']))
        
        revenue_mtd = self.data.get('revenue_mtd', 0)
        revenue_ytd = self.data.get('revenue_ytd', 0)
        revenue_last_month = self.data.get('revenue_last_month', 0)
        revenue_growth = self.data.get('revenue_growth', 0)
        
        summary_data = [
            ['Period', 'Revenue', 'Change'],
            ['Month-to-Date', f'$ {revenue_mtd:,.2f}', '-'],
            ['Last Month', f'$ {revenue_last_month:,.2f}', f'{revenue_growth:+.1f}%'],
            ['Year-to-Date', f'$ {revenue_ytd:,.2f}', '-'],
            ['Total (All Time)', f'$ {total_revenue:,.2f}', '-'],
        ]
        
        summary_table = Table(summary_data, colWidths=[2*inch, 2*inch, 2*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), self.accent_color),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.Color(0.8, 0.8, 0.8)),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.Color(0.97, 0.97, 0.97), colors.white]),
        ]))
        story.append(summary_table)
        
        story.append(Spacer(1, 30))
        story.append(HRFlowable(width="100%", thickness=0.5, color=colors.Color(0.8, 0.8, 0.8)))
        story.append(Spacer(1, 8))
        
        generated_date = datetime.now().strftime('%B %d, %Y at %I:%M %p')
        story.append(Paragraph(f"Generated on {generated_date}", styles['FooterText']))
        story.append(Paragraph(f"Prepared by: Dynamic.IQ-COREx Financial Reporting System", styles['FooterText']))
        story.append(Spacer(1, 4))
        story.append(Paragraph("The accompanying notes are an integral part of these financial statements.", styles['FooterText']))
        story.append(Paragraph("This statement has been prepared in accordance with Generally Accepted Accounting Principles (GAAP).", styles['FooterText']))
        story.append(Paragraph("For internal management use - Subject to review and audit.", styles['FooterText']))
        
        doc.build(story)
        buffer.seek(0)
        return buffer.getvalue()
