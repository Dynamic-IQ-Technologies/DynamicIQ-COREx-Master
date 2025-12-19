from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from io import BytesIO
from datetime import datetime
import os

class ShippingDocumentGenerator:
    
    def __init__(self):
        self.styles = getSampleStyleSheet()
        self.company_name = "Dynamic.IQ-COREx"
        self.company_address = "123 Manufacturing Drive"
        self.company_city_state_zip = "Industrial City, ST 12345"
        self.company_phone = "(555) 123-4567"
        self.company_email = "shipping@dynamiciq.com"
    
    def _get_header_style(self):
        return ParagraphStyle(
            'HeaderStyle',
            parent=self.styles['Heading1'],
            fontSize=18,
            textColor=colors.HexColor('#1e3a5f'),
            alignment=TA_CENTER,
            spaceAfter=6
        )
    
    def _get_subheader_style(self):
        return ParagraphStyle(
            'SubHeaderStyle',
            parent=self.styles['Normal'],
            fontSize=10,
            textColor=colors.grey,
            alignment=TA_CENTER,
            spaceAfter=12
        )
    
    def _get_section_header_style(self):
        return ParagraphStyle(
            'SectionHeader',
            parent=self.styles['Heading2'],
            fontSize=12,
            textColor=colors.HexColor('#1e3a5f'),
            spaceBefore=12,
            spaceAfter=6
        )
    
    def _get_normal_style(self):
        return ParagraphStyle(
            'NormalCustom',
            parent=self.styles['Normal'],
            fontSize=9,
            leading=12
        )
    
    def _get_small_style(self):
        return ParagraphStyle(
            'SmallStyle',
            parent=self.styles['Normal'],
            fontSize=8,
            textColor=colors.grey
        )
    
    def _format_date(self, date_str):
        if not date_str:
            return "-"
        try:
            if isinstance(date_str, str):
                dt = datetime.strptime(date_str, '%Y-%m-%d')
            else:
                dt = date_str
            return dt.strftime('%B %d, %Y')
        except:
            return str(date_str)
    
    def _create_company_header(self, elements):
        header_style = self._get_header_style()
        subheader_style = self._get_subheader_style()
        
        elements.append(Paragraph(self.company_name, header_style))
        elements.append(Paragraph(
            f"{self.company_address}<br/>{self.company_city_state_zip}<br/>{self.company_phone} | {self.company_email}",
            subheader_style
        ))
        elements.append(Spacer(1, 12))
    
    def generate_packing_slip(self, shipment, lines, sales_order=None, customer=None):
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
        elements = []
        
        self._create_company_header(elements)
        
        title_style = ParagraphStyle(
            'TitleStyle',
            parent=self.styles['Heading1'],
            fontSize=20,
            textColor=colors.HexColor('#1e3a5f'),
            alignment=TA_CENTER,
            spaceBefore=6,
            spaceAfter=12
        )
        elements.append(Paragraph("PACKING SLIP", title_style))
        
        section_style = self._get_section_header_style()
        normal_style = self._get_normal_style()
        
        info_data = [
            ["Packing Slip #:", shipment.get('document_number', shipment['shipment_number']), "Ship Date:", self._format_date(shipment.get('ship_date'))],
            ["Shipment #:", shipment['shipment_number'], "Carrier:", shipment.get('carrier') or '-'],
            ["Sales Order:", sales_order.get('so_number') if sales_order else '-', "Tracking #:", shipment.get('tracking_number') or '-'],
        ]
        
        info_table = Table(info_data, colWidths=[1.5*inch, 2*inch, 1.5*inch, 2*inch])
        info_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#1e3a5f')),
            ('TEXTCOLOR', (2, 0), (2, -1), colors.HexColor('#1e3a5f')),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
        ]))
        elements.append(info_table)
        elements.append(Spacer(1, 20))
        
        ship_to_data = [
            ["SHIP TO:", "SHIP FROM:"],
            [
                f"{shipment.get('ship_to_name', '-')}\n{shipment.get('ship_to_address', '')}\n{shipment.get('ship_to_city', '')}, {shipment.get('ship_to_state', '')} {shipment.get('ship_to_postal_code', '')}\n{shipment.get('ship_to_country', '')}",
                f"{self.company_name}\n{self.company_address}\n{self.company_city_state_zip}"
            ]
        ]
        
        address_table = Table(ship_to_data, colWidths=[3.5*inch, 3.5*inch])
        address_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('FONTSIZE', (0, 1), (-1, 1), 9),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#1e3a5f')),
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f1f5f9')),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOX', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
            ('LINEBELOW', (0, 0), (-1, 0), 1, colors.HexColor('#1e3a5f')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        elements.append(address_table)
        elements.append(Spacer(1, 20))
        
        elements.append(Paragraph("LINE ITEMS", section_style))
        
        line_headers = ["Line", "Part Number", "Description", "Qty", "UOM", "S/N or Lot", "Pkg #"]
        line_data = [line_headers]
        
        for line in lines:
            serial_lot = line.get('serial_number') or line.get('lot_number') or '-'
            line_data.append([
                str(line.get('line_number', '')),
                line.get('code', ''),
                line.get('product_name', '')[:30],
                str(line.get('quantity_shipped', '')),
                line.get('unit_of_measure', 'EA'),
                serial_lot[:15] if serial_lot else '-',
                line.get('package_number') or '-'
            ])
        
        line_table = Table(line_data, colWidths=[0.5*inch, 1.2*inch, 2*inch, 0.6*inch, 0.6*inch, 1.2*inch, 0.7*inch])
        line_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1e3a5f')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('ALIGN', (0, 1), (0, -1), 'CENTER'),
            ('ALIGN', (3, 1), (4, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e2e8f0')),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8fafc')]),
        ]))
        elements.append(line_table)
        elements.append(Spacer(1, 30))
        
        if shipment.get('special_instructions'):
            elements.append(Paragraph("SPECIAL INSTRUCTIONS", section_style))
            elements.append(Paragraph(shipment['special_instructions'], normal_style))
            elements.append(Spacer(1, 20))
        
        footer_data = [
            ["Total Packages:", str(len(set(l.get('package_number') or 'N/A' for l in lines))), 
             "Total Weight:", f"{shipment.get('weight', 0)} {shipment.get('weight_unit', 'lbs')}"],
            ["Packed By:", "_____________________", "Date:", "_____________________"],
            ["Received By:", "_____________________", "Date:", "_____________________"],
        ]
        
        footer_table = Table(footer_data, colWidths=[1.5*inch, 2*inch, 1.5*inch, 2*inch])
        footer_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
        ]))
        elements.append(footer_table)
        
        doc.build(elements)
        buffer.seek(0)
        return buffer
    
    def generate_certificate_of_conformance(self, shipment, lines, sales_order=None, customer=None, signatory=None):
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
        elements = []
        
        self._create_company_header(elements)
        
        title_style = ParagraphStyle(
            'TitleStyle',
            parent=self.styles['Heading1'],
            fontSize=20,
            textColor=colors.HexColor('#1e3a5f'),
            alignment=TA_CENTER,
            spaceBefore=6,
            spaceAfter=6
        )
        elements.append(Paragraph("CERTIFICATE OF CONFORMANCE", title_style))
        
        cert_num_style = ParagraphStyle(
            'CertNumStyle',
            parent=self.styles['Normal'],
            fontSize=11,
            textColor=colors.HexColor('#64748b'),
            alignment=TA_CENTER,
            spaceAfter=20
        )
        cert_number = shipment.get('document_number', f"COC-{shipment['shipment_number']}")
        elements.append(Paragraph(f"Certificate No: {cert_number}", cert_num_style))
        
        section_style = self._get_section_header_style()
        normal_style = self._get_normal_style()
        
        info_data = [
            ["Shipment Number:", shipment['shipment_number'], "Date Issued:", self._format_date(datetime.now().strftime('%Y-%m-%d'))],
            ["Sales Order:", sales_order.get('so_number') if sales_order else '-', "Ship Date:", self._format_date(shipment.get('ship_date'))],
            ["Customer:", customer.get('name') if customer else shipment.get('ship_to_name', '-'), "", ""],
        ]
        
        info_table = Table(info_data, colWidths=[1.5*inch, 2.25*inch, 1.25*inch, 2*inch])
        info_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#1e3a5f')),
            ('TEXTCOLOR', (2, 0), (2, -1), colors.HexColor('#1e3a5f')),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
        ]))
        elements.append(info_table)
        elements.append(Spacer(1, 20))
        
        elements.append(Paragraph("STATEMENT OF CONFORMANCE", section_style))
        
        conformance_text = """
        We hereby certify that the materials and/or products listed below have been manufactured, 
        inspected, and tested in accordance with the applicable specifications, drawings, and 
        requirements. All items conform to the quality standards and requirements as specified 
        in the purchase order and applicable regulatory requirements.
        """
        elements.append(Paragraph(conformance_text.strip(), normal_style))
        elements.append(Spacer(1, 20))
        
        elements.append(Paragraph("ITEMS COVERED BY THIS CERTIFICATE", section_style))
        
        line_headers = ["Part Number", "Description", "Qty", "S/N or Lot #", "Condition"]
        line_data = [line_headers]
        
        for line in lines:
            serial_lot = line.get('serial_number') or line.get('lot_number') or 'N/A'
            line_data.append([
                line.get('code', ''),
                line.get('product_name', '')[:35],
                str(line.get('quantity_shipped', '')),
                serial_lot,
                line.get('condition', 'New')
            ])
        
        line_table = Table(line_data, colWidths=[1.3*inch, 2.5*inch, 0.7*inch, 1.3*inch, 1*inch])
        line_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1e3a5f')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('ALIGN', (2, 1), (2, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e2e8f0')),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8fafc')]),
        ]))
        elements.append(line_table)
        elements.append(Spacer(1, 30))
        
        elements.append(Paragraph("COMPLIANCE STANDARDS", section_style))
        compliance_text = "ISO 9001:2015 Quality Management System | AS9100D Aerospace Quality Standard"
        elements.append(Paragraph(compliance_text, normal_style))
        elements.append(Spacer(1, 30))
        
        sig_name = signatory or "Quality Assurance Representative"
        sig_data = [
            ["Authorized Signatory:", "", "Date:", ""],
            ["", "", "", ""],
            [f"Name: {sig_name}", "", f"Date: {datetime.now().strftime('%B %d, %Y')}", ""],
            ["Title: Quality Manager", "", "", ""],
        ]
        
        sig_table = Table(sig_data, colWidths=[2.5*inch, 1*inch, 2.5*inch, 1*inch])
        sig_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('FONTNAME', (0, 0), (0, 0), 'Helvetica-Bold'),
            ('FONTNAME', (2, 0), (2, 0), 'Helvetica-Bold'),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#1e3a5f')),
            ('LINEBELOW', (0, 1), (0, 1), 1, colors.black),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
        ]))
        elements.append(sig_table)
        
        doc.build(elements)
        buffer.seek(0)
        return buffer
    
    def generate_commercial_invoice(self, shipment, lines, sales_order=None, customer=None):
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter, topMargin=0.5*inch, bottomMargin=0.5*inch)
        elements = []
        
        self._create_company_header(elements)
        
        title_style = ParagraphStyle(
            'TitleStyle',
            parent=self.styles['Heading1'],
            fontSize=20,
            textColor=colors.HexColor('#1e3a5f'),
            alignment=TA_CENTER,
            spaceBefore=6,
            spaceAfter=12
        )
        elements.append(Paragraph("COMMERCIAL INVOICE", title_style))
        
        section_style = self._get_section_header_style()
        normal_style = self._get_normal_style()
        
        invoice_number = shipment.get('document_number', f"INV-{shipment['shipment_number']}")
        
        info_data = [
            ["Invoice Number:", invoice_number, "Invoice Date:", self._format_date(datetime.now().strftime('%Y-%m-%d'))],
            ["Shipment #:", shipment['shipment_number'], "Ship Date:", self._format_date(shipment.get('ship_date'))],
            ["Sales Order:", sales_order.get('so_number') if sales_order else '-', "Terms:", "Net 30"],
        ]
        
        info_table = Table(info_data, colWidths=[1.5*inch, 2.25*inch, 1.25*inch, 2*inch])
        info_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#1e3a5f')),
            ('TEXTCOLOR', (2, 0), (2, -1), colors.HexColor('#1e3a5f')),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
        ]))
        elements.append(info_table)
        elements.append(Spacer(1, 15))
        
        address_data = [
            ["SOLD TO:", "SHIP TO:"],
            [
                f"{customer.get('name') if customer else shipment.get('ship_to_name', '-')}\n{customer.get('address') if customer else shipment.get('ship_to_address', '')}\n{customer.get('city') if customer else shipment.get('ship_to_city', '')}, {customer.get('state') if customer else shipment.get('ship_to_state', '')} {customer.get('postal_code') if customer else shipment.get('ship_to_postal_code', '')}",
                f"{shipment.get('ship_to_name', '-')}\n{shipment.get('ship_to_address', '')}\n{shipment.get('ship_to_city', '')}, {shipment.get('ship_to_state', '')} {shipment.get('ship_to_postal_code', '')}\n{shipment.get('ship_to_country', 'USA')}"
            ]
        ]
        
        address_table = Table(address_data, colWidths=[3.5*inch, 3.5*inch])
        address_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('FONTSIZE', (0, 1), (-1, 1), 9),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#1e3a5f')),
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f1f5f9')),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOX', (0, 0), (-1, -1), 1, colors.HexColor('#e2e8f0')),
            ('LINEBELOW', (0, 0), (-1, 0), 1, colors.HexColor('#1e3a5f')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        elements.append(address_table)
        elements.append(Spacer(1, 20))
        
        line_headers = ["Part Number", "Description", "Qty", "Unit Price", "Total", "HS Code", "Origin"]
        line_data = [line_headers]
        
        subtotal = 0
        for line in lines:
            unit_price = line.get('unit_price', 0) or 0
            qty = line.get('quantity_shipped', 0) or 0
            total = unit_price * qty
            subtotal += total
            
            line_data.append([
                line.get('code', ''),
                line.get('product_name', '')[:25],
                str(int(qty)),
                f"${unit_price:,.2f}",
                f"${total:,.2f}",
                line.get('hs_code') or '-',
                line.get('country_of_origin') or 'USA'
            ])
        
        line_table = Table(line_data, colWidths=[1.1*inch, 1.8*inch, 0.5*inch, 0.9*inch, 0.9*inch, 0.8*inch, 0.7*inch])
        line_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1e3a5f')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('ALIGN', (2, 1), (4, -1), 'RIGHT'),
            ('ALIGN', (5, 1), (6, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e2e8f0')),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8fafc')]),
        ]))
        elements.append(line_table)
        elements.append(Spacer(1, 15))
        
        freight = shipment.get('freight_cost', 0) or 0
        insurance = shipment.get('insurance_value', 0) or 0
        grand_total = subtotal + freight + insurance
        
        totals_data = [
            ["", "", "", "", "Subtotal:", f"${subtotal:,.2f}"],
            ["", "", "", "", "Freight:", f"${freight:,.2f}"],
            ["", "", "", "", "Insurance:", f"${insurance:,.2f}"],
            ["", "", "", "", "TOTAL:", f"${grand_total:,.2f}"],
        ]
        
        totals_table = Table(totals_data, colWidths=[1.1*inch, 1.8*inch, 0.5*inch, 0.9*inch, 1.2*inch, 1.2*inch])
        totals_table.setStyle(TableStyle([
            ('FONTNAME', (4, 0), (4, -1), 'Helvetica-Bold'),
            ('FONTNAME', (4, -1), (5, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('ALIGN', (4, 0), (5, -1), 'RIGHT'),
            ('TEXTCOLOR', (4, -1), (5, -1), colors.HexColor('#1e3a5f')),
            ('LINEABOVE', (4, -1), (5, -1), 1, colors.HexColor('#1e3a5f')),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
        ]))
        elements.append(totals_table)
        elements.append(Spacer(1, 20))
        
        export_data = [
            ["Currency:", "USD", "Incoterms:", "FOB Origin"],
            ["Reason for Export:", "SALE", "Country of Origin:", "USA"],
        ]
        
        export_table = Table(export_data, colWidths=[1.5*inch, 2*inch, 1.5*inch, 2*inch])
        export_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (2, 0), (2, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#1e3a5f')),
            ('TEXTCOLOR', (2, 0), (2, -1), colors.HexColor('#1e3a5f')),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
        ]))
        elements.append(export_table)
        elements.append(Spacer(1, 20))
        
        declaration_style = ParagraphStyle(
            'Declaration',
            parent=self.styles['Normal'],
            fontSize=8,
            textColor=colors.grey,
            alignment=TA_CENTER
        )
        declaration = "I declare that the information provided on this invoice is true and correct to the best of my knowledge."
        elements.append(Paragraph(declaration, declaration_style))
        
        doc.build(elements)
        buffer.seek(0)
        return buffer
