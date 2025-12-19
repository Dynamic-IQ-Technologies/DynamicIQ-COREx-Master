"""
Professional Marketing Presentation PDF Generator
Creates elegant, branded PDF presentations using ReportLab
"""
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.pdfgen import canvas
from io import BytesIO
from datetime import datetime
import os


def hex_to_rgb(hex_color):
    """Convert hex color to RGB tuple (0-1 scale)"""
    hex_color = hex_color.lstrip('#')
    if len(hex_color) != 6:
        hex_color = '2563eb'
    r = int(hex_color[0:2], 16) / 255
    g = int(hex_color[2:4], 16) / 255
    b = int(hex_color[4:6], 16) / 255
    return (r, g, b)


class PresentationPDFGenerator:
    """Generate professional marketing presentation PDFs"""
    
    def __init__(self, presentation_data, company_settings):
        self.data = presentation_data
        self.settings = company_settings
        self.primary_color = hex_to_rgb(company_settings.get('brand_primary_color') or '#2563eb')
        self.secondary_color = hex_to_rgb(company_settings.get('brand_secondary_color') or '#1e40af')
        self.accent_color = hex_to_rgb(company_settings.get('brand_accent_color') or '#f97316')
        self.company_name = company_settings.get('company_name') or 'Company'
        
    def _create_styles(self):
        """Create custom paragraph styles"""
        styles = getSampleStyleSheet()
        
        styles.add(ParagraphStyle(
            name='HeroTitle',
            parent=styles['Heading1'],
            fontSize=32,
            textColor=colors.white,
            alignment=TA_CENTER,
            spaceAfter=20,
            fontName='Helvetica-Bold'
        ))
        
        styles.add(ParagraphStyle(
            name='HeroSubtitle',
            parent=styles['Normal'],
            fontSize=16,
            textColor=colors.white,
            alignment=TA_CENTER,
            spaceAfter=30
        ))
        
        styles.add(ParagraphStyle(
            name='SectionTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.Color(*self.primary_color),
            alignment=TA_CENTER,
            spaceBefore=30,
            spaceAfter=20,
            fontName='Helvetica-Bold'
        ))
        
        styles.add(ParagraphStyle(
            name='CardTitle',
            parent=styles['Heading2'],
            fontSize=14,
            textColor=colors.Color(*self.primary_color),
            spaceBefore=10,
            spaceAfter=5,
            fontName='Helvetica-Bold'
        ))
        
        styles.add(ParagraphStyle(
            name='CardBody',
            parent=styles['Normal'],
            fontSize=11,
            textColor=colors.Color(0.3, 0.3, 0.3),
            spaceAfter=10
        ))
        
        styles.add(ParagraphStyle(
            name='StatNumber',
            parent=styles['Normal'],
            fontSize=28,
            textColor=colors.Color(*self.primary_color),
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        ))
        
        styles.add(ParagraphStyle(
            name='StatLabel',
            parent=styles['Normal'],
            fontSize=11,
            textColor=colors.Color(0.4, 0.4, 0.4),
            alignment=TA_CENTER
        ))
        
        styles.add(ParagraphStyle(
            name='FeatureItem',
            parent=styles['Normal'],
            fontSize=11,
            textColor=colors.Color(0.3, 0.3, 0.3),
            leftIndent=15,
            spaceAfter=5
        ))
        
        styles.add(ParagraphStyle(
            name='TestimonialQuote',
            parent=styles['Normal'],
            fontSize=14,
            textColor=colors.Color(0.3, 0.3, 0.3),
            alignment=TA_CENTER,
            fontName='Helvetica-Oblique',
            spaceBefore=20,
            spaceAfter=10
        ))
        
        styles.add(ParagraphStyle(
            name='TestimonialAuthor',
            parent=styles['Normal'],
            fontSize=12,
            textColor=colors.Color(*self.primary_color),
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        ))
        
        styles.add(ParagraphStyle(
            name='CTATitle',
            parent=styles['Heading1'],
            fontSize=22,
            textColor=colors.white,
            alignment=TA_CENTER,
            spaceAfter=10,
            fontName='Helvetica-Bold'
        ))
        
        styles.add(ParagraphStyle(
            name='CTABody',
            parent=styles['Normal'],
            fontSize=14,
            textColor=colors.white,
            alignment=TA_CENTER
        ))
        
        styles.add(ParagraphStyle(
            name='Footer',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.Color(0.5, 0.5, 0.5),
            alignment=TA_CENTER
        ))
        
        return styles
    
    def _draw_hero_background(self, canvas, doc):
        """Draw gradient-like hero background"""
        canvas.saveState()
        canvas.setFillColor(colors.Color(*self.primary_color))
        canvas.rect(0, doc.height + doc.topMargin - 180, doc.width + doc.leftMargin + doc.rightMargin, 220, fill=1, stroke=0)
        canvas.setFillColor(colors.Color(*self.secondary_color))
        canvas.rect(0, doc.height + doc.topMargin - 180, doc.width + doc.leftMargin + doc.rightMargin, 40, fill=1, stroke=0)
        canvas.restoreState()
    
    def _add_page_number(self, canvas, doc):
        """Add page number and footer to each page"""
        canvas.saveState()
        canvas.setFont('Helvetica', 9)
        canvas.setFillColor(colors.Color(0.5, 0.5, 0.5))
        page_num = canvas.getPageNumber()
        text = f"{self.company_name} | Page {page_num}"
        canvas.drawCentredString(doc.width / 2 + doc.leftMargin, 0.5 * inch, text)
        canvas.restoreState()
    
    def generate(self):
        """Generate the PDF and return as BytesIO buffer"""
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
        
        hero = self.data.get('hero', {})
        company = self.data.get('company', {})
        
        story.append(Spacer(1, 40))
        story.append(Paragraph(hero.get('headline', 'Transform Your Operations'), styles['HeroTitle']))
        story.append(Paragraph(hero.get('subheadline', 'Next-generation solutions for modern business'), styles['HeroSubtitle']))
        story.append(Spacer(1, 20))
        
        company_info = f"<b>{company.get('name', self.company_name)}</b>"
        if company.get('tagline'):
            company_info += f"<br/>{company.get('tagline')}"
        story.append(Paragraph(company_info, styles['HeroSubtitle']))
        story.append(Spacer(1, 60))
        
        value_props = self.data.get('value_propositions', [])
        if value_props:
            story.append(Paragraph("Why Choose Us", styles['SectionTitle']))
            story.append(Spacer(1, 10))
            
            for vp in value_props:
                story.append(Paragraph(f"<b>{vp.get('title', '')}</b>", styles['CardTitle']))
                story.append(Paragraph(vp.get('description', ''), styles['CardBody']))
            
            story.append(Spacer(1, 20))
        
        capabilities = self.data.get('capabilities', [])
        if capabilities:
            story.append(PageBreak())
            story.append(Paragraph("Platform Capabilities", styles['SectionTitle']))
            story.append(Spacer(1, 10))
            
            for cap in capabilities:
                story.append(Paragraph(f"<b>{cap.get('category', '')}</b>", styles['CardTitle']))
                features = cap.get('features', [])
                for feature in features:
                    story.append(Paragraph(f"• {feature}", styles['FeatureItem']))
                story.append(Spacer(1, 10))
        
        industries = self.data.get('industries', [])
        if industries:
            story.append(PageBreak())
            story.append(Paragraph("Industries We Serve", styles['SectionTitle']))
            story.append(Spacer(1, 10))
            
            for ind in industries:
                story.append(Paragraph(f"<b>{ind.get('name', '')}</b>", styles['CardTitle']))
                story.append(Paragraph(ind.get('description', ''), styles['CardBody']))
        
        stats = self.data.get('stats', [])
        if stats:
            story.append(Spacer(1, 30))
            story.append(Paragraph("By The Numbers", styles['SectionTitle']))
            story.append(Spacer(1, 10))
            
            stat_data = []
            for stat in stats[:4]:
                stat_data.append([
                    Paragraph(stat.get('value', ''), styles['StatNumber']),
                    Paragraph(stat.get('label', ''), styles['StatLabel'])
                ])
            
            if stat_data:
                col_width = (doc.width - 40) / len(stat_data)
                stat_table = Table([[cell[0] for cell in stat_data], [cell[1] for cell in stat_data]], 
                                   colWidths=[col_width] * len(stat_data))
                stat_table.setStyle(TableStyle([
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('TOPPADDING', (0, 0), (-1, -1), 15),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 15),
                ]))
                story.append(stat_table)
        
        testimonial = self.data.get('testimonial', {})
        if testimonial and testimonial.get('quote'):
            story.append(Spacer(1, 30))
            story.append(Paragraph(f'"{testimonial.get("quote", "")}"', styles['TestimonialQuote']))
            author_info = testimonial.get('author', '')
            if testimonial.get('title'):
                author_info += f", {testimonial.get('title')}"
            if testimonial.get('company'):
                author_info += f" - {testimonial.get('company')}"
            story.append(Paragraph(author_info, styles['TestimonialAuthor']))
        
        cta = self.data.get('cta', {})
        if cta:
            story.append(PageBreak())
            story.append(Spacer(1, 100))
            
            cta_data = [[
                Paragraph(cta.get('headline', 'Ready to Get Started?'), styles['CTATitle']),
            ], [
                Paragraph(cta.get('subheadline', 'Contact us today to learn more'), styles['CTABody'])
            ]]
            
            cta_table = Table(cta_data, colWidths=[doc.width - 60])
            cta_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, -1), colors.Color(*self.primary_color)),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, -1), 30),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 30),
                ('LEFTPADDING', (0, 0), (-1, -1), 20),
                ('RIGHTPADDING', (0, 0), (-1, -1), 20),
                ('ROUNDEDCORNERS', [10, 10, 10, 10]),
            ]))
            story.append(cta_table)
            
            if cta.get('contact_info'):
                story.append(Spacer(1, 20))
                story.append(Paragraph(cta.get('contact_info'), styles['CardBody']))
        
        story.append(Spacer(1, 50))
        story.append(Paragraph(
            f"Generated on {datetime.now().strftime('%B %d, %Y')} | {self.company_name}",
            styles['Footer']
        ))
        
        def first_page(canvas, doc):
            self._draw_hero_background(canvas, doc)
            self._add_page_number(canvas, doc)
        
        def later_pages(canvas, doc):
            self._add_page_number(canvas, doc)
        
        doc.build(story, onFirstPage=first_page, onLaterPages=later_pages)
        buffer.seek(0)
        return buffer


def generate_presentation_pdf(presentation_data, company_settings):
    """Generate a professional marketing presentation PDF"""
    generator = PresentationPDFGenerator(presentation_data, company_settings)
    return generator.generate()
