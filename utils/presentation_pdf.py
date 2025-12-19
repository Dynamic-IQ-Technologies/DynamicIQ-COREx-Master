"""
Professional Marketing Presentation PDF Generator
Creates elegant, branded PDF presentations using ReportLab
"""
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from io import BytesIO
from datetime import datetime


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
            fontSize=28,
            textColor=colors.white,
            alignment=TA_CENTER,
            spaceAfter=12,
            fontName='Helvetica-Bold',
            leading=34
        ))
        
        styles.add(ParagraphStyle(
            name='HeroSubtitle',
            parent=styles['Normal'],
            fontSize=14,
            textColor=colors.white,
            alignment=TA_CENTER,
            spaceAfter=8,
            leading=18
        ))
        
        styles.add(ParagraphStyle(
            name='CompanyName',
            parent=styles['Normal'],
            fontSize=16,
            textColor=colors.white,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold',
            spaceAfter=4
        ))
        
        styles.add(ParagraphStyle(
            name='SectionTitle',
            parent=styles['Heading1'],
            fontSize=20,
            textColor=colors.Color(*self.primary_color),
            alignment=TA_CENTER,
            spaceBefore=20,
            spaceAfter=16,
            fontName='Helvetica-Bold'
        ))
        
        styles.add(ParagraphStyle(
            name='CardTitle',
            parent=styles['Heading2'],
            fontSize=13,
            textColor=colors.Color(*self.primary_color),
            spaceBefore=8,
            spaceAfter=4,
            fontName='Helvetica-Bold'
        ))
        
        styles.add(ParagraphStyle(
            name='CardBody',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.Color(0.3, 0.3, 0.3),
            spaceAfter=8,
            leading=14
        ))
        
        styles.add(ParagraphStyle(
            name='StatNumber',
            parent=styles['Normal'],
            fontSize=24,
            textColor=colors.Color(*self.primary_color),
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        ))
        
        styles.add(ParagraphStyle(
            name='StatLabel',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.Color(0.4, 0.4, 0.4),
            alignment=TA_CENTER
        ))
        
        styles.add(ParagraphStyle(
            name='FeatureItem',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.Color(0.3, 0.3, 0.3),
            leftIndent=12,
            spaceAfter=4,
            leading=13
        ))
        
        styles.add(ParagraphStyle(
            name='TestimonialQuote',
            parent=styles['Normal'],
            fontSize=12,
            textColor=colors.Color(0.3, 0.3, 0.3),
            alignment=TA_CENTER,
            fontName='Helvetica-Oblique',
            spaceBefore=12,
            spaceAfter=8,
            leading=16
        ))
        
        styles.add(ParagraphStyle(
            name='TestimonialAuthor',
            parent=styles['Normal'],
            fontSize=11,
            textColor=colors.Color(*self.primary_color),
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        ))
        
        styles.add(ParagraphStyle(
            name='CTATitle',
            parent=styles['Heading1'],
            fontSize=18,
            textColor=colors.white,
            alignment=TA_CENTER,
            spaceAfter=8,
            fontName='Helvetica-Bold'
        ))
        
        styles.add(ParagraphStyle(
            name='CTABody',
            parent=styles['Normal'],
            fontSize=12,
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
    
    def _add_page_number(self, canvas, doc):
        """Add page number and footer to each page"""
        canvas.saveState()
        canvas.setFont('Helvetica', 9)
        canvas.setFillColor(colors.Color(0.5, 0.5, 0.5))
        page_num = canvas.getPageNumber()
        text = f"{self.company_name} | Page {page_num}"
        canvas.drawCentredString(doc.width / 2 + doc.leftMargin, 0.4 * inch, text)
        canvas.restoreState()
    
    def generate(self):
        """Generate the PDF and return as BytesIO buffer"""
        buffer = BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=letter,
            rightMargin=0.6*inch,
            leftMargin=0.6*inch,
            topMargin=0.5*inch,
            bottomMargin=0.6*inch
        )
        
        styles = self._create_styles()
        story = []
        
        hero = self.data.get('hero', {})
        company = self.data.get('company', {})
        
        hero_content = []
        hero_content.append([Paragraph(hero.get('headline', 'Transform Your Operations'), styles['HeroTitle'])])
        hero_content.append([Paragraph(hero.get('subheadline', 'Next-generation solutions for modern business'), styles['HeroSubtitle'])])
        hero_content.append([Spacer(1, 15)])
        hero_content.append([Paragraph(company.get('name', self.company_name), styles['CompanyName'])])
        if company.get('tagline'):
            hero_content.append([Paragraph(company.get('tagline'), styles['HeroSubtitle'])])
        
        hero_table = Table(hero_content, colWidths=[doc.width])
        hero_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.Color(*self.primary_color)),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, 0), 30),
            ('BOTTOMPADDING', (0, -1), (-1, -1), 30),
            ('LEFTPADDING', (0, 0), (-1, -1), 20),
            ('RIGHTPADDING', (0, 0), (-1, -1), 20),
        ]))
        story.append(hero_table)
        story.append(Spacer(1, 25))
        
        value_props = self.data.get('value_propositions', [])
        if value_props:
            story.append(Paragraph("Why Choose Us", styles['SectionTitle']))
            
            for vp in value_props:
                vp_content = []
                vp_content.append(Paragraph(f"<b>{vp.get('title', '')}</b>", styles['CardTitle']))
                vp_content.append(Paragraph(vp.get('description', ''), styles['CardBody']))
                story.append(KeepTogether(vp_content))
            
            story.append(Spacer(1, 10))
        
        story.append(PageBreak())
        
        capabilities = self.data.get('capabilities', [])
        if capabilities:
            story.append(Paragraph("Platform Capabilities", styles['SectionTitle']))
            
            for cap in capabilities:
                cap_content = []
                cap_content.append(Paragraph(f"<b>{cap.get('category', '')}</b>", styles['CardTitle']))
                features = cap.get('features', [])
                for feature in features:
                    cap_content.append(Paragraph(f"• {feature}", styles['FeatureItem']))
                cap_content.append(Spacer(1, 8))
                story.append(KeepTogether(cap_content))
        
        story.append(PageBreak())
        
        industries = self.data.get('industries', [])
        if industries:
            story.append(Paragraph("Industries We Serve", styles['SectionTitle']))
            
            for ind in industries:
                ind_content = []
                ind_content.append(Paragraph(f"<b>{ind.get('name', '')}</b>", styles['CardTitle']))
                desc = ind.get('description') or ind.get('use_case', '')
                if desc:
                    ind_content.append(Paragraph(desc, styles['CardBody']))
                story.append(KeepTogether(ind_content))
            
            story.append(Spacer(1, 20))
        
        stats = self.data.get('stats', [])
        if stats:
            story.append(Paragraph("By The Numbers", styles['SectionTitle']))
            story.append(Spacer(1, 10))
            
            stats_to_show = stats[:4]
            if stats_to_show:
                stat_row1 = []
                stat_row2 = []
                for stat in stats_to_show:
                    stat_row1.append(Paragraph(stat.get('value', ''), styles['StatNumber']))
                    stat_row2.append(Paragraph(stat.get('label', ''), styles['StatLabel']))
                
                col_width = (doc.width - 20) / len(stats_to_show)
                stat_table = Table([stat_row1, stat_row2], colWidths=[col_width] * len(stats_to_show))
                stat_table.setStyle(TableStyle([
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('TOPPADDING', (0, 0), (-1, -1), 12),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
                    ('BACKGROUND', (0, 0), (-1, -1), colors.Color(0.97, 0.97, 0.97)),
                ]))
                story.append(stat_table)
            
            story.append(Spacer(1, 20))
        
        testimonial = self.data.get('testimonial', {})
        if testimonial and testimonial.get('quote'):
            story.append(Spacer(1, 15))
            story.append(Paragraph(f'"{testimonial.get("quote", "")}"', styles['TestimonialQuote']))
            author_info = testimonial.get('author', '')
            if testimonial.get('title'):
                author_info += f", {testimonial.get('title')}"
            if testimonial.get('company'):
                author_info += f" - {testimonial.get('company')}"
            story.append(Paragraph(author_info, styles['TestimonialAuthor']))
        
        story.append(PageBreak())
        
        cta = self.data.get('cta', {})
        if cta:
            story.append(Spacer(1, 80))
            
            cta_content = []
            cta_content.append([Paragraph(cta.get('headline', 'Ready to Get Started?'), styles['CTATitle'])])
            cta_content.append([Paragraph(cta.get('subheadline', 'Contact us today to learn more'), styles['CTABody'])])
            if cta.get('contact_info'):
                cta_content.append([Spacer(1, 8)])
                cta_content.append([Paragraph(cta.get('contact_info'), styles['CTABody'])])
            
            cta_table = Table(cta_content, colWidths=[doc.width - 40])
            cta_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, -1), colors.Color(*self.primary_color)),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('TOPPADDING', (0, 0), (-1, 0), 25),
                ('BOTTOMPADDING', (0, -1), (-1, -1), 25),
                ('LEFTPADDING', (0, 0), (-1, -1), 20),
                ('RIGHTPADDING', (0, 0), (-1, -1), 20),
            ]))
            story.append(cta_table)
        
        story.append(Spacer(1, 40))
        story.append(Paragraph(
            f"Generated on {datetime.now().strftime('%B %d, %Y')} | {self.company_name}",
            styles['Footer']
        ))
        
        doc.build(story, onFirstPage=self._add_page_number, onLaterPages=self._add_page_number)
        buffer.seek(0)
        return buffer


def generate_presentation_pdf(presentation_data, company_settings):
    """Generate a professional marketing presentation PDF"""
    generator = PresentationPDFGenerator(presentation_data, company_settings)
    return generator.generate()
