"""
FAA Form 8130-3 Certificate Generation Service
Generates Authorized Release Certificates for completed work orders
"""
import os
import hashlib
from datetime import datetime
from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfgen import canvas


class FAA8130Service:
    """Service class for generating FAA Form 8130-3 certificates"""
    
    UPLOAD_FOLDER = 'static/uploads/8130'
    
    @staticmethod
    def ensure_upload_folder():
        """Ensure the upload folder exists"""
        if not os.path.exists(FAA8130Service.UPLOAD_FOLDER):
            os.makedirs(FAA8130Service.UPLOAD_FOLDER)
    
    @staticmethod
    def generate_certificate_number(conn):
        """Generate a unique certificate number"""
        result = conn.execute(
            'SELECT COUNT(*) as count FROM faa_8130_certificates'
        ).fetchone()
        count = result['count'] if result else 0
        year = datetime.now().year
        return f"8130-{year}-{count + 1:05d}"
    
    @staticmethod
    def get_work_order_data(conn, work_order_id):
        """Get all necessary data from work order for 8130 generation"""
        wo = conn.execute('''
            SELECT wo.*, 
                   p.code as product_code, p.name as product_name, p.description as product_description,
                   c.name as customer_name, c.shipping_address as customer_address,
                   cs.company_name, cs.address_line1 as company_address, cs.city as company_city,
                   cs.state as company_state, cs.postal_code as company_zip, cs.country as company_country
            FROM work_orders wo
            JOIN products p ON wo.product_id = p.id
            LEFT JOIN customers c ON wo.customer_id = c.id
            LEFT JOIN company_settings cs ON cs.id = 1
            WHERE wo.id = ?
        ''', (work_order_id,)).fetchone()
        # Convert sqlite3.Row to dict for .get() support
        return dict(wo) if wo else None
    
    @staticmethod
    def get_existing_certificate(conn, work_order_id):
        """Check if a certificate already exists for this work order"""
        return conn.execute('''
            SELECT * FROM faa_8130_certificates 
            WHERE work_order_id = ? AND status = 'Issued'
        ''', (work_order_id,)).fetchone()
    
    @staticmethod
    def generate_pdf(certificate_data, wo_data):
        """Generate the FAA Form 8130-3 PDF document"""
        FAA8130Service.ensure_upload_folder()
        
        filename = f"{certificate_data['certificate_number']}.pdf"
        filepath = os.path.join(FAA8130Service.UPLOAD_FOLDER, filename)
        
        buffer = BytesIO()
        c = canvas.Canvas(buffer, pagesize=letter)
        width, height = letter
        
        # Header
        c.setFont("Helvetica-Bold", 16)
        c.drawCentredString(width/2, height - 0.5*inch, "FAA FORM 8130-3")
        c.setFont("Helvetica-Bold", 14)
        c.drawCentredString(width/2, height - 0.75*inch, "Authorized Release Certificate")
        
        # Draw form border
        c.setStrokeColor(colors.black)
        c.setLineWidth(2)
        c.rect(0.5*inch, 0.5*inch, width - inch, height - 1.25*inch)
        
        y_pos = height - 1.25*inch
        left_margin = 0.6*inch
        
        # Block 1: Approving Authority
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left_margin, y_pos, "1. APPROVING CIVIL AVIATION AUTHORITY/COUNTRY")
        c.setFont("Helvetica", 10)
        c.drawString(left_margin, y_pos - 15, certificate_data.get('issuing_authority', 'FAA / United States'))
        
        # Block 2: Form Tracking Number
        c.setFont("Helvetica-Bold", 9)
        c.drawString(width/2, y_pos, "2. FORM TRACKING NUMBER")
        c.setFont("Helvetica", 10)
        c.drawString(width/2, y_pos - 15, certificate_data['certificate_number'])
        
        y_pos -= 45
        c.setLineWidth(0.5)
        c.line(left_margin, y_pos, width - 0.6*inch, y_pos)
        
        # Block 3: Title
        y_pos -= 5
        c.setFont("Helvetica-Bold", 11)
        c.drawCentredString(width/2, y_pos - 12, "3. AUTHORIZED RELEASE CERTIFICATE")
        
        y_pos -= 35
        c.line(left_margin, y_pos, width - 0.6*inch, y_pos)
        
        # Block 4: Organization
        y_pos -= 5
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left_margin, y_pos - 12, "4. ORGANIZATION NAME AND ADDRESS")
        c.setFont("Helvetica", 10)
        org_name = certificate_data.get('organization_name', wo_data.get('company_name', ''))
        org_address = certificate_data.get('organization_address', '')
        if not org_address and wo_data.get('company_address'):
            org_address = f"{wo_data.get('company_address', '')}, {wo_data.get('company_city', '')}, {wo_data.get('company_state', '')} {wo_data.get('company_zip', '')}"
        c.drawString(left_margin, y_pos - 27, org_name or 'N/A')
        c.drawString(left_margin, y_pos - 42, org_address or 'N/A')
        
        y_pos -= 60
        c.line(left_margin, y_pos, width - 0.6*inch, y_pos)
        
        # Block 5: Work Order Reference
        y_pos -= 5
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left_margin, y_pos - 12, "5. WORK ORDER/CONTRACT/INVOICE")
        c.setFont("Helvetica", 10)
        c.drawString(left_margin, y_pos - 27, wo_data.get('wo_number', 'N/A'))
        
        # Block 11: Approval Number (on the right)
        c.setFont("Helvetica-Bold", 9)
        c.drawString(width/2 + 0.5*inch, y_pos - 12, "11. APPROVAL/AUTHORIZATION NUMBER")
        c.setFont("Helvetica", 10)
        c.drawString(width/2 + 0.5*inch, y_pos - 27, certificate_data.get('approval_number', 'N/A'))
        
        y_pos -= 45
        c.line(left_margin, y_pos, width - 0.6*inch, y_pos)
        
        # Block 6: Item Details
        y_pos -= 5
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left_margin, y_pos - 12, "6. ITEM")
        
        # Sub-blocks
        c.setFont("Helvetica-Bold", 8)
        c.drawString(left_margin + 10, y_pos - 27, "a. NAME")
        c.setFont("Helvetica", 10)
        c.drawString(left_margin + 10, y_pos - 42, wo_data.get('product_name', 'N/A'))
        
        c.setFont("Helvetica-Bold", 8)
        c.drawString(left_margin + 10, y_pos - 57, "b. PART NUMBER")
        c.setFont("Helvetica", 10)
        c.drawString(left_margin + 10, y_pos - 72, wo_data.get('product_code', 'N/A'))
        
        c.setFont("Helvetica-Bold", 8)
        c.drawString(width/2, y_pos - 57, "c. DESCRIPTION")
        c.setFont("Helvetica", 9)
        desc = wo_data.get('product_description', '') or ''
        c.drawString(width/2, y_pos - 72, desc[:50] + ('...' if len(desc) > 50 else ''))
        
        y_pos -= 90
        c.line(left_margin, y_pos, width - 0.6*inch, y_pos)
        
        # Block 7: Quantity
        y_pos -= 5
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left_margin, y_pos - 12, "7. QUANTITY")
        c.setFont("Helvetica", 10)
        c.drawString(left_margin, y_pos - 27, str(wo_data.get('quantity', 1)))
        
        # Block 8: Serial/Batch Number
        c.setFont("Helvetica-Bold", 9)
        c.drawString(width/3, y_pos - 12, "8. SERIAL NUMBER")
        c.setFont("Helvetica", 10)
        c.drawString(width/3, y_pos - 27, certificate_data.get('serial_number', 'N/A'))
        
        c.setFont("Helvetica-Bold", 9)
        c.drawString(width*2/3, y_pos - 12, "BATCH NUMBER")
        c.setFont("Helvetica", 10)
        c.drawString(width*2/3, y_pos - 27, certificate_data.get('batch_number', 'N/A'))
        
        y_pos -= 45
        c.line(left_margin, y_pos, width - 0.6*inch, y_pos)
        
        # Block 9: Status/Work
        y_pos -= 5
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left_margin, y_pos - 12, "9. STATUS/WORK")
        c.setFont("Helvetica", 10)
        status_work = certificate_data.get('status_work', wo_data.get('disposition', 'Overhauled'))
        c.drawString(left_margin, y_pos - 27, status_work)
        
        y_pos -= 45
        c.line(left_margin, y_pos, width - 0.6*inch, y_pos)
        
        # Block 12: Remarks
        y_pos -= 5
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left_margin, y_pos - 12, "12. REMARKS")
        c.setFont("Helvetica", 9)
        remarks = certificate_data.get('remarks', '') or ''
        remarks_lines = [remarks[i:i+90] for i in range(0, len(remarks), 90)][:3]
        for i, line in enumerate(remarks_lines):
            c.drawString(left_margin, y_pos - 27 - (i*12), line)
        
        y_pos -= 65
        c.line(left_margin, y_pos, width - 0.6*inch, y_pos)
        
        # Block 13: Certifying Staff
        y_pos -= 5
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left_margin, y_pos - 12, "13. CERTIFYING STAFF - I certify that the item(s) identified above were")
        c.drawString(left_margin, y_pos - 24, "manufactured/inspected in conformity with approved design data and are in")
        c.drawString(left_margin, y_pos - 36, "condition for safe operation.")
        
        c.setFont("Helvetica-Bold", 8)
        c.drawString(left_margin, y_pos - 55, "NAME:")
        c.setFont("Helvetica", 10)
        c.drawString(left_margin + 50, y_pos - 55, certificate_data.get('certifier_name', 'N/A'))
        
        c.setFont("Helvetica-Bold", 8)
        c.drawString(width/2, y_pos - 55, "CERTIFICATE NO:")
        c.setFont("Helvetica", 10)
        c.drawString(width/2 + 80, y_pos - 55, certificate_data.get('certifier_certificate_number', 'N/A'))
        
        c.setFont("Helvetica-Bold", 8)
        c.drawString(left_margin, y_pos - 72, "DATE:")
        c.setFont("Helvetica", 10)
        c.drawString(left_margin + 40, y_pos - 72, certificate_data.get('certifier_signature_date', datetime.now().strftime('%Y-%m-%d')))
        
        y_pos -= 90
        c.line(left_margin, y_pos, width - 0.6*inch, y_pos)
        
        # Block 14: Authorized Signature
        y_pos -= 5
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left_margin, y_pos - 12, "14. AUTHORIZED SIGNATURE")
        
        c.setFont("Helvetica-Bold", 8)
        c.drawString(left_margin, y_pos - 35, "NAME:")
        c.setFont("Helvetica", 10)
        c.drawString(left_margin + 50, y_pos - 35, certificate_data.get('authorized_signature_name', 'N/A'))
        
        c.setFont("Helvetica-Bold", 8)
        c.drawString(width/2, y_pos - 35, "DATE:")
        c.setFont("Helvetica", 10)
        c.drawString(width/2 + 40, y_pos - 35, certificate_data.get('authorized_signature_date', datetime.now().strftime('%Y-%m-%d')))
        
        # Footer
        c.setFont("Helvetica", 8)
        c.drawCentredString(width/2, 0.6*inch, f"Generated by Dynamic.IQ-COREx on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        c.save()
        
        # Get PDF content and compute hash
        pdf_content = buffer.getvalue()
        pdf_hash = hashlib.sha256(pdf_content).hexdigest()
        
        # Write to file
        with open(filepath, 'wb') as f:
            f.write(pdf_content)
        
        return filepath, pdf_hash
    
    @staticmethod
    def create_certificate(conn, work_order_id, form_data, user_id):
        """Create a new 8130 certificate record and generate PDF"""
        from models import AuditLogger
        
        wo_data = FAA8130Service.get_work_order_data(conn, work_order_id)
        
        if not wo_data:
            raise ValueError("Work order not found")
        
        if wo_data['status'] != 'Completed':
            raise ValueError("Work order must be completed to generate 8130")
        
        # Check for existing certificate
        existing = FAA8130Service.get_existing_certificate(conn, work_order_id)
        if existing:
            raise ValueError(f"Certificate {existing['certificate_number']} already exists for this work order")
        
        # Generate certificate number
        certificate_number = FAA8130Service.generate_certificate_number(conn)
        issue_date = datetime.now().strftime('%Y-%m-%d')
        
        # Prepare certificate data
        certificate_data = {
            'certificate_number': certificate_number,
            'issuing_authority': form_data.get('issuing_authority', 'FAA / United States'),
            'organization_name': form_data.get('organization_name') or wo_data.get('company_name', ''),
            'organization_address': form_data.get('organization_address', ''),
            'serial_number': form_data.get('serial_number', ''),
            'batch_number': form_data.get('batch_number', ''),
            'status_work': form_data.get('status_work', wo_data.get('disposition', 'Overhauled')),
            'approval_number': form_data.get('approval_number', ''),
            'remarks': form_data.get('remarks', ''),
            'certifier_name': form_data.get('certifier_name', ''),
            'certifier_certificate_number': form_data.get('certifier_certificate_number', ''),
            'certifier_signature_date': form_data.get('certifier_signature_date', issue_date),
            'authorized_signature_name': form_data.get('authorized_signature_name', ''),
            'authorized_signature_date': form_data.get('authorized_signature_date', issue_date),
        }
        
        # Generate PDF
        pdf_path, pdf_hash = FAA8130Service.generate_pdf(certificate_data, wo_data)
        
        # Insert certificate record
        cursor = conn.execute('''
            INSERT INTO faa_8130_certificates (
                certificate_number, work_order_id, issue_date,
                issuing_authority, organization_name, organization_address,
                work_order_reference, part_name, part_number, part_description,
                quantity, serial_number, batch_number, status_work,
                approval_number, remarks, certifier_name, certifier_certificate_number,
                certifier_signature_date, authorized_signature_name, authorized_signature_date,
                pdf_file_path, pdf_file_hash, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            certificate_number, work_order_id, issue_date,
            certificate_data['issuing_authority'],
            certificate_data['organization_name'],
            certificate_data['organization_address'],
            wo_data['wo_number'],
            wo_data['product_name'],
            wo_data['product_code'],
            wo_data.get('product_description', ''),
            wo_data['quantity'],
            certificate_data['serial_number'],
            certificate_data['batch_number'],
            certificate_data['status_work'],
            certificate_data['approval_number'],
            certificate_data['remarks'],
            certificate_data['certifier_name'],
            certificate_data['certifier_certificate_number'],
            certificate_data['certifier_signature_date'],
            certificate_data['authorized_signature_name'],
            certificate_data['authorized_signature_date'],
            pdf_path, pdf_hash, user_id
        ))
        
        certificate_id = cursor.lastrowid
        
        # Log audit trail
        AuditLogger.log_change(
            conn=conn,
            record_type='faa_8130_certificates',
            record_id=certificate_id,
            action_type='Created',
            modified_by=user_id,
            changed_fields={'certificate_number': certificate_number, 'work_order_id': work_order_id}
        )
        
        return {
            'id': certificate_id,
            'certificate_number': certificate_number,
            'pdf_path': pdf_path
        }
