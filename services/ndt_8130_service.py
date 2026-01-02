"""
NDT FAA Form 8130-3 Certificate Generation Service
Generates Authorized Release Certificates for completed NDT work orders
"""
import os
import hashlib
from datetime import datetime
from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.pdfgen import canvas


class NDT8130Service:
    """Service class for generating FAA Form 8130-3 certificates for NDT inspections"""
    
    UPLOAD_FOLDER = 'static/uploads/ndt_8130'
    
    @staticmethod
    def ensure_upload_folder():
        """Ensure the upload folder exists"""
        if not os.path.exists(NDT8130Service.UPLOAD_FOLDER):
            os.makedirs(NDT8130Service.UPLOAD_FOLDER)
    
    @staticmethod
    def generate_certificate_number(conn):
        """Generate a unique certificate number for NDT 8130"""
        result = conn.execute(
            'SELECT COUNT(*) as count FROM ndt_8130_certificates'
        ).fetchone()
        count = result['count'] if result else 0
        year = datetime.now().year
        return f"NDT-8130-{year}-{count + 1:05d}"
    
    @staticmethod
    def get_ndt_work_order_data(conn, ndt_wo_id):
        """Get all necessary data from NDT work order for 8130 generation"""
        ndt_wo = conn.execute('''
            SELECT nwo.*, 
                   c.name as customer_name, c.shipping_address as customer_address,
                   p.code as product_code, p.name as product_name,
                   t.first_name || ' ' || t.last_name as technician_name,
                   t.technician_number,
                   r.first_name || ' ' || r.last_name as reviewer_name,
                   r.technician_number as reviewer_number,
                   cs.company_name, cs.address_line1 as company_address, 
                   cs.city as company_city, cs.state as company_state, 
                   cs.postal_code as company_zip, cs.country as company_country
            FROM ndt_work_orders nwo
            LEFT JOIN customers c ON nwo.customer_id = c.id
            LEFT JOIN products p ON nwo.product_id = p.id
            LEFT JOIN ndt_technicians t ON nwo.assigned_technician_id = t.id
            LEFT JOIN ndt_technicians r ON nwo.reviewer_id = r.id
            LEFT JOIN company_settings cs ON cs.id = 1
            WHERE nwo.id = ?
        ''', (ndt_wo_id,)).fetchone()
        return dict(ndt_wo) if ndt_wo else None
    
    @staticmethod
    def get_inspection_results(conn, ndt_wo_id):
        """Get inspection results for the NDT work order"""
        results = conn.execute('''
            SELECT ir.*, t.first_name || ' ' || t.last_name as technician_name
            FROM ndt_inspection_results ir
            LEFT JOIN ndt_technicians t ON ir.technician_id = t.id
            WHERE ir.ndt_wo_id = ?
            ORDER BY ir.inspection_date DESC
        ''', (ndt_wo_id,)).fetchall()
        return [dict(r) for r in results]
    
    @staticmethod
    def get_existing_certificate(conn, ndt_wo_id):
        """Check if a certificate already exists for this NDT work order"""
        return conn.execute('''
            SELECT * FROM ndt_8130_certificates 
            WHERE ndt_wo_id = ? AND status = 'Issued'
        ''', (ndt_wo_id,)).fetchone()
    
    @staticmethod
    def generate_pdf(certificate_data, wo_data, inspection_results):
        """Generate the FAA Form 8130-3 PDF document for NDT"""
        NDT8130Service.ensure_upload_folder()
        
        filename = f"{certificate_data['certificate_number']}.pdf"
        filepath = os.path.join(NDT8130Service.UPLOAD_FOLDER, filename)
        
        buffer = BytesIO()
        c = canvas.Canvas(buffer, pagesize=letter)
        width, height = letter
        
        c.setFont("Helvetica-Bold", 16)
        c.drawCentredString(width/2, height - 0.5*inch, "FAA FORM 8130-3")
        c.setFont("Helvetica-Bold", 14)
        c.drawCentredString(width/2, height - 0.75*inch, "Authorized Release Certificate - NDT Inspection")
        
        c.setStrokeColor(colors.black)
        c.setLineWidth(2)
        c.rect(0.5*inch, 0.5*inch, width - inch, height - 1.25*inch)
        
        y_pos = height - 1.25*inch
        left_margin = 0.6*inch
        
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left_margin, y_pos, "1. APPROVING CIVIL AVIATION AUTHORITY/COUNTRY")
        c.setFont("Helvetica", 10)
        c.drawString(left_margin, y_pos - 15, certificate_data.get('issuing_authority', 'FAA / United States'))
        
        c.setFont("Helvetica-Bold", 9)
        c.drawString(width/2, y_pos, "2. FORM TRACKING NUMBER")
        c.setFont("Helvetica", 10)
        c.drawString(width/2, y_pos - 15, certificate_data['certificate_number'])
        
        y_pos -= 45
        c.setLineWidth(0.5)
        c.line(left_margin, y_pos, width - 0.6*inch, y_pos)
        
        y_pos -= 5
        c.setFont("Helvetica-Bold", 11)
        c.drawCentredString(width/2, y_pos - 12, "3. AUTHORIZED RELEASE CERTIFICATE - NDT INSPECTION")
        
        y_pos -= 35
        c.line(left_margin, y_pos, width - 0.6*inch, y_pos)
        
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
        
        y_pos -= 5
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left_margin, y_pos - 12, "5. NDT WORK ORDER REFERENCE")
        c.setFont("Helvetica", 10)
        c.drawString(left_margin, y_pos - 27, wo_data.get('ndt_wo_number', 'N/A'))
        
        c.setFont("Helvetica-Bold", 9)
        c.drawString(width/2 + 0.5*inch, y_pos - 12, "11. APPROVAL/AUTHORIZATION NUMBER")
        c.setFont("Helvetica", 10)
        c.drawString(width/2 + 0.5*inch, y_pos - 27, certificate_data.get('approval_number', 'N/A'))
        
        y_pos -= 45
        c.line(left_margin, y_pos, width - 0.6*inch, y_pos)
        
        y_pos -= 5
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left_margin, y_pos - 12, "6. ITEM INSPECTED")
        
        c.setFont("Helvetica-Bold", 8)
        c.drawString(left_margin + 10, y_pos - 27, "a. NAME/DESCRIPTION")
        c.setFont("Helvetica", 10)
        part_name = wo_data.get('product_name') or wo_data.get('part_description', 'N/A')
        c.drawString(left_margin + 10, y_pos - 42, part_name[:60])
        
        c.setFont("Helvetica-Bold", 8)
        c.drawString(left_margin + 10, y_pos - 57, "b. PART NUMBER")
        c.setFont("Helvetica", 10)
        c.drawString(left_margin + 10, y_pos - 72, wo_data.get('product_code', 'N/A'))
        
        c.setFont("Helvetica-Bold", 8)
        c.drawString(width/2, y_pos - 27, "c. NDT METHODS")
        c.setFont("Helvetica", 10)
        ndt_methods = wo_data.get('ndt_methods', 'N/A')
        c.drawString(width/2, y_pos - 42, ndt_methods)
        
        c.setFont("Helvetica-Bold", 8)
        c.drawString(width/2, y_pos - 57, "d. APPLICABLE CODE")
        c.setFont("Helvetica", 9)
        c.drawString(width/2, y_pos - 72, wo_data.get('applicable_code', 'N/A'))
        
        y_pos -= 90
        c.line(left_margin, y_pos, width - 0.6*inch, y_pos)
        
        y_pos -= 5
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left_margin, y_pos - 12, "7. QUANTITY")
        c.setFont("Helvetica", 10)
        c.drawString(left_margin, y_pos - 27, str(certificate_data.get('quantity', 1)))
        
        c.setFont("Helvetica-Bold", 9)
        c.drawString(width/3, y_pos - 12, "8. SERIAL NUMBER")
        c.setFont("Helvetica", 10)
        c.drawString(width/3, y_pos - 27, certificate_data.get('serial_number') or wo_data.get('serial_number', 'N/A'))
        
        c.setFont("Helvetica-Bold", 9)
        c.drawString(width*2/3, y_pos - 12, "HEAT/LOT NUMBER")
        c.setFont("Helvetica", 10)
        c.drawString(width*2/3, y_pos - 27, certificate_data.get('heat_number') or wo_data.get('heat_number', 'N/A'))
        
        y_pos -= 45
        c.line(left_margin, y_pos, width - 0.6*inch, y_pos)
        
        y_pos -= 5
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left_margin, y_pos - 12, "9. INSPECTION RESULT")
        c.setFont("Helvetica-Bold", 11)
        inspection_result = certificate_data.get('inspection_result', 'ACCEPTABLE')
        if inspection_result == 'ACCEPTABLE':
            c.setFillColor(colors.green)
        else:
            c.setFillColor(colors.red)
        c.drawString(left_margin, y_pos - 30, inspection_result)
        c.setFillColor(colors.black)
        
        c.setFont("Helvetica-Bold", 9)
        c.drawString(width/2, y_pos - 12, "ACCEPTANCE CRITERIA")
        c.setFont("Helvetica", 9)
        criteria = wo_data.get('acceptance_criteria', 'Per applicable specifications')
        criteria_lines = [criteria[i:i+40] for i in range(0, min(len(criteria), 80), 40)]
        for i, line in enumerate(criteria_lines[:2]):
            c.drawString(width/2, y_pos - 27 - (i*12), line)
        
        y_pos -= 50
        c.line(left_margin, y_pos, width - 0.6*inch, y_pos)
        
        y_pos -= 5
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left_margin, y_pos - 12, "10. INSPECTION DETAILS")
        c.setFont("Helvetica", 9)
        
        if inspection_results:
            detail_y = y_pos - 27
            for i, result in enumerate(inspection_results[:3]):
                method = result.get('method', '')
                insp_result = result.get('result', '')
                insp_date = result.get('inspection_date', '')
                tech_name = result.get('technician_name', '')
                detail_text = f"{method}: {insp_result} - {insp_date} by {tech_name}"
                c.drawString(left_margin + 10, detail_y, detail_text[:80])
                detail_y -= 12
        
        y_pos -= 60
        c.line(left_margin, y_pos, width - 0.6*inch, y_pos)
        
        y_pos -= 5
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left_margin, y_pos - 12, "12. REMARKS")
        c.setFont("Helvetica", 9)
        remarks = certificate_data.get('remarks', '') or ''
        remarks_lines = [remarks[i:i+90] for i in range(0, len(remarks), 90)][:2]
        for i, line in enumerate(remarks_lines):
            c.drawString(left_margin, y_pos - 27 - (i*12), line)
        
        y_pos -= 55
        c.line(left_margin, y_pos, width - 0.6*inch, y_pos)
        
        y_pos -= 5
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left_margin, y_pos - 12, "13. NDT TECHNICIAN CERTIFICATION")
        c.drawString(left_margin, y_pos - 24, "I certify the item(s) were inspected per applicable NDT procedures and meet acceptance criteria.")
        
        c.setFont("Helvetica-Bold", 8)
        c.drawString(left_margin, y_pos - 42, "NAME:")
        c.setFont("Helvetica", 10)
        c.drawString(left_margin + 50, y_pos - 42, certificate_data.get('certifier_name', 'N/A'))
        
        c.setFont("Helvetica-Bold", 8)
        c.drawString(width/2, y_pos - 42, "CERT NO:")
        c.setFont("Helvetica", 10)
        c.drawString(width/2 + 60, y_pos - 42, certificate_data.get('certifier_certificate_number', 'N/A'))
        
        c.setFont("Helvetica-Bold", 8)
        c.drawString(left_margin, y_pos - 57, "DATE:")
        c.setFont("Helvetica", 10)
        c.drawString(left_margin + 40, y_pos - 57, certificate_data.get('certifier_signature_date', datetime.now().strftime('%Y-%m-%d')))
        
        y_pos -= 75
        c.line(left_margin, y_pos, width - 0.6*inch, y_pos)
        
        y_pos -= 5
        c.setFont("Helvetica-Bold", 9)
        c.drawString(left_margin, y_pos - 12, "14. LEVEL III REVIEW/AUTHORIZED SIGNATURE")
        
        c.setFont("Helvetica-Bold", 8)
        c.drawString(left_margin, y_pos - 30, "NAME:")
        c.setFont("Helvetica", 10)
        c.drawString(left_margin + 50, y_pos - 30, certificate_data.get('authorized_signature_name', 'N/A'))
        
        c.setFont("Helvetica-Bold", 8)
        c.drawString(width/2, y_pos - 30, "DATE:")
        c.setFont("Helvetica", 10)
        c.drawString(width/2 + 40, y_pos - 30, certificate_data.get('authorized_signature_date', datetime.now().strftime('%Y-%m-%d')))
        
        c.setFont("Helvetica", 8)
        c.drawCentredString(width/2, 0.6*inch, f"Generated by Dynamic.IQ-COREx on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        c.save()
        
        pdf_content = buffer.getvalue()
        pdf_hash = hashlib.sha256(pdf_content).hexdigest()
        
        with open(filepath, 'wb') as f:
            f.write(pdf_content)
        
        return filepath, pdf_hash
    
    @staticmethod
    def create_certificate(conn, ndt_wo_id, form_data, user_id):
        """Create a new 8130 certificate record for NDT and generate PDF"""
        from models import AuditLogger
        
        wo_data = NDT8130Service.get_ndt_work_order_data(conn, ndt_wo_id)
        
        if not wo_data:
            raise ValueError("NDT work order not found")
        
        if wo_data['status'] != 'Approved':
            raise ValueError("NDT work order must be Approved to generate 8130")
        
        existing = NDT8130Service.get_existing_certificate(conn, ndt_wo_id)
        if existing:
            raise ValueError(f"Certificate {existing['certificate_number']} already exists for this NDT work order")
        
        inspection_results = NDT8130Service.get_inspection_results(conn, ndt_wo_id)
        
        certificate_number = NDT8130Service.generate_certificate_number(conn)
        issue_date = datetime.now().strftime('%Y-%m-%d')
        
        all_acceptable = all(r.get('result') == 'Acceptable' for r in inspection_results) if inspection_results else True
        
        certificate_data = {
            'certificate_number': certificate_number,
            'issuing_authority': form_data.get('issuing_authority', 'FAA / United States'),
            'organization_name': form_data.get('organization_name') or wo_data.get('company_name', ''),
            'organization_address': form_data.get('organization_address', ''),
            'serial_number': form_data.get('serial_number') or wo_data.get('serial_number', ''),
            'batch_number': form_data.get('batch_number', ''),
            'heat_number': form_data.get('heat_number') or wo_data.get('heat_number', ''),
            'quantity': int(form_data.get('quantity', 1)),
            'inspection_result': 'ACCEPTABLE' if all_acceptable else 'REJECTED',
            'status_work': form_data.get('status_work', 'Inspected/Tested'),
            'approval_number': form_data.get('approval_number', ''),
            'remarks': form_data.get('remarks', ''),
            'certifier_name': form_data.get('certifier_name', ''),
            'certifier_certificate_number': form_data.get('certifier_certificate_number', ''),
            'certifier_signature_date': form_data.get('certifier_signature_date', issue_date),
            'authorized_signature_name': form_data.get('authorized_signature_name', ''),
            'authorized_signature_date': form_data.get('authorized_signature_date', issue_date),
        }
        
        pdf_path, pdf_hash = NDT8130Service.generate_pdf(certificate_data, wo_data, inspection_results)
        
        cursor = conn.execute('''
            INSERT INTO ndt_8130_certificates (
                certificate_number, ndt_wo_id, issue_date,
                issuing_authority, organization_name, organization_address,
                work_order_reference, part_name, part_number, part_description,
                quantity, serial_number, batch_number, heat_number,
                ndt_methods, applicable_code, acceptance_criteria, inspection_result,
                status_work, approval_number, remarks, 
                certifier_name, certifier_certificate_number, certifier_signature_date,
                authorized_signature_name, authorized_signature_date,
                pdf_file_path, pdf_file_hash, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            certificate_number, ndt_wo_id, issue_date,
            certificate_data['issuing_authority'],
            certificate_data['organization_name'],
            certificate_data['organization_address'],
            wo_data['ndt_wo_number'],
            wo_data.get('product_name', wo_data.get('part_description', '')),
            wo_data.get('product_code', ''),
            wo_data.get('part_description', ''),
            certificate_data['quantity'],
            certificate_data['serial_number'],
            certificate_data['batch_number'],
            certificate_data['heat_number'],
            wo_data.get('ndt_methods', ''),
            wo_data.get('applicable_code', ''),
            wo_data.get('acceptance_criteria', ''),
            certificate_data['inspection_result'],
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
        
        AuditLogger.log_change(
            conn=conn,
            record_type='ndt_8130_certificates',
            record_id=certificate_id,
            action_type='Created',
            modified_by=user_id,
            changed_fields={'certificate_number': certificate_number, 'ndt_wo_id': ndt_wo_id}
        )
        
        return {
            'id': certificate_id,
            'certificate_number': certificate_number,
            'pdf_path': pdf_path
        }
