"""
Certificate of Compliance (CoC) Generation Service
Automatically generates a CoC PDF when a work order is marked Completed
and saves it to the work order's Documents tab.
"""
import os
import logging
from datetime import datetime
from io import BytesIO

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

logger = logging.getLogger(__name__)

UPLOAD_BASE = os.path.join('uploads', 'work_order_documents')


class CoCService:
    """Generates and saves Certificate of Compliance PDFs for completed work orders."""

    @staticmethod
    def _get_wo_data(conn, work_order_id):
        return conn.execute('''
            SELECT wo.*,
                   p.code as product_code, p.name as product_name,
                   p.description as product_description,
                   c.name as customer_name,
                   cs.company_name, cs.address_line1 as company_address,
                   cs.city as company_city, cs.state as company_state,
                   cs.postal_code as company_zip, cs.phone as company_phone
            FROM work_orders wo
            JOIN products p ON wo.product_id = p.id
            LEFT JOIN customers c ON wo.customer_id = c.id
            LEFT JOIN company_settings cs ON cs.id = 1
            WHERE wo.id = ?
        ''', (work_order_id,)).fetchone()

    @staticmethod
    def _get_tasks(conn, work_order_id):
        try:
            return conn.execute('''
                SELECT task_name AS name,
                       status,
                       remarks    AS description,
                       actual_hours
                FROM work_order_tasks
                WHERE work_order_id = ?
                ORDER BY sequence_number, id
            ''', (work_order_id,)).fetchall()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return []

    @staticmethod
    def _already_exists(conn, work_order_id):
        """Return True if a CoC document is already recorded for this WO."""
        try:
            row = conn.execute('''
                SELECT id FROM work_order_documents
                WHERE work_order_id = ? AND document_type = 'Certificate of Compliance'
                LIMIT 1
            ''', (work_order_id,)).fetchone()
            return row is not None
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return False

    @staticmethod
    def _build_pdf(wo, tasks, filepath):
        """Build the PDF and write it to filepath. Returns file size in bytes."""
        styles = getSampleStyleSheet()

        title_style = ParagraphStyle(
            'CoCTitle',
            parent=styles['Title'],
            fontSize=18,
            textColor=colors.HexColor('#1a3c5e'),
            spaceAfter=4,
            alignment=TA_CENTER,
        )
        sub_style = ParagraphStyle(
            'CoCSubtitle',
            parent=styles['Normal'],
            fontSize=11,
            textColor=colors.HexColor('#2c6fad'),
            spaceAfter=2,
            alignment=TA_CENTER,
        )
        label_style = ParagraphStyle(
            'Label',
            parent=styles['Normal'],
            fontSize=8,
            textColor=colors.HexColor('#666666'),
            spaceBefore=0,
            spaceAfter=0,
        )
        value_style = ParagraphStyle(
            'Value',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.black,
            spaceBefore=0,
            spaceAfter=0,
        )
        statement_style = ParagraphStyle(
            'Statement',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.HexColor('#222222'),
            leading=14,
            spaceAfter=6,
        )
        footer_style = ParagraphStyle(
            'Footer',
            parent=styles['Normal'],
            fontSize=8,
            textColor=colors.HexColor('#888888'),
            alignment=TA_CENTER,
        )

        doc = SimpleDocTemplate(
            filepath,
            pagesize=letter,
            leftMargin=0.75 * inch,
            rightMargin=0.75 * inch,
            topMargin=0.6 * inch,
            bottomMargin=0.6 * inch,
        )

        story = []

        company_name = wo.get('company_name') or 'Dynamic.IQ-COREx'
        company_addr_parts = [
            wo.get('company_address') or '',
            wo.get('company_city') or '',
            wo.get('company_state') or '',
            wo.get('company_zip') or '',
        ]
        company_addr = ', '.join(p for p in company_addr_parts if p)
        company_phone = wo.get('company_phone') or ''

        wo_number = wo.get('wo_number') or str(wo.get('id', ''))
        product_name = wo.get('product_name') or ''
        product_code = wo.get('product_code') or ''
        customer_name = wo.get('customer_name') or 'N/A'
        quantity = wo.get('quantity') or ''
        serial_number = wo.get('serial_number') or 'N/A'
        disposition = wo.get('disposition') or 'N/A'
        planned_start = wo.get('planned_start_date') or 'N/A'
        actual_end = wo.get('actual_end_date') or datetime.now().strftime('%Y-%m-%d')
        repair_category = wo.get('repair_category') or 'N/A'
        workorder_type = wo.get('workorder_type') or 'N/A'
        notes = wo.get('notes') or ''

        doc_date = datetime.now().strftime('%B %d, %Y')
        doc_number = f"COC-{wo_number}-{datetime.now().strftime('%Y%m%d')}"

        story.append(Paragraph(company_name.upper(), title_style))
        story.append(Paragraph('CERTIFICATE OF COMPLIANCE', sub_style))
        story.append(Paragraph(f'Document No.: {doc_number}', ParagraphStyle(
            'DocNo', parent=styles['Normal'], fontSize=9,
            textColor=colors.HexColor('#555555'), alignment=TA_CENTER, spaceAfter=8
        )))
        story.append(HRFlowable(width='100%', thickness=2, color=colors.HexColor('#1a3c5e'), spaceAfter=10))

        def field_row(label, value):
            return [
                Paragraph(label, label_style),
                Paragraph(str(value), value_style),
            ]

        header_data = [
            [Paragraph('WORK ORDER NUMBER', label_style), Paragraph('DATE ISSUED', label_style),
             Paragraph('PRODUCT CODE', label_style), Paragraph('PRODUCT NAME', label_style)],
            [Paragraph(wo_number, value_style), Paragraph(doc_date, value_style),
             Paragraph(product_code, value_style), Paragraph(product_name, value_style)],
        ]
        header_table = Table(header_data, colWidths=[1.5 * inch, 1.5 * inch, 1.5 * inch, 2.5 * inch])
        header_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#eef3f8')),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 3),
            ('TOPPADDING', (0, 0), (-1, 0), 4),
            ('BOTTOMPADDING', (0, 1), (-1, 1), 6),
            ('TOPPADDING', (0, 1), (-1, 1), 4),
            ('BOX', (0, 0), (-1, -1), 0.5, colors.HexColor('#b0c4d8')),
            ('INNERGRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#b0c4d8')),
        ]))
        story.append(header_table)
        story.append(Spacer(1, 6))

        detail_data = [
            [Paragraph('CUSTOMER', label_style), Paragraph('QUANTITY', label_style),
             Paragraph('SERIAL / LOT NUMBER', label_style), Paragraph('DISPOSITION', label_style)],
            [Paragraph(customer_name, value_style), Paragraph(str(quantity), value_style),
             Paragraph(serial_number, value_style), Paragraph(disposition, value_style)],
            [Paragraph('WORK ORDER TYPE', label_style), Paragraph('REPAIR CATEGORY', label_style),
             Paragraph('START DATE', label_style), Paragraph('COMPLETION DATE', label_style)],
            [Paragraph(workorder_type, value_style), Paragraph(repair_category, value_style),
             Paragraph(str(planned_start), value_style), Paragraph(str(actual_end), value_style)],
        ]
        detail_table = Table(detail_data, colWidths=[1.75 * inch, 1.25 * inch, 2.0 * inch, 2.0 * inch])
        detail_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#eef3f8')),
            ('BACKGROUND', (0, 2), (-1, 2), colors.HexColor('#eef3f8')),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOX', (0, 0), (-1, -1), 0.5, colors.HexColor('#b0c4d8')),
            ('INNERGRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#b0c4d8')),
        ]))
        story.append(detail_table)
        story.append(Spacer(1, 10))

        story.append(HRFlowable(width='100%', thickness=0.5, color=colors.HexColor('#b0c4d8'), spaceAfter=8))

        if tasks:
            story.append(Paragraph('WORK PERFORMED', ParagraphStyle(
                'SectionHeader', parent=styles['Normal'], fontSize=9, fontName='Helvetica-Bold',
                textColor=colors.HexColor('#1a3c5e'), spaceAfter=4
            )))
            task_rows = [[
                Paragraph('TASK', label_style),
                Paragraph('DESCRIPTION', label_style),
                Paragraph('STATUS', label_style),
                Paragraph('ACTUAL HRS', label_style),
            ]]
            for t in tasks:
                status_text = t.get('status') or 'Completed'
                hrs = t.get('actual_hours')
                hrs_text = f"{float(hrs):.1f}" if hrs else '—'
                task_rows.append([
                    Paragraph(t.get('name') or '', value_style),
                    Paragraph(t.get('description') or '', value_style),
                    Paragraph(status_text, value_style),
                    Paragraph(hrs_text, value_style),
                ])
            task_table = Table(task_rows, colWidths=[1.8 * inch, 3.2 * inch, 1.0 * inch, 1.0 * inch])
            task_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#eef3f8')),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f7fafc')]),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
                ('TOPPADDING', (0, 0), (-1, -1), 3),
                ('BOX', (0, 0), (-1, -1), 0.5, colors.HexColor('#b0c4d8')),
                ('INNERGRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#b0c4d8')),
            ]))
            story.append(task_table)
            story.append(Spacer(1, 10))

        if notes:
            story.append(Paragraph('NOTES / REMARKS', ParagraphStyle(
                'SectionHeader', parent=styles['Normal'], fontSize=9, fontName='Helvetica-Bold',
                textColor=colors.HexColor('#1a3c5e'), spaceAfter=4
            )))
            notes_table = Table([[Paragraph(notes, value_style)]], colWidths=[7.0 * inch])
            notes_table.setStyle(TableStyle([
                ('BOX', (0, 0), (-1, -1), 0.5, colors.HexColor('#b0c4d8')),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('LEFTPADDING', (0, 0), (-1, -1), 8),
                ('RIGHTPADDING', (0, 0), (-1, -1), 8),
            ]))
            story.append(notes_table)
            story.append(Spacer(1, 10))

        story.append(HRFlowable(width='100%', thickness=0.5, color=colors.HexColor('#b0c4d8'), spaceAfter=8))
        story.append(Paragraph('STATEMENT OF CONFORMANCE', ParagraphStyle(
            'SectionHeader', parent=styles['Normal'], fontSize=9, fontName='Helvetica-Bold',
            textColor=colors.HexColor('#1a3c5e'), spaceAfter=6
        )))

        conformance_text = (
            f"We hereby certify that the item(s) described in this document have been "
            f"manufactured, inspected, and tested in accordance with applicable specifications, "
            f"standards, and requirements, and are found to conform to the applicable design data "
            f"and are in a condition for safe operation. "
            f"Work Order <b>{wo_number}</b> — Product: <b>{product_code} {product_name}</b> — "
            f"Quantity: <b>{quantity}</b> — Disposition: <b>{disposition}</b>."
        )
        story.append(Paragraph(conformance_text, statement_style))
        story.append(Spacer(1, 16))

        sig_data = [
            [Paragraph('Authorized Signature', label_style),
             Paragraph('', label_style),
             Paragraph('Date', label_style),
             Paragraph('', label_style)],
            [Paragraph('________________________', value_style),
             Paragraph('', value_style),
             Paragraph('________________________', value_style),
             Paragraph('', value_style)],
            [Paragraph('Title / Certification Number', label_style),
             Paragraph('', label_style),
             Paragraph('Issuing Organization', label_style),
             Paragraph('', label_style)],
            [Paragraph('________________________', value_style),
             Paragraph('', value_style),
             Paragraph(company_name, value_style),
             Paragraph('', value_style)],
        ]
        sig_table = Table(sig_data, colWidths=[2.5 * inch, 0.5 * inch, 2.5 * inch, 1.5 * inch])
        sig_table.setStyle(TableStyle([
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
        ]))
        story.append(sig_table)

        story.append(Spacer(1, 20))
        story.append(HRFlowable(width='100%', thickness=0.5, color=colors.HexColor('#cccccc'), spaceAfter=4))
        addr_line = company_addr
        if company_phone:
            addr_line += f'  |  Tel: {company_phone}'
        story.append(Paragraph(addr_line, footer_style))
        story.append(Paragraph(f'Document generated automatically on {doc_date} by {company_name}', footer_style))

        doc.build(story)
        return os.path.getsize(filepath)

    @staticmethod
    def generate_and_save(conn, work_order_id, created_by_user_id=None, force=False):
        """
        Generate the CoC PDF and record it in work_order_documents.
        Safe to call in a transaction — does NOT commit.
        If force=True, deletes any existing CoC record and regenerates.
        Returns dict with 'success', 'filename', 'message'.
        """
        try:
            if CoCService._already_exists(conn, work_order_id):
                if not force:
                    return {'success': True, 'filename': None,
                            'message': 'Certificate of Compliance already exists for this work order.'}
                # Remove old DB records so a fresh one is created
                conn.execute('''
                    DELETE FROM work_order_documents
                    WHERE work_order_id = ? AND document_type = 'Certificate of Compliance'
                ''', (work_order_id,))

            wo = CoCService._get_wo_data(conn, work_order_id)
            if not wo:
                return {'success': False, 'filename': None, 'message': 'Work order not found.'}

            wo = dict(wo)
            tasks = CoCService._get_tasks(conn, work_order_id)

            wo_number = wo.get('wo_number') or str(work_order_id)
            upload_dir = os.path.join(UPLOAD_BASE, str(work_order_id))
            os.makedirs(upload_dir, exist_ok=True)

            safe_wo_num = wo_number.replace('/', '-').replace('\\', '-')
            filename = f"CoC-{safe_wo_num}.pdf"
            filepath = os.path.join(upload_dir, filename)

            file_size = CoCService._build_pdf(wo, tasks, filepath)

            conn.execute('''
                INSERT INTO work_order_documents
                (work_order_id, document_type, document_name, file_path,
                 original_filename, file_size, mime_type, description, uploaded_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                work_order_id,
                'Certificate of Compliance',
                filename,
                filepath,
                filename,
                file_size,
                'application/pdf',
                f'Auto-generated Certificate of Compliance for Work Order {wo_number}',
                created_by_user_id,
            ))

            logger.info(f'CoC generated for WO {wo_number}: {filepath}')
            return {'success': True, 'filename': filename,
                    'message': f'Certificate of Compliance generated: {filename}'}

        except Exception as exc:
            logger.error(f'CoC generation failed for WO {work_order_id}: {exc}', exc_info=True)
            return {'success': False, 'filename': None, 'message': f'CoC generation error: {exc}'}
