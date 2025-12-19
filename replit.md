## Overview

Dynamic.IQ-COREx is a comprehensive Flask-based Manufacturing Resource Planning (MRP) system designed to optimize production processes, inventory, Bill of Materials (BOM), work orders, and purchase orders. It offers robust role-based access control, efficient material tracking, production planning, supplier management, and report generation. The system aims to enhance operational efficiency, deliver critical business insights, and includes advanced AI-driven modules for strategic insights and automation in supplier discovery and market/capability analysis.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### UI/UX Decisions

The system features a professional, elegant user interface built on Bootstrap 5, Bootstrap Icons, and custom CSS design tokens. It utilizes the Inter font family and a refined slate-based color palette for a cohesive executive look. Key UI elements include an Executive Dashboard with KPI cards, Chart.js visualizations, and responsive grid layouts.

**Professional Notification System (December 2025):**
-   Custom confirmation modals replace browser default `confirm()` dialogs
-   Custom alert modals replace browser default `alert()` dialogs
-   Elegant dark blue gradient header with icon indicators
-   Type-specific styling: info, warning, danger, success, question
-   Global functions available: `showConfirm(message, options)` and `showAlert(message, options)`
-   Options include: title, type, confirmText, cancelText
-   Toast notifications for success/error feedback with `showToast(message, type, title)`

### Technical Implementations

The backend is developed with Flask, using Blueprints and an SQLite database. It implements session-based authentication and comprehensive role-based access control. The `MRPEngine` handles core MRP logic. An Audit Trail System logs CUD operations. The frontend uses Jinja2 for templating.

**Key Modules and Features:**
-   **Core MRP**: Products & BOM, Inventory, Work Orders, Purchase Orders, Task Templates.
-   **Supply Chain & Sales**: RFQ Module, Contact Management, Sales Module, Shipping & Receiving, Invoice Management, Sales Order Exchange Management.
-   **Asset & Service Management**: Tools Management, Service Management, MRO Capabilities Management.
-   **Quality & Compliance**: NDT Module, Quality Management System (QMS).
-   **AI-Powered Modules**: AI Supplier Discovery, Market & Capability Analysis, Capacity Planning Module, Customer Service Module, Organizational Analyzer, Financial Analyzer, ERP Copilot (AI Helper), AI Super Master Scheduler, Part Analyzer, Business Analytics AI Super Agent, Secure IT Manager AI Super Agent.
-   **Integration**: Salesforce Data Migration Agent.

### Patent-Eligible Architecture (December 2025)

The system implements a novel patent-eligible architecture for ERP exchange management with the following components:

**1. Exchange Dependency Graph Engine** (`engines/exchange_graph.py`)
-   DAG-based data structure with O(1) node lookup and O(V+E) traversal
-   Hash-linked nodes for integrity verification
-   BFS dependency resolution for upstream/downstream analysis
-   Cached traversal results for performance optimization

**2. Deterministic Event Processing Engine** (`engines/event_engine.py`)
-   Idempotent event processing with idempotency key tracking
-   Hash-chained events for tamper detection and replay verification
-   Event sourcing for deterministic state reconstruction
-   Handler registration for event-driven processing

**3. AI Execution Path Modifier** (`engines/ai_executor.py`)
-   Risk vector computation from historical event patterns
-   CONCRETE execution path modifications (not recommendations):
  - Priority queue reordering for high-risk chains
  - Cache preloading decisions based on access predictions
  - Lock escalation for critical operations
  - Resource allocation adjustments
-   Effectiveness tracking for continuous learning

**4. Performance Instrumentation System** (`engines/performance_profiler.py`)
-   Latency measurement with before/after baseline comparison
-   Cache hit ratio tracking
-   Query reduction metrics
-   Comprehensive performance reporting

**5. Cryptographic Security Layer** (`security_utils/crypto.py`)
-   HMAC-SHA256 based access key generation
-   Role-scoped cryptographic access control
-   Tamper-evident audit trails with hash chain verification
-   Integrity verification with corruption detection

**6. Exchange Orchestrator** (`engines/orchestrator.py`)
-   Unified integration layer connecting all engines
-   Demonstrates patentable method:
  1. AI analyzes risk from historical event patterns
  2. System behavior is MODIFIED based on predictions
  3. Events are processed through cryptographic verification
  4. Operations are instrumented for improvement evidence

### System Design Choices

-   **Inventory Management**: Real-time stock level tracking, low stock alerts, and automatic updates.
-   **Reporting System**: Provides various reports including inventory valuation, work order cost analysis, and material requirements.
-   **Accounting System**: Features a Chart of Accounts (COA), General Ledger (GL), Manual Journal Entries, automatic GL posting, and financial reports.
-   **Time Clock Station**: Dedicated employee time tracking system with simplified employee code authentication and automatic labor cost tracking. Features skill-based task filtering - employees can only clock into work order tasks for which they have the required skills. Employee skills are displayed in the clock station header with certification badges.
-   **Labor Resources & Skillset Management**: Comprehensive multi-skillset assignment system for tracking employee competencies and strategic capacity planning.
-   **Service/Misc PO for Work Orders**: Create purchase orders for miscellaneous charges and outside services (heat treatment, plating, NDT, etc.) directly linked to work orders. Includes service line tracking, receiving workflow, and automatic integration into work order cost calculations. Supports categories: Outside Processing, Heat Treatment, Plating/Coating, Testing/Inspection, Machining, NDT Services, Calibration, Engineering Services, Expedite Fee, Freight/Shipping, Tooling, Consulting.
-   **UOM Conversion System**: High-precision unit of measure conversion for purchase orders using Python Decimal for accuracy. Features include: base quantity tracking, conversion factor audit trails, extended cost calculations, and proper cost allocation on receiving. Key tables: `uom_master`, `uom_conversions`. Utility module: `utils/uom_conversion.py`.
-   **Part Intake System**: AI-powered supplier web part capture system for converting supplier catalog data into ERP products. Features include:
    - Web URL/PDF/file capture with metadata extraction
    - AI extraction using GPT-4o with confidence scoring
    - Automatic data normalization and technical attribute parsing
    - Duplicate detection by MPN, supplier cross-reference, and description similarity
    - Conversion workflow: create new product or link to existing
    - Audit trail tracking all actions (capture, AI extraction, manual edits, conversion)
    - Key tables: `part_intake_records`, `part_intake_extracted_data`, `part_intake_supplier_xref`, `part_intake_audit`
    - Routes module: `routes/part_intake_routes.py`
-   **Dual Exchange Workflow**: Sales Order to Purchase Order exchange automation for aviation parts exchange programs. Features include:
    - Sales Orders with Exchange Type = "Dual Exchange" can generate linked Exchange Fee Purchase Orders
    - Exchange owner selection (Customer or Supplier) with validation
    - Unique exchange reference ID generation for traceability
    - One-to-one relationship enforcement (prevents duplicate Exchange POs per Sales Order)
    - Exchange Fee banner on PO view with source Sales Order reference
    - Exchange Owner Information section on PO view
    - Linked Exchange POs table on Sales Order view
    - Audit trail logging for Exchange PO creation
    - Database fields: `purchase_orders.is_exchange`, `exchange_owner_type`, `exchange_owner_id`, `exchange_reference_id`, `source_sales_order_id`, `exchange_status`
    - Routes: `salesorder_routes.create_exchange_po`, `salesorder_routes.get_exchange_owner_details`
-   **Exchange PO Obligations Tracking**: Comprehensive tracking of exchange obligations in the Exchange Management dashboard:
    - Exchange PO Obligations table shows all Exchange POs with owner information
    - Tracks who owes the exchange (Customer Owned vs Supplier Owned)
    - Shows owner name (the specific customer or supplier responsible)
    - Due date tracking with overdue indicators
    - Days overdue calculation with visual alerts
    - Linked source Sales Order references
    - Exchange reference ID display
    - Stats summary: Total, Customer Owned, Supplier Owned, Overdue counts
    - Integration with Exchange detail view showing Dual Exchange POs in POs tab
-   **Professional Shipping Document Generation System** (December 2025): Comprehensive PDF document generation for shipments with versioning, persistence, and audit trail. Features include:
    - Packing Slip generation with line items, ship-to address, package details, and signature lines
    - Certificate of Conformance (C of C) with compliance standards, signatory name, and certification statement
    - Commercial Invoice with pricing, HS codes, country of origin, and export declaration
    - Document versioning (V1, V2, etc.) for each document type per shipment
    - PDF persistence to `static/documents/` directory with file_path tracking
    - Document history panel showing all generated documents with status badges
    - Download/view functionality for previously generated documents
    - Status tracking: Draft, Final, Unsigned, Signed
    - Audit trail logging for document creation and status changes
    - Key table: `shipment_documents` with versioning, status, file_path, and electronic signature support
    - Utility module: `utils/shipping_documents.py` (ShippingDocumentGenerator class using ReportLab)
    - Routes module: `routes/shipping_routes.py`
-   **Work Order Accordion Layout with Task-Level Material Requirements** (December 2025): Redesigned work order record page with collapsible accordion sections and task-level material management. Features include:
    - Accordion-based page structure with logical collapsible sections: Overview, Customer Info, Tasks, Materials, Labor, Cost, Documents, Audit Trail
    - KPI summary cards at top showing total tasks, completed count, actual hours, and total cost
    - Sticky action bar with quick access buttons for common operations
    - Task cards within accordion with expandable content showing task details and materials
    - Task-level material requirements nested under each task with inline add/edit/issue/consume functionality
    - Material status tracking: Planned, Partially Issued, Issued, Consumed
    - Material shortage indicators with visual pulse animation for unfulfilled requirements
    - Task status updates with validation (cannot complete task without issuing required materials)
    - Lot/Serial number tracking on material issuance
    - Material rollup summary per task showing count, total required, issued, and consumed
    - Key table: `work_order_task_materials` with task_id, product_id, required_qty, issued_qty, consumed_qty, material_status, lot_number, serial_number
    - Routes: `add_task_material`, `edit_task_material`, `delete_task_material`, `issue_task_material`, `consume_task_material`, `update_task_status`
-   **Marketing Presentation Generator with PDF Download** (December 2025): AI-powered marketing presentation generator with professional PDF export. Features include:
    - Brand colors: Primary, Secondary, and Accent colors with color picker UI
    - Marketing tagline field for value proposition statement
    - Brand tone selection: Enterprise, Innovative, or Authoritative
    - Marketing description for executive-level system summary
    - Target industries field for market focus
    - Key differentiators field for competitive advantages
    - AI-powered presentation generation using OpenAI GPT-4o
    - Professional PDF download with branded hero section, value propositions, capabilities, industries, stats, testimonials, and CTA
    - PDF includes company branding colors, multi-page layout, and elegant typography
    - Color preview panel for visual validation
    - Database columns in `company_settings`: `marketing_tagline`, `brand_primary_color`, `brand_secondary_color`, `brand_accent_color`, `brand_tone`, `marketing_description`, `target_industries`, `key_differentiators`
    - Routes module: `routes/settings_routes.py` (`edit_marketing_settings`, `generate_presentation`, `download_presentation_pdf`)
    - Utility module: `utils/presentation_pdf.py` (PresentationPDFGenerator class using ReportLab)
    - Template: `templates/settings/edit_marketing.html`, `templates/settings/generate_presentation.html`

## External Dependencies

-   **Python Packages**: `Flask`, `Flask-Login`, `Werkzeug`, `ReportLab`, `Pandas`, `openpyxl`, `openai`, `sqlite3`.
-   **Frontend Libraries**: Bootstrap 5.3.0, Bootstrap Icons 1.11.0, Chart.js 4.4.0.
-   **AI Integration**: OpenAI API (GPT-4o).
-   **Database**: SQLite (`mrp.db`).
-   **Environment Variables**: `SESSION_SECRET`.