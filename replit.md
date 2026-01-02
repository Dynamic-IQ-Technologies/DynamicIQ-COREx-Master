## Overview

Dynamic.IQ-COREx is a Flask-based Manufacturing Resource Planning (MRP) system designed to optimize production processes, inventory, Bill of Materials (BOM), work orders, and purchase orders. It provides robust role-based access control, efficient material tracking, production planning, supplier management, and report generation. The system aims to enhance operational efficiency, deliver critical business insights, and includes advanced AI-driven modules for strategic insights, automation in supplier discovery, and market/capability analysis, contributing to improved business intelligence and operational excellence.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### UI/UX Decisions

The system features a professional, elegant user interface built on Bootstrap 5, Bootstrap Icons, and custom CSS design tokens, utilizing the Inter font family and a slate-based color palette. Key UI elements include an Executive Dashboard with KPI cards, Chart.js visualizations, responsive grid layouts, and a custom professional notification system for confirmations, alerts, and toast messages.

### Technical Implementations

The backend is developed with Flask, using Blueprints and an SQLite database, implementing session-based authentication and role-based access control. The `MRPEngine` handles core MRP logic, and an Audit Trail System logs CUD operations. The frontend uses Jinja2 for templating.

**Key Modules and Features:**
-   **Core MRP**: Products & BOM, Inventory, Work Orders, Purchase Orders, Task Templates.
-   **Supply Chain & Sales**: RFQ Module, Contact Management, Sales Module, Shipping & Receiving, Invoice Management, Sales Order Exchange Management.
-   **Asset & Service Management**: Tools Management, Service Management, MRO Capabilities Management.
-   **Quality & Compliance**: NDT Module, Quality Management System (QMS).
-   **AI-Powered Modules**: AI Supplier Discovery, Market & Capability Analysis, Capacity Planning, Customer Service, Organizational Analyzer, Financial Analyzer, ERP Copilot, AI Super Master Scheduler, Part Analyzer, Business Analytics AI Super Agent, Secure IT Manager AI Super Agent.
-   **Integration**: Salesforce Data Migration Agent.

### Patent-Eligible Architecture

The system incorporates a novel architecture for ERP exchange management, comprising:
-   **Exchange Dependency Graph Engine**: A DAG-based structure for O(1) node lookup and O(V+E) traversal, with hash-linked nodes and BFS dependency resolution.
-   **Deterministic Event Processing Engine**: Idempotent event processing with hash-chained events for tamper detection and event sourcing.
-   **AI Execution Path Modifier**: Computes risk vectors from historical event patterns to modify execution paths, including priority queue reordering, cache preloading, and resource allocation adjustments.
-   **Performance Instrumentation System**: Measures latency, cache hit ratios, and query reduction for performance reporting.
-   **Cryptographic Security Layer**: HMAC-SHA256 based access key generation, role-scoped access control, and tamper-evident audit trails.
-   **Exchange Orchestrator**: Unifies all engines, demonstrating a patentable method of AI-driven risk analysis, system behavior modification, cryptographic verification, and performance instrumentation.

### System Design Choices

-   **Inventory Management**: Real-time stock tracking, low stock alerts, and automatic updates.
-   **Reporting System**: Various reports including inventory valuation and work order cost analysis.
-   **Accounting System**: Chart of Accounts, General Ledger, Manual Journal Entries, and financial reports.
-   **Time Clock Station**: Employee time tracking with skill-based task filtering and labor cost tracking.
-   **Labor Resources & Skillset Management**: Multi-skillset assignment for capacity planning.
-   **Service/Misc PO for Work Orders**: Create purchase orders for services and charges linked to work orders, integrating costs.
-   **UOM Conversion System**: High-precision unit of measure conversion using Python Decimal for accuracy in purchase orders.
-   **Part Intake System**: AI-powered web part capture for converting supplier catalog data into ERP products with AI extraction, normalization, and duplicate detection.
-   **Dual Exchange Workflow**: Automates Sales Order to Purchase Order exchange for aviation parts programs, including exchange fee tracking and traceability.
-   **Exchange PO Obligations Tracking**: Dashboard for tracking exchange obligations, due dates, and owner information.
-   **Professional Shipping Document Generation System**: Generates versioned packing slips, Certificates of Conformance, and commercial invoices with persistence and audit trails.
-   **Work Order Accordion Layout with Task-Level Material Requirements**: Redesigned work order page with collapsible sections, KPI summary cards, task-level material management with status tracking, and shortage indicators.
-   **Marketing Presentation Generator with PDF Download**: AI-powered generator using brand colors, taglines, and tones to create professional PDF presentations.
-   **Repair Order (External Repair / MRO Services) Module**: Manages external repairs with a full lifecycle (Draft to Closed), state machine enforcement, item locking, outbound shipment creation, receiving workflow, and cost variance tracking.
-   **Customer Portal with Work Order Quote Approval**: Token-based customer portal allowing customers to view pending work order quotes with pricing breakdowns (parts, labor, consumables, fees), approve or decline quotes with signature capture and notes, and full audit trail tracking including customer information and IP addresses.
-   **Master Routing (Work Order Template) Module**: Standardized process templates for work orders with reusable operations, material requirements, and quality checks. Supports status workflow (Draft -> Under Review -> Approved -> Active -> Obsolete), automatic application to work orders during creation, and product-based routing matching.
-   **Professional Sales Order Document Generation**: Print-ready Sales Order viewing with professional layout including company branding, order/customer information blocks, line item tables with serial/lot tracking, totals section, and notes. Features PDF download capability using ReportLab for high-quality document generation.
-   **Master Plan Planning Report**: Comprehensive planning report for products flagged as "Master Plan Part" showing inventory on hand, exchange status (customer/supplier owed), active work orders with status breakdown, and predictive forecast based on 90-day consumption history. Calculates weeks of supply and assigns status indicators (Critical, Low, Adequate, Sufficient) to support inventory planning decisions. Includes detailed exchange line views with customer/supplier pending returns and due dates.
-   **Sales Order Email Acknowledgement System**: Preview and email sales order acknowledgements when confirming orders. Features professional HTML email template with company branding, order details, line items, and totals. Supports optional email sending (requires SMTP_HOST, RESEND_API_KEY, or SENDGRID_API_KEY environment variable configuration).
-   **Inventory Document Upload System**: Attach documents (PDF, images, spreadsheets, etc.) directly to inventory line items via a "Documents" tab in the Associated Transactions section. Supports document type categorization (Certificate, Test Report, Inspection, Warranty, Manual, Specification, Photo), file size display, uploader tracking, and secure download. Role-based access controls uploading (Admin, Production Staff, Procurement) and deletion (Admin, Production Staff).
-   **Work Order Allocation to Sales Order Lines**: Link available work orders to sales order line items. Features purple-themed UI to distinguish from inventory allocation (green). Modal displays available work orders for the same product with status, priority, and disposition. Supports allocation and deallocation with confirmation dialogs. Ensures work orders can only be linked to one sales order at a time.
-   **Work Order Reconciliation Module**: Formal cost reconciliation process for work orders following ERP best practices. Compares planned vs actual for labor hours/cost, material quantity/cost, and outside services. Auto-calculates variances with color-coded indicators. Requires reconciliation notes for approval. Work orders cannot be completed until reconciled. Role-restricted to Admin, Finance, and Supervisor roles. Reconciliation is locked after approval (Admin can invalidate if needed). Full audit trail logging of all reconciliation actions.
-   **Invoice Customer Email Portal**: Token-based secure customer portal for viewing invoices. Features include: secure 60-day token links, professional HTML email templates with invoice details and line items, customer portal page for invoice viewing with print capability. Uses Brevo API for email delivery (BREVO_API_KEY and BREVO_FROM_EMAIL environment secrets). Accessible from invoice view page via "Send to Customer" button for Approved/Posted/Paid invoices.
-   **Supplier Portal for Interactive PO Management**: Token-based secure supplier portal enabling suppliers to view their open purchase orders without logging in. Features include: configurable link validity (30-365 days), supplier dashboard showing all open POs with KPI summary, detailed PO view with line items, ability for suppliers to update tracking numbers, estimated ship dates, and notes for each line item. All supplier updates are logged with IP address and timestamp for complete audit trail. Portal access is tracked with access counts and last access timestamps. Accessible from supplier detail page via "Generate Link" button (Admin/Procurement roles).
-   **FAA-Compliant Inventory Labels**: Generate printable PDF labels for inventory items in compliance with 14 CFR Part 45 FAA identification and marking requirements. Labels include part number with barcode, serial number with barcode (for serialized items), lot/batch number, manufacturer code, MSN/ESN, condition, country of origin, trace tag/type, lifecycle data (TSN/TSO/CSN/CSO), manufacturing and expiration dates, warehouse/bin location, and quantity. Supports multiple label sizes (4x6, 4x4, 2x4 inches) and multiple copies. Accessible from inventory view page via "FAA Label" dropdown button.
-   **Executive Sales Dashboard**: C-Suite strategic intelligence dashboard for MRO operations. Features comprehensive KPIs (Revenue MTD/QTD/YTD, Gross Margin %, Pipeline Value, Win Rate, Revenue at Risk, DSO, A/R Outstanding), 12-month revenue trend chart, revenue by type doughnut chart, sales pipeline visualization by stage, quote aging analysis, upcoming work inductions forecast (30/60/90 days), top 20 customers by revenue, weighted revenue forecasts, executive alerts panel for proactive insights, and AI-powered Sales Copilot for natural language business queries. Accessible from Sales section in navigation.
-   **Executive Procurement Dashboard**: Supply chain strategic intelligence dashboard for procurement operations. Features comprehensive KPIs (Spend 30/90/365 days with trend comparison, Open PO Value/Count, PO Cycle Time, Supplier OTIF %, Inventory Value, Low Stock/Stock-Out counts, Overdue POs, WO Material Shortages, AOG Spend %), 12-month spend trend chart, spend by category doughnut chart, top suppliers by spend ranking, supplier performance scorecard (on-time %, avg lead time), pending deliveries with due date tracking, 30/60/90 day delivery forecast, executive alerts for overdue POs/low stock/material shortages, and AI-powered Procurement Copilot for natural language supply chain queries. Role-based access for Admin, Finance, and Supervisor roles. Accessible from Procurement section in navigation.
-   **Leads Management System**: CRM-grade lead capture, evaluation, and conversion module for Aviation/MRO sales teams. Features include: structured lead capture with Aviation-specific fields (business type, aircraft platforms, ATA chapters, compliance certifications), automated lead scoring (0-100 with Hot/Warm/Cold categorization) based on revenue potential, urgency, strategic fit, and compliance readiness. Supports controlled lifecycle workflow (New -> Contacted -> Qualified -> In Evaluation -> Approved for Conversion -> Converted/Disqualified) with status change audit trails. One-click conversion to Customer or Supplier accounts with duplicate prevention. Public web lead submission form with honeypot spam protection and timestamp validation. Activity timeline for tracking calls, emails, meetings, and follow-ups. Lead analytics dashboard with conversion rates, source performance, top performers, and 12-month trends. AI-powered Sales Engagement Copilot for prioritization and conversion insights. Role-based access for Admin, Finance, Supervisor, and Sales roles. Accessible from Sales section in navigation.

## External Dependencies

-   **Python Packages**: `Flask`, `Flask-Login`, `Werkzeug`, `ReportLab`, `Pandas`, `openpyxl`, `openai`, `sqlite3`.
-   **Frontend Libraries**: Bootstrap 5.3.0, Bootstrap Icons 1.11.0, Chart.js 4.4.0.
-   **AI Integration**: OpenAI API (GPT-4o).
-   **Database**: SQLite (`mrp.db`).
-   **Environment Variables**: `SESSION_SECRET`.