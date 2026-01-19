## Overview

Dynamic.IQ-COREx is a Flask-based Manufacturing Resource Planning (MRP) system designed to optimize production processes, inventory, Bill of Materials (BOM), work orders, and purchase orders. It provides robust role-based access control, efficient material tracking, production planning, supplier management, and report generation. The system aims to enhance operational efficiency, deliver critical business insights, and includes advanced AI-driven modules for strategic insights, automation in supplier discovery, and market/capability analysis, contributing to improved business intelligence and operational excellence.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### UI/UX Decisions

The system features a professional, elegant user interface built on Bootstrap 5, Bootstrap Icons, and custom CSS design tokens, utilizing the Inter font family and a slate-based color palette. Key UI elements include an Executive Dashboard with KPI cards, Chart.js visualizations, responsive grid layouts, and a custom professional notification system.

### Technical Implementations

The backend is developed with Flask, using Blueprints, implementing session-based authentication and role-based access control. The `MRPEngine` handles core MRP logic, and an Audit Trail System logs CUD operations. The frontend uses Jinja2 for templating. Key modules include Core MRP, Supply Chain & Sales, Asset & Service Management, Quality & Compliance, and various AI-Powered Modules.

### Database Configuration

The system supports dual database environments:
-   **Development**: SQLite (`mrp.db`) for fast iteration and local development
-   **Production**: PostgreSQL (Neon-backed) for data persistence and scalability

The environment is determined by the `REPLIT_DEPLOYMENT` flag:
-   When `REPLIT_DEPLOYMENT=1`, the app uses PostgreSQL via `DATABASE_URL`
-   When not set (development), the app uses SQLite

**Migration Script**: `python scripts/init_postgres.py` - Mirrors SQLite schema to PostgreSQL and migrates all data.

**PostgreSQL Compatibility Layer**: The `PostgresConnection` wrapper in `models.py` provides SQLite-like interface:
-   Converts `?` placeholders to `%s` for PostgreSQL
-   Automatically adds `RETURNING id` for simple INSERT statements
-   Intercepts `SELECT last_insert_rowid()` calls and returns cached insert ID
-   Note: For full PostgreSQL parity, future refactoring should replace `SELECT last_insert_rowid()` calls with `cursor.lastrowid` usage

### Patent-Eligible Architecture

The system incorporates a novel architecture for ERP exchange management, comprising:
-   **Exchange Dependency Graph Engine**: A DAG-based structure for O(1) node lookup and O(V+E) traversal, with hash-linked nodes and BFS dependency resolution.
-   **Deterministic Event Processing Engine**: Idempotent event processing with hash-chained events for tamper detection and event sourcing.
-   **AI Execution Path Modifier**: Computes risk vectors from historical event patterns to modify execution paths.
-   **Performance Instrumentation System**: Measures latency, cache hit ratios, and query reduction for performance reporting.
-   **Cryptographic Security Layer**: HMAC-SHA256 based access key generation, role-scoped access control, and tamper-evident audit trails.
-   **Exchange Orchestrator**: Unifies all engines, demonstrating a patentable method of AI-driven risk analysis, system behavior modification, cryptographic verification, and performance instrumentation.

### System Design Choices

-   **Inventory Management**: Real-time stock tracking and low stock alerts.
-   **Reporting System**: Various financial and operational reports.
-   **Accounting System**: Chart of Accounts, General Ledger, Manual Journal Entries.
-   **Time Clock Station**: Employee time tracking with skill-based task filtering.
-   **Labor Resources & Skillset Management**: Multi-skillset assignment for capacity planning.
-   **Service/Misc PO for Work Orders**: Create purchase orders for services linked to work orders.
-   **UOM Conversion System**: High-precision unit of measure conversion using Python Decimal.
-   **Part Intake System**: AI-powered web part capture for converting supplier catalog data into ERP products.
-   **Dual Exchange Workflow**: Automates Sales Order to Purchase Order exchange.
-   **Exchange PO Obligations Tracking**: Dashboard for tracking exchange obligations.
-   **Professional Shipping Document Generation System**: Generates versioned packing slips, Certificates of Conformance, and commercial invoices.
-   **Work Order Accordion Layout with Task-Level Material Requirements**: Redesigned work order page with collapsible sections and task-level material management.
-   **Marketing Presentation Generator with PDF Download**: AI-powered generator for professional PDF presentations.
-   **Repair Order (External Repair / MRO Services) Module**: Manages external repairs with a full lifecycle and cost variance tracking.
-   **Customer Portal with Work Order Quote Approval**: Token-based customer portal for viewing and approving work order quotes with audit trails.
-   **Master Routing (Work Order Template) Module**: Standardized process templates for work orders.
-   **Professional Sales Order Document Generation**: Print-ready Sales Order viewing with PDF download capability.
-   **Master Part Planning Report**: Comprehensive planning report for "Master Plan Part" products including inventory, exchange status, work orders, and predictive forecast.
-   **Sales Order Email Acknowledgement System**: Preview and email sales order acknowledgements with professional HTML templates.
-   **Inventory Document Upload System**: Attach documents directly to inventory line items with categorization and role-based access.
-   **Work Order Allocation to Sales Order Lines**: Link available work orders to sales order line items with visual distinctions.
-   **Work Order Reconciliation Module**: Formal cost reconciliation process for work orders with variance analysis and audit trails.
-   **Invoice Customer Email Portal**: Token-based secure customer portal for viewing invoices with email delivery.
-   **Supplier Portal for Interactive PO Management**: Token-based secure supplier portal for viewing and updating open purchase orders.
-   **FAA-Compliant Inventory Labels**: Generate printable PDF labels for inventory items complying with FAA regulations.
-   **Executive Sales Dashboard**: C-Suite strategic intelligence dashboard for MRO operations with KPIs and AI Copilot.
-   **Executive Procurement Dashboard**: Supply chain strategic intelligence dashboard for procurement operations with KPIs and AI Copilot.
-   **Leads Management System**: CRM-grade lead capture, evaluation, and conversion module with AI-powered sales engagement.
-   **Component Buyout Workflow**: Integrated workflow to create Component Buyout Purchase Orders directly from Work Orders.
-   **Core Due Days Tracking for Exchange Orders**: Automatic calculation of Expected Core Return Date based on Order Date + Core Due Days. Core Due Days field is required for Exchange Sales Orders (0-365 days). Expected Core Return Date is read-only and auto-calculated in real-time. Changes to Core Due Days are logged in the audit trail. Visible on create, edit, and view pages for Exchange type orders only.
-   **COREx NeuroIQ Executive Intelligence System**: AI-powered conversational interface with dual-mode interaction (voice and text). Features Web Speech API for voice input (speech-to-text), Speech Synthesis for voice responses (text-to-speech), real-time business context gathering, proactive insights panel, and executive role perspective indicators. Uses OpenAI GPT-4o via Replit AI Integrations.
-   **COREx Guide Transaction Assistant**: AI-powered proactive assistant embedded in transaction forms (Sales Orders, Purchase Orders, Work Orders, Inventory, Invoices, Quotes, Receiving). Features include:
    - Auto-initialization on create/edit pages with context-aware greeting
    - Field-level validation on blur with inline hint notifications
    - Required field detection with proactive reminders
    - Pre-submit transaction integrity checks
    - Conversational chat interface for guidance requests
    - Contextual quick-action suggestions based on transaction type
    - Backend API routes: `/api/corex-guide/assist`, `/api/corex-guide/validate-field`, `/api/corex-guide/transaction-check`

## External Dependencies

-   **Python Packages**: `Flask`, `Flask-Login`, `Werkzeug`, `ReportLab`, `Pandas`, `openpyxl`, `openai`, `psycopg2-binary`.
-   **Frontend Libraries**: Bootstrap 5.3.0, Bootstrap Icons 1.11.0, Chart.js 4.4.0.
-   **AI Integration**: OpenAI API (GPT-4o).
-   **Database**: SQLite (`mrp.db`) for development, PostgreSQL for production.