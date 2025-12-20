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

## External Dependencies

-   **Python Packages**: `Flask`, `Flask-Login`, `Werkzeug`, `ReportLab`, `Pandas`, `openpyxl`, `openai`, `sqlite3`.
-   **Frontend Libraries**: Bootstrap 5.3.0, Bootstrap Icons 1.11.0, Chart.js 4.4.0.
-   **AI Integration**: OpenAI API (GPT-4o).
-   **Database**: SQLite (`mrp.db`).
-   **Environment Variables**: `SESSION_SECRET`.