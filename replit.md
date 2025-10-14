## Overview

Dynamic.IQ.MRP is a comprehensive Manufacturing Resource Planning (MRP) system built with Flask, designed to optimize production processes, inventory, Bill of Materials (BOM), work orders, and purchase orders. It offers robust role-based access control, efficient material tracking, production planning, supplier management, and report generation. The system aims to provide a scalable, user-friendly solution to enhance operational efficiency and deliver critical business insights.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### UI/UX Decisions

The system features a responsive and modern user interface built with Bootstrap 5 and Bootstrap Icons, ensuring a consistent layout via a base template. It utilizes card-based and accordion-style displays, including an executive financial accounting dashboard with interactive Chart.js visualizations and color-coded KPI indicators.

### Technical Implementations

The backend is developed with Flask using Blueprints and a SQLite database (`mrp.db`). It features session-based authentication, comprehensive role-based access control (Admin, Planner, Production Staff, Procurement, Accountant), and an `MRPEngine` for core MRP logic like recursive BOM explosion and automatic sequential numbering. An Audit Trail System logs all CUD operations. The frontend uses Jinja2 for templating.

**Key Modules and Features:**
-   **Products & BOM**: Manages product data, multi-level BOMs with revision control, cost tracking, and interactive views.
-   **Inventory**: Tracks stock levels, supports serialized products with unique serial number tracking, and allows manual adjustments.
-   **Work Orders**: Manages production orders with disposition types (Manufacture, Repair, Overhaul, Teardown, Inspect), status tracking, cost allocation, and integrated task/labor planning. **Full CRUD Operations**: Create, view, edit (with restrictions on completed orders), and status updates with automatic material requirement recalculation when product changes. **WIP → Finished Goods Transfer**: Automatic GL posting when work orders complete, transferring accumulated costs (Material + Labor + Overhead) from WIP (1140) to Finished Goods Inventory (1150), with inventory quantity updates and product cost recalculation. **Work Order Quote System**: Generate, manage, and share professional quotes directly from work orders with automatic data population, customizable pricing, PDF generation with company branding, and status workflow (Draft → Submitted → Approved/Rejected → Converted). Features multi-line item support (Parts, Labor, Other), tax calculation, and comprehensive audit trails.
-   **Purchase Orders**: Supports multi-line procurement, supplier relationships, dynamic line item management, partial/full receiving, and secure edit capabilities with audit trails.
-   **Material Management**: Comprehensive systems for Receiving, Issuance, and Returns, all with automatic GL posting.
-   **Suppliers & UOM**: Manages supplier information and a centralized Unit of Measure master.
-   **Time Tracking**: Employee clock-in/out system with real-time tracking, cost calculation, and a dedicated mobile-friendly Clock Station with PIN authentication and security features.
-   **Sales Module**: Comprehensive sales order management including customer CRUD, various order types (Outright Sales, Exchanges, Managed Repair), advanced line management with workflow, tax calculation, inventory integration (including serialized products), a 5-state order workflow, and robust validation (stock, pricing, discounts, credit limits). Includes Core Due Tracking for exchanges.
-   **Shipping & Receiving Module**: Manages shipment lifecycle for Sales and Work Orders with multi-line support, tracking information, and status workflows. Includes package details and inventory integration.
-   **Invoice Management Module**: Comprehensive billing and A/R system generating invoices from Sales/Work Orders, supporting a full invoice lifecycle (Draft → Approved → Posted → Paid), multi-line items, and an Invoice Dashboard with advanced filtering and KPIs. **Automatic Revenue Recognition**: When invoices are Posted, the system automatically creates GL entries (DR: Accounts Receivable 1120, CR: Sales Revenue 4100) ensuring accurate financial reporting.

### System Design Choices

-   **Inventory Management**: Real-time stock level tracking, low stock alerts, and automatic updates.
-   **Reporting System**: Provides various reports including inventory valuation, work order cost analysis, material requirements, material usage, and active labor.
-   **Accounting System**: Features a Chart of Accounts (COA), General Ledger (GL), Manual Journal Entries, and automatic GL posting for inventory transactions. It includes financial reports (Trial Balance, Balance Sheet, Income Statement) and an Accounts Payable (A/P) module with automated vendor invoice creation, payment tracking with GL posting, aging reports, and an Executive Accounting Dashboard providing real-time financial KPIs and interactive visualizations. **Manufacturing Cost Flow**: Automated GL entries for Material Receiving (DR: Inventory, CR: A/P), Work Order Completion (DR: Finished Goods, CR: WIP), and A/P Payments (DR: A/P, CR: Cash). Access is role-based, primarily for Admin and Accountant roles.

## External Dependencies

-   **Python Packages**: `Flask`, `Flask-Login`, `Werkzeug`, `ReportLab`, `sqlite3`.
-   **Frontend Libraries**: Bootstrap 5.3.0, Bootstrap Icons 1.11.0, Chart.js.
-   **Database**: SQLite (`mrp.db`).
-   **Environment Variables**: `SESSION_SECRET`.