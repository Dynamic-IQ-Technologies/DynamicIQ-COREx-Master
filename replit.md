# Dynamic.IQ.MRP

## Overview

Dynamic.IQ.MRP is a comprehensive Manufacturing Resource Planning (MRP) system built with Flask. It is designed to streamline production processes, inventory management, Bill of Materials (BOM), work orders, and purchase orders. The system supports multiple user roles with robust role-based access control, enabling efficient material tracking, production planning, supplier management, and report generation. Its core purpose is to provide a scalable, user-friendly solution for manufacturing resource planning, aiming to enhance operational efficiency and provide critical business insights.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### UI/UX Decisions

The system utilizes Bootstrap 5 for a responsive and modern user interface, complemented by Bootstrap Icons. A consistent layout is maintained through a base template (`base.html`) with dynamic navigation based on user roles. Data is presented using a card-based UI, and an accordion-style grouped display is used for BOMs. The system features an executive financial accounting dashboard with interactive Chart.js visualizations and color-coded KPI indicators.

### Technical Implementations

**Backend**: Developed with Flask, using a modular design with Blueprints. It employs a SQLite database (`mrp.db`) for data storage, managed through a `Database` class with raw SQL. Session-based authentication with Flask sessions and Werkzeug secures user access, while a comprehensive role-based access control system (Admin, Planner, Production Staff, Procurement, Accountant) with granular permissions governs functionality. The `MRPEngine` class handles core MRP logic, including recursive BOM explosion and automatic sequential numbering for key entities (Work Orders, Purchase Orders, Receiving Transactions, Inventory IDs). An Audit Trail System automatically logs all create, update, and delete operations with detailed change tracking.

**Frontend**: Leverages Jinja2 for server-side templating.

**Data Management**:
- **Products**: Manages product codes, types (Raw Material/Component/Finished Good), units, and costs, with CSV import/export.
- **Bill of Materials (BOM)**: Supports multi-level BOMs with hierarchy, revision control, cost tracking, an interactive tree view, and advanced filtering. Includes quick component addition and mass update functionalities.
- **Inventory**: Tracks quantity, reorder points, safety stock, condition, location, and reserved quantities. Features manual adjustments, auto-generated IDs, and CSV import/export. **Serialized Product Support**: Includes checkbox to mark products as serialized, unique serial number tracking with validation, filtering by serialized/non-serialized products, serial number search functionality, and serial number display across all inventory views and reports.
- **Work Orders**: Manages production orders with status tracking, cost allocation, and dynamic material requirements based on BOMs. Includes integrated task lists with labor planning and cost tracking.
- **Purchase Orders**: Supports multi-line procurement with supplier relationships, dynamic line item management, UOM selection, partial/full receiving, and professional print functionality. Features **secure edit capability** with receipt history preservation: uses UPDATE logic with validated line IDs, prevents product changes on received lines, enforces ordered quantity >= received quantity, blocks deletion of received lines, and maintains audit trail integrity.
- **Material Receiving**: Comprehensive system for receiving against purchase orders, including partial receipts, condition tracking, automatic inventory updates, and **automatic GL posting**.
- **Material Issuance**: Manages issuing materials from inventory to work orders, with quantity validation, cost tracking, automatic inventory deduction, and **batch issuance** capability. Features **automatic GL posting**.
- **Material Returns**: Handles returns of materials from work orders to inventory, reversing costs and replenishing stock.
- **Inventory Adjustments**: Allows manual adjustments with reason tracking, cost impact calculation, and an audit trail. Features **automatic GL posting**.
- **Suppliers**: Manages vendor contact information with CSV import/export.
- **Unit of Measure (UOM) Master**: Centralized system for managing standard units, integrated with Purchase Orders.
- **Company Settings**: Configurable business information for document generation.
- **Work Order Tasks & Labor Planning**: Manages tasks, labor resources, time tracking, and cost calculations for work orders.
- **Time Tracking**: Employee clock-in/clock-out system with real-time tracking, cost calculation, and history views.
- **Active Labor Report**: Real-time report on clocked-in employees, accessible to Admin and Planner roles.
- **Sales Module**: Comprehensive sales order management system supporting multiple transaction types:
    - **Customer Management**: Full CRUD operations for customer records with contact information, billing/shipping addresses, payment terms, credit limits, and tax-exempt status. Features CSV import/export capability.
    - **Sales Order Types**: Supports Outright Sales, Exchanges, and Managed Repair transactions with type-specific fields (core charges, repair charges, expected return dates, service notes).
    - **Enhanced Line Management**: Advanced line-level transaction support:
        - **Line Types**: Outright Sale, Exchange, and Managed Repair with dynamic field visibility based on type
        - **Line Status Workflow**: Draft, Confirmed, Shipped, Closed tracking per line item
        - **Exchange Lines**: Core charge, core due days, expected core condition, core/stock disposition tracking. Automatic core_due_tracking record creation with calculated due dates.
        - **Managed Repair Lines**: Quoted TAT, Repair NTE (Not To Exceed), vendor repair source, repair status, return-to address management
        - **Audit Trail**: Created_by, modified_by, created_at, modified_at tracking on all line items
        - **Dynamic UI**: JavaScript-driven conditional field display based on line type selection
    - **Tax Calculation**: Configurable tax rate with automatic calculation and persistence across order modifications. Tax computed on subtotal including line-level core charges and header core/repair charges.
    - **Inventory Integration**: Automatic inventory validation and deduction during order fulfillment. Prevents overselling with real-time availability checks. Supports serialized product tracking with serial number capture per line item.
    - **Order Workflow**: Status tracking (Draft → Pending → Shipped → Completed) with role-based actions. Fulfillment process validates inventory, deducts quantities, and updates order status automatically.
    - **Payment Tracking**: Infrastructure for deposits, partial payments, and balance due tracking using existing payments table with flexible reference system.
    - **Core Due Tracking**: Dedicated table (core_due_tracking) for monitoring expected core returns on Exchange transactions with disposition management and refund processing.

### System Design Choices

- **Inventory Management**: Real-time stock level tracking, low stock alerts, and automatic updates.
- **Reporting System**: Includes inventory valuation, work order cost analysis, material requirements reports (with procurement capability), material usage, and active labor reports.
- **Accounting System**:
    - **Chart of Accounts (COA)**: Hierarchical structure with standard accounts.
    - **General Ledger (GL)**: View all posted journal entries with filters.
    - **Manual Journal Entries**: Create and manage journal entries with balance validation and post/unpost workflow.
    - **Automatic GL Posting**: Automates journal entry creation and posting for all inventory transactions.
    - **Financial Reports**: Trial Balance, Balance Sheet, Income Statement (P&L).
    - **Accounts Payable (A/P)**: Automated vendor invoice creation upon material receiving. Auto-generates unique AP numbers (AP-0000001), calculates payment due dates based on supplier terms (default Net 30), creates GL entries (DR: Inventory, CR: AP), and prevents orphaned records with GL validation. Features A/P dashboard with aging reports (Current, 1-30, 31-60, 61-90, 90+ days), top vendor analysis, status management, and CSV export. Integrated audit trail tracks all A/P changes. Each receiving transaction creates its own A/P record for proper accounting of partial deliveries.
    - **Executive Accounting Dashboard**: Real-time financial performance dashboard providing leadership with comprehensive visibility into organizational finances. Features KPIs (Revenue, Expenses, Profit Margin, A/P, Cash on Hand, Net Income, Inventory Value), interactive Chart.js visualizations (Revenue vs Expense Trend, A/P Aging, Top Vendors by Spend, Top Work Orders by Cost), period filtering (MTD/QTD/YTD), vendor filtering, drill-down quick links to detailed reports, and CSV export functionality. Color-coded indicators highlight positive trends (green), declines (red), and warnings (yellow).
    - **Role-based Access**: Limited to Admin and Accountant roles (with Procurement having view-only access to A/P).

## External Dependencies

-   **Python Packages**: `Flask`, `Flask-Login`, `Werkzeug`, `sqlite3` (standard library).
-   **Frontend Libraries**: Bootstrap 5.3.0, Bootstrap Icons 1.11.0.
-   **Database**: SQLite file-based database (`mrp.db`).
-   **Environment Variables**: `SESSION_SECRET` for Flask session encryption.