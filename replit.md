# Dynamic.IQ.MRP

## Overview

Dynamic.IQ.MRP is a comprehensive Manufacturing Resource Planning (MRP) system built with Flask. It is designed to streamline production processes, inventory management, Bill of Materials (BOM), work orders, and purchase orders. The system supports multiple user roles with robust role-based access control, enabling efficient material tracking, production planning, supplier management, and report generation. Its core purpose is to provide a scalable, user-friendly solution for manufacturing resource planning, aiming to enhance operational efficiency and provide critical business insights.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### UI/UX Decisions

The system utilizes Bootstrap 5 for a responsive and modern user interface, complemented by Bootstrap Icons. A consistent layout is maintained through a base template (`base.html`) with dynamic navigation based on user roles. Data is presented using a card-based UI, and an accordion-style grouped display is used for BOMs.

### Technical Implementations

**Backend**: Developed with Flask, using a modular design with Blueprints. It employs a SQLite database (`mrp.db`) for data storage, managed through a `Database` class with raw SQL. Session-based authentication with Flask sessions and Werkzeug secures user access, while a comprehensive role-based access control system (Admin, Planner, Production Staff, Procurement, Accountant) with granular permissions governs functionality. The `MRPEngine` class handles core MRP logic, including recursive BOM explosion and automatic sequential numbering for key entities (Work Orders, Purchase Orders, Receiving Transactions, Inventory IDs). An Audit Trail System automatically logs all create, update, and delete operations with detailed change tracking.

**Frontend**: Leverages Jinja2 for server-side templating.

**Data Management**:
- **Products**: Manages product codes, types (Raw Material/Component/Finished Good), units, and costs, with CSV import/export.
- **Bill of Materials (BOM)**: Supports multi-level BOMs with hierarchy, revision control, cost tracking, an interactive tree view, and advanced filtering. Includes quick component addition and mass update functionalities.
- **Inventory**: Tracks quantity, reorder points, safety stock, condition, location, and reserved quantities. Features manual adjustments, auto-generated IDs, and CSV import/export.
- **Work Orders**: Manages production orders with status tracking, cost allocation, and dynamic material requirements based on BOMs. Includes integrated task lists with labor planning and cost tracking.
- **Purchase Orders**: Supports multi-line procurement with supplier relationships, dynamic line item management, UOM selection, partial/full receiving, and professional print functionality.
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

### System Design Choices

- **Inventory Management**: Real-time stock level tracking, low stock alerts, and automatic updates.
- **Reporting System**: Includes inventory valuation, work order cost analysis, material requirements reports (with procurement capability), material usage, and active labor reports.
- **Accounting System**:
    - **Chart of Accounts (COA)**: Hierarchical structure with standard accounts.
    - **General Ledger (GL)**: View all posted journal entries with filters.
    - **Manual Journal Entries**: Create and manage journal entries with balance validation and post/unpost workflow.
    - **Automatic GL Posting**: Automates journal entry creation and posting for all inventory transactions.
    - **Financial Reports**: Trial Balance, Balance Sheet, Income Statement (P&L).
    - **Role-based Access**: Limited to Admin and Accountant roles.

## External Dependencies

-   **Python Packages**: `Flask`, `Flask-Login`, `Werkzeug`, `sqlite3` (standard library).
-   **Frontend Libraries**: Bootstrap 5.3.0, Bootstrap Icons 1.11.0.
-   **Database**: SQLite file-based database (`mrp.db`).
-   **Environment Variables**: `SESSION_SECRET` for Flask session encryption.