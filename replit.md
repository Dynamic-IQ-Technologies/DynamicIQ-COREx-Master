## Overview

Dynamic.IQ.MRP is a comprehensive Flask-based Manufacturing Resource Planning (MRP) system designed to optimize production processes, inventory, Bill of Materials (BOM), work orders, and purchase orders. It offers robust role-based access control, efficient material tracking, production planning, supplier management, and report generation, aiming to provide a scalable, user-friendly solution to enhance operational efficiency and deliver critical business insights.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### UI/UX Decisions

The system features a responsive and modern user interface built with Bootstrap 5 and Bootstrap Icons, using a base template for consistency. It incorporates card-based and accordion-style displays, including an executive financial accounting dashboard with interactive Chart.js visualizations and color-coded KPI indicators.

### Technical Implementations

The backend is developed with Flask using Blueprints and an SQLite database (`mrp.db`). It implements session-based authentication and comprehensive role-based access control (Admin, Planner, Production Staff, Procurement, Accountant). The `MRPEngine` handles core MRP logic like recursive BOM explosion and automatic sequential numbering. An Audit Trail System logs all CUD operations. The frontend uses Jinja2 for templating.

**Key Modules and Features:**
-   **Products & BOM**: Manages product data, multi-level BOMs with revision control, and interactive views. Product costs are automatically updated during receiving based on actual purchase prices. Includes a comprehensive product-level UOM Conversion Management system with versioning, CRUD operations, multiple version support, effective dates, dynamic conversion previews, and an audit trail for changes.
-   **Inventory**: Tracks stock levels, supports serialized products, allows manual adjustments, displays unit cost and total inventory value, and handles NULL costs.
-   **Work Orders**: Manages production orders with disposition types, customer association, status tracking (14 predefined stages), cost allocation, and integrated task/labor planning. Supports full CRUD operations. Features automatic GL posting for WIP to Finished Goods transfer upon completion. Includes a Work Order Quote System for generating and managing professional quotes with customizable pricing, PDF generation, and status workflows. Implements an advanced Material Allocation and Issuance Workflow with two-phase control, including allocation, issuance, returns, deallocation, and automatic GL posting, with role-based access controls and audit logging. **Advanced Filtering and Sorting**: Work order list page features a collapsible filter panel with search box (WO number, product, customer), dropdown filters (status, disposition, priority, operational status, customer), date range filtering (planned start date), multi-column sorting with ascending/descending order, active filter badges display, clear all filters functionality, results count display, and empty state messaging.
-   **Purchase Orders**: Supports multi-line procurement, supplier relationships, dynamic line item management, and partial/full receiving. Features an advanced UOM conversion system that integrates product-specific conversion factors for accurate inventory tracking and display, with API endpoints for dynamic UOM calculations. Product costs are automatically updated from PO lines during receiving.
-   **Sales Module**: Comprehensive sales order management including customer CRUD, various order types, advanced line management, tax calculation, inventory integration (including serialized products), a 5-state order workflow, and robust validation. Features line-level inventory allocation with serial number tracking, automatic serial number population, duplicate serial number validation, and line-level release to shipping for partial shipments.
-   **Shipping & Receiving Module**: Manages shipment lifecycle for Sales and Work Orders with multi-line support, tracking information, and status workflows. Includes a pending shipments workflow with controlled release and status updates.
-   **Invoice Management Module**: Comprehensive billing and A/R system generating invoices from Sales/Work Orders, supporting a full invoice lifecycle, multi-line items, and an Invoice Dashboard. Features automatic revenue recognition with GL entries upon invoice posting.
-   **Service Management Module**: Comprehensive service work order system for standard Service and NDT work orders. Features customer association, equipment tracking, multi-line labor tracking with automatic cost calculation (including conditional service rates), materials allocation with optional inventory deduction, expense tracking, status workflow, and approval processes. Supports dynamic part/material selection with serial number tracking during creation and an integrated invoice generation workflow with automatic line creation and numbering.

### System Design Choices

-   **Inventory Management**: Real-time stock level tracking, low stock alerts, and automatic updates.
-   **Reporting System**: Provides various reports including inventory valuation, work order cost analysis, material requirements (including both production and service work order materials with net inventory tracking), material usage, and active labor.
-   **Accounting System**: Features a Chart of Accounts (COA), General Ledger (GL), Manual Journal Entries, and automatic GL posting for inventory and A/P transactions. Includes financial reports (Trial Balance, Balance Sheet, Income Statement) and an Accounts Payable (A/P) module with automated vendor invoice creation, payment tracking, aging reports, and an Executive Accounting Dashboard. Provides detailed GL account drill-down views with filtering, sorting, running balance calculation, and drill-down links to source transactions. Manufacturing cost flow includes automated GL entries for material receiving, work order completion, and A/P payments. Includes accounting preferences to control automated GL posting behavior for invoice approvals.
-   **Time Clock Station**: Dedicated employee time tracking system with secure PIN-based authentication, clock in/out functionality, work order and task assignment tracking, hours calculation, and reporting. Features work order selection during clock-in with dynamic task dropdown population, comprehensive input validation, and integration with work orders and tasks for accurate time-to-job tracking. Includes recent activity history showing work order and task information for each punch.

## External Dependencies

-   **Python Packages**: `Flask`, `Flask-Login`, `Werkzeug`, `ReportLab`, `sqlite3`.
-   **Frontend Libraries**: Bootstrap 5.3.0, Bootstrap Icons 1.11.0, Chart.js.
-   **Database**: SQLite (`mrp.db`).
-   **Environment Variables**: `SESSION_SECRET`.