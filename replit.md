# Dynamic.IQ.MRP

## Overview

Dynamic.IQ.MRP is a comprehensive Manufacturing Resource Planning (MRP) system built with Flask, designed to streamline production processes, inventory management, Bill of Materials (BOM), work orders, and purchase orders. It supports multiple user roles (Admin, Planner, Production Staff, Procurement) with robust role-based access control, enabling organizations to efficiently track materials, plan production, manage suppliers, and generate essential reports. The system aims to provide a robust, scalable, and user-friendly solution for manufacturing resource planning.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Backend Architecture

**Framework**: Flask web application using Python.

**Database Layer**: SQLite database (`mrp.db`) managed through a `Database` class, utilizing raw SQL queries. The schema includes tables for users, products, BOMs, inventory (enhanced with condition, warehouse/bin location, status tracking, reserved quantities), work orders, purchase orders (with received quantity tracking), suppliers, receiving transactions, material issues, material returns, inventory adjustments, user permissions, and company settings. A singleton pattern is enforced for company settings.

**Authentication & Authorization**: Session-based authentication with Flask sessions and Werkzeug for password hashing. Role-based access control is implemented using decorators (`@login_required`, `@role_required`) for four distinct roles: Admin, Planner, Production Staff, and Procurement. A granular permissions system allows fine-grained control beyond roles. New user registration defaults to "Production Staff" role.

**Application Structure**: Modular design using Flask Blueprints for features like authentication, products, BOMs, inventory, etc. A context processor injects the current user into templates, and a before-request hook initializes the database. Secure file upload handling is included.

**Business Logic**: The `MRPEngine` class manages core MRP calculations, including a recursive BOM explosion algorithm to determine material requirements, supporting scrap percentage calculations. Automatic sequential numbering is implemented for Work Orders (WO-XXXXXX), Purchase Orders (PO-XXXXXX), Receiving Transactions (RCV-XXXXXX), and Inventory IDs (INV-XXXXXX). Material receiving automatically updates inventory levels and purchase order statuses.

### Frontend Architecture

**Template Engine**: Jinja2 for server-side templating with inheritance.

**UI Framework**: Bootstrap 5 for a responsive design, complemented by Bootstrap Icons.

**Layout Pattern**: A base template (`base.html`) provides consistent navigation and layout, with dynamic menu visibility based on user roles. Flash messages provide user feedback, and data is presented using card-based UI components.

### Data Management

**Core Entities**:
- **Products**: Managed with code, type (Raw Material/Component/Finished Good), unit, and cost. Supports CSV import/export.
- **Bill of Materials (BOM)**: Industry-standard multi-level system with hierarchy support, auto-generated find numbers, categories (e.g., Electrical, Mechanical), revision control, reference designators, document links, and cost tracking. Features an accordion-style grouped display where each parent product (assembly) appears once with all child components listed in a nested table underneath. Includes interactive tree view, advanced filtering (supporting mixed category/status children), clone functionality, mass updates, and roll-up summaries. Supports CSV import/export with validation. Quick "Add Line" workflow allows rapid component addition with green ➕ button that pre-selects parent product.
- **Inventory**: Enhanced tracking with quantity, reorder points, safety stock, condition (New/Serviceable/Overhauled/Repaired), warehouse/bin location, status (Available/Reserved/Out of Stock), and reserved quantities for work orders. Includes manual creation, auto-generated Inventory IDs, and CSV import/export for updates.
- **Work Orders**: Production orders with status tracking, cost allocation, and manual material requirements management. Supports auto-generated WO numbers, automatic BOM-based material calculations, and the ability to add, edit, or delete material requirements directly from the work order view. Material requirements automatically integrate with inventory to show available quantities, issued quantities, and shortages. Status automatically updates when materials are issued or returned. Work order view includes integrated task list showing all associated tasks with status, planned/actual hours, labor costs, and assigned labor counts.
- **Purchase Orders**: Multi-line procurement tracking with supplier relationships, auto-generated PO numbers, dynamic line item management (add/remove products with JavaScript), UOM (Unit of Measure) selection with decimal quantity support per line, detailed individual views, partial/full receiving support with shipment tracking, and professional print/download functionality with company branding. Integrates with UOM Master for standardized unit management across all PO operations. Features automatic migration of legacy single-line POs to multi-line structure with full backward compatibility. Supports adding multiple products to a single purchase order with individual quantity, price, and UOM tracking per line item.
- **Material Receiving**: Complete receiving system against purchase orders with auto-generated receipt numbers (RCV-XXXXXX), partial receipt support, condition tracking (New/Serviceable/Overhauled/Repaired), warehouse location assignment, packing slip and shipment tracking, automatic inventory updates, and PO status management. Validates received quantities and ensures product integrity. **Automatic GL Posting**: Creates journal entries (Debit: Inventory 1130, Credit: Accounts Payable 2110) for every receipt.
- **Material Issuance**: Issue materials from inventory to work orders with auto-generated issue numbers (ISS-XXXXXX), quantity validation, cost tracking (unit/total), automatic inventory deduction, and work order material cost accumulation. Prevents issuing more than available inventory. Automatically updates material requirement status (Satisfied/Shortage) after issuance. Features **batch issuance** capability with multi-select checkboxes allowing selection of multiple materials for simultaneous issuance to a single work order. Batch operations use SQLite SAVEPOINT-based transaction control for per-material atomicity, ensuring successful materials are committed even if others fail. Provides detailed feedback with three-tier messaging (full success, partial success, complete failure) and individual error reporting. **Automatic GL Posting**: Creates journal entries (Debit: WIP 1140, Credit: Inventory 1130) for every issuance.
- **Material Returns**: Return unused or excess materials from work orders back to inventory with auto-generated return numbers (RET-XXXXXX), validates against issued quantities, tracks condition and reasons, automatically reverses work order material costs, and replenishes inventory. Full cost tracking ensures accurate financial records. Automatically updates material requirement status after return.
- **Inventory Adjustments**: Manual inventory quantity adjustments with auto-generated adjustment numbers (ADJ-XXXXXX), reason tracking (physical count, damage, scrap, etc.), adjustment type (Increase/Decrease), cost impact calculation, full audit trail with before/after quantities. Includes validation warnings for zero-cost products to maintain data integrity. **Automatic GL Posting**: Creates journal entries for increases (Debit: Inventory 1130, Credit: Other Income 4300) and decreases (Debit: Material Cost 5100, Credit: Inventory 1130).
- **Suppliers**: Vendor management with contact information, supporting CSV import/export.
- **Unit of Measure (UOM) Master**: Centralized UOM management system with 30+ pre-seeded standard units (EA, PC, KG, LB, M, FT, L, GAL, etc.). Supports CRUD operations, activation/deactivation, and future conversion factor management. Admin-only access. Integrates with Purchase Orders for standardized unit selection.
- **Company Settings**: Configurable business information (singleton pattern) including general, contact, tax/regulatory details, and logo upload for professional document generation. Admin-only editing.
- **Work Order Tasks & Labor Planning**: Complete task management system with labor resource tracking, time tracking, and planned vs actual analysis. Features task creation (TASK-XXXXXX), labor resource management (EMP-XXXXXX), labor issuance (LBR-XXXXXX), automatic cost calculations cascading from labor → task → work order, and task summaries integrated into work order views.
- **Time Tracking (Clock In/Clock Out)**: Employee time tracking system with secure user-to-employee linking via user_id foreign key. Features real-time clock in/clock out to work orders and tasks, auto-generated entry numbers (CLK-XXXXXX), elapsed time calculation, hourly rate tracking, automatic labor cost calculation, work notes, and comprehensive history views. Includes ownership verification to prevent unauthorized access to other users' time entries. Supervisor view available for Admin/Planner roles to monitor all employee time tracking. Labor resources must be linked to user accounts for time tracking access.
- **Active Labor Report**: Real-time report showing all currently clocked-in employees with live updates. Displays employee details, work orders, tasks, elapsed time, hourly rates, and estimated labor costs. Features summary statistics including total employees clocked in, combined hourly rate, and estimated current labor cost. Auto-refreshing elapsed time and cost calculations. Print-friendly format. Accessible to Admin and Planner roles only.

**Inventory Management**: Features real-time stock level tracking, low stock alerts, manual adjustments, and automatic updates from work order processing.

**Reporting System**: Provides inventory valuation, work order cost analysis, material requirements reports (with summary statistics, CSV export, and direct procurement capability), material usage tracking, and active labor report showing real-time clocked-in employees with labor cost tracking.

**Accounting System**: Comprehensive double-entry accounting module with:
- **Chart of Accounts (COA)**: Hierarchical account structure with parent-child relationships, supporting Assets, Liabilities, Equity, Revenue, and Expense accounts. Auto-seeded with standard accounts (30+ accounts) organized by type. Full CRUD operations with account activation/deactivation.
- **General Ledger (GL)**: Complete GL viewing with date and account filters, showing all posted journal entries with debit/credit columns and transaction sources.
- **Manual Journal Entries**: Create journal entries with dynamic line items, auto-generated entry numbers (JE-XXXXXX), real-time debit/credit balance validation (must balance within $0.01), minimum 2 lines required. Post/Unpost workflow with audit trail tracking created by/posted by users and timestamps. Draft and Posted status management.
- **Automatic GL Posting**: GLAutoPost helper class automatically creates and posts journal entries for all inventory transactions (receiving, issuance, adjustments). Entries are automatically balanced, linked to source transactions via reference_type/reference_id, and immediately posted. Error handling ensures GL failures don't break inventory operations. All inventory activity is automatically reflected in financial reports.
- **Financial Reports**:
  - **Trial Balance**: Shows account balances with debit/credit totals, balance verification, filterable by date
  - **Balance Sheet**: Assets, Liabilities, and Equity with balance check (Assets = Liabilities + Equity)
  - **Income Statement (P&L)**: Revenue and Expense breakdown with Net Income calculation, filterable by date range
- **Role-based Access**: Admin and Accountant roles only, with Admin having exclusive unpost privileges

### Access Control & Permissions

**Role Hierarchy**: Defines access levels for Admin (full access), Planner (manages products, BOMs, work orders, reports), Production Staff (creates work orders, adjusts inventory, views dashboards), Procurement (manages suppliers and purchase orders), and Accountant (manages accounting, financial reports, journal entries - with Admin having exclusive unpost privileges).

**Permission Model**: Route-level authorization via decorators and template-level conditional rendering based on user roles and granular permissions.

## External Dependencies

**Python Packages**:
- `Flask`: Web framework.
- `Flask-Login`: User session management.
- `Werkzeug`: Password hashing and security utilities.
- `sqlite3`: Database (Python standard library).

**Frontend Libraries**:
- Bootstrap 5.3.0: UI framework.
- Bootstrap Icons 1.11.0: Icon set.

**Environment Variables**:
- `SESSION_SECRET`: Flask session encryption key.

**Database**:
- SQLite file-based database (`mrp.db`).