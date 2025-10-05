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
- **Work Orders**: Production orders with status tracking, cost allocation, and manual material requirements management. Supports auto-generated WO numbers, automatic BOM-based material calculations, and the ability to add, edit, or delete material requirements directly from the work order view. Material requirements automatically integrate with inventory to show available quantities, issued quantities, and shortages. Status automatically updates when materials are issued or returned.
- **Purchase Orders**: Procurement tracking with supplier relationships, auto-generated PO numbers, detailed individual views, partial/full receiving support with shipment tracking, and professional print/download functionality with company branding.
- **Material Receiving**: Complete receiving system against purchase orders with auto-generated receipt numbers (RCV-XXXXXX), partial receipt support, condition tracking (New/Serviceable/Overhauled/Repaired), warehouse location assignment, packing slip and shipment tracking, automatic inventory updates, and PO status management. Validates received quantities and ensures product integrity.
- **Material Issuance**: Issue materials from inventory to work orders with auto-generated issue numbers (ISS-XXXXXX), quantity validation, cost tracking (unit/total), automatic inventory deduction, and work order material cost accumulation. Prevents issuing more than available inventory. Automatically updates material requirement status (Satisfied/Shortage) after issuance.
- **Material Returns**: Return unused or excess materials from work orders back to inventory with auto-generated return numbers (RET-XXXXXX), validates against issued quantities, tracks condition and reasons, automatically reverses work order material costs, and replenishes inventory. Full cost tracking ensures accurate financial records. Automatically updates material requirement status after return.
- **Inventory Adjustments**: Manual inventory quantity adjustments with auto-generated adjustment numbers (ADJ-XXXXXX), reason tracking (physical count, damage, scrap, etc.), adjustment type (Increase/Decrease), cost impact calculation, full audit trail with before/after quantities. Includes validation warnings for zero-cost products to maintain data integrity.
- **Suppliers**: Vendor management with contact information, supporting CSV import/export.
- **Company Settings**: Configurable business information (singleton pattern) including general, contact, tax/regulatory details, and logo upload for professional document generation. Admin-only editing.

**Inventory Management**: Features real-time stock level tracking, low stock alerts, manual adjustments, and automatic updates from work order processing.

**Reporting System**: Provides inventory valuation, work order cost analysis, material requirements reports (with summary statistics, CSV export, and direct procurement capability), and material usage tracking.

### Access Control & Permissions

**Role Hierarchy**: Defines access levels for Admin (full access), Planner (manages products, BOMs, work orders, reports), Production Staff (creates work orders, adjusts inventory, views dashboards), and Procurement (manages suppliers and purchase orders).

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