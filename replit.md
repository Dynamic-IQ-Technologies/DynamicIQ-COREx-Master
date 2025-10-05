# Dynamic.IQ.MRP

## Overview

Dynamic.IQ.MRP is a comprehensive Manufacturing Resource Planning (MRP) system built with Flask, designed to streamline production processes, inventory management, Bill of Materials (BOM), work orders, and purchase orders. It supports multiple user roles (Admin, Planner, Production Staff, Procurement) with robust role-based access control, enabling organizations to efficiently track materials, plan production, manage suppliers, and generate essential reports. The system aims to provide a robust, scalable, and user-friendly solution for manufacturing resource planning.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Backend Architecture

**Framework**: Flask web application using Python.

**Database Layer**: SQLite database (`mrp.db`) managed through a `Database` class, utilizing raw SQL queries. The schema includes tables for users, products, BOMs, inventory, work orders, purchase orders, suppliers, user permissions, and company settings. A singleton pattern is enforced for company settings.

**Authentication & Authorization**: Session-based authentication with Flask sessions and Werkzeug for password hashing. Role-based access control is implemented using decorators (`@login_required`, `@role_required`) for four distinct roles: Admin, Planner, Production Staff, and Procurement. A granular permissions system allows fine-grained control beyond roles. New user registration defaults to "Production Staff" role.

**Application Structure**: Modular design using Flask Blueprints for features like authentication, products, BOMs, inventory, etc. A context processor injects the current user into templates, and a before-request hook initializes the database. Secure file upload handling is included.

**Business Logic**: The `MRPEngine` class manages core MRP calculations, including a recursive BOM explosion algorithm to determine material requirements, supporting scrap percentage calculations. Automatic sequential numbering is implemented for Work Orders (WO-XXXXXX), Purchase Orders (PO-XXXXXX), and Inventory IDs (INV-XXXXXX).

### Frontend Architecture

**Template Engine**: Jinja2 for server-side templating with inheritance.

**UI Framework**: Bootstrap 5 for a responsive design, complemented by Bootstrap Icons.

**Layout Pattern**: A base template (`base.html`) provides consistent navigation and layout, with dynamic menu visibility based on user roles. Flash messages provide user feedback, and data is presented using card-based UI components.

### Data Management

**Core Entities**:
- **Products**: Managed with code, type (Raw Material/Component/Finished Good), unit, and cost. Supports CSV import/export.
- **Bill of Materials (BOM)**: Industry-standard multi-level system with hierarchy support, auto-generated find numbers, categories (e.g., Electrical, Mechanical), revision control, reference designators, document links, and cost tracking. Features an interactive tree view, advanced filtering, clone functionality, mass updates, and roll-up summaries. Supports CSV import/export with validation.
- **Inventory**: Tracks quantity, reorder points, and safety stock. Includes manual creation, auto-generated Inventory IDs, and CSV import/export for updates.
- **Work Orders**: Production orders with status tracking and cost allocation. Auto-generated WO numbers.
- **Purchase Orders**: Procurement tracking with supplier relationships, auto-generated PO numbers, detailed individual views, direct receiving, and professional print/download functionality with company branding.
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