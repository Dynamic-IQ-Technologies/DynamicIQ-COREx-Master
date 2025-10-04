# Dynamic.IQ.MRP

## Overview

This is a fully functional Manufacturing Resource Planning (MRP) system built with Flask that manages production processes, inventory, bill of materials (BOM), work orders, and purchase orders. The system supports multiple user roles (Admin, Planner, Production Staff, Procurement) with secure role-based access control, enabling organizations to track materials, plan production, manage suppliers, and generate comprehensive reports.

## Recent Changes

**October 4, 2025**: Complete MRP application implemented
- Created comprehensive database schema with 8 tables (users, products, BOMs, suppliers, inventory, work orders, purchase orders, material requirements)
- Implemented full authentication system with secure role-based access control
- Built CRUD operations for all entities with proper authorization checks
- Created MRP calculation engine with recursive multi-level BOM explosion logic
- Implemented cost tracking for material, labor, and overhead costs
- Built complete web interface with 20+ templates for all features
- Added reporting system for inventory valuation, work order costs, and material usage
- Fixed security vulnerability: new user registration now defaults to "Production Staff" role to prevent privilege escalation

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Backend Architecture

**Framework**: Flask web application using Python

**Database Layer**: 
- SQLite database (`mrp.db`) with raw SQL queries
- Database abstraction through a `Database` class that manages connections
- Schema includes users, products, BOMs, inventory, work orders, purchase orders, and suppliers
- Uses SQLite's row factory for dictionary-like result access

**Authentication & Authorization**:
- Session-based authentication using Flask sessions
- Password hashing with Werkzeug's security utilities
- Decorator-based access control (`@login_required`, `@role_required`)
- Four user roles: Admin, Planner, Production Staff, and Procurement
- Secure registration: new users are automatically assigned "Production Staff" role (admins must promote users to elevated roles)

**Application Structure**:
- Blueprint-based modular routing system
- Separate route modules for each major feature (auth, products, BOMs, suppliers, inventory, work orders, purchase orders, reports)
- Context processor injects current user into all templates
- Before-request hook initializes database on each request

**Business Logic**:
- `MRPEngine` class handles core MRP calculations
- BOM explosion algorithm recursively calculates material requirements
- Material requirements calculation based on work orders
- Supports scrap percentage calculations in BOM requirements

### Frontend Architecture

**Template Engine**: Jinja2 templates with template inheritance

**UI Framework**: Bootstrap 5 for responsive design

**Icons**: Bootstrap Icons

**Layout Pattern**:
- Base template (`base.html`) provides navigation and common layout
- Navigation menu dynamically shows/hides features based on user role
- Flash message system for user feedback
- Card-based UI components for data presentation

### Data Management

**Core Entities**:
- **Products**: Managed with code, type (Raw Material/Component/Finished Good), unit of measure, and cost
- **Bill of Materials (BOM)**: Parent-child relationships with quantity and scrap percentage
- **Inventory**: Tracks quantity, reorder points, and safety stock levels
- **Work Orders**: Production orders with status tracking, cost allocation (material/labor/overhead)
- **Purchase Orders**: Procurement tracking with supplier relationships
- **Suppliers**: Vendor management with contact information

**Inventory Management**:
- Real-time stock level tracking
- Low stock alerts based on reorder points
- Manual inventory adjustment capability
- Automatic inventory updates from work order processing

**Reporting System**:
- Inventory valuation reports
- Work order cost analysis
- Material usage tracking
- Purchase order suggestions based on stock levels

### Access Control & Permissions

**Role Hierarchy**:
- **Admin**: Full system access including delete operations
- **Planner**: Can manage products, BOMs, work orders, view reports
- **Production Staff**: Can create work orders, adjust inventory, view dashboards
- **Procurement**: Can manage suppliers and purchase orders

**Permission Model**:
- Route-level authorization using decorators
- Template-level conditional rendering based on user role
- Session storage of user role for quick access checks

## External Dependencies

**Python Packages**:
- `Flask`: Web framework (3.1.2)
- `Flask-Login`: User session management (0.6.3)
- `Werkzeug`: Password hashing and security utilities (3.1.3)
- `sqlite3`: Database (Python standard library)

**Frontend Libraries** (CDN-based):
- Bootstrap 5.3.0: UI framework
- Bootstrap Icons 1.11.0: Icon set

**Environment Variables**:
- `SESSION_SECRET`: Flask session encryption key (defaults to dev key if not set)

**Database**:
- SQLite file-based database (`mrp.db`)
- No external database server required
- Schema auto-initialization on application startup