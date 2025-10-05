# Dynamic.IQ.MRP

## Overview

This is a fully functional Manufacturing Resource Planning (MRP) system built with Flask that manages production processes, inventory, bill of materials (BOM), work orders, and purchase orders. The system supports multiple user roles (Admin, Planner, Production Staff, Procurement) with secure role-based access control, enabling organizations to track materials, plan production, manage suppliers, and generate comprehensive reports.

## Recent Changes

**October 5, 2025**: User Management, Permissions, Import/Export, Auto-numbering, Material Requirements Report with Direct Procurement, Individual PO View with Print/Download, and Industry-Standard BOM System features added
- Created admin-only User Management interface to view all users and change roles
- Implemented granular Permissions Management system with user_permissions table
- Added permission categories: Products, BOM, Inventory, Work Orders, Purchase Orders, Suppliers, Reports, Users
- Created User.get_permissions(), User.set_permission(), and User.get_all_with_permissions() model methods
- Built permissions management UI with category-based checkboxes for fine-grained access control
- Updated navigation menu with User Management and Permissions links (Admin only)
- Added comprehensive Import/Export functionality with CSV support for BOMs, Products, Inventory, and Suppliers
  - **BOMs**: Template download, export all BOMs, import with validation (Admin/Planner only)
  - **Products**: Template download, export all products, import new/update existing with auto inventory creation (Admin/Planner only)
  - **Inventory**: Template download, export all inventory, import quantity updates (Admin/Production Staff only)
  - **Suppliers**: Template download, export all suppliers, import new/update existing (Admin/Procurement only)
  - All imports support robust error handling, per-row validation, and detailed error reporting
  - Error reporting: Shows specific errors for up to 10 failed rows to help users fix issues
- Implemented automatic Work Order number generation
  - Format: WO-XXXXXX (6 digits starting from WO-000001)
  - Sequential numbering with retry logic to handle concurrent submissions
  - Displays next WO number on creation form
  - Handles legacy work order formats gracefully
- Implemented automatic Purchase Order number generation
  - Format: PO-XXXXXX (6 digits starting from PO-000001)
  - Sequential numbering with retry logic to handle concurrent submissions
  - Displays next PO number on creation form
  - Handles legacy purchase order formats gracefully
- Added manual Inventory creation capability
  - Auto-generated Inventory IDs in format INV-XXXXXX
  - Create inventory records for products without existing inventory
  - Inventory ID displayed when creating or receiving inventory
  - Purchase order receiving now shows the inventory ID upon completion
- Created comprehensive Material Requirements Report
  - Summary dashboard with 4 key metrics: Total Requirements, Items with Shortages, Total Value, Shortage Value
  - Material Shortages by Product section showing aggregated shortage quantities and values
  - Detailed requirements table with work order links, product details, quantities, costs, and status
  - Rows with shortages highlighted in yellow for quick identification
  - CSV export functionality for external analysis
  - Ordered by planned start date and shortage priority
  - **Direct Procurement Capability**: Create purchase orders directly from material requirements
    - Select multiple items with shortages using checkboxes
    - Auto-populate quantities based on shortage amounts
    - Select supplier for each product
    - Adjust quantities and unit prices before creating POs
    - Automatically generates sequential PO numbers
    - **Clickable PO Links**: Success messages include direct links to created purchase orders
    - Streamlined procurement workflow from identification to viewing
  - Accessible from Reports menu in sidebar navigation
- Added Purchase Order Print/Download Functionality
  - Professional formatted PO document with company branding
  - View/Print button opens PO in new tab for browser printing or PDF save
  - Download button saves formatted PO as HTML file
  - Includes comprehensive order details, supplier information, and product specifications
  - Print-optimized styling with clean layout for physical documents
- Upgraded BOM System to Full Industry Standards (Aviation/Manufacturing/MRO)
  - **Multi-Level BOM Hierarchy**: Automatic level numbering (1.0, 1.1, 1.1.1) with recursive tree structure
  - **Find Number Designation**: Auto-generated sequential find numbers per assembly with manual override capability
  - **Category Management**: Predefined categories (Electrical, Mechanical, Hardware, Consumable, Subassembly, Other)
  - **Revision Control**: Revision tracking, effectivity dates, and status (Active, Obsolete, Pending)
  - **Reference Designators**: Support for component position identifiers (R1, C2, U3)
  - **Document Links**: Attach drawings, specs, and manuals to BOM items
  - **Tree View UI**: Interactive expand/collapse visualization with color-coded icons for assemblies vs components
  - **Advanced Filtering**: Filter by parent product, category, and status with real-time updates
  - **Clone BOM**: Copy entire BOM structures to new products as templates
  - **Mass Update**: Bulk update status, revision, or category for all items in an assembly
  - **Roll-up Summaries**: Category-based cost and quantity aggregation with summary dashboards
  - **Validation**: Duplicate part prevention, auto-calculated extended costs, visual status indicators
  - **Enhanced Data**: Unit cost, extended cost, notes, scrap percentage tracking
- UI improvements: Green "Dynamic.IQ.MRP" branding, animated diagonal lines on login background, fixed label overlapping
- Test admin account created: username=admin, password=admin123

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
- Schema includes users, products, BOMs, inventory, work orders, purchase orders, suppliers, and user_permissions
- Uses SQLite's row factory for dictionary-like result access
- User permissions table stores granular permissions per user with unique constraints

**Authentication & Authorization**:
- Session-based authentication using Flask sessions
- Password hashing with Werkzeug's security utilities
- Decorator-based access control (`@login_required`, `@role_required`)
- Four user roles: Admin, Planner, Production Staff, and Procurement
- Secure registration: new users are automatically assigned "Production Staff" role (admins must promote users to elevated roles)

**Application Structure**:
- Blueprint-based modular routing system
- Separate route modules for each major feature (auth, products, BOMs, suppliers, inventory, work orders, purchase orders, reports, users, permissions)
- Context processor injects current user into all templates
- Before-request hook initializes database on each request
- Granular permissions system allows fine-grained access control beyond role-based authorization

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
  - CSV Import/Export: Bulk import/export with template download, auto-creates inventory for new products
  - Format: Code, Name, Description, Unit of Measure, Product Type, Cost
- **Bill of Materials (BOM)**: Industry-standard multi-level BOM system with comprehensive features
  - **Hierarchy Support**: Multi-level parent-child relationships with automatic level calculation and tree visualization
  - **Find Numbers**: Auto-generated sequential find numbers (1, 2, 3...) per assembly with manual override
  - **Categories**: Electrical, Mechanical, Hardware, Consumable, Subassembly, Other
  - **Revision Control**: Revision tracking (A, B, C...), effectivity dates, status (Active, Obsolete, Pending)
  - **Reference Data**: Reference designators (R1, C2), document links, notes, quantity, scrap percentage
  - **Cost Tracking**: Unit cost and extended cost auto-calculation
  - **Tree View**: Interactive expand/collapse hierarchy with color-coded assembly/component icons
  - **Filtering**: Real-time filter by parent product, category, and status
  - **Clone Function**: Copy complete BOM structure to new products
  - **Mass Update**: Bulk update status, revision, or category for entire assemblies
  - **Roll-up Summaries**: Cost and quantity aggregation by category with dashboard metrics
  - **Validation**: Duplicate prevention, required field checks, visual indicators for inactive parts
  - CSV Import/Export: Bulk import and export BOMs with validation and error handling
  - Format: Parent Code, Parent Name, Child Code, Child Name, Quantity, Scrap Percentage
- **Inventory**: Tracks quantity, reorder points, and safety stock levels
  - Manual Creation: Create inventory records for products without existing inventory
  - Auto-generated Inventory IDs in format INV-XXXXXX (displayed in list and confirmation messages)
  - CSV Import/Export: Bulk import/export for updating inventory levels and parameters
  - Format: Product Code, Product Name, Quantity, Reorder Point, Safety Stock
  - Inventory ID automatically created when receiving purchase orders
- **Work Orders**: Production orders with status tracking, cost allocation (material/labor/overhead)
  - Auto-generated WO numbers in format WO-XXXXXX
- **Purchase Orders**: Procurement tracking with supplier relationships
  - Auto-generated PO numbers in format PO-XXXXXX
  - Individual PO view with detailed supplier info, product details, cost breakdown, and inventory status
  - Clickable PO numbers in list view for quick access
  - Direct receive functionality from PO detail page
  - **Print/Download Capability**: Professional formatted PO documents with print and download options
    - View/Print button opens formatted PO in new tab for printing
    - Download button saves PO as HTML file (PO_XXXXXX.html)
    - Professional layout with company branding, supplier details, and order information
    - Print-optimized styling for clean paper/PDF output
- **Suppliers**: Vendor management with contact information
  - CSV Import/Export: Bulk import/export with template download for supplier management
  - Format: Code, Name, Contact Person, Email, Phone, Address

**Inventory Management**:
- Real-time stock level tracking
- Low stock alerts based on reorder points
- Manual inventory adjustment capability
- Automatic inventory updates from work order processing

**Reporting System**:
- Inventory valuation reports
- Work order cost analysis
- Material requirements report with summary statistics, CSV export, and direct procurement capability
- Material usage tracking
- Purchase order suggestions based on stock levels
- Integrated procurement workflow from material requirements to purchase orders

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