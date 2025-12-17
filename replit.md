## Overview

Dynamic.IQ-MRPx is a comprehensive Flask-based Manufacturing Resource Planning (MRP) system designed to optimize production processes, inventory, Bill of Materials (BOM), work orders, and purchase orders. It offers robust role-based access control, efficient material tracking, production planning, supplier management, and report generation, aiming to provide a scalable, user-friendly solution to enhance operational efficiency and deliver critical business insights. The system also includes advanced AI-driven modules for supplier discovery and market/capability analysis, leveraging AI for strategic insights and automation.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### UI/UX Decisions

The system features a professional, elegant user interface with a modern design system built on Bootstrap 5, Bootstrap Icons, and custom CSS design tokens. The UI uses the Inter font family and a refined slate-based color palette for a cohesive executive look.

**Design System (`static/css/style.css`)**:
- CSS design tokens for colors, typography, shadows, and border-radius
- Primary palette: Blue (#2563eb), Success (#059669), Warning (#d97706), Danger (#dc2626), Info (#0891b2), Purple (#7c3aed)
- Neutral palette: Slate scale from 50-900 for text, backgrounds, and borders
- Consistent shadow system (sm, default, md, lg) for depth hierarchy
- Inter font family with fallback to system fonts

**Executive Dashboard**: The main dashboard features an elegant, professional design including:
- 8 KPI cards with accent top-borders, icon chips, and hover effects
- Section labels with uppercase styling for visual hierarchy
- Interactive Chart.js visualizations with coordinated color palette
- Stats mini-cards for key metrics (Products, Customers, Suppliers, Employees)
- Real-time tables with refined styling (uppercase headers, zebra striping)
- Role-based widgets for Service Operations, Workforce Status, and Material Requirements
- Quick action cards with hover states for creating new orders and accessing reports
- Responsive grid layout (4 columns on XL, 3 on LG, 2 on MD, 1 on mobile)

**Table Sorting & Filtering**: All list pages include client-side sortable columns. Click any column header to sort by that column (ascending/descending). The reusable `TableUtils` module (`static/js/table-utils.js`) supports string, number, currency, and date sorting with proper parsing of formatted values. Several pages also include quick search filters for instant row filtering.

**Currency Formatting**: All monetary values throughout the application use a centralized Jinja2 `|currency` filter defined in `app.py`. This ensures consistent professional formatting with dollar sign, thousands separators, and two decimal places (e.g., `$ 8,750.00`). Negative values display as `-$ 8,750.00`.

### Technical Implementations

The backend is developed with Flask using Blueprints and an SQLite database (`mrp.db`). It implements session-based authentication and comprehensive role-based access control (Admin, Planner, Production Staff, Procurement, Accountant). The `MRPEngine` handles core MRP logic like recursive BOM explosion and automatic sequential numbering. An Audit Trail System logs all CUD operations. The frontend uses Jinja2 for templating.

**Key Modules and Features:**
-   **Products & BOM**: Manages product data, multi-level BOMs with revision control, and UOM conversion management.
-   **Inventory**: Tracks stock levels, supports serialized products, and handles adjustments.
-   **Work Orders**: Manages production orders with disposition types, customer association, status tracking, cost allocation, and integrated task/labor planning. Includes a Work Order Quote System and an advanced Material Allocation and Issuance Workflow. Features advanced filtering, sorting, and task-level material/skill management with availability and match indicators.
-   **Task Templates**: Reusable task template system for standardizing work order tasks. Features template creation with multiple task items, categories, priorities, and planned hours. Templates can be applied to work orders with one click, automatically creating all defined tasks with "Not Started" status. Accessible from Operations menu and directly from work order view.
-   **Purchase Orders**: Supports multi-line procurement, supplier relationships, dynamic line item management, and partial/full receiving with integrated UOM conversion. Features a Quick Add Product modal allowing users to create new products directly from PO line items when a part number doesn't exist in the system. Includes Mass Update functionality for bulk updating status, dates, and notes across multiple purchase orders.
-   **Contact Management**: Both suppliers and customers support multiple contacts with fields for name, title, department, email, phone, and mobile. Includes primary contact designation with single-primary enforcement and full CRUD operations accessible from the edit pages.
-   **Sales Module**: Comprehensive sales order management including customer CRUD, various order types, advanced line management, tax calculation, inventory integration (including serialized products), and a 5-state order workflow. Features line-level inventory allocation with serial number tracking.
-   **Shipping & Receiving Module**: Manages shipment lifecycle for Sales and Work Orders with multi-line support, tracking information, and status workflows.
-   **Invoice Management Module**: Comprehensive billing and A/R system generating invoices from Sales/Work Orders, supporting a full invoice lifecycle, multi-line items, and an Invoice Dashboard with automatic revenue recognition.
-   **Service Management Module**: Comprehensive service work order system for standard Service and NDT work orders. Features customer association, equipment tracking, multi-line labor tracking, materials allocation, expense tracking, status workflow, and approval processes.
-   **MRO Capabilities Management**: Dedicated system for managing MRO capabilities associated with part numbers, including capability code management, compliance specifications, certification requirements, and status management. Supports flexible specifications with units, ranges, and critical flags.
-   **AI Supplier Discovery**: AI-powered supplier discovery integrated directly into the Material Requirements page. Users can click "Find Suppliers" on any shortage item to run AI-powered discovery, view ranked supplier recommendations with confidence scores, and approve/reject suppliers - all within a modal interface without leaving the page.
-   **Market & Capability Analysis**: AI-driven module (using OpenAI GPT-4o) that generates comprehensive strategic reports from airline fleet data. It supports both AI-powered fleet data auto-generation and CSV/Excel upload, performs automated capability matching, and generates detailed 11-section executive reports with AI-calculated win probability percentages based on multiple factors. Features interactive Chart.js visualizations, filtering, and export functionalities.
-   **Capacity Planning Module**: Comprehensive capacity planning system for managing production load and work center utilization. Features:
    - Work center management with efficiency factors, cost tracking, and resource assignment
    - Labor resource allocation with utilization percentages and effective date ranges
    - Capacity overrides for specific dates (holidays, maintenance, schedule changes)
    - Work order operations linked to work centers with planned hours and setup time
    - Work order tasks can be assigned to work centers for capacity planning (tasks include planned hours)
    - Real-time utilization calculation incorporating both operations AND tasks, overrides and resource factors
    - Interactive dashboard with Chart.js bar/doughnut charts for capacity visualization
    - Bottleneck detection with status indicators (Normal <85%, Warning 85-100%, Critical >100%)
    - Printable capacity reports with operation and task details plus override summaries
    - Proper date overlap logic for accurate load calculations across date ranges (uses task-level dates with fallback to WO dates)
-   **Customer Service Module**: Internal customer service dashboard providing full visibility into customer orders and work orders. Features:
    - Executive dashboard with KPI cards (total orders, active orders, pending confirmations, overdue count)
    - Orders by status chart visualization with doughnut chart
    - At-risk orders tracking with risk level indicators (Overdue, Critical, Warning)
    - Work order confirmation workflow with checklist (quote approved, materials available, capacity available)
    - Pending quote follow-up tracking with days pending indicators
    - Order stage progress tracking with 6 default stages (Order Received, Engineering Review, Material Procurement, Production, Quality Assurance, Shipping)
    - Order detail view with linked work orders, stage progress visualization, notes, and activity timeline
    - Sales Order to Work Order linking via so_id foreign key for full traceability
    - Create Work Order directly from order detail page with product selection from order lines
    - Quote approval workflow that updates order status and initiates stage tracking
    - Order status automation engine that automatically updates sales order status based on work order release and stage completion
    - Customer visibility in work order tables showing linked customer and SO number
    - Customer Communications Log for tracking calls, emails, meetings with follow-up scheduling
    - Quick Notes system for adding notes to orders with type categorization (General, Internal, Customer, Urgent, Follow-up) and pinning
    - Activity Timeline showing all order activities (status changes, notes, communications) in chronological order
    - Analytics dashboard with KPIs (total communications, pending/overdue follow-ups), charts by type and month, top customers, and stage duration metrics
    - Escalation Management with priority levels (Critical, High, Medium, Low), escalation levels (1-3), assignment tracking, and resolution workflow
    - SLA Configuration for defining service level agreements with response time, resolution time, and escalation thresholds
    - SLA Breach Monitor showing orders at risk of missing service targets with Critical/Warning status indicators
    - Customer Feedback System for tracking satisfaction ratings (1-5 stars), recommendation likelihood, and feedback comments
    - Customer Portal: Public-facing web portal allowing customers to view their order status via unique secure links without logging in. Features include:
      - Secure token-based access using secrets.token_urlsafe(32)
      - Portal dashboard showing all customer orders with status, dates, and totals
      - Order detail view with line items, stage progress, and linked work orders
      - Portal management in customer edit page (generate link, enable/disable, regenerate)
      - Separate portal templates with customer-friendly design
      - Routes: /portal/<token> for dashboard, /portal/<token>/order/<id> for details
    - Database tables: order_stage_tracking, work_order_confirmations, customer_communications, order_notes, order_activity_log, order_escalations, sla_configurations, customer_feedback (work_orders.so_id links to sales_orders)

### System Design Choices

-   **Inventory Management**: Real-time stock level tracking, low stock alerts, and automatic updates.
-   **Reporting System**: Provides various reports including inventory valuation, work order cost analysis, material requirements, material usage, and active labor.
-   **Accounting System**: Features a Chart of Accounts (COA), General Ledger (GL), Manual Journal Entries, and automatic GL posting for inventory and A/P transactions. Includes financial reports (Trial Balance, Balance Sheet, Income Statement) and an Accounts Payable (A/P) module with an Executive Accounting Dashboard.
-   **Time Clock Station**: Dedicated employee time tracking system with secure PIN-based authentication, clock in/out functionality, work order and task assignment tracking.
-   **Labor Resources & Skillset Management**: Comprehensive multi-skillset assignment system for tracking employee competencies with proficiency levels and visual indicators.

## External Dependencies

-   **Python Packages**: `Flask`, `Flask-Login`, `Werkzeug`, `ReportLab`, `Pandas`, `openpyxl`, `openai`, `sqlite3`.
-   **Frontend Libraries**: Bootstrap 5.3.0, Bootstrap Icons 1.11.0, Chart.js 4.4.0.
-   **AI Integration**: OpenAI API via Replit AI Integrations (GPT-4o).
-   **Database**: SQLite (`mrp.db`).
-   **Environment Variables**: `SESSION_SECRET`.