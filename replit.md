## Overview

Dynamic.IQ-MRPx is a comprehensive Flask-based Manufacturing Resource Planning (MRP) system designed to optimize production processes, inventory, Bill of Materials (BOM), work orders, and purchase orders. It offers robust role-based access control, efficient material tracking, production planning, supplier management, and report generation. The system aims to enhance operational efficiency, deliver critical business insights, and includes advanced AI-driven modules for strategic insights and automation in supplier discovery and market/capability analysis.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### UI/UX Decisions

The system features a professional, elegant user interface built on Bootstrap 5, Bootstrap Icons, and custom CSS design tokens. It utilizes the Inter font family and a refined slate-based color palette for a cohesive executive look. Key UI elements include an Executive Dashboard with KPI cards, Chart.js visualizations, and responsive grid layouts. Table sorting, filtering, and consistent currency formatting are implemented throughout the application.

### Technical Implementations

The backend is developed with Flask, using Blueprints and an SQLite database. It implements session-based authentication and comprehensive role-based access control. The `MRPEngine` handles core MRP logic, including recursive BOM explosion and automatic sequential numbering. An Audit Trail System logs CUD operations. The frontend uses Jinja2 for templating.

**Key Modules and Features:**
-   **Products & BOM**: Manages product data, multi-level BOMs with revision control, and UOM conversion.
-   **Inventory**: Tracks stock levels, supports serialized products, and handles adjustments.
-   **Work Orders**: Manages production orders with disposition types, customer association, status tracking, cost allocation, integrated task/labor planning, material allocation, configurable workflow stages with color-coded badges, and mass update functionality for bulk operations (Admin/Planner roles).
-   **Task Templates**: Reusable task template system for standardizing work order tasks.
-   **Purchase Orders**: Supports multi-line procurement, supplier relationships, dynamic line item management, and partial/full receiving with UOM conversion. Includes Quick Add Product modal and Mass Update functionality.
-   **Tools Management**: Comprehensive tool and equipment tracking system including calibration scheduling, location management, and checkout/checkin workflow.
-   **RFQ (Request for Quotation) Module**: Full RFQ lifecycle management from creation to quote comparison and selection.
-   **Contact Management**: Manages multiple contacts for suppliers and customers with primary contact designation.
-   **Sales Module**: Comprehensive sales order management including various order types, tax calculation, inventory integration, and a 5-state order workflow.
-   **Shipping & Receiving Module**: Manages shipment lifecycle for Sales and Work Orders.
-   **Invoice Management Module**: Comprehensive billing and A/R system generating invoices from Sales/Work Orders.
-   **Service Management Module**: Comprehensive service work order system for various service types, including labor, materials, expenses, and approval processes.
-   **MRO Capabilities Management**: Manages MRO capabilities associated with part numbers, compliance, and certification requirements.
-   **AI Supplier Discovery**: AI-powered supplier discovery integrated into material requirements for recommending suppliers.
-   **Market & Capability Analysis**: AI-driven module for generating strategic reports from fleet data with capability matching and win probability analysis.
-   **Capacity Planning Module**: Comprehensive system for managing production load, work center utilization, labor resource allocation, and bottleneck detection.
-   **Customer Service Module**: Internal dashboard for customer order and work order visibility, including a confirmation workflow, at-risk order tracking, communications log, activity timeline, escalation management, SLA configuration, customer feedback, and a secure Customer Portal.
-   **Organizational Analyzer Module**: AI-powered executive intelligence dashboard providing CEO-level organizational insights with KPIs, AI-powered recommendations, alerts, and forecasting capabilities.
-   **Financial Analyzer Module**: Super AI CFO providing executive-level financial intelligence including cash position, burn rate, runway analysis, revenue/margins, operational efficiency metrics, risk indicators (A/R aging, concentration risk), AI-powered CFO analysis with health scoring, scenario modeling (growth, cost reduction, stress testing), and 90-day financial outlook.
-   **ERP Copilot (AI Helper)**: Floating, context-aware AI assistant embedded throughout the application. Provides user guidance, natural language ERP queries, process enforcement, workflow explanations, smart recommendations, and audit support. Features role-aware assistance (Admin, Sales, Planner, etc.), customer-friendly mode for portal users, and guarded action execution with confirmation prompts. Accessible via floating button (bottom-right) on all authenticated pages.
-   **NDT (Non-Destructive Testing) Module**: Comprehensive NDT operations management system featuring technician registry with certification tracking (UT, MT, PT, RT, VT, ET methods at Levels I-III), NDT work order lifecycle management with 8-state workflow (Draft→Scheduled→In Inspection→Results Recorded→Under Review→Approved/Rejected→Closed), inspection result recording with defect tracking, certification validation enforcement (technicians must be certified for methods on inspection date), Level III review approval workflow, integration with Sales Orders and Manufacturing Work Orders, invoice/quote generation directly from NDT work orders, and an operations dashboard with KPIs (first pass yield, rejection rate, cycle time, certification compliance).
-   **AI Super Master Scheduler Module**: AI-powered Master Production Schedule (MPS) system providing finite-capacity scheduling, ATP/CTP (Available-to-Promise/Capable-to-Promise) calculations, exception detection and classification (late orders, capacity overloads, material shortages, bottlenecks), AI-driven recommendations for conflict resolution, scenario comparison (Plan A/B/C), schedule override governance with justification logging, and real-time capacity load visualization. Features dashboard with OTD metrics, at-risk orders, bottleneck radar, and AI analysis capabilities.
-   **Salesforce Data Migration Agent**: Enterprise data integration module for migrating Salesforce CRM data into the ERP. Features OAuth 2.0 authentication for Salesforce connections, automatic object discovery (contacts, accounts, opportunities, custom objects), schema introspection and dynamic ERP table generation, intelligent field mapping with type conversion, batch ETL pipeline with error handling, data reconciliation and validation (count/checksum matching), comprehensive audit trail, and zero-data-loss architecture. Supports full, incremental, and selective migration types with multi-phase workflow (Discovery → Schema → Mapping → ETL → Validation).
-   **Business Analytics AI Super Agent**: Autonomous intelligent manager for ERP process optimization and predictive analytics. Features real-time KPI monitoring across all modules (Finance, Supply Chain, MRO, HR, Sales, Operations), AI-powered analysis and recommendations using GPT-4o, predictive analytics for demand/inventory/revenue forecasting, bottleneck identification and process optimization, active alerts system, process health scoring, executive summary generation, natural language query interface for asking business questions, and quick actions for common analysis tasks.
-   **Secure IT Manager AI Super Agent**: Comprehensive IT security and governance module (Admin-only access) featuring security operations center dashboard with security posture scoring, real-time threat monitoring, incident response management with severity tracking, access governance with user risk scoring algorithm, compliance framework monitoring (SOC2, HIPAA, PCI-DSS, ISO27001, NIST), AI agent trust and oversight system, change management workflow, security alert management with resolution tracking, AI-powered natural language security commands, system lockdown capabilities, and comprehensive audit logging for all security operations.

### System Design Choices

-   **Inventory Management**: Real-time stock level tracking, low stock alerts, and automatic updates.
-   **Reporting System**: Provides various reports including inventory valuation, work order cost analysis, and material requirements.
-   **Accounting System**: Features a Chart of Accounts (COA), General Ledger (GL), Manual Journal Entries, automatic GL posting, and financial reports.
-   **Time Clock Station**: Dedicated employee time tracking system with simplified employee code authentication (no PIN for fast shop floor access), clock in/out functionality, work order and task assignment with automatic labor cost tracking. Clock-out calculates hours worked and updates work_order_tasks.actual_hours and actual_labor_cost.
-   **Labor Resources & Skillset Management**: Comprehensive multi-skillset assignment system for tracking employee competencies.

## External Dependencies

-   **Python Packages**: `Flask`, `Flask-Login`, `Werkzeug`, `ReportLab`, `Pandas`, `openpyxl`, `openai`, `sqlite3`.
-   **Frontend Libraries**: Bootstrap 5.3.0, Bootstrap Icons 1.11.0, Chart.js 4.4.0.
-   **AI Integration**: OpenAI API (GPT-4o).
-   **Database**: SQLite (`mrp.db`).
-   **Environment Variables**: `SESSION_SECRET`.

## Sample Data

The system includes comprehensive sample data for testing and showcasing all functionalities. Run `python seed_sample_data.py` to populate the database with:

-   **10 Customers**: Major airlines (Delta, United, Southwest, American, JetBlue, Alaska, FedEx, UPS, Spirit, Frontier) with portal access enabled
-   **10 Suppliers**: Aerospace OEMs (Boeing, Airbus, Pratt & Whitney, GE, Honeywell, Collins, Spirit AeroSystems, Safran, Parker, Moog)
-   **10 MRO Capabilities**: Landing gear, fuel systems, flight controls, engine mounts, APU, pneumatics, NDT services, composites, avionics
-   **15 Products**: Aircraft parts (actuators, pumps, valves), assemblies, consumables (O-rings, fluids, fasteners), and raw materials
-   **8 Work Orders**: Various statuses with tasks, stages, and customer assignments
-   **6 Sales Orders**: Different order types (Standard Sale, Repair Exchange, Outright)
-   **5 Purchase Orders**: Multi-line orders with different suppliers
-   **3 RFQs**: With supplier assignments and quotes
-   **3 NDT Work Orders**: With technicians and certifications (UT, MT, PT, RT, VT, ET)
-   **3 Service Work Orders**: On-site repair, calibration, emergency AOG
-   **5 Work Centers**: Hydraulics, Avionics, NDT Lab, Assembly, Test Cell
-   **8 Labor Resources**: Technicians with multi-skillset assignments
-   **5 Tools**: With calibration tracking
-   **Chart of Accounts, SLA Configurations, Task Templates**

**Test Login Credentials:**
-   Admin: `admin` / `admin123`
-   Planner: `jsmith` / `planner123`
-   Sales: `mwilson` / `sales123`
-   Technician: `tgarcia` / `tech123`
-   Customer Service: `lchen` / `cs123`