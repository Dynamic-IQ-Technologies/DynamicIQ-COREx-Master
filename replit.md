## Overview

Dynamic.IQ-COREx is a Flask-based Manufacturing Resource Planning (MRP) system designed to optimize production processes, inventory, Bill of Materials (BOM), work orders, and purchase orders. It provides robust role-based access control, efficient material tracking, production planning, supplier management, and report generation. The system aims to enhance operational efficiency, deliver critical business insights, and includes advanced AI-driven modules for strategic insights, automation in supplier discovery, and market/capability analysis, contributing to improved business intelligence and operational excellence.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### UI/UX Decisions

The system features a professional, elegant user interface built on Bootstrap 5, Bootstrap Icons, and custom CSS design tokens, utilizing the Inter font family and a slate-based color palette. Key UI elements include an Executive Dashboard with KPI cards, Chart.js visualizations, responsive grid layouts, and a custom professional notification system.

### Technical Implementations

The backend is developed with Flask, using Blueprints, implementing session-based authentication and role-based access control. The `MRPEngine` handles core MRP logic, and an Audit Trail System logs CUD operations. The frontend uses Jinja2 for templating. Key modules include Core MRP, Supply Chain & Sales, Asset & Service Management, Quality & Compliance, and various AI-Powered Modules.

The system incorporates a novel architecture for ERP exchange management, comprising an Exchange Dependency Graph Engine, Deterministic Event Processing Engine, AI Execution Path Modifier, Performance Instrumentation System, Cryptographic Security Layer, and an Exchange Orchestrator.

### System Design Choices

The system supports dual database environments (SQLite for development, PostgreSQL for production) with a comprehensive PostgreSQL compatibility layer that:
- Translates SQLite functions to PostgreSQL equivalents (JULIANDAY → date arithmetic, strftime → TO_CHAR, GROUP_CONCAT → STRING_AGG)
- Converts double-quoted strings to single quotes for PostgreSQL
- Automatically converts Decimal values to float when fetching data (prevents TypeError in templates)
- Handles date arithmetic patterns (date('now', '+7 days') → CURRENT_DATE + INTERVAL '7 days')

Key features include:
- **Inventory Management**: Real-time tracking, alerts, FAA-compliant labels, and cost transfer system.
- **Work Order Management**: Accordion layout with task-level material requirements, master routing templates, and reconciliation module.
- **Sales Order Management**: Dual exchange workflow (SO to PO), professional document generation, email acknowledgements, and allocation to work orders.
- **Purchase Order Management**: Service/misc POs for work orders, exchange PO obligations tracking, supplier portal, and quick access to inventory on receiving.
- **Accounting & Reporting**: Chart of Accounts, General Ledger, financial and operational reports.
- **Labor Management**: Time clock station with skill-based task filtering, labor resources, and skillset management.
- **AI-Powered Modules**: COREx NeuroIQ Executive Intelligence System (conversational AI), COREx Guide Transaction Assistant (proactive field assistance), Part Intake System (web part capture), Marketing Presentation Generator, Executive Sales/Procurement Dashboards with AI Copilot, and Leads Management with AI-powered sales engagement.
- **Quality & Compliance**: Duplicate Detection System with multiple algorithms and configurable thresholds.
- **Core Tracking**: Core Due Days tracking for exchange orders.
- **Dynamic Material Issue Module**: High-performance multi-material issuance with real-time inventory validation.
- **Unplanned Receipt Module**: Registration and controlled management of items arriving without documentation, supporting full lifecycle tracking from intake through inventory conversion or work order processing, with role-based approvals and complete audit trail.
- **Inventory Split Function**: Ability to split inventory records into multiple records for the same product, enabling flexible location/condition management.

## External Dependencies

-   **Python Packages**: `Flask`, `Flask-Login`, `Werkzeug`, `ReportLab`, `Pandas`, `openpyxl`, `openai`, `psycopg2-binary`.
-   **Frontend Libraries**: Bootstrap 5.3.0, Bootstrap Icons 1.11.0, Chart.js 4.4.0.
-   **AI Integration**: OpenAI API (GPT-4o).
-   **Database**: SQLite (for development), PostgreSQL (for production).