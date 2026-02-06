## Overview

Dynamic.IQ-COREx is a Flask-based Manufacturing Resource Planning (MRP) system designed to optimize production processes, inventory, Bill of Materials (BOM), work orders, and purchase orders. It provides robust role-based access control, efficient material tracking, production planning, supplier management, and report generation. The system aims to enhance operational efficiency, deliver critical business insights, and includes advanced AI-driven modules for strategic insights, automation in supplier discovery, and market/capability analysis, contributing to improved business intelligence and operational excellence.

## User Preferences

Preferred communication style: Simple, everyday language.
AI Report Generation: Do not use special characters when generating AI market analysis reports.

## System Architecture

### UI/UX Decisions

The system features a professional, elegant user interface built on Bootstrap 5, Bootstrap Icons, and custom CSS design tokens, utilizing the Inter font family and a slate-based color palette. Key UI elements include an Executive Dashboard with KPI cards, Chart.js visualizations, responsive grid layouts, and a custom professional notification system.

### Technical Implementations

The backend is developed with Flask, using Blueprints, implementing session-based authentication and role-based access control. The `MRPEngine` handles core MRP logic, and an Audit Trail System logs CUD operations. The frontend uses Jinja2 for templating. Key modules include Core MRP, Supply Chain & Sales, Asset & Service Management, Quality & Compliance, and various AI-Powered Modules.

The system incorporates a novel architecture for ERP exchange management, comprising an Exchange Dependency Graph Engine, Deterministic Event Processing Engine, AI Execution Path Modifier, Performance Instrumentation System, Cryptographic Security Layer, and an Exchange Orchestrator.

### System Design Choices

The system uses **PostgreSQL for both development and production** to ensure consistent behavior. This eliminates SQLite vs PostgreSQL compatibility issues (NULL handling, empty strings, date formats). The PostgreSQL compatibility layer provides:
- Translates SQLite functions to PostgreSQL equivalents (JULIANDAY → EXTRACT(EPOCH FROM ...), strftime → TO_CHAR, GROUP_CONCAT → STRING_AGG)
- Converts double-quoted strings to single quotes for PostgreSQL
- Automatically converts Decimal values to float when fetching data (prevents TypeError in templates)
- Handles date arithmetic patterns (date('now', '+7 days') → CURRENT_DATE + INTERVAL '7 days')
- Uses balanced parenthesis parser for complex nested functions like JULIANDAY(COALESCE(...))
- Preserves time precision in JULIANDAY calculations using EXTRACT(EPOCH FROM ...) / 86400.0 (returns fractional days)
- Handles SUBSTR → SUBSTRING, datetime('now') → CURRENT_TIMESTAMP translations
- Date parameter casting: date(?, '+7 days') → ((?::date) + INTERVAL '7 days')::date
- GROUP_CONCAT → STRING_AGG with balanced parenthesis matching for complex nested expressions

**PostgreSQL Query Compatibility Notes:**
- Use GROUP BY 1, 2, etc. instead of column aliases (PostgreSQL strict mode)
- Use HAVING COUNT(*) > 0 instead of HAVING alias > 0 (aliases not allowed in HAVING)
- Use ORDER BY 1 DESC instead of ORDER BY alias DESC (prefer column indices)
- All non-aggregated columns must appear in GROUP BY clause
- Use CASE WHEN instead of COALESCE when mixing TEXT and TIMESTAMP types

### Error Handling & Reliability

The system implements enterprise-grade error handling:
- **Global Exception Handler**: Catches all unhandled exceptions with correlation IDs for traceability
- **Request Correlation IDs**: Every request gets a unique ID (8 chars) for log correlation
- **Structured Error Responses**: Standardized JSON/HTML error responses with category and correlation_id
- **Error Handler Decorators**: `@route_error_handler` and `@api_error_handler` for consistent error handling
- **Safe Template Utilities**: Global Jinja functions (`safe_get`, `safe_int`, `safe_float`, `safe_str`, `coalesce`) for null-safe data access
- **Startup Validation**: Environment variable checks at application startup

### Production Hardening (utils/production_hardening.py)

The system implements comprehensive production reliability measures:
- **Environment Parity**: Validates required env vars (DATABASE_URL, SESSION_SECRET) on startup
- **Schema Validation**: Checks critical tables exist with required columns
- **Schema Drift Detection**: Compares dev SQLite schema to prod PostgreSQL
- **Transaction Safety**: TransactionManager class with auto-rollback on failures
- **Pre-Insert Validation**: Validates required fields before database writes
- **Structured Errors**: StructuredError class with error codes, categories, and correlation IDs
- **Cold Start Protection**: App marks ready only after all validations pass
- **Startup Self-Audit**: Complete validation run on every deployment

### Health Check Endpoints (routes/health_routes.py)

Production monitoring endpoints:
- `/health` - Basic app readiness check
- `/health/db` - Database connection and critical table validation
- `/health/transactions` - Write + rollback test to verify transaction capability
- `/health/schema` - Schema drift detection between dev and prod
- `/health/full` - Complete system health status
- **Error Categories**: Validation, Authorization, Data, System - for clear error classification
- **Error Logging**: Full stack traces with correlation IDs logged to `error_handler` logger
- **Utilities Module**: `utils/error_handler.py` provides reusable validation and error handling functions

### Production Query Validator (utils/production_query_validator.py)

Pre-deployment validation tool to catch PostgreSQL compatibility issues:
- Run `python utils/production_query_validator.py` before deploying to production
- Detects issues NOT handled by PostgresTranslatingCursor:
  - NULL comparisons with boolean columns (`is_core = 0` when column may be NULL)
  - Column name mismatches (`sales_order_id` vs `so_id` in sales_order_lines)
  - IFNULL function (use COALESCE instead)
- Note: SQLite functions like `datetime('now')`, `julianday()`, `strftime()`, `GROUP_CONCAT()` are auto-translated

**Critical PostgreSQL NULL Handling Rules:**
- In PostgreSQL, `NULL = 0` returns NULL (not TRUE), causing WHERE clauses to fail
- For nullable boolean columns, use: `COALESCE(column, 0) = 0` or `(column IS NULL OR column = 0)`
- Affected columns in this codebase: `is_core`, `is_replacement` in sales_order_lines

**Schema Reference - sales_order_lines:**
- Foreign key to sales_orders: `so_id` (NOT `sales_order_id`)
- Nullable boolean fields: `is_core`, `is_replacement`

Key features include:
- **Inventory Management**: Real-time tracking, alerts, FAA-compliant labels, and cost transfer system.
- **Work Order Management**: Accordion layout with task-level material requirements, master routing templates, and reconciliation module.
- **Sales Order Management**: Dual exchange workflow (SO to PO), professional document generation, email acknowledgements, and allocation to work orders.
- **Purchase Order Management**: Service/misc POs for work orders, exchange PO obligations tracking, supplier portal, and quick access to inventory on receiving.
- **Accounting & Reporting**: Chart of Accounts, General Ledger, financial and operational reports, with comprehensive automatic journal entry generation for all financial transactions.
- **Automatic GL Journal Entries**: All financial transactions auto-generate double-entry journal entries:
  - Material Receiving: DR Inventory (1130), CR A/P (2110)
  - Material Issue to WO: DR WIP (1140), CR Inventory (1130)
  - Labor Tracking: DR WIP (1140), CR Wages Payable (2150)
  - Work Order Completion: DR Finished Goods (1150), CR WIP (1140)
  - Sales Invoice Posting: DR A/R (1120), CR Sales Revenue (4100)
  - A/R Payment Received: DR Cash (1110), CR A/R (1120)
  - A/P Payment Made: DR A/P (2110), CR Cash (1110)
  - Tool Purchase: DR Equipment (1210), CR A/P (2110) or CR Cash (1110)
  - NDT Labor/Materials/Subcontract: All tracked through proper WIP accounting
- **Labor Management**: Time clock station with skill-based task filtering, labor resources, and skillset management.
- **AI-Powered Modules**: COREx NeuroIQ Executive Intelligence System (conversational AI), COREx Guide Transaction Assistant (proactive field assistance), Part Intake System (web part capture), Marketing Presentation Generator, Executive Sales/Procurement Dashboards with AI Copilot, and Leads Management with AI-powered sales engagement.
- **Quality & Compliance**: Duplicate Detection System with multiple algorithms and configurable thresholds.
- **Core Tracking**: Core Due Days tracking for exchange orders.
- **Dynamic Material Issue Module**: High-performance multi-material issuance with real-time inventory validation.
- **Unplanned Receipt Module**: Registration and controlled management of items arriving without documentation, supporting full lifecycle tracking from intake through inventory conversion or work order processing, with role-based approvals and complete audit trail.
- **Inventory Split Function**: Ability to split inventory records into multiple records for the same product, enabling flexible location/condition management.
- **Document Template & Form Management Module**: Enterprise-grade document template system with version control, dynamic tokens, and terms library. Supports 10 document types (Work Order, Quote, Sales Order, Invoice, Purchase Order, Packing Slip, RMA, Certificate, RFQ, Receiving) with customizable headers/footers, template activation workflow, cloning, and role-based access control.
- **Permissions Manager**: Section-based permissions system with section-level visibility toggles (hide entire sidebar sections), individual page access controls, and functional CRUD permissions. Features user selector dropdown, two-tab layout (Menu & Sections / Feature Permissions), sticky save bar with change counter, and instant client-side switching between users. Section keys: section_executive, section_mro, section_sales, section_procurement, section_operations, section_labor, section_ndt, section_service, section_quality, section_shipping, section_accounting, section_reports.
- **ASC-AI (Autonomous System Correction) Engine**: Self-healing production system that detects, diagnoses, and automatically corrects errors with quarantine oversight for high-risk corrections:
  - **Anomaly Detection**: HTTP errors, database failures, FK violations, partial commits, ledger imbalances, duplicate transactions
  - **Root Cause Analysis**: Dependency graph construction to trace error origins
  - **Auto-Correction**: Safe corrections applied when confidence >= 0.90 and entity is non-financial/compliance
  - **Quarantine System**: Financial and compliance entities always quarantined for human review
  - **Immutable Audit Log**: SHA-256 checksums, chain validation, complete before/after state capture
  - **Transaction Guard**: Decorator for wrapping critical operations with auto-rollback
  - **Admin Console**: Dashboard at `/asc-ai/dashboard` for viewing anomalies, corrections, quarantine items, and system health

## External Dependencies

-   **Python Packages**: `Flask`, `Flask-Login`, `Werkzeug`, `ReportLab`, `Pandas`, `openpyxl`, `openai`, `psycopg2-binary`.
-   **Frontend Libraries**: Bootstrap 5.3.0, Bootstrap Icons 1.11.0, Chart.js 4.4.0.
-   **AI Integration**: OpenAI API (GPT-4o).
-   **Database**: SQLite (for development), PostgreSQL (for production).