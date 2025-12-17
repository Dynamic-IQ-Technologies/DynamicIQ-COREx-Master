## Overview

Dynamic.IQ-MRPx is a comprehensive Flask-based Manufacturing Resource Planning (MRP) system designed to optimize production processes, inventory, Bill of Materials (BOM), work orders, and purchase orders. It offers robust role-based access control, efficient material tracking, production planning, supplier management, and report generation. The system aims to enhance operational efficiency, deliver critical business insights, and includes advanced AI-driven modules for strategic insights and automation in supplier discovery and market/capability analysis.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### UI/UX Decisions

The system features a professional, elegant user interface built on Bootstrap 5, Bootstrap Icons, and custom CSS design tokens. It utilizes the Inter font family and a refined slate-based color palette for a cohesive executive look. Key UI elements include an Executive Dashboard with KPI cards, Chart.js visualizations, and responsive grid layouts.

### Technical Implementations

The backend is developed with Flask, using Blueprints and an SQLite database. It implements session-based authentication and comprehensive role-based access control. The `MRPEngine` handles core MRP logic. An Audit Trail System logs CUD operations. The frontend uses Jinja2 for templating.

**Key Modules and Features:**
-   **Core MRP**: Products & BOM, Inventory, Work Orders, Purchase Orders, Task Templates.
-   **Supply Chain & Sales**: RFQ Module, Contact Management, Sales Module, Shipping & Receiving, Invoice Management, Sales Order Exchange Management.
-   **Asset & Service Management**: Tools Management, Service Management, MRO Capabilities Management.
-   **Quality & Compliance**: NDT Module, Quality Management System (QMS).
-   **AI-Powered Modules**: AI Supplier Discovery, Market & Capability Analysis, Capacity Planning Module, Customer Service Module, Organizational Analyzer, Financial Analyzer, ERP Copilot (AI Helper), AI Super Master Scheduler, Part Analyzer, Business Analytics AI Super Agent, Secure IT Manager AI Super Agent.
-   **Integration**: Salesforce Data Migration Agent.

### Patent-Eligible Architecture (December 2025)

The system implements a novel patent-eligible architecture for ERP exchange management with the following components:

**1. Exchange Dependency Graph Engine** (`engines/exchange_graph.py`)
-   DAG-based data structure with O(1) node lookup and O(V+E) traversal
-   Hash-linked nodes for integrity verification
-   BFS dependency resolution for upstream/downstream analysis
-   Cached traversal results for performance optimization

**2. Deterministic Event Processing Engine** (`engines/event_engine.py`)
-   Idempotent event processing with idempotency key tracking
-   Hash-chained events for tamper detection and replay verification
-   Event sourcing for deterministic state reconstruction
-   Handler registration for event-driven processing

**3. AI Execution Path Modifier** (`engines/ai_executor.py`)
-   Risk vector computation from historical event patterns
-   CONCRETE execution path modifications (not recommendations):
  - Priority queue reordering for high-risk chains
  - Cache preloading decisions based on access predictions
  - Lock escalation for critical operations
  - Resource allocation adjustments
-   Effectiveness tracking for continuous learning

**4. Performance Instrumentation System** (`engines/performance_profiler.py`)
-   Latency measurement with before/after baseline comparison
-   Cache hit ratio tracking
-   Query reduction metrics
-   Comprehensive performance reporting

**5. Cryptographic Security Layer** (`security_utils/crypto.py`)
-   HMAC-SHA256 based access key generation
-   Role-scoped cryptographic access control
-   Tamper-evident audit trails with hash chain verification
-   Integrity verification with corruption detection

**6. Exchange Orchestrator** (`engines/orchestrator.py`)
-   Unified integration layer connecting all engines
-   Demonstrates patentable method:
  1. AI analyzes risk from historical event patterns
  2. System behavior is MODIFIED based on predictions
  3. Events are processed through cryptographic verification
  4. Operations are instrumented for improvement evidence

### System Design Choices

-   **Inventory Management**: Real-time stock level tracking, low stock alerts, and automatic updates.
-   **Reporting System**: Provides various reports including inventory valuation, work order cost analysis, and material requirements.
-   **Accounting System**: Features a Chart of Accounts (COA), General Ledger (GL), Manual Journal Entries, automatic GL posting, and financial reports.
-   **Time Clock Station**: Dedicated employee time tracking system with simplified employee code authentication and automatic labor cost tracking.
-   **Labor Resources & Skillset Management**: Comprehensive multi-skillset assignment system for tracking employee competencies and strategic capacity planning.

## External Dependencies

-   **Python Packages**: `Flask`, `Flask-Login`, `Werkzeug`, `ReportLab`, `Pandas`, `openpyxl`, `openai`, `sqlite3`.
-   **Frontend Libraries**: Bootstrap 5.3.0, Bootstrap Icons 1.11.0, Chart.js 4.4.0.
-   **AI Integration**: OpenAI API (GPT-4o).
-   **Database**: SQLite (`mrp.db`).
-   **Environment Variables**: `SESSION_SECRET`.