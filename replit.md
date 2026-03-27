## Overview

Dynamic.IQ-COREx is a Flask-based enterprise MRP/ERP system designed for aerospace MRO manufacturing and supply chain management. It optimizes production processes, inventory, Bill of Materials (BOM), work orders, purchase orders, and financials. The system provides robust role-based access control, efficient material tracking, production planning, supplier management, and report generation. It includes advanced AI-driven modules for strategic insights, automation in supplier discovery, market/capability analysis, an AI assistant (NeuroIQ), QuickBooks integration readiness, and full email integration via Brevo.

## User Preferences

Preferred communication style: Simple, everyday language.
AI Report Generation: Do not use special characters when generating AI market analysis reports.

## System Architecture

### UI/UX Decisions

The system features a professional, elegant user interface built on Bootstrap 5, Bootstrap Icons, and custom CSS design tokens, utilizing the Inter font family and a slate-based color palette. Key UI elements include an Executive Dashboard with KPI cards, Chart.js visualizations, responsive grid layouts, and a custom professional notification system.

### Technical Implementations

The backend is developed with Flask using Blueprints, implementing session-based authentication and role-based access control. The `MRPEngine` handles core MRP logic, and an Audit Trail System logs CUD operations. The frontend uses Jinja2 for templating. Key modules include Core MRP, Supply Chain & Sales, Asset & Service Management, Quality & Compliance, and various AI-Powered Modules.

The system incorporates a novel architecture for ERP exchange management, comprising an Exchange Dependency Graph Engine, Deterministic Event Processing Engine, AI Execution Path Modifier, Performance Instrumentation System, Cryptographic Security Layer, and an Exchange Orchestrator.

### System Design Choices

The system uses **PostgreSQL for both development and production** to ensure consistent behavior. When `DATABASE_URL` is present (Replit, Railway, or any hosted environment) the app uses PostgreSQL. Key SQL compatibility notes:
- **Translation layer** in `PostgresConnection` automatically converts SQLite syntax to PostgreSQL (julianday→EXTRACT, strftime→TO_CHAR, date('now',...)→CURRENT_DATE±INTERVAL, GROUP_CONCAT→STRING_AGG, INSERT OR IGNORE/REPLACE→ON CONFLICT)
- `?` placeholders are auto-converted to `%s`; subquery aliases injected automatically for bare `FROM (subquery)` patterns
- Four route files (`main_routes`, `executive_routes`, `operations_routes`, `customer_service_routes`) have `USE_POSTGRES = os.environ.get('DATABASE_URL') is not None` which selects native PostgreSQL branches (TO_CHAR, CURRENT_DATE) when DB is PostgreSQL
- PostgreSQL returns DATE columns as `datetime.date` objects — all engines/routes normalise to ISO strings using `_ds()` helpers

**Connection Pooling**: `models.py` maintains a per-process `psycopg2.pool.ThreadedConnectionPool` (min=1, max=6) via the `_get_pg_pool()` singleton. `Database.get_connection()` allocates a connection from this pool; `PostgresConnection.close()` returns it. This keeps Railway's PostgreSQL well within its connection limit across all gunicorn workers.

The system implements enterprise-grade error handling with a Global Exception Handler, Request Correlation IDs, Structured Error Responses, and Safe Template Utilities. Production hardening includes environment parity validation, schema validation and drift detection, transaction safety, and pre-insert validation. Health check endpoints provide monitoring for application readiness, database connectivity, transaction capability, and schema consistency.

### Email Integration — Brevo

All outbound email across the system uses Brevo (formerly Sendinblue) via the `utils/brevo_helper.py` helper:
- `from utils.brevo_helper import get_brevo_credentials` → returns `(api_key, from_email, from_name)`
- Credentials are stored in `company_settings` (DB-first), with `.env` vars as fallback
- The following six routes send email via Brevo: `salesorder`, `invoice`, `customer_service`, `rfq`, `purchaseorder`, `ndt`
- Flash messages for missing credentials direct users to **Company Settings**, not environment variables

## Key Features

- **Inventory Management**: Real-time tracking, alerts, and cost transfer.
- **Work Order Management**:
  - Task-level material requirements and master routing templates
  - **Material shortage indicator**: Red/green dot on the Work Orders list and a status badge on the Work Order detail header. Checks both direct WO lines (`material_requirements` → `material_issues`) and task-level lines (`work_order_task_materials` joined through `work_order_tasks`; NOTE: `work_order_task_materials.work_order_id` is always NULL — join must be via `task_id → work_order_tasks.work_order_id`)
  - **Redesigned list page**: Three Bootstrap tabs — *Work Orders* (full filter + table), *AOG* (red badge count), *Warranty* (green badge count). Tab state persisted in `localStorage`. Inline stage edit syncs across all tabs. Shared Jinja macro renders the common table.
- **Sales Order Management**: Dual exchange workflow, document generation, email acknowledgements.
- **Purchase Order Management**: Service/misc POs, exchange obligations, supplier portal.
- **Accounting & Reporting**: Chart of Accounts, General Ledger, financial and operational reports with automatic journal entries. Auto-invoice generation on shipment with AR/Revenue GL entries.
- **Labor Management**: Time clock, skill-based task filtering, resource management.
- **AI-Powered Modules**: COREx NeuroIQ Executive Intelligence System (conversational AI with streaming — guard `if not chunk.choices: continue`; sentence-queue TTS via `ttsEnqueue`, `ttsReset`, `speakResponse`), COREx Guide Transaction Assistant, Part Intake System, Marketing Presentation Generator, AI Copilot dashboards, Leads Management (with QR Code lead capture).
- **Supply Chain Risk Radar** (`/risk-radar`): AI-scoring engine for supplier and part risk (0-100). Supplier scoring: OTIF 35pts, lead-time variance 20pts, quality incidents 15pts, shortage exposure 20pts, overdue POs 10pts. Part scoring: shortage severity 40pts, supplier risk 30pts, urgency vs lead time 20pts, single-source concentration 10pts. AI narrative via GPT-4o-mini. Async recalculation with polling. Risk badges in MRR and Supplier view. Tables: `supply_risk_profiles`, `risk_signals`, `risk_events`, `risk_score_history`. Supplier rows include "Simulate" button linking to Digital Twin.
- **Digital Twin Simulation Suite** (`/digital-twin`): What-if simulation engine. Syncs live ERP state; runs 4 scenario types (supplier_failure, lead_time_increase, demand_spike, maintenance_deferral). Quantifies downtime hours, revenue impact, parts at risk, blocked WOs; AI executive summary via GPT-4o-mini. Table: `twin_simulations`. Engine: `engines/twin_engine.py`; routes: `routes/twin_routes.py`.
- **AI Supplier Discovery Engine (Precision Mode)**: Aerospace-grade supplier matching with 4-tier hierarchy (Exact/Intelligent Equivalent/Functional Equivalent/Fuzzy), CAGE code/NSN cross-referencing, counterfeit risk screening, supply chain risk intelligence, cost estimation, and alternate part identification.
- **Intelligent Reporting Module**: AI-powered self-service analytics hub with natural language report creation, guided builder, and visualizations.
- **Enterprise Risk Engine (ERE)**: Predictive multi-domain risk intelligence system with risk scoring, cross-domain correlation, predictive analysis, and AI risk briefings.
- **Capability Recommendation Engine**: Auto-recommends capabilities based on product demand history (work orders, sales orders, purchase orders) with demand scoring and one-click conversion to active capabilities.
- **Quality & Compliance**: Duplicate Detection System.
- **Core Tracking**: Core Due Days for exchange orders.
- **Dynamic Material Issue Module**: High-performance multi-material issuance with real-time inventory validation.
- **Unplanned Receipt Module**: Controlled management of items arriving without documentation.
- **Inventory Split Function**: Ability to split inventory records for flexible location/condition management.
- **Enterprise Forensic Intelligence (Traceability Engine)**: Cross-module traceability system reconstructing complete operational history for any item, including timeline, relationship graph, and cost evolution.
- **Predictive Inventory Intelligence**: AI-driven inventory management with quality scoring, automated cycle count scheduling (High=weekly, Medium=monthly, Low=quarterly), demand forecasting, what-if scenario simulation, reorder recommendations, and OpenAI-powered executive summaries. Tables: `inventory_cycle_counts`, `inventory_ai_recommendations`.
- **AI Customer Service Agent**: GPT-4o powered customer communication assistant at `/customer-service/ai-agent`. Generates personalised emails based on order history; supports 8 scenarios. Human review queue with approve/edit/reject workflow; approved emails sent via Brevo. Bulk auto-scan for at-risk customers. Extra columns on `customer_communications`: `ai_generated`, `ai_status`, `ai_context`, `email_body`, `sent_at`.
- **Document Template & Form Management Module**: Enterprise-grade document template system with version control, dynamic tokens, and terms library.
- **Permissions Manager**: Section-based permissions with section-level visibility toggles and individual page access controls.
- **ASC-AI (Autonomous System Correction) Engine**: Self-healing production system for anomaly detection, root cause analysis, and auto-correction with quarantine oversight.
- **Master Scheduler** (`/master-scheduler`): AI-powered Master Production Schedule (MPS) engine (`engines/master_scheduler.py`) with finite-capacity scheduling, constraint detection, Gantt view, capacity load charts, and scenario comparison. Routes: `routes/master_scheduler_routes.py`.

  **PostgreSQL date-compatibility fixes applied (March 2026):**
  - Added `_ds(d)` static helper in `MasterSchedulerEngine` — normalises `datetime.date` / `datetime.datetime` / `str` / `None` to ISO string or `None`
  - `get_demand_orders`: all `due_date` and `start_date` fields wrapped in `_ds()` — prevents sort-key crash (`datetime.date` vs string `'9999-12-31'`)
  - `_calculate_capacity_load`: `planned_start_date` / `planned_end_date` from operations wrapped in `_ds()` before `datetime.strptime()` call
  - Routes helper `_ds()` added in `routes/master_scheduler_routes.py` — applied to `horizon_start/end` passed to engine, and all date fields in JSON responses (`scheduled_start/end`, `original_due_date`, `planned_end_date`)
  - All SQLite functions replaced: `datetime('now')` → `NOW()`, `date('now')` → `CURRENT_DATE`, `date('now', '+7 days')` → `CURRENT_DATE + INTERVAL '7 days'`, `julianday()` expressions → `CURRENT_DATE - date_col`, `date(created_at)` → `created_at::date`

- **10-Layer Security Architecture (Secure IT Manager)**: Industry-leading zero-trust security operating layer with:
  1. Zero Trust Core — continuous identity verification, device fingerprinting, behavioral biometrics, ephemeral token rotation, context-aware access decisions
  2. AI Threat Engine — transaction monitoring, behavioral baseline analysis, z-score anomaly detection, lateral movement prevention, silent containment
  3. Polymorphic Architecture — dynamic endpoint rotation, API signature shuffling, moving target defense, runtime memory protection
  4. Data Security Layer — AES-256 encryption at rest, TLS 1.3 in transit, field-level encryption, tokenization, integrity hashing, data sharding
  5. Supply Chain Hardening — continuous dependency scanning, signed builds, SBOM enforcement, runtime integrity validation, vendor anomaly monitoring
  6. Active Defense — honeypots, honeytokens, deception endpoints, intrusion kill-chain detection, attack fingerprinting, geo-intelligence filtering
  7. Self-Healing Infrastructure — auto session revocation, secret rotation, clean redeployment, integrity revalidation, zero-downtime recovery
  8. Quantum-Ready Encryption — hybrid classical + post-quantum crypto (CRYSTALS-Kyber), key abstraction layer, crypto-agility framework
  9. Human Risk Mitigation — MFA enforcement, FIDO2/WebAuthn readiness, privileged access timeboxing, JIT elevation, insider threat monitoring
  10. Security Governance — ISO 27001, NIST 800-53, SOC2, CMMC mapping, continuous control validation, AI risk heatmap

## Key DB Column Notes (prevent repeat bugs)

- `audit_trail` columns: `id, record_type, record_id, action_type, modified_by, modified_by_name, modified_at, changed_fields, ip_address, user_agent` — NO `action` column, NO `created_at`; use `action_type` and `modified_at`
- `company_settings`: uses `company_name` NOT `name`
- `work_orders`: use `planned_end_date` NOT `due_date`
- `purchase_orders.expected_date`: stored as TEXT — guard `IS NOT NULL AND != ''` then cast `::date`
- `invoices` (AR): has `due_date`; NO `updated_at`
- `sales_order_lines`: total column is `line_total`
- `purchase_order_lines`: total column is `total_price`
- `boms`: child part column is `child_product_id`
- `inventory`: last modified column is `last_updated`
- `ndt_invoices`: has `gl_entry_id INTEGER`
- Invoice tables: AR = `invoices`; AP = `vendor_invoices`; NDT = `ndt_invoices`; suppliers table = `suppliers` NOT `vendors`
- `work_order_task_materials.work_order_id` is ALWAYS NULL — join via `task_id → work_order_tasks.work_order_id`
- PostgreSQL returns DATE columns as `datetime.date` objects — always normalise with `_ds()` before Python string comparisons or JSON serialisation
- JSONB fields: use the `_j(v, default)` helper; never call `json.loads()` directly
- Null guards in templates: ALWAYS `(value or 0) > 0`
- `fetchone()[0]` FAILS on PG dict rows — use `fetchone()['column_name']`
- Always add `conn.rollback()` in except blocks

## Pre-existing Non-Blocking Warnings (safe to ignore)

- `users.password` column missing
- `products.sku` column missing
- `work_orders.work_order_number` column missing
- `sales_orders.order_number` column missing

## External Dependencies

- **Python Packages**: `Flask`, `Flask-Login`, `Werkzeug`, `ReportLab`, `Pandas`, `openpyxl`, `openai`, `psycopg2-binary`, `qrcode`, `Pillow`, `sib-api-v3-sdk` (Brevo).
- **Frontend Libraries**: Bootstrap 5.3.0, Bootstrap Icons 1.11.0, Chart.js 4.4.0.
- **AI Integration**: OpenAI API (GPT-4o, GPT-4o-mini). Pattern: `OpenAI(api_key=..., base_url=...)` — both fields required; use `gpt-4o-mini` for most tasks.
- **Email**: Brevo (sib-api-v3-sdk). Credentials via `utils/brevo_helper.get_brevo_credentials()` — DB-first, env-var fallback.
- **Database**: PostgreSQL (development and production).
- **Secrets in use**: `BREVO_API_KEY`, `BREVO_FROM_EMAIL`.
- **Pending secrets** (not yet provided): `QB_CLIENT_ID`, `QB_CLIENT_SECRET` (QuickBooks integration).

## Accounting Governance & Journal Integrity (GAAP Compliance)

### Central Accounting Engine (`utils/accounting_engine.py`)
All financial transactions must generate a double-entry journal entry automatically. The engine enforces:
- **Debits must equal credits** (tolerance ≤ $0.01) — engine raises `ValueError` if unbalanced
- **Atomic posting** — JE creation and source record update occur in the same transaction
- **Full traceability** — every JE records `transaction_source`, `reference_type`, `reference_id`, `created_by`, `created_at`

### Transaction → Journal Entry Mappings
| Transaction | Debit | Credit |
|---|---|---|
| AR Invoice issued | Accounts Receivable (1120) | Sales Revenue (4100) |
| Customer payment received | Cash (1110) | Accounts Receivable (1120) |
| AP Vendor Invoice | Inventory (1130) or Expense | Accounts Payable (2110) |
| AP Vendor Payment | Accounts Payable (2110) | Cash (1110) |
| Inventory Receipt | Inventory (1130) | Accounts Payable (2110) |
| WIP Issuance | WIP (1140) | Inventory (1130) |
| WO Completion | Finished Goods (1150) | WIP (1140) |
| COGS | COGS (5000) | Inventory (1130) |
| Inventory Write-Up | Inventory (1130) | Other Income (4300) |
| Inventory Write-Down | COGS (5000) | Inventory (1130) |

### Public Functions
- `post_ar_invoice(conn, invoice_id, user_id)` — creates AR-INV-xxxxxx JE
- `post_ap_invoice(conn, vendor_invoice_id, user_id, expense_account_code=None)` — creates AP-INV-xxxxxx JE
- `post_ar_payment(conn, invoice_id, amount, date, method, ref, user_id)` — creates AR-PAY-xxxxxx JE
- `post_ap_payment(conn, vi_id, amount, date, method, ref, user_id)` — creates AP-PAY-xxxxxx JE
- `integrity_check(conn)` — returns dict with orphan transactions, unbalanced JEs, empty JE headers
- `backfill_missing_je(conn, user_id)` — generates missing JEs for all historical transactions

### Auto-JE Hooks (wired into routes)
- **`purchaseorder_routes.py`**: quick-receive, exchange-PO-receive, service-PO-receive, component-buyout-receive — all link `gl_entry_id` to vendor_invoice on creation
- **`invoice_routes.py`**: `post_invoice()` and `record_payment()` — `lastrowid` bugs fixed to use cursor properly
- **`ap_routes.py`**: `record_payment()` — `lastrowid` bug fixed
- **`models.py` GLAutoPost**: `SELECT last_insert_rowid()` replaced with `cursor.lastrowid`

### GL Integrity Dashboard
- **Route**: `GET /accounting/gl-integrity` — shows per-category violation counts, lists of offending records, backfill button
- **API**: `POST /api/accounting/backfill-je` — generates missing JEs for all AR/AP invoices; returns created/errors JSON
- **API**: `GET /api/accounting/integrity-check` — returns full integrity report as JSON
- **Sidebar link**: "GL Integrity Check" under Accounting section (shield-check icon)

### Backfill Status (run March 2026)
10 historical JEs created: 4 AR invoices + 6 AP vendor invoices. Post-backfill: **0 violations**, all 22 GL entries balanced.

## QB AP Sync (Accounts Payable → QuickBooks Bills)
- **`qb_ap_bill_map`** table tracks each `vendor_invoice_id → QB Bill ID` mapping with `sync_status`, `last_synced_at`, `qb_bill_number`, `qb_total_amount`.
- **`auto_sync_ap`** boolean column added to `qb_sync_config`; saved/loaded in the settings panel.
- **`sync_vendor_invoice_to_qb(conn, vendor_invoice_id, trigger)`** — core sync function: resolves/creates QB Vendor by `DisplayName`, builds Bill payload, creates on first push and sparse-updates on re-sync.
- **`_sync_unsynced_vendor_invoices(conn)`** — nightly batch: finds all `vendor_invoices` not yet in `qb_ap_bill_map` and pushes them.
- **Background scheduler** (`_qb_auto_pull_loop`) now also calls `_sync_unsynced_vendor_invoices` nightly (00:00–01:00) when `auto_sync_ap` is enabled.
- **API endpoints**: `POST /api/qb/sync-vendor-invoice/<id>` (single), `POST /api/qb/sync-ap/all` (batch), `GET /api/qb/ap-sync-status` (list with sync state).
- **QB Dashboard**: AP Sync panel card, "AP Bills" tab with per-row Sync buttons and bulk Sync All button, `autoSyncAp` toggle in Sync Settings.
