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

The system uses **PostgreSQL for both development and production** to ensure consistent behavior, eliminating compatibility issues. A PostgreSQL compatibility layer translates SQLite functions and handles data type conversions and date arithmetic for seamless operation.

The system implements enterprise-grade error handling with a Global Exception Handler, Request Correlation IDs, Structured Error Responses, and Safe Template Utilities. Production hardening includes environment parity validation, schema validation and drift detection, transaction safety, and pre-insert validation. Health check endpoints provide monitoring for application readiness, database connectivity, transaction capability, and schema consistency. A Production Query Validator tool assists in pre-deployment validation for PostgreSQL compatibility.

Key features include:
- **Inventory Management**: Real-time tracking, alerts, and cost transfer.
- **Work Order Management**: Task-level material requirements, master routing templates.
- **Sales Order Management**: Dual exchange workflow, document generation, email acknowledgements.
- **Purchase Order Management**: Service/misc POs, exchange obligations, supplier portal.
- **Accounting & Reporting**: Chart of Accounts, General Ledger, financial and operational reports with automatic journal entries.
- **Labor Management**: Time clock, skill-based task filtering, resource management.
- **AI-Powered Modules**: COREx NeuroIQ Executive Intelligence System (conversational AI, predictive risk), COREx Guide Transaction Assistant, Part Intake System, Marketing Presentation Generator, AI Copilot dashboards, Leads Management (with QR Code lead capture).
- **AI Supplier Discovery Engine (Precision Mode)**: Aerospace-grade supplier matching with 4-tier hierarchy (Exact/Intelligent Equivalent/Functional Equivalent/Fuzzy), CAGE code/NSN cross-referencing, counterfeit risk screening, supply chain risk intelligence, cost estimation, and alternate part identification.
- **Intelligent Reporting Module**: AI-powered self-service analytics hub with natural language report creation, guided builder, and visualizations.
- **Enterprise Risk Engine (ERE)**: Predictive multi-domain risk intelligence system with risk scoring, cross-domain correlation, predictive analysis, and AI risk briefings.
- **Capability Recommendation Engine**: Auto-recommends capabilities based on product demand history (work orders, sales orders, purchase orders) with demand scoring and one-click conversion to active capabilities.
- **Quality & Compliance**: Duplicate Detection System.
- **Core Tracking**: Core Due Days for exchange orders.
- **Dynamic Material Issue Module**: High-performance multi-material issuance with real-time inventory validation.
- **Unplanned Receipt Module**: Controlled management of items arriving without documentation.
- **Inventory Split Function**: Ability to split inventory records for flexible location/condition management.
- **Enterprise Forensic Intelligence (Traceability Engine)**: Cross-module traceability system to reconstruct complete operational history for any item, including timeline, relationship graph, and cost evolution.
- **Document Template & Form Management Module**: Enterprise-grade document template system with version control, dynamic tokens, and terms library.
- **Permissions Manager**: Section-based permissions system with section-level visibility toggles and individual page access controls.
- **ASC-AI (Autonomous System Correction) Engine**: Self-healing production system for anomaly detection, root cause analysis, and auto-correction with quarantine oversight.
- **10-Layer Security Architecture (Secure IT Manager)**: Industry-leading zero-trust security operating layer with:
  1. Zero Trust Core - Continuous identity verification, device fingerprinting, behavioral biometrics, ephemeral token rotation, context-aware access decisions
  2. AI Threat Engine - Transaction monitoring, behavioral baseline analysis, z-score anomaly detection, lateral movement prevention, silent containment
  3. Polymorphic Architecture - Dynamic endpoint rotation, API signature shuffling, moving target defense, runtime memory protection
  4. Data Security Layer - AES-256 encryption at rest, TLS 1.3 in transit, field-level encryption, tokenization, integrity hashing, data sharding
  5. Supply Chain Hardening - Continuous dependency scanning, signed builds, SBOM enforcement, runtime integrity validation, vendor anomaly monitoring
  6. Active Defense - Honeypots, honeytokens, deception endpoints, intrusion kill-chain detection, attack fingerprinting, geo-intelligence filtering
  7. Self-Healing Infrastructure - Auto session revocation, secret rotation, clean redeployment, integrity revalidation, zero-downtime recovery
  8. Quantum-Ready Encryption - Hybrid classical + post-quantum crypto (CRYSTALS-Kyber), key abstraction layer, crypto-agility framework
  9. Human Risk Mitigation - MFA enforcement, FIDO2/WebAuthn readiness, privileged access timeboxing, JIT elevation, insider threat monitoring
  10. Security Governance - ISO 27001, NIST 800-53, SOC2, CMMC mapping, continuous control validation, AI risk heatmap

## External Dependencies

-   **Python Packages**: `Flask`, `Flask-Login`, `Werkzeug`, `ReportLab`, `Pandas`, `openpyxl`, `openai`, `psycopg2-binary`, `qrcode`, `Pillow`.
-   **Frontend Libraries**: Bootstrap 5.3.0, Bootstrap Icons 1.11.0, Chart.js 4.4.0.
-   **AI Integration**: OpenAI API (GPT-4o).
-   **Database**: PostgreSQL.