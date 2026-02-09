from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import User
from auth import login_required, role_required

permission_bp = Blueprint('permission_routes', __name__, url_prefix='/permissions')

SIDEBAR_SECTIONS = [
    {
        'key': 'section_executive',
        'label': 'Executive',
        'icon': 'bi-briefcase',
        'pages': [
            {'key': 'menu_dashboard', 'label': 'Dashboard', 'icon': 'bi-speedometer2'},
            {'key': 'menu_org_analyzer', 'label': 'Org Analyzer', 'icon': 'bi-bar-chart-line'},
            {'key': 'menu_neuroiq', 'label': 'COREx NeuroIQ', 'icon': 'bi-stars'},
        ]
    },
    {
        'key': 'section_mro',
        'label': 'Product Management',
        'icon': 'bi-airplane-engines',
        'pages': [
            {'key': 'menu_capabilities', 'label': 'Capabilities', 'icon': 'bi-gear-wide-connected'},
            {'key': 'menu_market_analysis', 'label': 'Market Analysis', 'icon': 'bi-graph-up-arrow'},
            {'key': 'menu_products', 'label': 'Products', 'icon': 'bi-box'},
            {'key': 'menu_part_analyzer', 'label': 'Part Analyzer', 'icon': 'bi-search-heart'},
            {'key': 'menu_bom', 'label': 'Bill of Materials', 'icon': 'bi-diagram-3'},
        ]
    },
    {
        'key': 'section_sales',
        'label': 'Sales',
        'icon': 'bi-cash-coin',
        'pages': [
            {'key': 'menu_sales_orders', 'label': 'Sales Orders / Executive Dashboard', 'icon': 'bi-receipt'},
            {'key': 'menu_leads', 'label': 'Leads', 'icon': 'bi-person-lines-fill'},
            {'key': 'menu_customers', 'label': 'Customers', 'icon': 'bi-people'},
            {'key': 'menu_customer_service', 'label': 'Customer Service', 'icon': 'bi-headset'},
            {'key': 'menu_exchange', 'label': 'Exchange Management', 'icon': 'bi-arrow-left-right'},
            {'key': 'menu_quotes', 'label': 'Quotes', 'icon': 'bi-file-earmark-text'},
        ]
    },
    {
        'key': 'section_procurement',
        'label': 'Procurement',
        'icon': 'bi-cart4',
        'pages': [
            {'key': 'menu_suppliers', 'label': 'Suppliers', 'icon': 'bi-truck'},
            {'key': 'menu_purchase_orders', 'label': 'Purchase Orders', 'icon': 'bi-cart'},
            {'key': 'menu_receiving', 'label': 'Receiving', 'icon': 'bi-inbox-fill'},
            {'key': 'menu_inventory', 'label': 'Inventory', 'icon': 'bi-boxes'},
            {'key': 'menu_supplier_discovery', 'label': 'AI Supplier Discovery', 'icon': 'bi-robot'},
            {'key': 'menu_rfq', 'label': 'RFQs', 'icon': 'bi-file-earmark-text'},
            {'key': 'menu_tools', 'label': 'Tools', 'icon': 'bi-wrench'},
        ]
    },
    {
        'key': 'section_operations',
        'label': 'Operations',
        'icon': 'bi-gear-wide-connected',
        'pages': [
            {'key': 'menu_operations_dashboard', 'label': 'Operations Dashboard', 'icon': 'bi-speedometer2'},
            {'key': 'menu_work_orders', 'label': 'Work Orders', 'icon': 'bi-clipboard-check'},
            {'key': 'menu_task_templates', 'label': 'Task Templates', 'icon': 'bi-file-earmark-text'},
            {'key': 'menu_master_routings', 'label': 'Master Routings', 'icon': 'bi-diagram-3'},
            {'key': 'menu_capacity_planning', 'label': 'Capacity Planning', 'icon': 'bi-calendar3-range'},
            {'key': 'menu_master_scheduler', 'label': 'Master Scheduler', 'icon': 'bi-calendar-week'},
        ]
    },
    {
        'key': 'section_labor',
        'label': 'Labor Tracker',
        'icon': 'bi-person-badge',
        'pages': [
            {'key': 'menu_clock_station', 'label': 'Clock Station', 'icon': 'bi-clock-fill'},
            {'key': 'menu_time_tracking', 'label': 'Time Reports', 'icon': 'bi-clipboard-data'},
            {'key': 'menu_labor_resources', 'label': 'Labor Resources', 'icon': 'bi-people'},
            {'key': 'menu_skillsets', 'label': 'Skillsets', 'icon': 'bi-award'},
        ]
    },
    {
        'key': 'section_ndt',
        'label': 'NDT',
        'icon': 'bi-radioactive',
        'pages': [
            {'key': 'menu_ndt', 'label': 'NDT Operations', 'icon': 'bi-radioactive'},
        ]
    },
    {
        'key': 'section_service',
        'label': 'Service Management',
        'icon': 'bi-wrench-adjustable-circle',
        'pages': [
            {'key': 'menu_repair_orders', 'label': 'Repair Orders (MRO)', 'icon': 'bi-tools'},
        ]
    },
    {
        'key': 'section_quality',
        'label': 'Quality',
        'icon': 'bi-shield-check',
        'pages': [
            {'key': 'menu_qms', 'label': 'Quality Management', 'icon': 'bi-shield-check'},
        ]
    },
    {
        'key': 'section_shipping',
        'label': 'Shipping & Receiving',
        'icon': 'bi-box-seam',
        'pages': [
            {'key': 'menu_shipping', 'label': 'Shipping & Receiving', 'icon': 'bi-truck'},
        ]
    },
    {
        'key': 'section_accounting',
        'label': 'Accounting',
        'icon': 'bi-calculator',
        'pages': [
            {'key': 'menu_executive_dashboard', 'label': 'Executive Dashboard', 'icon': 'bi-graph-up-arrow'},
            {'key': 'menu_financial_analyzer', 'label': 'Financial Analyzer', 'icon': 'bi-currency-dollar'},
            {'key': 'menu_accounting', 'label': 'Chart of Accounts / GL', 'icon': 'bi-list-ul'},
            {'key': 'menu_journal_entries', 'label': 'Journal Entries', 'icon': 'bi-journal-text'},
            {'key': 'menu_ap', 'label': 'A/P Management', 'icon': 'bi-receipt'},
            {'key': 'menu_financial_reports', 'label': 'Financial Reports', 'icon': 'bi-file-earmark-text'},
            {'key': 'menu_business_analytics', 'label': 'AI Analytics Agent', 'icon': 'bi-robot'},
            {'key': 'menu_invoices', 'label': 'Invoices', 'icon': 'bi-receipt-cutoff'},
        ]
    },
    {
        'key': 'section_reports',
        'label': 'Reports',
        'icon': 'bi-file-earmark-bar-graph',
        'pages': [
            {'key': 'menu_reports', 'label': 'Reports', 'icon': 'bi-file-earmark-bar-graph'},
        ]
    },
]

FUNCTIONAL_PERMISSIONS = {
    'products': {
        'label': 'Products',
        'icon': 'bi-box',
        'perms': {
            'view_products': 'View Products',
            'create_products': 'Create Products',
            'edit_products': 'Edit Products',
            'delete_products': 'Delete Products',
            'mass_update_products': 'Mass Update Products',
        }
    },
    'bom': {
        'label': 'Bill of Materials',
        'icon': 'bi-diagram-3',
        'perms': {
            'view_bom': 'View BOMs',
            'create_bom': 'Create BOMs',
            'edit_bom': 'Edit BOMs',
            'delete_bom': 'Delete BOMs',
        }
    },
    'inventory': {
        'label': 'Inventory',
        'icon': 'bi-boxes',
        'perms': {
            'view_inventory': 'View Inventory',
            'adjust_inventory': 'Adjust Inventory',
            'issue_materials': 'Issue Materials',
            'receive_inventory': 'Receive Inventory',
        }
    },
    'work_orders': {
        'label': 'Work Orders',
        'icon': 'bi-clipboard-check',
        'perms': {
            'view_work_orders': 'View Work Orders',
            'create_work_orders': 'Create Work Orders',
            'edit_work_orders': 'Edit Work Orders',
            'delete_work_orders': 'Delete Work Orders',
            'process_work_orders': 'Process Work Orders',
            'manage_wo_quotes': 'Manage Work Order Quotes',
        }
    },
    'master_routings': {
        'label': 'Master Routings',
        'icon': 'bi-diagram-3',
        'perms': {
            'view_master_routings': 'View Master Routings',
            'create_master_routings': 'Create Master Routings',
            'edit_master_routings': 'Edit Master Routings',
            'approve_master_routings': 'Approve Master Routings',
        }
    },
    'purchase_orders': {
        'label': 'Purchase Orders',
        'icon': 'bi-cart',
        'perms': {
            'view_purchase_orders': 'View Purchase Orders',
            'create_purchase_orders': 'Create Purchase Orders',
            'edit_purchase_orders': 'Edit Purchase Orders',
            'delete_purchase_orders': 'Delete Purchase Orders',
            'approve_purchase_orders': 'Approve Purchase Orders',
        }
    },
    'sales_orders': {
        'label': 'Sales Orders',
        'icon': 'bi-receipt',
        'perms': {
            'view_sales_orders': 'View Sales Orders',
            'create_sales_orders': 'Create Sales Orders',
            'edit_sales_orders': 'Edit Sales Orders',
            'delete_sales_orders': 'Delete Sales Orders',
            'manage_exchange': 'Manage Exchange Orders',
        }
    },
    'customers': {
        'label': 'Customers',
        'icon': 'bi-people',
        'perms': {
            'view_customers': 'View Customers',
            'create_customers': 'Create Customers',
            'edit_customers': 'Edit Customers',
            'delete_customers': 'Delete Customers',
        }
    },
    'suppliers': {
        'label': 'Suppliers',
        'icon': 'bi-truck',
        'perms': {
            'view_suppliers': 'View Suppliers',
            'create_suppliers': 'Create Suppliers',
            'edit_suppliers': 'Edit Suppliers',
            'delete_suppliers': 'Delete Suppliers',
        }
    },
    'rfq': {
        'label': 'RFQs',
        'icon': 'bi-file-earmark-text',
        'perms': {
            'view_rfq': 'View RFQs',
            'create_rfq': 'Create RFQs',
            'edit_rfq': 'Edit RFQs',
            'delete_rfq': 'Delete RFQs',
        }
    },
    'shipping_receiving': {
        'label': 'Shipping & Receiving',
        'icon': 'bi-box-seam',
        'perms': {
            'view_shipments': 'View Shipments',
            'create_shipments': 'Create Shipments',
            'process_receiving': 'Process Receiving',
            'generate_shipping_docs': 'Generate Shipping Documents',
        }
    },
    'invoices': {
        'label': 'Invoices',
        'icon': 'bi-receipt-cutoff',
        'perms': {
            'view_invoices': 'View Invoices',
            'create_invoices': 'Create Invoices',
            'edit_invoices': 'Edit Invoices',
            'delete_invoices': 'Delete Invoices',
        }
    },
    'repair_orders': {
        'label': 'Repair Orders',
        'icon': 'bi-tools',
        'perms': {
            'view_repair_orders': 'View Repair Orders',
            'create_repair_orders': 'Create Repair Orders',
            'edit_repair_orders': 'Edit Repair Orders',
            'delete_repair_orders': 'Delete Repair Orders',
        }
    },
    'tools': {
        'label': 'Tools',
        'icon': 'bi-wrench',
        'perms': {
            'view_tools': 'View Tools',
            'create_tools': 'Create Tools',
            'edit_tools': 'Edit Tools',
            'manage_calibration': 'Manage Calibration',
        }
    },
    'mro_capabilities': {
        'label': 'MRO Capabilities',
        'icon': 'bi-gear-wide-connected',
        'perms': {
            'view_capabilities': 'View MRO Capabilities',
            'create_capabilities': 'Create Capabilities',
            'edit_capabilities': 'Edit Capabilities',
            'market_analysis': 'Access Market Analysis',
        }
    },
    'ndt': {
        'label': 'NDT',
        'icon': 'bi-radioactive',
        'perms': {
            'view_ndt': 'View NDT Operations',
            'create_ndt': 'Create NDT Records',
            'edit_ndt': 'Edit NDT Records',
            'certify_ndt': 'Certify NDT Results',
        }
    },
    'qms': {
        'label': 'Quality Management',
        'icon': 'bi-shield-check',
        'perms': {
            'view_qms': 'View QMS Documents',
            'create_qms': 'Create QMS Documents',
            'edit_qms': 'Edit QMS Documents',
            'approve_qms': 'Approve QMS Documents',
        }
    },
    'labor_resources': {
        'label': 'Labor Resources',
        'icon': 'bi-person-badge',
        'perms': {
            'view_labor': 'View Labor Resources',
            'manage_labor': 'Manage Labor Resources',
            'manage_skillsets': 'Manage Skillsets',
            'view_time_tracking': 'View Time Tracking',
            'clock_in_out': 'Clock In/Out',
        }
    },
    'accounting': {
        'label': 'Accounting',
        'icon': 'bi-calculator',
        'perms': {
            'view_accounting': 'View Accounting',
            'manage_chart_of_accounts': 'Manage Chart of Accounts',
            'create_journal_entries': 'Create Journal Entries',
            'view_financial_reports': 'View Financial Reports',
            'manage_ap': 'Manage Accounts Payable',
        }
    },
    'reports': {
        'label': 'Reports',
        'icon': 'bi-file-earmark-bar-graph',
        'perms': {
            'view_reports': 'View Reports',
            'export_reports': 'Export Reports',
            'view_executive_dashboard': 'View Executive Dashboard',
            'view_operations_dashboard': 'View Operations Dashboard',
        }
    },
    'ai_modules': {
        'label': 'AI Modules',
        'icon': 'bi-robot',
        'perms': {
            'use_erp_copilot': 'Use ERP Copilot',
            'use_supplier_discovery': 'Use AI Supplier Discovery',
            'use_master_scheduler': 'Use AI Master Scheduler',
            'use_customer_service_ai': 'Use Customer Service AI',
            'use_part_analyzer': 'Use Part Analyzer',
            'use_business_analytics': 'Use Business Analytics AI',
            'use_it_manager': 'Use IT Manager AI',
            'use_financial_analyzer': 'Use Financial Analyzer',
            'use_org_analyzer': 'Use Organizational Analyzer',
            'use_capacity_planning': 'Use Capacity Planning',
            'use_marketing_generator': 'Use Marketing Presentation Generator',
        }
    },
    'users': {
        'label': 'Users',
        'icon': 'bi-people-fill',
        'perms': {
            'view_users': 'View Users',
            'manage_users': 'Manage Users',
            'manage_permissions': 'Manage Permissions',
        }
    },
    'system': {
        'label': 'System',
        'icon': 'bi-sliders',
        'perms': {
            'view_audit_trail': 'View Audit Trail',
            'manage_settings': 'Manage System Settings',
            'manage_uom': 'Manage Units of Measure',
            'manage_task_templates': 'Manage Task Templates',
            'salesforce_migration': 'Salesforce Data Migration',
        }
    },
}

def _get_all_permission_keys():
    keys = []
    for section in SIDEBAR_SECTIONS:
        keys.append(section['key'])
        for page in section['pages']:
            keys.append(page['key'])
    for cat_data in FUNCTIONAL_PERMISSIONS.values():
        for perm_key in cat_data['perms']:
            keys.append(perm_key)
    return keys

AVAILABLE_PERMISSIONS = {}
for section in SIDEBAR_SECTIONS:
    if section['key'] not in AVAILABLE_PERMISSIONS:
        AVAILABLE_PERMISSIONS.setdefault('menu_access', {})[section['key']] = section['label'] + ' (Section)'
    for page in section['pages']:
        AVAILABLE_PERMISSIONS.setdefault('menu_access', {})[page['key']] = page['label']
for cat_key, cat_data in FUNCTIONAL_PERMISSIONS.items():
    AVAILABLE_PERMISSIONS[cat_key] = cat_data['perms']


@permission_bp.route('/')
@login_required
@role_required('Admin')
def list_permissions():
    users = User.get_all_with_permissions()
    return render_template('permissions/list.html', 
                         users=users,
                         sidebar_sections=SIDEBAR_SECTIONS,
                         functional_permissions=FUNCTIONAL_PERMISSIONS)


@permission_bp.route('/user/<int:user_id>')
@login_required
@role_required('Admin')
def get_user_permissions(user_id):
    user = User.get_by_id(user_id)
    if not user:
        return jsonify({'error': 'User not found'}), 404
    permissions = User.get_permissions(user_id)
    return jsonify({'permissions': permissions})


@permission_bp.route('/update/<int:user_id>', methods=['POST'])
@login_required
@role_required('Admin')
def update_permissions(user_id):
    user = User.get_by_id(user_id)
    if not user:
        flash('User not found', 'danger')
        return redirect(url_for('permission_routes.list_permissions'))
    
    all_keys = set(_get_all_permission_keys())
    for perm_key in all_keys:
        perm_value = 1 if perm_key in request.form else 0
        User.set_permission(user_id, perm_key, perm_value)
    
    flash(f'Permissions updated for {user["username"]}', 'success')
    return redirect(url_for('permission_routes.list_permissions'))
