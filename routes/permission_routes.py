from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import User
from auth import login_required, role_required

permission_bp = Blueprint('permission_routes', __name__, url_prefix='/permissions')

AVAILABLE_PERMISSIONS = {
    'menu_access': {
        'menu_dashboard': 'Dashboard',
        'menu_executive_dashboard': 'Executive Dashboard',
        'menu_operations_dashboard': 'Operations Dashboard',
        'menu_products': 'Products',
        'menu_part_analyzer': 'Part Analyzer',
        'menu_capabilities': 'MRO Capabilities',
        'menu_market_analysis': 'Market Analysis',
        'menu_inventory': 'Inventory',
        'menu_work_orders': 'Work Orders',
        'menu_master_routings': 'Master Routings',
        'menu_task_templates': 'Task Templates',
        'menu_clock_station': 'Clock Station',
        'menu_tools': 'Tools Management',
        'menu_ndt': 'NDT Operations',
        'menu_qms': 'Quality Management',
        'menu_customers': 'Customers',
        'menu_customer_service': 'Customer Service AI',
        'menu_sales_orders': 'Sales Orders',
        'menu_exchange': 'Exchange Management',
        'menu_quotes': 'Quotes',
        'menu_suppliers': 'Suppliers',
        'menu_supplier_discovery': 'AI Supplier Discovery',
        'menu_rfq': 'RFQ Management',
        'menu_purchase_orders': 'Purchase Orders',
        'menu_repair_orders': 'Repair Orders',
        'menu_shipping': 'Shipping',
        'menu_receiving': 'Receiving',
        'menu_invoices': 'Invoices',
        'menu_labor_resources': 'Labor Resources',
        'menu_skillsets': 'Skillsets',
        'menu_time_tracking': 'Time Tracking',
        'menu_accounting': 'Chart of Accounts',
        'menu_journal_entries': 'Journal Entries',
        'menu_ap': 'Accounts Payable',
        'menu_financial_reports': 'Financial Reports',
        'menu_reports': 'Reports',
        'menu_contacts': 'Contacts',
        'menu_erp_copilot': 'ERP Copilot',
        'menu_master_scheduler': 'AI Master Scheduler',
        'menu_capacity_planning': 'Capacity Planning',
        'menu_business_analytics': 'Business Analytics AI',
        'menu_it_manager': 'IT Manager AI',
        'menu_org_analyzer': 'Organizational Analyzer',
        'menu_financial_analyzer': 'Financial Analyzer',
        'menu_marketing_generator': 'Marketing Generator',
        'menu_users': 'Users Management',
        'menu_permissions': 'Permissions',
        'menu_uom': 'Units of Measure',
        'menu_audit_trail': 'Audit Trail',
        'menu_settings': 'Settings',
    },
    'products': {
        'view_products': 'View Products',
        'create_products': 'Create Products',
        'edit_products': 'Edit Products',
        'delete_products': 'Delete Products',
        'mass_update_products': 'Mass Update Products',
    },
    'bom': {
        'view_bom': 'View BOMs',
        'create_bom': 'Create BOMs',
        'edit_bom': 'Edit BOMs',
        'delete_bom': 'Delete BOMs',
    },
    'inventory': {
        'view_inventory': 'View Inventory',
        'adjust_inventory': 'Adjust Inventory',
        'issue_materials': 'Issue Materials',
        'receive_inventory': 'Receive Inventory',
    },
    'work_orders': {
        'view_work_orders': 'View Work Orders',
        'create_work_orders': 'Create Work Orders',
        'edit_work_orders': 'Edit Work Orders',
        'delete_work_orders': 'Delete Work Orders',
        'process_work_orders': 'Process Work Orders',
        'manage_wo_quotes': 'Manage Work Order Quotes',
    },
    'master_routings': {
        'view_master_routings': 'View Master Routings',
        'create_master_routings': 'Create Master Routings',
        'edit_master_routings': 'Edit Master Routings',
        'approve_master_routings': 'Approve Master Routings',
    },
    'purchase_orders': {
        'view_purchase_orders': 'View Purchase Orders',
        'create_purchase_orders': 'Create Purchase Orders',
        'edit_purchase_orders': 'Edit Purchase Orders',
        'delete_purchase_orders': 'Delete Purchase Orders',
        'approve_purchase_orders': 'Approve Purchase Orders',
    },
    'sales_orders': {
        'view_sales_orders': 'View Sales Orders',
        'create_sales_orders': 'Create Sales Orders',
        'edit_sales_orders': 'Edit Sales Orders',
        'delete_sales_orders': 'Delete Sales Orders',
        'manage_exchange': 'Manage Exchange Orders',
    },
    'customers': {
        'view_customers': 'View Customers',
        'create_customers': 'Create Customers',
        'edit_customers': 'Edit Customers',
        'delete_customers': 'Delete Customers',
    },
    'suppliers': {
        'view_suppliers': 'View Suppliers',
        'create_suppliers': 'Create Suppliers',
        'edit_suppliers': 'Edit Suppliers',
        'delete_suppliers': 'Delete Suppliers',
    },
    'rfq': {
        'view_rfq': 'View RFQs',
        'create_rfq': 'Create RFQs',
        'edit_rfq': 'Edit RFQs',
        'delete_rfq': 'Delete RFQs',
    },
    'shipping_receiving': {
        'view_shipments': 'View Shipments',
        'create_shipments': 'Create Shipments',
        'process_receiving': 'Process Receiving',
        'generate_shipping_docs': 'Generate Shipping Documents',
    },
    'invoices': {
        'view_invoices': 'View Invoices',
        'create_invoices': 'Create Invoices',
        'edit_invoices': 'Edit Invoices',
        'delete_invoices': 'Delete Invoices',
    },
    'repair_orders': {
        'view_repair_orders': 'View Repair Orders',
        'create_repair_orders': 'Create Repair Orders',
        'edit_repair_orders': 'Edit Repair Orders',
        'delete_repair_orders': 'Delete Repair Orders',
    },
    'tools': {
        'view_tools': 'View Tools',
        'create_tools': 'Create Tools',
        'edit_tools': 'Edit Tools',
        'manage_calibration': 'Manage Calibration',
    },
    'mro_capabilities': {
        'view_capabilities': 'View MRO Capabilities',
        'create_capabilities': 'Create Capabilities',
        'edit_capabilities': 'Edit Capabilities',
        'market_analysis': 'Access Market Analysis',
    },
    'ndt': {
        'view_ndt': 'View NDT Operations',
        'create_ndt': 'Create NDT Records',
        'edit_ndt': 'Edit NDT Records',
        'certify_ndt': 'Certify NDT Results',
    },
    'qms': {
        'view_qms': 'View QMS Documents',
        'create_qms': 'Create QMS Documents',
        'edit_qms': 'Edit QMS Documents',
        'approve_qms': 'Approve QMS Documents',
    },
    'labor_resources': {
        'view_labor': 'View Labor Resources',
        'manage_labor': 'Manage Labor Resources',
        'manage_skillsets': 'Manage Skillsets',
        'view_time_tracking': 'View Time Tracking',
        'clock_in_out': 'Clock In/Out',
    },
    'accounting': {
        'view_accounting': 'View Accounting',
        'manage_chart_of_accounts': 'Manage Chart of Accounts',
        'create_journal_entries': 'Create Journal Entries',
        'view_financial_reports': 'View Financial Reports',
        'manage_ap': 'Manage Accounts Payable',
    },
    'reports': {
        'view_reports': 'View Reports',
        'export_reports': 'Export Reports',
        'view_executive_dashboard': 'View Executive Dashboard',
        'view_operations_dashboard': 'View Operations Dashboard',
    },
    'ai_modules': {
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
    },
    'users': {
        'view_users': 'View Users',
        'manage_users': 'Manage Users',
        'manage_permissions': 'Manage Permissions',
    },
    'system': {
        'view_audit_trail': 'View Audit Trail',
        'manage_settings': 'Manage System Settings',
        'manage_uom': 'Manage Units of Measure',
        'manage_task_templates': 'Manage Task Templates',
        'salesforce_migration': 'Salesforce Data Migration',
    }
}

@permission_bp.route('/')
@login_required
@role_required('Admin')
def list_permissions():
    users = User.get_all_with_permissions()
    return render_template('permissions/list.html', 
                         users=users, 
                         available_permissions=AVAILABLE_PERMISSIONS)

@permission_bp.route('/update/<int:user_id>', methods=['POST'])
@login_required
@role_required('Admin')
def update_permissions(user_id):
    user = User.get_by_id(user_id)
    if not user:
        flash('User not found', 'danger')
        return redirect(url_for('permission_routes.list_permissions'))
    
    for category in AVAILABLE_PERMISSIONS.values():
        for perm_key in category.keys():
            perm_value = 1 if perm_key in request.form else 0
            User.set_permission(user_id, perm_key, perm_value)
    
    flash(f'Permissions updated for {user["username"]}', 'success')
    return redirect(url_for('permission_routes.list_permissions'))
