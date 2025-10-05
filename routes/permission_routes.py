from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import User
from auth import login_required, role_required

permission_bp = Blueprint('permission_routes', __name__, url_prefix='/permissions')

AVAILABLE_PERMISSIONS = {
    'products': {
        'view_products': 'View Products',
        'create_products': 'Create Products',
        'edit_products': 'Edit Products',
        'delete_products': 'Delete Products',
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
    },
    'work_orders': {
        'view_work_orders': 'View Work Orders',
        'create_work_orders': 'Create Work Orders',
        'edit_work_orders': 'Edit Work Orders',
        'delete_work_orders': 'Delete Work Orders',
        'process_work_orders': 'Process Work Orders',
    },
    'purchase_orders': {
        'view_purchase_orders': 'View Purchase Orders',
        'create_purchase_orders': 'Create Purchase Orders',
        'edit_purchase_orders': 'Edit Purchase Orders',
        'delete_purchase_orders': 'Delete Purchase Orders',
    },
    'suppliers': {
        'view_suppliers': 'View Suppliers',
        'create_suppliers': 'Create Suppliers',
        'edit_suppliers': 'Edit Suppliers',
        'delete_suppliers': 'Delete Suppliers',
    },
    'reports': {
        'view_reports': 'View Reports',
        'export_reports': 'Export Reports',
    },
    'users': {
        'view_users': 'View Users',
        'manage_users': 'Manage Users',
        'manage_permissions': 'Manage Permissions',
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
