from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import User
from auth import login_required, role_required

user_bp = Blueprint('user_routes', __name__)

@user_bp.route('/users')
@role_required('Admin')
def list_users():
    users = User.get_all()
    return render_template('users/list.html', users=users)

@user_bp.route('/users/<int:user_id>/update-role', methods=['POST'])
@role_required('Admin')
def update_user_role(user_id):
    new_role = request.form.get('role')
    
    if new_role not in ['Admin', 'Planner', 'Production Staff', 'Procurement']:
        flash('Invalid role selected.', 'danger')
        return redirect(url_for('user_routes.list_users'))
    
    try:
        User.update_role(user_id, new_role)
        flash('User role updated successfully!', 'success')
    except Exception as e:
        flash(f'Error updating role: {str(e)}', 'danger')
    
    return redirect(url_for('user_routes.list_users'))

@user_bp.route('/users/<int:user_id>/toggle-active', methods=['POST'])
@role_required('Admin')
def toggle_user_active(user_id):
    if user_id == session.get('user_id'):
        flash('You cannot deactivate your own account.', 'danger')
        return redirect(url_for('user_routes.list_users'))
    
    is_active = request.form.get('is_active') == '1'
    
    try:
        User.toggle_active(user_id, is_active)
        status = 'activated' if is_active else 'deactivated'
        flash(f'User {status} successfully!', 'success')
    except Exception as e:
        flash(f'Error updating user status: {str(e)}', 'danger')
    
    return redirect(url_for('user_routes.list_users'))
