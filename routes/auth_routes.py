from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import User, Database
from datetime import datetime

auth_bp = Blueprint('auth_routes', __name__)

def log_access_event(user_id, username, action_type, success, details=None):
    """Log access events to IT access audit table"""
    try:
        db = Database()
        conn = db.get_connection()
        
        ip_address = request.remote_addr if request else 'unknown'
        
        conn.execute('''
            INSERT INTO it_access_audit (user_id, username, action_type, success, ip_address, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, username, action_type, 1 if success else 0, ip_address, details, datetime.now()))
        
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error logging access event: {e}")

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        user = User.get_by_username(username)
        
        if user and User.verify_password(user, password):
            session['user_id'] = user['id']
            session['username'] = user['username']
            session['role'] = user['role']
            User.update_last_login(user['id'])
            log_access_event(user['id'], username, 'login', True, 'Successful login')
            flash(f'Welcome back, {user["username"]}!', 'success')
            return redirect(url_for('main_routes.dashboard'))
        else:
            log_access_event(None, username, 'login', False, 'Invalid credentials')
            flash('Invalid username or password', 'danger')
    
    return render_template('login.html')

@auth_bp.route('/logout')
def logout():
    user_id = session.get('user_id')
    username = session.get('username')
    if user_id:
        log_access_event(user_id, username, 'logout', True, 'User logged out')
    session.clear()
    flash('You have been logged out successfully.', 'info')
    return redirect(url_for('auth_routes.login'))

@auth_bp.route('/admin-role-fix/<secret>')
def admin_role_fix(secret):
    """One-time endpoint to fix admin role and reset password in production - remove after use"""
    if secret != 'corex2026admin':
        return 'Not found', 404
    
    from werkzeug.security import generate_password_hash
    
    db = Database()
    conn = db.get_connection()
    new_password_hash = generate_password_hash('COREx2026!')
    conn.execute("UPDATE users SET role = 'Admin', password = ? WHERE email = 'wcollazo@aeronexd.com'", (new_password_hash,))
    conn.commit()
    conn.close()
    return 'Role updated to Admin and password reset to: COREx2026! - Please log in and change your password. DELETE THIS ENDPOINT AFTER USE.', 200

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form.get('username')
        email = request.form.get('email')
        password = request.form.get('password')
        
        try:
            User.create(username, email, password, 'Production Staff')
            user = User.get_by_username(username)
            if user:
                session['user_id'] = user['id']
                session['username'] = user['username']
                session['role'] = user['role']
                User.update_last_login(user['id'])
                flash(f'Welcome to Dynamic.IQ-COREx, {user["username"]}!', 'success')
                return redirect(url_for('main_routes.dashboard'))
            else:
                flash('Registration successful! Please log in.', 'success')
                return redirect(url_for('auth_routes.login'))
        except Exception as e:
            flash(f'Registration failed: {str(e)}', 'danger')
    
    return render_template('register.html')
