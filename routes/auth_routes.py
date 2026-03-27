from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import User, Database
from datetime import datetime, timedelta
import secrets

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


def send_reset_email(to_email, to_name, reset_url):
    """Send password reset email via Brevo. Returns (success, error_message)."""
    try:
        from utils.brevo_helper import get_brevo_credentials
        api_key, from_email, from_name = get_brevo_credentials()
        if not api_key or not from_email:
            return False, "Email service not configured. Please contact your administrator."

        import sib_api_v3_sdk
        from sib_api_v3_sdk.rest import ApiException

        html_content = f"""
        <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 600px; margin: 0 auto; background: #f8fafc; padding: 40px 20px;">
            <div style="background: linear-gradient(135deg, #1e293b 0%, #334155 100%); border-radius: 16px 16px 0 0; padding: 32px; text-align: center;">
                <h1 style="color: white; margin: 0; font-size: 1.8rem; font-weight: 800;">
                    Dynamic.<span style="color: #f97316;">IQ</span>-COREx
                </h1>
                <p style="color: #94a3b8; margin: 8px 0 0; font-size: 0.95rem;">Autonomous Enterprise Operating System</p>
            </div>
            <div style="background: white; border-radius: 0 0 16px 16px; padding: 40px 32px; box-shadow: 0 4px 20px rgba(0,0,0,0.08);">
                <h2 style="color: #1e293b; margin: 0 0 16px; font-size: 1.4rem;">Password Reset Request</h2>
                <p style="color: #475569; line-height: 1.7; margin: 0 0 24px;">
                    Hi {to_name or 'there'},<br><br>
                    We received a request to reset your password. Click the button below to choose a new one.
                    This link will expire in <strong>1 hour</strong>.
                </p>
                <div style="text-align: center; margin: 32px 0;">
                    <a href="{reset_url}"
                       style="display: inline-block; background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
                              color: white; text-decoration: none; padding: 14px 36px; border-radius: 12px;
                              font-weight: 600; font-size: 1rem; box-shadow: 0 4px 14px rgba(15,23,42,0.25);">
                        Reset My Password
                    </a>
                </div>
                <p style="color: #94a3b8; font-size: 0.85rem; line-height: 1.6; margin: 24px 0 0; border-top: 1px solid #e2e8f0; padding-top: 24px;">
                    If you did not request a password reset, you can safely ignore this email — your password will not change.<br><br>
                    For security, this link expires in 1 hour. If it has expired, visit the login page and request a new reset link.
                </p>
            </div>
        </div>
        """

        configuration = sib_api_v3_sdk.Configuration()
        configuration.api_key['api-key'] = api_key
        api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))
        send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(
            to=[{"email": to_email, "name": to_name or to_email}],
            sender={"email": from_email, "name": from_name},
            subject="Reset your Dynamic.IQ-COREx password",
            html_content=html_content
        )
        api_instance.send_transac_email(send_smtp_email)
        return True, None
    except Exception as e:
        return False, str(e)


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

@auth_bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form.get('username')
        email = request.form.get('email')
        password = request.form.get('password')
        
        try:
            User.create(username, email, password, 'Admin')
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


@auth_bp.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        if not email:
            flash('Please enter your email address.', 'danger')
            return render_template('forgot_password.html')

        db = Database()
        conn = db.get_connection()
        try:
            user = conn.execute(
                'SELECT id, username, email FROM users WHERE LOWER(email) = ?', (email,)
            ).fetchone()

            if user:
                token = secrets.token_urlsafe(32)
                expires_at = datetime.now() + timedelta(hours=1)
                conn.execute(
                    '''INSERT INTO password_reset_tokens (user_id, token, expires_at, used)
                       VALUES (?, ?, ?, 0)''',
                    (user['id'], token, expires_at)
                )
                conn.commit()

                reset_url = url_for('auth_routes.reset_password', token=token, _external=True)
                success, err = send_reset_email(user['email'], user['username'], reset_url)
                if not success:
                    print(f"[PasswordReset] Email send failed: {err}")

            conn.close()
        except Exception as e:
            print(f"[PasswordReset] Error: {e}")
            try:
                conn.close()
            except Exception:
                pass

        flash(
            'If that email is associated with an account, a password reset link has been sent. '
            'Please check your inbox (and spam folder).',
            'info'
        )
        return redirect(url_for('auth_routes.login'))

    return render_template('forgot_password.html')


@auth_bp.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    db = Database()
    conn = db.get_connection()
    try:
        record = conn.execute(
            '''SELECT prt.*, u.username, u.email
               FROM password_reset_tokens prt
               JOIN users u ON prt.user_id = u.id
               WHERE prt.token = ? AND prt.used = 0''',
            (token,)
        ).fetchone()
    except Exception as e:
        print(f"[PasswordReset] Token lookup error: {e}")
        record = None

    if not record:
        conn.close()
        flash('This password reset link is invalid or has already been used.', 'danger')
        return redirect(url_for('auth_routes.login'))

    expires_at = record['expires_at']
    if isinstance(expires_at, str):
        try:
            expires_at = datetime.strptime(expires_at, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            expires_at = datetime.strptime(expires_at, '%Y-%m-%d %H:%M:%S')

    if datetime.now() > expires_at:
        conn.close()
        flash('This password reset link has expired. Please request a new one.', 'warning')
        return redirect(url_for('auth_routes.forgot_password'))

    if request.method == 'POST':
        password = request.form.get('password', '')
        confirm = request.form.get('confirm_password', '')

        if len(password) < 8:
            conn.close()
            flash('Password must be at least 8 characters long.', 'danger')
            return render_template('reset_password.html', token=token)

        if password != confirm:
            conn.close()
            flash('Passwords do not match.', 'danger')
            return render_template('reset_password.html', token=token)

        try:
            from werkzeug.security import generate_password_hash
            new_hash = generate_password_hash(password)
            conn.execute(
                'UPDATE users SET password_hash = ? WHERE id = ?',
                (new_hash, record['user_id'])
            )
            conn.execute(
                'UPDATE password_reset_tokens SET used = 1 WHERE token = ?',
                (token,)
            )
            conn.commit()
            conn.close()
            log_access_event(record['user_id'], record['username'], 'password_reset', True, 'Password reset via email link')
            flash('Your password has been reset successfully. Please log in with your new password.', 'success')
            return redirect(url_for('auth_routes.login'))
        except Exception as e:
            print(f"[PasswordReset] Update error: {e}")
            try:
                conn.close()
            except Exception:
                pass
            flash('An error occurred while resetting your password. Please try again.', 'danger')
            return render_template('reset_password.html', token=token)

    conn.close()
    return render_template('reset_password.html', token=token)
