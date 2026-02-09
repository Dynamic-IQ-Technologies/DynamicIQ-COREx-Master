from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from models import Database, AuditLogger
from auth import login_required, role_required
from datetime import datetime

external_app_bp = Blueprint('external_app_routes', __name__)

ICON_OPTIONS = [
    ('bi-box-arrow-up-right', 'External Link'),
    ('bi-globe', 'Globe'),
    ('bi-cloud', 'Cloud'),
    ('bi-database', 'Database'),
    ('bi-server', 'Server'),
    ('bi-hdd-network', 'Network'),
    ('bi-envelope', 'Email'),
    ('bi-chat-dots', 'Chat'),
    ('bi-calendar', 'Calendar'),
    ('bi-kanban', 'Kanban'),
    ('bi-clipboard-data', 'Clipboard'),
    ('bi-graph-up', 'Analytics'),
    ('bi-cart', 'Shopping'),
    ('bi-credit-card', 'Payments'),
    ('bi-truck', 'Shipping'),
    ('bi-building', 'Business'),
    ('bi-file-earmark-text', 'Documents'),
    ('bi-shield-check', 'Security'),
    ('bi-tools', 'Tools'),
    ('bi-gear', 'Settings'),
    ('bi-lightning', 'Automation'),
    ('bi-robot', 'AI / Bot'),
    ('bi-terminal', 'Terminal'),
    ('bi-code-slash', 'Code'),
    ('bi-diagram-3', 'Workflow'),
    ('bi-megaphone', 'Marketing'),
    ('bi-headset', 'Support'),
    ('bi-camera-video', 'Video'),
    ('bi-telephone', 'Phone'),
    ('bi-pin-map', 'Location'),
]

COLOR_OPTIONS = [
    ('#6366f1', 'Indigo'),
    ('#3b82f6', 'Blue'),
    ('#06b6d4', 'Cyan'),
    ('#10b981', 'Emerald'),
    ('#f59e0b', 'Amber'),
    ('#ef4444', 'Red'),
    ('#ec4899', 'Pink'),
    ('#8b5cf6', 'Purple'),
    ('#64748b', 'Slate'),
    ('#0ea5e9', 'Sky'),
]


@external_app_bp.route('/connected-apps/')
@login_required
@role_required('Admin')
def list_apps():
    db = Database()
    conn = db.get_connection()
    apps = conn.execute('SELECT * FROM external_apps ORDER BY sort_order, name').fetchall()
    conn.close()
    return render_template('external_apps/list.html', apps=apps, icons=ICON_OPTIONS, colors=COLOR_OPTIONS)


@external_app_bp.route('/connected-apps/add', methods=['POST'])
@login_required
@role_required('Admin')
def add_app():
    name = request.form.get('name', '').strip()
    url = request.form.get('url', '').strip()
    description = request.form.get('description', '').strip()
    icon = request.form.get('icon', 'bi-box-arrow-up-right')
    color = request.form.get('color', '#6366f1')
    category = request.form.get('category', 'General').strip()
    open_in_new_tab = 1 if request.form.get('open_in_new_tab') else 0
    sort_order = int(request.form.get('sort_order', 0) or 0)

    if not name or not url:
        flash('Name and URL are required.', 'danger')
        return redirect(url_for('external_app_routes.list_apps'))

    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute(
        '''INSERT INTO external_apps (name, url, description, icon, color, category, open_in_new_tab, sort_order, created_by)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (name, url, description or None, icon, color, category or 'General', open_in_new_tab, sort_order, session.get('user_id'))
    )
    conn.commit()
    conn.close()

    AuditLogger.log(session.get('user_id'), 'CREATE', 'external_apps', cursor.lastrowid, f'Added connected app: {name}')
    flash(f'Connected app "{name}" added successfully.', 'success')
    return redirect(url_for('external_app_routes.list_apps'))


@external_app_bp.route('/connected-apps/edit/<int:app_id>', methods=['POST'])
@login_required
@role_required('Admin')
def edit_app(app_id):
    name = request.form.get('name', '').strip()
    url = request.form.get('url', '').strip()
    description = request.form.get('description', '').strip()
    icon = request.form.get('icon', 'bi-box-arrow-up-right')
    color = request.form.get('color', '#6366f1')
    category = request.form.get('category', 'General').strip()
    open_in_new_tab = 1 if request.form.get('open_in_new_tab') else 0
    sort_order = int(request.form.get('sort_order', 0) or 0)

    if not name or not url:
        flash('Name and URL are required.', 'danger')
        return redirect(url_for('external_app_routes.list_apps'))

    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    db = Database()
    conn = db.get_connection()
    conn.execute(
        '''UPDATE external_apps SET name=?, url=?, description=?, icon=?, color=?, category=?, open_in_new_tab=?, sort_order=?, updated_at=CURRENT_TIMESTAMP
           WHERE id=?''',
        (name, url, description or None, icon, color, category or 'General', open_in_new_tab, sort_order, app_id)
    )
    conn.commit()
    conn.close()

    AuditLogger.log(session.get('user_id'), 'UPDATE', 'external_apps', app_id, f'Updated connected app: {name}')
    flash(f'Connected app "{name}" updated.', 'success')
    return redirect(url_for('external_app_routes.list_apps'))


@external_app_bp.route('/connected-apps/toggle/<int:app_id>', methods=['POST'])
@login_required
@role_required('Admin')
def toggle_app(app_id):
    db = Database()
    conn = db.get_connection()
    app = conn.execute('SELECT id, name, is_active FROM external_apps WHERE id = ?', (app_id,)).fetchone()
    if not app:
        flash('App not found.', 'danger')
        conn.close()
        return redirect(url_for('external_app_routes.list_apps'))

    new_status = 0 if app['is_active'] else 1
    conn.execute('UPDATE external_apps SET is_active = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?', (new_status, app_id))
    conn.commit()
    conn.close()

    status_text = 'enabled' if new_status else 'disabled'
    AuditLogger.log(session.get('user_id'), 'UPDATE', 'external_apps', app_id, f'{status_text} connected app: {app["name"]}')
    flash(f'App "{app["name"]}" {status_text}.', 'success')
    return redirect(url_for('external_app_routes.list_apps'))


@external_app_bp.route('/connected-apps/delete/<int:app_id>', methods=['POST'])
@login_required
@role_required('Admin')
def delete_app(app_id):
    db = Database()
    conn = db.get_connection()
    app = conn.execute('SELECT id, name FROM external_apps WHERE id = ?', (app_id,)).fetchone()
    if not app:
        flash('App not found.', 'danger')
        conn.close()
        return redirect(url_for('external_app_routes.list_apps'))

    conn.execute('DELETE FROM external_apps WHERE id = ?', (app_id,))
    conn.commit()
    conn.close()

    AuditLogger.log(session.get('user_id'), 'DELETE', 'external_apps', app_id, f'Deleted connected app: {app["name"]}')
    flash(f'App "{app["name"]}" deleted.', 'success')
    return redirect(url_for('external_app_routes.list_apps'))
