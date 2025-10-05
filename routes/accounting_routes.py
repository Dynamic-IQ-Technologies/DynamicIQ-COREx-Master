from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from models import Database
from auth import login_required, role_required

accounting_bp = Blueprint('accounting_routes', __name__)

@accounting_bp.route('/chart-of-accounts')
@login_required
@role_required('Admin', 'Accountant')
def list_accounts():
    db = Database()
    conn = db.get_connection()
    
    accounts = conn.execute('''
        SELECT * FROM chart_of_accounts 
        ORDER BY account_code
    ''').fetchall()
    
    conn.close()
    
    return render_template('accounting/chart_of_accounts.html', accounts=accounts)

@accounting_bp.route('/chart-of-accounts/create', methods=['GET', 'POST'])
@role_required('Admin')
def create_account():
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            account_code = request.form['account_code']
            account_name = request.form['account_name']
            account_type = request.form['account_type']
            description = request.form.get('description', '')
            parent_id = request.form.get('parent_account_id', '')
            
            parent_account_id = int(parent_id) if parent_id else None
            
            conn.execute('''
                INSERT INTO chart_of_accounts (account_code, account_name, account_type, parent_account_id, description, is_active)
                VALUES (?, ?, ?, ?, ?, 1)
            ''', (account_code, account_name, account_type, parent_account_id, description))
            
            conn.commit()
            flash('Account created successfully!', 'success')
            return redirect(url_for('accounting_routes.list_accounts'))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error creating account: {str(e)}', 'danger')
        finally:
            conn.close()
    
    accounts = conn.execute('SELECT * FROM chart_of_accounts WHERE is_active = 1 ORDER BY account_code').fetchall()
    conn.close()
    
    return render_template('accounting/create_account.html', accounts=accounts)

@accounting_bp.route('/chart-of-accounts/<int:id>/edit', methods=['GET', 'POST'])
@role_required('Admin')
def edit_account(id):
    db = Database()
    conn = db.get_connection()
    
    if request.method == 'POST':
        try:
            account_name = request.form['account_name']
            description = request.form.get('description', '')
            is_active = 1 if request.form.get('is_active') == 'on' else 0
            parent_id = request.form.get('parent_account_id', '')
            
            parent_account_id = int(parent_id) if parent_id else None
            
            conn.execute('''
                UPDATE chart_of_accounts 
                SET account_name = ?, parent_account_id = ?, description = ?, is_active = ?
                WHERE id = ?
            ''', (account_name, parent_account_id, description, is_active, id))
            
            conn.commit()
            flash('Account updated successfully!', 'success')
            return redirect(url_for('accounting_routes.list_accounts'))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error updating account: {str(e)}', 'danger')
        finally:
            conn.close()
    
    account = conn.execute('SELECT * FROM chart_of_accounts WHERE id = ?', (id,)).fetchone()
    accounts = conn.execute('SELECT * FROM chart_of_accounts WHERE is_active = 1 AND id != ? ORDER BY account_code', (id,)).fetchall()
    conn.close()
    
    return render_template('accounting/edit_account.html', account=account, accounts=accounts)

@accounting_bp.route('/general-ledger')
@login_required
@role_required('Admin', 'Accountant')
def general_ledger():
    db = Database()
    conn = db.get_connection()
    
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    account_id = request.args.get('account_id', '')
    
    query = '''
        SELECT 
            gl.entry_number,
            gl.entry_date,
            gl.description,
            gl.transaction_source,
            coa.account_code,
            coa.account_name,
            gl_lines.debit,
            gl_lines.credit,
            gl.status
        FROM gl_entry_lines gl_lines
        JOIN gl_entries gl ON gl_lines.gl_entry_id = gl.id
        JOIN chart_of_accounts coa ON gl_lines.account_id = coa.id
        WHERE 1=1
    '''
    
    params = []
    
    if start_date:
        query += ' AND gl.entry_date >= ?'
        params.append(start_date)
    
    if end_date:
        query += ' AND gl.entry_date <= ?'
        params.append(end_date)
    
    if account_id:
        query += ' AND coa.id = ?'
        params.append(account_id)
    
    query += ' ORDER BY gl.entry_date DESC, gl.entry_number DESC'
    
    entries = conn.execute(query, params).fetchall()
    accounts = conn.execute('SELECT * FROM chart_of_accounts WHERE is_active = 1 ORDER BY account_code').fetchall()
    
    conn.close()
    
    return render_template('accounting/general_ledger.html', 
                         entries=entries, 
                         accounts=accounts,
                         start_date=start_date,
                         end_date=end_date,
                         account_id=account_id)
