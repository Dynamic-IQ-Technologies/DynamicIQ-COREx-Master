from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify, Response
from models import Database
from auth import login_required, role_required
from datetime import datetime
import math
import csv
import io

journal_bp = Blueprint('journal_routes', __name__)

@journal_bp.route('/journal-entries')
@login_required
@role_required('Admin', 'Accountant')
def list_journals():
    db = Database()
    conn = db.get_connection()
    
    entries = conn.execute('''
        SELECT 
            gl.*,
            u.username as created_by_name,
            (SELECT SUM(debit) FROM gl_entry_lines WHERE gl_entry_id = gl.id) as total_debit,
            (SELECT SUM(credit) FROM gl_entry_lines WHERE gl_entry_id = gl.id) as total_credit
        FROM gl_entries gl
        LEFT JOIN users u ON gl.created_by = u.id
        ORDER BY gl.entry_date DESC, gl.entry_number DESC
    ''').fetchall()
    
    conn.close()
    
    return render_template('accounting/journal_entries.html', entries=entries)

@journal_bp.route('/journal-entries/create', methods=['GET', 'POST'])
@role_required('Admin', 'Accountant')
def create_journal():
    if request.method == 'POST':
        db = Database()
        conn = db.get_connection()
        
        try:
            entry_date = request.form['entry_date']
            description = request.form['description']
            
            # Generate entry number
            last_entry = conn.execute('''
                SELECT entry_number FROM gl_entries 
                WHERE entry_number LIKE 'JE-%'
                ORDER BY CAST(SUBSTR(entry_number, 4) AS INTEGER) DESC 
                LIMIT 1
            ''').fetchone()
            
            if last_entry:
                try:
                    last_number = int(last_entry['entry_number'].split('-')[1])
                    next_number = last_number + 1
                except (ValueError, IndexError):
                    next_number = 1
            else:
                next_number = 1
            
            entry_number = f'JE-{next_number:06d}'
            
            # Validate debit/credit balance
            total_debit = 0
            total_credit = 0
            
            lines = []
            account_ids = request.form.getlist('account_id[]')
            debits = request.form.getlist('debit[]')
            credits = request.form.getlist('credit[]')
            line_descriptions = request.form.getlist('line_description[]')
            
            for i in range(len(account_ids)):
                if account_ids[i]:
                    debit = float(debits[i]) if debits[i] else 0
                    credit = float(credits[i]) if credits[i] else 0
                    
                    if not math.isfinite(debit) or not math.isfinite(credit):
                        raise ValueError("Invalid debit or credit amount")
                    
                    if debit < 0 or credit < 0:
                        raise ValueError("Debit and credit amounts must be non-negative")
                    
                    if debit == 0 and credit == 0:
                        continue
                    
                    total_debit += debit
                    total_credit += credit
                    
                    lines.append({
                        'account_id': int(account_ids[i]),
                        'debit': debit,
                        'credit': credit,
                        'description': line_descriptions[i] if i < len(line_descriptions) else ''
                    })
            
            if abs(total_debit - total_credit) > 0.01:
                flash(f'Journal entry is not balanced! Debit: ${total_debit:.2f}, Credit: ${total_credit:.2f}', 'danger')
                conn.close()
                return redirect(url_for('journal_routes.create_journal'))
            
            if len(lines) < 2:
                flash('Journal entry must have at least 2 lines', 'danger')
                conn.close()
                return redirect(url_for('journal_routes.create_journal'))
            
            # Insert journal entry header
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO gl_entries (entry_number, entry_date, description, transaction_source, status, created_by, created_at)
                VALUES (?, ?, ?, 'Manual Journal', 'Draft', ?, ?)
            ''', (entry_number, entry_date, description, session.get('user_id'), datetime.now().isoformat()))
            
            entry_id = cursor.lastrowid
            
            # Insert journal entry lines
            for line in lines:
                cursor.execute('''
                    INSERT INTO gl_entry_lines (gl_entry_id, account_id, debit, credit, description)
                    VALUES (?, ?, ?, ?, ?)
                ''', (entry_id, line['account_id'], line['debit'], line['credit'], line['description']))
            
            conn.commit()
            flash(f'Journal Entry {entry_number} created successfully!', 'success')
            return redirect(url_for('journal_routes.list_journals'))
            
        except Exception as e:
            conn.rollback()
            flash(f'Error creating journal entry: {str(e)}', 'danger')
        finally:
            conn.close()
    
    db = Database()
    conn = db.get_connection()
    accounts = conn.execute('SELECT * FROM chart_of_accounts WHERE is_active = 1 ORDER BY account_code').fetchall()
    conn.close()
    
    return render_template('accounting/create_journal.html', accounts=accounts)

@journal_bp.route('/journal-entries/<int:id>')
@login_required
@role_required('Admin', 'Accountant')
def view_journal(id):
    db = Database()
    conn = db.get_connection()
    
    entry = conn.execute('''
        SELECT gl.*, u.username as created_by_name
        FROM gl_entries gl
        LEFT JOIN users u ON gl.created_by = u.id
        WHERE gl.id = ?
    ''', (id,)).fetchone()
    
    lines = conn.execute('''
        SELECT gl_lines.*, coa.account_code, coa.account_name
        FROM gl_entry_lines gl_lines
        JOIN chart_of_accounts coa ON gl_lines.account_id = coa.id
        WHERE gl_lines.gl_entry_id = ?
    ''', (id,)).fetchall()
    
    conn.close()
    
    return render_template('accounting/view_journal.html', entry=entry, lines=lines)

@journal_bp.route('/journal-entries/<int:id>/post', methods=['POST'])
@role_required('Admin', 'Accountant')
def post_journal(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('''
            UPDATE gl_entries 
            SET status = 'Posted', posted_by = ?, posted_at = ?
            WHERE id = ?
        ''', (session.get('user_id'), datetime.now().isoformat(), id))
        
        conn.commit()
        flash('Journal entry posted successfully!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error posting journal entry: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('journal_routes.view_journal', id=id))

@journal_bp.route('/journal-entries/<int:id>/unpost', methods=['POST'])
@role_required('Admin')
def unpost_journal(id):
    db = Database()
    conn = db.get_connection()
    
    try:
        conn.execute('''
            UPDATE gl_entries 
            SET status = 'Draft', posted_by = NULL, posted_at = NULL
            WHERE id = ?
        ''', (id,))
        
        conn.commit()
        flash('Journal entry unposted successfully!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Error unposting journal entry: {str(e)}', 'danger')
    finally:
        conn.close()
    
    return redirect(url_for('journal_routes.view_journal', id=id))

# GL Account Detail View Routes
@journal_bp.route('/gl-account-detail/<account_code>')
@login_required
@role_required('Admin', 'Accountant', 'Planner')
def gl_account_detail(account_code):
    """Display detailed transaction list for a specific GL account"""
    db = Database()
    conn = db.get_connection()
    
    # Get account details
    account = conn.execute('''
        SELECT * FROM chart_of_accounts WHERE account_code = ?
    ''', (account_code,)).fetchone()
    
    if not account:
        flash('GL Account not found', 'danger')
        conn.close()
        return redirect(url_for('journal_routes.list_journals'))
    
    # Get filter parameters
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    transaction_type = request.args.get('transaction_type', '')
    search_ref = request.args.get('search_ref', '')
    sort_by = request.args.get('sort_by', 'entry_date')
    sort_dir = request.args.get('sort_dir', 'DESC')
    
    # Build query for transactions
    query = '''
        SELECT 
            gel.id as line_id,
            gel.debit,
            gel.credit,
            gel.description as line_description,
            ge.id as entry_id,
            ge.entry_number,
            ge.entry_date,
            ge.description as entry_description,
            ge.transaction_source,
            ge.reference_type,
            ge.reference_id,
            ge.status,
            coa.account_code,
            coa.account_name,
            coa.account_type
        FROM gl_entry_lines gel
        JOIN gl_entries ge ON gel.gl_entry_id = ge.id
        JOIN chart_of_accounts coa ON gel.account_id = coa.id
        WHERE coa.account_code = ? AND ge.status = 'Posted'
    '''
    params = [account_code]
    
    # Apply filters
    if start_date:
        query += ' AND ge.entry_date >= ?'
        params.append(start_date)
    
    if end_date:
        query += ' AND ge.entry_date <= ?'
        params.append(end_date)
    
    if transaction_type:
        query += ' AND ge.transaction_source = ?'
        params.append(transaction_type)
    
    if search_ref:
        query += ' AND (ge.entry_number LIKE ? OR ge.description LIKE ? OR gel.description LIKE ?)'
        search_pattern = f'%{search_ref}%'
        params.extend([search_pattern, search_pattern, search_pattern])
    
    # Add sorting
    valid_sort_columns = ['entry_date', 'entry_number', 'debit', 'credit', 'transaction_source']
    if sort_by in valid_sort_columns:
        query += f' ORDER BY ge.{sort_by} {sort_dir}, gel.id {sort_dir}'
    else:
        query += ' ORDER BY ge.entry_date DESC, gel.id DESC'
    
    transactions = conn.execute(query, params).fetchall()
    
    # Calculate running balance
    transactions_with_balance = []
    running_balance = 0.0
    
    for trans in transactions:
        # For Asset, Expense accounts: Debit increases, Credit decreases
        # For Liability, Equity, Revenue accounts: Credit increases, Debit decreases
        if account['account_type'] in ['Asset', 'Expense']:
            running_balance += (trans['debit'] - trans['credit'])
        else:
            running_balance += (trans['credit'] - trans['debit'])
        
        trans_dict = dict(trans)
        trans_dict['running_balance'] = running_balance
        transactions_with_balance.append(trans_dict)
    
    # Get unique transaction sources for filter dropdown
    transaction_sources = conn.execute('''
        SELECT DISTINCT transaction_source 
        FROM gl_entries
        WHERE id IN (
            SELECT DISTINCT gl_entry_id FROM gl_entry_lines 
            WHERE account_id = (SELECT id FROM chart_of_accounts WHERE account_code = ?)
        )
        ORDER BY transaction_source
    ''', (account_code,)).fetchall()
    
    # Calculate summary statistics
    summary = {
        'total_debits': sum(t['debit'] for t in transactions),
        'total_credits': sum(t['credit'] for t in transactions),
        'net_change': running_balance,
        'transaction_count': len(transactions)
    }
    
    conn.close()
    
    return render_template('accounting/gl_account_detail.html',
                         account=account,
                         transactions=transactions_with_balance,
                         transaction_sources=transaction_sources,
                         summary=summary,
                         filters={
                             'start_date': start_date,
                             'end_date': end_date,
                             'transaction_type': transaction_type,
                             'search_ref': search_ref,
                             'sort_by': sort_by,
                             'sort_dir': sort_dir
                         })

@journal_bp.route('/gl-account-detail/<account_code>/export')
@login_required
@role_required('Admin', 'Accountant', 'Planner')
def export_gl_account_detail(account_code):
    """Export GL account transactions to CSV"""
    db = Database()
    conn = db.get_connection()
    
    # Get account details
    account = conn.execute('''
        SELECT * FROM chart_of_accounts WHERE account_code = ?
    ''', (account_code,)).fetchone()
    
    if not account:
        flash('GL Account not found', 'danger')
        conn.close()
        return redirect(url_for('journal_routes.list_journals'))
    
    # Get filter parameters (same as detail view)
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    transaction_type = request.args.get('transaction_type', '')
    search_ref = request.args.get('search_ref', '')
    
    # Build query
    query = '''
        SELECT 
            ge.entry_number,
            ge.entry_date,
            ge.transaction_source,
            ge.reference_type,
            ge.reference_id,
            gel.debit,
            gel.credit,
            ge.description as entry_description,
            gel.description as line_description
        FROM gl_entry_lines gel
        JOIN gl_entries ge ON gel.gl_entry_id = ge.id
        JOIN chart_of_accounts coa ON gel.account_id = coa.id
        WHERE coa.account_code = ? AND ge.status = 'Posted'
    '''
    params = [account_code]
    
    # Apply same filters
    if start_date:
        query += ' AND ge.entry_date >= ?'
        params.append(start_date)
    
    if end_date:
        query += ' AND ge.entry_date <= ?'
        params.append(end_date)
    
    if transaction_type:
        query += ' AND ge.transaction_source = ?'
        params.append(transaction_type)
    
    if search_ref:
        query += ' AND (ge.entry_number LIKE ? OR ge.description LIKE ? OR gel.description LIKE ?)'
        search_pattern = f'%{search_ref}%'
        params.extend([search_pattern, search_pattern, search_pattern])
    
    query += ' ORDER BY ge.entry_date, gel.id'
    
    transactions = conn.execute(query, params).fetchall()
    
    # Create CSV
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write headers
    writer.writerow([
        'Entry Number', 'Date', 'Transaction Type', 'Reference Type', 
        'Reference ID', 'Debit', 'Credit', 'Entry Description', 'Line Description'
    ])
    
    # Write data rows with running balance
    running_balance = 0.0
    for trans in transactions:
        if account['account_type'] in ['Asset', 'Expense']:
            running_balance += (trans['debit'] - trans['credit'])
        else:
            running_balance += (trans['credit'] - trans['debit'])
        
        writer.writerow([
            trans['entry_number'],
            trans['entry_date'],
            trans['transaction_source'],
            trans['reference_type'] or '',
            trans['reference_id'] or '',
            f"{trans['debit']:.2f}",
            f"{trans['credit']:.2f}",
            trans['entry_description'],
            trans['line_description']
        ])
    
    # Add summary row
    writer.writerow([])
    writer.writerow(['Summary', '', '', '', '', 
                    f"{sum(t['debit'] for t in transactions):.2f}",
                    f"{sum(t['credit'] for t in transactions):.2f}",
                    f"Net Change: {running_balance:.2f}"])
    
    conn.close()
    
    # Create response
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename=GL_{account_code}_{datetime.now().strftime("%Y%m%d")}.csv'}
    )
