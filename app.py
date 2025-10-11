from flask import Flask, session
from models import Database, User
from routes.auth_routes import auth_bp
from routes.main_routes import main_bp
from routes.product_routes import product_bp
from routes.bom_routes import bom_bp
from routes.supplier_routes import supplier_bp
from routes.inventory_routes import inventory_bp
from routes.workorder_routes import workorder_bp
from routes.purchaseorder_routes import po_bp
from routes.receiving_routes import receiving_bp
from routes.issuance_routes import issuance_bp
from routes.returns_routes import returns_bp
from routes.adjustment_routes import adjustment_bp
from routes.report_routes import report_bp
from routes.user_routes import user_bp
from routes.permission_routes import permission_bp
from routes.settings_routes import settings_bp
from routes.task_routes import task_bp
from routes.labor_routes import labor_bp
from routes.labor_issuance_routes import labor_issuance_bp
from routes.accounting_routes import accounting_bp
from routes.journal_routes import journal_bp
from routes.financial_reports_routes import financial_reports_bp
from routes.time_tracking_routes import time_tracking_bp
from routes.uom_routes import uom_bp
from routes.audit_routes import audit_bp
from routes.ap_routes import ap_bp
from routes.executive_routes import executive_routes
from routes.customer_routes import customer_bp
from routes.salesorder_routes import salesorder_bp
from routes.clock_station_routes import clock_station_bp
from routes.shipping_routes import shipping_bp
import os

app = Flask(__name__)
app.secret_key = os.environ.get('SESSION_SECRET', 'dev-secret-key-change-in-production')

app.register_blueprint(auth_bp)
app.register_blueprint(main_bp)
app.register_blueprint(product_bp)
app.register_blueprint(bom_bp)
app.register_blueprint(supplier_bp)
app.register_blueprint(inventory_bp)
app.register_blueprint(workorder_bp)
app.register_blueprint(po_bp)
app.register_blueprint(receiving_bp)
app.register_blueprint(issuance_bp)
app.register_blueprint(returns_bp)
app.register_blueprint(adjustment_bp)
app.register_blueprint(report_bp)
app.register_blueprint(user_bp)
app.register_blueprint(permission_bp)
app.register_blueprint(settings_bp)
app.register_blueprint(task_bp)
app.register_blueprint(labor_bp)
app.register_blueprint(labor_issuance_bp)
app.register_blueprint(accounting_bp)
app.register_blueprint(journal_bp)
app.register_blueprint(financial_reports_bp)
app.register_blueprint(time_tracking_bp)
app.register_blueprint(uom_bp)
app.register_blueprint(audit_bp)
app.register_blueprint(ap_bp)
app.register_blueprint(executive_routes)
app.register_blueprint(customer_bp)
app.register_blueprint(salesorder_bp)
app.register_blueprint(clock_station_bp)
app.register_blueprint(shipping_bp)

@app.context_processor
def inject_user():
    user = None
    if 'user_id' in session:
        user = User.get_by_id(session['user_id'])
    return dict(user=user)

@app.before_request
def initialize_database():
    db = Database()
    db.init_db()
    db.seed_chart_of_accounts()
    db.seed_unit_of_measure()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
