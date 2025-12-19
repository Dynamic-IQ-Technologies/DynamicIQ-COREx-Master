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
from routes.task_template_routes import task_template_bp
from routes.labor_routes import labor_bp
from routes.skillset_routes import skillset_bp
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
from routes.invoice_routes import invoice_bp
from routes.quote_routes import quote_bp
from routes.service_wo_routes import service_wo_bp
from routes.capability_routes import capability_bp
from routes.market_analysis_routes import market_analysis_bp
from routes.supplier_discovery_routes import supplier_discovery_bp
from routes.capacity_routes import capacity_bp
from routes.customer_service_routes import customer_service_bp
from routes.portal_routes import portal_bp
from routes.tools_routes import tools_bp
from routes.rfq_routes import rfq_bp
from routes.rfq_portal_routes import rfq_portal_bp
from routes.org_analyzer_routes import org_analyzer_bp
from routes.financial_analyzer_routes import financial_analyzer_bp
from routes.erp_helper_routes import erp_helper_bp
from routes.ndt_routes import ndt_bp
from routes.master_scheduler_routes import master_scheduler_bp
from routes.salesforce_migration_routes import sf_migration_bp
from routes.business_analytics_routes import business_analytics_bp
from routes.it_manager_routes import it_manager_bp
from routes.qms_routes import qms_bp
from routes.exchange_routes import exchange_bp
from routes.part_intake_routes import part_intake_bp
from routes.operations_routes import operations_bp
import os

app = Flask(__name__)
app.secret_key = os.environ.get('SESSION_SECRET', 'dev-secret-key-change-in-production')

@app.template_filter('currency')
def currency_filter(value):
    """Format a number as currency: $ 8,750.00"""
    try:
        if value is None:
            return '$ 0.00'
        num = float(value)
        if num < 0:
            return '-$ {:,.2f}'.format(abs(num))
        return '$ {:,.2f}'.format(num)
    except (ValueError, TypeError):
        return '$ 0.00'

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
app.register_blueprint(task_template_bp)
app.register_blueprint(labor_bp)
app.register_blueprint(skillset_bp)
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
app.register_blueprint(invoice_bp)
app.register_blueprint(quote_bp)
app.register_blueprint(service_wo_bp)
app.register_blueprint(capability_bp)
app.register_blueprint(market_analysis_bp)
app.register_blueprint(supplier_discovery_bp)
app.register_blueprint(capacity_bp)
app.register_blueprint(customer_service_bp)
app.register_blueprint(portal_bp)
app.register_blueprint(tools_bp)
app.register_blueprint(rfq_bp)
app.register_blueprint(rfq_portal_bp)
app.register_blueprint(org_analyzer_bp)
app.register_blueprint(financial_analyzer_bp)
app.register_blueprint(erp_helper_bp)
app.register_blueprint(ndt_bp)
app.register_blueprint(master_scheduler_bp)
app.register_blueprint(sf_migration_bp)
app.register_blueprint(business_analytics_bp)
app.register_blueprint(it_manager_bp)
app.register_blueprint(qms_bp)
app.register_blueprint(exchange_bp)
app.register_blueprint(part_intake_bp)
app.register_blueprint(operations_bp)

@app.context_processor
def inject_user():
    user = None
    if 'user_id' in session:
        user = User.get_by_id(session['user_id'])
    return dict(user=user)

def initialize_application():
    """Run expensive initialization once at application startup"""
    db = Database()
    db.init_db()
    db.seed_chart_of_accounts()
    db.seed_unit_of_measure()
    
    from services.exchange_chain_service import get_exchange_chain_service
    exchange_service = get_exchange_chain_service()
    exchange_service.load_graph_from_database()

initialize_application()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
