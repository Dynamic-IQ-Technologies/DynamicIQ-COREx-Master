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
from routes.report_routes import report_bp
from routes.user_routes import user_bp
from routes.permission_routes import permission_bp
from routes.settings_routes import settings_bp
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
app.register_blueprint(report_bp)
app.register_blueprint(user_bp)
app.register_blueprint(permission_bp)
app.register_blueprint(settings_bp)

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
