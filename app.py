from flask import Flask
from api.routes_balances import bp as balances_bp
from api.routes_positions import bp as positions_bp
from api.routes_funding import bp as funding_bp
from api.routes_closed import bp as closed_bp

def create_app():
    app = Flask(__name__)
    app.register_blueprint(balances_bp)
    app.register_blueprint(positions_bp)
    app.register_blueprint(funding_bp)
    app.register_blueprint(closed_bp)
    return app

if __name__ == "__main__":
    app = create_app()
    app.run(debug=True, host="0.0.0.0", port=5000)
