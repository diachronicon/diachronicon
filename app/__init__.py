import json
import logging
import os

from werkzeug.datastructures import ImmutableOrderedMultiDict
from flask import Flask, send_from_directory
from flask.wrappers import Request
from flask_login import LoginManager
from flask_wtf.csrf import CSRFProtect

from config import Config, loggingConfig
import app.logging_utils as logging_utils

logger = logging_utils.init_logger(Config.LOGGING_FILE, loggingConfig)

login_manager = LoginManager()
csrf = CSRFProtect()


class RequestWithOrderedFormData(Request):
    parameter_storage_class = ImmutableOrderedMultiDict


def create_app(test_config_obj=None, remove_wsgi_logger=False):
    app = Flask(__name__, instance_relative_config=True)
    app.request_class = RequestWithOrderedFormData

    if test_config_obj is None:
        app.config.from_object(Config)
    else:
        app.config.from_object(test_config_obj)

    os.makedirs(app.instance_path, exist_ok=True)

    if remove_wsgi_logger:
        logger.removeHandler('wsgi')

    # ------------------------------------------------------------------
    # Database session
    # ------------------------------------------------------------------
    from app.database import engine, db_session
    app.engine = engine
    app.db_session = db_session

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        app.db_session.remove()

    # ------------------------------------------------------------------
    # Flask-Login
    # ------------------------------------------------------------------
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = 'Пожалуйста, войдите для доступа к этой странице.'
    login_manager.login_message_category = 'warning'

    @login_manager.user_loader
    def load_user(user_id):
        from app.models import User
        return app.db_session.get(User, int(user_id))

    # ------------------------------------------------------------------
    # CSRF protection
    # Validates X-CSRFToken header on JSON POST requests from the
    # annotation form's fetch() calls.
    # ------------------------------------------------------------------
    csrf.init_app(app)

    # ------------------------------------------------------------------
    # Jinja2 custom filters
    # ------------------------------------------------------------------
    app.jinja_env.filters['from_json'] = lambda s: json.loads(s or '{}')

    # ------------------------------------------------------------------
    # Blueprints
    # ------------------------------------------------------------------
    from app.auth import bp as auth_bp
    app.register_blueprint(auth_bp)

    from app.main import bp as main_bp
    app.register_blueprint(main_bp)

    from app.search import bp as search_bp
    app.register_blueprint(search_bp)

    from app.errors import bp as errors_bp
    app.register_blueprint(errors_bp)

    from app.annotation import bp as annotation_bp
    app.register_blueprint(annotation_bp)

    # ------------------------------------------------------------------
    # CLI commands
    # ------------------------------------------------------------------
    from app.search.cli import embeddings_cli
    app.cli.add_command(embeddings_cli)

    if app.debug:
        from werkzeug.debug import DebuggedApplication
        app.wsgi_app = DebuggedApplication(app.wsgi_app, evalex=True)

    @app.route('/favicon.ico')
    def favicon():
        return send_from_directory('app/', 'favicon.ico')

    return app


from app import models  # noqa: E402, F401