from flask import Blueprint

bp = Blueprint('annotation', __name__, url_prefix='/annotation')

from app.annotation import routes  # noqa: E402, F401