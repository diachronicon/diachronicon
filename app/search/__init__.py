from flask import Blueprint

bp = Blueprint('search', __name__, static_folder='static/search/')

from app.search import routes       # noqa: E402, F401
from app.search import construction  # noqa: E402, F401