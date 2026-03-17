from functools import wraps

from flask import abort
from flask_login import current_user, login_required  # noqa: F401  (re-exported)


def annotator_required(f):
    """Restrict a view to annotators and admins.

    Apply *after* @login_required so that unauthenticated users are sent to
    the login page rather than receiving a bare 403.

    Usage::

        @bp.route('/annotate')
        @login_required
        @annotator_required
        def annotate():
            ...
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            abort(401)
        if current_user.role not in ('annotator', 'admin'):
            abort(403)
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    """Restrict a view to admins only.

    Apply *after* @login_required.

    Usage::

        @bp.route('/admin')
        @login_required
        @admin_required
        def admin_panel():
            ...
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            abort(401)
        if current_user.role != 'admin':
            abort(403)
        return f(*args, **kwargs)
    return decorated