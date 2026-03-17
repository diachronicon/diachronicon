from urllib.parse import urlparse

from flask import render_template, redirect, url_for, flash, request
from flask_login import login_user, logout_user, current_user
from werkzeug.security import check_password_hash

from app.auth import bp
from app.auth.forms import LoginForm


@bp.route('/login', methods=['GET', 'POST'])
def login():
    # Already authenticated users go straight to the homepage
    if current_user.is_authenticated:
        return redirect(url_for('main.main'))

    form = LoginForm()

    if form.validate_on_submit():
        from flask import current_app
        from app.models import User

        user = (
            current_app.db_session
            .query(User)
            .filter_by(username=form.username.data.strip())
            .first()
        )

        if user is None or not check_password_hash(
            user.password_hash, form.password.data
        ):
            flash('Неверное имя пользователя или пароль.', 'danger')
            return render_template('auth/login.html', title='Вход', form=form)

        if not user.is_active:
            flash('Аккаунт отключён. Обратитесь к администратору.', 'warning')
            return render_template('auth/login.html', title='Вход', form=form)

        login_user(user, remember=form.remember_me.data)

        # Safe redirect: only follow `next` if it is a relative URL
        next_page = request.args.get('next')
        if not next_page or urlparse(next_page).netloc != '':
            next_page = url_for('main.main')

        return redirect(next_page)

    return render_template('auth/login.html', title='Вход', form=form)


@bp.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('main.main'))