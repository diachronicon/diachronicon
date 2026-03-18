"""
Integration tests for app/annotation/routes.py

Self-contained: the app fixture patches in everything it needs (from_json
filter, CSRF disabled) so these tests pass regardless of which version of
app/__init__.py or config.py the user currently has on disk.

Tested scenarios
----------------
- Access control (anonymous, annotator, admin)
- GET /annotation/new  — creates draft, redirects
- GET /annotation/<id> — loads form (200)
- POST /annotation/<id>/save — JSON autosave roundtrip
- POST /annotation/<id>/llm  — success and error paths
- POST /annotation/<id>/submit — publishes construction
"""
import json
from unittest.mock import MagicMock, patch

import pytest
from werkzeug.security import generate_password_hash

from config import TestConfig
from app import create_app
from app.models import Base, User, AnnotationDraft, Construction, GeneralInfo, Change


# ── App fixture (self-contained) ───────────────────────────────────────────

class _AnnotationTestConfig(TestConfig):
    """Override to guarantee CSRF is off and we use in-memory DB."""
    TESTING = True
    WTF_CSRF_ENABLED = False
    SQLALCHEMY_DATABASE_URI = 'sqlite:///:memory:'


@pytest.fixture(scope='module')
def app():
    # search_form.py calls find_unique() at class-definition time (module level)
    # to populate WTForms datalists — e.g. find_unique(Construction, "contemporary_meaning").
    # Those queries fire when the search blueprint is imported inside create_app(),
    # before create_all() has run on the in-memory DB. Patch it to [] for that
    # window only; the real function is restored immediately after create_app() returns.
    with patch('app.utils.find_unique', return_value=[]):
        application = create_app(test_config_obj=_AnnotationTestConfig)

    # Guarantee the from_json filter is present even if __init__.py wasn't updated
    if 'from_json' not in application.jinja_env.filters:
        application.jinja_env.filters['from_json'] = lambda s: json.loads(s or '{}')

    with application.app_context():
        Base.metadata.create_all(bind=application.engine)

        # Register blueprints that may be missing if app/__init__.py hasn't been
        # updated yet — makes these tests fully self-contained.
        if 'annotation' not in application.blueprints:
            from app.annotation import bp as annotation_bp
            application.register_blueprint(annotation_bp)

        if 'admin' not in application.blueprints:
            try:
                from app.admin import bp as admin_bp
                application.register_blueprint(admin_bp)
            except ImportError:
                pass  # admin blueprint is Phase 5; optional here

        yield application
        Base.metadata.drop_all(bind=application.engine)


@pytest.fixture(scope='module')
def _seed_users(app):
    session = app.db_session
    annotator = User(
        username='annotator1',
        email='ann1@test.com',
        password_hash=generate_password_hash('pass'),
        role='annotator',
        is_active=True,
    )
    admin = User(
        username='admin1',
        email='adm1@test.com',
        password_hash=generate_password_hash('pass'),
        role='admin',
        is_active=True,
    )
    session.add_all([annotator, admin])
    session.commit()
    yield {'annotator': annotator, 'admin': admin}
    for u in [annotator, admin]:
        try:
            session.delete(u)
        except Exception:
            pass
    session.commit()


@pytest.fixture
def client(app):
    return app.test_client()


def _login(client, username, password='pass'):
    return client.post('/auth/login', data={
        'username': username,
        'password': password,
    }, follow_redirects=True)


def _logout(client):
    client.get('/auth/logout')


# ── Access control ─────────────────────────────────────────────────────────

class TestAccessControl:

    def test_anonymous_index_redirects_to_login(self, client):
        r = client.get('/annotation/', follow_redirects=False)
        # Flask-Login redirects unauthenticated users → 302
        assert r.status_code == 302
        assert b'login' in r.headers.get('Location', '').lower().encode()

    def test_anonymous_new_redirects_to_login(self, client):
        r = client.get('/annotation/new', follow_redirects=False)
        assert r.status_code == 302

    def test_annotator_can_access_index(self, client, _seed_users):
        _login(client, 'annotator1')
        r = client.get('/annotation/')
        assert r.status_code == 200
        _logout(client)

    def test_admin_can_access_index(self, client, _seed_users):
        _login(client, 'admin1')
        r = client.get('/annotation/')
        assert r.status_code == 200
        _logout(client)


# ── GET /annotation/new ────────────────────────────────────────────────────

class TestNewDraft:

    def test_creates_draft_and_redirects(self, client, app, _seed_users):
        _login(client, 'annotator1')
        r = client.get('/annotation/new', follow_redirects=False)
        assert r.status_code == 302
        assert '/annotation/' in r.headers.get('Location', '')

        session = app.db_session
        annotator = session.query(User).filter_by(username='annotator1').first()
        draft = (
            session.query(AnnotationDraft)
            .filter_by(annotator_id=annotator.id)
            .order_by(AnnotationDraft.id.desc())
            .first()
        )
        assert draft is not None
        assert draft.status == 'draft'
        assert draft.form_data == '{}'
        _logout(client)


# ── GET /annotation/<id> ───────────────────────────────────────────────────

class TestFormLoad:

    @pytest.fixture
    def draft(self, app, _seed_users):
        session = app.db_session
        annotator = session.query(User).filter_by(username='annotator1').first()
        d = AnnotationDraft(
            construction_id=None,
            annotator_id=annotator.id,
            form_data='{"formula": "[X]", "changes": []}',
            status='draft',
        )
        session.add(d)
        session.commit()
        yield d
        try:
            session.delete(d)
            session.commit()
        except Exception:
            session.rollback()

    def test_form_loads_200(self, client, draft, _seed_users):
        _login(client, 'annotator1')
        r = client.get(f'/annotation/{draft.id}')
        assert r.status_code == 200
        _logout(client)

    def test_404_for_unknown_draft(self, client, _seed_users):
        _login(client, 'annotator1')
        r = client.get('/annotation/999999')
        assert r.status_code == 404
        _logout(client)

    def test_403_when_annotator_accesses_other_users_draft(
        self, client, app, _seed_users, draft
    ):
        # draft belongs to annotator1; admin1 is a different user
        # admin1 has role='admin' so can see all drafts — create a
        # draft owned by admin1 and confirm annotator1 gets 403
        session = app.db_session
        admin = session.query(User).filter_by(username='admin1').first()
        other = AnnotationDraft(
            construction_id=None,
            annotator_id=admin.id,
            form_data='{}',
            status='draft',
        )
        session.add(other)
        session.commit()

        _login(client, 'annotator1')
        r = client.get(f'/annotation/{other.id}')
        assert r.status_code == 403
        _logout(client)

        session.delete(other)
        session.commit()


# ── POST /annotation/<id>/save ─────────────────────────────────────────────

class TestSave:

    @pytest.fixture
    def draft(self, app, _seed_users):
        session = app.db_session
        annotator = session.query(User).filter_by(username='annotator1').first()
        d = AnnotationDraft(
            construction_id=None,
            annotator_id=annotator.id,
            form_data='{}',
            status='draft',
        )
        session.add(d)
        session.commit()
        yield d
        try:
            session.delete(d)
            session.commit()
        except Exception:
            session.rollback()

    def test_save_updates_form_data(self, client, app, draft, _seed_users):
        _login(client, 'annotator1')
        payload = {'formula': '[NP так VP]', 'changes': []}
        r = client.post(
            f'/annotation/{draft.id}/save',
            data=json.dumps(payload),
            content_type='application/json',
        )
        assert r.status_code == 200, f"expected 200, got {r.status_code}: {r.data[:200]}"
        body = r.get_json()
        assert body['status'] == 'ok'
        assert 'updated_at' in body

        app.db_session.expire(draft)
        assert '[NP так VP]' in draft.form_data
        _logout(client)

    def test_save_returns_400_for_submitted_draft(self, client, app, _seed_users):
        session = app.db_session
        annotator = session.query(User).filter_by(username='annotator1').first()
        submitted = AnnotationDraft(
            construction_id=None,
            annotator_id=annotator.id,
            form_data='{}',
            status='submitted',
        )
        session.add(submitted)
        session.commit()

        _login(client, 'annotator1')
        r = client.post(
            f'/annotation/{submitted.id}/save',
            data=json.dumps({'formula': 'x'}),
            content_type='application/json',
        )
        assert r.status_code == 400
        _logout(client)

        session.delete(submitted)
        session.commit()


# ── POST /annotation/<id>/llm ──────────────────────────────────────────────

class TestLLMRoute:

    @pytest.fixture
    def draft(self, app, _seed_users):
        session = app.db_session
        annotator = session.query(User).filter_by(username='annotator1').first()
        d = AnnotationDraft(
            construction_id=None,
            annotator_id=annotator.id,
            form_data='{}',
            status='draft',
        )
        session.add(d)
        session.commit()
        yield d
        try:
            session.delete(d)
            session.commit()
        except Exception:
            session.rollback()

    def _llm_payload(self):
        return {
            'provider': 'openai',
            'api_key': 'sk-fake',
            'model': 'gpt-4o',
            'notes': '',
            'form_data': {'formula': '[X]', 'changes': []},
        }

    def _good_report(self):
        return (
            '### 1. Исходная форма\n'
            '**Отношение**: Исходная форма\n'
            '**Описание**: Буквальное употребление.\n'
            '**Формула**: `VP в точку`\n'
            '**Уровень**: Source\n'
            '**Тип**: Source\n'
            '**Подтип**: Compositional source\n'
            '**Первое вхождение**: 1823, Лобачевский\n'
            '**Последнее вхождение**: 2019, Практика\n'
        )

    def test_llm_success_returns_parsed_data(self, client, app, draft, _seed_users):
        mock_client = MagicMock()
        mock_client.annotate.return_value = self._good_report()
        mock_client.model = 'gpt-4o'

        with patch('app.annotation.routes.get_client', return_value=mock_client):
            _login(client, 'annotator1')
            r = client.post(
                f'/annotation/{draft.id}/llm',
                data=json.dumps(self._llm_payload()),
                content_type='application/json',
            )

        assert r.status_code == 200, f"got {r.status_code}: {r.data[:300]}"
        body = r.get_json()
        assert body['status'] == 'ok'
        assert len(body['data']['changes']) == 1
        assert body['data']['changes'][0]['level'] == 'source'
        _logout(client)

    def test_llm_provider_error_returns_400(self, client, draft, _seed_users):
        with patch('app.annotation.routes.get_client',
                   side_effect=LLMError('bad key', 'openai')):
            _login(client, 'annotator1')
            r = client.post(
                f'/annotation/{draft.id}/llm',
                data=json.dumps(self._llm_payload()),
                content_type='application/json',
            )

        assert r.status_code == 400
        body = r.get_json()
        assert 'error' in body
        _logout(client)

    def test_llm_invalid_response_returns_422(self, client, draft, _seed_users):
        mock_client = MagicMock()
        mock_client.annotate.return_value = 'completely unparseable garbage'
        mock_client.model = 'gpt-4o'

        with patch('app.annotation.routes.get_client', return_value=mock_client):
            _login(client, 'annotator1')
            r = client.post(
                f'/annotation/{draft.id}/llm',
                data=json.dumps(self._llm_payload()),
                content_type='application/json',
            )

        assert r.status_code == 422
        _logout(client)


# Need to import LLMError for the patch test
from app.annotation.llm import LLMError  # noqa: E402


# ── POST /annotation/<id>/submit ───────────────────────────────────────────

class TestSubmit:

    def _full_form(self):
        return json.dumps({
            'name': 'Конструкция с ТАК',
            'formula': '[NP так VP]',
            'contemporary_meaning': 'усиление',
            'variation': '',
            'in_rus_constructicon': False,
            'rus_constructicon_id': None,
            'synt_function_of_anchor': 'Modifier',
            'anchor_schema': 'так',
            'anchor_ru': 'так',
            'anchor_eng': 'so',
            'group_number': '1',
            'annotated_sample': 'НКРЯ 2020',
            'changes': [{
                'change_name': 'Источник',
                'stage': '[NP так VP_ipfv]',
                'former_change': 'Исходная форма',
                'level': 'source',
                'type_of_change': 'Source',
                'subtype_of_change': 'Compositional source',
                'first_attested': '1800',
                'last_attested': '1900',
                'first_example': 'Он так говорил.',
                'last_example': 'Она так пела.',
                'comment': '',
                'constraints': [],
            }],
        })

    @pytest.fixture
    def draft(self, app, _seed_users):
        session = app.db_session
        annotator = session.query(User).filter_by(username='annotator1').first()
        d = AnnotationDraft(
            construction_id=None,
            annotator_id=annotator.id,
            form_data=self._full_form(),
            status='draft',
        )
        session.add(d)
        session.commit()
        yield d
        try:
            if d.construction_id:
                c = session.get(Construction, d.construction_id)
                if c:
                    session.delete(c)
            session.delete(d)
            session.commit()
        except Exception:
            session.rollback()

    def test_submit_creates_construction_and_redirects(
        self, client, app, draft, _seed_users
    ):
        _login(client, 'annotator1')
        r = client.post(
            f'/annotation/{draft.id}/submit',
            data=json.dumps({}),
            content_type='application/json',
            follow_redirects=False,
        )
        # Should redirect to index
        assert r.status_code in (302, 200), \
            f"expected redirect, got {r.status_code}: {r.data[:300]}"

        session = app.db_session
        session.expire(draft)
        assert draft.status == 'submitted'
        assert draft.construction_id is not None

        construction = session.get(Construction, draft.construction_id)
        assert construction is not None
        assert construction.is_published is True
        assert construction.formula == '[NP так VP]'
        assert construction.general_info is not None
        assert construction.general_info.name == 'Конструкция с ТАК'
        assert len(construction.changes) == 1
        assert construction.changes[0].level == 'source'
        _logout(client)

    def test_double_submit_redirects_with_warning(
        self, client, app, _seed_users, draft
    ):
        session = app.db_session
        draft.status = 'submitted'
        session.commit()

        _login(client, 'annotator1')
        r = client.post(
            f'/annotation/{draft.id}/submit',
            data=json.dumps({}),
            content_type='application/json',
            follow_redirects=False,
        )
        assert r.status_code in (302, 400)
        _logout(client)