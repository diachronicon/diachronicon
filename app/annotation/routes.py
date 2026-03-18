"""
Annotation blueprint routes.

URL prefix: /annotation

All routes require @login_required + @annotator_required.

Routes
------
GET  /annotation/                     — list current user's drafts
GET  /annotation/new                  — create blank draft, redirect to form
GET  /annotation/<draft_id>           — open draft form
POST /annotation/<draft_id>/save      — JSON autosave
POST /annotation/<draft_id>/llm       — trigger LLM annotation
POST /annotation/<draft_id>/submit    — finalise and publish
"""
from __future__ import annotations

import json
import logging
from datetime import datetime

from flask import (
    abort,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from flask_login import current_user, login_required

from app.annotation import bp
from app.annotation.constants import CHANGE_TAXONOMY_JSON_SAFE
from app.annotation.llm import LLMError, get_client
from app.annotation.parser import parse_llm_report, serialize_form
from app.auth.utils import annotator_required

logger = logging.getLogger(f'diachronicon.{__name__}')


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_draft_or_404(draft_id: int):
    from app.models import AnnotationDraft
    draft = current_app.db_session.get(AnnotationDraft, draft_id)
    if draft is None:
        abort(404)
    if current_user.role != 'admin' and draft.annotator_id != current_user.id:
        abort(403)
    return draft


def _publish_draft(draft, db_session) -> None:
    """Create Construction + related records from draft.form_data and publish.

    Mutates *draft* (sets construction_id, status). Does NOT commit.
    """
    from app.models import Change, Constraint, Construction, GeneralInfo

    try:
        data = json.loads(draft.form_data or '{}')
    except json.JSONDecodeError:
        data = {}

    # --- Construction -------------------------------------------------------
    construction = Construction(
        formula=data.get('formula') or '',
        contemporary_meaning=data.get('contemporary_meaning') or '',
        variation=data.get('variation') or '',
        in_rus_constructicon=bool(data.get('in_rus_constructicon', False)),
        rus_constructicon_id=data.get('rus_constructicon_id'),
        synt_function_of_anchor=data.get('synt_function_of_anchor') or '',
        anchor_schema=data.get('anchor_schema') or '',
        anchor_ru=data.get('anchor_ru') or '',
        anchor_eng=data.get('anchor_eng') or '',
        is_published=True,
        is_draft=False,
    )
    db_session.add(construction)
    db_session.flush()

    # --- GeneralInfo --------------------------------------------------------
    general_info = GeneralInfo(
        construction_id=construction.id,
        name=data.get('name') or '',
        group_number=data.get('group_number') or '',
        annotated_sample=data.get('annotated_sample') or '',
        status='ready',
    )
    db_session.add(general_info)

    # --- Changes ------------------------------------------------------------
    for ch_data in (data.get('changes') or []):
        if not isinstance(ch_data, dict):
            continue

        # Merge change_name into comment for storage
        # (change_name is annotation-workflow metadata; comment is the DB field)
        change_name = ch_data.get('change_name') or ''
        comment = ch_data.get('comment') or ''
        if change_name and change_name not in comment:
            comment = f'[{change_name}]\n\n{comment}' if comment else f'[{change_name}]'

        change = Change(
            construction_id=construction.id,
            stage=ch_data.get('stage') or '',
            former_change=ch_data.get('former_change') or '',
            level=ch_data.get('level') or '',
            type_of_change=ch_data.get('type_of_change') or '',
            subtype_of_change=ch_data.get('subtype_of_change') or '',
            first_attested=ch_data.get('first_attested') or '',
            last_attested=ch_data.get('last_attested') or '',
            first_example=ch_data.get('first_example') or '',
            last_example=ch_data.get('last_example') or '',
            comment=comment,
        )
        change.first_attested_year = Change.parse_year(change.first_attested)
        change.last_attested_year  = Change.parse_year(change.last_attested)

        db_session.add(change)
        db_session.flush()

        for con_data in (ch_data.get('constraints') or []):
            if not isinstance(con_data, dict):
                continue
            db_session.add(Constraint(
                change_id=change.id,
                construction_id=construction.id,
                element=con_data.get('element') or '',
                syntactic=con_data.get('syntactic') or '',
                semantic=con_data.get('semantic') or '',
            ))

    draft.construction_id = construction.id
    draft.status = 'submitted'


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@bp.route('/', methods=['GET'])
@login_required
@annotator_required
def index():
    from app.models import AnnotationDraft
    session = current_app.db_session

    if current_user.role == 'admin':
        drafts = (
            session.query(AnnotationDraft)
            .order_by(AnnotationDraft.updated_at.desc())
            .all()
        )
    else:
        drafts = (
            session.query(AnnotationDraft)
            .filter_by(annotator_id=current_user.id)
            .order_by(AnnotationDraft.updated_at.desc())
            .all()
        )

    return render_template(
        'annotation/index.html',
        title='Аннотирование',
        drafts=drafts,
    )


@bp.route('/new', methods=['GET'])
@login_required
@annotator_required
def new():
    from app.models import AnnotationDraft
    session = current_app.db_session

    draft = AnnotationDraft(
        construction_id=None,
        annotator_id=current_user.id,
        form_data='{}',
        status='draft',
    )
    session.add(draft)
    session.commit()

    logger.info('New draft %d created by user %d', draft.id, current_user.id)
    return redirect(url_for('annotation.form', draft_id=draft.id))


@bp.route('/<int:draft_id>', methods=['GET'])
@login_required
@annotator_required
def form(draft_id: int):
    draft = _get_draft_or_404(draft_id)

    try:
        form_data = json.loads(draft.form_data or '{}')
    except json.JSONDecodeError:
        form_data = {}

    return render_template(
        'annotation/form.html',
        title=f'Аннотирование — черновик #{draft.id}',
        draft=draft,
        form_data=form_data,
        taxonomy=CHANGE_TAXONOMY_JSON_SAFE,
    )


@bp.route('/<int:draft_id>/save', methods=['POST'])
@login_required
@annotator_required
def save(draft_id: int):
    draft = _get_draft_or_404(draft_id)

    if draft.status == 'submitted':
        return jsonify({'error': 'Эта аннотация уже опубликована.'}), 400

    payload = request.get_json(silent=True) or {}
    cleaned = serialize_form(payload)

    draft.form_data  = json.dumps(cleaned, ensure_ascii=False)
    draft.updated_at = datetime.utcnow()
    current_app.db_session.commit()

    return jsonify({
        'status': 'ok',
        'updated_at': draft.updated_at.strftime('%H:%M:%S'),
    })


@bp.route('/<int:draft_id>/llm', methods=['POST'])
@login_required
@annotator_required
def llm(draft_id: int):
    draft = _get_draft_or_404(draft_id)

    if draft.status == 'submitted':
        return jsonify({'error': 'Эта аннотация уже опубликована.'}), 400

    payload = request.get_json(silent=True) or {}
    provider  = (payload.get('provider') or '').strip()
    api_key   = (payload.get('api_key')  or '').strip()
    model     = (payload.get('model')    or '').strip() or None

    # Current form state used as context for the LLM
    current_form_data = payload.get('form_data') or {}
    construction_text = (
        current_form_data.get('contemporary_meaning')
        or current_form_data.get('name')
        or ''
    )
    formula = current_form_data.get('formula') or ''

    # Annotator-supplied corpus notes / extracts (free-text field in the form)
    notes = (payload.get('notes') or current_form_data.get('notes') or '').strip()

    try:
        client      = get_client(provider, api_key, model)
        raw_report  = client.annotate(construction_text, formula, notes)
    except LLMError as exc:
        logger.warning('LLM error (%s): %s', exc.provider, exc)
        return jsonify(exc.to_dict()), 400

    parsed = parse_llm_report(raw_report)

    if '_parse_error' in parsed:
        return jsonify({
            'error':      parsed['_parse_error'],
            'provider':   provider,
            'raw_report': raw_report,   # return raw so annotator can inspect
        }), 422

    # Persist provider/model metadata
    draft.llm_provider = provider
    draft.llm_model    = model or client.model
    current_app.db_session.commit()

    logger.info(
        'LLM annotation complete for draft %d (provider=%s, model=%s, changes=%d)',
        draft_id, provider, draft.llm_model, len(parsed.get('changes', [])),
    )
    return jsonify({
        'status':     'ok',
        'data':       parsed,
        'raw_report': raw_report,   # returned so the UI can show it if needed
    })


@bp.route('/<int:draft_id>/submit', methods=['POST'])
@login_required
@annotator_required
def submit(draft_id: int):
    draft = _get_draft_or_404(draft_id)

    if draft.status == 'submitted':
        flash('Эта аннотация уже опубликована.', 'warning')
        return redirect(url_for('annotation.index'))

    payload = request.get_json(silent=True)
    if payload:
        cleaned = serialize_form(payload)
        draft.form_data = json.dumps(cleaned, ensure_ascii=False)

    try:
        _publish_draft(draft, current_app.db_session)
        current_app.db_session.commit()
    except Exception as exc:
        current_app.db_session.rollback()
        logger.exception('Failed to publish draft %d', draft_id)
        flash(f'Ошибка при публикации: {exc}', 'danger')
        return redirect(url_for('annotation.form', draft_id=draft_id))

    flash('Конструкция опубликована!', 'success')
    logger.info('Draft %d published as construction %d', draft_id, draft.construction_id)
    return redirect(url_for('annotation.index'))