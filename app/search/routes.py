"""app/search/routes.py

Search routes for the Diachronicon.

Two search modes are supported:
  Exact search  — structured filters via SQLQuery (formula, level, dates, etc.)
  Semantic search — free-text query embedded with sentence-transformers,
                    scored by cosine similarity against ConstructionEmbedding.
  Hybrid        — semantic pre-filtering combined with exact SQL constraints.

The active route is GET/POST /search/ backed by the SingleForm WTForms class.
Results are passed to search_2.html together with a `search_mode` string so
the template can label what kind of results are shown.
"""
from __future__ import annotations

import logging
import typing as T
from datetime import datetime

import wtforms
import wtforms.validators
from flask import current_app, render_template, request
from flask_wtf import FlaskForm
from sqlalchemy import select

from app.models import (
    Change,
    Construction,
    ConstructionEmbedding,
    DBModel,
    FormulaElement,
    GeneralInfo,
)
from app.search import bp
from app.search.query_sqlalchemy import SQLQuery, default_sqlquery
from app.search.search_form import (
    BootstrapIntegerField,
    BootstrapStringField,
    BoostrapSelectField,
    DataList,
)
from app.utils import find_unique
from app.models import SYNT_FUNCTION_OF_ANCHOR_VALUES, UNKNOWN_SYNT_FUNCTION_OF_ANCHOR

logger = logging.getLogger(f"diachronicon.{__name__}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def group_rows_by_construction(rows) -> T.Dict:
    result: T.Dict = {}
    for row in rows:
        result.setdefault(row["id"], []).append(row)
    return result


def _has_exact_filters(form_data: T.Dict) -> bool:
    """Return True if any exact-search field in the form is non-empty."""
    skip = {"csrf_token", "semantic_query"}

    def _check(d):
        if isinstance(d, dict):
            return any(
                bool(v) and k not in skip
                for k, v in d.items()
                if not isinstance(v, (dict, list))
            ) or any(_check(v) for v in d.values() if isinstance(v, (dict, list)))
        if isinstance(d, list):
            return any(_check(item) for item in d)
        return False

    return _check(form_data)


# ---------------------------------------------------------------------------
# WTForms form classes
# ---------------------------------------------------------------------------

class ConstructionForm(FlaskForm):
    """Exact filters that map to Construction / GeneralInfo columns."""

    semantic_query = BootstrapStringField(
        label="Поиск по смыслу",
        description="Свободный текст — ищет по близости значения",
    )

    formula = BootstrapStringField(label="Формула")

    _meaning_datalist_id = "meaning_values"
    _meaning_values = find_unique(Construction, "contemporary_meaning")
    _meaning_datalist = DataList(id=_meaning_datalist_id,
                                 literal_options=_meaning_values)
    contemporary_meaning = BootstrapStringField(
        label="Значение",
        render_kw=dict(
            div_extra_contents=[_meaning_datalist],
            list=_meaning_datalist_id,
        ),
        description="значение конструкции в последний период",
    )

    in_rus_constructicon = BoostrapSelectField(
        label="Есть в конструктиконе?",
        choices=[
            ("", "Есть в конструктиконе?"),
            ("True", "Да"),
            ("False", "Нет"),
        ],
        render_kw=dict(selected=""),
        coerce=lambda val: val == "True",
    )

    num_changes__from = BootstrapIntegerField(
        label="Количество изменений (от)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)],
    )
    num_changes__to = BootstrapIntegerField(
        label="Количество изменений (до)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)],
    )


class AnchorForm(FlaskForm):
    """Exact filters on anchor / syntactic-function fields."""

    _synt_functions_anchor = [
        v for v in SYNT_FUNCTION_OF_ANCHOR_VALUES
        if v != UNKNOWN_SYNT_FUNCTION_OF_ANCHOR
    ]
    _synt_functions_datalist_id = "synt_function_of_anchor_values"
    _synt_functions_datalist = DataList(
        id=_synt_functions_datalist_id,
        literal_options=_synt_functions_anchor,
    )

    synt_function_of_anchor = BootstrapStringField(
        label="Синт. функция якоря",
        render_kw=dict(
            div_extra_contents=[_synt_functions_datalist],
            list=_synt_functions_datalist_id,
        ),
    )

    anchor_schema = BootstrapStringField(label="Схема якоря")
    anchor_ru = BootstrapStringField(label="Якорь (рус.)")

    anchor_length__from = BootstrapIntegerField(
        label="Длина якоря (от)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)],
    )
    anchor_length__to = BootstrapIntegerField(
        label="Длина якоря (до)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)],
    )


class ChangeForm(FlaskForm):
    """Exact filters on individual Change records."""

    formula = BootstrapStringField(label="Формула на этом этапе", name="stage")

    _stages_datalist_id = "stage_numbers"
    _stages_datalist = DataList(
        id=_stages_datalist_id,
        with_attr_options=[
            {"label": "первый", "value": 1},
            {"label": "последний", "value": -1},
            {"label": "предпоследний", "value": -2},
        ],
    )
    stage_abs = BootstrapIntegerField(
        label="Этап в истории конструкции",
        render_kw=dict(div_extra_contents=[_stages_datalist],
                       list=_stages_datalist_id),
        validators=[wtforms.validators.Optional(strip_whitespace=True)],
    )

    level = BoostrapSelectField(
        label="Уровень изменения",
        choices=[
            ("", "Уровень изменения?"),
            ("synt", "Синтаксическое"),
            ("sem", "Семантическое"),
            ("source", "Источник"),
        ],
    )

    _type_datalist_id = "types_of_change_values"
    _types_of_change = find_unique(Change, "type_of_change")
    _type_datalist = DataList(id=_type_datalist_id,
                              literal_options=_types_of_change)
    type_of_change = BootstrapStringField(
        label="Тип изменения",
        render_kw=dict(div_extra_contents=[_type_datalist],
                       list=_type_datalist_id),
    )

    _subtype_datalist_id = "subtypes_of_change_values"
    _subtypes_of_change = find_unique(Change, "subtype_of_change")
    _subtype_datalist = DataList(id=_subtype_datalist_id,
                                 literal_options=_subtypes_of_change)
    subtype_of_change = BootstrapStringField(
        label="Подтип изменения",
        render_kw=dict(div_extra_contents=[_subtype_datalist],
                       list=_subtype_datalist_id),
    )

    duration__from = BootstrapIntegerField(
        label="Длительность от (лет)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)],
    )
    duration__to = BootstrapIntegerField(
        label="Длительность до (лет)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)],
    )

    first_attested__from = BootstrapIntegerField(
        label="Первое вхождение от (год)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)],
    )
    first_attested__to = BootstrapIntegerField(
        label="Первое вхождение до (год)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)],
    )

    last_attested__from = BootstrapIntegerField(
        label="Последнее вхождение от (год)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)],
    )
    last_attested__to = BootstrapIntegerField(
        label="Последнее вхождение до (год)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)],
    )


class SingleForm(FlaskForm):
    """Root form combining construction, anchor, and change sub-forms."""
    construction = wtforms.FormField(ConstructionForm)
    anchor = wtforms.FormField(AnchorForm)
    changes = wtforms.FieldList(wtforms.FormField(ChangeForm), min_entries=3)


# ---------------------------------------------------------------------------
# Search route
# ---------------------------------------------------------------------------

@bp.route('/search/', methods=['GET', 'POST'])
def search():
    """Hybrid search: exact SQL filters + optional semantic vector search."""
    form = SingleForm()

    # Populate form from GET args (bookmarkable search URLs)
    if request.args:
        form.process(request.args)

    year = datetime.now().year

    # Empty form — just render
    if not (request.args or request.method == 'POST'):
        return render_template(
            'search_2.html', _form=form, title="Поиск", year=year,
        )

    # ------------------------------------------------------------------
    # Determine which modes are active
    # ------------------------------------------------------------------
    form_data = form.data
    semantic_text: str = (
        form_data.get("construction", {}).get("semantic_query", "") or ""
    ).strip()
    has_exact = _has_exact_filters(form_data)
    has_semantic = bool(semantic_text)

    if not has_exact and not has_semantic:
        # Form was submitted but completely empty
        return render_template(
            'search_2.html', _form=form, title="Поиск", year=year,
        )

    # ------------------------------------------------------------------
    # Semantic search — get ranked construction IDs
    # ------------------------------------------------------------------
    semantic_ids: T.Optional[T.List[int]] = None
    semantic_scores: T.Dict[int, float] = {}

    if has_semantic:
        try:
            from app.search.semantic import semantic_search
            ranked = semantic_search(semantic_text, current_app.db_session, top_k=50)
            semantic_ids = [cid for cid, _ in ranked]
            semantic_scores = {cid: score for cid, score in ranked}
        except ImportError:
            # sentence-transformers not installed — degrade gracefully
            logger.warning(
                "sentence-transformers not installed; semantic search disabled."
            )
            has_semantic = False

    # ------------------------------------------------------------------
    # Exact SQL search via SQLQuery
    # ------------------------------------------------------------------
    results_by_constr: T.Dict = {}
    search_mode = "none"

    if has_exact:
        query: SQLQuery = default_sqlquery()
        query.parse_form(form_data)
        stmt = query.query()

        # If semantic pre-filtering produced IDs, constrain the SQL to those
        if semantic_ids is not None:
            stmt = stmt.where(Construction.id.in_(semantic_ids))

        with current_app.engine.connect() as conn:
            rows = conn.execute(stmt).mappings().all()

        results_by_constr = group_rows_by_construction(rows)
        search_mode = "hybrid" if has_semantic else "exact"

    elif has_semantic and semantic_ids is not None:
        # Semantic-only: fetch lightweight construction data for the ranked IDs
        if semantic_ids:
            stmt = (
                select(
                    Construction.id,
                    Construction.formula,
                    GeneralInfo.name,
                )
                .join_from(Construction, GeneralInfo, isouter=True)
                .where(Construction.id.in_(semantic_ids))
                .where(Construction.is_published.is_(True))
            )
            with current_app.engine.connect() as conn:
                rows = conn.execute(stmt).mappings().all()

            # Re-order rows by semantic score (highest first)
            rows_sorted = sorted(
                rows, key=lambda r: semantic_scores.get(r["id"], 0), reverse=True
            )
            results_by_constr = group_rows_by_construction(rows_sorted)
        search_mode = "semantic"

    return render_template(
        'search_2.html',
        _form=form,
        title="Поиск: результаты",
        year=year,
        results_by_constr=results_by_constr,
        n_results=len(results_by_constr),
        search_mode=search_mode,
        semantic_query=semantic_text,
        use_constr=True,
    )