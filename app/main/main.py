import typing as T
from datetime import datetime
from json import dumps as json_dumps

import sqlalchemy.sql.expression
from sqlalchemy import (
    select,
    or_,
    and_
)
from sqlalchemy.orm import (
    aliased,
    joinedload,
    selectinload,
    Load,
    load_only
)
import wtforms
import wtforms.validators
from flask import current_app
from flask import render_template, abort, request, redirect, url_for, jsonify
from flask_wtf import FlaskForm

from app.models import (
    Construction,
    Change,
    GeneralInfo,
    Constraint,
    FormulaElement,
    ConstructionVariant,
    DBModel,
    Model2Field2Val
)
import app.database

from app.main import bp
from app.search.search_form import (
    make_sign_options_for_param,
    make_options_from_values,
    DataList,
    BootstrapBooleanField,
    BoostrapSelectField,
    BootstrapStringField,
    BootstrapIntegerField,
)
from app.search.query_sqlalchemy import (
    default_sqlquery,
    SQLQuery,
    SQLSubForm,
    SQLTokensQuery,
)
from app.search.routes import (
    group_rows_by_construction,
)
from app.utils import (
    find_unique
)

# STRIPABLE = "()/[],."
STRIPABLE = "()"


class SimpleSearchForm(FlaskForm):
    _constructions_datalist_id = "construction_values"
    # _construction_values = find_unique(Construction, "formula")
    _construction_values = find_unique(GeneralInfo, "name")
    _constructions_datalist = DataList(
        id=_constructions_datalist_id,
        literal_options=_construction_values
    )

    formula = BootstrapStringField(
        label="Выберите конструкцию",
        render_kw=dict(
            div_extra_contents = [_constructions_datalist],
            list = _constructions_datalist_id,
            autocomplete = "off"
        ),
        # description="формула конструкции",
    )


def get_first_alternative(s: str) -> str:
    return s.split("/")[0]


def clean_formula(formula: str, choose_first_alt: bool=True):
    # fixes search for whole construction as suggested by datalist
    conversion = (lambda x: x) if not choose_first_alt else get_first_alternative
    return " ".join([conversion(elem.strip("()")) for elem in formula.split()])


@bp.route('/', methods=["GET", "POST"])
@bp.route('/index/', methods=["GET", "POST"])
def main():
    simple_form = SimpleSearchForm()

    if simple_form.is_submitted():
        print(f'FORM SUBMITTED')

        queried_formula = simple_form.data["formula"]
        print(queried_formula)
        final_formula = clean_formula(queried_formula)

        print(f"clean formula: {final_formula}")

        # stmt = select(Construction).where(Construction.formula == queried_formula)
        q = SQLQuery()
        # q.parse_form({"construction": {"formula": final_formula}})
        q.parse_form({"general_info": {"name": queried_formula}})
        # query = SQLTokensQuery("formula", queried_formula, Construction)
        # stmt = query.query(select(Construction, q, q))
        stmt = q.query()
        print(stmt)
        print(stmt.compile(compile_kwargs={"literal_binds": True}))

        with current_app.engine.connect() as conn:
            results = conn.execute(stmt).mappings().all()

        print(results)

        by_constr = group_rows_by_construction(results)
        results = [constrs[0] for id_, constrs in by_constr.items()]

        print(results)

        return redirect(url_for("search.construction", index=results[0]["id"]))

        return render_template(
            'main.html', title='Главная',
            _form=simple_form,
            results=results,
            results_by_constr=by_constr
        )
    
    return render_template(
        'main.html', title='Главная',
        _form=simple_form,
    )


@bp.route("/about")
def about():
    return render_template(
        # '/errors/404.html',
        "about.html",
        # message="Page under construction"
        title='Описание',
        year=datetime.now().year,
    )   


@bp.route("/api/constructions")
def constructions_list():
    stmt = select(
        GeneralInfo.name.distinct(),
        GeneralInfo.construction_id
    ).order_by(GeneralInfo.construction_id)

    with current_app.engine.connect() as conn:
        results = conn.execute(stmt).all()

    print(type(results))
    print(type(results[0]))
    dict_results = [{"id": res.construction_id, "name": res.name} for res in results]

    return jsonify(dict_results)

