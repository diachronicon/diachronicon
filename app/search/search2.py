import re
import typing as T
from typing import Tuple, List, Dict, Union, Type, Optional

from datetime import datetime
from itertools import chain
import logging
from operator import (
    le,
    ge,
    eq
)

# import pandas as pd
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
from flask import render_template, abort, request, redirect
from flask_wtf import FlaskForm
from app.models import SYNT_FUNCTION_OF_ANCHOR_VALUES
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
from app.search import bp
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
)
from app.utils import (
    find_unique
)


logger = logging.getLogger(f"diachronicon.{__name__}")
logger.setLevel(logging.ERROR)
logger.handlers.clear()
logger.addHandler(logging.NullHandler())

_OPERATORS = {'le': le, 'ge': ge, 'eq': eq}
# CHANGE_COLUMNS = [str(col).removeprefix('change.')  # instead of `.removeprefix(')
#                   for col in Change.__table__.columns]

MEANING_VALUES = []  # Construction.contemporary_meaning.unique()
SYNT_FUNCTIONS_ANCHOR = [v for v in SYNT_FUNCTION_OF_ANCHOR_VALUES
                         if v != "<unknown>"]
TYPES_OF_CHANGE = []
CONSTRUCTION_NAMES = []


HTML_NAME2MODEL = {
    'c': Construction,
    'constraint': Constraint,
    'change': Change,
    # '': GeneralInfo,
}

html_name2table_name = {
    'meaning': 'contemporary_meaning'
}


def make_basic_formula_query(stmt: sqlalchemy.sql.expression.Select,
                             model: Construction, value: str,
                             *args, column="formula", **kwargs
                             ):
    return stmt.where(getattr(model, column).like(f"%{value}%"))


def _make_skip_optional_subquery(cur_elem, distance, model=FormulaElement):
    return select(model.id).where(
        FormulaElement.construction_id == cur_elem.construction_id,
        FormulaElement.is_optional == True,
        cur_elem.order == FormulaElement.order + distance,
    )


def make_byelem_formula_query(
    stmt: sqlalchemy.sql.expression.Select,
    model: Type[FormulaElement], value: str,
    *args
):
    """Update stmt to filter by formula elementwise, allowing simple regex/logic

    :param stmt:
    :param model:
    :param value:
    :return:
    """
    elements = value.split(' ')
    print(elements)

    element_tables = [aliased(FormulaElement) for i in range(len(elements))]

    # params = {}

    stmt = stmt.where(getattr(model, 'id') == element_tables[0].construction_id)

    for i, element_value in enumerate(elements):
        cur_elem_table = element_tables[i]

        # TODO: опционально пропускать при поиске по элементам те, что в скобках
        # TODO: asterisk instead of element
        if i != 0:
            stmt = stmt.where(
                cur_elem_table.construction_id == element_tables[0].construction_id,
                or_(
                    cur_elem_table.order == element_tables[i-1].order + 1,
                    and_(
                        cur_elem_table.order == element_tables[i-1].order + 2,
                        _make_skip_optional_subquery(cur_elem_table, 1).exists()
                    )
                )
            )

        # params[f'gloss{i}'] = value  # or text(value)
        corrected_val = element_value.replace('*', '%')
        stmt = stmt.where(getattr(cur_elem_table, 'value').ilike(corrected_val))

    return stmt


def make_formula_query(stmt, model, value, params_values):
    if '*' in value:
        return make_byelem_formula_query(stmt, model, value)
    return make_basic_formula_query(stmt, model, value)


def make_duration_query(stmt, model, _, params_values):
    # TODO: mutating while iterating is baaad
    duration_value = params_values.pop('duration')
    duration_sign = params_values.pop('duration_sign')
    # logger.debug(f"in duration: {duration_sign}, {duration_value}, {type(duration_value)} ")
    if not duration_sign:
        raise ValueError
    # if not duration_sign:
    #     logger.warning(f"no argument in form: `duration-sign`")
    #     return stmt

    # if not duration_value.isdigit():
    #     logger.warning(f"")

    op = _OPERATORS[duration_sign]
    logger.info(f"op and value are: {op} {duration_value}")
    return stmt.where(
        # op(model.last_attested - model.first_attested, duration_value)
        op(getattr(model, "last_attested") - getattr(model, "first_attested"),
           duration_value)
    )


param2query_maker = {
    # 'formula': make_formula_query,
    'formula': make_formula_query,
    "anchor_ru": make_formula_query,
    "anchor_en": lambda *args, **kwargs: make_basic_formula_query(
        *args, **kwargs, column="anchor_en"),
    "anchor_eng": lambda *args, **kwargs: make_basic_formula_query(
        *args, **kwargs, column="anchor_eng"),
    'duration': make_duration_query,
    'duration_sign': make_duration_query,
    "stage": lambda *args, **kwargs: make_basic_formula_query(
        *args, **kwargs, column="stage"),
    'element': lambda *args, **kwargs: make_basic_formula_query(
        *args, **kwargs, column="element"),
}


param2type_caster = {
    "first_attested": int,
    "last_attested": int,
    "duration": int,
    "in_rus_constructicon": lambda val: True if val == "on" else False
}


class BaseQuery:
    name_sep = "-"
    name_prefix: str
    basic_query_model = Construction
    basic_query_fields = ["id", "formula"]

    def __init__(self, **kwargs):
        super().__init__()
        for name, val in kwargs.items():
            setattr(self, name, val)

    @classmethod
    def from_submitted_args(cls, args):
        queried = {}

        prefix = cls.name_prefix + cls.name_sep
        len_prefix = len(prefix)

        logger.debug(str(prefix))
        for name, val in args.items():
            print(name, name.startswith(prefix), bool(val), val)
            if name.startswith(prefix) and val:
                actual_name = name[len_prefix:]
                queried[actual_name] = val

        logger.debug(str(queried))
        return cls(**queried)

    def _make_basic_query(self, *models: DBModel):
        basic_model = self.basic_query_model
        basic_fields = []
        if not basic_model in models:
            basic_fields = [getattr(basic_model, field)
                            for field in self.basic_query_fields]

        stmt = select(*basic_fields, *models)
        logger.info(f"base sql:\n{str(stmt)}")
        return stmt

    def make_basic_query(self):
        return self._make_basic_query()

    @staticmethod
    def tokenize_formula(formula):
        return [elem.replace("*", "%") for elem in formula.split()]

    def query_formula_element_regex(
        self, formula: str, formula_of_model: DBModel,
        cur_stmt: sqlalchemy.sql.expression.Select = None
    ):
        if cur_stmt is None:
            cur_stmt = self._make_basic_query(formula_of_model)

        elements = self.tokenize_formula(formula)

        first_element = elements[0]
        first_table = FormulaElement

        stmt = cur_stmt.where(formula_of_model.id == first_table.construction_id,
                              first_table.value.ilike(first_element))

        remaining_elements = elements[1:]
        element_tables = (
            [first_table]
            + [aliased(FormulaElement) for _ in range(len(remaining_elements))]
        )

        for i, element_value in enumerate(remaining_elements):
            cur_elem_table = element_tables[i]

            # TODO: опционально пропускать при поиске по элементам те, что в скобках
            # TODO: asterisk instead of element
            stmt = stmt.where(
                cur_elem_table.construction_id == first_table.construction_id,
                or_(
                    cur_elem_table.order == element_tables[i - 1].order + 1,
                    and_(
                        cur_elem_table.order == element_tables[i - 1].order + 2,
                        _make_skip_optional_subquery(cur_elem_table, 1).exists()
                    ))
            )

            # params[f'gloss{i}'] = value  # or text(value)
            stmt = stmt.where(getattr(cur_elem_table, 'value').ilike(element_value))

        return stmt

    def __repr__(self):
        return (
            f"{self.__class__.__name__}"
            f"({ ', '.join(f'{key}={val!r}' for key, val in vars(self).items()) })"
        )


class ChangeQuery(BaseQuery):
    name_prefix = "change"
    # __slots__ = ("construction_id", "stage", "level", "type_of_change",
    #              "first_attested")


class ConstraintQuery(BaseQuery):
    name_prefix = "change"


class ConstructionQuery(BaseQuery):
    name_prefix = "c"


# def _make_basic_select_list(columns=("id", "formula")) -> List[sqlalchemy.column]:
#     return [getattr(Construction, column)
#             for column in columns]


def make_select(
    model2items: Model2Field2Val, basic_construction_columns=("id", "formula"),
    count_construction_columns=("variants", "changes", "construction"),
    # non_queried_to_join = (ConstructionVariant),
    ignore_params=re.compile(r"duration"),
    model2basic_columns={Change: ("stage", "level", "type_of_change")}
) -> sqlalchemy.sql.expression.Select:
    """Make a minimal select based on mapping of model to params and values"""
    # basic_select_list = _make_basic_select_list(basic_construction_columns)
    print("in make_select")

    no_construction_model2items = {model: items
                                   for model, items in model2items.items()
                                   if model is not Construction}
    print(no_construction_model2items)
    # we always query construction
    stmt = select(Construction, *no_construction_model2items)
    print(stmt)

    # other models, like `Change` or `Constraint` must be joined to Construction
    #  (this is helped by `relationship` in their definition)
    # TODO: does querying variants or formula_element's require select here?
    for model, items in model2items.items():
        if model is Construction:
            continue

        print(model, items)
        stmt = stmt.join_from(Construction, model)
        print(f"updated stmt: {stmt}")

    construction_columns = list(basic_construction_columns)
    print(model2items.get(Construction, []))
    for column in model2items.get(Construction, []):
        print(column)
        if column not in basic_construction_columns:
            construction_columns.append(column)
    print(f"constr cols: {construction_columns}")

    # TODO: does filtering by `__dict__` always work? False positives/negatives?
    # load basic and queried columns of Construction
    stmt = stmt.options(
        Load(Construction).load_only(
            *[getattr(Construction, column) for column in construction_columns
              if column in Construction.__dict__])
    )

    print(f"select with load_only and join:\n{stmt}")

    # load queried columns of other tables
    for model, items in no_construction_model2items.items():
        print(f"model, items: {model}, {items}")
        basic_columns = model2basic_columns.get(model, [])
        for item in items:
            columns = chain(basic_columns, item)
            stmt = stmt.options(Load(model).load_only(
                *[getattr(model, column) for column in columns
                  if column in model.__dict__])
            )
            print(f"updated statement:\n{stmt}")

    return stmt


def extract_queried(
    args: Dict[str, str], parts_sep='-', do_conversion=True
) -> Tuple[Model2Field2Val, Model2Field2Val]:
    """Extract query parameters and map them to models

    :return
    """
    print(f"in extract")

    model2field2val = current_model2query = {}
    model2derivable_param2val = {}

    for key, value in args.items():
        if not value:
            continue

        logger.debug(f"{key} -- {value}")

        key_model_part, key_param_part = key.split(parts_sep, maxsplit=1)
        key_param_part = html_name2table_name.get(key_param_part) or key_param_part

        current_model = HTML_NAME2MODEL[key_model_part]
        # there could be multiple constraints or changes coded
        #   in html as `constraint-3-element` for example
        if parts_sep not in key_param_part:
            is_param_singleton = True
            key_param = key_param_part
        else:
            is_param_singleton = False
            i, key_param = key_param_part.split(parts_sep, maxsplit=1)

        print(key_param, current_model, key_param in current_model.__dict__,
              current_model.__dict__)
        if key_param not in current_model.__dict__:
            current_model2query = model2derivable_param2val

        if key_param in param2type_caster:
            cast_func = param2type_caster[key_param]
            value = cast_func(value)

        if is_param_singleton:
            current_model2query.setdefault(
                current_model, {}
            )[key_param_part] = value
            continue

        # here the key is of the type `change-1-first_attested`
        model_list = current_model2query.setdefault(current_model, [])

        i = int(i)

        print(i, key_param, model_list, len(model_list) < i)

        if len(model_list) < i:
            model_list.append({key_param: value})
        else:
            model_list[i-1][key_param] = value

    print(f"end of extract:\n{model2field2val}\n{model2derivable_param2val}")
    return model2field2val, model2derivable_param2val


def add_base_fields_for_derivable(
    model2field2val: Model2Field2Val,
    model2derivable_param2val: Model2Field2Val
) -> None:
    print(f"in add_base_fields_for_derivable")

    # duration
    for i, change_desc in enumerate(model2derivable_param2val.get(Change, [])):
        if "duration" in change_desc:
            # TODO: we could also add it only to the first dict,
            #   which is enough to be included?
            orig_changes = model2field2val.setdefault(Change, [])
            if i < len(orig_changes):
                orig_changes[i].update({"first_attested": None,
                                        "last_attested": None})
            else:  # TODO: is this proper?
                orig_changes.append({"first_attested": None, "last_attested": None})

    # attestation
    for i, change_desc in enumerate(model2field2val.get(Change, [])):
        if ("first_attested" in change_desc) ^ ("last_attested" in change_desc):
            change_desc["first_attested"] = change_desc.get("first_attested", None)
            change_desc["last_attested"] = change_desc.get("last_attested", None)


def build_query(
    args: Dict[str, str], parts_sep='-', ready_only=False,
    # i_to_zero_base=True
) -> sqlalchemy.sql.expression.Select:
    """Collect html params into database query"""
    # TODO: use Bundles? One for that which is constant and duplicated in rows
    #   and the other for what was queried at the start

    model2field2val, model2derivable_param2val = extract_queried(args)

    print(model2field2val, model2derivable_param2val, sep="\n")
    # if not model2field2val:
    #     return

    add_base_fields_for_derivable(model2field2val, model2derivable_param2val)
    print("after add_base_..")
    print(model2field2val, model2derivable_param2val, sep="\n")

    if ready_only:
        model2field2val.setdefault(GeneralInfo, {})['status'] = 'ready'

    # make the query itself
    # stmt = select(*[model for model in items_by_model])
    stmt = make_select(model2field2val)
    print(f"the basic select is\n{stmt}")

    for model, params_values in model2field2val.items():
        if not isinstance(params_values, list):
            pass
        else:
            # TODO
            #   code for aliases? How should multiple constraints or multiple
            #   changes be connected?
            params_values = params_values[0]

        for param, val in params_values.items():
            print(f"model, param, val: {model}, {param}, {val}")
            # if '_' in param:  # a helper parameter
            #     continue

            if val is None:
                continue

            if param in param2query_maker:
                # special processing of certain search fields
                print(param)
                stmt = param2query_maker[param](stmt, model, val, params_values)
            else:
                # a simple equality testing
                stmt = stmt.where(getattr(model, param) == val)

    changes = model2derivable_param2val.get(Change, [])
    for change in changes:
        if "duration" in change:
            stmt = make_duration_query(stmt, Change, change["duration"], change)
        # TODO: remove once searching many is supported
        break

    print("final select is:",
          stmt.compile(compile_kwargs={"literal_binds": True}), sep="\n")

    return stmt


def group_rows_by_construction(rows: List[Dict]) -> Dict:
    row_id2data = {}

    for row in rows:
        id_ = row["id"]
        row_id2data.setdefault(id_, []).append(row)

    return row_id2data


def reduce_rows(rows: Dict):
    final_res = {}
    for constr_id, results in rows.items():
        print(constr_id, results)
        one_res = results[0]
        if len(one_res) == 3 and set(one_res) == {"id", "formula", "name"}:
            final_res[constr_id] = [one_res]
        else:
            final_res[constr_id] = results

    return final_res


# @bp.route('/search/', methods=['GET', 'POST'])
# def search():
#     """Search view"""

#     # TODO: implement as singletons?
#     try:
#         meaning_values = Construction.contemporary_meaning.unique()
#     except (ValueError, TypeError):
#         meaning_values = MEANING_VALUES
#     try:
#         synt_functions_anchor = Construction.synt_function_of_anchor.type.enums
#     except (ValueError, TypeError):
#         synt_functions_anchor = SYNT_FUNCTIONS_ANCHOR
#     types_of_change = TYPES_OF_CHANGE
#     try:
#         with current_app.engine.connect() as conn:
#             q = conn.execute(select(Change.type_of_change).order_by(
#                 Change.type_of_change.asc()).distinct())
#             types_of_change = q.scalars().all()
#     except Exception as e:
#         print(e)
#         raise e
#         print("-" * 50)
#         types_of_change = TYPES_OF_CHANGE

#     print(types_of_change)

#     query_args = request.args
#     print(*query_args.items(), sep='\n')
#     logger.debug(f"{query_args}")

#     # a GET request with
#     #   - no parameters or unfilled parameters
#     #   - or a `no-search` flag (usually after linking from construction page)
#     if (request.method != 'POST'
#         and (not (request.args and any(val for key, val in query_args.items()))
#              or query_args.get('no-search') == '1')
#     ):
#         return render_template(
#             'search.html',
#             title='Поиск',
#             year=datetime.now().year,
#             meaning_values=meaning_values,
#             synt_functions_anchor=synt_functions_anchor,
#             query=request.args,
#         )

#     print(f'in conditional')

#     print(request.form)
#     print(request.data)

#     stmt = build_query(query_args)

#     print("built stmt")

#     with current_app.engine.connect() as conn:
#         results = conn.execute(stmt).mappings().all()

#     for row in results:
#         print(type(row))
#         print(row)
#         # print(row._fields)
#         # for field in row:
#         #     print(vars(field))
#         # print(row._asdict())
#     row_id2data = reduce_rows(group_rows_by_construction(results))

#     print(row_id2data)
#     print("formula" in query_args, query_args, sep="\n")

#     changes_queried = any(field in query_args for field in [
#         'change-1-stage', 'change-1-stage-abs', 'change-1-level',
#         'change-1-type_of_change', 'change-1-duration_sign'
#     ])

#     return render_template(
#         'search.html',
#         title='Поиск: результаты',
#         year=datetime.now().year,
#         meaning_values=meaning_values,
#         synt_functions_anchor=synt_functions_anchor,
#         types_of_change=types_of_change,
#         form_input=request.form,
#         results=row_id2data,
#         n_param_results=len(results),
#         query=query_args,
#         changes_queried=changes_queried
#     )



def safe_get(callable: T.Callable[[], T.Optional[T.List[str]]], default=None):
    try:
        return callable()
    except (ValueError, TypeError):
        return default


class ConstructionForm(FlaskForm):
    # construction_id = BootstrapStringField(
    #     label="id конструкции", name="construction_id",
    # )
    formula = BootstrapStringField(
        label="Формула", name="formula",
    )

    _meaning_datalist_id = "meaning_values"
    # _meaning_values = safe_get(Construction.contemporary_meaning.unique) or MEANING_VALUES
    _meaning_values = find_unique(Construction, "contemporary_meaning")
    _meaning_datalist = DataList(
        id=_meaning_datalist_id,
        literal_options=_meaning_values)
    contemporary_meaning = BootstrapStringField(
        label="Значение", name="contemporary_meaning",
        render_kw=dict(
            # label_extra_text = Markup('<span class="symbol symbol-form symbol-logic"></span>'),
            div_extra_contents = [_meaning_datalist],
            list = _meaning_datalist_id,
        ), description="значение конструкции в последний период",   
    )

    # in_rus_constructicon = BootstrapBooleanField(
    #     label="Есть в конструктиконе", name="in_rus_constructicon",
    #     # default=None
    #     # false_values=()
    #     validators=[wtforms.validators.Optional(strip_whitespace=True)]
    # )
    in_rus_constructicon = BoostrapSelectField(
        label="Есть в конструктиконе?", name = "in_rus_constructicon",
        choices=[("", "Есть в конструктиконе?"), ("True", "Да"), ("False", "Нет")],
        render_kw=dict(selected=""),
        coerce=lambda val: val == "True"
    )


    # _num_changes_sign_options, selected = make_sign_options_for_param("Количество")
    # num_changes_sign = BoostrapSelectField(
    #     _num_changes_sign_options[0][1], name="num_changes_sign", 
    #     choices=_num_changes_sign_options,
    #     render_kw=dict(selected=selected))
    # num_changes = BootstrapIntegerField(
    #     label="Количество изменений", name="num_changes",
    #     validators=[wtforms.validators.Optional(strip_whitespace=True)]
    # )
    num_changes__from = BootstrapIntegerField(
        label="количество изменений (от)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)]
    )
    num_changes__to = BootstrapIntegerField(
        label="количество изменений (до)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)]
    )


class AnchorForm(FlaskForm):
    _synt_functions_datalist_id = "synt_function_of_anchor_values"
    _synt_functions_anchor = SYNT_FUNCTIONS_ANCHOR
    print(_synt_functions_anchor)
    _synt_functions_datalist = DataList(
        id=_synt_functions_datalist_id,
        literal_options=_synt_functions_anchor)
    
    synt_function_of_anchor = BootstrapStringField(
        label="Синт. функция якоря", name="synt_function_of_anchor",
        render_kw=dict(div_extra_contents = [_synt_functions_datalist],
                       list=_synt_functions_datalist_id)
    )

    anchor_schema = BootstrapStringField(label="Схема якоря", name="anchor_schema")
    anchor_ru = BootstrapStringField(label="Якорь (рус.)", name="anchor_ru")

    anchor_length__from = BootstrapIntegerField(
        label="Длина якоря (от)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)]
    )
    anchor_length__to = BootstrapIntegerField(
        label="Длина якоря (до)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)]
    )

    
class ChangeForm(FlaskForm):
    # number of changes

    formula = BootstrapStringField(label="Формула на этом этапе", name="stage")

    _stages_datalist_id = "stage_numbers"
    _stages_datalist = DataList(
        id=_stages_datalist_id,
        with_attr_options=[{"label": "первый", "value": 1},
                           {"label": "последний", "value": -1},
                           {"label": "предпоследний", "value": -2},]
    )
    stage_abs = BootstrapIntegerField(
        label="Этап в истории конструкции", name="stage_abs",
        render_kw=dict(div_extra_contents = [_stages_datalist],
                       list=_stages_datalist_id),
        validators=[wtforms.validators.Optional(strip_whitespace=True)]
    )

    level = BoostrapSelectField(
        label="Уровень изменения", name="level",
        choices=[("", "Уровень изменения?"), ("synt", "Синтаксическое"),
                 ("sem", "Семантическое")]
    )

    # should be changed into select with multiple options
    _type_of_change_datalist_id = "types_of_change_values"
    # _types_of_change = safe_get(Change.type_of_change.unique) or TYPES_OF_CHANGE
    _types_of_change = find_unique(Change, "type_of_change")
    _types_of_change_datalist = DataList(
        id=_type_of_change_datalist_id,
        literal_options=_types_of_change
    )
    type_of_change = BootstrapStringField(
        label="Тип изменения", name="type_of_change",
        render_kw=dict(div_extra_contents = [_types_of_change_datalist],
                       list=_type_of_change_datalist_id)
    )

    _subtype_of_change_datalist_id = "subtypes_of_change_values"
    # _types_of_change = safe_get(Change.type_of_change.unique) or TYPES_OF_CHANGE
    _subtypes_of_change = find_unique(Change, "subtype_of_change")
    _subtypes_of_change_datalist = DataList(
        id=_subtype_of_change_datalist_id,
        literal_options=_subtypes_of_change
    )
    subtype_of_change = BootstrapStringField(
        label="Подтип изменения", name="subtype_of_change",
        render_kw=dict(div_extra_contents = [_subtypes_of_change_datalist],
                       list=_subtype_of_change_datalist_id)
    )

    # _duration_sign_options, selected = make_sign_options_for_param("Длительность")
    # duration_sign = BoostrapSelectField(
    #     _duration_sign_options[0][1], name="duration_sign", 
    #     choices=_duration_sign_options,
    #     render_kw=dict(selected=selected))
    # duration = BootstrapIntegerField(
    #     "Длительность периода", name="duration", 
    #     validators=[wtforms.validators.Optional(strip_whitespace=True)])
    
    duration__from = BootstrapIntegerField(
        label="От (лет)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)]
    )
    duration__to = BootstrapIntegerField(
        label="До (лет)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)]
    )

    # first_attested = BootstrapIntegerField(
    #     "Первое вхождение в таком виде", name="first_attested",
    #     validators=[wtforms.validators.Optional(strip_whitespace=True)])
    first_attested__from = BootstrapIntegerField(
        "От (год)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)])
    first_attested__to = BootstrapIntegerField(
        "До (год)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)])
    
    # last_attested = BootstrapIntegerField(
    #     "Последнее вхождение в таком виде", name="last_attested",
    #     validators=[wtforms.validators.Optional(strip_whitespace=True)])

    last_attested__from = BootstrapIntegerField(
        "От (год)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)])
    last_attested__to = BootstrapIntegerField(
        "До (год)",
        validators=[wtforms.validators.Optional(strip_whitespace=True)])
    

class SingleForm(FlaskForm):
    construction = wtforms.FormField(ConstructionForm)
    anchor = wtforms.FormField(AnchorForm)
    changes = wtforms.FieldList(wtforms.FormField(ChangeForm), min_entries=3)

class SearchForm(FlaskForm):
    forms = wtforms.FieldList(wtforms.FormField(SingleForm), min_entries=2)
    # submit = wtforms.SubmitField()



@bp.route('/search/', methods=['GET', 'POST'])
def search():
    form = SingleForm()
    if args := request.args:
        print(args)
        form.process(args)
    print("form initialized")

    # if form.validate_on_submit():
    #     print("hooray")
    #     print(form.data)
    if form.data:
        print("wow!")
        print(form.data)
        return render_template("search_2.html", _form=form)
    
    print("rendering clean form")
    return render_template('search_2.html', _form=form, title="Продвинутый поиск")

    # a GET request with
    #   - no parameters or unfilled parameters
    #   - or a `no-search` flag (usually after linking from construction page)
    if (request.method != 'POST'
        and (not (request.args and any(val for key, val in query_args.items()))
             or query_args.get('no-search') == '1')
    ):
        return render_template(
            'search.html',
            title='Поиск',
            year=datetime.now().year,
            meaning_values=meaning_values,
            synt_functions_anchor=synt_functions_anchor,
            query=request.args,
        )

@bp.route('/form', methods=["POST"])
def receive():
    form = SingleForm()
    print("in receive")
    print(form.is_submitted(), form.validate_on_submit())

    print(form.data)

    query = default_sqlquery()
    # query.parse_form(form.data, do_extra_processing=True)
    query.parse_form(form.data)

    print("parsed form")
    stmt = query.query()
    print("made stmt")

    print(query.form.tree())
    print(stmt)
    print(stmt.compile(compile_kwargs={"literal_binds": True}))

    with current_app.engine.connect() as conn:
        _results = conn.execute(stmt)
        # return 0
        results = _results.mappings().all()

    # for res in results:
    #     print(res)

    results_by_constr = group_rows_by_construction(results)
    print(results_by_constr)

    return render_template(
        "search_2.html", _form=form, results=results, 
        results_by_constr=results_by_constr, use_constr=True
    )


class SimpleSearchForm(FlaskForm):
    _construction_values = find_unique(Construction, "formula")
    _construction_options, _selected = make_options_from_values(
        _construction_values, "конструкцию")
    formula = BoostrapSelectField(
        _construction_options[0][1], name="formula", 
        choices=_construction_options,
        render_kw=dict(selected=_selected))
    
    # submit = wtforms.SubmitField()


# @bp.route('/simple-search/')
def simple_search():
    # try:
    #     with current_app.engine.connect() as conn:
    #         all_formulas = conn.execute(select(Construction.formula)).scalars().all()
    # except:
    #     all_formulas = []
    simple_form = SimpleSearchForm()

    if simple_form.is_submitted():
        print(f'FORM SUBMITTED')

        queried_formula = simple_form.data["formula"]
        stmt = select(Construction).where(Construction.formula == queried_formula)
        with current_app.engine.connect() as conn:
            results = conn.execute(stmt).mappings().all()

        return render_template(
            'search_simple.html',
            # '/errors/404.html',
            # message="Page under construction"
            _form=simple_form,
            results=results,
            # items=all_formulas,
        )

    return render_template(
            'search_simple.html',
            _form=simple_form,
        )

# @bp.route('/simple-form', methods=["POST"])
def receive_simple():
    simple_form = SimpleSearchForm()
    print("in receive")
    print(simple_form.is_submitted(), simple_form.validate_on_submit())

    queried_formula = simple_form.data["formula"]
    stmt = select(Construction).where(Construction.formula == queried_formula)
    print(stmt)
    with current_app.engine.connect() as conn:
        results = conn.execute(stmt).mappings().all()

    print(results)

    return render_template(
        'search_simple.html',
        _form=simple_form,
        results=results,
        query=simple_form,
    )
