import typing as T
from datetime import datetime
from itertools import chain
import logging

from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    Enum,
    Boolean,
    ForeignKey,
    Table,
    DateTime,
)
from sqlalchemy.orm import (
    relationship,
    declarative_base,
    declarative_mixin,
)

from app.constants import NO_DATE


logger = logging.getLogger(f"diachronicon.{__name__}")

PRECISE_DATE_UNTIL_YEAR = 2005
CURRENT_STATUS = "настоящее время"

MAX_FORMULA_LEN = 200
REPR_CHAR_LIM = 25

# Kept for reference / backwards-compat imports; no longer used as a SQL Enum
# because real data contains compound values like "Verb Predicate или Matrix Predicate"
UNKNOWN_SYNT_FUNCTION_OF_ANCHOR = "<unknown>"
SYNT_FUNCTION_OF_ANCHOR_VALUES = (
    UNKNOWN_SYNT_FUNCTION_OF_ANCHOR,
    "Argument",
    "Coordinator",
    "Discourse Particle",
    "Government",
    "Matrix Predicate",
    "Modifier",
    "Nominal Quantifier",
    "Object",
    "Parenthetical",
    "Praedicative Expression",
    "Subject",
    "Subordinator",
    "Verb Predicate",
    "Word-Formation",
)

SEMANTICS_TAG = "sem"
MORPHOSYNTAX_TAG = "synt"

Base = declarative_base()

@declarative_mixin
class ShallowEqMixin:
    _comparable_args: T.List[str] = []

    def get_comparable_args(self):
        return self._comparable_args

    def __shallow_eq__(self, other: T.Any) -> bool:
        if not isinstance(other, type(self)):
            return False
        return all(
            getattr(self, _arg, None) == getattr(other, _arg, None)
            for _arg in self._comparable_args
        )

    def shallow_eq(self, other: T.Any) -> bool:
        return self.__shallow_eq__(other)


@declarative_mixin
class ConstructionMixin:
    id = Column(Integer, primary_key=True)
    formula = Column(String(MAX_FORMULA_LEN))


@declarative_mixin
class TagMixin:
    id = Column(Integer, primary_key=True)
    name = Column(String(100))


class User(Base):
    """Annotators and admins.  Public visitors are anonymous (no User row)."""
    __tablename__ = 'user'

    id = Column(Integer, primary_key=True)
    username = Column(String(64), unique=True, nullable=False, index=True)
    email = Column(String(120), unique=True, nullable=False, index=True)
    password_hash = Column(String(256), nullable=False)
    role = Column(
        Enum('admin', 'annotator', name='user_role'),
        nullable=False,
    )
    is_active = Column(Boolean, default=True, nullable=False, server_default='1')
    created_at = Column(DateTime, default=datetime.utcnow)

    drafts = relationship(
        "AnnotationDraft",
        back_populates="annotator",
        cascade="all, delete-orphan",
    )

    @property
    def is_authenticated(self):
        return True

    @property
    def is_anonymous(self):
        return False

    def get_id(self):
        return str(self.id)

    def __repr__(self):
        return f'User({self.id!r}, {self.username!r}, {self.role!r})'


class GeneralTag(TagMixin, Base):
    __tablename__ = "tag"
    kind = Column(
        Enum(*(SEMANTICS_TAG, MORPHOSYNTAX_TAG), name='tag_kind',
             create_constraint=True)
    )

    def __repr__(self):
        return f'GeneralTag({self.id!r}, {self.name!r}, {self.kind!r})'


construction_to_tags = Table(
    "construction_to_tags",
    Base.metadata,
    Column("construction_id", Integer, ForeignKey("construction.id"), primary_key=True),
    Column("tag_id", Integer, ForeignKey("tag.id"), primary_key=True),
)

change_to_tags = Table(
    "change_to_tags",
    Base.metadata,
    Column("change_id", Integer, ForeignKey("change.id"), primary_key=True),
    Column("tag_id", Integer, ForeignKey("tag.id"), primary_key=True),
)


class GeneralInfo(Base):
    __tablename__ = 'general_info'

    construction_id = Column(
        Integer, ForeignKey("construction.id"), primary_key=True
    )
    name = Column(String(200))

    # Annotation-workflow metadata kept for provenance
    group_number = Column(String(20))   # String: source data contains values like '19?'
    annotated_sample = Column(String(400))
    term_paper = Column(String(400))

    # 'ready' | 'draft'
    status = Column(String(30))

    construction = relationship("Construction", back_populates="general_info")

    def __repr__(self):
        return (f'GeneralInfo({self.construction_id!r}, {self.name!r}, '
                f'{self.status!r})')


class Construction(ConstructionMixin, Base):
    __tablename__ = 'construction'

    orig_id = Column(String(30))          # ID from source data (may have group suffix)
    contemporary_meaning = Column(String(400))
    variation = Column(String(400))

    in_rus_constructicon = Column(Boolean)
    rus_constructicon_id = Column(Integer)

    # Changed from Enum to String: real data contains compound / free-text values
    synt_function_of_anchor = Column(String(100))

    anchor_schema = Column(String(200))
    anchor_ru = Column(String(200))
    anchor_eng = Column(String(200))

    is_published = Column(
        Boolean, default=False, nullable=False, server_default='0'
    )
    is_draft = Column(
        Boolean, default=True, nullable=False, server_default='1'
    )

    morphosyntax_tags = relationship(
        "GeneralTag",
        secondary=construction_to_tags,
        primaryjoin=(
            f"and_(Construction.id==construction_to_tags.c.construction_id, "
            f"GeneralTag.kind=='{MORPHOSYNTAX_TAG}')"
        ),
        overlaps="semantic_tags",
    )
    semantic_tags = relationship(
        "GeneralTag",
        secondary=construction_to_tags,
        primaryjoin=(
            f"and_(Construction.id==construction_to_tags.c.construction_id, "
            f"GeneralTag.kind=='{SEMANTICS_TAG}')"
        ),
        overlaps="morphosyntax_tags",
    )

    general_info = relationship(
        "GeneralInfo", back_populates="construction", uselist=False
    )
    formula_elements = relationship(
        "FormulaElement", back_populates="construction",
        cascade="all, delete-orphan",
    )
    changes = relationship(
        "Change", back_populates="construction",
        cascade="all, delete-orphan",
    )
    constraints = relationship("Constraint", order_by="Constraint.id")
    variants = relationship(
        "ConstructionVariant", back_populates="construction",
        order_by="ConstructionVariant.id",
    )
    embeddings = relationship(
        "ConstructionEmbedding", back_populates="construction",
        cascade="all, delete-orphan",
    )
    annotation_drafts = relationship(
        "AnnotationDraft", back_populates="construction",
    )

    def get_alternate_formulas(self):
        return [v.formula for v in self.variants if v.is_main != 1]

    def exist_constraints(self):
        return bool(self.constraints)

    def exist_changes_constraints(self):
        return any(ch.exist_constraints() for ch in self.changes)

    def set_changes_one_based(self):
        self.id_to_id1 = {}
        changes = self.changes
        if not changes:
            return changes

        for new_id1, ch in enumerate(changes, start=1):
            self.id_to_id1.setdefault(ch.id, new_id1)
            ch.id1 = new_id1

        for ch in changes:
            for _ch in chain(ch.previous_changes, ch.next_changes):
                _ch.id1 = self.id_to_id1.get(_ch.id, _ch.id)

        return changes

    def __repr__(self):
        return (
            f'Construction({self.id!r}, orig={self.orig_id!r}, '
            f'{self.formula!r}, published={self.is_published!r})'
        )



class ConstructionVariant(ConstructionMixin, Base):
    __tablename__ = 'construction_variant'

    construction_id = Column(Integer, ForeignKey(Construction.id))
    construction = relationship("Construction", back_populates="variants")
    change_id = Column(Integer, ForeignKey("change.id"))
    changes = relationship("Change", back_populates="variants")
    is_main = Column(Boolean)

    formula_elements = relationship(
        "FormulaElement", back_populates="construction_variant",
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return (
            f'{self.__class__.__name__}({self.id!r}, {self.formula!r}, '
            f'constr={self.construction_id!r}, main={self.is_main!r})'
        )



class FormulaElement(Base, ShallowEqMixin):
    __tablename__ = 'formula_element'
    _comparable_args = ["value", "order", "depth", "is_optional", "has_variants"]

    id = Column(Integer, primary_key=True)
    formula_id = Column(Integer, unique=True)
    construction_id = Column(Integer, ForeignKey(Construction.id))
    construction_variant_id = Column(Integer, ForeignKey(ConstructionVariant.id))

    value = Column(String(100))
    order = Column(Integer)
    depth = Column(Integer)
    is_optional = Column(Boolean, default=False)
    has_variants = Column(Boolean, nullable=True)

    construction = relationship("Construction", back_populates="formula_elements")
    construction_variant = relationship(
        "ConstructionVariant", back_populates="formula_elements"
    )

    def __repr__(self):
        return (
            f'FormulaElement({self.id!r}, {self.value!r}, '
            f'order={self.order!r}, optional={self.is_optional!r})'
        )


change_to_previous_changes = Table(
    "change_to_previous_changes",
    Base.metadata,
    Column("change_id", Integer, ForeignKey("change.id"), primary_key=True),
    Column("previous_change_id", Integer, ForeignKey("change.id"), primary_key=True),
)


class Change(Base):
    __tablename__ = 'change'

    _names = {
        'стадия': 'stage',
        'уровень': 'level',
        'тип изменения': 'type_of_change',
        'первое вхождение (дата)': 'first_attested',
        'последнее вхождение (дата)': 'last_attested',
        'первое вхождение': 'first_example',
        'последнее вхождение': 'last_example',
        'комментарий': 'comment',
    }

    id = Column(Integer, primary_key=True)
    construction_id = Column(Integer, ForeignKey(Construction.id))

    # Formula at this historical stage of the construction
    stage = Column(String(400))
    # Raw string from source data (may be comma-separated list of IDs, e.g. "2, 3, 4")
    former_change = Column(String(200))

    # 'source' | 'synt' | 'sem'
    level = Column(String(10))
    type_of_change = Column(String(100))
    subtype_of_change = Column(String(200))

    # Attestation stored as the original source string (may be ranges like '1794-1795')
    first_attested = Column(String(20))
    last_attested = Column(String(20))
    # Derived integer years — computed at import time, used for range search / sort
    first_attested_year = Column(Integer, nullable=True, index=True)
    last_attested_year = Column(Integer, nullable=True, index=True)

    first_example = Column(Text)
    last_example = Column(Text)

    comment = Column(Text)
    frequency_trend = Column(String(400))
    sources = Column(String(500))

    morphosyntax_tags = relationship(
        "GeneralTag",
        secondary=change_to_tags,
        primaryjoin=(
            f"and_(Change.id==change_to_tags.c.change_id, "
            f"GeneralTag.kind=='{MORPHOSYNTAX_TAG}')"
        ),
        overlaps="semantic_tags",
    )
    semantic_tags = relationship(
        "GeneralTag",
        secondary=change_to_tags,
        primaryjoin=(
            f"and_(Change.id==change_to_tags.c.change_id, "
            f"GeneralTag.kind=='{SEMANTICS_TAG}')"
        ),
        overlaps="morphosyntax_tags",
    )

    construction = relationship("Construction", back_populates="changes")
    variants = relationship(
        "ConstructionVariant", back_populates="changes",
        order_by="ConstructionVariant.id",
    )
    constraints = relationship("Constraint", order_by="Constraint.id")

    previous_changes = relationship(
        "Change",
        secondary=change_to_previous_changes,
        primaryjoin=id == change_to_previous_changes.c.change_id,
        secondaryjoin=id == change_to_previous_changes.c.previous_change_id,
        back_populates="next_changes",
    )
    next_changes = relationship(
        "Change",
        secondary=change_to_previous_changes,
        primaryjoin=id == change_to_previous_changes.c.previous_change_id,
        secondaryjoin=id == change_to_previous_changes.c.change_id,
        back_populates="previous_changes",
    )

    @staticmethod
    def parse_year(
        year: T.Union[str, int, None], left_bias: float = 0.0
    ) -> T.Optional[int]:
        """Return an integer year from a potentially range-formatted string.

        Handles: plain integers, '1794-1795' ranges, '1980-ые' decade notation.
        Returns None when the value is missing, '-', or otherwise unparseable.
        """
        if not year or year == '-':
            return None
        year_str = str(year).strip()
        if year_str.isnumeric():
            return int(year_str)
        if '-' in year_str:
            if year_str.endswith('-ые'):
                return int(year_str.split('-')[0])
            parts = year_str.split('-')
            if len(parts) == 2:
                try:
                    left_val = int(parts[0])
                    right_val = int(parts[1])
                    return int(left_val + (right_val - left_val) * (1 - left_bias))
                except ValueError:
                    pass
        logger.debug(f"unsupported year format: `{year}`")
        return None

    @property
    def first_attested_(self) -> T.Union[int, str, None]:
        return self.parse_year(self.first_attested) or self.first_attested

    @property
    def last_attested_(self) -> T.Union[int, str, None]:
        return self.parse_year(self.last_attested) or self.last_attested

    @property
    def last_attested_dt_aware(self) -> T.Union[int, str, None]:
        year = self.parse_year(self.last_attested) or self.last_attested
        if isinstance(year, int):
            return year if year < PRECISE_DATE_UNTIL_YEAR else CURRENT_STATUS
        return year

    def exist_constraints(self):
        return bool(self.constraints)

    def __repr__(self):
        return (
            f'Change({self.id!r}, constr={self.construction_id!r}, '
            f'{self.level!r}, {self.type_of_change!r}, '
            f'{self.first_attested!r}–{self.last_attested!r})'
        )


class Constraint(Base):
    __tablename__ = 'constraint'

    id = Column(Integer, primary_key=True)
    # Nullable: a constraint may apply to the construction as a whole (no specific change)
    change_id = Column(Integer, ForeignKey(Change.id), nullable=True)
    construction_id = Column(Integer, ForeignKey(Construction.id))

    element = Column(String(30))
    syntactic = Column(String(500))
    semantic = Column(String(500))

    change = relationship("Change", back_populates="constraints")

    def __repr__(self):
        return (
            f'Constraint({self.id!r}, change={self.change_id!r}, '
            f'constr={self.construction_id!r}, {self.element!r})'
        )


class AnnotationDraft(Base):
    __tablename__ = 'annotation_draft'

    id = Column(Integer, primary_key=True)
    # Null when creating a brand-new construction (not yet persisted)
    construction_id = Column(Integer, ForeignKey(Construction.id), nullable=True)
    annotator_id = Column(Integer, ForeignKey('user.id'), nullable=False)

    # Full serialised form state as JSON
    form_data = Column(Text, nullable=False, default='{}')

    # Which LLM provider/model was used for the last auto-annotation (if any)
    llm_provider = Column(String(50), nullable=True)
    llm_model = Column(String(100), nullable=True)

    status = Column(
        Enum('draft', 'submitted', 'published', name='draft_status'),
        nullable=False,
        default='draft',
        server_default='draft',
    )
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    construction = relationship("Construction", back_populates="annotation_drafts")
    annotator = relationship("User", back_populates="drafts")

    def __repr__(self):
        return (
            f'AnnotationDraft({self.id!r}, constr={self.construction_id!r}, '
            f'annotator={self.annotator_id!r}, {self.status!r})'
        )

class ConstructionEmbedding(Base):
    __tablename__ = 'construction_embedding'

    id = Column(Integer, primary_key=True)
    construction_id = Column(Integer, ForeignKey(Construction.id), nullable=False)
    # Which text field was embedded, e.g. 'contemporary_meaning', 'variation'
    field_name = Column(String(64), nullable=False)
    # JSON-serialised list of floats, e.g. "[0.12, -0.34, ...]"
    embedding = Column(Text, nullable=False)
    embedding_model = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    construction = relationship("Construction", back_populates="embeddings")

    def __repr__(self):
        return (
            f'ConstructionEmbedding({self.id!r}, constr={self.construction_id!r}, '
            f'{self.field_name!r}, model={self.embedding_model!r})'
        )

DBModel = T.Type[T.Union[
    Construction, Change, GeneralInfo, Constraint, FormulaElement,
    ConstructionVariant, User, AnnotationDraft, ConstructionEmbedding,
]]
Model2Field2Val = T.Dict[
    DBModel,
    T.Union[
        T.Dict[str, T.Optional[str]],
        T.List[T.Dict[str, T.Optional[str]]],
    ],
]