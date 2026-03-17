import typing as T
from abc import  ABCMeta, abstractmethod
from copy import deepcopy
from operator import (
    lt,
    gt,
    le,
    ge,
    eq,
    ne,
)
import enum
import re

OperatorsStr = T.Literal["lt", "gt", "le", "ge", "eq", "ne"]

class Operators(enum.Enum):
    lt = lt
    gt = gt
    le = le
    ge = ge
    eq = eq
    ne = ne



Operator2Sign = {
    lt: "<", "lt": "<",
    gt: ">", "gt": ">",
    le: "≤", "le": "≤",
    ge: "≥", "ge": "≥",
    eq: "=", "eq": "=",
    ne: "≠", "ne": "≠"
}

Operator2Name = {
    lt: "lt",
    gt: "gt",
    le: "le",
    ge: "ge",
    eq: "eq",
    ne: "ne",
}


_VT: T.TypeAlias = T.Union[str, int, T.Type[None]]
VT = (str, int, type(None))

BETWEEN_COMPARISON_STRICT: bool = False
OP_L_BY_STRICT = {True: "lt", False: "le"}
OP_G_BY_STRICT = {True: "gt", False: "ge"}

# these repeat the names of the classes
# _COMPARISON = "Comparison"
_CONJUCTION = "Conjunction"
_CONJUNCTION_COPIES = "ConjunctionCopies"
_SUBFORM = "SubForm"
_BETWEEN_COMPARISON = "BetweenComparison"


class QueryMeta(ABCMeta):
    REGISTRY = {}

    def __new__(
        mcls: type[T.Self], name: str, bases: tuple[type, ...],
        namespace: dict[str, T.Any], **kwargs: T.Any
    ) -> T.Self:
        # print(mcls)
        # print(name, bases, namespace)

        new_cls = super().__new__(mcls, name, bases, namespace, **kwargs)
        new_cls.REGISTRY = mcls.REGISTRY
        # print(f"added registry to {name}")

        mcls.REGISTRY[name] = new_cls

        return new_cls

    @classmethod
    def get_registry(cls):
        return dict(cls.REGISTRY)


class BaseQueryElement(metaclass=QueryMeta):
    """Abstract class for query elements. Defines normal and tree representation"""
    _args = ()  

    _BASE_INDENT = " " * 2
    INDENT = _BASE_INDENT

    _extendable = False

    def is_extendable(self):
        return self._extendable

    def extend(self, *args, **kwargs):
        raise NotImplementedError(f"method not implemented for `{type(self)}`")

    def query(self, *args, **kwargs) -> T.Any:
        print(f"querying: {self}, {type(self)}")
        raise NotImplementedError(f"method not implemented for `{type(self)}`")

    def increase_indent(self, times=1) -> None:
        try:
            self.INDENT += self._BASE_INDENT * times
        except AttributeError as e:
            print(e)
            raise e

    @abstractmethod
    def __repr__(self) -> str: ...

    @abstractmethod
    def __tree_repr__(self) -> str: ...

    def tree(self):
        """Return representation of the object used for tree"""
        return self.__tree_repr__()
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return False
        return all(getattr(self, _arg, None) == getattr(other, _arg, None)
                   for _arg in self._args)
    

class Comparison(BaseQueryElement):
    """Comparison of a `param` to a certain `value` by `op` (eq, neq, gt, ge, lt, le)"""
    _args = ("param", "op", "value")

    def __init__(self, param: str, op: T.Union[Operators, OperatorsStr], value: _VT) -> None:
        self.param = param

        # print(f"op is : {op} (type={type(op)})")

        if isinstance(op, str):
            actual_op = getattr(Operators, op, None)
            if actual_op is None:
                raise ValueError(f"illegal string representation of op: {op}")

            self.str_op = op
            self.op = actual_op.value
        else:
            if op in Operator2Name:
                self.str_op = Operator2Name[op]
                self.op = op
            else:
                raise ValueError(
                    f"illegal value for `op` (it should be one of `le`, `lt` etc. "
                    f" or built-in `operator` module members): {op}")

        self.value = value

    @staticmethod
    def op2sign(op: T.Union[Operators, OperatorsStr]) -> str:
        """Return string representation of the operator"""
        return Operator2Sign[op]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.param}, {self.op}, {self.value})"
    

    def _str(self, param: T.Optional[str]=None) -> str:
        param = param or self.param
        return f"{param}{self.op2sign(self.op)}{self.value}"

    def __str__(self, param: T.Optional[str]=None) -> str:
        return self._str()
    
    def __tree_repr__(self) -> str:
        """Tree representation of comparison. Defaults to `self.__str__()`"""
        return self.__str__()
    

class BetweenComparison(BaseQueryElement):
    """Comparison of a `param` to be between certain values"""
    _args = ("param", "value_from", "value_to")
    do_strict_comparison = BETWEEN_COMPARISON_STRICT

    def __init__(self, param: str, value_from: _VT, value_to: _VT) -> None:
        self.param = param
        self.value_from = value_from
        self.value_to = value_to

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.param}, {self.value_from}, {self.value_to})"
    

    def _str(self, param: T.Optional[str]=None) -> str:
        param = param or self.param
        return f"{param}-between({self.value_from}, {self.value_to})"

    def __str__(self, param: T.Optional[str]=None) -> str:
        return self._str()        
    
    def __tree_repr__(self) -> str:
        return self.__str__()
    

class StringPattern(BaseQueryElement):
    _args = ("param", "value")

    def __init__(self, param: str, value: str) -> None:
        super().__init__()
        self.param = param
        self.pattern = value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.param!r}, {self.pattern!r})"

    def __str__(self) -> str:
        return f'str-pattern("{self.pattern}")'
    
    def __tree_repr__(self) -> str:
        return self.__str__()


class BinaryConnective(BaseQueryElement):
    _args = ("items",)

    self_repr: str

    _extendable = True

    def __init__(self, items: T.List[BaseQueryElement]) -> None:
        self.items = items

    def extend(self, new_items):
        self.items.extend(new_items)

    def increase_indent(self, times=1):
        super().increase_indent()
        for item in self.items:
            item.increase_indent(times=times)
        
    def __tree_repr__(self) -> str:
        print(f"({self.__class__.__name__}) making tree repr"
              f" (indent={repr(self.INDENT)}) for: {self.items}")

        self.increase_indent()

        return f"\n{self.INDENT}".join(
            [self.self_repr] + [item.__tree_repr__() for item in self.items])

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.items.__repr__()})"


class Conjunction(BinaryConnective):
    """Conjunction (AND & ⋀) in a query. A conjunct is any other query element."""

    self_repr = "AND"

class ConjunctionCopies(Conjunction):
    """Conjunction (AND & ⋀) in a query. The conjuncts are distinct instances of a common model."""

    self_repr = "AND (distinct instances)"
    

class Disjunction(BinaryConnective):
    """Disjunction (OR | ⋁) in a query. A disjunct is any other query element."""

    self_repr = "OR"


class SubForm(BaseQueryElement):
    _args = ("name", "content")

    def __init__(self, name: str, content: BaseQueryElement) -> None:
        self.name = name
        self.content = content

    def is_extendable(self):
        return self.content.is_extendable()

    def extend(self, elements: T.Union[T.List[BaseQueryElement], BaseQueryElement]) -> None:
        if not self.content.is_extendable():
            raise ValueError(f"Content, which is `{type(self.content)}` is not extendable")
        self.content.extend(elements)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name!r}, {self.content!r})"
    
    def increase_indent(self, times=1):
        super().increase_indent()
        self.content.increase_indent()
    
    def __tree_repr__(self) -> str:
        print(f"({self.__class__.__name__}) making tree repr"
              f" (indent={repr(self.INDENT)}) for: {self.content}")

        self.content.increase_indent(times=4)
        return f"[{self.name}]:\n{self.INDENT}{self.content.__tree_repr__()}"


PrimitiveFormType: T.TypeAlias = T.Dict[str, _VT]
BasicFormType: T.TypeAlias = T.Union[PrimitiveFormType, T.List[PrimitiveFormType]]
FormType: T.TypeAlias = T.Union[T.Dict[str, T.Union['FormType', BasicFormType]], PrimitiveFormType]

class ElementDerivation(metaclass=QueryMeta):
    def __init__(self, *args, **kwargs) -> None: ...
    def __call__(self, form: FormType) -> T.Optional[BaseQueryElement]: ...


class ValueWithSignDerivation(ElementDerivation):
    """Remember dict keys for `param`, `op(erator)` and later fetch them from form in `__call__`"""
    def __init__(self, param, op_key, comparison_model: Comparison=None) -> None:
        self.param = param
        self.op_key = op_key
        self.comparison = comparison_model or self.REGISTRY[_COMPARISON]

        print(self.comparison)
    
    def __call__(self, form: PrimitiveFormType) -> T.Optional[Comparison]:
        """Fetch `op` and value from form"""   
        param = self.param
        value = form.get(param)

        op_key = self.op_key
        str_op = form.get(op_key)

        if all((param, value, str_op)):
            for key in (param, op_key):
                form.pop(key)

            op = getattr(Operators, str_op, None)
            if op is None:
                raise ValueError(f"`{op_key}` has bad value: `{str_op}`")
            
            return self.comparison(param, str_op, value)
        else:
            print(f"{form} doesn't have one of `{param}`, `{op_key}`")
            return None
        
    def __repr__(self) -> str:
        return (f"{self.__class__.__name__}({self.param!r}, {self.op_key!r}, "
                f"{self.comparison!r})")


class ValueBetweenDerivation(ElementDerivation):
    do_strict_comparison = BETWEEN_COMPARISON_STRICT

    def __init__(
        self, key_from: str, key_to: str, param_key=None, param=None,
        comparison_model: Comparison=None, comparison_between_model: Comparison=None
    ) -> None:
        self.key_from = key_from
        self.key_to = key_to
        self.param_key = param_key
        self.param = param
        self.comparison = comparison_model or self.REGISTRY[_COMPARISON]
        self.comparison_between = comparison_between_model or self.REGISTRY[_BETWEEN_COMPARISON]

        print(self.comparison, self.comparison_between)

    @classmethod
    def from_ends_keys(cls, full_key_from: str, full_key_to: str, sep: str="__", **kwargs):
        """Initialize from `key_from` and `key_to` that share a common prefix — param"""
        prefix1, _ = full_key_from.split(sep)
        prefix2, _ = full_key_to.split(sep)
        if prefix1 != prefix2:
            raise ValueError(f"prefixes don't match for keys: `{full_key_from}`, `{full_key_to}`")
        
        return cls(full_key_from, full_key_to, param=prefix1, **kwargs)

    def __call__(self, form: FormType) -> T.Optional[BaseQueryElement]:
        """Fetch `param` (if not known) and values (from and/or to) from form"""
        param = self.param

        value_from = form.get(self.key_from)
        value_to = form.get(self.key_to)

        print(param, self.key_from, value_from, self.key_to, value_to)
        if (param or form.get(self.param_key)) and (value_from or value_to):
            for key in (self.key_from, self.key_to, self.param_key):
                if key in form:
                    form.pop(key)

            param = param or form.get(self.param_key)
            if value_from and value_to:
                return self.comparison_between(param, value_from, value_to)
            elif value_from:
                op_greater = OP_G_BY_STRICT[self.do_strict_comparison]
                return self.comparison(param, op_greater, value_from)
            elif value_to:
                op_less = OP_L_BY_STRICT[self.do_strict_comparison]
                return self.comparison(param, op_less, value_to)
        else:
            print(f"{form} doesn't have `param_key` or one of (`{self.key_from}`, `{self.key_to}`)")
            return None


# TODO: if this is used for derivation it should reset `subforms_used` in the end
# or do without it alltogether
class BaseQuery(metaclass=QueryMeta):
    """Base class for deriving tree from dict forms"""
    def __init__(
        self, form2derivable_fields: T.Optional[T.Dict[str, T.List[ElementDerivation]]]=None
    ) -> None:
        self.form2derivable_fields = form2derivable_fields or {}

        self.subforms_used: T.List[SubForm] = []
        self.used_subforms2name: T.Dict[str, SubForm] = {}

    def make_connective(
        self, form_name: T.Optional[str], elements: T.List[BaseQueryElement]
    ) -> BinaryConnective:
        if form_name == "changes":
            return self.REGISTRY["ConjunctionCopies"](elements)

        return self.REGISTRY["Conjunction"](elements)
    
    def derive_field(
        self, form: T.Union[FormType, BasicFormType], form_name: T.Optional[str],
        field_derivation: ElementDerivation
    ):
        """Derive a single field. This function may be overriden in child `Query` for callbacks"""
        return field_derivation(form)
    
    def derive_fields(
        self, form: T.Union[FormType, BasicFormType], form_name: T.Optional[str]=None
    ):
        """Derives query fields that result from multiple fields of the input"""
        derived_fields = []
        if self.form2derivable_fields and form_name is not None:
            derivable_fields_this_form = self.form2derivable_fields.get(form_name, [])

            print(f"deriving: {derivable_fields_this_form}")

            for derivable_field in derivable_fields_this_form:
                maybe_derived_field = self.derive_field(form, form_name, derivable_field)
                if maybe_derived_field is not None:
                    derived_fields.append(maybe_derived_field)
        
        return derived_fields
    
    def add_derivation(self, form_name: str, derivation: ElementDerivation):
        """Add a derivation this `Query` should perform"""
        self.form2derivable_fields.setdefault(form_name, []).append(derivation)

    def parse_val(self, form_name: str, key: str, val: str) -> BaseQueryElement:
        """Parse a single value. This function may be overriden in child `Query`."""
        return self.REGISTRY[_COMPARISON](key, eq, val)
    
    def parse_form_name(self, form_name: str) -> str:
        """Replace form name if needed"""
        return form_name

    def parse(
        self, form: T.Union[FormType, BasicFormType], form_name: T.Optional[str]=None
    ) -> T.Union[T.List[BaseQueryElement], BaseQueryElement]:
        old_form_name = form_name
        form_name = self.parse_form_name(form_name)
        print(f"old name: `{old_form_name}`, new_name: `{form_name}`")
        print(form)

        elements = []
        if isinstance(form, dict):
            # derive complex fields
            elements.extend(self.derive_fields(form, form_name=form_name))

            for key, val in form.items():
                is_empty = val in (None, "")
                print(f"parsing pair <`{key}`, val>")
                if isinstance(val, VT) and not is_empty:
                    print(f"val is token: {val}")
                    element = self.parse_val(form_name, key, val)
                    elements.append(element)
                elif isinstance(val, (list, dict)):
                    print(f"val is collection: {val}")
                    form_name = self.parse_form_name(key)
                    val_parse = self.parse(val, form_name=form_name)
                    if val_parse:
                        # if form_name not in self.used_subforms2name:
                        subform = self.REGISTRY["SubForm"](
                            form_name, self.make_connective(form_name, val_parse))
                        elements.append(subform)
                        self.subforms_used.append(subform)
                        # self.used_subforms2name[form_name] = subform
                        # else:
                        #     subform = self.used_subforms2name[form_name]
                        #     # if subform.is_extendable():
                        #     subform.extend(val_parse)

                elif not is_empty:
                    print("unknown connective")

        elif isinstance(form, list):
            for subform in form:
                print(f"parsing subform: {subform}")
                subform_parse = self.parse(subform, form_name=form_name)
                if subform_parse:
                    elements.append(self.make_connective(None, subform_parse))
        else:
            print("unknown form type")

        print(f"returning {'empty' if not elements else elements}")
        return elements
    

    def parse_form(self, form: T.Union[FormType, BasicFormType], print_tree: bool=False):
        form = deepcopy(form)
        res = self.REGISTRY["Conjunction"](self.parse(form))
        self.form = res

        if print_tree:
            print(res.tree())
        return res


class Query(BaseQuery): ...

_COMPARISON = Comparison.__name__
