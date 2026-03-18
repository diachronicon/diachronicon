"""
Parsing utilities for the annotation tool.

Two entry points:

  parse_llm_report(text)   — parse a logging-form report (markdown or plain
                             text, as produced by the LLM or pasted from a
                             .docx) into a form_data dict.

  parse_llm_response(text) — legacy JSON path; falls back to parse_llm_report.

  serialize_form(data)     — normalise a form-data dict for DB storage.

The LLM returns the variation logging form defined in the annotation prompt.
Three output styles are handled automatically:

  GPT style    "### N. title\\n**Field**: value\\n---\\n..."
  Sonnet style "N. title\\nField: value\\n..."
  Opus style   Heading3 "N. title" followed by one paragraph containing all
               fields as "Field: value\\n Field: value\\n ..." (soft-return
               separated, each continuation line starts with a space).
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

logger = logging.getLogger(f'diachronicon.{__name__}')

# ---------------------------------------------------------------------------
# Field-level normalisations
# ---------------------------------------------------------------------------

LEVEL_MAP: dict[str, str] = {
    'Source': 'source', 'source': 'source',
    'Synt':   'synt',   'synt':   'synt',
    'Sem':    'sem',    'sem':    'sem',
}

# Strip [brackets] from relationship references like "Следует за [1]"
_BRACKET_NUM_RE = re.compile(r'\[(\d+)\]')

# All accepted field-label variants → internal key
_FIELD_ALIASES: dict[str, str] = {
    'Отношение':           'former_change',
    'Relation':            'former_change',
    'Описание':            'comment',
    'Description':         'comment',
    'Формула':             'stage',
    'Formula':             'stage',
    'Уровень':             'level',
    'Level':               'level',
    'Тип':                 'type_of_change',
    'Type':                'type_of_change',
    'Подтип':              'subtype_of_change',
    'Subtype':             'subtype_of_change',
    'Первое вхождение':    '_first_raw',
    'First attestation':   '_first_raw',
    'First entry':         '_first_raw',
    'Последнее вхождение': '_last_raw',
    'Last attestation':    '_last_raw',
    'Last entry':          '_last_raw',
    'Примечание':          '_note',    # consumed but not stored
}

# Regex: matches "**Field**:" or "Field:" at the start of a logical line.
# We compile it once with all known field names alternated.
_KNOWN_FIELDS_ALT = '|'.join(re.escape(k) for k in _FIELD_ALIASES)
_FIELD_LINE_RE = re.compile(
    r'(?:^|\n)\s*\*{0,2}(' + _KNOWN_FIELDS_ALT + r')\*{0,2}\s*:\s*',
)

# Horizontal rules used by GPT as change separators
_HR_RE = re.compile(r'^\s*---+\s*$', re.MULTILINE)

# Numbered-section start: optional "### " then "N. "
_SECTION_START_RE = re.compile(r'(?:^|\n)(?:###\s+)?(\d+)\.\s')

# ---------------------------------------------------------------------------
# Text-level helpers
# ---------------------------------------------------------------------------

def _strip_markdown(text: str) -> str:
    """Remove bold/italic/backtick/wiki-link markers."""
    text = re.sub(r'\*{1,2}([^*\n]*?)\*{1,2}', r'\1', text)
    text = re.sub(r'`([^`\n]*)`', r'\1', text)
    text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
    return text


def _normalise_relationship(val: str) -> str:
    """'Следует за [1]' → 'Следует за 1'."""
    return _BRACKET_NUM_RE.sub(r'\1', val).strip()


# ---------------------------------------------------------------------------
# Attested-field parsing (year + example text)
# ---------------------------------------------------------------------------

_YEAR_AT_START_RE = re.compile(r'^(\d{4}(?:[–\-]\d{2,4})?)')
_YEAR_IN_PARENS_RE = re.compile(r'\((\d{4}(?:[–\-]\d{2,4})?)\)')

_DASH_QUOTE_RE = re.compile(
    r'[—–]\s*(?:[«"„](.+?)[»"]|"(.+?)")',
    re.DOTALL,
)
_GUILLEMET_RE = re.compile(r'[«„](.+?)[»"]', re.DOTALL)


def _extract_year(text: str) -> str:
    text = text.strip()
    # Format GPT/Opus: "1823, Author …"
    m = _YEAR_AT_START_RE.match(text)
    if m:
        return m.group(1)
    # Format Sonnet: "Author. Title (1823) — …"
    m = _YEAR_IN_PARENS_RE.search(text)
    if m:
        return m.group(1)
    return ''


# ---------------------------------------------------------------------------
# Block splitting
# ---------------------------------------------------------------------------

def _split_into_change_blocks(text: str) -> list[str]:
    """Split a full report into per-change raw text blocks."""
    # Remove horizontal rules first
    text = _HR_RE.sub('\n', text)

    positions = [m.start() for m in _SECTION_START_RE.finditer(text)]
    if not positions:
        return []

    return [
        text[positions[i]: positions[i + 1] if i + 1 < len(positions) else len(text)].strip()
        for i in range(len(positions))
    ]


# ---------------------------------------------------------------------------
# Field extraction from one block
# ---------------------------------------------------------------------------

def _extract_fields_from_block(block: str) -> dict[str, str]:
    """
    Return {internal_key: value_text} for all recognisable fields in *block*.
    Handles the three output styles via the unified regex split.
    The Opus style produces one big paragraph where fields are separated by
    '\\n ' (newline + space); normalising that to plain newlines first makes
    the same regex work for all styles.
    """
    # Opus normalisation: \n<space> → \n
    block = re.sub(r'\n ', '\n', block)

    parts = _FIELD_LINE_RE.split(block)
    # parts = [pre-text, field_name, value_text, field_name, value_text, ...]

    fields: dict[str, str] = {'_title_block': parts[0] if parts else ''}

    i = 1
    while i < len(parts) - 1:
        label = parts[i].strip()
        raw_value = parts[i + 1].strip() if (i + 1) < len(parts) else ''
        # Trim trailing lines that look like the start of the next section
        # (can happen with poor line-break handling)
        raw_value = raw_value.strip()
        raw_value = _strip_markdown(raw_value)
        db_key = _FIELD_ALIASES.get(label)
        if db_key and db_key != '_note':
            fields[db_key] = raw_value
        i += 2

    return fields


# ---------------------------------------------------------------------------
# Single-block → change dict
# ---------------------------------------------------------------------------

def _parse_change_block(block: str) -> dict | None:
    fields = _extract_fields_from_block(block)

    # Extract the human-readable change title from the leading text
    title = ''
    for line in fields.pop('_title_block', '').splitlines():
        line = line.strip()
        m = re.match(r'^(?:###\s+)?\d+\.\s+(.*)', line)
        if m:
            title = _strip_markdown(m.group(1)).strip()
            break

    # Skip blocks that have no recognisable field content (e.g., summary table rows)
    if not any(k in fields for k in ('stage', 'level', 'type_of_change', '_first_raw')):
        return None

    first_raw = fields.pop('_first_raw', '')
    last_raw  = fields.pop('_last_raw', '')

    return {
        'change_name':       title,
        'stage':             fields.get('stage', ''),
        'former_change':     _normalise_relationship(fields.get('former_change', '')),
        'level':             LEVEL_MAP.get(
                                 fields.get('level', ''),
                                 fields.get('level', '').lower(),
                             ),
        'type_of_change':    fields.get('type_of_change', ''),
        'subtype_of_change': fields.get('subtype_of_change', ''),
        'first_attested':    _extract_year(first_raw),
        'last_attested':     _extract_year(last_raw),
        'first_example':     first_raw,   # full text: source citation + quote
        'last_example':      last_raw,
        'comment':           fields.get('comment', ''),
        'constraints':       [],
    }


# ---------------------------------------------------------------------------
# Public: parse a logging-form report
# ---------------------------------------------------------------------------

def parse_llm_report(text: str) -> dict:
    """
    Parse a logging-form report produced by the LLM into a form_data dict.

    The top-level construction fields (name, formula, etc.) are returned empty
    because the prompt produces only the changes list; the annotator fills
    those fields directly in the form.

    Never raises. Returns {'_parse_error': ...} on total failure.
    """
    result: dict = {
        'name': '', 'formula': '', 'contemporary_meaning': '',
        'variation': '', 'in_rus_constructicon': False,
        'rus_constructicon_id': None, 'synt_function_of_anchor': '',
        'anchor_schema': '', 'anchor_ru': '', 'anchor_eng': '',
        'group_number': '', 'annotated_sample': '',
        'changes': [],
    }

    blocks = _split_into_change_blocks(text)
    if not blocks:
        logger.warning('parse_llm_report: no numbered blocks found')
        result['_parse_error'] = (
            'Не найдено пронумерованных разделов изменений. '
            'Убедитесь, что LLM вернул отчёт в формате списка изменений.'
        )
        return result

    for block in blocks:
        change = _parse_change_block(block)
        if change is not None:
            result['changes'].append(change)

    if not result['changes']:
        result['_parse_error'] = (
            'Разделы найдены, но поля не удалось извлечь. '
            'Проверьте формат ответа LLM.'
        )

    logger.debug('parse_llm_report: extracted %d change(s)', len(result['changes']))
    return result


# ---------------------------------------------------------------------------
# Legacy: parse JSON LLM response (falls back to parse_llm_report)
# ---------------------------------------------------------------------------

_STR_KEYS = [
    'name', 'formula', 'contemporary_meaning', 'variation',
    'synt_function_of_anchor', 'anchor_schema', 'anchor_ru', 'anchor_eng',
    'group_number', 'annotated_sample',
]

_CHANGE_STR_KEYS = [
    'change_name', 'stage', 'former_change', 'level', 'type_of_change',
    'subtype_of_change', 'first_attested', 'last_attested',
    'first_example', 'last_example', 'comment',
]

_CONSTRAINT_STR_KEYS = ['element', 'syntactic', 'semantic']


def _coerce_str(val: Any) -> str:
    return '' if val is None else str(val).strip()

def _coerce_bool(val: Any, default: bool = False) -> bool:
    if isinstance(val, bool): return val
    if isinstance(val, str):  return val.lower() in ('true', '1', 'yes')
    if isinstance(val, int):  return bool(val)
    return default

def _coerce_int_or_none(val: Any):
    if val is None or val == '': return None
    try: return int(val)
    except (ValueError, TypeError): return None

def _clean_constraint(raw: Any) -> dict:
    if not isinstance(raw, dict):
        return {k: '' for k in _CONSTRAINT_STR_KEYS}
    return {k: _coerce_str(raw.get(k)) for k in _CONSTRAINT_STR_KEYS}

def _clean_change(raw: Any) -> dict:
    if not isinstance(raw, dict):
        raw = {}
    change = {k: _coerce_str(raw.get(k)) for k in _CHANGE_STR_KEYS}
    raw_c = raw.get('constraints') or []
    if not isinstance(raw_c, list): raw_c = []
    change['constraints'] = [_clean_constraint(c) for c in raw_c]
    return change


def parse_llm_response(raw: str) -> dict:
    """Parse a raw JSON string into form_data. Falls back to parse_llm_report."""
    raw = raw.strip()
    raw = re.sub(r'^```(?:json)?\s*', '', raw)
    raw = re.sub(r'\s*```$', '', raw).strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.info('JSON parse failed (%s); trying report parser', exc)
        return parse_llm_report(raw)

    if not isinstance(data, dict):
        return parse_llm_report(raw)

    result: dict = {k: _coerce_str(data.get(k)) for k in _STR_KEYS}
    result['in_rus_constructicon'] = _coerce_bool(data.get('in_rus_constructicon'))
    result['rus_constructicon_id'] = _coerce_int_or_none(data.get('rus_constructicon_id'))
    raw_changes = data.get('changes') or []
    if not isinstance(raw_changes, list): raw_changes = []
    result['changes'] = [_clean_change(c) for c in raw_changes]
    return result


# ---------------------------------------------------------------------------
# Serialise form dict for DB storage
# ---------------------------------------------------------------------------

def serialize_form(data: dict) -> dict:
    """Normalise a form-data dict (from JS autosave POST) for json.dumps."""
    result: dict = {k: _coerce_str(data.get(k)) for k in _STR_KEYS}
    result['in_rus_constructicon'] = _coerce_bool(data.get('in_rus_constructicon'))
    result['rus_constructicon_id'] = _coerce_int_or_none(data.get('rus_constructicon_id'))
    raw_changes = data.get('changes') or []
    if not isinstance(raw_changes, list): raw_changes = []
    result['changes'] = [_clean_change(c) for c in raw_changes]
    return result