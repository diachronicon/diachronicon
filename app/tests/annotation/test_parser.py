"""
Unit tests for app/annotation/parser.py

Tests cover:
  - parse_llm_report:  GPT style, Sonnet style, Opus style
  - parse_llm_response: JSON path, fallback to report parser, fence-stripping
  - serialize_form:    type coercion, roundtrip consistency
  - _extract_year:     all attested date formats
"""
import json
import pytest

from app.annotation.parser import (
    parse_llm_report,
    parse_llm_response,
    serialize_form,
    _extract_year,
    _normalise_relationship,
)


# ── Sample texts in each format ────────────────────────────────────────────

# GPT 4o / 5 style: ### N. title, **Field**: value, --- separator
GPT_TWO_CHANGES = """
## Список изменений

### 1. Буквальное пространственное употребление

**Отношение**: Исходная форма  
**Описание**: Самое раннее употребление — буквальное пространственное.  
**Формула**: `VP в точку`  
**Уровень**: Source  
**Тип**: Source  
**Подтип**: Compositional source  
**Первое вхождение**: 1823, Н. И. Лобачевский. *Геометрия* — «не может падать в точку С»  
**Последнее вхождение**: 2014, А. Механик. *Порошки* — «непосредственно в точку»

---

### 2. Идиоматизация: попасть в точку = метко угадать

**Отношение**: Следует за 1  
**Описание**: Переносное оценочное значение.  
**Формула**: `VP в точку`  
**Уровень**: Sem  
**Тип**: new idiomatic use  
**Подтип**: metaphor  
**Первое вхождение**: 1863, В. А. Слепцов. *Письма об Осташкове* — «нечаянно попал в точку»  
**Последнее вхождение**: 2020, Н. Непряхин. *Анатомия заблуждений* — «попадание в точку»
"""

# Sonnet style: N. title, Field: value (no markdown bold, no --- separator)
SONNET_TWO_CHANGES = """
Список изменений
1. Композициональный источник
Отношение: Исходная форма
Описание: Первые фиксации конструкции в геометрических текстах.
Формула: VP в точку
Уровень: Source
Тип: Source
Подтип: Compositional source
Первое вхождение: Н. И. Лобачевский. Геометрия (1823) — «не может падать в точку F»
Последнее вхождение: Практика оконечивания кабеля (2019) — «из точки А в точку В»
2. Идиоматический источник
Отношение: Следует за 1
Описание: Метафорическое переосмысление: попасть в точку = угадать.
Формула: VP-попасть в (самую) точку
Уровень: Source
Тип: Source
Подтип: Idiomatic source
Первое вхождение: В. А. Слепцов. Письма об Осташкове (1863) — «нечаянно попал в точку»
Последнее вхождение: О. А. Славникова. 2017 (2017) — «попадая немедленно в точку»
"""

# Opus style: all fields in one soft-return paragraph per change
# (actual paragraph text contains \n + space as separator)
OPUS_TWO_CHANGES = """
Список изменений
1. Композициональный источник: «пруд прудить»
Отношение: Исходная форма\n Описание: Раннее употребление свободного словосочетания.\n Формула: NP-Ins разве пруд Verb-Inf(прудить)\n Уровень: Source\n Тип: Source\n Подтип: Compositional source\n Первое вхождение: 1790, П. А. Плавильщиков. Бобыль — «разве пруд прудить»\n Последнее вхождение: 2002, Л. Дубинина. Пруд качал в себе звезду
2. Идиоматический источник: стандартизация → «хоть пруд пруди»
Отношение: Следует за [1]\n Описание: Глагол застывает в форме императива.\n Формула: NP-Ins хоть пруд пруди\n Уровень: Source\n Тип: Source\n Подтип: Idiomatic source\n Первое вхождение: 1829, М. П. Погодин. Черная немочь — «Невест много, хоть пруд пруди»\n Последнее вхождение: 2014, М. Б. Бару. Повесть о двух головах
"""


# ── _extract_year ──────────────────────────────────────────────────────────

class TestExtractYear:
    def test_year_at_start(self):
        assert _extract_year('1823, Лобачевский') == '1823'

    def test_year_in_parens(self):
        assert _extract_year('Лобачевский. Геометрия (1823) — «текст»') == '1823'

    def test_year_range_at_start(self):
        assert _extract_year('1856-1857, Щедрин') == '1856-1857'

    def test_year_range_en_dash(self):
        assert _extract_year('1856–1857, Щедрин') == '1856–1857'

    def test_no_year_returns_empty(self):
        assert _extract_year('без даты, текст') == ''

    def test_empty_string(self):
        assert _extract_year('') == ''

    def test_year_range_short(self):
        # "1923-24" style
        result = _extract_year('1923-24, Волконский. Воспоминания')
        assert result.startswith('1923')


# ── _normalise_relationship ────────────────────────────────────────────────

class TestNormaliseRelationship:
    def test_removes_brackets(self):
        assert _normalise_relationship('Следует за [1]') == 'Следует за 1'

    def test_plain_unchanged(self):
        assert _normalise_relationship('Сопутствует 2') == 'Сопутствует 2'

    def test_initial_form(self):
        assert _normalise_relationship('Исходная форма') == 'Исходная форма'

    def test_strips_whitespace(self):
        assert _normalise_relationship('  Следует за [3]  ') == 'Следует за 3'


# ── parse_llm_report — GPT style ───────────────────────────────────────────

class TestParseLLMReportGPT:

    def test_extracts_two_changes(self):
        result = parse_llm_report(GPT_TWO_CHANGES)
        assert '_parse_error' not in result
        assert len(result['changes']) == 2

    def test_first_change_fields(self):
        result = parse_llm_report(GPT_TWO_CHANGES)
        ch = result['changes'][0]
        assert ch['level'] == 'source'
        assert ch['type_of_change'] == 'Source'
        assert ch['subtype_of_change'] == 'Compositional source'
        assert ch['former_change'] == 'Исходная форма'
        assert ch['first_attested'] == '1823'
        assert ch['last_attested'] == '2014'
        assert 'Лобачевский' in ch['first_example']

    def test_second_change_relationship(self):
        result = parse_llm_report(GPT_TWO_CHANGES)
        ch = result['changes'][1]
        assert ch['former_change'] == 'Следует за 1'
        assert ch['level'] == 'sem'
        assert ch['subtype_of_change'] == 'metaphor'
        assert ch['first_attested'] == '1863'

    def test_formula_stripped_of_backticks(self):
        result = parse_llm_report(GPT_TWO_CHANGES)
        ch = result['changes'][0]
        assert '`' not in ch['stage']
        assert 'VP' in ch['stage']

    def test_top_level_fields_empty(self):
        result = parse_llm_report(GPT_TWO_CHANGES)
        assert result['name'] == ''
        assert result['formula'] == ''

    def test_changes_have_empty_constraints(self):
        result = parse_llm_report(GPT_TWO_CHANGES)
        for ch in result['changes']:
            assert ch['constraints'] == []


# ── parse_llm_report — Sonnet style ───────────────────────────────────────

class TestParseLLMReportSonnet:

    def test_extracts_two_changes(self):
        result = parse_llm_report(SONNET_TWO_CHANGES)
        assert '_parse_error' not in result
        assert len(result['changes']) == 2

    def test_first_change_fields(self):
        result = parse_llm_report(SONNET_TWO_CHANGES)
        ch = result['changes'][0]
        assert ch['level'] == 'source'
        assert ch['former_change'] == 'Исходная форма'
        assert ch['first_attested'] == '1823'
        assert ch['last_attested'] == '2019'

    def test_second_change_fields(self):
        result = parse_llm_report(SONNET_TWO_CHANGES)
        ch = result['changes'][1]
        assert ch['former_change'] == 'Следует за 1'
        assert ch['subtype_of_change'] == 'Idiomatic source'
        assert ch['first_attested'] == '1863'
        assert ch['last_attested'] == '2017'

    def test_formula_preserved(self):
        result = parse_llm_report(SONNET_TWO_CHANGES)
        ch = result['changes'][1]
        assert 'попасть' in ch['stage'] or 'VP' in ch['stage']


# ── parse_llm_report — Opus style ─────────────────────────────────────────

class TestParseLLMReportOpus:

    def test_extracts_two_changes(self):
        result = parse_llm_report(OPUS_TWO_CHANGES)
        assert '_parse_error' not in result
        assert len(result['changes']) == 2

    def test_first_change_year(self):
        result = parse_llm_report(OPUS_TWO_CHANGES)
        ch = result['changes'][0]
        assert ch['first_attested'] == '1790'

    def test_second_change_bracket_relationship(self):
        result = parse_llm_report(OPUS_TWO_CHANGES)
        ch = result['changes'][1]
        # [1] brackets should be stripped
        assert ch['former_change'] == 'Следует за 1'
        assert '[' not in ch['former_change']

    def test_second_change_year(self):
        result = parse_llm_report(OPUS_TWO_CHANGES)
        ch = result['changes'][1]
        assert ch['first_attested'] == '1829'


# ── parse_llm_report — edge cases ─────────────────────────────────────────

class TestParseLLMReportEdgeCases:

    def test_empty_text_returns_error(self):
        result = parse_llm_report('')
        assert '_parse_error' in result

    def test_text_without_numbered_sections_returns_error(self):
        result = parse_llm_report('Здесь нет никаких пронумерованных разделов.')
        assert '_parse_error' in result

    def test_single_change_no_separator(self):
        text = (
            '1. Исходная форма\n'
            'Отношение: Исходная форма\n'
            'Формула: VP в точку\n'
            'Уровень: Source\n'
            'Тип: Source\n'
            'Подтип: Compositional source\n'
            'Первое вхождение: 1823, Лобачевский\n'
            'Последнее вхождение: 2019, Практика\n'
        )
        result = parse_llm_report(text)
        assert len(result['changes']) == 1
        assert result['changes'][0]['level'] == 'source'

    def test_level_normalisation_uppercase(self):
        text = (
            '1. Test\n'
            'Уровень: Synt\n'
            'Тип: Change in anchor\n'
            'Подтип: adding a component\n'
            'Первое вхождение: 1900, Тест\n'
            'Последнее вхождение: 2000, Тест\n'
        )
        result = parse_llm_report(text)
        assert result['changes'][0]['level'] == 'synt'

    def test_level_normalisation_sem(self):
        text = (
            '1. Test\n'
            'Уровень: Sem\n'
            'Тип: new idiomatic use\n'
            'Подтип: metaphor\n'
            'Первое вхождение: 1863, Автор\n'
            'Последнее вхождение: 2020, Автор\n'
        )
        result = parse_llm_report(text)
        assert result['changes'][0]['level'] == 'sem'


# ── parse_llm_response — JSON path and fallback ────────────────────────────

MINIMAL_JSON = {
    'name': 'тест', 'formula': '[X]', 'contemporary_meaning': 'смысл',
    'variation': '', 'in_rus_constructicon': False, 'rus_constructicon_id': None,
    'synt_function_of_anchor': '', 'anchor_schema': '', 'anchor_ru': '',
    'anchor_eng': '', 'group_number': '', 'annotated_sample': '',
    'changes': [],
}


class TestParseLLMResponse:

    def test_valid_json_parsed(self):
        result = parse_llm_response(json.dumps(MINIMAL_JSON))
        assert result['name'] == 'тест'
        assert result['changes'] == []

    def test_json_fence_stripped(self):
        raw = '```json\n' + json.dumps(MINIMAL_JSON) + '\n```'
        result = parse_llm_response(raw)
        assert '_parse_error' not in result

    def test_plain_fence_stripped(self):
        raw = '```\n' + json.dumps(MINIMAL_JSON) + '\n```'
        result = parse_llm_response(raw)
        assert '_parse_error' not in result

    def test_non_json_falls_back_to_report_parser(self):
        # A valid logging form should be parsed via the fallback
        result = parse_llm_response(SONNET_TWO_CHANGES)
        assert '_parse_error' not in result
        assert len(result['changes']) == 2

    def test_json_with_changes(self):
        data = dict(MINIMAL_JSON)
        data['changes'] = [{
            'change_name': 'тест', 'stage': '[X]', 'former_change': 'Исходная форма',
            'level': 'source', 'type_of_change': 'Source',
            'subtype_of_change': 'Compositional source',
            'first_attested': '1800', 'last_attested': '1900',
            'first_example': 'пример 1', 'last_example': 'пример 2',
            'comment': '', 'constraints': [],
        }]
        result = parse_llm_response(json.dumps(data))
        assert len(result['changes']) == 1
        assert result['changes'][0]['level'] == 'source'


# ── serialize_form ─────────────────────────────────────────────────────────

class TestSerializeForm:

    def test_empty_returns_skeleton(self):
        result = serialize_form({})
        assert result['changes'] == []
        assert result['in_rus_constructicon'] is False
        assert result['rus_constructicon_id'] is None

    def test_roundtrip(self):
        data = parse_llm_report(GPT_TWO_CHANGES)
        serialised = serialize_form(data)
        again = serialize_form(serialised)
        assert serialised == again

    def test_bool_coercion(self):
        result = serialize_form({'in_rus_constructicon': 'true'})
        assert result['in_rus_constructicon'] is True

    def test_int_coercion(self):
        result = serialize_form({'rus_constructicon_id': '42'})
        assert result['rus_constructicon_id'] == 42

    def test_none_text_becomes_empty_string(self):
        result = serialize_form({'name': None, 'formula': None})
        assert result['name'] == ''
        assert result['formula'] == ''

    def test_change_name_preserved(self):
        data = {'changes': [{'change_name': 'идиоматизация', 'stage': '[X]',
                              'level': 'sem', 'constraints': []}]}
        result = serialize_form(data)
        assert result['changes'][0]['change_name'] == 'идиоматизация'