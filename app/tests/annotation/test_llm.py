"""
Unit tests for app/annotation/llm.py

All external API calls are mocked — no real credentials needed.
"""
import json
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from app.annotation.llm import (
    LLMError,
    OpenAIClient,
    AnthropicClient,
    GeminiClient,
    get_client,
    SYSTEM_PROMPT,
)


# ── get_client factory ─────────────────────────────────────────────────────

class TestGetClient:

    def test_returns_openai_client(self):
        client = get_client('openai', 'sk-test')
        assert isinstance(client, OpenAIClient)
        assert client.api_key == 'sk-test'
        assert client.model == 'gpt-4o'

    def test_returns_anthropic_client(self):
        client = get_client('anthropic', 'sk-ant-test')
        assert isinstance(client, AnthropicClient)

    def test_returns_gemini_client(self):
        client = get_client('gemini', 'AIzaSy-test')
        assert isinstance(client, GeminiClient)

    def test_custom_model_respected(self):
        client = get_client('openai', 'sk-test', model='gpt-4-turbo')
        assert client.model == 'gpt-4-turbo'

    def test_unknown_provider_raises(self):
        with pytest.raises(LLMError) as exc_info:
            get_client('grok', 'key')
        assert 'grok' in str(exc_info.value).lower()

    def test_empty_api_key_raises(self):
        with pytest.raises(LLMError):
            get_client('openai', '')

    def test_whitespace_api_key_raises(self):
        with pytest.raises(LLMError):
            get_client('openai', '   ')

    def test_provider_case_insensitive(self):
        client = get_client('OpenAI', 'sk-test')
        assert isinstance(client, OpenAIClient)

    def test_empty_model_falls_back_to_default(self):
        client = get_client('anthropic', 'sk-ant', model='')
        assert client.model
        assert 'claude' in client.model.lower()

    def test_gemini_default_model(self):
        client = get_client('gemini', 'key')
        assert 'gemini' in client.model.lower()


# ── SYSTEM_PROMPT content ──────────────────────────────────────────────────

class TestSystemPrompt:

    def test_contains_logging_form_field_labels(self):
        for label in ['Отношение', 'Описание', 'Формула',
                      'Уровень', 'Тип', 'Подтип',
                      'Первое вхождение', 'Последнее вхождение']:
            assert label in SYSTEM_PROMPT, f'Missing label: {label}'

    def test_contains_taxonomy_levels(self):
        for level in ['Source', 'Synt', 'Sem']:
            assert level in SYSTEM_PROMPT

    def test_contains_key_subtypes(self):
        for subtype in ['metaphor', 'metonymy', 'deidiomatization',
                        'Compositional source', 'Idiomatic source',
                        'pragmaticalization of a routine']:
            assert subtype in SYSTEM_PROMPT, f'Missing subtype: {subtype}'

    def test_contains_relationship_vocabulary(self):
        for term in ['Исходная форма', 'Следует за', 'Сопутствует']:
            assert term in SYSTEM_PROMPT, f'Missing relationship term: {term}'

    def test_contains_summary_table_header(self):
        assert 'Сводная таблица' in SYSTEM_PROMPT


# ── LLMError ──────────────────────────────────────────────────────────────

class TestLLMError:

    def test_to_dict(self):
        err = LLMError('something broke', 'openai')
        d = err.to_dict()
        assert d['error'] == 'something broke'
        assert d['provider'] == 'openai'

    def test_str(self):
        err = LLMError('msg', 'gemini')
        assert str(err) == 'msg'


# ── User message builder ───────────────────────────────────────────────────

class TestBuildUserMessage:

    def test_includes_all_parts(self):
        client = get_client('openai', 'sk-x')
        msg = client._build_user_message(
            'значение конструкции', '[NP]', 'корпусные примеры'
        )
        assert 'значение конструкции' in msg
        assert '[NP]' in msg
        assert 'корпусные примеры' in msg

    def test_empty_notes_still_produces_message(self):
        client = get_client('openai', 'sk-x')
        msg = client._build_user_message('смысл', '[VP]', '')
        assert 'смысл' in msg
        assert '[VP]' in msg


# ── Sample report (realistic minimal logging-form excerpt) ─────────────────

_SAMPLE_REPORT = """\
### 1. Исходная форма
**Отношение**: Исходная форма
**Описание**: Буквальное пространственное употребление.
**Формула**: `VP в точку`
**Уровень**: Source
**Тип**: Source
**Подтип**: Compositional source
**Первое вхождение**: 1823, Лобачевский. *Геометрия* — «в точку»
**Последнее вхождение**: 2019, Практика — «в точку»
"""


# ── OpenAIClient._call ─────────────────────────────────────────────────────

class TestOpenAIClient:

    def test_call_returns_content(self):
        client = OpenAIClient(api_key='sk-test', model='gpt-4o')
        fake_response = MagicMock()
        fake_response.choices[0].message.content = _SAMPLE_REPORT

        with patch('openai.OpenAI') as MockOpenAI:
            MockOpenAI.return_value.chat.completions.create.return_value = fake_response
            result = client._call('test message')

        assert 'Исходная форма' in result

    def test_sdk_import_error_raises_llm_error(self):
        client = OpenAIClient(api_key='sk', model='gpt-4o')
        with patch.dict('sys.modules', {'openai': None}):
            with pytest.raises(LLMError) as exc_info:
                client._call('msg')
            assert 'openai' in str(exc_info.value).lower()

    def test_api_exception_wrapped_as_llm_error(self):
        client = OpenAIClient(api_key='sk', model='gpt-4o')
        with patch('openai.OpenAI') as MockOpenAI:
            MockOpenAI.return_value.chat.completions.create.side_effect = (
                RuntimeError('rate limit')
            )
            with pytest.raises(LLMError):
                client.annotate('text', '[X]', '')


# ── AnthropicClient._call ──────────────────────────────────────────────────

class TestAnthropicClient:

    def test_call_returns_text(self):
        client = AnthropicClient(api_key='sk-ant', model='claude-sonnet-4-6')
        fake_response = MagicMock()
        fake_response.content[0].text = _SAMPLE_REPORT

        with patch('anthropic.Anthropic') as MockAnthropic:
            MockAnthropic.return_value.messages.create.return_value = fake_response
            result = client._call('msg')

        assert 'Исходная форма' in result

    def test_passes_system_prompt(self):
        client = AnthropicClient(api_key='sk-ant', model='claude-sonnet-4-6')
        fake_response = MagicMock()
        fake_response.content[0].text = _SAMPLE_REPORT

        with patch('anthropic.Anthropic') as MockAnthropic:
            mock_create = MockAnthropic.return_value.messages.create
            mock_create.return_value = fake_response
            client._call('msg')
            _, kwargs = mock_create.call_args
            assert kwargs.get('system') == SYSTEM_PROMPT

    def test_max_tokens_generous(self):
        client = AnthropicClient(api_key='sk-ant', model='claude-sonnet-4-6')
        fake_response = MagicMock()
        fake_response.content[0].text = _SAMPLE_REPORT

        with patch('anthropic.Anthropic') as MockAnthropic:
            mock_create = MockAnthropic.return_value.messages.create
            mock_create.return_value = fake_response
            client._call('msg')
            _, kwargs = mock_create.call_args
            assert kwargs.get('max_tokens', 0) >= 4096

    def test_sdk_import_error_raises_llm_error(self):
        client = AnthropicClient(api_key='k', model='claude-sonnet-4-6')
        with patch.dict('sys.modules', {'anthropic': None}):
            with pytest.raises(LLMError):
                client._call('msg')


# ── GeminiClient._call ─────────────────────────────────────────────────────

class TestGeminiClient:

    def _make_genai_mock(self, return_text: str):
        """
        Build a mock for google.generativeai that returns *return_text* from
        generate_content().text.

        The key fix vs the previous version: we set mock_google.generativeai
        to the same object as mock_genai so that both
          sys.modules['google.generativeai']
        and
          sys.modules['google'].generativeai
        resolve to the same mock.  Without this, Python's import machinery
        may follow the attribute chain on the 'google' module mock and return
        a fresh MagicMock that is unrelated to our configured mock_genai.
        """
        fake_response = MagicMock()
        fake_response.text = return_text

        mock_model_instance = MagicMock()
        mock_model_instance.generate_content.return_value = fake_response

        mock_genai = MagicMock()
        mock_genai.GenerativeModel.return_value = mock_model_instance

        # Critical: make google.generativeai accessible via attribute lookup
        # on the google package mock, not just via sys.modules key.
        mock_google = MagicMock()
        mock_google.generativeai = mock_genai

        modules = {
            'google':              mock_google,
            'google.generativeai': mock_genai,
        }
        return modules, mock_genai, mock_model_instance

    def test_call_returns_text(self):
        client = GeminiClient(api_key='AIza-test', model='gemini-2.0-flash')
        modules, mock_genai, _ = self._make_genai_mock(_SAMPLE_REPORT)

        with patch.dict('sys.modules', modules):
            result = client._call('msg')

        assert result == _SAMPLE_REPORT

    def test_configure_called_with_api_key(self):
        client = GeminiClient(api_key='AIza-test', model='gemini-2.0-flash')
        modules, mock_genai, _ = self._make_genai_mock(_SAMPLE_REPORT)

        with patch.dict('sys.modules', modules):
            client._call('msg')

        mock_genai.configure.assert_called_once_with(api_key='AIza-test')

    def test_generative_model_called_with_correct_model(self):
        client = GeminiClient(api_key='AIza-test', model='gemini-2.0-flash')
        modules, mock_genai, _ = self._make_genai_mock(_SAMPLE_REPORT)

        with patch.dict('sys.modules', modules):
            client._call('msg')

        call_kwargs = mock_genai.GenerativeModel.call_args
        assert call_kwargs is not None
        # model_name is the first positional or keyword arg
        args, kwargs = call_kwargs
        model_name = kwargs.get('model_name') or (args[0] if args else None)
        assert model_name == 'gemini-2.0-flash'

    def test_generate_content_called_with_message(self):
        client = GeminiClient(api_key='AIza-test', model='gemini-2.0-flash')
        modules, _, mock_model = self._make_genai_mock(_SAMPLE_REPORT)

        with patch.dict('sys.modules', modules):
            client._call('test user message')

        mock_model.generate_content.assert_called_once_with('test user message')

    def test_sdk_import_error_raises_llm_error(self):
        client = GeminiClient(api_key='key', model='gemini-2.0-flash')
        with patch.dict('sys.modules', {'google': None, 'google.generativeai': None}):
            with pytest.raises((LLMError, Exception)):
                client._call('msg')