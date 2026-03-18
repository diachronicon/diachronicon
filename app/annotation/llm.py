"""
LLM client abstraction for auto-annotation.

Three concrete clients are provided:
  - OpenAIClient    (openai SDK)
  - AnthropicClient (anthropic SDK)
  - GeminiClient    (google-generativeai SDK)

All share the same interface:
  client.annotate(construction_text, formula, notes) -> str

The returned string is the raw logging-form report in the format defined
by the annotation prompt (prompt_extended_design_16_03.md).
The caller passes it to parser.parse_llm_report().
"""
from __future__ import annotations

import logging

from app.annotation.constants import (
    CHANGE_TAXONOMY,
    SYNT_FUNCTION_OF_ANCHOR_VALUES,
)

logger = logging.getLogger(f'diachronicon.{__name__}')

# ---------------------------------------------------------------------------
# System prompt
# Derived from prompt_extended_design_16_03.md.
# The "search corpus" steps are reframed for the annotation tool context:
# the LLM receives the annotator's notes / corpus extracts and produces
# the logging form from that material.
# ---------------------------------------------------------------------------

_TAXONOMY_LINES: list[str] = []
for _level, _types in CHANGE_TAXONOMY.items():
    _TAXONOMY_LINES.append(f'\nLevel: {_level}')
    for _type, _subtypes in _types.items():
        _TAXONOMY_LINES.append(f'  Type: {_type}')
        for _sub in _subtypes:
            _TAXONOMY_LINES.append(f'    Subtype: {_sub}')
_TAXONOMY_TEXT = '\n'.join(_TAXONOMY_LINES)

_SYNT_FUNCTIONS = ' | '.join(SYNT_FUNCTION_OF_ANCHOR_VALUES)

SYSTEM_PROMPT = """\
You are a professional historical linguist specialising in Russian, working in \
the field of diachronic construction grammar, and an ace corpora researcher. \
Your task is to analyse a given Russian partially fixed expression \
(a **construction**) and track its history across a corpus.

The annotator will supply you with:
  1. The construction name and formula.
  2. A description of its contemporary meaning.
  3. Any corpus extracts, notes, or partial annotations they have already prepared.

### Your task

Based on the supplied material, produce a detailed numbered list of all \
significant syntactic and semantic changes that occurred during the \
construction's development. Follow the research algorithm below.

### Research Algorithm

1. Analyse both syntactic and semantic properties of each attested variant. \
   The expression must remain persistent and recognisable throughout its changes.
2. Produce a numbered list of all significant syntactic and semantic changes.
3. For every change, fill in the variation logging form (see format below).
4. Reorganise the list into the most linguistically sound developmental order.
5. Identify the relationship at the beginning of each change: whether it \
   succeeds the previous one or is concurrent with it.
6. Follow the logging form with a summary table.

### Strict constraints

* ONLY USE WHAT IS GIVEN IN THE SUPPLIED MATERIAL.
* DO NOT hallucinate. DO NOT invent examples or dates.
* If data is missing, admit it explicitly.
* Base all decisions on the supplied corpus data and general linguistic principles.

---

### Level / Type / Subtype taxonomy (select exactly one path per change)
""" + _TAXONOMY_TEXT + """

---

### Glosses for formulas

Content words: Noun, Adj, NumColl, NumCrd, NumOrd, PronDem, PronInt, PronPers,
  PronPoss, Adv, Pred, Aux, Bare, Cop, Cvb, Inf, PtcpAct, PtcpPass, Verb
Morphology suffixes (append to gloss): .Nom .Gen .Dat .Acc .Ins .Loc .Pl .Sg
  .Masc .Fem .Neut .Short .Comp .Sup .Prs .Pst .Fut .Pfv .Ipfv .1 .2 .3
  .Refl .Pass .Caus .Neg .Dim

Syntactic function in anchor:
""" + _SYNT_FUNCTIONS + """

---

### Output format — FOLLOW EXACTLY

Produce the variation logging form for EACH change, then the summary table.
Use Russian field labels exactly as shown. Do not add extra headings.

Variation logging form (repeat for each change):

### N. [short description of the change in Russian]
**Отношение**: [Исходная форма | Следует за N | Сопутствует N]
**Описание**: [paragraph describing the change in Russian]
**Формула**: [abstract construction formula using the glosses above]
**Уровень**: [Source | Synt | Sem]
**Тип**: [type from taxonomy]
**Подтип**: [subtype from taxonomy]
**Первое вхождение**: [year, Author. *Title* — «example text»]
**Последнее вхождение**: [year, Author. *Title* — «example text»]

---

Summary table (fill after the changes list):

## Сводная таблица изменений (в хронологическом порядке развития)

| № | Изменение | Формула | Первое вхождение (год, текст) | Последнее вхождение (год, текст) | Отношение |
|---|-----------|---------|-------------------------------|----------------------------------|-----------|
| 1 | … | … | … | … | Исходная форма |
| 2 | … | … | … | … | Следует за / Сопутствует [N] |
"""


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------

class LLMError(Exception):
    """Raised when an LLM call fails. Carries the provider name."""

    def __init__(self, message: str, provider: str):
        super().__init__(message)
        self.provider = provider

    def to_dict(self) -> dict:
        return {'error': str(self), 'provider': self.provider}


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------

class LLMClient:
    """Abstract base. Subclasses implement _call(user_message) -> str."""

    provider: str = 'base'

    def __init__(self, api_key: str, model: str):
        self.api_key = api_key
        self.model = model

    def _build_user_message(
        self, construction_text: str, formula: str, notes: str
    ) -> str:
        return (
            f'Construction name / contemporary meaning:\n{construction_text}\n\n'
            f'Formula: {formula}\n\n'
            f'Annotator notes / corpus extracts (may be empty):\n{notes}'
        )

    def annotate(
        self, construction_text: str, formula: str, notes: str
    ) -> str:
        """Return raw logging-form report string from the LLM."""
        user_message = self._build_user_message(
            construction_text, formula, notes
        )
        try:
            return self._call(user_message)
        except LLMError:
            raise
        except Exception as exc:
            logger.exception('LLM call failed (%s)', self.provider)
            raise LLMError(str(exc), self.provider) from exc

    def _call(self, user_message: str) -> str:  # pragma: no cover
        raise NotImplementedError


# ---------------------------------------------------------------------------
# OpenAI
# ---------------------------------------------------------------------------

class OpenAIClient(LLMClient):
    provider = 'openai'

    def _call(self, user_message: str) -> str:
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise LLMError(
                'openai package not installed. Run: pip install openai',
                self.provider,
            ) from exc

        client = OpenAI(api_key=self.api_key)
        response = client.chat.completions.create(
            model=self.model,
            messages=[
                {'role': 'system', 'content': SYSTEM_PROMPT},
                {'role': 'user',   'content': user_message},
            ],
            temperature=0.2,
        )
        return response.choices[0].message.content


# ---------------------------------------------------------------------------
# Anthropic
# ---------------------------------------------------------------------------

class AnthropicClient(LLMClient):
    provider = 'anthropic'

    def _call(self, user_message: str) -> str:
        try:
            import anthropic
        except ImportError as exc:
            raise LLMError(
                'anthropic package not installed. Run: pip install anthropic',
                self.provider,
            ) from exc

        client = anthropic.Anthropic(api_key=self.api_key)
        response = client.messages.create(
            model=self.model,
            max_tokens=8192,
            system=SYSTEM_PROMPT,
            messages=[{'role': 'user', 'content': user_message}],
            temperature=0.2,
        )
        return response.content[0].text


# ---------------------------------------------------------------------------
# Google Gemini
# ---------------------------------------------------------------------------

class GeminiClient(LLMClient):
    provider = 'gemini'

    def _call(self, user_message: str) -> str:
        try:
            import google.generativeai as genai
        except ImportError as exc:
            raise LLMError(
                'google-generativeai package not installed. '
                'Run: pip install google-generativeai',
                self.provider,
            ) from exc

        genai.configure(api_key=self.api_key)
        model = genai.GenerativeModel(
            model_name=self.model,
            system_instruction=SYSTEM_PROMPT,
            generation_config=genai.types.GenerationConfig(
                temperature=0.2,
            ),
        )
        response = model.generate_content(user_message)
        return response.text


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

_CLIENTS: dict[str, type[LLMClient]] = {
    'openai':    OpenAIClient,
    'anthropic': AnthropicClient,
    'gemini':    GeminiClient,
}

_DEFAULT_MODELS: dict[str, str] = {
    'openai':    'gpt-4o',
    'anthropic': 'claude-sonnet-4-6',
    'gemini':    'gemini-2.0-flash',
}


def get_client(provider: str, api_key: str, model: str | None = None) -> LLMClient:
    """Return a configured LLMClient. Raises LLMError for bad inputs."""
    provider = provider.lower().strip()
    if provider not in _CLIENTS:
        raise LLMError(
            f"Unknown provider '{provider}'. Choose: {', '.join(_CLIENTS)}",
            provider,
        )
    if not api_key or not api_key.strip():
        raise LLMError('API key is required.', provider)

    resolved_model = (model or '').strip() or _DEFAULT_MODELS[provider]
    return _CLIENTS[provider](api_key=api_key.strip(), model=resolved_model)