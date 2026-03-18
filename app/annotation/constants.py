"""
Change-level taxonomy extracted from the chinstr sheet of the source xlsx.
These are the controlled vocabulary values for Change.level, Change.type_of_change,
and Change.subtype_of_change.
"""

# Full taxonomy: level → type → [subtypes]
CHANGE_TAXONOMY = {
    'synt': {
        'Change in anchor': [
            'deidiomatization',
            'loss of a component',
            'replacement of a component',
            'adding a component',
            'standardization',
        ],
        'Change in inner syntax': [
            'adding a new dependent',
            'change in government: clause',
            'change in government: new case',
            'change in government: new clause',
            'change in government: new part of speech',
            'change in government: new preposition',
            'change in government: new verb form',
            'loss of component',
        ],
        'Change in outer syntax': [
            'change in polarity',
            'new syntactic role: adjunct of AdjP',
            'new syntactic role: adjunct of NP',
            'new syntactic role: adjunct of PredP',
            'new syntactic role: adjunct of VP',
            'new syntactic role: predicate',
        ],
    },
    'sem': {
        'new idiomatic use': [
            'metaphor',
            'metonymy',
            'widening',
            'narrowing',
            'rebranding',
        ],
        'Change in semantic compatibility': [
            'extension: new type of NP dependent',
            'extension: new type of N head',
            'extension: new type of V head',
            'extension: loss of scalarity',
            'compatibility constraint',
            'loss of compatibility constraint',
        ],
        'change in pragmatics': [
            'pragmaticalization of a routine',
            'depragmaticalization of a routine',
            'new illocutive goal',
        ],
    },
    'source': {
        'Source': [
            'Compositional source',
            'Idiomatic source',
        ],
    },
}

# Syntactic function values (mirrors SYNT_FUNCTION_OF_ANCHOR_VALUES in models.py,
# minus the sentinel <unknown>)
SYNT_FUNCTION_OF_ANCHOR_VALUES = [
    'Argument',
    'Coordinator',
    'Discourse Particle',
    'Government',
    'Matrix Predicate',
    'Modifier',
    'Nominal Quantifier',
    'Object',
    'Parenthetical',
    'Praedicative Expression',
    'Subject',
    'Subordinator',
    'Verb Predicate',
    'Word-Formation',
]

# Flat lists used for datalists and LLM system prompt
ALL_TYPES_OF_CHANGE = [
    t
    for level_types in CHANGE_TAXONOMY.values()
    for t in level_types.keys()
]

ALL_SUBTYPES_OF_CHANGE = [
    s
    for level_types in CHANGE_TAXONOMY.values()
    for subtypes in level_types.values()
    for s in subtypes
]

# Serialisable form of the taxonomy for use in the Jinja2 template (JS)
# Shape: { synt: { "Change in anchor": ["deidiomatization", ...], ... }, sem: {...}, source: {...} }
CHANGE_TAXONOMY_JSON_SAFE = {
    level: {
        type_name: subtypes
        for type_name, subtypes in types.items()
    }
    for level, types in CHANGE_TAXONOMY.items()
}