"""app/search/semantic.py

Vector (semantic) search over construction text fields.

The sentence-transformers model is loaded lazily on first use so the app
starts normally even before the model has been downloaded.

Public API
----------
build_index(db_session, published_only=True)
    Embed all constructions and persist to ConstructionEmbedding table.

semantic_search(query_text, db_session, top_k=20)
    Return [(construction_id, score), ...] ranked by cosine similarity.

Fields and weights
------------------
Three fields are embedded per construction:

  contemporary_meaning  — the semantic label (weight 1.0)
  variation             — variant forms (weight 0.5)
  change_comment        — all Change.comment values concatenated (weight 2.0)

The free-text annotations in change_comment are the richest semantic signal
and are therefore given the highest weight when combining scores.
"""
from __future__ import annotations

import json
import logging
import typing as T

import numpy as np
from sqlalchemy import select
from sqlalchemy.orm import scoped_session

from config import Config

logger = logging.getLogger(f"diachronicon.{__name__}")

# Fields embedded per construction.
# "change_comment" is a virtual field: all Change.comment values for a
# construction are joined and embedded as a single vector.
EMBED_FIELDS: T.List[str] = [
    "contemporary_meaning",
    "variation",
    "change_comment",
]

# Weight applied to each field's cosine score before the weighted average.
# Higher value = stronger influence on final ranking.
FIELD_WEIGHTS: T.Dict[str, float] = {
    "contemporary_meaning": 1.0,
    "variation": 0.5,
    "change_comment": 2.0,
}

# Separator used when joining multiple change comments into one text
_COMMENT_SEP = " | "

# Cached model — loaded once per process on first call
_model = None


def _get_model():
    """Lazy-load the sentence-transformers model."""
    global _model
    if _model is None:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ImportError(
                "sentence-transformers is not installed. "
                "Run: pip install sentence-transformers"
            )
        model_name = getattr(Config, "EMBEDDING_MODEL",
                             "paraphrase-multilingual-mpnet-base-v2")
        logger.info(f"Loading embedding model: {model_name}")
        _model = SentenceTransformer(model_name)
        logger.info("Embedding model loaded.")
    return _model


def _embed(texts: T.List[str]) -> np.ndarray:
    """Embed a list of strings, returning an (N, D) float32 array."""
    model = _get_model()
    return model.encode(texts, convert_to_numpy=True, show_progress_bar=False)


def _build_comment_texts(
    constructions, db_session
) -> T.Dict[int, str]:
    """Return a mapping of construction_id → concatenated change comments.

    Only non-empty, non-null comments are included.
    Constructions with no comments are omitted from the returned dict.
    """
    from app.models import Change

    construction_ids = [c.id for c in constructions]

    changes = (
        db_session.query(Change)
        .filter(
            Change.construction_id.in_(construction_ids),
            Change.comment.isnot(None),
            Change.comment != '',
        )
        .all()
    )

    comment_map: T.Dict[int, T.List[str]] = {}
    for ch in changes:
        comment_map.setdefault(ch.construction_id, []).append(ch.comment.strip())

    return {
        cid: _COMMENT_SEP.join(comments)
        for cid, comments in comment_map.items()
        if comments
    }


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def build_index(
    db_session: scoped_session,
    published_only: bool = True,
    batch_size: int = 64,
    verbose: bool = False,
) -> int:
    """Compute embeddings for all constructions and store in the database.

    Existing rows for a construction+field are replaced (upsert by delete+insert).

    Parameters
    ----------
    db_session:
        Active SQLAlchemy scoped session.
    published_only:
        If True (default), only embed published constructions.
    batch_size:
        Number of constructions to embed in each forward pass.
    verbose:
        Print progress to stdout.

    Returns
    -------
    int
        Number of embedding rows written.
    """
    from app.models import Construction, ConstructionEmbedding

    model_name = getattr(Config, "EMBEDDING_MODEL",
                         "paraphrase-multilingual-mpnet-base-v2")

    # Fetch constructions
    stmt = select(Construction)
    if published_only:
        stmt = stmt.where(Construction.is_published.is_(True))
    constructions = db_session.execute(stmt).scalars().all()

    if not constructions:
        logger.warning("build_index: no constructions found.")
        return 0

    if verbose:
        print(f"Building index for {len(constructions)} constructions "
              f"across {len(EMBED_FIELDS)} fields…")

    # Pre-compute aggregated change comments (one text per construction)
    comment_texts = _build_comment_texts(constructions, db_session)
    if verbose:
        print(f"  {len(comment_texts)} constructions have change comments.")

    rows_written = 0

    for field_name in EMBED_FIELDS:

        # Build (construction_id, text) pairs for this field
        if field_name == "change_comment":
            id_text_pairs = [
                (cid, text)
                for cid, text in comment_texts.items()
            ]
        else:
            id_text_pairs = [
                (c.id, getattr(c, field_name, None))
                for c in constructions
                if getattr(c, field_name, None)
            ]

        if not id_text_pairs:
            if verbose:
                print(f"  [{field_name}] no data, skipping.")
            continue

        ids, texts = zip(*id_text_pairs)

        # Embed in batches
        all_embeddings: T.List[np.ndarray] = []
        for i in range(0, len(texts), batch_size):
            batch = list(texts[i: i + batch_size])
            vecs = _embed(batch)
            all_embeddings.append(vecs)
            if verbose:
                done = min(i + batch_size, len(texts))
                print(f"  [{field_name}] {done}/{len(texts)}")

        embeddings = np.vstack(all_embeddings)

        # Delete old rows for this field and replace
        (
            db_session.query(ConstructionEmbedding)
            .filter(
                ConstructionEmbedding.construction_id.in_(list(ids)),
                ConstructionEmbedding.field_name == field_name,
            )
            .delete(synchronize_session=False)
        )

        for constr_id, vec in zip(ids, embeddings):
            row = ConstructionEmbedding(
                construction_id=constr_id,
                field_name=field_name,
                embedding=json.dumps(vec.tolist()),
                embedding_model=model_name,
            )
            db_session.add(row)
            rows_written += 1

        db_session.commit()

    if verbose:
        print(f"Done. {rows_written} embedding rows written.")

    return rows_written


def semantic_search(
    query_text: str,
    db_session: scoped_session,
    top_k: int = 20,
    fields: T.Optional[T.List[str]] = None,
) -> T.List[T.Tuple[int, float]]:
    """Return construction IDs ranked by semantic similarity to query_text.

    Scores from different fields are combined as a weighted average using
    FIELD_WEIGHTS. Fields not present in FIELD_WEIGHTS default to weight 1.0.

    Parameters
    ----------
    query_text:
        Free-text query in any language (Russian preferred).
    db_session:
        Active SQLAlchemy scoped session.
    top_k:
        Maximum number of results to return.
    fields:
        Which embedding fields to search. Defaults to EMBED_FIELDS.

    Returns
    -------
    List of (construction_id, score) tuples, highest score first.
    Returns an empty list if no embeddings are stored.
    """
    from app.models import ConstructionEmbedding

    if not query_text or not query_text.strip():
        return []

    search_fields = fields or EMBED_FIELDS

    # Load all embeddings for the requested fields
    rows = (
        db_session.query(ConstructionEmbedding)
        .filter(ConstructionEmbedding.field_name.in_(search_fields))
        .all()
    )

    if not rows:
        logger.warning(
            "semantic_search: no embeddings found. "
            "Run `flask embeddings rebuild` first."
        )
        return []

    # Embed the query once
    query_vec = _embed([query_text.strip()])[0]
    query_norm = query_vec / (np.linalg.norm(query_vec) + 1e-10)

    # Accumulate weighted scores per construction
    # scores_by_id maps construction_id → list of (weighted_score, weight)
    scores_by_id: T.Dict[int, T.List[T.Tuple[float, float]]] = {}

    for row in rows:
        try:
            vec = np.array(json.loads(row.embedding), dtype=np.float32)
        except (json.JSONDecodeError, ValueError):
            continue

        vec_norm = vec / (np.linalg.norm(vec) + 1e-10)
        raw_score = float(np.dot(query_norm, vec_norm))
        weight = FIELD_WEIGHTS.get(row.field_name, 1.0)

        scores_by_id.setdefault(row.construction_id, []).append(
            (raw_score * weight, weight)
        )

    # Weighted average: sum(score * weight) / sum(weight)
    averaged: T.List[T.Tuple[int, float]] = []
    for cid, weighted_pairs in scores_by_id.items():
        total_weighted = sum(ws for ws, _ in weighted_pairs)
        total_weight = sum(w for _, w in weighted_pairs)
        averaged.append((cid, total_weighted / total_weight))

    averaged.sort(key=lambda x: x[1], reverse=True)
    return averaged[:top_k]