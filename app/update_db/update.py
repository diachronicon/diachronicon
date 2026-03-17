"""app/update_db/update.py

Import / refresh the Diachronicon database from the canonical Excel workbook.

Sheets consumed:
    cnstruct  — one row per construction formula
    gen_inf   — display name, group number, links, publication status (130 rows)
    ch        — diachronic changes (720 rows across 130 constructions)

Usage (from project root):
    python -m app.update_db.update --excel path/to/Diachronicon.xlsx
    python -m app.update_db.update --excel path/to/Diachronicon.xlsx --clear
    python -m app.update_db.update --excel path/to/Diachronicon.xlsx --dry-run

Options:
    --excel PATH   Path to the .xlsx workbook  (required)
    --clear        Wipe and re-import everything (default: upsert)
    --dry-run      Parse and validate without writing to the database
    --verbose      Print per-row progress
"""
from __future__ import annotations

import abc
import argparse
import logging
import re
import sys
import traceback
import typing as T
from collections import defaultdict
from pathlib import Path

import pandas as pd

logging.basicConfig(
    format='%(levelname)s [%(name)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Column-name normalisation maps (Excel header → ORM field name)
# ---------------------------------------------------------------------------

CNSTRUCT_RENAME: T.Dict[str, str] = {
    'construction_id':              'orig_id',
    'contemporary meaning':         'contemporary_meaning',
    'in russian constructicon':     'in_rus_constructicon',
    'number in russian constructicon': 'rus_constructicon_id',
}

GEN_INF_RENAME: T.Dict[str, str] = {
    'construction name': 'name',
}

CH_RENAME: T.Dict[str, str] = {
    'construction formula': 'stage',
    'former change':        'former_change',
    'type of change':       'type_of_change',
    'subtype of change':    'subtype_of_change',
    'first entry':          'first_attested',
    'last entry':           'last_attested',
    'first example':        'first_example',
    'last example':         'last_example',
}


# ---------------------------------------------------------------------------
# Formula tokenisation  (builds FormulaElement rows)
# ---------------------------------------------------------------------------

VARIANTS_SEPS: T.Tuple[str, ...] = ('/', '|')


class _EOF:
    """Sentinel for end-of-iterator."""


EOF = _EOF()


def _read_until(
    it: T.Iterator, stop: T.Callable[[T.Any], bool]
) -> T.Tuple[str, T.Any]:
    buf: T.List[str] = []
    while True:
        nxt = next(it, EOF)
        if stop(nxt):
            return ''.join(buf), nxt
        buf.append(nxt)  # type: ignore[arg-type]


def tokenize_formula(
    formula: str,
    elems_sep: str = ' ',
    span_start: str = '(',
    span_end: str = ')',
    variants_seps: T.Tuple[str, ...] = VARIANTS_SEPS,
) -> T.List[T.Dict]:
    """Tokenise a construction formula into a nested token list."""
    SPECIAL = {elems_sep, span_start, span_end, *variants_seps}

    parts: T.List[T.Dict] = []
    cur_part: T.List[T.Dict] = parts
    queue: T.List[T.List] = [parts]
    it = iter(formula)
    symbol: T.Any = ''

    while True:
        symbol, prev = next(it, EOF), symbol

        if symbol is EOF:
            break

        if symbol not in SPECIAL:
            rest, special = _read_until(it, lambda s: s in SPECIAL or s is EOF)
            cur_part.append({'val': symbol + rest})
            symbol, prev = special, symbol
            if symbol is EOF:
                break

        if symbol == span_start:
            cur_part = []
            queue.append(cur_part)
        elif symbol == span_end:
            result = {'type': 'maybe_span', 'val': queue.pop()}
            cur_part = queue[-1]
            cur_part.append(result)

    return parts


def flatten_span(
    tokens: T.List[T.Dict], depth: int = 1, order: int = 0
) -> T.List[T.Dict]:
    """Flatten a span token list into FormulaElement-shaped dicts."""
    flat: T.List[T.Dict] = []

    if len(tokens) == 1:
        return [{'value': tokens[0]['val'], 'is_optional': True, 'depth': depth - 1}]

    for tok in tokens:
        if tok.get('type') == 'maybe_span':
            flat.extend(flatten_span(tok['val'], depth=depth + 1))
        else:
            flat.append({'value': tok['val'], 'depth': depth, 'is_optional': False})

    return flat


def parse_formula(formula: str) -> T.List[T.Dict]:
    """Return a list of element dicts ready to be stored as FormulaElement rows."""
    elements: T.List[T.Dict] = []
    order = 0

    for tok in tokenize_formula(formula):
        if tok.get('type') == 'maybe_span':
            span_els = flatten_span(tok['val'])
            for i, el in enumerate(span_els):
                el['order'] = order + i
            order += len(span_els)
            elements.extend(span_els)
        else:
            elements.append({'value': tok['val'], 'order': order})
            order += 1

    return elements


# ---------------------------------------------------------------------------
# Year / date helpers
# ---------------------------------------------------------------------------

def _parse_year_str(raw: T.Any, left_bias: float = 0.5) -> T.Optional[int]:
    """Return an integer year from a cell value.  Returns None if unparseable."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip()
    if not s or s == '-':
        return None
    if s.isnumeric():
        return int(s)
    if '-' in s:
        if s.endswith('-ые'):
            return int(s.split('-')[0])
        parts = s.split('-')
        if len(parts) == 2:
            try:
                lo, hi = int(parts[0]), int(parts[1])
                return int(lo + (hi - lo) * left_bias)
            except ValueError:
                pass
    return None


def _clean_str(val: T.Any) -> T.Optional[str]:
    """Return a stripped string or None for empty/NaN/dash values."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return None if s in ('', '-', 'NaN') else s


def _clean_bool(val: T.Any) -> T.Optional[bool]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    s = str(val).strip().lower()
    if s in ('1', 'yes', 'true', 'да'):
        return True
    if s in ('0', 'no', 'false', 'нет'):
        return False
    return None


def _clean_int(val: T.Any) -> T.Optional[int]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None


def _parse_former_change(raw: T.Any) -> T.List[int]:
    """Parse former_change cell → list of Excel change_id integers."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return []
    s = str(raw).strip()
    if not s or s == '-':
        return []
    ids: T.List[int] = []
    for part in re.split(r'[,\s]+', s):
        part = part.strip()
        if part.isnumeric():
            ids.append(int(part))
    return ids


# ---------------------------------------------------------------------------
# Main importer class
# ---------------------------------------------------------------------------

class DiachroniconImporter:
    """Reads the Excel workbook and writes to the Diachronicon database."""

    def __init__(
        self,
        excel_path: str | Path,
        db_session,
        clear: bool = False,
        dry_run: bool = False,
        verbose: bool = False,
    ):
        self.excel_path = Path(excel_path)
        self.session = db_session
        self.clear = clear
        self.dry_run = dry_run
        self.verbose = verbose

        self._stats: T.Dict[str, int] = defaultdict(int)
        # Map:  orig_id (str) → Construction.id  (set after DB flush)
        self._orig_id_to_db_id: T.Dict[str, int] = {}
        # Map:  excel_change_id (int) → Change.id  (set after DB flush)
        self._excel_change_id_to_db_id: T.Dict[int, int] = {}

    # ------------------------------------------------------------------ #
    # Entry point
    # ------------------------------------------------------------------ #

    def run(self) -> None:
        logger.info(f"Loading workbook: {self.excel_path}")
        xl = pd.read_excel(str(self.excel_path), sheet_name=None, dtype=str)

        df_cnstruct = self._load_cnstruct(xl)
        df_gen_inf = self._load_gen_inf(xl)
        df_ch = self._load_ch(xl)

        if self.dry_run:
            logger.info("Dry-run mode: validation only, no DB writes.")
            self._validate(df_cnstruct, df_gen_inf, df_ch)
            return

        if self.clear:
            self._clear_tables()

        self._import_constructions(df_cnstruct, df_gen_inf)
        self._import_changes(df_ch)
        self._resolve_change_graph(df_ch)

        logger.info(
            "Import complete.  Stats: %s",
            {k: v for k, v in sorted(self._stats.items())},
        )

    # ------------------------------------------------------------------ #
    # Sheet loaders
    # ------------------------------------------------------------------ #

    def _load_cnstruct(self, xl: T.Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = xl['cnstruct'].copy()
        df.columns = [c.strip().lower() for c in df.columns]
        df.rename(columns=CNSTRUCT_RENAME, inplace=True)
        df['orig_id'] = df['orig_id'].apply(lambda x: _clean_str(x) or '')
        return df

    def _load_gen_inf(self, xl: T.Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = xl['gen_inf'].copy()
        # Drop the pivot-table summary columns that appear to the right
        df = df[[c for c in df.columns if not str(c).startswith('Unnamed:')]]
        df.columns = [c.strip().lower() for c in df.columns]
        df.rename(columns=GEN_INF_RENAME, inplace=True)
        df['construction_id'] = df['construction_id'].apply(_clean_int)
        df = df[df['construction_id'].notna()].copy()
        df['construction_id'] = df['construction_id'].astype(int)
        return df

    def _load_ch(self, xl: T.Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = xl['ch'].copy()
        df.columns = [c.strip().lower() for c in df.columns]
        df.rename(columns=CH_RENAME, inplace=True)
        df['construction_id'] = df['construction_id'].apply(_clean_int)
        df['change_id'] = df['change_id'].apply(_clean_int)
        df = df[df['construction_id'].notna() & df['change_id'].notna()].copy()
        df['construction_id'] = df['construction_id'].astype(int)
        df['change_id'] = df['change_id'].astype(int)
        return df

    # ------------------------------------------------------------------ #
    # Validation (dry-run)
    # ------------------------------------------------------------------ #

    def _validate(
        self,
        df_cnstruct: pd.DataFrame,
        df_gen_inf: pd.DataFrame,
        df_ch: pd.DataFrame,
    ) -> None:
        logger.info(f"  cnstruct rows:   {len(df_cnstruct)}")
        logger.info(f"  gen_inf rows:    {len(df_gen_inf)}")
        logger.info(f"  changes rows:    {len(df_ch)}")

        gen_ids = set(df_gen_inf['construction_id'])
        ch_ids = set(df_ch['construction_id'])
        overlap = gen_ids & ch_ids
        logger.info(
            f"  gen_inf ∩ ch:    {len(overlap)} constructions have both metadata and changes"
        )
        missing_ch = gen_ids - ch_ids
        if missing_ch:
            logger.warning(f"  Constructions in gen_inf with no changes: {missing_ch}")

    # ------------------------------------------------------------------ #
    # Database wipe
    # ------------------------------------------------------------------ #

    def _clear_tables(self) -> None:
        from app.models import (
            Change, Construction, ConstructionVariant, Constraint,
            FormulaElement, GeneralInfo, ConstructionEmbedding, AnnotationDraft,
            change_to_previous_changes, construction_to_tags, change_to_tags,
        )
        logger.info("Clearing existing data…")
        # Order matters: child tables before parent tables
        for tbl in (
            change_to_previous_changes,
            change_to_tags,
            construction_to_tags,
        ):
            self.session.execute(tbl.delete())
        for model in (
            AnnotationDraft, ConstructionEmbedding,
            Constraint, FormulaElement, ConstructionVariant,
            Change, GeneralInfo,
        ):
            self.session.query(model).delete()
        self.session.query(Construction).delete()
        self.session.commit()
        logger.info("Tables cleared.")

    # ------------------------------------------------------------------ #
    # Construction import
    # ------------------------------------------------------------------ #

    def _import_constructions(
        self,
        df_cnstruct: pd.DataFrame,
        df_gen_inf: pd.DataFrame,
    ) -> None:
        from app.models import Construction, GeneralInfo, FormulaElement

        # Build gen_inf lookup keyed by integer construction_id
        gen_inf_by_int_id: T.Dict[int, T.Dict] = {}
        for _, row in df_gen_inf.iterrows():
            int_id = int(row['construction_id'])
            gen_inf_by_int_id[int_id] = row.to_dict()

        # Build set of integer IDs that have changes (to set is_published correctly)
        # (populated later; for now all constructions start as draft)

        logger.info(f"Importing {len(df_cnstruct)} constructions…")

        for _, row in df_cnstruct.iterrows():
            orig_id_raw = _clean_str(row.get('orig_id', '')) or ''

            # Try to extract the base integer ID from values like '1', '1(221)', '1(19?)'
            base_int_id = _extract_base_int(orig_id_raw)

            # Resolve visibility: published if gen_inf.status == 'ready'
            gen_row = gen_inf_by_int_id.get(base_int_id, {}) if base_int_id else {}
            gen_status = _clean_str(gen_row.get('status', ''))
            is_ready = gen_status == 'ready'

            formula_str = _clean_str(row.get('formula', ''))
            if not formula_str:
                self._stats['skipped_no_formula'] += 1
                continue

            constr = self._get_or_create_construction(orig_id_raw)

            constr.formula = formula_str
            constr.contemporary_meaning = _clean_str(row.get('contemporary_meaning'))
            constr.variation = _clean_str(row.get('variation'))
            constr.in_rus_constructicon = _clean_bool(row.get('in_rus_constructicon'))
            constr.rus_constructicon_id = _clean_int(row.get('rus_constructicon_id'))
            constr.synt_function_of_anchor = _clean_str(
                row.get('synt_function_of_anchor')
            )
            constr.anchor_schema = _clean_str(row.get('anchor_schema'))
            constr.anchor_ru = _clean_str(row.get('anchor_ru'))
            constr.anchor_eng = _clean_str(row.get('anchor_eng'))

            # Visibility: ready constructions are published and not draft
            constr.is_published = is_ready
            constr.is_draft = not is_ready

            # Attach GeneralInfo for constructions that have a matching gen_inf row
            if gen_row and base_int_id:
                if constr.general_info is None:
                    constr.general_info = GeneralInfo(construction=constr)
                gi = constr.general_info
                gi.name = _clean_str(gen_row.get('name'))
                gi.group_number = _clean_str(gen_row.get('group_number'))
                gi.annotated_sample = _clean_str(gen_row.get('annotated_sample'))
                gi.term_paper = _clean_str(gen_row.get('term_paper'))
                gi.status = gen_status

            # Formula elements — rebuild for this construction
            constr.formula_elements = []
            for el_dict in parse_formula(formula_str):
                fe = FormulaElement(
                    value=el_dict.get('value'),
                    order=el_dict.get('order', 0),
                    depth=el_dict.get('depth', 0),
                    is_optional=el_dict.get('is_optional', False),
                    has_variants='/' in (el_dict.get('value') or ''),
                )
                constr.formula_elements.append(fe)

            self.session.add(constr)
            self._stats['constructions_upserted'] += 1

            if self.verbose:
                logger.debug(f"  Construction {orig_id_raw}: {formula_str[:60]}")

        self.session.flush()

        # Populate the orig_id → db_id map after flush (IDs are now assigned)
        from app.models import Construction as _C
        for c in self.session.query(_C).all():
            if c.orig_id:
                self._orig_id_to_db_id[c.orig_id] = c.id

        self.session.commit()
        logger.info(
            f"  {self._stats['constructions_upserted']} constructions written."
        )

    def _get_or_create_construction(self, orig_id: str):
        from app.models import Construction
        existing = (
            self.session.query(Construction)
            .filter_by(orig_id=orig_id)
            .first()
        )
        if existing:
            return existing
        c = Construction(orig_id=orig_id)
        return c

    # ------------------------------------------------------------------ #
    # Change import
    # ------------------------------------------------------------------ #

    def _import_changes(self, df_ch: pd.DataFrame) -> None:
        from app.models import Change, Construction

        # Build reverse map: integer construction_id → DB Construction.id
        # using orig_id (plain integer string)
        int_to_db_constr_id: T.Dict[int, int] = {}
        from app.models import Construction as _C
        for c in self.session.query(_C).all():
            base = _extract_base_int(c.orig_id or '')
            if base and base not in int_to_db_constr_id:
                int_to_db_constr_id[base] = c.id

        logger.info(f"Importing {len(df_ch)} changes…")

        for _, row in df_ch.iterrows():
            excel_cid = int(row['construction_id'])
            excel_change_id = int(row['change_id'])

            db_constr_id = int_to_db_constr_id.get(excel_cid)
            if db_constr_id is None:
                logger.warning(
                    f"  Change {excel_change_id}: no Construction for "
                    f"construction_id={excel_cid}, skipping."
                )
                self._stats['changes_skipped_no_construction'] += 1
                continue

            first_att_raw = _clean_str(row.get('first_attested'))
            last_att_raw = _clean_str(row.get('last_attested'))

            change = self._get_or_create_change(excel_change_id, db_constr_id)

            change.construction_id = db_constr_id
            change.stage = _clean_str(row.get('stage'))
            change.former_change = _clean_str(row.get('former_change'))
            change.level = _clean_str(row.get('level'))
            change.type_of_change = _clean_str(row.get('type_of_change'))
            change.subtype_of_change = _clean_str(row.get('subtype_of_change'))
            change.comment = _clean_str(row.get('comment'))
            change.first_attested = first_att_raw
            change.last_attested = last_att_raw
            change.first_attested_year = _parse_year_str(first_att_raw)
            change.last_attested_year = _parse_year_str(last_att_raw)
            change.first_example = _clean_str(row.get('first_example'))
            change.last_example = _clean_str(row.get('last_example'))
            change.frequency_trend = _clean_str(row.get('frequency_trend'))
            change.sources = _clean_str(row.get('sources'))

            self.session.add(change)
            self._stats['changes_upserted'] += 1

        self.session.flush()

        # Populate excel_change_id → DB id map
        # We stored the excel change_id temporarily in Change.former_change metadata;
        # instead, query by construction_id + stage to rebuild the map robustly.
        # Simpler: iterate the dataframe again now that all rows are flushed.
        from app.models import Change as _Ch
        # Build a (construction_id_excel, excel_change_id) → DB change map
        # by reading back all changes and correlating via construction
        self._excel_change_id_to_db_id = {}
        for _, row in df_ch.iterrows():
            excel_cid = int(row['construction_id'])
            excel_chid = int(row['change_id'])
            db_constr_id = int_to_db_constr_id.get(excel_cid)
            if db_constr_id is None:
                continue
            # Find the change we just wrote by matching stage + construction_id
            stage = _clean_str(row.get('stage'))
            level = _clean_str(row.get('level'))
            hit = (
                self.session.query(_Ch)
                .filter_by(
                    construction_id=db_constr_id,
                    stage=stage,
                    level=level,
                )
                .first()
            )
            if hit:
                self._excel_change_id_to_db_id[excel_chid] = hit.id

        self.session.commit()
        logger.info(f"  {self._stats['changes_upserted']} changes written.")

    def _get_or_create_change(self, excel_change_id: int, db_constr_id: int):
        """Return an existing Change or a new unsaved one."""
        from app.models import Change
        # If we already resolved this excel id, look it up
        db_id = self._excel_change_id_to_db_id.get(excel_change_id)
        if db_id:
            existing = self.session.query(Change).get(db_id)
            if existing:
                return existing
        return Change()

    # ------------------------------------------------------------------ #
    # Change graph resolution  (former_change → previous_changes M2M)
    # ------------------------------------------------------------------ #

    def _resolve_change_graph(self, df_ch: pd.DataFrame) -> None:
        from app.models import Change, change_to_previous_changes

        # Clear existing graph edges to avoid duplicates on re-import
        self.session.execute(change_to_previous_changes.delete())
        self.session.flush()

        resolved = 0
        missing = 0

        for _, row in df_ch.iterrows():
            excel_chid = int(row['change_id'])
            db_chid = self._excel_change_id_to_db_id.get(excel_chid)
            if db_chid is None:
                continue

            change = self.session.query(Change).get(db_chid)
            if change is None:
                continue

            prev_excel_ids = _parse_former_change(row.get('former_change'))
            for prev_excel_id in prev_excel_ids:
                prev_db_id = self._excel_change_id_to_db_id.get(prev_excel_id)
                if prev_db_id is None:
                    logger.warning(
                        f"  former_change={prev_excel_id} referenced by "
                        f"change {excel_chid} not found in DB."
                    )
                    missing += 1
                    continue
                prev_change = self.session.query(Change).get(prev_db_id)
                if prev_change and prev_change not in change.previous_changes:
                    change.previous_changes.append(prev_change)
                    resolved += 1

        self.session.commit()
        logger.info(
            f"  Change graph: {resolved} edges resolved, {missing} references unresolved."
        )
        self._stats['graph_edges_resolved'] = resolved
        self._stats['graph_edges_unresolved'] = missing


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _extract_base_int(orig_id: str) -> T.Optional[int]:
    """Extract the leading integer from IDs like '1', '1(221)', '10(202)'.

    Returns None for IDs that don't start with an integer, e.g. '1(19?)'.
    """
    if not orig_id:
        return None
    m = re.match(r'^(\d+)', orig_id.strip())
    if m:
        return int(m.group(1))
    return None


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description='Import the Diachronicon Excel workbook into the database.'
    )
    p.add_argument('--excel', required=True, help='Path to the .xlsx workbook')
    p.add_argument(
        '--clear',
        action='store_true',
        default=False,
        help='Delete all existing constructions and changes before import',
    )
    p.add_argument(
        '--dry-run',
        action='store_true',
        default=False,
        help='Parse and validate only; do not write to the database',
    )
    p.add_argument(
        '--verbose',
        action='store_true',
        default=False,
        help='Print per-row progress',
    )
    return p


def main() -> None:
    args = _build_argparser().parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    excel_path = Path(args.excel)
    if not excel_path.exists():
        logger.error(f"File not found: {excel_path}")
        sys.exit(1)

    # Bootstrap the database connection
    from app.database_utils import get_default_database
    from app.database_utils import init_db
    from app.models import Base

    engine, db_session = get_default_database()

    if not args.dry_run:
        # Ensure all tables exist before importing
        init_db(Base, engine)

    importer = DiachroniconImporter(
        excel_path=excel_path,
        db_session=db_session,
        clear=args.clear,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    try:
        importer.run()
    except Exception:
        traceback.print_exc()
        if not args.dry_run:
            db_session.rollback()
        sys.exit(1)
    finally:
        db_session.remove()


if __name__ == '__main__':
    main()