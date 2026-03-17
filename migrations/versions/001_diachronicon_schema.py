"""Full diachronicon schema — initial migration

Revision ID: 001_diachronicon_schema
Revises:
Create Date: 2024-01-01 00:00:00.000000

Fully idempotent: uses try/except around every DDL operation so the
migration succeeds regardless of prior partial runs, Base.metadata.create_all()
having been called, or SQLAlchemy Inspector cache staleness.
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine.reflection import Inspector

revision = '001_diachronicon_schema'
down_revision = None
branch_labels = None
depends_on = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_tables(conn) -> set:
    """Return current table names, bypassing any Inspector cache."""
    result = conn.execute(
        sa.text("SELECT name FROM sqlite_master WHERE type='table'")
    )
    return {row[0] for row in result}


def _get_columns(conn, table: str) -> set:
    result = conn.execute(sa.text(f"PRAGMA table_info('{table}')"))
    return {row[1] for row in result}


def _get_indexes(conn, table: str) -> set:
    result = conn.execute(sa.text(f"PRAGMA index_list('{table}')"))
    return {row[1] for row in result}


def _create_table(name: str, *args, **kwargs) -> None:
    """Create a table, silently ignoring 'already exists' errors."""
    try:
        op.create_table(name, *args, **kwargs)
    except Exception as e:
        if 'already exists' in str(e).lower():
            pass
        else:
            raise


def _add_column(table: str, column: sa.Column, conn) -> None:
    """Add a column, silently ignoring 'already exists' / 'duplicate column' errors."""
    if column.name in _get_columns(conn, table):
        return
    try:
        with op.batch_alter_table(table) as batch_op:
            batch_op.add_column(column)
    except Exception as e:
        msg = str(e).lower()
        if 'already exists' in msg or 'duplicate column' in msg:
            pass
        else:
            raise


def _create_index(name: str, table: str, cols: list, **kwargs) -> None:
    """Create an index, silently ignoring 'already exists' errors."""
    try:
        op.create_index(name, table, cols, **kwargs)
    except Exception as e:
        if 'already exists' in str(e).lower():
            pass
        else:
            raise


def _drop_column_if_exists(conn, table: str, col: str) -> None:
    if col not in _get_columns(conn, table):
        return
    try:
        with op.batch_alter_table(table) as batch_op:
            batch_op.drop_column(col)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# upgrade
# ---------------------------------------------------------------------------

def upgrade() -> None:
    conn = op.get_bind()

    # ------------------------------------------------------------------
    # Core linguistic tables
    # ------------------------------------------------------------------
    _create_table(
        'tag',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(100), nullable=True),
        sa.Column('kind', sa.Enum('sem', 'synt', name='tag_kind',
                                  create_constraint=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )

    _create_table(
        'construction',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('formula', sa.String(200), nullable=True),
        sa.Column('orig_id', sa.String(30), nullable=True),
        sa.Column('contemporary_meaning', sa.String(400), nullable=True),
        sa.Column('variation', sa.String(400), nullable=True),
        sa.Column('in_rus_constructicon', sa.Boolean(), nullable=True),
        sa.Column('rus_constructicon_id', sa.Integer(), nullable=True),
        sa.Column('synt_function_of_anchor', sa.String(100), nullable=True),
        sa.Column('anchor_schema', sa.String(200), nullable=True),
        sa.Column('anchor_ru', sa.String(200), nullable=True),
        sa.Column('anchor_eng', sa.String(200), nullable=True),
        sa.Column('is_published', sa.Boolean(), nullable=False,
                  server_default='0'),
        sa.Column('is_draft', sa.Boolean(), nullable=False,
                  server_default='1'),
        sa.PrimaryKeyConstraint('id'),
    )
    # Add new visibility columns when upgrading an old schema
    _add_column('construction',
                sa.Column('is_published', sa.Boolean(), nullable=False,
                          server_default='0'), conn)
    _add_column('construction',
                sa.Column('is_draft', sa.Boolean(), nullable=False,
                          server_default='1'), conn)
    # Drop legacy denormalised tag columns
    _drop_column_if_exists(conn, 'construction', 'morphosyntags')
    _drop_column_if_exists(conn, 'construction', 'semantags')

    _create_table(
        'general_info',
        sa.Column('construction_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(200), nullable=True),
        sa.Column('group_number', sa.String(20), nullable=True),
        sa.Column('annotated_sample', sa.String(400), nullable=True),
        sa.Column('term_paper', sa.String(400), nullable=True),
        sa.Column('status', sa.String(30), nullable=True),
        sa.ForeignKeyConstraint(['construction_id'], ['construction.id']),
        sa.PrimaryKeyConstraint('construction_id'),
    )
    # Drop old student-project annotator columns
    _drop_column_if_exists(conn, 'general_info', 'supervisor')
    _drop_column_if_exists(conn, 'general_info', 'author_name')
    _drop_column_if_exists(conn, 'general_info', 'author_surname')

    _create_table(
        'construction_variant',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('formula', sa.String(200), nullable=True),
        sa.Column('construction_id', sa.Integer(), nullable=True),
        sa.Column('change_id', sa.Integer(), nullable=True),
        sa.Column('is_main', sa.Boolean(), nullable=True),
        sa.ForeignKeyConstraint(['construction_id'], ['construction.id']),
        sa.PrimaryKeyConstraint('id'),
    )

    _create_table(
        'change',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('construction_id', sa.Integer(), nullable=True),
        sa.Column('stage', sa.String(400), nullable=True),
        sa.Column('former_change', sa.String(200), nullable=True),
        sa.Column('level', sa.String(10), nullable=True),
        sa.Column('type_of_change', sa.String(100), nullable=True),
        sa.Column('subtype_of_change', sa.String(200), nullable=True),
        sa.Column('first_attested', sa.String(20), nullable=True),
        sa.Column('last_attested', sa.String(20), nullable=True),
        sa.Column('first_attested_year', sa.Integer(), nullable=True),
        sa.Column('last_attested_year', sa.Integer(), nullable=True),
        sa.Column('first_example', sa.Text(), nullable=True),
        sa.Column('last_example', sa.Text(), nullable=True),
        sa.Column('comment', sa.Text(), nullable=True),
        sa.Column('frequency_trend', sa.String(400), nullable=True),
        sa.Column('sources', sa.String(500), nullable=True),
        sa.ForeignKeyConstraint(['construction_id'], ['construction.id']),
        sa.PrimaryKeyConstraint('id'),
    )
    _add_column('change',
                sa.Column('first_attested_year', sa.Integer(), nullable=True),
                conn)
    _add_column('change',
                sa.Column('last_attested_year', sa.Integer(), nullable=True),
                conn)
    _drop_column_if_exists(conn, 'change', 'morphosyntags')
    _drop_column_if_exists(conn, 'change', 'semantags')

    _create_index('ix_change_first_attested_year', 'change',
                  ['first_attested_year'])
    _create_index('ix_change_last_attested_year', 'change',
                  ['last_attested_year'])

    _create_table(
        'formula_element',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('formula_id', sa.Integer(), nullable=True),
        sa.Column('construction_id', sa.Integer(), nullable=True),
        sa.Column('construction_variant_id', sa.Integer(), nullable=True),
        sa.Column('value', sa.String(100), nullable=True),
        sa.Column('order', sa.Integer(), nullable=True),
        sa.Column('depth', sa.Integer(), nullable=True),
        sa.Column('is_optional', sa.Boolean(), nullable=True),
        sa.Column('has_variants', sa.Boolean(), nullable=True),
        sa.ForeignKeyConstraint(['construction_id'], ['construction.id']),
        sa.ForeignKeyConstraint(['construction_variant_id'],
                                ['construction_variant.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('formula_id'),
    )

    _create_table(
        'constraint',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('change_id', sa.Integer(), nullable=True),
        sa.Column('construction_id', sa.Integer(), nullable=True),
        sa.Column('element', sa.String(30), nullable=True),
        sa.Column('syntactic', sa.String(500), nullable=True),
        sa.Column('semantic', sa.String(500), nullable=True),
        sa.ForeignKeyConstraint(['change_id'], ['change.id']),
        sa.ForeignKeyConstraint(['construction_id'], ['construction.id']),
        sa.PrimaryKeyConstraint('id'),
    )

    _create_table(
        'change_to_previous_changes',
        sa.Column('change_id', sa.Integer(), nullable=False),
        sa.Column('previous_change_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['change_id'], ['change.id']),
        sa.ForeignKeyConstraint(['previous_change_id'], ['change.id']),
        sa.PrimaryKeyConstraint('change_id', 'previous_change_id'),
    )

    _create_table(
        'construction_to_tags',
        sa.Column('construction_id', sa.Integer(), nullable=False),
        sa.Column('tag_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['construction_id'], ['construction.id']),
        sa.ForeignKeyConstraint(['tag_id'], ['tag.id']),
        sa.PrimaryKeyConstraint('construction_id', 'tag_id'),
    )

    _create_table(
        'change_to_tags',
        sa.Column('change_id', sa.Integer(), nullable=False),
        sa.Column('tag_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['change_id'], ['change.id']),
        sa.ForeignKeyConstraint(['tag_id'], ['tag.id']),
        sa.PrimaryKeyConstraint('change_id', 'tag_id'),
    )

    # ------------------------------------------------------------------
    # Auth / annotation / vector search tables
    # ------------------------------------------------------------------
    _create_table(
        'user',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('username', sa.String(64), nullable=False),
        sa.Column('email', sa.String(120), nullable=False),
        sa.Column('password_hash', sa.String(256), nullable=False),
        sa.Column('role', sa.Enum('admin', 'annotator', name='user_role'),
                  nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False,
                  server_default='1'),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('username'),
        sa.UniqueConstraint('email'),
    )
    _create_index('ix_user_username', 'user', ['username'], unique=True)
    _create_index('ix_user_email', 'user', ['email'], unique=True)

    _create_table(
        'annotation_draft',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('construction_id', sa.Integer(), nullable=True),
        sa.Column('annotator_id', sa.Integer(), nullable=False),
        sa.Column('form_data', sa.Text(), nullable=False, server_default='{}'),
        sa.Column('llm_provider', sa.String(50), nullable=True),
        sa.Column('llm_model', sa.String(100), nullable=True),
        sa.Column('status',
                  sa.Enum('draft', 'submitted', 'published',
                           name='draft_status'),
                  nullable=False, server_default='draft'),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['annotator_id'], ['user.id']),
        sa.ForeignKeyConstraint(['construction_id'], ['construction.id']),
        sa.PrimaryKeyConstraint('id'),
    )

    _create_table(
        'construction_embedding',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('construction_id', sa.Integer(), nullable=False),
        sa.Column('field_name', sa.String(64), nullable=False),
        sa.Column('embedding', sa.Text(), nullable=False),
        sa.Column('embedding_model', sa.String(100), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['construction_id'], ['construction.id']),
        sa.PrimaryKeyConstraint('id'),
    )

    # ------------------------------------------------------------------
    # Drop stale phrase table from the old unrelated migration
    # ------------------------------------------------------------------
    if 'phrase' in _get_tables(conn):
        op.drop_table('phrase')


# ---------------------------------------------------------------------------
# downgrade
# ---------------------------------------------------------------------------

def downgrade() -> None:
    conn = op.get_bind()
    tables_now = _get_tables(conn)

    for tbl in (
        'construction_embedding', 'annotation_draft',
        'change_to_tags', 'construction_to_tags',
        'change_to_previous_changes', 'constraint', 'formula_element',
        'change', 'construction_variant', 'general_info',
        'construction', 'tag', 'user',
    ):
        if tbl in tables_now:
            op.drop_table(tbl)