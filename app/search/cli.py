"""app/search/cli.py

Flask CLI command group for managing the semantic search index.

Usage
-----
    flask embeddings rebuild          # embed all published constructions
    flask embeddings rebuild --all    # include drafts/unpublished
    flask embeddings status           # show index coverage
"""
import click
from flask import current_app
from flask.cli import AppGroup

embeddings_cli = AppGroup(
    'embeddings',
    help='Manage the semantic search embedding index.',
)


@embeddings_cli.command('rebuild')
@click.option(
    '--all', 'include_all', is_flag=True, default=False,
    help='Include unpublished / draft constructions.',
)
def rebuild(include_all: bool):
    """Compute and store embeddings for all constructions.

    Re-running this command is safe — existing embeddings are replaced.
    """
    from app.search.semantic import build_index

    published_only = not include_all
    scope = "all constructions" if include_all else "published constructions only"
    click.echo(f"Building embedding index ({scope})…")

    db_session = current_app.db_session
    try:
        n = build_index(db_session, published_only=published_only, verbose=True)
        click.secho(f"✓ Index built: {n} embedding rows written.", fg='green')
    except ImportError as e:
        click.secho(f"✗ {e}", fg='red')
        raise SystemExit(1)
    except Exception as e:
        click.secho(f"✗ Error during indexing: {e}", fg='red')
        db_session.rollback()
        raise


@embeddings_cli.command('status')
def status():
    """Show how many constructions have embeddings vs total."""
    from sqlalchemy import select, func
    from app.models import Construction, ConstructionEmbedding

    db_session = current_app.db_session

    total = db_session.query(Construction).count()
    published = (
        db_session.query(Construction)
        .filter(Construction.is_published.is_(True))
        .count()
    )
    embedded = (
        db_session.query(ConstructionEmbedding.construction_id.distinct())
        .count()
    )

    click.echo(f"Constructions total:     {total}")
    click.echo(f"  published:             {published}")
    click.echo(f"  with embeddings:       {embedded}")

    if embedded == 0:
        click.secho(
            "Index is empty. Run `flask embeddings rebuild` to build it.",
            fg='yellow',
        )
    elif embedded < published:
        click.secho(
            f"Index is incomplete ({published - embedded} published constructions "
            f"have no embeddings). Run `flask embeddings rebuild`.",
            fg='yellow',
        )
    else:
        click.secho("Index is up to date.", fg='green')