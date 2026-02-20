"""CLI for claude-code-to-sqlite."""
import sys

import click
import sqlite_utils
from pathlib import Path

from claude_code_to_sqlite import utils


DEFAULT_CLAUDE_DIR = Path.home() / ".claude" / "projects"


@click.group()
@click.version_option()
def cli():
    "Save Claude Code session transcripts to a SQLite database"


@cli.command()
@click.argument("db_path", type=click.Path(file_okay=True, dir_okay=False))
@click.argument(
    "session_dir",
    type=click.Path(exists=True),
    required=False,
)
@click.option(
    "--include-agents",
    is_flag=True,
    help="Include agent-* session files",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Only process the first N files",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Parse files but don't write to database",
)
@click.option(
    "--silent",
    is_flag=True,
    help="Suppress progress output",
)
def sessions(db_path, session_dir, include_agents, limit, dry_run, silent):
    """Import Claude Code sessions from a directory.

    Reads JSONL session files from SESSION_DIR (defaults to ~/.claude/projects/)
    and imports them into a SQLite database at DB_PATH.

    \b
    Examples:
        claude-code-to-sqlite sessions claude.db
        claude-code-to-sqlite sessions claude.db ~/my-sessions/
        claude-code-to-sqlite sessions claude.db --include-agents
        claude-code-to-sqlite sessions claude.db --limit=10 --dry-run
    """
    if session_dir is None:
        session_dir = DEFAULT_CLAUDE_DIR
        if not session_dir.exists():
            raise click.ClickException(
                f"Default session directory not found: {session_dir}\n"
                "Please provide a path to your session files."
            )
    session_dir = Path(session_dir)

    # Collect files
    files = utils.collect_session_files(session_dir, include_agents=include_agents)
    if limit:
        files = files[:limit]

    if not files:
        raise click.ClickException(f"No session files found in {session_dir}")

    if not silent:
        click.echo(f"Found {len(files)} session files in {session_dir}")

    if not dry_run:
        db = sqlite_utils.Database(db_path)

    # Process files
    session_count = 0
    message_count = 0
    errors = []

    for i, filepath in enumerate(files):
        # Determine project from parent directory name
        try:
            rel = filepath.relative_to(session_dir)
            project_dir = rel.parts[0] if len(rel.parts) > 1 else "default"
            project = utils.dir_to_project(project_dir)
        except (ValueError, IndexError):
            project = "unknown"

        try:
            session_row, message_rows = utils.process_session(filepath, project)
            if session_row:
                if not dry_run:
                    utils.save_session(db, session_row, message_rows)
                session_count += 1
                message_count += len(message_rows)
            if not silent and (i + 1) % 100 == 0:
                click.echo(f"  processed {i + 1}/{len(files)}...")
        except Exception as e:
            errors.append({"file": str(filepath), "error": str(e)})
            if not silent:
                click.echo(f"  error: {filepath.name}: {e}", err=True)

    if not dry_run:
        if not silent:
            click.echo("Finalizing database schema...")
        utils.ensure_db_shape(db)

    if not silent:
        click.echo(f"\n{session_count} sessions, {message_count:,} messages")
        if errors:
            click.echo(f"{len(errors)} errors", err=True)
        if not dry_run:
            db_size = Path(db_path).stat().st_size / (1024 * 1024)
            click.echo(f"Database: {db_path} ({db_size:.1f} MB)")

    if errors:
        sys.exit(1)


@cli.command()
@click.argument("db_path", type=click.Path(file_okay=True, dir_okay=False))
@click.argument("session_file", type=click.Path(exists=True))
@click.option("--project", default=None, help="Project name for this session")
def session(db_path, session_file, project):
    """Import a single session file.

    \b
    Examples:
        claude-code-to-sqlite session claude.db path/to/session.jsonl
        claude-code-to-sqlite session claude.db session.jsonl --project myapp
    """
    db = sqlite_utils.Database(db_path)
    filepath = Path(session_file)

    session_row, message_rows = utils.process_session(filepath, project)
    if not session_row:
        raise click.ClickException(f"No data found in {session_file}")

    utils.save_session(db, session_row, message_rows)
    utils.ensure_db_shape(db)

    click.echo(
        f"Imported session {session_row['session_id']}: "
        f"{len(message_rows)} messages"
    )


@cli.command(name="web-export")
@click.argument("db_path", type=click.Path(file_okay=True, dir_okay=False))
@click.argument("zip_path", type=click.Path(exists=True))
@click.option(
    "--silent",
    is_flag=True,
    help="Suppress progress output",
)
def web_export(db_path, zip_path, silent):
    """Import conversations from a claude.ai data export ZIP.

    The ZIP file is downloaded from claude.ai Settings > Account > Export Data.
    It contains conversations.json with all your web chat history.

    \b
    Examples:
        claude-code-to-sqlite web-export claude.db data-export.zip
    """
    if not silent:
        click.echo(f"Loading web export from {zip_path}...")

    conversations = utils.load_web_export(zip_path)
    if not conversations:
        raise click.ClickException("No conversations found in export")

    if not silent:
        click.echo(f"Found {len(conversations)} conversations")

    db = sqlite_utils.Database(db_path)
    session_count = 0
    message_count = 0
    errors = []

    for i, conv in enumerate(conversations):
        try:
            session_row, message_rows = utils.process_web_conversation(conv)
            if session_row:
                utils.save_session(db, session_row, message_rows)
                session_count += 1
                message_count += len(message_rows)
        except Exception as e:
            errors.append({"uuid": conv.get("uuid", "?"), "error": str(e)})
            if not silent:
                click.echo(f"  error: {conv.get('name', '?')}: {e}", err=True)

    utils.ensure_db_shape(db)

    if not silent:
        click.echo(f"\n{session_count} conversations, {message_count:,} messages")
        if errors:
            click.echo(f"{len(errors)} errors", err=True)
        db_size = Path(db_path).stat().st_size / (1024 * 1024)
        click.echo(f"Database: {db_path} ({db_size:.1f} MB)")

    if errors:
        sys.exit(1)


@cli.command()
@click.argument("db_path", type=click.Path(exists=True))
def stats(db_path):
    """Show statistics about a Claude Code SQLite database.

    \b
    Examples:
        claude-code-to-sqlite stats claude.db
    """
    db = sqlite_utils.Database(db_path)

    if "sessions" not in db.table_names():
        raise click.ClickException("No sessions table found in database")

    session_count = db.execute("SELECT count(*) FROM sessions").fetchone()[0]
    message_count = db.execute("SELECT count(*) FROM messages").fetchone()[0]
    total_tokens = db.execute(
        "SELECT coalesce(sum(total_tokens), 0) FROM sessions"
    ).fetchone()[0]

    click.echo(f"Sessions:  {session_count:,}")
    click.echo(f"Messages:  {message_count:,}")
    click.echo(f"Tokens:    {total_tokens:,}")

    # Projects
    projects = db.execute(
        "SELECT project, count(*) as c FROM sessions GROUP BY project ORDER BY c DESC LIMIT 10"
    ).fetchall()
    if projects:
        click.echo(f"\nTop projects:")
        for project, count in projects:
            click.echo(f"  {count:>5}  {project}")

    # Models
    models = db.execute(
        "SELECT model, count(*) as c FROM messages WHERE model IS NOT NULL "
        "GROUP BY model ORDER BY c DESC LIMIT 10"
    ).fetchall()
    if models:
        click.echo(f"\nTop models:")
        for model, count in models:
            click.echo(f"  {count:>7}  {model}")

    # Date range
    date_range = db.execute(
        "SELECT min(start_time), max(end_time) FROM sessions "
        "WHERE start_time IS NOT NULL"
    ).fetchone()
    if date_range[0]:
        start = date_range[0][:10] if date_range[0] else "?"
        end = date_range[1][:10] if date_range[1] else "?"
        click.echo(f"\nDate range: {start} to {end}")
