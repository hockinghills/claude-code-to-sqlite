"CLI for claude-code-to-sqlite."
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
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
)
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
    "Import Claude Code sessions from a directory"
    if session_dir is None:
        session_dir = DEFAULT_CLAUDE_DIR
        if not session_dir.exists():
            raise click.ClickException(
                f"Default session directory not found: {session_dir}\n"
                "Please provide a path to your session files."
            )
    session_dir = Path(session_dir)

    files = utils.collect_session_files(session_dir, include_agents=include_agents)
    if limit is not None:
        files = files[:limit]

    if not files:
        raise click.ClickException(f"No session files found in {session_dir}")

    if not dry_run:
        db = sqlite_utils.Database(db_path)

    session_count = 0
    message_count = 0
    errors = []

    if silent:
        for filepath in files:
            project = _project_from_path(filepath, session_dir)
            try:
                session_row, message_rows, _ = utils.process_session(filepath, project)
                if session_row:
                    if not dry_run:
                        utils.save_session(db, session_row, message_rows)
                    session_count += 1
                    message_count += len(message_rows)
            except Exception as e:
                errors.append({"file": str(filepath), "error": str(e)})
    else:
        all_warnings = []
        with click.progressbar(
            files,
            label=f"Importing {len(files)} sessions",
            show_pos=True,
        ) as bar:
            for filepath in bar:
                project = _project_from_path(filepath, session_dir)
                try:
                    session_row, message_rows, warnings = utils.process_session(filepath, project)
                    all_warnings.extend(warnings)
                    if session_row:
                        if not dry_run:
                            utils.save_session(db, session_row, message_rows)
                        session_count += 1
                        message_count += len(message_rows)
                except Exception as e:
                    errors.append({"file": str(filepath), "error": str(e)})
        for w in all_warnings:
            click.echo(w, err=True)

    if not dry_run:
        utils.ensure_db_shape(db)

    if not silent:
        click.echo(f"{session_count} sessions, {message_count:,} messages")
        if not dry_run:
            db_size = Path(db_path).stat().st_size / (1024 * 1024)
            click.echo(f"Database: {db_path} ({db_size:.1f} MB)")

    if errors:
        raise click.ClickException(f"{len(errors)} files had errors")


@cli.command()
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
)
@click.argument("session_file", type=click.Path(exists=True))
@click.option("--project", default=None, help="Project name for this session")
def session(db_path, session_file, project):
    "Import a single session file"
    db = sqlite_utils.Database(db_path)
    filepath = Path(session_file)

    session_row, message_rows, warnings = utils.process_session(filepath, project)
    for w in warnings:
        click.echo(w, err=True)
    if not session_row:
        raise click.ClickException(f"No data found in {session_file}")

    utils.save_session(db, session_row, message_rows)
    utils.ensure_db_shape(db)

    click.echo(
        f"Imported session {session_row['session_id']}: "
        f"{len(message_rows)} messages"
    )


@cli.command(name="web-export")
@click.argument(
    "db_path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
)
@click.argument("zip_path", type=click.Path(exists=True))
@click.option(
    "--silent",
    is_flag=True,
    help="Suppress progress output",
)
def web_export(db_path, zip_path, silent):
    "Import conversations from a claude.ai data export ZIP"
    conversations = utils.load_web_export(zip_path)
    if not conversations:
        raise click.ClickException("No conversations found in export")

    db = sqlite_utils.Database(db_path)
    session_count = 0
    message_count = 0
    errors = []

    if silent:
        for conv in conversations:
            try:
                session_row, message_rows = utils.process_web_conversation(
                    conv, zip_path=zip_path
                )
                if session_row:
                    utils.save_session(db, session_row, message_rows)
                    session_count += 1
                    message_count += len(message_rows)
            except Exception as e:
                errors.append({"uuid": conv.get("uuid", "?"), "error": str(e)})
    else:
        with click.progressbar(
            conversations,
            label=f"Importing {len(conversations)} conversations",
            show_pos=True,
        ) as bar:
            for conv in bar:
                try:
                    session_row, message_rows = utils.process_web_conversation(
                        conv, zip_path=zip_path
                    )
                    if session_row:
                        utils.save_session(db, session_row, message_rows)
                        session_count += 1
                        message_count += len(message_rows)
                except Exception as e:
                    errors.append({"uuid": conv.get("uuid", "?"), "error": str(e)})

    utils.ensure_db_shape(db)

    if not silent:
        click.echo(f"{session_count} conversations, {message_count:,} messages")
        db_size = Path(db_path).stat().st_size / (1024 * 1024)
        click.echo(f"Database: {db_path} ({db_size:.1f} MB)")

    if errors:
        raise click.ClickException(f"{len(errors)} conversations had errors")


@cli.command()
@click.argument(
    "db_path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, allow_dash=False),
)
def stats(db_path):
    "Show statistics about a Claude Code SQLite database"
    db = sqlite_utils.Database(db_path)

    table_names = db.table_names()
    if "sessions" not in table_names:
        raise click.ClickException("No sessions table found in database")

    has_messages = "messages" in table_names

    session_count = db.execute("SELECT count(*) FROM sessions").fetchone()[0]
    message_count = (
        db.execute("SELECT count(*) FROM messages").fetchone()[0]
        if has_messages else 0
    )
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
        click.echo("\nTop projects:")
        for project, count in projects:
            click.echo(f"  {count:>5}  {project}")

    # Models
    if has_messages:
        models = db.execute(
            "SELECT model, count(*) as c FROM messages WHERE model IS NOT NULL "
            "GROUP BY model ORDER BY c DESC LIMIT 10"
        ).fetchall()
    else:
        models = []
    if models:
        click.echo("\nTop models:")
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


def _project_from_path(filepath, session_dir):
    "Determine project name from a session file's location in the directory tree."
    try:
        rel = filepath.relative_to(session_dir)
        project_dir = rel.parts[0] if len(rel.parts) > 1 else "default"
        return utils.dir_to_project(project_dir)
    except (ValueError, IndexError):
        return "unknown"
