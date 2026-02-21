"Parsing and SQLite insertion logic for Claude Code session transcripts."
import json
import re
import zipfile
from pathlib import Path

# Max uncompressed size we'll load from a ZIP (500 MB)
MAX_ZIP_ENTRY_BYTES = 500 * 1024 * 1024


# --- File filtering ---

SKIP_PATTERNS = [
    re.compile(r"[\\/](?:subagents?)[\\/]"),
    re.compile(r"[\\/]processing[\\/]"),
]


def should_skip_file(filepath, include_agents=False):
    "Return True if this file should not be ingested."
    s = filepath.as_posix()
    for pat in SKIP_PATTERNS:
        if pat.search(s):
            return True
    name = filepath.name
    if not include_agents and name.startswith("agent-"):
        return True
    if ".backup" in name:
        return True
    if " copy" in name or "(1)" in name:
        return True
    # Skip Claude Code metadata files that aren't session transcripts
    if name in ("sessions-index.json", "timeline.json"):
        return True
    # Skip files inside .timelines directories
    if ".timelines" in filepath.as_posix():
        return True
    return False


def collect_session_files(data_path, include_agents=False):
    "Collect JSONL/JSON session files from a directory tree."
    data_path = Path(data_path)
    files = sorted(
        list(data_path.rglob("*.jsonl")) + list(data_path.rglob("*.json"))
    )
    kept = []
    for f in files:
        if not should_skip_file(f, include_agents=include_agents):
            kept.append(f)
    return kept


# --- JSONL parsing with corruption recovery ---

def _extract_records_from_bad_line(line):
    "Try to extract valid JSON records from a corrupted/concatenated line."
    starts = [m.start() for m in re.finditer(r'\{"parentUuid"', line)]
    starts += [m.start() for m in re.finditer(r'\{"type":', line)]
    starts = sorted(set(starts))
    if not starts:
        return []
    valid = []
    for i in range(len(starts)):
        start = starts[i]
        end = starts[i + 1] if i + 1 < len(starts) else len(line)
        segment = line[start:end].strip()
        try:
            valid.append(json.loads(segment))
        except json.JSONDecodeError:
            pass
    return valid


def load_session_file(filepath):
    "Load a session file (JSONL or JSON) and return (records, warnings)."
    filepath = Path(filepath)

    if filepath.suffix == ".json":
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and "loglines" in data:
            return data["loglines"], []
        if isinstance(data, list):
            return data, []
        return [data], []

    # JSONL format (CLI sessions)
    records = []
    warnings = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                extracted = _extract_records_from_bad_line(line)
                if extracted:
                    records.extend(extracted)
                else:
                    warnings.append(
                        f"{filepath.name} line {line_num}: "
                        "unrecoverable JSON parse error"
                    )
    return records, warnings


# --- Content extraction ---

def _base64_decoded_size_kb(encoded_len):
    "Estimate decoded size in KB from base64 encoded character count."
    return (encoded_len * 3 / 4) / 1024


def replace_base64_content(content):
    "Replace base64 image/document data with a size placeholder."
    if isinstance(content, str):
        if "base64" in content[:500] and len(content) > 1000:
            size_kb = _base64_decoded_size_kb(len(content))
            return f"[base64 content, ~{size_kb:.0f}KB decoded]"
        return content
    if isinstance(content, dict):
        source = content.get("source", {})
        if source.get("type") == "base64":
            data = source.get("data", "")
            size_kb = _base64_decoded_size_kb(len(data))
            media = source.get("media_type", "unknown")
            ctype = content.get("type", "file")
            return f"[{ctype}: {media}, ~{size_kb:.0f}KB decoded]"
        return str(content)
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict):
                source = item.get("source", {})
                if source.get("type") == "base64":
                    data = source.get("data", "")
                    size_kb = _base64_decoded_size_kb(len(data))
                    media = source.get("media_type", "unknown")
                    ctype = item.get("type", "file")
                    parts.append(f"[{ctype}: {media}, ~{size_kb:.0f}KB decoded]")
                elif item.get("type") == "text":
                    parts.append(item.get("text", ""))
                else:
                    parts.append(str(item))
            else:
                parts.append(str(item))
        return "\n".join(parts)
    return str(content)


def extract_text(raw_content):
    "Extract readable text from message content (string or content blocks)."
    if raw_content is None:
        return ""
    if isinstance(raw_content, str):
        return raw_content
    if isinstance(raw_content, list):
        parts = []
        for block in raw_content:
            if isinstance(block, dict):
                btype = block.get("type")
                if btype == "text":
                    parts.append(block.get("text", ""))
                elif btype == "thinking":
                    pass  # extracted separately
                elif btype == "tool_use":
                    parts.append(f"[tool_use: {block.get('name', '')}]")
                elif btype == "tool_result":
                    result = block.get("content", "")
                    result = replace_base64_content(result)
                    if isinstance(result, list):
                        result = "\n".join(
                            b.get("text", str(b)) for b in result
                        )
                    parts.append(str(result))
                elif btype in ("image", "document"):
                    parts.append(replace_base64_content(block))
                else:
                    parts.append(str(block))
            else:
                parts.append(str(block))
        return "\n".join(parts)
    return str(raw_content)


def extract_thinking(raw_content):
    "Extract thinking/reasoning text from content blocks."
    if not isinstance(raw_content, list):
        return None
    parts = []
    for block in raw_content:
        if isinstance(block, dict) and block.get("type") == "thinking":
            text = block.get("thinking", "")
            if text:
                parts.append(text)
    return "\n".join(parts) if parts else None


def extract_tool_calls(raw_content):
    "Extract tool call info from assistant content blocks."
    if not isinstance(raw_content, list):
        return []
    calls = []
    for block in raw_content:
        if isinstance(block, dict) and block.get("type") == "tool_use":
            calls.append({
                "id": block.get("id", ""),
                "name": block.get("name", ""),
                "input": block.get("input", {}),
            })
    return calls


def dir_to_project(dirname):
    """Convert encoded directory name back to a path.

    Claude Code encodes absolute paths by replacing / with -
    and stripping the leading /. Hyphens are ambiguous, so this
    is best-effort. Names without a leading - are returned unchanged.
    """
    if dirname.startswith("-"):
        return "/" + dirname[1:].replace("-", "/")
    return dirname


# --- Session processing ---

def process_session(filepath, project=None):
    "Process a single session file into (session_dict, message_dicts, warnings)."
    filepath = Path(filepath)
    records, warnings = load_session_file(filepath)
    if not records:
        return None, [], warnings

    session_id = None
    summary = None
    custom_title = None
    models = set()
    total_input_tokens = 0
    total_output_tokens = 0
    timestamps = []
    cwd = None
    client_version = None
    permission_mode = None
    message_rows = []
    msg_index = 0
    source = "cli"

    for record in records:
        rtype = record.get("type")

        # Detect browser export format (has metadata record with source field)
        if rtype == "metadata" and record.get("source") == "browser_export":
            source = "browser"
            session_id = record.get("original_uuid", filepath.stem)
            custom_title = record.get("name")
            continue

        # Session-level metadata
        if not session_id:
            session_id = record.get("sessionId", filepath.stem)
        if not cwd:
            cwd = record.get("cwd")
        if not client_version:
            client_version = record.get("version")
        if not permission_mode and record.get("permissionMode"):
            permission_mode = record["permissionMode"]

        ts = record.get("timestamp")
        if ts:
            timestamps.append(ts)

        if rtype == "summary":
            summary = record.get("summary", "")
            continue

        if rtype == "custom-title":
            custom_title = record.get("customTitle", "")
            continue

        # Skip bulky snapshots from messages table
        if rtype == "file-history-snapshot":
            continue

        msg = record.get("message", {})
        role = msg.get("role") or rtype or "unknown"
        raw_content = msg.get("content") or record.get("content", "")
        model = msg.get("model")

        if model:
            models.add(model)

        usage = msg.get("usage", {})
        input_tokens = usage.get("input_tokens", 0)
        output_tokens = usage.get("output_tokens", 0)
        cache_read = usage.get("cache_read_input_tokens", 0)
        cache_create = usage.get("cache_creation_input_tokens", 0)
        total_input_tokens += input_tokens
        total_output_tokens += output_tokens

        content_text = extract_text(raw_content)
        thinking = extract_thinking(raw_content) if role == "assistant" else None

        tool_calls = extract_tool_calls(raw_content)
        tool_names = [tc["name"] for tc in tool_calls] if tool_calls else None

        tool_use_id = None
        is_tool_result = False
        if isinstance(raw_content, list):
            for block in raw_content:
                if isinstance(block, dict) and block.get("type") == "tool_result":
                    tool_use_id = block.get("tool_use_id")
                    is_tool_result = True
                    break

        message_rows.append({
            "session_id": session_id,
            "message_index": msg_index,
            "role": role,
            "content": content_text[:100_000] if content_text else "",
            "thinking": thinking[:100_000] if thinking else None,
            "timestamp": ts,
            "uuid": record.get("uuid"),
            "parent_uuid": record.get("parentUuid"),
            "model": model,
            "record_type": rtype,
            "tool_names": json.dumps(tool_names) if tool_names else None,
            "tool_use_id": tool_use_id,
            "is_tool_result": is_tool_result,
            "is_sidechain": record.get("isSidechain", False),
            "input_tokens": input_tokens or None,
            "output_tokens": output_tokens or None,
            "cache_read_tokens": cache_read or None,
            "cache_create_tokens": cache_create or None,
            "stop_reason": record.get("stopReason") or msg.get("stop_reason"),
            "duration_ms": record.get("durationMs"),
        })
        msg_index += 1

    if not session_id:
        return None, [], warnings

    session_row = {
        "session_id": session_id,
        "project": project,
        "cwd": cwd,
        "client_version": client_version,
        "permission_mode": permission_mode,
        "models": json.dumps(sorted(models)) if models else None,
        "summary": summary,
        "custom_title": custom_title,
        "start_time": min(timestamps) if timestamps else None,
        "end_time": max(timestamps) if timestamps else None,
        "message_count": len(message_rows),
        "user_message_count": sum(1 for m in message_rows if m["role"] == "user"),
        "assistant_message_count": sum(1 for m in message_rows if m["role"] == "assistant"),
        "total_input_tokens": total_input_tokens,
        "total_output_tokens": total_output_tokens,
        "total_tokens": total_input_tokens + total_output_tokens,
        "file_path": str(filepath),
        "file_size_bytes": filepath.stat().st_size,
        "source": source,
    }

    return session_row, message_rows, warnings


# --- Web export (claude.ai ZIP) processing ---

def load_web_export(zip_path):
    "Load conversations from a claude.ai data export ZIP."
    zip_path = Path(zip_path)
    with zipfile.ZipFile(zip_path) as zf:
        # Search for conversations.json anywhere in the archive
        conversations_path = None
        for info in zf.infolist():
            if info.filename.endswith("conversations.json"):
                conversations_path = info
                break

        if conversations_path is None:
            raise ValueError(
                f"No conversations.json found in {zip_path.name}. "
                "Expected a claude.ai data export ZIP."
            )

        # Check uncompressed size before loading
        if conversations_path.file_size > MAX_ZIP_ENTRY_BYTES:
            size_mb = conversations_path.file_size / (1024 * 1024)
            limit_mb = MAX_ZIP_ENTRY_BYTES / (1024 * 1024)
            raise ValueError(
                f"conversations.json is {size_mb:.0f} MB, "
                f"exceeds {limit_mb:.0f} MB limit"
            )

        with zf.open(conversations_path) as f:
            return json.load(f)


def process_web_conversation(conversation, zip_path=None):
    "Process a single conversation from a claude.ai web export."
    session_id = conversation.get("uuid", "")
    name = conversation.get("name", "")
    summary_text = conversation.get("summary", "")
    created_at = conversation.get("created_at")
    updated_at = conversation.get("updated_at")
    chat_messages = conversation.get("chat_messages", [])

    if not chat_messages:
        return None, []

    message_rows = []
    timestamps = []

    for msg_index, msg in enumerate(chat_messages):
        sender = msg.get("sender", "unknown")
        # Map claude.ai roles to standard roles
        role = {"human": "user", "assistant": "assistant"}.get(sender, sender)

        # Content: try structured content blocks first, fall back to text
        raw_content = msg.get("content")
        text_field = msg.get("text", "")

        if isinstance(raw_content, list) and raw_content:
            content_text = extract_text(raw_content)
            thinking = extract_thinking(raw_content)
        else:
            content_text = text_field or ""
            thinking = None

        ts = msg.get("created_at")
        if ts:
            timestamps.append(ts)

        message_rows.append({
            "session_id": session_id,
            "message_index": msg_index,
            "role": role,
            "content": content_text[:100_000] if content_text else "",
            "thinking": thinking[:100_000] if thinking else None,
            "timestamp": ts,
            "uuid": msg.get("uuid"),
            "parent_uuid": None,
            "model": None,
            "record_type": role,
            "tool_names": None,
            "tool_use_id": None,
            "is_tool_result": False,
            "is_sidechain": False,
            "input_tokens": None,
            "output_tokens": None,
            "cache_read_tokens": None,
            "cache_create_tokens": None,
            "stop_reason": None,
            "duration_ms": None,
        })

    session_row = {
        "session_id": session_id,
        "project": None,
        "cwd": None,
        "client_version": None,
        "permission_mode": None,
        "models": None,
        "summary": summary_text or None,
        "custom_title": name or None,
        "start_time": min(timestamps) if timestamps else created_at,
        "end_time": max(timestamps) if timestamps else updated_at,
        "message_count": len(message_rows),
        "user_message_count": sum(1 for m in message_rows if m["role"] == "user"),
        "assistant_message_count": sum(1 for m in message_rows if m["role"] == "assistant"),
        "total_input_tokens": 0,
        "total_output_tokens": 0,
        "total_tokens": 0,
        "file_path": str(zip_path) if zip_path else None,
        "file_size_bytes": Path(zip_path).stat().st_size if zip_path else None,
        "source": "web",
    }

    return session_row, message_rows


# --- SQLite insertion ---

SESSIONS_COLUMNS = {
    "session_id": str,
    "project": str,
    "cwd": str,
    "client_version": str,
    "permission_mode": str,
    "models": str,
    "summary": str,
    "custom_title": str,
    "start_time": str,
    "end_time": str,
    "message_count": int,
    "user_message_count": int,
    "assistant_message_count": int,
    "total_input_tokens": int,
    "total_output_tokens": int,
    "total_tokens": int,
    "file_path": str,
    "file_size_bytes": int,
    "source": str,
}

MESSAGES_COLUMNS = {
    "session_id": str,
    "message_index": int,
    "role": str,
    "content": str,
    "thinking": str,
    "timestamp": str,
    "uuid": str,
    "parent_uuid": str,
    "model": str,
    "record_type": str,
    "tool_names": str,
    "tool_use_id": str,
    "is_tool_result": int,
    "is_sidechain": int,
    "input_tokens": int,
    "output_tokens": int,
    "cache_read_tokens": int,
    "cache_create_tokens": int,
    "stop_reason": str,
    "duration_ms": int,
}


def save_session(db, session_row, message_rows):
    "Save a session and its messages to the database."
    db["sessions"].insert(
        session_row,
        pk="session_id",
        columns=SESSIONS_COLUMNS,
        alter=True,
        replace=True,
    )
    if message_rows:
        db["messages"].insert_all(
            message_rows,
            pk=("session_id", "message_index"),
            foreign_keys=[("session_id", "sessions", "session_id")],
            columns=MESSAGES_COLUMNS,
            alter=True,
            replace=True,
        )
    return session_row["session_id"]


FTS_CONFIG = {
    "messages": {
        "columns": ["content", "thinking"],
        "create_triggers": True,
    },
}

VIEWS = {
    "tool_usage": {
        "requires": ["messages"],
        "sql": """
            SELECT
                json_each.value as tool_name,
                count(*) as uses,
                count(distinct session_id) as sessions
            FROM messages, json_each(messages.tool_names)
            WHERE tool_names IS NOT NULL
            GROUP BY tool_name
            ORDER BY uses DESC
        """,
    },
    "sessions_overview": {
        "requires": ["sessions"],
        "sql": """
            SELECT
                session_id,
                project,
                coalesce(custom_title, summary, session_id) as title,
                start_time,
                end_time,
                message_count,
                user_message_count,
                assistant_message_count,
                total_input_tokens,
                total_output_tokens,
                total_tokens,
                models,
                client_version
            FROM sessions
            ORDER BY start_time DESC
        """,
    },
    "projects_summary": {
        "requires": ["sessions"],
        "sql": """
            SELECT
                project,
                count(*) as session_count,
                sum(message_count) as total_messages,
                sum(total_tokens) as total_tokens,
                min(start_time) as first_session,
                max(end_time) as last_session
            FROM sessions
            GROUP BY project
            ORDER BY session_count DESC
        """,
    },
    "daily_activity": {
        "requires": ["sessions"],
        "sql": """
            SELECT
                date(start_time) as day,
                count(*) as sessions,
                sum(message_count) as messages,
                sum(total_tokens) as tokens,
                sum(user_message_count) as user_messages,
                sum(assistant_message_count) as assistant_messages
            FROM sessions
            WHERE start_time IS NOT NULL
            GROUP BY date(start_time)
            ORDER BY day DESC
        """,
    },
    "model_usage": {
        "requires": ["messages"],
        "sql": """
            SELECT
                model,
                count(*) as message_count,
                count(distinct session_id) as session_count,
                sum(input_tokens) as total_input_tokens,
                sum(output_tokens) as total_output_tokens
            FROM messages
            WHERE model IS NOT NULL
            GROUP BY model
            ORDER BY message_count DESC
        """,
    },
}


def ensure_db_shape(db):
    "Set up indexes, FTS, and views after all data is inserted."
    table_names = db.table_names()

    if "messages" in table_names:
        for cols in [["session_id"], ["role"], ["timestamp"], ["model"]]:
            db["messages"].create_index(cols, if_not_exists=True)

    if "sessions" in table_names:
        for cols in [["project"], ["start_time"]]:
            db["sessions"].create_index(cols, if_not_exists=True)

    for table_name, fts_conf in FTS_CONFIG.items():
        fts_table = f"{table_name}_fts"
        if table_name in table_names and fts_table not in table_names:
            db[table_name].enable_fts(
                fts_conf["columns"],
                create_triggers=fts_conf.get("create_triggers", True),
            )

    for view_name, view_conf in VIEWS.items():
        if all(t in table_names for t in view_conf["requires"]):
            db.create_view(view_name, view_conf["sql"], replace=True)

    db.index_foreign_keys()
