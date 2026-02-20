"""Tests for claude_code_to_sqlite.utils."""
import json
import zipfile
import tempfile
from pathlib import Path

import sqlite_utils
import pytest

from claude_code_to_sqlite import utils


# --- Fixtures ---

@pytest.fixture
def tmp_dir(tmp_path):
    return tmp_path


@pytest.fixture
def db(tmp_path):
    return sqlite_utils.Database(tmp_path / "test.db")


def write_jsonl(path, records):
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")


def make_cli_session(tmp_dir, session_id="abc-123", messages=None):
    """Create a minimal CLI-format JSONL session file."""
    if messages is None:
        messages = [
            {
                "type": "summary",
                "summary": "Test session",
                "sessionId": session_id,
            },
            {
                "type": "user",
                "sessionId": session_id,
                "timestamp": "2025-06-15T10:00:00Z",
                "uuid": "msg-001",
                "parentUuid": None,
                "cwd": "/home/user/project",
                "version": "2.0.0",
                "message": {
                    "role": "user",
                    "content": "Hello, can you help me?",
                },
            },
            {
                "type": "assistant",
                "sessionId": session_id,
                "timestamp": "2025-06-15T10:00:05Z",
                "uuid": "msg-002",
                "parentUuid": "msg-001",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "text", "text": "Sure, I can help!"},
                    ],
                    "model": "claude-sonnet-4-5-20250929",
                    "usage": {
                        "input_tokens": 100,
                        "output_tokens": 50,
                        "cache_read_input_tokens": 20,
                    },
                },
            },
        ]
    filepath = tmp_dir / f"{session_id}.jsonl"
    write_jsonl(filepath, messages)
    return filepath


def make_cli_session_with_tools(tmp_dir, session_id="tools-123"):
    """Create a CLI session with tool use."""
    records = [
        {
            "type": "user",
            "sessionId": session_id,
            "timestamp": "2025-08-01T10:00:00Z",
            "uuid": "msg-001",
            "message": {
                "role": "user",
                "content": "Read the file please",
            },
        },
        {
            "type": "assistant",
            "sessionId": session_id,
            "timestamp": "2025-08-01T10:00:05Z",
            "uuid": "msg-002",
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "Let me read that file."},
                    {
                        "type": "tool_use",
                        "id": "call_001",
                        "name": "Read",
                        "input": {"file_path": "/tmp/test.py"},
                    },
                ],
                "model": "claude-sonnet-4-5-20250929",
            },
        },
        {
            "type": "user",
            "sessionId": session_id,
            "timestamp": "2025-08-01T10:00:06Z",
            "uuid": "msg-003",
            "message": {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "call_001",
                        "content": "print('hello')",
                    },
                ],
            },
        },
    ]
    filepath = tmp_dir / f"{session_id}.jsonl"
    write_jsonl(filepath, records)
    return filepath


def make_cli_session_with_thinking(tmp_dir, session_id="think-123"):
    """Create a CLI session with thinking blocks."""
    records = [
        {
            "type": "user",
            "sessionId": session_id,
            "timestamp": "2025-09-01T10:00:00Z",
            "uuid": "msg-001",
            "message": {
                "role": "user",
                "content": "What is 2+2?",
            },
        },
        {
            "type": "assistant",
            "sessionId": session_id,
            "timestamp": "2025-09-01T10:00:05Z",
            "uuid": "msg-002",
            "message": {
                "role": "assistant",
                "content": [
                    {
                        "type": "thinking",
                        "thinking": "The user wants to know 2+2. That's 4.",
                    },
                    {"type": "text", "text": "2+2 = 4"},
                ],
                "model": "claude-opus-4-5-20251101",
            },
        },
    ]
    filepath = tmp_dir / f"{session_id}.jsonl"
    write_jsonl(filepath, records)
    return filepath


def make_browser_session(tmp_dir, session_id="browser-456"):
    """Create a browser-export JSONL session file."""
    records = [
        {
            "type": "metadata",
            "source": "browser_export",
            "original_uuid": session_id,
            "name": "Browser chat about cooking",
            "created_at": "2025-05-01T12:00:00Z",
            "message_count": 2,
        },
        {
            "type": "user",
            "message": {
                "role": "user",
                "content": "How do I make pasta?",
            },
            "timestamp": "2025-05-01T12:00:01Z",
        },
        {
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": "Boil water, add pasta, cook for 8-10 minutes.",
            },
            "timestamp": "2025-05-01T12:00:10Z",
        },
    ]
    filepath = tmp_dir / f"browser-{session_id}.jsonl"
    write_jsonl(filepath, records)
    return filepath


def make_web_export_zip(tmp_dir):
    """Create a mock claude.ai web export ZIP."""
    conversations = [
        {
            "uuid": "web-conv-001",
            "name": "VW Golf troubleshooting",
            "summary": "Helping with car issues",
            "created_at": "2026-01-15T10:00:00Z",
            "updated_at": "2026-01-15T10:30:00Z",
            "account": {},
            "chat_messages": [
                {
                    "uuid": "wmsg-001",
                    "sender": "human",
                    "text": "My car won't start",
                    "content": [],
                    "created_at": "2026-01-15T10:00:01Z",
                    "updated_at": "2026-01-15T10:00:01Z",
                    "attachments": [],
                    "files": [],
                },
                {
                    "uuid": "wmsg-002",
                    "sender": "assistant",
                    "text": "Let me help you diagnose the issue.",
                    "content": [
                        {
                            "type": "thinking",
                            "thinking": "User has a car that won't start.",
                        },
                        {
                            "type": "text",
                            "text": "Let me help you diagnose the issue.",
                        },
                    ],
                    "created_at": "2026-01-15T10:00:10Z",
                    "updated_at": "2026-01-15T10:00:10Z",
                    "attachments": [],
                    "files": [],
                },
            ],
        },
        {
            "uuid": "web-conv-002",
            "name": "Python help",
            "summary": "",
            "created_at": "2026-01-16T14:00:00Z",
            "updated_at": "2026-01-16T14:05:00Z",
            "account": {},
            "chat_messages": [
                {
                    "uuid": "wmsg-003",
                    "sender": "human",
                    "text": "How do I read a file in Python?",
                    "content": [],
                    "created_at": "2026-01-16T14:00:01Z",
                    "updated_at": "2026-01-16T14:00:01Z",
                    "attachments": [],
                    "files": [],
                },
                {
                    "uuid": "wmsg-004",
                    "sender": "assistant",
                    "text": "Use open() with a context manager.",
                    "content": [],
                    "created_at": "2026-01-16T14:00:05Z",
                    "updated_at": "2026-01-16T14:00:05Z",
                    "attachments": [],
                    "files": [],
                },
            ],
        },
    ]
    zip_path = tmp_dir / "export.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("conversations.json", json.dumps(conversations))
        zf.writestr("users.json", json.dumps({"uuid": "user-1"}))
    return zip_path


# --- Tests: File filtering ---

class TestFileFiltering:
    def test_skip_agent_files(self, tmp_dir):
        (tmp_dir / "agent-abc123.jsonl").touch()
        (tmp_dir / "regular-session.jsonl").touch()
        files = utils.collect_session_files(tmp_dir)
        assert len(files) == 1
        assert files[0].name == "regular-session.jsonl"

    def test_include_agents_flag(self, tmp_dir):
        (tmp_dir / "agent-abc123.jsonl").touch()
        (tmp_dir / "regular-session.jsonl").touch()
        files = utils.collect_session_files(tmp_dir, include_agents=True)
        assert len(files) == 2

    def test_skip_backup_files(self, tmp_dir):
        (tmp_dir / "session.jsonl.backup").touch()
        (tmp_dir / "session.jsonl").touch()
        files = utils.collect_session_files(tmp_dir)
        assert len(files) == 1
        assert files[0].name == "session.jsonl"

    def test_skip_copy_files(self, tmp_dir):
        (tmp_dir / "session copy.jsonl").touch()
        (tmp_dir / "session.jsonl").touch()
        files = utils.collect_session_files(tmp_dir)
        assert len(files) == 1

    def test_skip_subagent_directories(self, tmp_dir):
        subdir = tmp_dir / "subagents"
        subdir.mkdir()
        (subdir / "agent-abc.jsonl").touch()
        (tmp_dir / "session.jsonl").touch()
        files = utils.collect_session_files(tmp_dir, include_agents=True)
        assert len(files) == 1

    def test_collects_json_and_jsonl(self, tmp_dir):
        (tmp_dir / "session.jsonl").touch()
        (tmp_dir / "session.json").touch()
        files = utils.collect_session_files(tmp_dir)
        assert len(files) == 2

    def test_skip_metadata_files(self, tmp_dir):
        """sessions-index.json and timeline.json are metadata, not sessions."""
        (tmp_dir / "sessions-index.json").touch()
        subdir = tmp_dir / ".timelines" / "abc"
        subdir.mkdir(parents=True)
        (subdir / "timeline.json").touch()
        (tmp_dir / "real-session.jsonl").touch()
        files = utils.collect_session_files(tmp_dir)
        assert len(files) == 1
        assert files[0].name == "real-session.jsonl"


# --- Tests: Content extraction ---

class TestContentExtraction:
    def test_extract_text_from_string(self):
        assert utils.extract_text("hello world") == "hello world"

    def test_extract_text_from_blocks(self):
        blocks = [
            {"type": "text", "text": "Hello"},
            {"type": "text", "text": "World"},
        ]
        result = utils.extract_text(blocks)
        assert "Hello" in result
        assert "World" in result

    def test_extract_text_skips_thinking(self):
        blocks = [
            {"type": "thinking", "thinking": "secret thoughts"},
            {"type": "text", "text": "visible text"},
        ]
        result = utils.extract_text(blocks)
        assert "secret thoughts" not in result
        assert "visible text" in result

    def test_extract_text_handles_tool_use(self):
        blocks = [
            {"type": "tool_use", "name": "Bash", "input": {"command": "ls"}},
        ]
        result = utils.extract_text(blocks)
        assert "Bash" in result

    def test_extract_thinking(self):
        blocks = [
            {"type": "thinking", "thinking": "Let me think..."},
            {"type": "text", "text": "Here's my answer"},
        ]
        result = utils.extract_thinking(blocks)
        assert result == "Let me think..."

    def test_extract_thinking_none_for_string(self):
        assert utils.extract_thinking("just a string") is None

    def test_extract_thinking_none_when_absent(self):
        blocks = [{"type": "text", "text": "no thinking here"}]
        assert utils.extract_thinking(blocks) is None

    def test_extract_tool_calls(self):
        blocks = [
            {"type": "text", "text": "Let me read that."},
            {"type": "tool_use", "id": "c1", "name": "Read", "input": {"path": "/tmp"}},
            {"type": "tool_use", "id": "c2", "name": "Bash", "input": {"cmd": "ls"}},
        ]
        calls = utils.extract_tool_calls(blocks)
        assert len(calls) == 2
        assert calls[0]["name"] == "Read"
        assert calls[1]["name"] == "Bash"

    def test_replace_base64_content(self):
        long_b64 = "data:image/png;base64," + "A" * 2000
        result = utils.replace_base64_content(long_b64)
        assert "base64 content" in result
        assert "A" * 100 not in result

    def test_replace_base64_preserves_short_strings(self):
        assert utils.replace_base64_content("hello") == "hello"

    def test_extract_text_from_none(self):
        assert utils.extract_text(None) == ""


# --- Tests: CLI session processing ---

class TestCLISessionProcessing:
    def test_basic_session(self, tmp_dir):
        filepath = make_cli_session(tmp_dir)
        session, messages = utils.process_session(filepath, project="/test")
        assert session is not None
        assert session["session_id"] == "abc-123"
        assert session["summary"] == "Test session"
        assert session["project"] == "/test"
        assert session["source"] == "cli"
        assert session["cwd"] == "/home/user/project"
        assert session["client_version"] == "2.0.0"
        assert len(messages) == 2

    def test_message_roles(self, tmp_dir):
        filepath = make_cli_session(tmp_dir)
        _, messages = utils.process_session(filepath)
        assert messages[0]["role"] == "user"
        assert messages[1]["role"] == "assistant"

    def test_message_content(self, tmp_dir):
        filepath = make_cli_session(tmp_dir)
        _, messages = utils.process_session(filepath)
        assert "Hello" in messages[0]["content"]
        assert "Sure" in messages[1]["content"]

    def test_token_counting(self, tmp_dir):
        filepath = make_cli_session(tmp_dir)
        session, _ = utils.process_session(filepath)
        assert session["total_input_tokens"] == 100
        assert session["total_output_tokens"] == 50
        assert session["total_tokens"] == 150

    def test_model_tracking(self, tmp_dir):
        filepath = make_cli_session(tmp_dir)
        session, _ = utils.process_session(filepath)
        models = json.loads(session["models"])
        assert "claude-sonnet-4-5-20250929" in models

    def test_timestamps(self, tmp_dir):
        filepath = make_cli_session(tmp_dir)
        session, _ = utils.process_session(filepath)
        assert session["start_time"] == "2025-06-15T10:00:00Z"
        assert session["end_time"] == "2025-06-15T10:00:05Z"

    def test_message_counts(self, tmp_dir):
        filepath = make_cli_session(tmp_dir)
        session, _ = utils.process_session(filepath)
        assert session["message_count"] == 2
        assert session["user_message_count"] == 1
        assert session["assistant_message_count"] == 1

    def test_tool_calls(self, tmp_dir):
        filepath = make_cli_session_with_tools(tmp_dir)
        _, messages = utils.process_session(filepath)
        assistant_msg = messages[1]
        assert assistant_msg["tool_names"] is not None
        tool_names = json.loads(assistant_msg["tool_names"])
        assert "Read" in tool_names

    def test_tool_results(self, tmp_dir):
        filepath = make_cli_session_with_tools(tmp_dir)
        _, messages = utils.process_session(filepath)
        tool_result_msg = messages[2]
        assert tool_result_msg["is_tool_result"] is True
        assert tool_result_msg["tool_use_id"] == "call_001"

    def test_thinking_blocks(self, tmp_dir):
        filepath = make_cli_session_with_thinking(tmp_dir)
        _, messages = utils.process_session(filepath)
        assistant_msg = messages[1]
        assert assistant_msg["thinking"] is not None
        assert "2+2" in assistant_msg["thinking"]
        assert "2+2 = 4" in assistant_msg["content"]

    def test_file_history_snapshots_skipped(self, tmp_dir):
        records = [
            {
                "type": "file-history-snapshot",
                "messageId": "snap-1",
                "snapshot": {"trackedFileBackups": {}, "timestamp": "2025-06-15T10:00:00Z"},
            },
            {
                "type": "user",
                "sessionId": "snap-test",
                "timestamp": "2025-06-15T10:00:01Z",
                "message": {"role": "user", "content": "hello"},
            },
        ]
        filepath = tmp_dir / "snap-test.jsonl"
        write_jsonl(filepath, records)
        _, messages = utils.process_session(filepath)
        assert len(messages) == 1
        assert messages[0]["role"] == "user"

    def test_empty_file(self, tmp_dir):
        filepath = tmp_dir / "empty.jsonl"
        filepath.write_text("")
        session, messages = utils.process_session(filepath)
        assert session is None
        assert messages == []

    def test_corrupted_line_recovery(self, tmp_dir):
        filepath = tmp_dir / "corrupt.jsonl"
        good = json.dumps({"type": "user", "sessionId": "c1", "timestamp": "2025-01-01T00:00:00Z", "message": {"role": "user", "content": "hello"}})
        bad = good.rstrip("}") + json.dumps({"type": "assistant", "sessionId": "c1", "timestamp": "2025-01-01T00:00:01Z", "message": {"role": "assistant", "content": "hi"}})
        filepath.write_text(good + "\n" + bad + "\n")
        session, messages = utils.process_session(filepath)
        assert session is not None
        assert len(messages) >= 1


# --- Tests: Browser export processing ---

class TestBrowserExportProcessing:
    def test_browser_session(self, tmp_dir):
        filepath = make_browser_session(tmp_dir)
        session, messages = utils.process_session(filepath)
        assert session is not None
        assert session["session_id"] == "browser-456"
        assert session["source"] == "browser"
        assert session["custom_title"] == "Browser chat about cooking"

    def test_browser_messages(self, tmp_dir):
        filepath = make_browser_session(tmp_dir)
        _, messages = utils.process_session(filepath)
        assert len(messages) == 2
        assert messages[0]["role"] == "user"
        assert "pasta" in messages[0]["content"]
        assert messages[1]["role"] == "assistant"
        assert "Boil" in messages[1]["content"]

    def test_browser_string_content(self, tmp_dir):
        """Browser exports use string content, not content blocks."""
        filepath = make_browser_session(tmp_dir)
        _, messages = utils.process_session(filepath)
        assert isinstance(messages[1]["content"], str)


# --- Tests: Web export processing ---

class TestWebExportProcessing:
    def test_load_web_export(self, tmp_dir):
        zip_path = make_web_export_zip(tmp_dir)
        conversations = utils.load_web_export(zip_path)
        assert len(conversations) == 2

    def test_process_web_conversation(self, tmp_dir):
        zip_path = make_web_export_zip(tmp_dir)
        conversations = utils.load_web_export(zip_path)
        session, messages = utils.process_web_conversation(conversations[0])
        assert session is not None
        assert session["session_id"] == "web-conv-001"
        assert session["source"] == "web"
        assert session["custom_title"] == "VW Golf troubleshooting"

    def test_web_role_mapping(self, tmp_dir):
        zip_path = make_web_export_zip(tmp_dir)
        conversations = utils.load_web_export(zip_path)
        _, messages = utils.process_web_conversation(conversations[0])
        assert messages[0]["role"] == "user"
        assert messages[1]["role"] == "assistant"

    def test_web_thinking_extraction(self, tmp_dir):
        zip_path = make_web_export_zip(tmp_dir)
        conversations = utils.load_web_export(zip_path)
        _, messages = utils.process_web_conversation(conversations[0])
        # Second message has thinking blocks in content
        assert messages[1]["thinking"] is not None
        assert "car" in messages[1]["thinking"]

    def test_web_text_fallback(self, tmp_dir):
        """When content is empty, should fall back to text field."""
        zip_path = make_web_export_zip(tmp_dir)
        conversations = utils.load_web_export(zip_path)
        _, messages = utils.process_web_conversation(conversations[1])
        # Second conversation has empty content arrays
        assert "file" in messages[0]["content"].lower() or "Python" in messages[0]["content"]

    def test_web_empty_conversation(self):
        conv = {"uuid": "empty", "name": "Empty", "chat_messages": []}
        session, messages = utils.process_web_conversation(conv)
        assert session is None
        assert messages == []


# --- Tests: SQLite insertion ---

class TestSQLiteInsertion:
    def test_save_and_retrieve_session(self, db, tmp_dir):
        filepath = make_cli_session(tmp_dir)
        session, messages = utils.process_session(filepath)
        utils.save_session(db, session, messages)
        rows = list(db["sessions"].rows)
        assert len(rows) == 1
        assert rows[0]["session_id"] == "abc-123"

    def test_save_and_retrieve_messages(self, db, tmp_dir):
        filepath = make_cli_session(tmp_dir)
        session, messages = utils.process_session(filepath)
        utils.save_session(db, session, messages)
        rows = list(db["messages"].rows)
        assert len(rows) == 2

    def test_idempotent_reimport(self, db, tmp_dir):
        filepath = make_cli_session(tmp_dir)
        session, messages = utils.process_session(filepath)
        utils.save_session(db, session, messages)
        utils.save_session(db, session, messages)
        assert db["sessions"].count == 1
        assert db["messages"].count == 2

    def test_multiple_sessions(self, db, tmp_dir):
        f1 = make_cli_session(tmp_dir, session_id="sess-1")
        f2 = make_cli_session(tmp_dir, session_id="sess-2")
        s1, m1 = utils.process_session(f1)
        s2, m2 = utils.process_session(f2)
        utils.save_session(db, s1, m1)
        utils.save_session(db, s2, m2)
        assert db["sessions"].count == 2
        assert db["messages"].count == 4

    def test_mixed_sources(self, db, tmp_dir):
        cli_file = make_cli_session(tmp_dir, session_id="cli-1")
        browser_file = make_browser_session(tmp_dir, session_id="browser-1")
        s1, m1 = utils.process_session(cli_file)
        s2, m2 = utils.process_session(browser_file)
        utils.save_session(db, s1, m1)
        utils.save_session(db, s2, m2)
        sources = {row["source"] for row in db["sessions"].rows}
        assert sources == {"cli", "browser"}

    def test_ensure_db_shape_creates_indexes(self, db, tmp_dir):
        filepath = make_cli_session(tmp_dir)
        session, messages = utils.process_session(filepath)
        utils.save_session(db, session, messages)
        utils.ensure_db_shape(db)
        msg_indexes = [idx.columns for idx in db["messages"].indexes]
        assert ["session_id"] in msg_indexes
        assert ["role"] in msg_indexes

    def test_ensure_db_shape_creates_fts(self, db, tmp_dir):
        filepath = make_cli_session(tmp_dir)
        session, messages = utils.process_session(filepath)
        utils.save_session(db, session, messages)
        utils.ensure_db_shape(db)
        assert "messages_fts" in db.table_names()

    def test_ensure_db_shape_creates_views(self, db, tmp_dir):
        filepath = make_cli_session(tmp_dir)
        session, messages = utils.process_session(filepath)
        utils.save_session(db, session, messages)
        utils.ensure_db_shape(db)
        view_names = db.view_names()
        assert "tool_usage" in view_names
        assert "sessions_overview" in view_names
        assert "projects_summary" in view_names
        assert "daily_activity" in view_names
        assert "model_usage" in view_names

    def test_fts_search(self, db, tmp_dir):
        filepath = make_cli_session(tmp_dir)
        session, messages = utils.process_session(filepath)
        utils.save_session(db, session, messages)
        utils.ensure_db_shape(db)
        results = list(
            db.execute(
                "SELECT content FROM messages WHERE rowid IN "
                "(SELECT rowid FROM messages_fts WHERE messages_fts MATCH 'help')"
            ).fetchall()
        )
        assert len(results) >= 1


# --- Tests: Edge cases ---

class TestEdgeCases:
    def test_session_with_no_messages(self, tmp_dir):
        records = [{"type": "summary", "summary": "Empty session", "sessionId": "empty-1"}]
        filepath = tmp_dir / "empty-1.jsonl"
        write_jsonl(filepath, records)
        session, messages = utils.process_session(filepath)
        # Summary-only sessions have no session_id from records
        # The stem is used as fallback
        assert messages == []

    def test_base64_in_user_content(self, tmp_dir):
        records = [
            {
                "type": "user",
                "sessionId": "b64-test",
                "timestamp": "2025-06-15T10:00:00Z",
                "message": {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Check this image"},
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/png",
                                "data": "A" * 5000,
                            },
                        },
                    ],
                },
            },
        ]
        filepath = tmp_dir / "b64-test.jsonl"
        write_jsonl(filepath, records)
        _, messages = utils.process_session(filepath)
        assert "AAAA" not in messages[0]["content"]
        assert "image/png" in messages[0]["content"]

    def test_dir_to_project_absolute(self):
        assert utils.dir_to_project("-home-louthenw-apps") == "/home/louthenw/apps"
        assert utils.dir_to_project("-etc-containers") == "/etc/containers"

    def test_dir_to_project_relative(self):
        """Relative directory names (no leading dash) are left unchanged."""
        assert utils.dir_to_project("my-cool-project") == "my-cool-project"
        assert utils.dir_to_project("projects") == "projects"

    def test_content_truncation(self, tmp_dir):
        """Very long content should be capped at 100K chars."""
        records = [
            {
                "type": "assistant",
                "sessionId": "long-test",
                "timestamp": "2025-06-15T10:00:00Z",
                "message": {
                    "role": "assistant",
                    "content": "x" * 200_000,
                },
            },
        ]
        filepath = tmp_dir / "long-test.jsonl"
        write_jsonl(filepath, records)
        _, messages = utils.process_session(filepath)
        assert len(messages[0]["content"]) == 100_000
