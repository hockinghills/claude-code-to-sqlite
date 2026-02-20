# claude-code-to-sqlite

Save [Claude Code](https://docs.anthropic.com/en/docs/claude-code) session transcripts to a SQLite database for exploration with [Datasette](https://datasette.io/).

Part of the [Dogsheep](https://dogsheep.github.io/) family of tools for building a personal data warehouse.

## Installation

```bash
pip install claude-code-to-sqlite
```

## Usage

### Import Claude Code CLI sessions

Import sessions from your local Claude Code history:

```bash
# Default: reads from ~/.claude/projects/
claude-code-to-sqlite sessions claude.db

# From a specific directory
claude-code-to-sqlite sessions claude.db /path/to/session-files/

# Include subagent session files
claude-code-to-sqlite sessions claude.db --include-agents
```

### Import claude.ai web conversations

Import conversations from a [claude.ai data export](https://support.anthropic.com/en/articles/7996885-how-do-i-export-my-data) ZIP file:

```bash
claude-code-to-sqlite web-export claude.db data-export.zip
```

### Import a single session file

```bash
claude-code-to-sqlite session claude.db path/to/session.jsonl --project myapp
```

### View database statistics

```bash
claude-code-to-sqlite stats claude.db
```

### Explore with Datasette

```bash
datasette claude.db
```

Then open http://localhost:8001 in your browser.

## Supported formats

| Format | Source | Command |
|---|---|---|
| JSONL | Claude Code CLI (`~/.claude/projects/`) | `sessions` |
| JSONL | Pre-split browser exports | `sessions` |
| JSON | claude-code-transcripts style | `sessions` |
| ZIP | claude.ai Settings > Export Data | `web-export` |

All formats can be imported into the same database. A `source` column on the `sessions` table tracks the origin (`cli`, `browser`, or `web`).

## Database schema

### Tables

**`sessions`** — One row per session.

| Column | Description |
|---|---|
| `session_id` | Primary key (UUID) |
| `project` | Working directory / project path |
| `cwd` | Current working directory at session start |
| `client_version` | Claude Code version |
| `models` | JSON array of model IDs used |
| `summary` | Auto-generated session summary |
| `custom_title` | User-set session title |
| `start_time` | First message timestamp |
| `end_time` | Last message timestamp |
| `message_count` | Total messages in session |
| `user_message_count` | User messages |
| `assistant_message_count` | Assistant messages |
| `total_input_tokens` | Sum of input tokens |
| `total_output_tokens` | Sum of output tokens |
| `total_tokens` | Input + output tokens |
| `source` | `cli`, `browser`, or `web` |

**`messages`** — One row per message.

| Column | Description |
|---|---|
| `session_id` | Foreign key to sessions |
| `message_index` | Position within session |
| `role` | `user`, `assistant`, `system`, etc. |
| `content` | Message text content |
| `thinking` | Extended thinking / reasoning text |
| `timestamp` | Message timestamp |
| `model` | Model ID for this response |
| `record_type` | Original record type |
| `tool_names` | JSON array of tools called |
| `tool_use_id` | Tool call ID for tool results |
| `is_tool_result` | Whether this is a tool response |
| `input_tokens` | Input tokens for this turn |
| `output_tokens` | Output tokens for this turn |
| `cache_read_tokens` | Prompt cache read tokens |
| `cache_create_tokens` | Prompt cache creation tokens |
| `stop_reason` | Why the model stopped |
| `duration_ms` | Response time in milliseconds |

### Full-text search

The `messages_fts` table provides full-text search across `content` and `thinking` columns:

```sql
SELECT * FROM messages_fts WHERE messages_fts MATCH 'datasette'
```

### Views

| View | Description |
|---|---|
| `sessions_overview` | Sessions with title, dates, token counts |
| `projects_summary` | Aggregate stats per project |
| `daily_activity` | Sessions, messages, and tokens per day |
| `tool_usage` | Tool call counts across all sessions |
| `model_usage` | Message and token counts per model |

## Example queries

```sql
-- Most active projects
SELECT project, count(*) as sessions, sum(total_tokens) as tokens
FROM sessions GROUP BY project ORDER BY sessions DESC

-- Tool usage breakdown
SELECT * FROM tool_usage LIMIT 20

-- Search for anything you've discussed
SELECT session_id, role, content
FROM messages
WHERE rowid IN (
    SELECT rowid FROM messages_fts WHERE messages_fts MATCH 'authentication'
)
LIMIT 20

-- Daily token burn
SELECT * FROM daily_activity LIMIT 30

-- Biggest sessions by token usage
SELECT session_id, summary, total_tokens, message_count
FROM sessions ORDER BY total_tokens DESC LIMIT 10

-- Model usage over time
SELECT date(timestamp) as day, model, count(*) as messages
FROM messages WHERE model IS NOT NULL
GROUP BY day, model ORDER BY day DESC
```

## Features

- **Idempotent imports**: Re-running the same import updates existing records without creating duplicates
- **Corrupted line recovery**: Handles concatenated/malformed JSONL lines common in older session files
- **Base64 stripping**: Replaces inline images and documents with size placeholders to keep the database manageable
- **Content truncation**: Caps individual messages at 100K characters
- **Schema evolution**: Uses `alter=True` so new fields in future Claude Code versions are automatically added

## Development

```bash
git clone https://github.com/hockinghills/claude-code-to-sqlite
cd claude-code-to-sqlite
pip install -e ".[test]"
pytest
```
