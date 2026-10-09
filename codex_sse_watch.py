#!/usr/bin/env python3
"""只读查看 Codex SSE / WebSocket 响应日志；--web 打开实时表格。

需要 Python 3.11+；默认持续监控，Ctrl+C 退出，--once 查看一次。
仅使用 Python 标准库，不启动、停止或修改 Codex 进程。
"""

import argparse
import collections
from contextlib import closing
import datetime
import errno
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import re
import secrets
import select
import socket
import socketserver
import sqlite3
import sys
import tempfile
import threading
import time
import tomllib
import unicodedata
import webbrowser
import weakref

WINDOW = 8 * 1024 * 1024
CACHE_LIMIT = 20_000
RESPONSE_LIMIT = 5000
PROGRESS_INTERVAL = 0.25
ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
TERMINAL = {"response.completed", "response.failed", "response.incomplete"}
EVENT_TYPES = TERMINAL | {"response.created", "response.in_progress"}
DESKTOP_MESSAGE = re.compile(r"^(?:\S+\s+\w+\s+\[AppServerConnection\]\s+)?Codex CLI stderr message=")
DESKTOP_RECORD = re.compile(r"^\d{4}-\d{2}-\d{2}T\S+\s+\w+\s+\[[^\]]+\]\s+")
LOG_RECORD_START = re.compile(
    r"^data:\s*|^(?:\d{4}-\d{2}-\d{2}T[^\s\0]+[ \t]+(?:TRACE|DEBUG|INFO|WARN|ERROR)[ \t]+)?"
    r"(?:codex_api::sse(?:::\w+)*:[ \t]*SSE event:\s*|tungstenite::protocol:[ \t]+Received message\s+)",
    re.MULTILINE,
)
JSON_CONTAINER_START = re.compile(r"[\[{]")
JSON_FRAME = re.compile(r'["{}\[\]]')
JSON_DECODER = json.JSONDecoder()
LOG_FILTER = "warn,codex_api=trace,tungstenite::protocol=trace,tungstenite::protocol::frame=off"
DESKTOP_LOG_DIR = Path.home() / "Library/Logs/com.openai.codex"


def remember(cache, key, value, limit=CACHE_LIMIT):
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > limit:
        cache.popitem(last=False)


def read_appended(path, cursors, window="default", on_reset=None):
    """只读完整行；保留半行，检测替换、截断及截断后重新增长。"""
    try:
        previous = cursors.get(str(path))
        stat = path.stat()
        identity = (stat.st_dev, stat.st_ino)
        version = (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
        if previous and previous[0] == identity and previous[3] == version:
            return
        with path.open("rb") as stream:
            stat = os.fstat(stream.fileno())
            identity = (stat.st_dev, stat.st_ino)
            version = (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
            prefix = stream.read(min(4096, stat.st_size))
            prefix_stamp = (len(prefix), hashlib.sha256(prefix).digest())
            resume = False
            # Same-size changes may be a rewrite. For growth, check bounded
            # samples at the start and cursor without rescanning old contents.
            if previous and previous[0] == identity and previous[3][0] < stat.st_size:
                _, offset, anchor, _, old_prefix = previous
                stream.seek(offset - len(anchor))
                resume = (hashlib.sha256(prefix[:old_prefix[0]]).digest() == old_prefix[1]
                          and stream.read(len(anchor)) == anchor)
            if resume:
                stream.seek(previous[1])
            else:
                if on_reset is not None:
                    on_reset()
                start = max(0, stat.st_size - (WINDOW if window == "default" else window)) if window else 0
                stream.seek(max(0, start - 1))
                if start and stream.read(1) != b"\n":
                    stream.readline()
            position = stream.tell()
            while position < stat.st_size:
                line = stream.readline()
                if not line or not line.endswith(b"\n"):
                    stream.seek(position)
                    break
                yield line
                position += len(line)
            offset = position
            anchor_start = max(0, offset - 64)
            stream.seek(anchor_start)
            anchor = stream.read(offset - anchor_start)
            # The caller releases cursors when files leave its scan set. Evicting
            # a live file here would repeatedly replay every file past the limit.
            cursors[str(path)] = (identity, offset, anchor, version, prefix_stamp)
    except FileNotFoundError:
        cursors.pop(str(path), None)
        if on_reset is not None:
            on_reset()
        return


def ro_database(path):
    db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
    db.execute("PRAGMA query_only=ON")
    db.row_factory = sqlite3.Row
    return db


def database_signature(paths):
    """SQLite WAL 的提交也会影响会话元数据，不能只检查主数据库文件。"""
    signature = []
    for path in paths:
        for source in (path, path.with_name(path.name + "-wal")):
            try:
                stat = source.stat()
                signature.append((stat.st_dev, stat.st_ino, stat.st_size,
                                  stat.st_mtime_ns, stat.st_ctime_ns))
            except FileNotFoundError:
                signature.append(None)
    return tuple(signature)


def log_text(line):
    """展开桌面 CLI 日志块，保留未加引号的片段，忽略其他桌面日志。"""
    text = ANSI.sub("", line.decode("utf-8", errors="replace")).rstrip("\r\n").lstrip()
    wrapper = DESKTOP_MESSAGE.match(text)
    if wrapper:
        payload = text[wrapper.end():]
        try:
            message, end = JSON_DECODER.raw_decode(payload)
        except ValueError:
            return payload, "desktop_raw"
        except RecursionError:
            return "", "ignored"
        if payload[end:].strip():
            return payload, "desktop_raw"
        if isinstance(message, dict):
            return json.dumps(message), "desktop"
        if isinstance(message, str):
            return ANSI.sub("", message), "desktop"
        return payload, "desktop_raw"
    if DESKTOP_RECORD.match(text):
        return "", "ignored"
    return text, "text"


def epoch_seconds(value):
    # Keep timestamps within the supported calendar range (before year 10000).
    return value if type(value) in (int, float) and 0 <= value < 253402300800 else None


def token_count(value):
    return value if type(value) is int and value >= 0 else None


def json_record_end(text, start):
    """Find an outer JSON boundary without recursing into rejected nested data."""
    if start >= len(text) or text[start] not in "{[":
        return None
    stack = bytearray()
    quoted = escaped = False
    for position in range(start, len(text)):
        char = text[position]
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char in "{[":
            if len(stack) >= WINDOW:
                return None
            stack.append(125 if char == "{" else 93)
        elif char in "}]":
            if not stack or stack.pop() != ord(char):
                return None
            if not stack:
                return position + 1
    return None


class JsonFragment:
    """Frame an unfinished outer record once; never retry its growing prefix."""

    def __init__(self, prefix, structured, ignored):
        self.prefix, self.structured, self.ignored = prefix, structured, ignored
        self.buffer = None if ignored else io.StringIO()
        self.size = len(prefix)
        self.stack = bytearray()
        self.started = self.quoted = self.escaped = self.invalid = False
        self.kind = None
        self.scalar = ""
        # The sentinel prevents a chunk boundary from becoming a new log line.
        self.tail = "\0"

    def scan(self, text):
        if self.invalid:
            return None
        position = 0
        if not self.started:
            position = len(text) - len(text.lstrip())
            if position == len(text):
                return None
            self.started = True
            self.kind = "container" if text[position] in "{[" else "string" if text[position] == '"' else "scalar"
        if self.kind == "scalar":
            candidate = self.scalar + text[position:]
            try:
                _, end = JSON_DECODER.raw_decode(candidate)
            except (ValueError, RecursionError):
                # Only these short prefixes can become a valid scalar after
                # more bytes arrive. Invalid tokens keep unknown provenance.
                if any(value.startswith(candidate) for value in ("true", "false", "null", "NaN", "Infinity", "-Infinity")):
                    self.scalar = candidate
                else:
                    self.invalid = True
                return None
            return position + end - len(self.scalar)
        while position < len(text):
            if self.quoted:
                if self.escaped:
                    position += 1
                    self.escaped = False
                while position < len(text):
                    # Brackets inside strings are content. Only quotes and
                    # their preceding backslash runs affect framing.
                    quote = text.find('"', position)
                    end = len(text) if quote < 0 else quote
                    slash = end - 1
                    while slash >= position and text[slash] == "\\":
                        slash -= 1
                    escaped = (end - slash - 1) % 2
                    if quote < 0:
                        self.escaped = bool(escaped)
                        return None
                    position = quote + 1
                    if escaped:
                        continue
                    self.quoted = False
                    if self.kind == "string":
                        return position
                    break
                if self.quoted:
                    return None
            for token in JSON_FRAME.finditer(text, position):
                char = token[0]
                if char == '"':
                    self.quoted = True
                    position = token.end()
                    break
                if char in "{[":
                    if len(self.stack) >= WINDOW:
                        self.invalid = True
                        self.stack.clear()
                        return None
                    self.stack.append(125 if char == "{" else 93)
                else:
                    if not self.stack or self.stack.pop() != ord(char):
                        self.invalid = True
                        self.stack.clear()
                        return None
                    if not self.stack:
                        return token.end()
            else:
                return None
        return None

    def feed(self, text):
        end = self.scan(text)
        recovery = self.tail + text
        # The leading sentinel blocks offset zero; recovery needs a newline.
        following = LOG_RECORD_START.search(recovery) if "\n" in recovery else None
        if following and (end is None or following.start() < len(self.tail) + end):
            return None, recovery[following.start():]
        consumed = len(text) if end is None else end
        self.size += consumed
        if self.size > WINDOW:
            # Keep the enclosing frame, even after its body is too large to
            # retain. Its closing byte still proves where new roots may begin.
            self.buffer = None
        elif self.buffer is not None and consumed:
            self.buffer.write(text[:consumed])
        if end is None and text:
            self.tail = "\0" + recovery[-512:]
        return end, None


def response_event(event, prefix, transport):
    if (not isinstance(event, dict) or not isinstance(event.get("type"), str)
            or event["type"] not in EVENT_TYPES):
        return None
    response = event.get("response")
    if not isinstance(response, dict):
        return None
    rid = response.get("id")
    if not isinstance(rid, str) or not rid:
        return None
    logged_at = None
    try:
        stamp = prefix.split()[0]
        timestamp = datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()
        logged_at = timestamp
    except (ValueError, IndexError, OverflowError, OSError):
        stamp = epoch_seconds(response.get("created_at"))
        timestamp = stamp if stamp is not None else time.time()
    reasoning = response.get("reasoning")
    effort = reasoning.get("effort") if isinstance(reasoning, dict) else None
    usage = response.get("usage")
    usage = usage if isinstance(usage, dict) else {}
    input_details, output_details = usage.get("input_tokens_details"), usage.get("output_tokens_details")
    input_details = input_details if isinstance(input_details, dict) else {}
    output_details = output_details if isinstance(output_details, dict) else {}
    model = response.get("model")
    error = response.get("error")
    error_code = error.get("code") if isinstance(error, dict) else None
    error_message = error.get("message") if isinstance(error, dict) else None
    incomplete = response.get("incomplete_details")
    incomplete_reason = incomplete.get("reason") if isinstance(incomplete, dict) else None
    service_tier, response_status = response.get("service_tier"), response.get("status")
    return rid, {"timestamp": timestamp, "type": event["type"],
                "model": model if isinstance(model, str) else None,
                "effort": effort if isinstance(effort, str) else None,
                "transport": transport,
                "logged_at": logged_at,
                "response_created_at": epoch_seconds(response.get("created_at")),
                "response_completed_at": epoch_seconds(response.get("completed_at")),
                "error_code": error_code if isinstance(error_code, str) else None,
                "error_message": error_message if isinstance(error_message, str) else None,
                "incomplete_reason": incomplete_reason if isinstance(incomplete_reason, str) else None,
                "service_tier": service_tier if isinstance(service_tier, str) else None,
                "response_status": response_status if isinstance(response_status, str) else None,
                "input_tokens": token_count(usage.get("input_tokens")),
                "cached_input_tokens": token_count(input_details.get("cached_tokens")),
                "cache_write_tokens": token_count(input_details.get("cache_write_tokens")),
                "output_tokens": token_count(usage.get("output_tokens")),
                "total_tokens": token_count(usage.get("total_tokens")),
                "reasoning_tokens": token_count(output_details.get("reasoning_tokens"))}


def parse_events(text, prefix=None):
    """只提取明确的 SSE 元数据，不在任意正文中搜索 JSON。"""
    tail_start = None
    consumed = 0
    for match in LOG_RECORD_START.finditer(text):
        if match.start() < consumed:
            continue
        if text.startswith("[DONE]", match.end()):
            consumed = match.end() + len("[DONE]")
            tail_start = None
            continue
        try:
            event, end = JSON_DECODER.raw_decode(text, match.end())
        except (ValueError, RecursionError):
            end = json_record_end(text, match.end())
            following = LOG_RECORD_START.search(text, match.end())
            if following and (end is None or following.start() < end):
                consumed = following.start()
                tail_start = None
                continue
            if end is not None:
                consumed = end
                tail_start = None
                continue
            tail_start = text.rfind("\n", 0, match.start()) + 1
            break
        consumed = end
        tail_start = None
        transport = "WebSocket" if "tungstenite::protocol" in match[0] else "SSE"
        parsed = response_event(event, prefix if match.start() == 0 and prefix is not None else match[0], transport)
        if parsed is not None:
            yield parsed
    # An incomplete object retains its enclosing marker, never an inner object.
    if tail_start is not None:
        tail = text[tail_start:] if len(text) - tail_start <= WINDOW else ""
    else:
        tail = text[max(consumed, len(text) - 512):]
    yield None, tail


def completed_event(line):
    text, _ = log_text(line)
    return next(((rid, event) for rid, event in parse_events(text)
                 if rid and event["type"] == "response.completed"), None)


class LinkArchiveError(OSError):
    pass


class LinkArchive:
    """Private, disposable storage for exact bindings evicted from memory."""

    def __init__(self):
        directory = db = None
        try:
            directory = tempfile.TemporaryDirectory(prefix="codex-response-links-")
            self.path = Path(directory.name) / "links.sqlite"
            self.path.touch(mode=0o600)
            # Only the collector uses the connection. Allow a finalizer on
            # another thread to close it after the archive becomes unreachable.
            db = sqlite3.connect(self.path, check_same_thread=False)
            self.db = db
            self.execute("PRAGMA cache_size=-1024")
            self.execute("PRAGMA synchronous=OFF")  # Rebuilt from sources on restart.
            self.execute("CREATE TABLE links (rid TEXT PRIMARY KEY, source TEXT NOT NULL, binding TEXT NOT NULL)")
            self.execute("CREATE INDEX links_source ON links(source)")
        except BaseException as error:
            self.release(db, directory)
            if isinstance(error, (OSError, sqlite3.Error)):
                raise LinkArchiveError("无法创建临时关联索引：" + str(error)) from error
            raise
        self.cleanup = weakref.finalize(self, self.release, db, directory)
        # The owning collector closes explicitly. Interpreter shutdown must not
        # close SQLite concurrently with a still-running owner.
        self.cleanup.atexit = False

    @staticmethod
    def release(db, directory):
        try:
            if db is not None:
                db.close()
        finally:
            if directory is not None:
                directory.cleanup()

    def execute(self, sql, parameters=()):
        try:
            return self.db.execute(sql, parameters)
        except sqlite3.Error as error:
            # Cache failures must not masquerade as a temporarily locked source
            # database or silently discard a binding that cannot be recovered.
            raise LinkArchiveError("临时关联索引读写失败：" + str(error)) from error

    def put(self, rid, source, link, request):
        self.execute("INSERT OR REPLACE INTO links VALUES (?, ?, ?)",
                     (json.dumps(rid), json.dumps(source), json.dumps((link, request))))

    def remove(self, rid):
        self.execute("DELETE FROM links WHERE rid=?", (json.dumps(rid),))

    def forget_source(self, source):
        self.execute("DELETE FROM links WHERE source=?", (json.dumps(source),))

    def find(self, rids):
        keys = list(rids)
        for offset in range(0, len(keys), 400):
            batch = [json.dumps(rid) for rid in keys[offset:offset + 400]]
            placeholders = ",".join("?" for _ in batch)
            try:
                rows = self.execute(f"SELECT rid, source, binding FROM links WHERE rid IN ({placeholders})", batch).fetchall()
            except sqlite3.Error as error:
                raise LinkArchiveError("临时关联索引读取失败：" + str(error)) from error
            for rid, source, binding in rows:
                link, request = json.loads(binding)
                yield json.loads(rid), json.loads(source), tuple(link), request

    def close(self):
        self.cleanup()

    def flush(self):
        try:
            self.db.commit()
        except sqlite3.Error as error:
            raise LinkArchiveError("临时关联索引提交失败：" + str(error)) from error


class CollectionStopped(Exception):
    pass


class Summary:
    def __init__(self, home, log_dir, thread=None, log=None, desktop_dir=None, progress=None):
        self.home, self.log_dir, self.thread, self.log = home, log_dir, thread, log
        self.desktop_dir = desktop_dir
        self.cursors = collections.OrderedDict()
        self.rollout_cursors = collections.OrderedDict()
        self.link_archive = None
        self.link_sources = {}
        self.source_links = {}
        self.events = collections.OrderedDict()
        self.records = collections.OrderedDict()
        self.links = collections.OrderedDict()
        self.requests = collections.OrderedDict()
        self.unobserved_links = collections.OrderedDict()
        self.contexts = collections.OrderedDict()
        self.context_keys = {}
        self.fragments = {}
        self.json_fragments = {}
        self.discarded_fragments = set()
        self.saw_websocket = False
        self.saw_structured_log = False
        self.emitted = collections.OrderedDict()
        self.rows = []
        self.status = ""
        self.dirty = True
        self.metadata = {}
        self.metadata_links = set()
        self.database_stamp = None
        self.base_status = None
        self.rollout_query = None
        self.rollout_threads = []
        self.row_cache = {}
        self.progress = progress
        self.progress_stage = None
        self.progress_at = 0.0
        self.stop_requested = None

    def check_stopped(self):
        if self.stop_requested is not None and self.stop_requested.is_set():
            raise CollectionStopped()

    def close(self):
        if self.link_archive is not None:
            self.link_archive.close()
            self.link_archive = None

    def report_progress(self, stage, *, files=None, total=None, byte_count=None,
                        events=None, linked=None, force=False):
        self.check_stopped()
        if self.progress is None:
            return
        now = time.monotonic()
        if not force and stage == self.progress_stage and now - self.progress_at < PROGRESS_INTERVAL:
            return
        self.progress_stage, self.progress_at = stage, now
        details = []
        if files is not None:
            details.append(f"已处理 {files}/{total} 个文件")
        if byte_count is not None:
            details.append(f"已读取 {byte_count:,} 字节")
        if events is not None:
            details.append(f"已识别 {events} 个响应事件")
        if linked is not None:
            details.append(f"已关联 {linked}/{len(self.records)} 条响应")
        self.progress(stage + ("：" + "，".join(details) if details else ""))

    def log_files(self):
        self.check_stopped()
        if self.log is not None:
            return [self.log]
        paths = {}
        for folder in (self.log_dir, self.desktop_dir):
            if folder is None:
                continue
            for path in folder.rglob("*"):
                self.check_stopped()
                if not (".log" in path.name or path.suffix in (".sse", ".jsonl")):
                    continue
                if path.suffix in (".gz", ".zip", ".bz2", ".xz"):
                    continue
                try:
                    if path.is_file():
                        paths[path] = path.stat().st_mtime_ns
                except FileNotFoundError:
                    continue
        self.check_stopped()
        return sorted(paths, key=lambda p: (paths[p], str(p)))

    def decode_events(self, line, path):
        text, source = log_text(line)
        if source == "ignored":
            return
        key = str(path)
        if (source == "desktop_raw" and key not in self.json_fragments
                and key not in self.fragments and key not in self.discarded_fragments
                and LOG_RECORD_START.match(text) is None):
            # A raw block can continue known CLI bytes, but an invalid standalone
            # message must not hide the next real log record.
            return
        wrapped = source.startswith("desktop")
        fragment = self.fragments.pop(key, "")
        pending = self.json_fragments.pop(key, None)
        separator = "" if wrapped else "\n"
        if key in self.discarded_fragments:
            # Once the enclosing bytes exceed the limit, only a real new log
            # prefix can restore provenance. A bare object may still be nested.
            text = fragment + separator + text
            following = LOG_RECORD_START.search(text)
            if following is None:
                self.fragments[key] = "\0" + text[-512:]
                return
            self.discarded_fragments.remove(key)
            text = text[following.start():]
        elif pending is not None:
            text = separator + text
        elif fragment:
            text = fragment + separator + text
        position = 0
        while position < len(text) or pending is not None:
            if pending is None:
                while position < len(text) and text[position].isspace():
                    position += 1
                if position == len(text):
                    break
                structured = text[position] in "{["
                ignored = False
                prefix = ""
                if structured:
                    json_start = position
                else:
                    match = LOG_RECORD_START.search(text, position)
                    container = JSON_CONTAINER_START.search(text, position, match.start() if match else len(text))
                    if container is not None:
                        json_start = container.start()
                        prefix = text[text.rfind("\n", 0, json_start) + 1:json_start]
                        structured = not prefix.strip()
                        ignored = not structured
                    elif match is None:
                        if wrapped:
                            tail_start = max(position, len(text) - 512)
                            self.fragments[key] = ("\0" if tail_start > position else "") + text[tail_start:]
                        break
                    else:
                        prefix = match[0]
                        json_start = match.end()
                        if text.startswith("[DONE]", json_start):
                            position = json_start + len("[DONE]")
                            continue
                try:
                    record, end = JSON_DECODER.raw_decode(text, json_start)
                except (ValueError, RecursionError):
                    pending = JsonFragment(prefix, structured, ignored)
                    text, position = text[json_start:], 0
            if pending is not None:
                end, following = pending.feed(text)
                if following is not None:
                    text, position, pending = following, 0, None
                    continue
                if end is None:
                    if pending.invalid and pending.size > WINDOW:
                        self.discarded_fragments.add(key)
                        self.fragments[key] = pending.tail
                    else:
                        self.json_fragments[key] = pending
                    break
                prefix, structured, ignored = pending.prefix, pending.structured, pending.ignored
                if pending.buffer is None and not ignored:
                    text, position, pending = text[end:], 0, None
                    continue
                if not ignored:
                    try:
                        record, _ = JSON_DECODER.raw_decode(pending.buffer.getvalue().lstrip())
                    except (ValueError, RecursionError):
                        if pending.kind != "container":
                            # A malformed scalar has no proven JSON boundary.
                            # Resume only at a real log prefix, as for truncation.
                            pending.invalid = True
                            pending.tail = "\0" + (pending.tail + text[:end])[-512:]
                            text, position = text[end:], 0
                            continue
                        text, position, pending = "\0" + text[end:], 0, None
                        continue
                pending = None
            position = end
            if ignored:
                # An inline container belongs to the ordinary log before it.
                # Keep that provenance through its closing byte and later chunks.
                text, position = "\0" + text[end:], 0
                continue
            if not structured:
                transport = "WebSocket" if "tungstenite::protocol" in prefix else "SSE"
                parsed = response_event(record, prefix, transport)
                if parsed is not None:
                    yield parsed
                continue
            if not isinstance(record, dict) or not isinstance(record.get("fields"), dict):
                continue
            self.saw_structured_log = True
            target = record.get("target", "")
            fields = record["fields"]
            message = fields.get("message")
            stamp = record.get("timestamp", "")
            if not isinstance(message, str):
                continue
            if target == "codex_api::endpoint::responses_websocket" and message.startswith("successfully connected"):
                self.saw_websocket = True
            if target == "codex_api::responses_websocket_timing":
                payload = fields.get("payload")
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except (ValueError, RecursionError):
                        continue
                metrics = payload.get("timing_metrics") if isinstance(payload, dict) else None
                rid = metrics.get("response_id") if isinstance(metrics, dict) else None
                if not isinstance(rid, str) or not rid:
                    continue
                try:
                    timestamp = datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp()
                except (ValueError, AttributeError, OverflowError):
                    continue
                self.saw_websocket = True
                request_start = fields.get("request_start_ms")
                try:
                    request_start = epoch_seconds(float(request_start) / 1000) if type(request_start) in (str, int, float) else None
                except (ValueError, OverflowError):
                    request_start = None
                # This proves a response ID was observed, not a final model or effort.
                yield rid, {"timestamp": timestamp, "type": "responsesapi.websocket_timing",
                            "transport": "WebSocket", "logged_at": timestamp,
                            "request_started_at": request_start,
                            "model": None, "effort": None, "reasoning_tokens": None}
            elif (target == "tungstenite::protocol" and message.startswith("Received message ")) or (
                isinstance(target, str) and re.fullmatch(r"codex_api::sse(?:::\w+)*", target)
                and message.startswith("SSE event:")
            ):
                if target == "tungstenite::protocol":
                    self.saw_websocket = True
                normalized = f"{target}: {message}"
                for rid, event in parse_events(normalized, prefix=str(stamp)):
                    if rid:
                        yield rid, event

    def reset_decoder(self, path):
        self.fragments.pop(str(path), None)
        self.json_fragments.pop(str(path), None)
        self.discarded_fragments.discard(str(path))

    def read_events(self):
        self.report_progress("正在查找日志文件")
        paths = self.log_files()
        active = {str(path) for path in paths}
        self.discarded_fragments.intersection_update(active)
        for fragments in (self.fragments, self.json_fragments, self.cursors):
            for key in fragments.keys() - active:
                del fragments[key]
        byte_count = event_count = 0
        self.report_progress("正在读取日志", files=0, total=len(paths), byte_count=0, events=0)
        for index, path in enumerate(paths):
            # A desktop log can exceed WINDOW before rotation. Read its complete
            # initial contents so restarts retain earlier responses and created events.
            for line in read_appended(path, self.cursors, window=None,
                                      on_reset=lambda path=path: self.reset_decoder(path)):
                self.check_stopped()
                for rid, event in self.decode_events(line, path):
                    if self.progress is not None:
                        event_count += 1
                    self.dirty = True
                    record = self.records.get(rid)
                    if record is None:
                        # An evicted completion must not attach to a fresh partial record.
                        self.events.pop(rid, None)
                        record = {"timestamp": event["timestamp"], "created": None, "latest": None, "final": None,
                                  "request_started_at": None, "first_logged_at": None}
                        self.records[rid] = record
                        self.unobserved_links.pop(rid, None)
                        if len(self.records) > RESPONSE_LIMIT:
                            expired, _ = self.records.popitem(last=False)
                            if expired in self.links:
                                self.unobserved_links[expired] = None
                    if record["request_started_at"] is None and event.get("request_started_at") is not None:
                        record["request_started_at"] = event["request_started_at"]
                    logged_at = event.get("logged_at")
                    if logged_at is not None and (record["first_logged_at"] is None or logged_at < record["first_logged_at"]):
                        record["first_logged_at"] = logged_at
                    typ = event["type"]
                    if typ == "response.created" and record["created"] is None:
                        record["created"] = event
                    if typ in TERMINAL and record["final"] is None:
                        record["final"] = event
                    # Reading a rotated file must not regress a final state to in_progress.
                    latest = record["latest"]
                    if latest is None or (typ != "responsesapi.websocket_timing" and (
                        latest["type"] == "responsesapi.websocket_timing" or event["timestamp"] >= latest["timestamp"]
                    )):
                        record["latest"] = event
                    if typ == "response.completed" and rid not in self.emitted:
                        remember(self.events, rid, event, limit=RESPONSE_LIMIT)
                if self.progress is not None:
                    byte_count += len(line)
                    self.report_progress("正在读取日志", files=index, total=len(paths),
                                         byte_count=byte_count, events=event_count)
            self.report_progress("正在读取日志", files=index + 1, total=len(paths),
                                 byte_count=byte_count, events=event_count)
        self.report_progress("正在读取日志", files=len(paths), total=len(paths),
                             byte_count=byte_count, events=event_count, force=True)

    def remember_link(self, rid, link, request, source=None):
        self.remove_link_source(rid)
        if source is not None:
            self.link_sources[rid] = source
            self.source_links.setdefault(source, set()).add(rid)
        self.links[rid], self.requests[rid] = link, request
        self.links.move_to_end(rid)
        self.requests.move_to_end(rid)
        if rid not in self.records:
            self.unobserved_links[rid] = None
            self.unobserved_links.move_to_end(rid)
        while len(self.links) > CACHE_LIMIT:
            expired = next(iter(self.unobserved_links))
            source = self.link_sources.get(expired)
            if source is not None:
                if self.link_archive is None:
                    self.link_archive = LinkArchive()
                # Write the exact binding before releasing its memory slot;
                # read_links commits once per file instead of once per record.
                self.link_archive.put(expired, source, self.links[expired], self.requests[expired])
            del self.unobserved_links[expired]
            del self.links[expired]
            del self.requests[expired]
            self.remove_link_source(expired)
        if self.link_archive is not None:
            self.link_archive.remove(rid)

    def remove_link_source(self, rid):
        source = self.link_sources.pop(rid, None)
        if source is not None:
            linked = self.source_links[source]
            linked.remove(rid)
            if not linked:
                del self.source_links[source]

    def forget_link_source(self, source):
        if self.link_archive is not None:
            self.link_archive.forget_source(source)
        for rid in self.source_links.pop(source, ()):
            del self.link_sources[rid]
            # Observed responses retain their already-proven immutable evidence.
            if rid not in self.records:
                self.links.pop(rid, None)
                self.requests.pop(rid, None)
                self.unobserved_links.pop(rid, None)

    def remember_context(self, key, context):
        self.contexts[key] = context
        self.contexts.move_to_end(key)
        self.context_keys.setdefault(key[0], set()).add(key)
        while len(self.contexts) > CACHE_LIMIT:
            expired, _ = self.contexts.popitem(last=False)
            keys = self.context_keys[expired[0]]
            keys.remove(expired)
            if not keys:
                del self.context_keys[expired[0]]

    def reset_contexts(self, thread_id):
        for key in self.context_keys.pop(thread_id, ()):
            del self.contexts[key]

    def reset_rollout(self, path, thread_id):
        self.forget_link_source(str(path))
        self.reset_contexts(thread_id)

    def read_links(self, state_path, state_stamp):
        try:
            self.read_link_sources(state_path, state_stamp)
        except LinkArchiveError:
            # SQLite can roll back a whole transaction, including evictions
            # sourced from files whose cursors already reached EOF. Discard the
            # index and all scan state so any later retry rebuilds exact evidence.
            self.rollout_cursors.clear()
            self.contexts.clear()
            self.context_keys.clear()
            self.rollout_query = None
            self.rollout_threads = []
            for rid in self.links.keys() - self.records.keys():
                del self.links[rid]
                self.requests.pop(rid, None)
            self.unobserved_links.clear()
            self.link_sources.clear()
            self.source_links.clear()
            self.close()
            raise

    def read_link_sources(self, state_path, state_stamp):
        missing = set(self.records).difference(self.links)
        self.report_progress("正在关联会话", linked=len(self.records) - len(missing))
        if not missing and (self.rollout_query is None or self.rollout_query[0] == state_stamp):
            return
        since = int(min(self.records[rid]["timestamp"] for rid in missing)) - 300 if missing else self.rollout_query[1]
        query = (state_stamp, since)
        if query != self.rollout_query:
            with closing(ro_database(state_path)) as state:
                self.rollout_threads = state.execute(
                    "SELECT id, rollout_path FROM threads WHERE updated_at >= ? ORDER BY updated_at DESC",
                    (query[1],),
                ).fetchall()
            self.rollout_query = query
        active = {thread["rollout_path"] for thread in self.rollout_threads if thread["rollout_path"]}
        for path in self.rollout_cursors.keys() - active:
            self.forget_link_source(path)
            del self.rollout_cursors[path]
        active_threads = {thread["id"] for thread in self.rollout_threads if thread["rollout_path"]}
        for tid in self.context_keys.keys() - active_threads:
            self.reset_contexts(tid)
        if not missing:
            if self.link_archive is not None:
                self.link_archive.flush()
            return
        total = sum(bool(thread["rollout_path"]) for thread in self.rollout_threads) if self.progress is not None else 0
        files = byte_count = 0
        for thread in self.rollout_threads:
            if not thread["rollout_path"]:
                continue
            path, tid = thread["rollout_path"], thread["id"]
            # Read full initial rollouts: turn_context can precede the response by >8 MiB.
            for line in read_appended(Path(path), self.rollout_cursors, window=None,
                                      on_reset=lambda path=path, tid=tid: self.reset_rollout(path, tid)):
                self.check_stopped()
                if self.progress is not None:
                    byte_count += len(line)
                    self.report_progress("正在关联会话", files=files, total=total,
                                         byte_count=byte_count, linked=len(self.records) - len(missing))
                if b"turn_context" not in line and b"token_usage_record" not in line:
                    continue
                try:
                    value = json.loads(line)
                except (ValueError, UnicodeDecodeError, RecursionError):
                    continue
                if not isinstance(value, dict) or not isinstance(value.get("payload"), dict):
                    continue
                payload = value["payload"]
                turn = payload.get("turn_id") or payload.get("root_turn_id")
                if not isinstance(turn, str) or not turn:
                    continue
                if value.get("type") == "turn_context":
                    self.remember_context((tid, turn), {
                        "request_model": payload.get("model") if isinstance(payload.get("model"), str) else None,
                        "request_effort": payload.get("effort") if isinstance(payload.get("effort"), str) else None,
                    })
                elif value.get("type") == "token_usage_record" and payload.get("thread_id") == tid:
                    rid = payload.get("response_id")
                    if isinstance(rid, str) and rid:
                        request = dict(self.contexts.get((tid, turn), {}))
                        if rid in self.records and (self.links.get(rid) != (tid, turn)
                                                    or self.requests.get(rid) != request):
                            self.dirty = True
                        # Bind the context at this response, before later context changes.
                        self.remember_link(rid, (tid, turn), request, source=path)
                        missing.discard(rid)
            if self.link_archive is not None:
                self.link_archive.flush()
            files += 1
            self.report_progress("正在关联会话", files=files, total=total,
                                 byte_count=byte_count, linked=len(self.records) - len(missing))
            if not missing:
                break
        # Validate sources and consume normal appends first. Exact archived
        # bindings restore late responses without replaying any historical bytes.
        if missing and self.link_archive is not None:
            for rid, source, link, request in self.link_archive.find(missing):
                self.check_stopped()
                self.remember_link(rid, link, request, source=source)
                self.dirty = True
                missing.discard(rid)
            self.link_archive.flush()
        self.report_progress("正在关联会话", files=files, total=total, byte_count=byte_count,
                             linked=len(self.records) - len(missing), force=True)

    def read_metadata(self, state_path, history_path, stamp):
        self.report_progress("正在读取会话元数据")
        if stamp == self.database_stamp and not self.dirty:
            return
        links = set(self.links[rid] for rid in self.records if rid in self.links)
        if stamp == self.database_stamp and links == self.metadata_links:
            return
        unchanged = stamp == self.database_stamp
        needed = links - self.metadata_links if unchanged else links
        metadata = {link: value for link, value in self.metadata.items() if link in links} if unchanged else {}
        if needed:
            thread_ids = sorted({tid for tid, _ in needed})
            titles, counts = {}, {}
            with closing(ro_database(state_path)) as state, closing(ro_database(history_path)) as history:
                # Batch by thread, never by turn: filtering turns before the window
                # function would silently change their original ordinal counts.
                for offset in range(0, len(thread_ids), 400):
                    batch = thread_ids[offset:offset + 400]
                    placeholders = ",".join("?" for _ in batch)
                    for row in state.execute(
                        f"SELECT id, title, name FROM threads WHERE id IN ({placeholders})", batch
                    ):
                        titles[row["id"]] = row["name"] or row["title"] or row["id"]
                    for row in history.execute(
                        "SELECT thread_id, turn_id, count(*) OVER (PARTITION BY thread_id ORDER BY rollout_ordinal) AS count "
                        f"FROM thread_turns WHERE thread_id IN ({placeholders})", batch
                    ):
                        link = (row["thread_id"], row["turn_id"])
                        if link in needed:
                            counts[link] = row["count"]
            for link in needed:
                if link[0] in titles:
                    metadata[link] = (titles[link[0]], counts.get(link))
        if metadata != self.metadata:
            self.dirty = True
        self.metadata = metadata
        self.metadata_links = links
        self.database_stamp = stamp

    def response_row(self, rid, record, link, title, turn):
        final, created = record["final"], record["created"]
        latest = final or record["latest"]
        request = self.requests.get(rid, {})
        # Parsed events and request contexts are replaced, never edited in place.
        # Compare only this row's inputs; an unrelated response or database write
        # must not rebuild thousands of complete rows.
        key = (record["timestamp"], record["request_started_at"], record["first_logged_at"],
               created, latest, final, link, title, turn, request)
        cached = self.row_cache.get(rid)
        if cached is not None and cached[0] == key:
            return cached[1]
        response_created_at = created.get("response_created_at") if created else None
        if response_created_at is None:
            response_created_at = latest.get("response_created_at")
        response_completed_at = final.get("response_completed_at") if final else None
        response_duration_ms = None
        if (response_created_at is not None and response_completed_at is not None
                and response_completed_at >= response_created_at):
            response_duration_ms = round((response_completed_at - response_created_at) * 1000)
        row = {
            "response_id": rid, "timestamp": record["timestamp"],
            "request_started_at": record["request_started_at"],
            "response_created_at": response_created_at,
            "response_completed_at": response_completed_at,
            "first_logged_at": record["first_logged_at"],
            "final_logged_at": final.get("logged_at") if final else None,
            "response_duration_ms": response_duration_ms,
            "transport": latest.get("transport"),
            "response_event": latest["type"],
            "response_status": latest.get("response_status"),
            "service_tier": latest.get("service_tier"),
            "error_code": final.get("error_code") if final else None,
            "error_message": final.get("error_message") if final else None,
            "incomplete_reason": final.get("incomplete_reason") if final else None,
            "thread_id": link[0] if link else None, "title": title, "turn": turn,
            "turn_id": link[1] if link else None,
            "request_model": None, "request_effort": None,
            "response_model": latest["model"],
            "first_effort": created["effort"] if created else None,
            "final_effort": final["effort"] if final else None,
            "reasoning_tokens": final["reasoning_tokens"] if final else None,
            "input_tokens": final.get("input_tokens") if final else None,
            "cached_input_tokens": final.get("cached_input_tokens") if final else None,
            "cache_write_tokens": final.get("cache_write_tokens") if final else None,
            "output_tokens": final.get("output_tokens") if final else None,
            "total_tokens": final.get("total_tokens") if final else None,
            "status": {"response.completed": "已完成", "response.failed": "失败",
                       "responsesapi.websocket_timing": "仅耗时日志",
                       "response.incomplete": "未完成"}.get(latest["type"], "响应中"),
            "http_status": None, "elapsed_ms": None,
        }
        row.update(request)
        pairs = [(row["request_model"], row["response_model"]),
                 (row["request_effort"], row["first_effort"]),
                 (row["request_effort"], row["final_effort"])]
        row["match"] = ("mismatch" if any(a is not None and b is not None and a != b for a, b in pairs)
                        else "match" if all(a is not None and b is not None for a, b in pairs) else None)
        self.row_cache[rid] = (key, row)
        return row

    def poll(self):
        self.read_events()
        if not self.records:
            self.rows = []
            if self.saw_websocket:
                self.status = "已检测到 WebSocket 连接，但未收到完整响应日志。需额外开启 tungstenite::protocol=trace。"
            elif self.saw_structured_log:
                self.status = "桌面 JSON 日志已写入，尚未发现可关联的响应事件。"
            else:
                self.status = f"等待响应日志。桌面版需以 RUST_LOG={LOG_FILTER} 和 CODEX_MAX_LOG_LEVEL=debug 启动。"
            return []
        state_path, history_path = self.home / "state_5.sqlite", self.home / "thread_history_1.sqlite"
        absent = [str(path) for path in (state_path, history_path) if not path.is_file()]
        base_status = ""
        if absent:
            base_status = "等待 Codex 会话数据库：" + "、".join(absent)
            if self.database_stamp is not None:
                self.metadata = {}
                self.database_stamp = None
                self.rollout_query = None
                self.dirty = True
        else:
            stamp = database_signature((state_path, history_path))
            self.read_links(state_path, stamp[:2])
            self.read_metadata(state_path, history_path, stamp)
        if base_status != self.base_status:
            self.dirty = True
        if not self.dirty:
            return []
        self.report_progress("正在整理响应")
        self.status = base_status
        self.base_status = base_status
        simple, rows, pending = [], [], 0
        for rid in self.row_cache.keys() - self.records.keys():
            del self.row_cache[rid]
        for rid, record in sorted(self.records.items(), key=lambda item: item[1]["timestamp"]):
            self.check_stopped()
            link = self.links.get(rid)
            if self.thread and (link is None or link[0] != self.thread):
                if link:
                    remember(self.emitted, rid, None)
                    self.events.pop(rid, None)
                continue
            title, turn = self.metadata.get(link, (None, None))
            if title is None or turn is None:
                pending += 1
            row = self.response_row(rid, record, link, title, turn)
            rows.append(row)
            if rid in self.events and title is not None and turn is not None:
                final = record["final"]
                simple.append((title, turn, final["model"], final["effort"]))
                remember(self.emitted, rid, None)
                self.events.pop(rid, None)
        if len(rows) != len(self.rows) or any(a is not b for a, b in zip(rows, self.rows)):
            self.rows = rows
        if pending and not self.status:
            self.status = f"{pending} 条响应等待本地会话记录关联；已收到的服务端字段仍可查看。"
        incomplete_logs = sum(row["status"] == "仅耗时日志" for row in rows)
        if incomplete_logs:
            self.status += (" " if self.status else "") + (
                f"{incomplete_logs} 条 WebSocket 记录仅有耗时日志，返回字段未落盘，无法补采。"
                "这些记录不能用于判断当前日志开关是否生效。"
            )
        self.dirty = False
        return simple



def width(text):
    return sum(
        0 if unicodedata.combining(ch) else 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
        for ch in text
    )


def clean(value):
    return "".join(ch if ch.isprintable() else " " for ch in str(value))


def cell(value, size, truncate=False):
    text = clean(value)
    displayed = width(text)
    if truncate and displayed > size:
        displayed = 0
        end = 0
        for char in text:
            char_width = width(char)
            if displayed + char_width > size - 1:
                break
            displayed += char_width
            end += 1
        text = text[:end] + "…"
        displayed += 1
    return text + " " * max(0, size - displayed)


def show(row, name_width):
    title, turn, model, effort = row
    print("  ".join((cell(title, name_width, truncate=bool(name_width)), cell(turn, 6),
                     cell(model or "未返回", 24), clean(effort or "未返回"))), flush=True)


def config_path(value, home):
    path = Path(value).expanduser()
    return path if path.is_absolute() else home / path


def poll_safely(summary):
    try:
        return summary.poll()
    except sqlite3.OperationalError as error:
        if error.sqlite_errorcode not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED, sqlite3.SQLITE_CANTOPEN):
            raise
        # Clear the temporary status after recovery even if metadata is unchanged.
        summary.dirty = True
        summary.status = "等待会话数据库可读：" + str(error)
        return []


def diagnostics(summary):
    result = {"log_dir": str(summary.log_dir),
              "desktop_log_dir": str(summary.desktop_dir) if summary.desktop_dir else None,
              "blocked_sqlite_log_insert": False}
    path = summary.home / "logs_2.sqlite"
    try:
        if path.is_file():
            with closing(ro_database(path)) as db:
                result["blocked_sqlite_log_insert"] = bool(db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name='codex_block_logs_insert'"
                ).fetchone())
    except (OSError, sqlite3.Error) as error:
        result["blocked_sqlite_log_insert"] = None
        result["sqlite_log_error"] = str(error)
    return result


def control_path(port):
    return Path(__file__).resolve().parent / ".runtime" / f"log-viewer-{port}.json"


def control(port, action="status", *, timeout=3):
    """用本实例的本地凭据控制服务，不按进程名或端口杀进程。"""
    if action == "stop" and control(port, timeout=timeout) is None:
        return None
    path = control_path(port)
    if not path.is_file():
        return None
    state = json.loads(path.read_text())
    with closing(http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)) as connection:
        try:
            connection.request("POST", "/api/control/" + action,
                               headers={"Authorization": "Bearer " + state["token"]})
            response = connection.getresponse()
        except ConnectionRefusedError:
            return None
        if response.status != 200:
            raise OSError("端口服务未通过查看器身份验证，未执行控制操作")
        result = json.loads(response.read(4096))
        if result.get("script") != str(Path(__file__).resolve()):
            raise OSError("端口服务不是当前查看器")
        return result


class RowSnapshot:
    """Immutable visible window; row versions allow deltas without a history log."""

    def __init__(self, data, instance, revision, previous=None):
        self.data = data
        self.instance, self.revision = instance, revision
        self.etag = f'"{instance}-{revision}"'
        self.versioned_rows = {}
        previous_rows = previous.versioned_rows if previous else {}
        for row in data["rows"]:
            rid = row["response_id"]
            old = previous_rows.get(rid)
            version = old[0] if old is not None and (old[1] is row or old[1] == row) else revision
            self.versioned_rows[rid] = (version, row)
        self.full_body = None
        self.delta_tag = None
        self.delta_body = None
        self.encode_lock = threading.Lock()

    def encode(self, requested_tag=None, delta=False):
        match = re.fullmatch(r'"' + re.escape(self.instance) + r'-(\d{1,16})"', requested_tag or "") if delta else None
        base = int(match[1]) if match else None
        if base is not None and not 0 <= base < self.revision:
            base = None
        with self.encode_lock:
            if base is not None:
                if requested_tag == self.delta_tag:
                    return self.delta_body
                rows = [row for version, row in self.versioned_rows.values() if version > base]
                if len(rows) == len(self.versioned_rows):
                    base = None  # A replaced window needs no extra ID-order payload.
            if base is None:
                if self.full_body is None:
                    self.full_body = json.dumps(self.data, ensure_ascii=False, separators=(",", ":")).encode(
                        "utf-8", errors="backslashreplace")
                return self.full_body
            if requested_tag != self.delta_tag:
                # The complete ID order also removes evicted rows. New/re-entering
                # rows always get a fresh version, even after a long pause.
                payload = dict(self.data, base=requested_tag,
                               order=list(self.versioned_rows),
                               rows=rows)
                self.delta_body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
                    "utf-8", errors="backslashreplace")
                self.delta_tag = requested_tag
            return self.delta_body


def serve(summary, port, lines, open_browser):
    collector_owns_summary = threading.Event()
    try:
        return serve_summary(summary, port, lines, open_browser, collector_owns_summary)
    finally:
        # A running collector still owns the archive during slow reads. It
        # closes it on exit; stopping HTTP must not wait for that read to finish.
        if not collector_owns_summary.is_set():
            summary.close()


def serve_summary(summary, port, lines, open_browser, collector_owns_summary):
    html = Path(__file__).with_name("codex_sse_dashboard.html").read_bytes()
    source_diagnostics = diagnostics(summary)
    last_view = None
    last_source = None
    instance_id = secrets.token_hex(8)
    revision = 0
    snapshot = RowSnapshot({"rows": [], "status": "正在读取日志", "loading": True,
                            "display_limit": min(lines, RESPONSE_LIMIT), "retained_count": 0,
                            "retention_limit": RESPONSE_LIMIT, "diagnostics": source_diagnostics},
                           instance_id, revision)
    token = secrets.token_urlsafe(32)
    changed = threading.Condition()
    stopped = threading.Event()
    waiting_requests = set()

    class Handler(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.settimeout(5)

        def handle(self):
            try:
                super().handle()
            except (BrokenPipeError, ConnectionResetError):
                # Pausing or hiding the page aborts its pending long poll.
                pass

        def local_request(self):
            host = self.headers.get("Host", "")
            expected = f"127.0.0.1:{self.server.server_port}"
            origin = self.headers.get("Origin")
            if host != expected or (origin and origin != "http://" + expected):
                self.send_error(403)
                return False
            return True

        def do_POST(self):
            if not self.local_request():
                return
            authorization = self.headers.get("Authorization", "")
            if not authorization.isascii() or not secrets.compare_digest(authorization, "Bearer " + token):
                self.send_error(403)
                return
            if self.path not in ("/api/control/status", "/api/control/stop"):
                self.send_error(404)
                return
            data = json.dumps({"pid": os.getpid(), "url": url,
                               "script": str(Path(__file__).resolve())}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            if self.path == "/api/control/stop":
                with changed:
                    stopped.set()
                    changed.notify_all()
                self.server.shutdown()

        def wait_for_rows(self, requested_tag, wait):
            deadline = time.monotonic() + wait
            try:
                with changed:
                    while wait and requested_tag == snapshot.etag and not stopped.is_set():
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        waiting_requests.add(self)
                        changed.wait(timeout=min(remaining, 1))
                        if select.select([self.connection], [], [], 0)[0] and not self.connection.recv(1, socket.MSG_PEEK):
                            return None
                    current = snapshot
                    stopping = stopped.is_set()
                    if not stopping:
                        # Normal updates and timeouts no longer belong to the
                        # shutdown drain, even if their response writes are slow.
                        waiting_requests.discard(self)
                if stopping:
                    self.send_error(503, "Viewer is stopping")
                    return None
                return current
            finally:
                with changed:
                    if self in waiting_requests:
                        waiting_requests.remove(self)
                        changed.notify_all()

        def do_GET(self):
            if not self.local_request():
                return
            response_tag = None
            if self.path == "/":
                data, mime = html, "text/html; charset=utf-8"
            elif self.path == "/api/rows":
                requested_tag = self.headers.get("If-None-Match")
                preference = re.fullmatch(r"wait=(\d{1,2})", self.headers.get("Prefer", ""))
                wait = min(int(preference[1]), 25) if preference else 0
                current = self.wait_for_rows(requested_tag, wait)
                if current is None:
                    return
                response_tag = current.etag
                if requested_tag == response_tag:
                    self.send_response(304)
                    self.send_header("ETag", response_tag)
                    self.send_header("Cache-Control", "no-store")
                    self.end_headers()
                    return
                data = current.encode(requested_tag, self.headers.get("X-Row-Delta") == "1")
                mime = "application/json; charset=utf-8"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", mime)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            if response_tag is not None:
                self.send_header("ETag", response_tag)
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; frame-ancestors 'none'")
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    class LocalServer(ThreadingHTTPServer):
        def server_bind(self):
            # HTTPServer resolves an FQDN here, which can stall local startup.
            # This viewer only uses the numeric loopback address.
            socketserver.TCPServer.server_bind(self)
            self.server_name, self.server_port = self.server_address[:2]

    try:
        server = LocalServer(("127.0.0.1", port), Handler)
    except OSError as bind_error:
        if bind_error.errno != errno.EADDRINUSE or not port:
            raise
        # The winner may be listening before it publishes its credentials.
        # Authenticate that instance; never treat an occupied port as identity.
        deadline = time.monotonic() + 1
        last_error = bind_error
        while (remaining := deadline - time.monotonic()) > 0:
            try:
                existing = control(port, timeout=min(remaining, 0.25))
            except (OSError, http.client.HTTPException) as error:
                last_error = error
            else:
                if existing:
                    print(f"查看器已运行：{existing['url']}（沿用当前实例配置，未重复启动）", flush=True)
                    if open_browser:
                        webbrowser.open(existing["url"])
                    return
            time.sleep(min(0.05, max(0, deadline - time.monotonic())))
        raise last_error

    with server:
        url = f"http://127.0.0.1:{server.server_port}"
        path = control_path(server.server_port)
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as temp:
            json.dump({"token": token, "pid": os.getpid()}, temp)
        os.replace(temp.name, path)
        failure = None

        def publish(view):
            nonlocal last_view, revision, snapshot
            if view == last_view:
                return
            updated = dict(view, diagnostics=source_diagnostics,
                           updated_at=datetime.datetime.now().astimezone().isoformat())
            with changed:
                if stopped.is_set():
                    return
                revision += 1
                snapshot = RowSnapshot(updated, instance_id, revision, snapshot)
                changed.notify_all()
            last_view = view

        def publish_progress(status):
            # Rows are built once after association. Progress owns only fresh
            # metadata, so published snapshots never share mutable counters.
            publish({"rows": [], "status": status, "loading": True,
                     "display_limit": min(lines, RESPONSE_LIMIT),
                     "retained_count": len(summary.records), "retention_limit": RESPONSE_LIMIT})

        def collect():
            nonlocal last_source, failure
            original_progress = summary.progress
            original_stop = summary.stop_requested
            summary.progress = publish_progress
            summary.stop_requested = stopped
            try:
                while not stopped.is_set():
                    try:
                        poll_safely(summary)
                    finally:
                        # Only the initial collection publishes phase updates.
                        summary.progress = original_progress
                    if stopped.is_set():
                        break
                    source = (summary.rows, summary.status, len(summary.records))
                    if last_source is None or source[0] is not last_source[0] or source[1:] != last_source[1:]:
                        view = {"rows": list(reversed(summary.rows[-lines:])), "status": summary.status,
                                "loading": False,
                                "display_limit": min(lines, RESPONSE_LIMIT),
                                "retained_count": len(summary.records), "retention_limit": RESPONSE_LIMIT}
                        publish(view)
                        last_source = source
                    stopped.wait(1)
            except CollectionStopped:
                pass
            except Exception as error:
                failure = error
                with changed:
                    stopped.set()
                    changed.notify_all()
                server.shutdown()
            finally:
                summary.progress = original_progress
                summary.stop_requested = original_stop
                summary.close()

        collector = threading.Thread(target=collect, name="log-collector", daemon=False)
        try:
            print("实时表格：" + url + "  （Ctrl+C 或双击停止脚本退出）", flush=True)
            collector_owns_summary.set()
            try:
                collector.start()
            except BaseException:
                collector_owns_summary.clear()
                raise
            if open_browser:
                threading.Thread(target=webbrowser.open, args=(url,), name="browser-opener", daemon=True).start()
            server.serve_forever(poll_interval=0.2)
        finally:
            with changed:
                stopped.set()
                changed.notify_all()
                # Only requests already waiting for rows need a final 503.
                # Daemon handlers still reading headers must not delay stopping.
                changed.wait_for(lambda: not waiting_requests)
            path.unlink(missing_ok=True)
        if failure is not None:
            raise failure


def main():
    parser = argparse.ArgumentParser(
        description="只读查看 Codex SSE / WebSocket 响应日志，关联本地会话、请求参数与返回字段；--web 打开实时表格。"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("-f", "--follow", action="store_true", help="持续监控（默认）；Ctrl+C 退出查看器")
    mode.add_argument("--once", action="store_true", help="只查看一次，不持续监控")
    mode.add_argument("--web", action="store_true", help="启动本机实时表格，支持跟随系统主题")
    mode.add_argument("--stop", action="store_true", help="停止指定端口的本查看器")
    mode.add_argument("--status", action="store_true", help="查看指定端口的运行状态")
    parser.add_argument("--no-open", action="store_true", help="不自动打开浏览器")
    parser.add_argument("--port", type=int, default=8765, help="网页端口，默认 8765；0 自动分配")
    parser.add_argument("--json", action="store_true", help="配合 --once 输出详细元数据 JSON")
    parser.add_argument("-n", "--lines", type=int, default=20, help="启动时显示最近 N 条，默认 20；0 只显示后续响应")
    parser.add_argument("--thread", help="只显示指定对话 ID")
    parser.add_argument("--name-width", type=int, default=42, help="名称列宽，0 为不截断")
    parser.add_argument("--home", type=Path, default=Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))))
    parser.add_argument("--log-dir", type=Path, help="覆盖 config.toml 的日志目录")
    parser.add_argument("--log", type=Path, help="只读取指定日志文件")
    parser.add_argument("--desktop-log-dir", type=Path, help="桌面日志目录，默认 ~/Library/Logs/com.openai.codex")
    parser.add_argument("--no-desktop", action="store_true", help="不读取桌面日志")
    args = parser.parse_args()
    if args.lines < 0 or args.name_width < 0:
        parser.error("数量和列宽不能为负数")
    if not 0 <= args.port <= 65535 or (args.web and args.lines == 0):
        parser.error("端口应为 0–65535；网页模式的 -n 必须大于 0")
    if args.json and not args.once:
        parser.error("--json 需要配合 --once")
    if args.stop or args.status:
        if args.port == 0:
            parser.error("停止或查看状态时需要指定实际端口")
        result = control(args.port, "stop" if args.stop else "status")
        if result is None:
            print("查看器未运行。")
        elif args.stop:
            deadline = time.monotonic() + 5
            while control_path(args.port).exists() and time.monotonic() < deadline:
                time.sleep(0.1)
            if control_path(args.port).exists():
                raise OSError("已发送停止请求，但尚未确认退出")
            print("查看器已停止。")
        else:
            print(f"查看器正在运行：{result['url']}，PID {result['pid']}")
        return
    home = args.home.expanduser().resolve()
    config_file = home / "config.toml"
    config = tomllib.loads(config_file.read_text(encoding="utf-8")) if config_file.is_file() else {}
    log_dir = config_path(args.log_dir or config.get("log_dir", str(home / "log")), home)
    sqlite_home = config_path(config.get("sqlite_home") or os.environ.get("CODEX_SQLITE_HOME") or str(home), home)
    log = args.log.expanduser().resolve() if args.log else None
    desktop = None if args.no_desktop or args.log or home != (Path.home() / ".codex").resolve() else DESKTOP_LOG_DIR
    if args.desktop_log_dir and not args.no_desktop:
        desktop = args.desktop_log_dir.expanduser().resolve()
    summary = Summary(sqlite_home, log_dir, args.thread, log, desktop)
    if args.web:
        serve(summary, args.port, args.lines, not args.no_open)
        return
    try:
        if args.json:
            poll_safely(summary)
            data = json.dumps({"rows": summary.rows[-args.lines:] if args.lines else [], "status": summary.status,
                               "diagnostics": diagnostics(summary)}, ensure_ascii=False, indent=2)
            print(data.encode("utf-8", errors="backslashreplace").decode("utf-8"))
            return
        print("  ".join((cell("对话名称", args.name_width), cell("轮数", 6), cell("模型", 24), "推理强度")), flush=True)
        if args.lines == 0:
            summary.read_events()
            for rid in summary.events:
                remember(summary.emitted, rid, None)
            summary.events.clear()
        first, last_notice = True, ""
        while True:
            rows = poll_safely(summary)
            if first:
                rows = rows[-args.lines:] if args.lines else []
            for row in rows:
                show(row, args.name_width)
            if not rows and summary.status and summary.status != last_notice:
                print(summary.status, file=sys.stderr, flush=True)
            last_notice = summary.status if not rows else ""
            if args.once:
                break
            first = False
            time.sleep(1)
    finally:
        summary.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except (OSError, sqlite3.Error, ValueError) as error:
        print("codex-sse: " + str(error), file=sys.stderr)
        sys.exit(1)
