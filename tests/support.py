"""Shared synthetic logs, temporary databases, and viewer module fixture."""

import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "codex_sse_watch.py"
spec = importlib.util.spec_from_file_location("codex_sse_watch", SCRIPT)
watch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(watch)


def event(rid, model="test-model", effort="high", raw=False):
    response = {"id": rid, "created_at": time.time(), "model": model, "reasoning": {"effort": effort}}
    data = json.dumps({"type": "response.completed", "response": response})
    return (("data: " if raw else "2026-10-08T12:00:00Z TRACE codex_api::sse::responses: SSE event: ") + data + "\n").encode()


class LogFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.logs = self.home / "sse-logs"
        self.logs.mkdir()
        (self.home / "config.toml").write_text('log_dir = "sse-logs"\nsqlite_home = "."\n')
        self.log = self.logs / "codex-tui.log"
        self.state = self.home / "state_5.sqlite"
        self.history = self.home / "thread_history_1.sqlite"
        with sqlite3.connect(self.state) as db:
            db.execute("CREATE TABLE threads (id TEXT PRIMARY KEY, title TEXT, name TEXT, rollout_path TEXT, updated_at INTEGER)")
        with sqlite3.connect(self.history) as db:
            db.execute("CREATE TABLE thread_turns (thread_id TEXT, turn_id TEXT, rollout_ordinal INTEGER)")
        self.add_thread("thread-a", "测试对话", "手动名称")
        self.turn("thread-a", "turn-a", 1)

    def add_thread(self, tid, title, name=None):
        rollout = self.home / (tid + ".jsonl")
        rollout.touch()
        with sqlite3.connect(self.state) as db:
            db.execute("INSERT INTO threads VALUES (?,?,?,?,?)", (tid, title, name, str(rollout), int(time.time()) + 86400))

    def turn(self, tid, turn, ordinal):
        with sqlite3.connect(self.history) as db:
            db.execute("INSERT INTO thread_turns VALUES (?,?,?)", (tid, turn, ordinal))

    def link(self, rid, tid="thread-a", turn="turn-a", root=None):
        payload = {"thread_id": tid, "turn_id": turn, "response_id": rid, "root_turn_id": root or turn}
        with (self.home / (tid + ".jsonl")).open("a") as out:
            out.write(json.dumps({"type": "token_usage_record", "payload": payload}) + "\n")

    def append(self, data, path=None):
        with (path or self.log).open("ab") as out:
            out.write(data)

    def summary(self, **kwargs):
        return watch.Summary(self.home, self.logs, **kwargs)

    def context(self, model="test-model", effort="high", turn="turn-a"):
        with (self.home / "thread-a.jsonl").open("a") as out:
            out.write(json.dumps({"type": "turn_context", "payload": {
                "turn_id": turn, "model": model, "effort": effort}}) + "\n")

    def sse(self, rid, typ="response.created", effort="high", model="test-model", tokens=None):
        response = {"id": rid, "model": model, "reasoning": {"effort": effort},
                    "created_at": time.time()}
        if tokens is not None:
            response["usage"] = {"output_tokens_details": {"reasoning_tokens": tokens}}
        return '2026-10-08T12:00:00Z TRACE codex_api::sse::responses: SSE event: ' + json.dumps(
            {"type": typ, "response": response})

    def desktop(self, text):
        return ('2026-10-08T12:00:00Z debug [AppServerConnection] Codex CLI stderr message=' +
                json.dumps(text) + '\n').encode()

    def structured(self, target, message, **fields):
        return json.dumps({"timestamp": "2026-10-08T07:45:00Z", "level": "TRACE",
                           "target": target, "fields": {"message": message, **fields}})

    def received(self, rid, typ="response.created"):
        payload = self.sse(rid, typ).split("SSE event: ", 1)[1]
        return self.structured("tungstenite::protocol", "Received message " + payload)

    def timing(self, rid):
        return self.structured("codex_api::responses_websocket_timing", "responses websocket timing",
                               model="must-not-be-used-as-returned-model",
                               payload=json.dumps({"type": "responsesapi.websocket_timing",
                                                   "timing_metrics": {"response_id": rid}}))
