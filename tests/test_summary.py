"""Response correlation, row projection, and cache behavior."""

import contextlib
import gc
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import threading
import unittest
from unittest import mock

from tests.support import LogFixture, SCRIPT, event, watch


class SummaryTests(LogFixture):
    def test_log_enumeration_checks_stop_for_non_logs_and_before_sorting(self):
        for stop_at in (2, "before-sort"):
            with self.subTest(stop_at=stop_at):
                summary = self.summary()
                summary.stop_requested = threading.Event()
                visited = []

                def entries(folder, pattern):
                    for index in range(1000 if stop_at == 2 else 2):
                        visited.append(index)
                        if len(visited) == stop_at:
                            summary.stop_requested.set()
                        yield self.logs / f"not-a-log-{index}.txt"
                    summary.stop_requested.set()

                with mock.patch.object(Path, "rglob", entries), \
                     mock.patch.object(watch, "sorted", side_effect=AssertionError("Sorted after stop"), create=True):
                    with self.assertRaises(watch.CollectionStopped):
                        summary.log_files()
                self.assertEqual(len(visited), 2)

    def test_cell_truncation_preserves_display_width_and_is_linear(self):
        cases = (("abcdef", 4, "abc…"), ("中文标题", 4, "中… "),
                 ("a\u0301bcde", 4, "a\u0301bc…"), ("\u0301\u0302abc", 1, "\u0301\u0302…"),
                 ("\u0301\u0302", 0, "\u0301\u0302"), ("中文", 4, "中文"),
                 ("abc", 0, "…"), ("a\nb", 4, "a b "))
        for value, size, expected in cases:
            with self.subTest(value=value, size=size):
                self.assertEqual(watch.cell(value, size, truncate=True), expected)
        title = "中a\u0301" * 1400
        combining = watch.unicodedata.combining
        calls = 0

        def count_combining(char):
            nonlocal calls
            calls += 1
            return combining(char)

        with mock.patch.object(watch.unicodedata, "combining", count_combining):
            self.assertEqual(watch.width(watch.cell(title, 42, truncate=True)), 42)
        self.assertLessEqual(calls, 2 * len(title) + 42)

    def test_turn_numbers_are_not_request_counts(self):
        self.turn("thread-a", "turn-b", 71)
        self.link("r1")
        self.link("r2")
        self.link("r3", turn="turn-b")
        self.append(event("r1") + event("r2", effort="xhigh") + event("r3", model="another-model"))
        summary = self.summary()
        self.assertEqual(summary.poll(), [
            ("手动名称", 1, "test-model", "high"),
            ("手动名称", 1, "test-model", "xhigh"),
            ("手动名称", 2, "another-model", "high"),
        ])
        self.assertEqual(summary.poll(), [])

    def test_delayed_rollout_and_history_projection(self):
        summary = self.summary()
        self.append(event("r1"))
        self.assertEqual(summary.poll(), [])
        self.link("r1", turn="delayed")
        self.assertEqual(summary.poll(), [])
        self.turn("thread-a", "delayed", 55)
        self.assertEqual(summary.poll(), [("手动名称", 2, "test-model", "high")])

    def test_rollout_can_precede_sse(self):
        self.link("r1")
        self.link("r2")
        self.append(event("r1"))
        summary = self.summary()
        self.assertEqual(len(summary.poll()), 1)
        self.append(event("r2"))
        self.assertEqual(len(summary.poll()), 1)

    def test_active_file_cursors_outlive_the_record_cache_limit(self):
        for index in range(4):
            self.append(b"ordinary log\n", self.logs / f"other-{index}.log")
            self.add_thread(f"thread-{index}", f"Thread {index}")
        self.append(event("unlinked"))
        summary = self.summary()
        remember = watch.remember
        # Shrink the captured default of remember(), rather than allocating
        # 20,001 real files just to exercise cursor eviction.
        with mock.patch.object(watch, "remember", side_effect=lambda cache, key, value, limit=3:
                               remember(cache, key, value, limit=limit)):
            summary.poll()
            self.assertEqual(len(summary.cursors), 5)
            self.assertEqual(len(summary.rollout_cursors), 5)
            with mock.patch.object(Path, "open", side_effect=AssertionError("Unchanged file reopened")):
                summary.poll()
                summary.poll()
        (self.logs / "other-0.log").unlink()
        with sqlite3.connect(self.state) as db:
            db.execute("DELETE FROM threads WHERE id='thread-0'")
        summary.poll()
        self.assertNotIn(str(self.logs / "other-0.log"), summary.cursors)
        self.assertNotIn(str(self.home / "thread-0.jsonl"), summary.rollout_cursors)

    def test_new_files_rotation_partial_lines_and_duplicates(self):
        self.link("r1")
        self.link("r2")
        self.link("r3")
        self.append(event("r1"))
        summary = self.summary()
        self.assertEqual(len(summary.poll()), 1)
        self.log.rename(self.logs / "codex-tui.log.1")
        self.append(event("r2")[:-1])
        self.assertEqual(summary.poll(), [])
        self.append(b"\n")
        self.assertEqual(len(summary.poll()), 1)
        self.append(event("r3", raw=True), self.logs / "new.jsonl")
        self.assertEqual(len(summary.poll()), 1)
        self.assertEqual(summary.poll(), [])

    def test_local_turn_id_and_thread_filter(self):
        self.add_thread("thread-child", "子对话")
        self.turn("thread-child", "child-turn", 100)
        self.link("child-response", "thread-child", "child-turn", root="turn-a")
        self.link("parent-response")
        self.append(event("parent-response") + event("child-response"))
        summary = self.summary(thread="thread-child")
        self.assertEqual(summary.poll(), [("子对话", 1, "test-model", "high")])
        self.assertEqual(len(summary.events), 0)

    def test_malformed_events_missing_fields_and_four_column_output(self):
        self.link("r1")
        self.append(b'data: [DONE]\ndata: []\ndata: null\ndata: {"type":"response.completed","response":null}\n')
        self.append(event("r1", model=None, effort=None))
        rows = self.summary().poll()
        self.assertEqual(rows, [("手动名称", 1, None, None)])
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            watch.show(rows[0], 42)
        self.assertEqual(output.getvalue().split(), ["手动名称", "1", "未返回", "未返回"])

    def test_database_is_read_only_and_missing_database_is_not_created(self):
        with contextlib.closing(watch.ro_database(self.state)) as db:
            with self.assertRaises(sqlite3.OperationalError):
                db.execute("DELETE FROM threads")
        missing = self.home / "absent.sqlite"
        with self.assertRaises(sqlite3.OperationalError):
            watch.ro_database(missing)
        self.assertFalse(missing.exists())
        alternate = self.home / "late-databases"
        alternate.mkdir()
        self.append(event("r1"))
        summary = watch.Summary(alternate, self.logs)
        self.assertEqual(summary.poll(), [])
        self.assertIn("等待 Codex 会话数据库", summary.status)
        self.assertEqual(list(alternate.iterdir()), [])


class DashboardTests(LogFixture):
    def test_desktop_chunks_created_completed_and_request_comparison(self):
        self.context()
        summary = self.summary()
        created = self.sse("r1")
        self.append(self.desktop(created[:90]))
        summary.poll()
        self.assertEqual(summary.rows, [])
        self.append(self.desktop(created[90:]))
        summary.poll()
        self.assertEqual(summary.rows[0]["first_effort"], "high")
        self.assertIsNone(summary.rows[0]["title"])
        self.link("r1")
        self.append(self.desktop(self.sse("r1", "response.completed", tokens=0)))
        summary.poll()
        row = summary.rows[0]
        self.assertEqual((row["status"], row["match"], row["reasoning_tokens"]), ("已完成", "match", 0))
        self.assertEqual((row["request_model"], row["request_effort"], row["turn"]), ("test-model", "high", 1))
        self.assertIsNone(row["http_status"])
        self.assertIsNone(row["elapsed_ms"])

    def test_missing_fields_not_filled_from_request_or_completed(self):
        self.context()
        self.link("r1")
        self.append(self.desktop(self.sse("r1", "response.completed", effort=None)))
        summary = self.summary()
        summary.poll()
        row = summary.rows[0]
        self.assertIsNone(row["first_effort"])
        self.assertIsNone(row["final_effort"])
        self.assertIsNone(row["match"])
        self.assertEqual(row["request_effort"], "high")

    def test_context_is_bound_per_response_even_in_same_turn(self):
        self.context(effort="high")
        self.link("r1")
        self.context(effort="xhigh")
        self.link("r2")
        self.append(self.desktop(self.sse("r1", "response.created") + '\n' +
                                 self.sse("r1", "response.completed") + '\n' +
                                 self.sse("r2", "response.created", effort="xhigh") + '\n' +
                                 self.sse("r2", "response.completed", effort="low")))
        summary = self.summary()
        summary.poll()
        rows = {r["response_id"]: r for r in summary.rows}
        self.assertEqual(rows["r1"]["request_effort"], "high")
        self.assertEqual(rows["r1"]["match"], "match")
        self.assertEqual(rows["r2"]["request_effort"], "xhigh")
        self.assertEqual(rows["r2"]["match"], "mismatch")

    def test_rollout_links_survive_an_overlapping_log_directory(self):
        self.context()
        self.link("r1")
        self.append(event("r1"))
        summary = watch.Summary(self.home, self.home)
        self.assertEqual(summary.poll(), [("手动名称", 1, "test-model", "high")])
        self.assertEqual(summary.rows[0]["request_effort"], "high")
        self.context(effort="xhigh")
        self.link("r2")
        self.append(event("r2", effort="xhigh"))
        self.assertEqual(summary.poll(), [("手动名称", 1, "test-model", "xhigh")])
        self.assertEqual(summary.rows[1]["request_effort"], "xhigh")
        self.assertEqual(summary.poll(), [])

    def test_terminal_status_survives_rotation_and_missing_database(self):
        self.append(self.desktop(self.sse("r1", "response.failed") + '\n' + self.sse("r1")))
        summary = watch.Summary(self.home / "absent", self.logs)
        summary.poll()
        self.assertEqual(summary.rows[0]["status"], "失败")
        self.assertIn("等待 Codex 会话数据库", summary.status)
        self.log.rename(self.logs / "codex-tui.log.1")
        self.append(self.desktop(self.sse("r1")))
        summary.poll()
        self.assertEqual(len(summary.rows), 1)
        self.assertEqual(summary.rows[0]["status"], "失败")

    def test_context_before_large_rollout_tail_is_read(self):
        self.context()
        with (self.home / "thread-a.jsonl").open("ab") as out:
            out.write(b' ' * (watch.WINDOW + 1) + b'\n')
        self.link("r1")
        self.append(event("r1"))
        summary = self.summary()
        summary.poll()
        self.assertEqual(summary.rows[0]["request_effort"], "high")

    def test_restart_reads_responses_and_created_events_before_tail_window(self):
        self.context()
        self.link("early")
        self.link("spanning")
        self.append(self.desktop(self.sse("early") + "\n" +
                                 self.sse("early", "response.completed") + "\n" +
                                 self.sse("spanning")))
        self.append(b"unrelated log entry\n" * 100)
        self.append(self.desktop(self.sse("spanning", "response.completed")))
        with mock.patch.object(watch, "WINDOW", 512):
            summary = self.summary()
            summary.poll()
        rows = {r["response_id"]: r for r in summary.rows}
        self.assertEqual(set(rows), {"early", "spanning"})
        self.assertTrue(all(r["first_effort"] == "high" and r["match"] == "match"
                            for r in rows.values()))

    def test_desktop_directory_and_bad_json_do_not_break_polling(self):
        desktop = self.home / "desktop"
        desktop.mkdir()
        (desktop / "app.log").write_bytes(b'Codex CLI stderr message=invalid\n' +
                                          self.desktop(self.sse("r1", "response.incomplete")))
        self.link("r1")
        summary = watch.Summary(self.home, self.logs, desktop_dir=desktop)
        summary.poll()
        self.assertEqual(summary.rows[0]["status"], "未完成")


class PerformanceTests(LogFixture):
    def test_initial_progress_reports_actual_files_bytes_and_phases(self):
        self.context()
        self.link("r1")
        self.link("r2")
        data = event("r1") + event("r2")
        self.append(data)
        observed = []
        summary = self.summary(progress=lambda status: observed.append(
            (status, len(summary.records), tuple(summary.rows))))
        with mock.patch.object(watch, "PROGRESS_INTERVAL", 0):
            summary.poll()
        reading = [entry for entry in observed if entry[0].startswith("正在读取日志")]
        self.assertIn("已处理 1/1 个文件", reading[-1][0])
        self.assertIn(f"已读取 {len(data):,} 字节", reading[-1][0])
        self.assertIn("已识别 2 个响应事件", reading[-1][0])
        self.assertEqual([entry[1] for entry in reading[:3]], [0, 1, 2])
        linking = [entry for entry in observed if entry[0].startswith("正在关联会话")]
        self.assertIn("已关联 2/2 条响应", linking[-1][0])
        rollout = self.home / "thread-a.jsonl"
        self.assertIn(f"已读取 {rollout.stat().st_size:,} 字节", linking[-1][0])
        self.assertTrue(any(status == "正在读取会话元数据" for status, _, _ in observed))
        self.assertEqual(observed[-1][0], "正在整理响应")
        self.assertTrue(all(rows == () for _, _, rows in observed))
        self.assertEqual(len(summary.rows), 2)
        # Publishing a progress update must not commit an early read cursor.
        self.assertEqual(summary.cursors[str(self.log)][1], len(data))
        self.assertEqual(summary.rollout_cursors[str(rollout)][1], rollout.stat().st_size)

    def test_progress_is_throttled_with_immediate_phase_boundaries(self):
        callback = mock.Mock()
        summary = self.summary(progress=callback)
        with mock.patch.object(watch.time, "monotonic", return_value=0):
            summary.report_progress("正在读取日志", files=0, total=10, byte_count=0)
            for index in range(10):
                summary.report_progress("正在读取日志", files=index, total=10, byte_count=index)
            self.assertEqual(callback.call_count, 1)
            summary.report_progress("正在关联会话")
            self.assertEqual(callback.call_count, 2)
        with mock.patch.object(watch.time, "monotonic", return_value=watch.PROGRESS_INTERVAL):
            summary.report_progress("正在关联会话", files=1, total=1, byte_count=123)
        self.assertEqual(callback.call_count, 3)
        summary.progress = None
        with mock.patch.object(watch.time, "monotonic", side_effect=AssertionError("Idle progress work")):
            summary.report_progress("正在读取日志")
        self.assertEqual(callback.call_count, 3)

    def test_idle_poll_reuses_rows_without_opening_files_or_databases(self):
        self.context()
        self.link("r1")
        self.append(event("r1") + self.desktop(self.sse("unlinked", "response.failed")))
        summary = self.summary()
        summary.poll()
        rows = summary.rows
        with mock.patch.object(Path, "open", side_effect=AssertionError("Unchanged file reopened")), \
             mock.patch.object(watch, "ro_database", side_effect=AssertionError("Idle database query")):
            self.assertEqual(summary.poll(), [])
            self.assertEqual(summary.poll(), [])
        self.assertIs(summary.rows, rows)

    def test_late_rollout_link_is_detected_without_database_or_log_changes(self):
        self.append(event("r1"))
        summary = self.summary()
        summary.poll()
        summary.poll()
        self.assertIsNone(summary.rows[0]["title"])
        self.context()
        self.link("r1")
        self.assertEqual(summary.poll(), [("手动名称", 1, "test-model", "high")])
        self.assertEqual(summary.rows[0]["request_effort"], "high")

    def test_future_rollout_links_cannot_evict_a_retained_response(self):
        self.context()
        self.link("kept-response")
        self.append(event("kept-response"))
        with (self.home / "thread-a.jsonl").open("a") as stream:
            for index in range(watch.CACHE_LIMIT):
                stream.write(json.dumps({"type": "token_usage_record", "payload": {
                    "thread_id": "thread-a", "turn_id": "turn-a", "response_id": f"future-{index}"}}) + "\n")
        summary = self.summary()
        self.assertEqual(summary.poll(), [("手动名称", 1, "test-model", "high")])
        self.assertEqual(summary.rows[0]["request_effort"], "high")
        self.assertLessEqual(len(summary.links), watch.CACHE_LIMIT)
        self.assertEqual(set(summary.links), set(summary.requests))
        self.assertEqual(summary.poll(), [])
        self.assertEqual(summary.rows[0]["turn"], 1)
        # The newest speculative link must still work when its SSE arrives later.
        latest = f"future-{watch.CACHE_LIMIT - 1}"
        self.append(event(latest))
        self.assertEqual(summary.poll(), [("手动名称", 1, "test-model", "high")])
        self.assertNotIn(latest, summary.unobserved_links)

    def test_evicted_future_links_restore_original_contexts_without_replay(self):
        self.add_thread("unrelated", "Unrelated")
        self.link("unrelated-response", tid="unrelated")
        self.context(model="early-model", effort="high")
        self.link("known")
        self.link("delayed-early")
        self.context(model="later-model", effort="xhigh")
        self.link("delayed-late")
        for index in range(4):
            self.link(f"future-{index}")
        self.append(event("known") + event("absent"))
        summary = self.summary()
        with mock.patch.object(watch, "CACHE_LIMIT", 4):
            summary.poll()
            self.assertNotIn("delayed-early", summary.links)
            self.assertNotIn("delayed-late", summary.links)
            self.append(event("delayed-early") + event("delayed-late"))
            opened = []
            original_open = Path.open

            def track_open(path, *args, **kwargs):
                opened.append(path)
                return original_open(path, *args, **kwargs)

            with mock.patch.object(Path, "open", track_open):
                summary.poll()
            self.assertNotIn(self.home / "thread-a.jsonl", opened)
            self.assertNotIn(self.home / "unrelated.jsonl", opened)
            rows = {row["response_id"]: row for row in summary.rows}
            self.assertEqual(rows["delayed-early"]["request_model"], "early-model")
            self.assertEqual(rows["delayed-early"]["request_effort"], "high")
            self.assertEqual(rows["delayed-late"]["request_model"], "later-model")
            self.assertEqual(rows["delayed-late"]["request_effort"], "xhigh")
            with mock.patch.object(Path, "open", side_effect=AssertionError("Idle file reopened")):
                summary.poll()
                summary.poll()
        self.assertLessEqual(len(summary.links), 4)
        self.assertEqual(set(summary.links), set(summary.requests))

    def test_large_history_and_continuous_new_responses_never_replay_rollouts(self):
        self.context(model="source-model")
        self.link("known")
        self.link("delayed")
        rollout = self.home / "thread-a.jsonl"
        with rollout.open("a") as stream:
            for index in range(20_002):
                stream.write(json.dumps({"type": "token_usage_record", "payload": {
                    "thread_id": "thread-a", "turn_id": "turn-a", "response_id": f"future-{index}"}}) + "\n")
        self.append(event("known"))
        summary = self.summary()
        self.addCleanup(summary.close)
        summary.poll()
        self.assertNotIn("delayed", summary.links)
        self.assertEqual(len(summary.links), watch.CACHE_LIMIT)
        self.assertIsNotNone(summary.link_archive)
        original_open = Path.open
        opened = []

        def track_open(path, *args, **kwargs):
            opened.append(path)
            return original_open(path, *args, **kwargs)

        for index in range(3):
            self.append(self.desktop(self.sse(f"new-{index}", "response.created")))
            with mock.patch.object(Path, "open", track_open):
                summary.poll()
            self.assertNotIn(rollout, opened)
        self.append(event("delayed"))
        with mock.patch.object(Path, "open", track_open):
            summary.poll()
        self.assertNotIn(rollout, opened)
        rows = {row["response_id"]: row for row in summary.rows}
        self.assertEqual(rows["delayed"]["request_model"], "source-model")
        self.assertIsNone(rows["new-0"]["thread_id"])
        self.assertEqual(list(summary.link_archive.find({"delayed"})), [])
        with mock.patch.object(Path, "open", side_effect=AssertionError("Idle file reopened")):
            for _ in range(20):
                summary.poll()
        self.context(model="appended-model", effort="low")
        self.link("new-0")
        summary.poll()
        rows = {row["response_id"]: row for row in summary.rows}
        self.assertEqual(rows["new-0"]["request_model"], "appended-model")
        self.assertEqual(rows["new-0"]["request_effort"], "low")
        self.assertLessEqual(len(summary.link_sources), watch.CACHE_LIMIT)
        self.assertEqual(sum(map(len, summary.source_links.values())), len(summary.link_sources))

    def test_archived_links_invalidate_on_rewrite_truncate_removal_and_reentry(self):
        rollout = self.home / "thread-a.jsonl"
        with mock.patch.object(watch, "CACHE_LIMIT", 3):
            for change in ("rewrite", "truncate", "remove", "leave-query"):
                with self.subTest(change=change):
                    rollout.write_text("")
                    self.log.write_bytes(b"")
                    self.context(model="old-model")
                    self.link("known")
                    self.link("old")
                    for index in range(4):
                        self.link(f"future-{index}")
                    self.append(event("known") + event("absent"))
                    summary = self.summary()
                    self.addCleanup(summary.close)
                    summary.poll()
                    self.assertEqual(len(list(summary.link_archive.find({"old"}))), 1)
                    if change == "rewrite":
                        replacement = rollout.with_suffix(".new")
                        replacement.write_text("")
                        os.replace(replacement, rollout)
                    elif change == "truncate":
                        rollout.write_text("")
                    elif change == "remove":
                        rollout.unlink()
                    else:
                        with sqlite3.connect(self.state) as db:
                            db.execute("UPDATE threads SET rollout_path=NULL WHERE id='thread-a'")
                    summary.poll()
                    self.assertEqual(list(summary.link_archive.find({"old"})), [])
                    self.assertNotIn(str(rollout), summary.source_links)
                    self.assertNotIn("thread-a", summary.context_keys)
                    self.assertEqual(summary.requests["known"]["request_model"], "old-model")
                    if change in ("remove", "leave-query"):
                        self.assertNotIn(str(rollout), summary.rollout_cursors)
                    rollout.write_text("")
                    self.context(model="new-model", effort="low")
                    self.link("reentered")
                    with sqlite3.connect(self.state) as db:
                        db.execute("UPDATE threads SET rollout_path=? WHERE id='thread-a'", (str(rollout),))
                    self.append(event("reentered"))
                    summary.poll()
                    self.assertEqual(list(summary.link_archive.find({"old"})), [])
                    self.assertEqual(summary.requests["reentered"]["request_model"], "new-model")

    def test_removed_rollout_query_releases_state_even_when_all_responses_are_linked(self):
        self.context()
        self.link("known")
        for index in range(4):
            self.link(f"future-{index}")
        self.append(event("known"))
        summary = self.summary()
        self.addCleanup(summary.close)
        with mock.patch.object(watch, "CACHE_LIMIT", 3):
            summary.poll()
        with sqlite3.connect(self.state) as db:
            db.execute("UPDATE threads SET rollout_path=NULL WHERE id='thread-a'")
        summary.poll()
        self.assertEqual(summary.rollout_cursors, {})
        self.assertEqual(summary.link_archive.execute("SELECT COUNT(*) FROM links").fetchone()[0], 0)
        self.assertEqual(summary.link_sources, {})
        self.assertEqual(summary.source_links, {})
        self.assertEqual(summary.contexts, {})
        self.assertIn("known", summary.links)

    def test_evicted_responses_release_their_protected_link_cache_slots(self):
        summary = self.summary()
        with mock.patch.object(watch, "RESPONSE_LIMIT", 2), mock.patch.object(watch, "CACHE_LIMIT", 4):
            for rid in ("old", "kept", "new"):
                self.context()
                self.link(rid)
                self.append(event(rid))
                summary.poll()
            self.assertEqual(set(summary.records), {"kept", "new"})
            self.assertIn("old", summary.unobserved_links)
            for index in range(4):
                summary.remember_link(f"future-{index}", ("thread-a", "turn-a"), {})
            self.assertNotIn("old", summary.links)
            self.assertEqual(set(summary.links), {"kept", "new", "future-2", "future-3"})
            self.assertEqual(set(summary.links), set(summary.requests))
            self.assertEqual(set(summary.unobserved_links), {"future-2", "future-3"})

    def test_rollout_reset_cannot_fill_early_requests_from_later_context(self):
        self.link("early")
        self.context(model="late-model", effort="xhigh")
        self.link("late")
        self.append(event("early") + event("late") + event("still-pending"))
        rollout = self.home / "thread-a.jsonl"
        original = rollout.read_bytes()
        for change in ("touch", "replace", "truncate"):
            with self.subTest(change=change):
                rollout.write_bytes(original)
                summary = self.summary()
                summary.poll()
                early = next(row for row in summary.rows if row["response_id"] == "early")
                self.assertIsNone(early["request_model"])
                self.assertIsNone(early["request_effort"])
                if change == "touch":
                    stat = rollout.stat()
                    os.utime(rollout, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
                elif change == "replace":
                    replacement = rollout.with_suffix(".new")
                    replacement.write_bytes(original)
                    os.replace(replacement, rollout)
                else:
                    rollout.write_bytes(original.splitlines(keepends=True)[0])
                summary.poll()
                early = next(row for row in summary.rows if row["response_id"] == "early")
                self.assertIsNone(early["request_model"])
                self.assertIsNone(early["request_effort"])

    def test_normal_rollout_append_retains_its_preceding_context(self):
        self.context(model="preceding-model", effort="high")
        self.link("first")
        self.append(event("first") + event("still-pending"))
        summary = self.summary()
        summary.poll()
        self.link("second")
        self.append(event("second"))
        summary.poll()
        second = next(row for row in summary.rows if row["response_id"] == "second")
        self.assertEqual((second["request_model"], second["request_effort"]), ("preceding-model", "high"))

    def test_cached_metadata_refreshes_from_wal_without_new_response(self):
        self.context()
        self.link("r1")
        self.append(event("r1"))
        with contextlib.closing(sqlite3.connect(self.state)) as state, \
             contextlib.closing(sqlite3.connect(self.history)) as history:
            state.execute("PRAGMA journal_mode=WAL")
            history.execute("PRAGMA journal_mode=WAL")
            summary = self.summary()
            summary.poll()
            previous = [(p.stat().st_size, p.stat().st_mtime_ns) for p in (self.state, self.history)]
            state.execute("UPDATE threads SET name='新名称' WHERE id='thread-a'")
            state.commit()
            history.execute("INSERT INTO thread_turns VALUES ('thread-a','earlier-turn',0)")
            history.commit()
            self.assertEqual(previous, [(p.stat().st_size, p.stat().st_mtime_ns) for p in (self.state, self.history)])
            summary.poll()
            self.assertEqual((summary.rows[0]["title"], summary.rows[0]["turn"]), ("新名称", 2))

    def test_response_cache_evicts_oldest_records_at_limit(self):
        for rid in ("r1", "r2", "r3", "r4"):
            self.link(rid)
            self.append(event(rid))
        with mock.patch.object(watch, "RESPONSE_LIMIT", 3):
            summary = self.summary()
            summary.poll()
        self.assertEqual(list(summary.records), ["r2", "r3", "r4"])
        self.assertEqual([r["response_id"] for r in summary.rows], ["r2", "r3", "r4"])

    def test_transient_database_failure_does_not_lose_consumed_response(self):
        self.context()
        self.link("r1")
        self.append(event("r1"))
        summary = self.summary()
        error = sqlite3.OperationalError("database is locked")
        error.sqlite_errorcode = sqlite3.SQLITE_BUSY
        with mock.patch.object(watch, "ro_database", side_effect=error):
            self.assertEqual(watch.poll_safely(summary), [])
        self.assertIn("等待会话数据库可读", summary.status)
        self.assertEqual(watch.poll_safely(summary), [("手动名称", 1, "test-model", "high")])
        self.assertEqual(summary.status, "")

    def test_idle_database_recovery_clears_status_without_rebuilding_rows(self):
        self.context()
        self.link("r1")
        self.append(event("r1"))
        summary = self.summary()
        summary.poll()
        rows = summary.rows
        with sqlite3.connect(self.state) as state:
            state.execute("PRAGMA user_version=1")
        error = sqlite3.OperationalError("database is locked")
        error.sqlite_errorcode = sqlite3.SQLITE_BUSY
        with mock.patch.object(watch, "ro_database", side_effect=error):
            self.assertEqual(watch.poll_safely(summary), [])
        self.assertIn("等待会话数据库可读", summary.status)
        self.assertEqual(watch.poll_safely(summary), [])
        self.assertEqual(summary.status, "")
        self.assertIs(summary.rows, rows)
        with mock.patch.object(watch, "ro_database", side_effect=AssertionError("Recovered idle query")):
            self.assertEqual(watch.poll_safely(summary), [])

    def test_only_changed_response_is_rebuilt(self):
        self.context()
        for rid in ("r1", "r2", "r3"):
            self.link(rid)
            self.append(self.desktop(self.sse(rid)))
        summary = self.summary()
        summary.poll()
        before = {row["response_id"]: row for row in summary.rows}
        self.append(self.desktop(self.sse("r2", "response.completed", effort="max", tokens=0)))
        summary.poll()
        after = {row["response_id"]: row for row in summary.rows}
        self.assertIs(after["r1"], before["r1"])
        self.assertIs(after["r3"], before["r3"])
        self.assertIsNot(after["r2"], before["r2"])
        self.assertEqual((after["r2"]["first_effort"], after["r2"]["final_effort"], after["r2"]["reasoning_tokens"]), ("high", "max", 0))

    def test_eviction_does_not_reload_unchanged_metadata(self):
        for index in range(3):
            turn = "turn-" + str(index)
            self.turn("thread-a", turn, index + 2)
            self.link("r" + str(index), turn=turn)
            self.append(event("r" + str(index)))
        with mock.patch.object(watch, "RESPONSE_LIMIT", 3):
            summary = self.summary()
            summary.poll()
            self.append(event("new-unlinked"))
            with mock.patch.object(watch, "ro_database", side_effect=AssertionError("Unchanged metadata queried during eviction")):
                summary.poll()
        self.assertEqual(set(summary.row_cache), {"r1", "r2", "new-unlinked"})
        self.assertNotIn(("thread-a", "turn-0"), summary.metadata)

    def test_irrelevant_database_write_preserves_rows(self):
        self.link("r1")
        self.append(event("r1"))
        summary = self.summary()
        summary.poll()
        before = summary.rows
        with sqlite3.connect(self.state) as db:
            db.execute("CREATE TABLE irrelevant (value TEXT)")
        self.assertEqual(summary.poll(), [])
        self.assertIs(summary.rows, before)

    def test_metadata_batches_keep_turn_numbers_separate_by_thread(self):
        self.add_thread("thread-b", "另一对话")
        self.turn("thread-b", "turn-b", 1)
        self.turn("thread-a", "turn-a2", 2)
        self.link("r1", turn="turn-a2")
        self.link("r2", tid="thread-b", turn="turn-b")
        self.append(event("r1") + event("r2"))
        summary = self.summary()
        summary.poll()
        self.assertEqual([(row["thread_id"], row["turn"]) for row in summary.rows], [("thread-a", 2), ("thread-b", 1)])


class LinkArchiveTests(LogFixture):
    def archived_summary(self):
        summary = self.summary()
        source = str(self.home / "thread-a.jsonl")
        with mock.patch.object(watch, "CACHE_LIMIT", 1):
            summary.remember_link("early", ("thread-a", "turn-a"), {"request_model": "early-model"}, source)
            summary.remember_link("later", ("thread-a", "turn-a"), {"request_model": "late-model"}, source)
        summary.link_archive.flush()
        return summary

    def test_archive_is_lazy_private_and_released_by_close_and_collection(self):
        self.assertIsNone(self.summary().link_archive)
        for cleanup in ("close", "collect"):
            with self.subTest(cleanup=cleanup):
                summary = self.archived_summary()
                path = summary.link_archive.path
                db = summary.link_archive.db
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
                self.assertEqual(path.parent.stat().st_mode & 0o777, 0o700)
                self.assertEqual(db.execute("PRAGMA cache_size").fetchone()[0], -1024)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM links").fetchone()[0], 1)
                if cleanup == "close":
                    summary.close()
                    summary.close()
                else:
                    del summary
                    gc.collect()
                self.assertFalse(path.parent.exists())
                with self.assertRaises(sqlite3.ProgrammingError):
                    db.execute("SELECT 1")

    def test_archive_creation_failure_removes_private_directory(self):
        created = []
        original_directory = watch.tempfile.TemporaryDirectory

        def directory(*args, **kwargs):
            temporary = original_directory(*args, dir=self.home, **kwargs)
            created.append(Path(temporary.name))
            return temporary

        with mock.patch.object(watch.tempfile, "TemporaryDirectory", directory), \
             mock.patch.object(watch.sqlite3, "connect", side_effect=sqlite3.OperationalError("synthetic open failure")):
            with self.assertRaisesRegex(OSError, "synthetic open failure"):
                watch.LinkArchive()
        self.assertTrue(created)
        self.assertTrue(all(not path.exists() for path in created))

    def test_archive_write_failure_discards_index_and_allows_exact_recovery(self):
        self.context(model="original")
        for rid in ("known", "early", "later", "overflow"):
            self.link(rid)
        self.append(event("known"))
        summary = self.summary()
        self.addCleanup(summary.close)
        paths = []

        def fail_write(archive, *args):
            paths.append(archive.path)
            raise watch.LinkArchiveError("synthetic disk full")

        with mock.patch.object(watch, "CACHE_LIMIT", 3), \
             mock.patch.object(watch.LinkArchive, "put", fail_write):
            with self.assertRaisesRegex(watch.LinkArchiveError, "synthetic disk full"):
                summary.poll()
        self.assertEqual(summary.rollout_cursors, {})
        self.assertIsNone(summary.link_archive)
        self.assertEqual(summary.unobserved_links, {})
        self.assertEqual(summary.contexts, {})
        self.assertEqual(summary.requests["known"]["request_model"], "original")
        self.assertTrue(paths)
        self.assertTrue(all(not path.parent.exists() for path in paths))
        self.append(event("early"))
        summary.poll()
        self.assertEqual(summary.requests["early"]["request_model"], "original")

    def test_sqlite_full_rolls_back_prior_evictions_and_rebuilds_every_source(self):
        self.add_thread("thread-b", "Other source")
        with sqlite3.connect(self.state) as db:
            db.execute("UPDATE threads SET updated_at=updated_at+10 WHERE id='thread-a'")
        original_model = "original-" + "x" * 600
        self.context(model=original_model, effort="high")
        for rid in ("a0", "a1", "a2"):
            self.link(rid)
        for rid in ("b0", "b1", "b2"):
            self.link(rid, tid="thread-b")
        self.append(event("pending"))
        summary = self.summary()
        self.addCleanup(summary.close)
        connect, put = sqlite3.connect, watch.LinkArchive.put
        inserted, rollbacks, paths = [], [], []

        def limited_connect(path, *args, **kwargs):
            if str(path).endswith("links.sqlite"):
                db = connect(":memory:", *args, **kwargs)
                db.execute("PRAGMA page_size=512")
                db.execute("PRAGMA max_page_count=8")
                return db
            return connect(path, *args, **kwargs)

        def observed_put(archive, rid, *args):
            paths.append(archive.path)
            self.assertIn(str(self.home / "thread-a.jsonl"), summary.rollout_cursors)
            try:
                put(archive, rid, *args)
            except watch.LinkArchiveError as error:
                self.assertEqual(error.__cause__.sqlite_errorcode, sqlite3.SQLITE_FULL)
                rollbacks.append((archive.db.in_transaction,
                                  archive.db.execute("SELECT COUNT(*) FROM links").fetchone()[0]))
                self.assertNotIn("a0", summary.links)
                raise
            inserted.append(rid)

        with mock.patch.object(watch, "CACHE_LIMIT", 3), \
             mock.patch.object(watch.sqlite3, "connect", limited_connect), \
             mock.patch.object(watch.LinkArchive, "put", observed_put):
            with self.assertRaises(watch.LinkArchiveError):
                summary.poll()
        self.assertIn("a0", inserted)
        self.assertEqual(rollbacks, [(False, 0)])
        self.assertEqual(summary.rollout_cursors, {})
        self.assertEqual(summary.links, {})
        self.assertIsNone(summary.link_archive)
        self.assertTrue(all(not path.parent.exists() for path in paths))
        self.append(event("a0"))
        summary.poll()
        self.assertEqual(summary.links["a0"], ("thread-a", "turn-a"))
        self.assertEqual(summary.requests["a0"], {"request_model": original_model, "request_effort": "high"})

    def test_commit_failure_cannot_restore_deleted_source_bindings(self):
        self.context(model="old-model")
        for rid in ("known", "deleted", "later-1", "later-2"):
            self.link(rid)
        self.append(event("known") + event("pending"))
        summary = self.summary()
        self.addCleanup(summary.close)
        with mock.patch.object(watch, "CACHE_LIMIT", 3):
            summary.poll()
        archive = summary.link_archive
        path = archive.path
        self.assertEqual(len(list(archive.find({"deleted"}))), 1)
        archive.execute("PRAGMA busy_timeout=0")
        with contextlib.closing(sqlite3.connect(path)) as reader:
            reader.execute("BEGIN")
            reader.execute("SELECT * FROM links").fetchall()
            (self.home / "thread-a.jsonl").write_text("")
            with self.assertRaises(watch.LinkArchiveError) as failure:
                summary.poll()
            self.assertEqual(failure.exception.__cause__.sqlite_errorcode, sqlite3.SQLITE_BUSY)
            self.assertIn("提交失败", str(failure.exception))
            reader.rollback()
        self.assertFalse(path.parent.exists())
        self.assertIsNone(summary.link_archive)
        self.assertEqual(summary.rollout_cursors, {})
        self.assertEqual(summary.unobserved_links, {})
        self.assertEqual(summary.requests["known"]["request_model"], "old-model")
        self.append(event("deleted"))
        summary.poll()
        self.assertNotIn("deleted", summary.links)
        deleted = next(row for row in summary.rows if row["response_id"] == "deleted")
        self.assertIsNone(deleted["request_model"])

    def test_archive_sqlite_errors_are_visible_and_cleanup_still_runs(self):
        summary = self.archived_summary()
        archive = summary.link_archive
        path = archive.path
        with mock.patch.object(archive, "db", wraps=archive.db) as database:
            database.execute.side_effect = sqlite3.OperationalError("synthetic write failure")
            with self.assertRaisesRegex(OSError, "synthetic write failure"):
                archive.put("another", "source", ("thread", "turn"), {})
        summary.close()
        self.assertFalse(path.parent.exists())

    def test_normal_append_overrides_an_older_archived_binding(self):
        self.context(model="early-model")
        self.link("known")
        self.link("delayed")
        for index in range(4):
            self.link(f"future-{index}")
        self.append(event("known"))
        summary = self.summary()
        self.addCleanup(summary.close)
        with mock.patch.object(watch, "CACHE_LIMIT", 3):
            summary.poll()
            self.assertEqual(len(list(summary.link_archive.find({"delayed"}))), 1)
            self.context(model="new-model", effort="low")
            self.link("delayed")
            self.append(event("delayed"))
            summary.poll()
        self.assertEqual(summary.requests["delayed"], {"request_model": "new-model", "request_effort": "low"})
        self.assertEqual(list(summary.link_archive.find({"delayed"})), [])

    def test_cli_and_precollector_failures_close_the_archive(self):
        for mode in ("once", "poll-error", "startup-error"):
            with self.subTest(mode=mode):
                summary = self.archived_summary()
                path = summary.link_archive.path
                with contextlib.redirect_stdout(io.StringIO()):
                    if mode == "startup-error":
                        with mock.patch.object(watch, "diagnostics", side_effect=OSError("synthetic startup failure")):
                            with self.assertRaisesRegex(OSError, "synthetic startup failure"):
                                watch.serve(summary, 0, 20, False)
                    else:
                        flags = [str(SCRIPT), "--home", str(self.home), "--once", "--json"]
                        with mock.patch.object(watch, "Summary", return_value=summary), \
                             mock.patch.object(sys, "argv", flags):
                            if mode == "poll-error":
                                with mock.patch.object(summary, "poll", side_effect=ValueError("synthetic poll failure")):
                                    with self.assertRaisesRegex(ValueError, "synthetic poll failure"):
                                        watch.main()
                            else:
                                watch.main()
                self.assertFalse(path.parent.exists())

    def test_slow_collector_owns_archive_cleanup_after_stop_or_failure(self):
        from tests.test_http import ServerResponsivenessTests

        for fail in (False, True):
            with self.subTest(fail=fail):
                summary = self.summary()
                entered, release, closed = threading.Event(), threading.Event(), threading.Event()
                owner_ids, close_ids, paths = [], [], []
                close = summary.close

                def slow_poll():
                    owner_ids.append(threading.get_ident())
                    summary.link_archive = watch.LinkArchive()
                    summary.link_archive.put("synthetic", "source", ("thread", "turn"), {})
                    summary.link_archive.flush()
                    paths.append(summary.link_archive.path)
                    entered.set()
                    release.wait(5)
                    # HTTP stopping cannot close a connection still in use here.
                    self.assertEqual(len(list(summary.link_archive.find({"synthetic"}))), 1)
                    if fail:
                        raise ValueError("synthetic collector failure")
                    return []

                def tracked_close():
                    close_ids.append(threading.get_ident())
                    try:
                        close()
                    finally:
                        closed.set()

                summary.poll, summary.close = slow_poll, tracked_close
                expected = "synthetic collector failure" if fail else None
                thread, _ = ServerResponsivenessTests.start_server(self, summary, expected_error=expected)
                try:
                    self.assertTrue(entered.wait(2))
                    if not fail:
                        watch.control(self.port, "stop")
                        thread.join(1)
                        self.assertFalse(thread.is_alive())
                        self.assertFalse(closed.is_set())
                        self.assertTrue(paths[0].exists())
                    release.set()
                    self.assertTrue(closed.wait(2))
                    thread.join(2)
                    self.assertFalse(thread.is_alive())
                    self.assertEqual(close_ids, owner_ids)
                    self.assertFalse(paths[0].parent.exists())
                finally:
                    release.set()
                    if thread.is_alive():
                        watch.control(self.port, "stop")
                    thread.join(2)

    def test_web_process_stop_cancels_scan_and_cleans_archive_before_exit(self):
        import subprocess
        import textwrap

        self.context()
        for rid in ("known", *(f"future-{index}" for index in range(8))):
            self.link(rid)
        self.append(event("known"))
        script = textwrap.dedent(r'''
            import sys
            import threading
            import time
            from pathlib import Path
            from tests.support import watch

            home = Path(sys.argv[1])
            watch.CACHE_LIMIT = 3
            watch.control_path = lambda port: home / f"control-{port}.json"
            summary = watch.Summary(home, home / "sse-logs")
            read_appended, close = watch.read_appended, summary.close
            http_returned = threading.Event()
            owner = None

            def gated_read(path, cursors, **kwargs):
                global owner
                for line in read_appended(path, cursors, **kwargs):
                    if cursors is summary.rollout_cursors and summary.link_archive is not None and owner is None:
                        owner = threading.get_ident()
                        assert not threading.current_thread().daemon
                        print("ARCHIVE_READY=" + str(summary.link_archive.path), flush=True)
                        while not summary.stop_requested.wait(0.01):
                            pass
                        assert http_returned.wait(3), "HTTP stop waited for the collector"
                    yield line

            def checked_close():
                assert threading.get_ident() == owner, "archive closed outside its owner"
                path = summary.link_archive.path
                close()
                assert not path.parent.exists()
                print("COLLECTOR_CLOSED", flush=True)

            watch.read_appended, summary.close = gated_read, checked_close
            watch.serve(summary, 0, 20, False)
            print("HTTP_RETURNED", flush=True)
            http_returned.set()
        ''')
        child = subprocess.Popen([sys.executable, "-B", "-c", script, str(self.home)],
                                 cwd=SCRIPT.parent, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            url = child.stdout.readline().split()[0].removeprefix("实时表格：")
            port = int(url.rsplit(":", 1)[1])
            archive_line = child.stdout.readline().strip()
            self.assertTrue(archive_line.startswith("ARCHIVE_READY="), archive_line)
            archive_path = Path(archive_line.removeprefix("ARCHIVE_READY="))
            self.assertTrue(archive_path.exists())
            with mock.patch.object(watch, "control_path", side_effect=lambda value: self.home / f"control-{value}.json"):
                watch.control(port, "stop", timeout=1)
            output, errors = child.communicate(timeout=5)
            self.assertEqual(child.returncode, 0, errors)
            self.assertEqual(errors, "")
            self.assertEqual(output.splitlines(), ["HTTP_RETURNED", "COLLECTOR_CLOSED"])
            self.assertFalse(archive_path.parent.exists())
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
