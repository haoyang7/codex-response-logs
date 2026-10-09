"""Event parsing, timestamps, log tails, and parser regressions."""

import collections
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from tests.support import LogFixture, event, watch


class WebSocketTests(LogFixture):
    def test_json_wrapped_websocket_chunks_and_multiple_records(self):
        self.context()
        self.link("r1")
        raw = self.received("r1") + "\n" + self.received("r1", "response.completed")
        self.append(self.desktop(raw[:137]))
        summary = self.summary()
        summary.poll()
        self.assertEqual(summary.rows, [])
        self.append(self.desktop(raw[137:]))
        summary.poll()
        self.assertEqual(len(summary.rows), 1)
        row = summary.rows[0]
        self.assertEqual((row["response_model"], row["first_effort"], row["final_effort"], row["match"]),
                         ("test-model", "high", "high", "match"))

    def test_timing_is_partial_evidence_and_late_timing_cannot_regress_response(self):
        self.context()
        self.link("r1")
        self.append(self.desktop(self.timing("r1")))
        summary = self.summary()
        summary.poll()
        row = summary.rows[0]
        self.assertEqual(row["status"], "仅耗时日志")
        self.assertEqual(row["request_model"], "test-model")
        for key in ("response_model", "first_effort", "final_effort", "http_status", "elapsed_ms", "match"):
            self.assertIsNone(row[key], key)
        self.append(self.desktop(self.received("r1") + "\n" + self.timing("r1")))
        summary.poll()
        self.assertEqual(summary.rows[0]["status"], "响应中")
        self.append(self.desktop(self.received("r1", "response.completed") + "\n" + self.timing("r1")))
        summary.poll()
        self.assertEqual(len(summary.rows), 1)
        self.assertEqual(summary.rows[0]["status"], "已完成")
        self.assertEqual(summary.rows[0]["match"], "match")

    def test_old_timing_record_does_not_claim_current_tracing_is_disabled(self):
        self.append(self.desktop(self.timing("old") + "\n" +
                                 self.received("new", "response.completed")))
        summary = self.summary()
        summary.poll()
        self.assertIn("无法补采", summary.status)
        self.assertNotIn("需额外开启", summary.status)

    def test_desktop_message_can_be_an_unquoted_json_object(self):
        self.link("r1")
        self.append(("2026-10-08T07:45:00Z debug [AppServerConnection] Codex CLI stderr message="
                     + self.received("r1", "response.completed") + "\n").encode())
        summary = self.summary()
        summary.poll()
        self.assertEqual(summary.rows[0]["response_model"], "test-model")
        self.assertEqual(summary.rows[0]["final_effort"], "high")

    def test_unquoted_desktop_continuation_preserves_websocket_events(self):
        self.context()
        self.link("r1")
        summary = self.summary()
        for typ in ("response.created", "response.completed"):
            payload = json.dumps({"type": typ, "response": {
                "id": "r1", "model": "test-model", "reasoning": {"effort": "high"}}},
                separators=(',', ':'))
            record = json.dumps({"timestamp": "2026-10-08T07:45:00Z",
                                 "target": "tungstenite::protocol",
                                 "fields": {"message": "Received message " + payload}}, separators=(',', ':'))
            boundary = record.index(r'\"id\"')
            self.append(self.desktop(record[:boundary]))
            summary.poll()
            self.append(("2026-10-08T07:45:00Z debug [AppServerConnection] Codex CLI stderr message="
                         + record[boundary:] + "\n").encode())
            summary.poll()
        self.assertEqual(len(summary.rows), 1)
        row = summary.rows[0]
        self.assertEqual((row["transport"], row["status"], row["first_effort"], row["final_effort"]),
                         ("WebSocket", "已完成", "high", "high"))
        self.assertEqual(summary.json_fragments, {})

    def test_native_desktop_error_does_not_hide_websocket_events(self):
        error = json.dumps(json.dumps({"error": {"message": "synthetic failure",
                                               "metadata": json.loads(self.received("injected"))}}))
        for level in ("debug", "info", "warning", "error"):
            with self.subTest(level=level):
                self.log.write_bytes((f"2026-10-08T07:45:00Z {level} [electron-message-handler] "
                                      f"request_failed errorMessage={error}\n").encode())
                self.append(self.desktop(self.received("real") + "\n" +
                                         self.received("real", "response.completed")))
                summary = self.summary()
                summary.poll()
                self.assertEqual([row["response_id"] for row in summary.rows], ["real"])
                self.assertEqual(summary.rows[0]["status"], "已完成")
                self.assertEqual(summary.json_fragments, {})

    def test_native_desktop_logs_do_not_interrupt_cli_chunks(self):
        summary = self.summary()
        record = self.received("r1", "response.completed")
        boundary = record.index(r'\"id\"')
        self.append(self.desktop(record[:boundary]))
        summary.poll()
        self.append(b'2026-10-08T07:45:00Z warning [electron-message-handler] payload={invalid}\n')
        summary.poll()
        self.append(self.desktop(record[boundary:]))
        summary.poll()
        self.assertEqual([row["response_id"] for row in summary.rows], ["r1"])
        self.assertEqual(summary.rows[0]["transport"], "WebSocket")
        self.assertEqual(summary.json_fragments, {})

    def test_unquoted_desktop_tail_preserves_all_json_chunk_boundaries(self):
        record = self.received("r1", "response.completed")
        for boundary in range(len(record) + 1):
            with self.subTest(boundary=boundary):
                summary = self.summary()
                parsed = list(summary.decode_events(self.desktop(record[:boundary]), self.log))
                raw = ("Codex CLI stderr message=" + record[boundary:] + "\n").encode()
                parsed.extend(summary.decode_events(raw, self.log))
                self.assertEqual([rid for rid, _ in parsed], ["r1"])

    def test_unquoted_desktop_chunk_keeps_whitespace_inside_response_strings(self):
        payload = json.dumps({"type": "response.failed", "response": {
            "id": "r1", "error": {"message": "synthetic failure message"}}})
        record = self.structured("tungstenite::protocol", "Received message " + payload)
        start = record.index(r'\"id\"')
        end = record.index("synthetic failure ") + len("synthetic failure ")
        summary = self.summary()
        self.append(self.desktop(record[:start]))
        self.append(("Codex CLI stderr message=" + record[start:end] + "\n").encode())
        self.append(self.desktop(record[end:]))
        summary.poll()
        self.assertEqual(summary.rows[0]["error_message"], "synthetic failure message")

    def test_connection_diagnosis_and_outgoing_frames_are_ignored(self):
        self.append(self.desktop(self.structured("codex_api::endpoint::responses_websocket",
                                                "successfully connected to websocket: wss://example.test")))
        self.append(self.desktop(self.structured("tungstenite::protocol", "Sending frame: " + self.sse("r1"))))
        summary = self.summary()
        summary.poll()
        self.assertEqual(summary.rows, [])
        self.assertIn("已检测到 WebSocket", summary.status)

    def test_plain_websocket_trace_and_truncated_json_recovery(self):
        self.link("r1")
        self.append((self.sse("r1").replace("codex_api::sse::responses: SSE event:",
                                          "tungstenite::protocol: Received message") + "\n").encode())
        summary = self.summary()
        summary.poll()
        self.assertEqual(summary.rows[0]["first_effort"], "high")
        self.append(self.desktop(self.received("bad")[:100]))
        summary.poll()
        self.append(self.desktop("\n" + self.sse("r1", "response.completed")))
        summary.poll()
        self.assertEqual(summary.rows[0]["final_effort"], "high")


class TimeFieldsTests(LogFixture):
    def response(self, rid, typ="response.completed", *, stamp="2026-10-08T09:43:13.231443Z",
                 transport="SSE", **fields):
        payload = json.dumps({"type": typ, "response": {"id": rid, "model": "test-model",
                             "reasoning": {"effort": "high"}, **fields}})
        if stamp is None:
            return "data: " + payload
        marker = ("codex_api::sse::responses: SSE event: " if transport == "SSE" else
                  "tungstenite::protocol: Received message ")
        return stamp + " TRACE " + marker + payload

    def test_sse_server_times_are_distinct_from_request_and_log_times(self):
        created = self.response("r1", "response.created", created_at=1791452586,
                                stamp="2026-10-08T09:43:12.039783Z")
        completed = self.response("r1", created_at=1791452586, completed_at=1791452590)
        # Exercise the structured Rust JSON format used by the desktop application.
        for line in (created, completed):
            stamp, _, message = line.partition(" TRACE codex_api::sse::responses: ")
            record = {"timestamp": stamp, "target": "codex_api::sse::responses",
                      "fields": {"message": message}}
            self.append(self.desktop(json.dumps(record)))
        summary = self.summary()
        summary.poll()
        row = summary.rows[0]
        self.assertEqual(row["response_created_at"], 1791452586)
        self.assertEqual(row["response_completed_at"], 1791452590)
        self.assertEqual(row["response_duration_ms"], 4000)
        self.assertAlmostEqual(row["first_logged_at"], 1791452592.039783, places=6)
        self.assertAlmostEqual(row["final_logged_at"], 1791452593.231443, places=6)
        self.assertEqual(row["transport"], "SSE")
        for key in ("request_started_at", "http_status", "elapsed_ms", "error_code"):
            self.assertIsNone(row[key], key)

    def test_created_event_time_wins_and_earliest_log_survives_out_of_order_events(self):
        self.append(self.desktop(self.response("r1", created_at=999, completed_at=1004)))
        self.append(self.desktop(self.response("r1", "response.created", created_at=1000,
                                              stamp="2026-10-08T09:43:12Z")))
        summary = self.summary()
        summary.poll()
        row = summary.rows[0]
        self.assertEqual((row["response_created_at"], row["response_duration_ms"]), (1000, 4000))
        self.assertEqual(row["first_logged_at"], 1791452592)
        self.assertEqual(row["status"], "已完成")
        self.assertAlmostEqual(row["final_logged_at"], 1791452593.231443, places=6)

    def test_terminal_object_supplies_missing_created_time_and_preserves_zero(self):
        self.append(self.desktop(self.response("r1", created_at=0, completed_at=0.125)))
        summary = self.summary()
        summary.poll()
        row = summary.rows[0]
        self.assertEqual((row["response_created_at"], row["response_completed_at"],
                          row["response_duration_ms"]), (0, 0.125, 125))
        self.assertIsNone(row["first_effort"])

    def test_missing_invalid_and_reversed_server_times_do_not_produce_duration(self):
        invalid = [None, True, False, "1791452586", -1, {}, [], float("nan"),
                   float("inf"), 10**400, 253402300800]
        for index, value in enumerate(invalid):
            self.append(self.desktop(self.response(f"created-{index}", created_at=value, completed_at=10)))
            self.append(self.desktop(self.response(f"completed-{index}", created_at=0, completed_at=value)))
        self.append(self.desktop(self.response("reversed", created_at=10, completed_at=9)))
        summary = self.summary()
        summary.poll()
        for row in summary.rows:
            with self.subTest(response=row["response_id"]):
                self.assertIsNone(row["response_duration_ms"])
                if row["response_id"].startswith("created-"):
                    self.assertIsNone(row["response_created_at"])
                if row["response_id"].startswith("completed-"):
                    self.assertIsNone(row["response_completed_at"])

    def test_untimed_raw_event_does_not_display_ingestion_time(self):
        self.append((self.response("untimed", stamp=None) + "\n").encode())
        summary = self.summary()
        summary.poll()
        row = summary.rows[0]
        for key in ("request_started_at", "response_created_at", "response_completed_at",
                    "first_logged_at", "final_logged_at", "response_duration_ms"):
            self.assertIsNone(row[key], key)
        self.assertEqual(row["transport"], "SSE")

    def test_raw_event_does_not_borrow_preceding_line_timestamp(self):
        self.append(self.desktop(self.response("logged") + "\n" + self.response("raw", stamp=None)))
        summary = self.summary()
        summary.poll()
        rows = {row["response_id"]: row for row in summary.rows}
        self.assertIsNotNone(rows["logged"]["first_logged_at"])
        self.assertIsNone(rows["raw"]["first_logged_at"])
        self.assertIsNone(rows["raw"]["final_logged_at"])

    def test_split_marker_and_split_json_preserve_original_log_time(self):
        summary = self.summary()
        for rid, boundary in (("marker", "SSE eve"), ("json", '"response": {"id"')):
            text = self.response(rid, created_at=1000, completed_at=1001)
            split = text.index(boundary) + len(boundary)
            self.append(self.desktop(text[:split]))
            summary.poll()
            self.assertNotIn(rid, [row["response_id"] for row in summary.rows])
            self.append(self.desktop(text[split:] + "\n"))
            summary.poll()
            row = next(row for row in summary.rows if row["response_id"] == rid)
            self.assertAlmostEqual(row["first_logged_at"], 1791452593.231443, places=6)
            self.assertEqual(row["final_logged_at"], row["first_logged_at"])

    def test_late_websocket_timing_adds_request_start_without_overwriting_terminal(self):
        self.append(self.desktop(self.response("r1", created_at=1000, completed_at=1004,
                                              transport="WebSocket", service_tier="default", status="completed",
                                              usage={"input_tokens": 17, "output_tokens": 9, "total_tokens": 26})))
        summary = self.summary()
        summary.poll()
        original = summary.rows[0].copy()
        self.assertIsNone(original["request_started_at"])
        self.append(self.desktop(self.structured("codex_api::responses_websocket_timing", "timing",
                    request_start_ms="1791445402559", payload=json.dumps({
                        "timing_metrics": {"response_id": "r1"}}))))
        summary.poll()
        self.assertEqual(len(summary.rows), 1)
        row = summary.rows[0]
        self.assertEqual(row["request_started_at"], 1791445402.559)
        self.assertEqual(row["transport"], "WebSocket")
        for key in ("status", "response_model", "final_effort", "final_logged_at", "response_duration_ms",
                    "input_tokens", "output_tokens", "total_tokens", "service_tier", "response_status", "response_event"):
            self.assertEqual(row[key], original[key], key)
        self.assertIsNone(row["elapsed_ms"])

    def test_websocket_request_start_rejects_invalid_values_and_accepts_epoch_zero(self):
        invalid = [None, True, False, "invalid", "NaN", "Infinity", -1, {}, 10**400, "253402300800000"]
        cases = [(f"invalid-{index}", value, None) for index, value in enumerate(invalid)]
        cases += [("zero", 0, 0), ("zero-string", "0", 0), ("milliseconds", 1234.5, 1.2345)]
        for rid, value, _ in cases:
            self.append(self.desktop(self.structured("codex_api::responses_websocket_timing", "timing",
                        request_start_ms=value, payload={"timing_metrics": {"response_id": rid}})))
        summary = self.summary()
        summary.poll()
        rows = {row["response_id"]: row for row in summary.rows}
        for rid, _, expected in cases:
            with self.subTest(response=rid):
                self.assertEqual(rows[rid]["request_started_at"], expected)
                self.assertEqual(rows[rid]["status"], "仅耗时日志")
                self.assertIsNone(rows[rid]["response_duration_ms"])

    def test_error_code_is_read_from_response_without_inventing_http_status(self):
        for rid, error in (("limited", {"code": "rate_limit_exceeded"}),
                           ("number", {"code": 429}), ("string", "rate_limit_exceeded")):
            self.append(self.desktop(self.response(rid, "response.failed", error=error)))
        summary = self.summary()
        summary.poll()
        rows = {row["response_id"]: row for row in summary.rows}
        self.assertEqual(rows["limited"]["error_code"], "rate_limit_exceeded")
        for rid in ("number", "string"):
            self.assertIsNone(rows[rid]["error_code"])
        for row in rows.values():
            self.assertEqual(row["status"], "失败")
            self.assertIsNone(row["http_status"])

    def test_diagnostic_fields_keep_terminal_usage_zeros_and_original_errors(self):
        self.context()
        self.link("diagnostic")
        self.append(self.desktop(self.response("diagnostic", "response.created", status="in_progress",
                                              usage={"input_tokens": 99})))
        summary = self.summary()
        summary.poll()
        self.assertIsNone(summary.rows[0]["input_tokens"])
        self.append(self.desktop(self.response("diagnostic", "response.incomplete", status="incomplete",
            service_tier="default", incomplete_details={"reason": "max_output_tokens"},
            error={"code": "fixture_error", "message": "Original <error> message"},
            usage={"input_tokens": 0, "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
                   "output_tokens": 3, "output_tokens_details": {"reasoning_tokens": 0}, "total_tokens": 3})))
        summary.poll()
        row = summary.rows[0]
        for key in ("input_tokens", "cached_input_tokens", "cache_write_tokens", "reasoning_tokens"):
            self.assertEqual(row[key], 0, key)
        self.assertEqual((row["output_tokens"], row["total_tokens"]), (3, 3))
        self.assertEqual((row["error_code"], row["error_message"]), ("fixture_error", "Original <error> message"))
        self.assertEqual(row["incomplete_reason"], "max_output_tokens")
        self.assertEqual((row["response_event"], row["response_status"], row["service_tier"]),
                         ("response.incomplete", "incomplete", "default"))
        self.assertEqual(row["turn_id"], "turn-a")
        self.assertIsNone(row["http_status"])
        self.assertIsNone(row["elapsed_ms"])

    def test_invalid_or_missing_diagnostic_values_are_not_inferred(self):
        cases = [None, [], {"input_tokens": True, "output_tokens": -1, "total_tokens": "8",
                           "input_tokens_details": {"cached_tokens": False, "cache_write_tokens": -1},
                           "output_tokens_details": {"reasoning_tokens": "0"}},
                 {"input_tokens_details": [], "output_tokens_details": "unknown"}]
        for index, usage in enumerate(cases):
            self.append(self.desktop(self.response(str(index), usage=usage, service_tier=1, status=False,
                incomplete_details={"reason": []}, error={"message": 123})))
        summary = self.summary()
        summary.poll()
        for row in summary.rows:
            for key in ("input_tokens", "output_tokens", "total_tokens", "cached_input_tokens", "cache_write_tokens",
                        "reasoning_tokens", "service_tier", "response_status", "incomplete_reason", "error_message"):
                self.assertIsNone(row[key], (row["response_id"], key))


class TailTests(unittest.TestCase):
    def test_complete_lines_use_a_constant_number_of_tell_calls(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "stream.log"
            complete = b"complete entry\n" * 2000
            path.write_bytes(complete + b"partial")
            cursors = collections.OrderedDict()
            with path.open("rb") as stream:
                observed = mock.MagicMock(wraps=stream)
                observed.__enter__.return_value = observed
                with mock.patch.object(Path, "open", return_value=observed):
                    lines = list(watch.read_appended(path, cursors, window=None))
            self.assertEqual(b"".join(lines), complete)
            self.assertEqual(cursors[str(path)][1], len(complete))
            self.assertLessEqual(observed.tell.call_count, 2)

    def test_growth_during_read_keeps_full_byte_positions_and_deferred_cursor_commit(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "stream.log"
            path.write_bytes(b"first\npartial")
            cursors = collections.OrderedDict()
            reader = watch.read_appended(path, cursors, window=None)
            self.assertEqual(next(reader), b"first\n")
            self.assertEqual(cursors, {})
            with path.open("ab") as stream:
                stream.write(b" finished\nlater\n")
            self.assertEqual(list(reader), [b"partial finished\n"])
            self.assertEqual(cursors[str(path)][1], len(b"first\npartial finished\n"))
            self.assertEqual(list(watch.read_appended(path, cursors, window=None)), [b"later\n"])

    def test_copytruncate_with_an_unchanged_tail_rereads_same_size_and_growth(self):
        old = b'old-response ' + b'unchanged suffix ' * 10 + b'\n'
        new = old.replace(b'old-response', b'new-response')
        for extra in (b'', b'another response\n'):
            with self.subTest(growth=bool(extra)), tempfile.TemporaryDirectory() as folder:
                path = Path(folder) / "stream.log"
                cursors = collections.OrderedDict()
                path.write_bytes(old)
                inode = path.stat().st_ino
                self.assertEqual(list(watch.read_appended(path, cursors)), [old])
                previous_mtime = path.stat().st_mtime_ns
                path.write_bytes(new + extra)
                os.utime(path, ns=(previous_mtime + 1_000_000, previous_mtime + 1_000_000))
                self.assertEqual(path.stat().st_ino, inode)
                reset = mock.Mock()
                expected = [new] + ([extra] if extra else [])
                self.assertEqual(list(watch.read_appended(path, cursors, on_reset=reset)), expected)
                reset.assert_called_once_with()

    def test_append_validation_reads_only_bounded_samples(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "stream.log"
            path.write_bytes(b"old entry\n" * 10_000)
            cursors = collections.OrderedDict()
            list(watch.read_appended(path, cursors))
            with path.open("ab") as stream:
                stream.write(b"appended entry\n")
            with path.open("rb") as stream:
                observed = mock.MagicMock(wraps=stream)
                observed.__enter__.return_value = observed
                with mock.patch.object(Path, "open", return_value=observed):
                    self.assertEqual(list(watch.read_appended(path, cursors)), [b"appended entry\n"])
            self.assertLessEqual(sum(call.args[0] for call in observed.read.call_args_list), 4096 + 128)
            self.assertTrue(all(0 <= call.args[0] <= 4096 for call in observed.read.call_args_list))

    def test_copytruncate_regrowth_replacement_and_partial_utf8(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "stream.log"
            cursors = collections.OrderedDict()
            path.write_bytes(b"old\n")
            self.assertEqual(list(watch.read_appended(path, cursors)), [b"old\n"])
            path.write_bytes(b"new content longer than old\n")
            self.assertEqual(list(watch.read_appended(path, cursors)), [b"new content longer than old\n"])
            path.write_bytes(b"x\n")
            self.assertEqual(list(watch.read_appended(path, cursors)), [b"x\n"])
            replacement = path.with_suffix(".new")
            replacement.write_bytes(b"replacement\n")
            os.replace(replacement, path)
            self.assertEqual(list(watch.read_appended(path, cursors)), [b"replacement\n"])
            raw = "中文\n".encode()
            with path.open("ab") as stream:
                stream.write(raw[:2])
            self.assertEqual(list(watch.read_appended(path, cursors)), [])
            with path.open("ab") as stream:
                stream.write(raw[2:])
            self.assertEqual(list(watch.read_appended(path, cursors)), [raw])

    def test_initial_window_retains_a_complete_boundary_line(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "stream.log"
            original = watch.WINDOW
            try:
                watch.WINDOW = 4
                path.write_bytes(b"skip\nabc\n")
                self.assertEqual(list(watch.read_appended(path, collections.OrderedDict())), [b"abc\n"])
            finally:
                watch.WINDOW = original


class JsonFragmentTests(unittest.TestCase):
    def test_quotes_and_backslash_runs_preserve_boundaries_across_empty_chunks(self):
        values = ("", "text [] {} 中文", "".join("\\" * count + '"[]{}' for count in range(9)) + "\\" * 9)
        for value in values:
            for record in (json.dumps(value, ensure_ascii=False),
                           json.dumps({"payload": value, "tail": [{}]}, ensure_ascii=False)):
                for boundary in range(len(record)):
                    with self.subTest(record=record, boundary=boundary):
                        fragment = watch.JsonFragment("", True, False)
                        self.assertEqual(fragment.feed(record[:boundary]), (None, None))
                        state = (bytes(fragment.stack), fragment.quoted, fragment.escaped,
                                 fragment.started, fragment.size, fragment.tail)
                        self.assertEqual(fragment.feed(""), (None, None))
                        self.assertEqual((bytes(fragment.stack), fragment.quoted, fragment.escaped,
                                          fragment.started, fragment.size, fragment.tail), state)
                        self.assertEqual(fragment.feed(record[boundary:] + " trailing ]}"),
                                         (len(record) - boundary, None))
                        self.assertEqual(fragment.buffer.getvalue(), record)
                        self.assertFalse(fragment.quoted or fragment.escaped or fragment.invalid or fragment.stack)
                fragment = watch.JsonFragment("", True, False)
                for index, char in enumerate(record):
                    self.assertEqual(fragment.feed(""), (None, None))
                    self.assertEqual(fragment.feed(char), (1 if index == len(record) - 1 else None, None))
                self.assertEqual(fragment.buffer.getvalue(), record)

    def test_brackets_inside_strings_do_not_require_python_framing_work(self):
        record = json.dumps({"payload": "[]{}" * 100_000})
        pattern = watch.JSON_FRAME
        matched = 0

        def finditer(*args):
            nonlocal matched
            for token in pattern.finditer(*args):
                matched += 1
                yield token

        fragment = watch.JsonFragment("", True, False)
        with mock.patch.object(watch, "JSON_FRAME") as framing:
            framing.finditer.side_effect = finditer
            for position in range(0, len(record), 4096):
                end = fragment.scan(record[position:position + 4096])
                expected = len(record) - position if position + 4096 >= len(record) else None
                self.assertEqual(end, expected)
        self.assertLessEqual(matched, 8)


class ParserRegressionTests(LogFixture):
    def test_ignored_fragments_do_not_store_or_redecode_their_bodies(self):
        texts = (
            ('ordinary payload={"padding":"' + "x[{}]" * 1000 + '","nested":' + self.received("injected") + '}', 1),
            ("ordinary [worker] payload={not JSON}", 2),
        )
        for text, initial_decodes in texts:
            for chunks in ((text,), (text[:25], text[25:-1], text[-1:])):
                with self.subTest(length=len(text), chunks=len(chunks)):
                    summary = self.summary()
                    lines = [self.desktop(chunk) for chunk in chunks]
                    with mock.patch.object(watch, "JSON_DECODER", wraps=watch.JSON_DECODER) as decoder:
                        for line in lines:
                            continuing = str(self.log) in summary.json_fragments
                            before = decoder.raw_decode.call_count
                            self.assertEqual(list(summary.decode_events(line, self.log)), [])
                            if continuing:
                                self.assertEqual(decoder.raw_decode.call_count - before, 1)
                            pending = summary.json_fragments.get(str(self.log))
                            if pending is not None:
                                self.assertTrue(pending.ignored)
                                self.assertIsNone(pending.buffer)
                                self.assertLessEqual(len(pending.tail), 513)
                    # Try each outer container once. Continuations only decode
                    # their desktop string wrapper, not the ignored body.
                    self.assertEqual(decoder.raw_decode.call_count, len(lines) + initial_decodes)
                    self.assertEqual(summary.json_fragments, {})
                    following = self.desktop("\n" + self.received("real"))
                    self.assertEqual([rid for rid, _ in summary.decode_events(following, self.log)], ["real"])

    def test_complete_ignored_container_above_the_fragment_limit_keeps_later_roots(self):
        ordinary = "ordinary payload=" + "[" * 300 + "0" + "]" * 300
        following = "\n" + self.received("real")
        with mock.patch.object(watch, "WINDOW", 256):
            for chunks in ((ordinary + following,), (ordinary, following)):
                with self.subTest(chunks=len(chunks)):
                    summary = self.summary()
                    parsed = [rid for chunk in chunks
                              for rid, _ in summary.decode_events(self.desktop(chunk), self.log)]
                    self.assertEqual(parsed, ["real"])
                    self.assertEqual(summary.discarded_fragments, set())

    def test_long_split_json_has_linear_scan_and_decode_work(self):
        body = json.dumps({"type": "response.completed", "response": {
            "id": "long", "padding": "x" * (128 * 1024) + '\\"[]{}'}})
        records = ("data: " + body, self.structured("codex_api::sse", "SSE event: " + body))
        original_scan = watch.JsonFragment.scan
        for record in records:
            for complete in (False, True):
                with self.subTest(structured=record.startswith("{"), complete=complete):
                    text = record if complete else record[:-10]
                    lines = [self.desktop(text[offset:offset + 251]) for offset in range(0, len(text), 251)]
                    scanned = 0

                    def scan(fragment, chunk):
                        nonlocal scanned
                        scanned += len(chunk)
                        return original_scan(fragment, chunk)

                    summary = self.summary()
                    with mock.patch.object(watch.JsonFragment, "scan", scan), \
                         mock.patch.object(watch, "JSON_DECODER", wraps=watch.JSON_DECODER) as decoder, \
                         mock.patch.object(watch, "LOG_RECORD_START", wraps=watch.LOG_RECORD_START) as prefixes:
                        parsed = [rid for line in lines for rid, _ in summary.decode_events(line, self.log)]
                    self.assertEqual(parsed, ["long"] if complete else [])
                    # Count input examined, not elapsed time. Both incomplete
                    # and complete records must avoid retrying growing prefixes.
                    decoded = sum(len(call.args[0]) - (call.args[1] if len(call.args) > 1 else 0)
                                  for call in decoder.raw_decode.call_args_list)
                    searched = sum(len(call.args[0]) for call in prefixes.search.call_args_list)
                    self.assertLessEqual(scanned, len(text))
                    self.assertLessEqual(decoded, sum(map(len, lines)) + 3 * len(text))
                    self.assertLessEqual(searched, 3 * len(text) + 513 * len(lines))
                    self.assertLessEqual(decoder.raw_decode.call_count, len(lines) + 4)

    def test_empty_desktop_chunks_do_not_grow_unfinished_record_storage(self):
        summary = self.summary()
        record = self.sse("unfinished", "response.completed")
        self.assertEqual(list(summary.decode_events(self.desktop(record[:-1]), self.log)), [])
        fragment = summary.json_fragments[str(self.log)]
        size, tail = fragment.size, fragment.tail
        fragment.buffer = mock.Mock(wraps=fragment.buffer)
        empty = self.desktop("")
        for _ in range(2000):
            self.assertEqual(list(summary.decode_events(empty, self.log)), [])
        self.assertIs(summary.json_fragments[str(self.log)], fragment)
        self.assertEqual((fragment.size, fragment.tail), (size, tail))
        fragment.buffer.write.assert_not_called()
        fragment.buffer.getvalue.assert_not_called()
        self.assertEqual([rid for rid, _ in summary.decode_events(self.desktop(record[-1:]), self.log)],
                         ["unfinished"])
        fragment.buffer.getvalue.assert_called_once_with()

    def test_real_prefix_split_across_chunks_recovers_after_long_damaged_json(self):
        summary = self.summary()
        damaged = 'data: {"response":{"padding":"' + "x" * 100_000
        for offset in range(0, len(damaged), 251):
            self.assertEqual(list(summary.decode_events(self.desktop(damaged[offset:offset + 251]), self.log)), [])
        following = "\n" + self.sse("recovered", "response.completed")
        parsed = [rid for char in following for rid, _ in summary.decode_events(self.desktop(char), self.log)]
        self.assertEqual(parsed, ["recovered"])
        self.assertEqual(summary.json_fragments, {})

    def test_split_scalar_sse_values_do_not_hide_following_structured_roots(self):
        for value in ('"escaped \\\" [] {} \\\\"', "false", "true", "null", "-12.5", "NaN", "-Infinity"):
            with self.subTest(value=value):
                self.assert_desktop_chunkings("data: " + value + "\n" + self.received("following"), ["following"])

    def test_invalid_event_types_do_not_stop_polling_or_hide_later_events(self):
        for value in ([], {}, None, 0, True, "unknown.event"):
            self.append(("data: " + json.dumps({"type": value, "response": {"id": "bad"}}) + "\n").encode())
        self.append(event("good"))
        summary = self.summary()
        watch.poll_safely(summary)
        self.assertEqual(set(summary.records), {"good"})
        self.append(event("later"))
        watch.poll_safely(summary)
        self.assertEqual(set(summary.records), {"good", "later"})

    def test_deep_json_is_skipped_at_each_event_input_boundary(self):
        deep = "[" * 10_000 + "0" + "]" * 10_000
        nested_record = '{"fields":{"message":"bad"},"extra":' + deep + '}'
        nested_event = '{"type":"response.completed","response":{"id":"bad","extra":' + deep + '}}'
        timing_payload = '{"timing_metrics":{"response_id":"bad"},"extra":' + deep + '}'
        cases = {
            "desktop": ("Codex CLI stderr message=" + nested_record + "\n").encode(),
            "sse": ("data: " + nested_event + "\n").encode(),
            "structured": (nested_record + "\n").encode(),
            "timing payload": self.desktop(self.structured(
                "codex_api::responses_websocket_timing", "timing", payload=timing_payload)),
        }
        for source, bad in cases.items():
            with self.subTest(source=source):
                self.log.write_bytes(bad + self.desktop(self.received("good", "response.completed")))
                summary = self.summary()
                watch.poll_safely(summary)
                self.assertEqual(set(summary.records), {"good"})
                self.append(event("later"))
                watch.poll_safely(summary)
                self.assertEqual(set(summary.records), {"good", "later"})

    def test_deep_rollout_record_does_not_hide_later_context_and_link(self):
        deep = "[" * 10_000 + "0" + "]" * 10_000
        with (self.home / "thread-a.jsonl").open("a") as stream:
            stream.write('{"type":"turn_context","payload":{"turn_id":"turn-a"},"extra":' + deep + '}\n')
        self.context()
        self.link("good")
        self.append(event("good"))
        summary = self.summary()
        self.assertEqual(watch.poll_safely(summary), [("手动名称", 1, "test-model", "high")])
        self.assertEqual(summary.rows[0]["request_effort"], "high")

    def test_plain_desktop_chunks_can_start_at_any_nested_object(self):
        nested = {"timestamp": "2026-10-08T12:00:00Z", "target": "codex_api::sse::responses",
                  "fields": {"message": "SSE event: " + json.dumps({
                      "type": "response.completed", "response": {"id": "must-not-appear"}})}}
        body = {"type": "response.completed", "response": {
            "id": "outer", "model": "test-model", "metadata": nested,
            "usage": {"output_tokens": 0}}}
        text = "2026-10-08T12:00:00Z TRACE codex_api::sse::responses: SSE event: " + json.dumps(body)
        for boundary, char in enumerate(text):
            if char != "{":
                continue
            with self.subTest(boundary=boundary):
                summary = self.summary()
                self.assertEqual(list(summary.decode_events(self.desktop(text[:boundary]), self.log)), [])
                parsed = list(summary.decode_events(self.desktop(text[boundary:]), self.log))
                self.assertEqual([rid for rid, _ in parsed], ["outer"])
                self.assertEqual(parsed[0][1]["output_tokens"], 0)
                self.assertEqual(parsed[0][1]["logged_at"], 1791460800)
                following = list(summary.decode_events(self.desktop(self.received("following")), self.log))
                self.assertEqual([rid for rid, _ in following], ["following"])

    def test_real_log_prefix_recovers_after_a_damaged_plain_log_line(self):
        for boundary in ("before", "after"):
            with self.subTest(boundary=boundary):
                summary = self.summary()
                partial = '2026-10-08T12:00:00Z TRACE codex_api::sse::responses: SSE event: {"type":'
                following = self.sse("following")
                first = partial + ("\n" if boundary == "before" else "")
                second = ("\n" if boundary == "after" else "") + following
                self.assertEqual(list(summary.decode_events(self.desktop(first), self.log)), [])
                parsed = list(summary.decode_events(self.desktop(second), self.log))
                self.assertEqual([rid for rid, _ in parsed], ["following"])
                self.assertNotIn(str(self.log), summary.fragments)

    def test_multiline_json_preserves_outer_identity_at_every_chunk_boundary(self):
        inner = json.loads(self.received("injected", "response.completed"))
        plain = '2026-10-08T12:00:00Z TRACE codex_api::sse::responses: SSE event: ' + json.dumps({
            "type": "response.completed", "response": {"id": "outer", "metadata": inner}}, indent=2)
        unrelated = json.dumps({"timestamp": "2026-10-08T12:00:00Z", "target": "unrelated",
                                "fields": {"message": "ordinary"}, "metadata": inner}, indent=2)
        array = json.dumps([inner], indent=2)
        for text, expected in ((plain, ["outer"]), (unrelated, []), (array, []), ("ordinary log\n" + array, [])):
            for boundary in range(len(text) + 1):
                with self.subTest(structured=text.startswith("{"), boundary=boundary):
                    summary = self.summary()
                    results = []
                    for chunk in (text[:boundary], text[boundary:]):
                        results.extend(rid for rid, _ in summary.decode_events(self.desktop(chunk), self.log))
                    self.assertEqual(results, expected)
            summary = self.summary()
            one_character_chunks = [rid for char in text
                                    for rid, _ in summary.decode_events(self.desktop(char), self.log)]
            self.assertEqual(one_character_chunks, expected)

    def assert_desktop_chunkings(self, text, expected):
        summary = self.summary()
        self.assertEqual([rid for rid, _ in summary.decode_events(self.desktop(text), self.log)], expected)
        for boundary in range(len(text) + 1):
            with self.subTest(boundary=boundary):
                summary = self.summary()
                parsed = [rid for chunk in (text[:boundary], text[boundary:])
                          for rid, _ in summary.decode_events(self.desktop(chunk), self.log)]
                self.assertEqual(parsed, expected)
        summary = self.summary()
        parsed = [rid for char in text for rid, _ in summary.decode_events(self.desktop(char), self.log)]
        self.assertEqual(parsed, expected)

    def test_inline_json_in_ordinary_logs_never_supplies_nested_records(self):
        inner = json.loads(self.received("injected", "response.completed"))
        payloads = (inner, [inner], {"items": [inner]}, {"items": [{"nested": [inner]}]})
        prefixes = (
            "2026-10-08T12:00:00Z INFO unrelated::logger: payload=",
            "ordinary payload=",
            "[worker] payload=",
            "ordinary {not JSON} payload=",
            "ordinary log\n" + "x" * 600 + " payload=",
        )
        following = "\n" + self.received("structured", "response.completed") + "\n" + self.sse("plain")
        for prefix in prefixes:
            for payload in payloads:
                with self.subTest(prefix=prefix[:60], payload=type(payload).__name__):
                    text = prefix + json.dumps(payload, indent=2)
                    self.assert_desktop_chunkings(text, [])
                    self.assert_desktop_chunkings(text + following, ["structured", "plain"])
                    summary = self.summary()
                    parsed = [rid for line in text.splitlines(keepends=True)
                              for rid, _ in summary.decode_events(line.encode(), self.log)]
                    self.assertEqual(parsed, [])

    def test_inline_container_end_does_not_create_a_new_root_on_the_same_line(self):
        inner = self.received("injected", "response.completed")
        for prefix in ("ordinary payload=[] ", "ordinary payload={} ", "[worker] "):
            with self.subTest(prefix=prefix):
                self.assert_desktop_chunkings(prefix + inner, [])
                self.assert_desktop_chunkings(prefix + inner + "\n" + self.received("real"), ["real"])

    def test_real_root_records_after_ordinary_lines_keep_their_boundaries(self):
        inner = json.loads(self.received("injected", "response.completed"))
        root = json.dumps(json.loads(self.received("real")), indent=2)
        for prefix in ("ordinary log\n", "ordinary payload=[]\n", "x" * 600 + "\n"):
            with self.subTest(prefix=prefix[:60]):
                self.assert_desktop_chunkings(prefix + root, ["real"])
                self.assert_desktop_chunkings(prefix + json.dumps([inner], indent=2) + "\n" + root, ["real"])

    def test_plain_event_markers_must_belong_to_the_actual_log_target(self):
        body = json.dumps({"type": "response.completed", "response": {"id": "injected", "created_at": 0}})
        markers = ("codex_api::sse: SSE event: ", "codex_api::sse::responses: SSE event: ",
                   "tungstenite::protocol: Received message ")
        prefixes = ("2026-10-08T12:00:00Z INFO unrelated::logger: payload=", "ordinary payload=",
                    "2026-10-08T12:00:00Z TRACE unrelated::", "x" * 600 + " payload=", "ordinary TRACE ",
                    "2026-10-08T12:00:00Z INFO unrelated::logger: payload=2026-10-08T12:00:00Z TRACE ")
        for marker in markers:
            for prefix in prefixes:
                with self.subTest(marker=marker, prefix=prefix[:60]):
                    text = prefix + marker + body
                    self.assertEqual([rid for rid, _ in watch.parse_events(text) if rid], [])
                    self.assertIsNone(watch.completed_event((text + "\n").encode()))
                    self.assert_desktop_chunkings(text, [])
                    self.assert_desktop_chunkings(text + "\n" + self.sse("real"), ["real"])
            for prefix in ("", "2026-10-08T12:00:00Z TRACE "):
                with self.subTest(marker=marker, prefix=prefix):
                    text = prefix + marker + body
                    self.assertEqual([rid for rid, _ in watch.parse_events(text) if rid], ["injected"])
                    self.assert_desktop_chunkings(text, ["injected"])
                    fields = next(fields for rid, fields in self.summary().decode_events(self.desktop(text), self.log))
                    self.assertEqual(fields["logged_at"], 1791460800 if prefix else None)
                    self.assertEqual(fields["transport"], "WebSocket" if marker.startswith("tungstenite") else "SSE")
        self.assert_desktop_chunkings("data: " + body, ["injected"])

    def test_structured_log_target_cannot_contain_another_log_prefix(self):
        message = "SSE event: " + json.dumps({"type": "response.completed", "response": {"id": "injected"}})
        targets = ("unrelated::codex_api::sse", "codex_api::sse_other",
                   "codex_api::sse_other\n2026-10-08T12:00:00Z TRACE codex_api::sse")
        for target in targets:
            with self.subTest(target=target):
                self.assert_desktop_chunkings(self.structured(target, message), [])

    def test_structured_response_does_not_require_a_valid_log_timestamp(self):
        message = "SSE event: " + json.dumps({
            "type": "response.completed", "response": {"id": "real", "created_at": 0}})
        for stamp in (None, "", 0, [], "invalid\nstamp", "\0"):
            with self.subTest(stamp=stamp):
                text = json.dumps({"timestamp": stamp, "target": "codex_api::sse",
                                   "fields": {"message": message}})
                self.assert_desktop_chunkings(text, ["real"])
                fields = next(fields for rid, fields in self.summary().decode_events(self.desktop(text), self.log))
                self.assertEqual(fields["timestamp"], 0)
                self.assertIsNone(fields["logged_at"])

    def test_ambiguous_structured_continuation_stays_inside_its_outer_record(self):
        start = self.received("outer")[:100]  # Ends at "fields":; an object here is a legal value.
        for close_outer in (False, True):
            with self.subTest(close_outer=close_outer):
                summary = self.summary()
                for chunk in (start, self.received("injected", "response.completed"), "}" if close_outer else ""):
                    self.assertEqual(list(summary.decode_events(self.desktop(chunk), self.log)), [])
                real = list(summary.decode_events(self.desktop("\n" + self.sse("real")), self.log))
                self.assertEqual([rid for rid, _ in real], ["real"])

    def test_deep_rejected_json_does_not_promote_a_nested_log(self):
        deep = "[" * 10_000 + "0" + "]" * 10_000
        prefixes = (
            ('2026-10-08T12:00:00Z TRACE codex_api::sse::responses: SSE event: '
             '{"type":"response.completed","response":{"id":"outer","extra":' + deep + ',"metadata":', '}}'),
            ('{"timestamp":"2026-10-08T12:00:00Z","target":"unrelated","fields":{"message":"ordinary"},'
             '"extra":' + deep + ',"metadata":', '}'),
        )
        for start, closing in prefixes:
            with self.subTest(structured=start.startswith("{")):
                summary = self.summary()
                self.assertEqual(list(summary.decode_events(self.desktop(start), self.log)), [])
                inner = self.received("injected", "response.completed")
                self.assertEqual(list(summary.decode_events(self.desktop(inner + closing), self.log)), [])
                real = list(summary.decode_events(self.desktop("\n" + self.sse("real")), self.log))
                self.assertEqual([rid for rid, _ in real], ["real"])

    def test_oversized_incomplete_record_retains_framing_and_releases_body(self):
        summary = self.summary()
        with mock.patch.object(watch, "WINDOW", 256):
            start = '{"fields":{"message":"ordinary"},"padding":"' + 'x' * 1000 + '","metadata":'
            self.append(self.desktop(start))
            summary.read_events()
            pending = summary.json_fragments[str(self.log)]
            self.assertIsNone(pending.buffer)
            self.assertEqual(bytes(pending.stack), b"}")
            for chunk in (self.received("injected") + '}', *('x' * 600 for _ in range(10))):
                self.assertEqual(list(summary.decode_events(self.desktop(chunk), self.log)), [])
                self.assertLessEqual(len(summary.fragments.get(str(self.log), "")), 513)
            real = "\n" + self.sse("real")
            boundary = real.index("SSE event:")
            self.assertEqual(list(summary.decode_events(self.desktop(real[:boundary]), self.log)), [])
            parsed = list(summary.decode_events(self.desktop(real[boundary:]), self.log))
            self.assertEqual([rid for rid, _ in parsed], ["real"])
            self.assertNotIn(str(self.log), summary.discarded_fragments)
            list(summary.decode_events(self.desktop(start), self.log))
            self.log.unlink()
            summary.read_events()
            self.assertEqual(summary.discarded_fragments, set())
            self.assertEqual(summary.fragments, {})
            self.assertEqual(summary.json_fragments, {})

    def test_oversized_record_closes_before_following_structured_events(self):
        start = '{"padding":"' + 'x' * 1000 + '\\'
        closing = '\\' + '\\"' + '{}[]","nested":' + self.received("injected") + '}'
        following = self.received("websocket", "response.completed") + "\n" + self.structured(
            "codex_api::sse", "SSE event: " + self.sse("sse").split("SSE event: ", 1)[1])
        for prefix in ("", "data: "):
            for same_chunk in (False, True):
                with self.subTest(prefix=prefix, same_chunk=same_chunk), mock.patch.object(watch, "WINDOW", 256):
                    summary = self.summary()
                    self.assertEqual(list(summary.decode_events(self.desktop(prefix + start), self.log)), [])
                    pending = summary.json_fragments[str(self.log)]
                    self.assertIsNone(pending.buffer)
                    self.assertTrue(pending.quoted and pending.escaped)
                    chunks = (closing + "\n" + following,) if same_chunk else (closing, "\n" + following)
                    parsed = [rid for chunk in chunks for rid, _ in summary.decode_events(self.desktop(chunk), self.log)]
                    self.assertEqual(parsed, ["websocket", "sse"])
                    self.assertEqual(summary.json_fragments, {})
                    self.assertEqual(summary.discarded_fragments, set())

    def test_oversized_invalid_framing_still_requires_a_real_log_prefix(self):
        for start in ('{"padding":"' + 'x' * 1000 + '"]', '[' * 300):
            with self.subTest(start=start[:30]), mock.patch.object(watch, "WINDOW", 256):
                summary = self.summary()
                self.assertEqual(list(summary.decode_events(self.desktop(start), self.log)), [])
                self.assertIn(str(self.log), summary.discarded_fragments)
                self.assertEqual(list(summary.decode_events(self.desktop(self.received("injected")), self.log)), [])
                parsed = list(summary.decode_events(self.desktop("\n" + self.sse("real")), self.log))
                self.assertEqual([rid for rid, _ in parsed], ["real"])

    def test_embedded_markers_are_content_and_do_not_replay_completed_events(self):
        body = {"type": "response.completed", "response": {
            "id": "outer", "error": {"message": "codex_api::sse::responses: SSE event: {not JSON}"}}}
        line = '2026-10-08T10:00:00Z TRACE codex_api::sse::responses: SSE event: ' + json.dumps(body)
        parsed = list(watch.parse_events(line))
        self.assertEqual([rid for rid, _ in parsed if rid], ["outer"])
        self.assertEqual(parsed[-1], (None, ""))
        summary = self.summary()
        self.assertEqual([rid for rid, _ in summary.decode_events(self.desktop(line), self.log)], ["outer"])
        self.assertEqual(list(summary.decode_events(self.desktop('\nunrelated log\n'), self.log)), [])

    def test_only_recognized_log_targets_and_raw_sse_supply_responses(self):
        unrelated = self.sse("false").replace('codex_api::sse::responses:', 'unrelated::logger:')
        records = list(watch.parse_events(unrelated + '\n' + event("raw", raw=True).decode()))
        self.assertEqual([rid for rid, _ in records if rid], ["raw"])

    def test_desktop_wrapper_words_in_response_content_do_not_hide_the_event(self):
        body = {"type": "response.completed", "response": {
            "id": "content", "error": {"message": "Guide to Codex CLI stderr message= in logs."}}}
        message = 'SSE event: ' + json.dumps(body)
        plain = '2026-10-08T10:00:00Z TRACE codex_api::sse::responses: ' + message
        structured = json.dumps({"timestamp": "2026-10-08T10:00:00Z",
                                 "target": "codex_api::sse::responses", "fields": {"message": message}})
        for text in (plain, structured):
            for line in ((text + '\n').encode(), self.desktop(text)):
                with self.subTest(line=line[:100]):
                    self.assertEqual([rid for rid, _ in self.summary().decode_events(line, self.log)], ["content"])

    def test_removed_log_files_release_plain_and_structured_fragments(self):
        summary = self.summary()
        for index in range(12):
            plain = self.logs / f'plain-{index}.log'
            structured = self.logs / f'structured-{index}.log'
            plain.write_bytes(self.desktop(self.sse("partial")[:-20]))
            structured.write_bytes(self.desktop(self.received("partial")[:-20]))
            summary.read_events()
            plain.unlink()
            structured.unlink()
        summary.read_events()
        self.assertEqual(summary.fragments, {})
        self.assertEqual(summary.json_fragments, {})
        self.assertEqual(summary.cursors, {})

    def test_deleted_explicit_log_releases_pending_and_discarded_state(self):
        for limit in (watch.WINDOW, 64):
            with self.subTest(limit=limit), mock.patch.object(watch, "WINDOW", limit):
                self.log.write_bytes(self.desktop(self.received("partial")[:-20]))
                summary = self.summary(log=self.log)
                summary.read_events()
                self.assertTrue(summary.json_fragments or summary.discarded_fragments)
                self.assertIn(str(self.log), summary.cursors)
                self.log.unlink()
                summary.read_events()
                self.assertEqual(summary.json_fragments, {})
                self.assertEqual(summary.fragments, {})
                self.assertEqual(summary.discarded_fragments, set())
                self.assertNotIn(str(self.log), summary.cursors)

    def test_truncated_log_releases_incremental_state_before_regrowth(self):
        self.log.write_bytes(self.desktop(self.received("old")[:-20]))
        summary = self.summary(log=self.log)
        summary.read_events()
        self.assertTrue(summary.json_fragments)
        self.log.write_bytes(b"")
        summary.read_events()
        self.assertEqual(summary.json_fragments, {})
        self.append(self.desktop(self.received("new", "response.completed")))
        summary.read_events()
        self.assertEqual(set(summary.records), {"new"})

    def test_replaced_file_does_not_complete_a_previous_files_partial_event(self):
        for text in (self.sse("old"), self.received("old")):
            with self.subTest(structured=text.startswith('{')):
                self.log.write_bytes(self.desktop(text[:-20]))
                summary = self.summary()
                summary.read_events()
                replacement = self.log.with_suffix('.new')
                replacement.write_bytes(self.desktop(text[-20:] + '\n' + self.sse("fresh")))
                os.replace(replacement, self.log)
                summary.read_events()
                self.assertEqual(set(summary.records), {"fresh"})

    def test_evicted_completion_does_not_crash_a_reappearing_partial_response(self):
        with mock.patch.object(watch, "RESPONSE_LIMIT", 2):
            summary = self.summary()
            self.append(event("old"))
            summary.poll()
            self.append(self.desktop(self.sse("new1") + '\n' + self.sse("new2")))
            summary.poll()
            self.assertNotIn("old", summary.records)
            self.link("old")
            self.append(self.desktop(self.sse("old")))
            self.assertEqual(summary.poll(), [])
        row = next(row for row in summary.rows if row["response_id"] == "old")
        self.assertEqual(row["status"], "响应中")
        self.assertIsNone(row["final_effort"])
        self.assertNotIn("old", summary.events)


if __name__ == "__main__":
    unittest.main(verbosity=2)
