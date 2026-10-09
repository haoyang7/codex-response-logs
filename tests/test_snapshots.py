"""Full and delta snapshots, encoding, and revision reconstruction."""

import concurrent.futures
import json
import unittest
from unittest import mock

from tests.support import watch


class SnapshotTests(unittest.TestCase):
    def test_immutable_row_identity_skips_comparison_and_equal_copies_keep_the_version(self):
        class Row(dict):
            comparisons = 0

            def __eq__(self, other):
                if self is other:
                    raise AssertionError("An unchanged row was compared to itself")
                self.comparisons += 1
                return super().__eq__(other)

        row = Row(response_id="r1", value=0)
        one = watch.RowSnapshot({"rows": [row]}, "instance", 1)
        two = watch.RowSnapshot({"rows": [row]}, "instance", 2, one)
        self.assertEqual(row.comparisons, 0)
        three = watch.RowSnapshot({"rows": [dict(row)]}, "instance", 3, two)
        self.assertEqual(row.comparisons, 1)
        self.assertEqual(three.versioned_rows["r1"][0], 1)

    def test_json_surrogates_round_trip_in_full_and_delta_snapshots(self):
        rid = "r\udcff"
        message = "bad\ud800 \udfff 中文 🚀 literal \\ud800"
        text = "data: " + json.dumps({"type": "response.failed", "response": {
            "id": rid, "created_at": 0, "model": "测试模型", "error": {"message": message},
            "usage": {"output_tokens": 0}}})
        parsed = next(fields for response_id, fields in watch.parse_events(text) if response_id == rid)
        row = dict(parsed, response_id=rid)
        steady = {"response_id": "steady"}
        one = watch.RowSnapshot({"rows": [{"response_id": rid}, steady]}, "instance", 1)
        two = watch.RowSnapshot({"rows": [row, steady]}, "instance", 2, one)
        full = two.encode()
        self.assertEqual(json.loads(full.decode("utf-8")), two.data)
        self.assertIn("测试模型".encode("utf-8"), full)
        self.assertIn(b"\\ud800", full)
        self.assertEqual(json.loads(full)["rows"][0]["error_message"], message)
        delta = json.loads(two.encode(one.etag, True).decode("utf-8"))
        self.assertEqual(delta["base"], one.etag)
        self.assertEqual(delta["rows"], [row])
        self.assertEqual(delta["order"], [rid, "steady"])
        self.assertEqual(delta["rows"][0]["output_tokens"], 0)

    def test_delta_reconstructs_a_window_after_skipped_revisions(self):
        a, b, c, e = ({"response_id": rid, "value": 0} for rid in "abce")
        one = watch.RowSnapshot({"rows": [a, b, c, e], "status": ""}, "instance", 1)
        d = {"response_id": "d", "value": 1}
        two = watch.RowSnapshot({"rows": [d, a, b, e], "status": ""}, "instance", 2, one)
        changed_b = dict(b, value=2)
        three = watch.RowSnapshot({"rows": [c, d, changed_b, e], "status": ""}, "instance", 3, two)
        payload = json.loads(three.encode(one.etag, True))
        self.assertEqual(payload["order"], ["c", "d", "b", "e"])
        # c re-entered the window. A reader at revision 2 has never seen its
        # current membership; include it even though its values are unchanged.
        after_two = json.loads(three.encode(two.etag, True))
        self.assertEqual([row["response_id"] for row in after_two["rows"]], ["c", "b"])
        prior = {row["response_id"]: row for row in one.data["rows"]}
        prior.update((row["response_id"], row) for row in payload["rows"])
        self.assertEqual([prior[rid] for rid in payload["order"]], three.data["rows"])
        self.assertEqual(set(three.versioned_rows), {"b", "c", "d", "e"})

    def test_replaced_window_uses_smaller_full_snapshot(self):
        one = watch.RowSnapshot({"rows": [{"response_id": "old"}]}, "instance", 1)
        two = watch.RowSnapshot({"rows": [{"response_id": "new"}]}, "instance", 2, one)
        self.assertEqual(json.loads(two.encode(one.etag, True)), two.data)
        self.assertIsNone(two.delta_body)

    def test_metadata_only_delta_omits_unchanged_row_bodies(self):
        row = {"response_id": "r1", "first_effort": "high", "final_effort": "high"}
        one = watch.RowSnapshot({"rows": [row], "status": "old"}, "instance", 1)
        two = watch.RowSnapshot({"rows": [dict(row)], "status": "new"}, "instance", 2, one)
        payload = json.loads(two.encode(one.etag, True))
        self.assertEqual(payload["rows"], [])
        self.assertEqual(payload["order"], ["r1"])
        self.assertEqual(payload["status"], "new")

    def test_unknown_or_future_base_returns_full_snapshot(self):
        data = {"rows": [{"response_id": "r1", "reasoning_tokens": 0}]}
        snapshot = watch.RowSnapshot(data, "instance", 3)
        for tag in (None, '"previous-instance-2"', '"instance-4"', '"instance--1"', '"instance-99999999999999999999999"', 'invalid'):
            with self.subTest(tag=tag):
                self.assertEqual(json.loads(snapshot.encode(tag, True)), data)
        self.assertEqual(json.loads(snapshot.encode('"instance-1"', False)), data)

    def test_encoding_is_lazy_shared_and_thread_safe(self):
        steady = {"response_id": "steady"}
        one = watch.RowSnapshot({"rows": [{"response_id": "r1"}, steady]}, "instance", 1)
        two = watch.RowSnapshot({"rows": [{"response_id": "r2"}, steady]}, "instance", 2, one)
        self.assertIsNone(two.full_body)
        self.assertIsNone(two.delta_body)
        with mock.patch.object(watch.json, "dumps", wraps=json.dumps) as dumps:
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                bodies = list(pool.map(lambda _: two.encode(one.etag, True), range(12)))
            self.assertEqual(dumps.call_count, 1)
        self.assertTrue(all(body is bodies[0] for body in bodies))
        self.assertIsNone(two.full_body)


if __name__ == "__main__":
    unittest.main(verbosity=2)
