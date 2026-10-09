"""Local HTTP access, long polling, and server responsiveness."""

import concurrent.futures
import contextlib
import http.client
import json
import os
import signal
import subprocess
import sys
import threading
import time
import unittest
from unittest import mock

from tests.support import LogFixture, SCRIPT, event, watch


class LocalWebApiTests(LogFixture):
    def test_loopback_startup_does_not_wait_for_reverse_dns(self):
        ready, resolving, release = threading.Event(), threading.Event(), threading.Event()
        observed, errors = [], []
        runtime = self.home / "runtime"

        def blocked_lookup(host):
            resolving.set()
            release.wait(5)
            return ("synthetic.local", [], [host])

        class ObservedServer(watch.ThreadingHTTPServer):
            def serve_forever(server, *args, **kwargs):
                observed.append(server)
                ready.set()
                super().serve_forever(*args, **kwargs)

        def run():
            try:
                watch.serve(self.summary(), 0, 20, False)
            except Exception as error:
                errors.append(error)

        with mock.patch.object(watch.socket, "gethostbyaddr", side_effect=blocked_lookup), \
             mock.patch.object(watch, "ThreadingHTTPServer", ObservedServer), \
             mock.patch.object(watch, "control_path", side_effect=lambda port: runtime / f"{port}.json"):
            thread = threading.Thread(target=run, daemon=True)
            thread.start()
            try:
                self.assertTrue(ready.wait(2), "loopback startup waited for reverse DNS")
                self.assertFalse(resolving.is_set())
                server = observed[0]
                port = server.server_port
                self.assertGreater(port, 0)
                self.assertEqual(server.server_address, ("127.0.0.1", port))
                self.assertEqual(server.socket.getsockname(), server.server_address)
                self.assertEqual(server.server_name, "127.0.0.1")
                url = f"http://127.0.0.1:{port}"
                for headers, expected in [({}, 200), ({"Origin": url}, 200),
                                          ({"Host": "evil.example"}, 403),
                                          ({"Origin": "http://evil.example"}, 403)]:
                    with self.subTest(headers=headers), \
                         contextlib.closing(http.client.HTTPConnection("127.0.0.1", port, timeout=2)) as connection:
                        connection.request("GET", "/api/rows", headers=headers)
                        response = connection.getresponse()
                        self.assertEqual(response.status, expected)
                        response.read()
                with contextlib.closing(http.client.HTTPConnection("127.0.0.1", port, timeout=2)) as connection:
                    connection.request("POST", "/api/control/stop")
                    response = connection.getresponse()
                    self.assertEqual(response.status, 403)
                    response.read()
                self.assertEqual(watch.control(port)["url"], url)
            finally:
                # Also clean up the original implementation after a timeout.
                release.set()
                if ready.wait(2) and thread.is_alive():
                    watch.control(observed[0].server_port, "stop")
                thread.join(3)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(list(runtime.glob("*.json")), [])

    def test_local_web_api_does_not_serve_files_or_accept_foreign_host(self):
        import urllib.request
        import urllib.error
        self.context()
        self.link("r1")
        self.append(event("r1"))
        child = subprocess.Popen([sys.executable, str(SCRIPT), "--home", str(self.home), "--web",
                                  "--no-open", "--port", "0", "-n", "2"], stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True)
        try:
            line = child.stdout.readline()
            url = line.split()[0].removeprefix("实时表格：")
            with urllib.request.urlopen(url + "/api/rows", timeout=5) as response:
                body = json.load(response)
                etag = response.headers["ETag"]
            deadline = time.monotonic() + 4
            while body.get("loading") and time.monotonic() < deadline:
                request = urllib.request.Request(url + "/api/rows", headers={"If-None-Match": etag, "Prefer": "wait=4"})
                with urllib.request.urlopen(request, timeout=5) as response:
                    body = json.load(response)
                    etag = response.headers["ETag"]
            self.assertFalse(body.get("loading"))
            self.assertEqual(body["rows"][0]["response_id"], "r1")
            self.assertEqual((body["display_limit"], body["retained_count"], body["retention_limit"]), (2, 1, 5000))
            request = urllib.request.Request(url + "/api/rows", headers={"If-None-Match": etag})
            with self.assertRaises(urllib.error.HTTPError) as unchanged:
                urllib.request.urlopen(request, timeout=5)
            self.assertEqual(unchanged.exception.code, 304)
            self.assertEqual(unchanged.exception.read(), b"")
            self.link("r2")
            self.link("r3")
            self.append(event("r2") + event("r3"))
            deadline = time.monotonic() + 4
            while True:
                try:
                    with urllib.request.urlopen(request, timeout=5) as response:
                        updated = json.load(response)
                        updated_etag = response.headers["ETag"]
                    break
                except urllib.error.HTTPError as error:
                    if error.code != 304 or time.monotonic() >= deadline:
                        raise
                    time.sleep(0.05)
            self.assertNotEqual(etag, updated_etag)
            self.assertEqual([r["response_id"] for r in updated["rows"]], ["r3", "r2"])
            self.assertEqual(updated["retained_count"], 3)
            with urllib.request.urlopen(url, timeout=5) as response:
                self.assertIn("首个响应档位", response.read().decode())
            for path, host, expected in [("/config.toml", None, 404), ("/api/rows", "evil.example", 403)]:
                req = urllib.request.Request(url + path, headers={"Host":host} if host else {})
                with self.assertRaises(urllib.error.HTTPError) as exc:
                    urllib.request.urlopen(req, timeout=5)
                self.assertEqual(exc.exception.code, expected)
        finally:
            child.send_signal(signal.SIGINT)
            child.communicate(timeout=5)


class LongPollTests(LogFixture):
    def start_web(self, observe_wait=False):
        self.append(event("r1"))
        self.link("r1")
        command = [sys.executable, str(SCRIPT)]
        if observe_wait:
            self.wait_entered = self.home / "long-poll-entered"
            script = '''
import sys
from pathlib import Path
from tests.support import watch
marker = Path(sys.argv.pop(1))
original_wait = watch.threading.Condition.wait
def observed_wait(condition, timeout=None):
    caller = sys._getframe(1)
    if caller.f_code.co_name == "wait_for_rows":
        handler = caller.f_locals["self"]
        if handler.headers.get("X-Test-Wait") == "1":
            marker.touch()
    return original_wait(condition, timeout)
watch.threading.Condition.wait = observed_wait
watch.main()
'''
            command = [sys.executable, "-c", script, str(self.wait_entered)]
        child = subprocess.Popen([*command, "--home", str(self.home), "--web",
                                  "--no-open", "--port", "0", "-n", "2"], cwd=SCRIPT.parent,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        def cleanup():
            if child.poll() is None:
                child.send_signal(signal.SIGINT)
            child.communicate(timeout=5)
        self.addCleanup(cleanup)
        url = child.stdout.readline().split()[0].removeprefix("实时表格：")
        self.port = int(url.rsplit(":", 1)[1])
        status, tag, body = self.fetch()
        deadline = time.monotonic() + 4
        while body.get("loading") and time.monotonic() < deadline:
            status, tag, body = self.fetch(tag, 4)
            self.assertEqual(status, 200)
        self.assertEqual(status, 200)
        self.assertFalse(body.get("loading"))
        return child

    def fetch(self, etag=None, wait=None, ready=None, delta=False, wait_marker=False):
        headers = {"If-None-Match": etag} if etag else {}
        if delta:
            headers["X-Row-Delta"] = "1"
        if wait is not None:
            headers["Prefer"] = "wait=" + str(wait)
        if wait_marker:
            headers["X-Test-Wait"] = "1"
        with contextlib.closing(http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)) as connection:
            connection.request("GET", "/api/rows", headers=headers)
            if ready:
                ready.set()
            response = connection.getresponse()
            body = response.read()
            return response.status, response.getheader("ETag"), json.loads(body) if response.status == 200 else body

    def test_long_poll_notifies_all_clients_and_does_not_block_reads(self):
        self.start_web()
        status, etag, original = self.fetch()
        self.assertEqual(status, 200)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            ready = [threading.Event(), threading.Event()]
            waiting = [pool.submit(self.fetch, etag, 4, started) for started in ready]
            for started in ready:
                self.assertTrue(started.wait(2))
            start = time.monotonic()
            self.assertEqual(self.fetch(etag)[0], 304)
            self.assertLess(time.monotonic() - start, 1)
            self.assertFalse(any(request.done() for request in waiting))
            self.link("r2")
            start = time.monotonic()
            self.append(event("r2"))
            results = [request.result(timeout=3) for request in waiting]
        self.assertLess(time.monotonic() - start, 2.5)
        self.assertEqual(results[0], results[1])
        status, updated_etag, updated = results[0]
        self.assertEqual(status, 200)
        self.assertNotEqual(etag, updated_etag)
        self.assertEqual([row["response_id"] for row in updated["rows"]], ["r2", "r1"])
        self.assertNotEqual(original["updated_at"], updated["updated_at"])
        self.assertEqual(self.fetch(etag, 25)[1], updated_etag)

    def test_long_poll_timeout_disconnect_and_authenticated_stop(self):
        child = self.start_web(observe_wait=True)
        _, etag, _ = self.fetch()
        start = time.monotonic()
        status, unchanged_etag, body = self.fetch(etag, 1)
        self.assertEqual((status, unchanged_etag, body), (304, etag, b""))
        self.assertGreaterEqual(time.monotonic() - start, 0.8)
        abandoned = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        abandoned.request("GET", "/api/rows", headers={"If-None-Match": etag, "Prefer": "wait=25"})
        abandoned.close()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            ready = threading.Event()
            waiting = pool.submit(self.fetch, etag, 25, ready, wait_marker=True)
            self.assertTrue(ready.wait(2))
            deadline = time.monotonic() + 2
            while not self.wait_entered.exists() and time.monotonic() < deadline:
                time.sleep(0.005)
            self.assertTrue(self.wait_entered.exists(), "request never entered the server's wait")
            start = time.monotonic()
            self.assertEqual(watch.control(self.port)["pid"], child.pid)
            self.assertEqual(watch.control(self.port, "stop")["pid"], child.pid)
            self.assertEqual(waiting.result(timeout=3)[0], 503)
        _, stderr = child.communicate(timeout=3)
        self.assertLess(time.monotonic() - start, 3)
        self.assertEqual(child.returncode, 0, stderr)
        self.assertNotIn("Traceback", stderr)
        self.assertFalse(watch.control_path(self.port).exists())

    def test_delta_delivery_preserves_legacy_api_and_handles_eviction(self):
        self.start_web()
        _, original_tag, original = self.fetch()
        self.link("r2")
        self.append(event("r2"))
        _, second_tag, delta = self.fetch(original_tag, 4, delta=True)
        self.assertEqual(delta["base"], original_tag)
        self.assertEqual(delta["order"], ["r2", "r1"])
        self.assertEqual([row["response_id"] for row in delta["rows"]], ["r2"])
        old_rows = {row["response_id"]: row for row in original["rows"]}
        old_rows.update((row["response_id"], row) for row in delta["rows"])
        _, _, full = self.fetch()
        self.assertNotIn("base", full)
        self.assertEqual([old_rows[rid] for rid in delta["order"]], full["rows"])
        self.link("r3")
        self.append(event("r3"))
        _, third_tag, latest = self.fetch(second_tag, 4, delta=True)
        self.assertEqual(latest["order"], ["r3", "r2"])
        self.assertEqual([row["response_id"] for row in latest["rows"]], ["r3"])
        self.assertEqual(self.fetch(third_tag, delta=True)[0], 304)
        _, _, recovered = self.fetch('"prior-process-8"', delta=True)
        self.assertNotIn("base", recovered)
        self.assertEqual([row["response_id"] for row in recovered["rows"]], ["r3", "r2"])
        self.link("r4")
        self.link("r5")
        self.append(event("r4") + event("r5"))
        _, _, replaced = self.fetch(third_tag, 4, delta=True)
        self.assertNotIn("base", replaced)
        self.assertEqual([row["response_id"] for row in replaced["rows"]], ["r5", "r4"])


class ServerResponsivenessTests(LogFixture):
    def start_server(self, summary, expected_error=None, open_browser=False):
        ready = threading.Event()
        observed = []
        errors = []

        class ObservedServer(watch.ThreadingHTTPServer):
            def __init__(server, *args, **kwargs):
                super().__init__(*args, **kwargs)
                server.requests_changed = threading.Condition()
                server.active_requests = 0
                observed.append(server)
                ready.set()

            def process_request_thread(server, *args):
                with server.requests_changed:
                    server.active_requests += 1
                    server.requests_changed.notify_all()
                try:
                    super().process_request_thread(*args)
                finally:
                    with server.requests_changed:
                        server.active_requests -= 1
                        server.requests_changed.notify_all()

        def run():
            try:
                watch.serve(summary, 0, 200, open_browser)
            except Exception as error:
                errors.append(error)

        with mock.patch.object(watch, "ThreadingHTTPServer", ObservedServer):
            thread = threading.Thread(target=run, daemon=True)
            thread.start()
            self.assertTrue(ready.wait(2))
        server = observed[0]
        self.port = server.server_port

        def cleanup():
            if thread.is_alive():
                watch.control(self.port, "stop")
            thread.join(3)
            self.assertFalse(thread.is_alive())
            self.assertEqual([str(error) for error in errors], [expected_error] if expected_error else [])
        self.addCleanup(cleanup)
        return thread, server

    def fetch(self, headers=None, path='/api/rows'):
        with contextlib.closing(http.client.HTTPConnection('127.0.0.1', self.port, timeout=1)) as connection:
            connection.request('GET', path, headers=headers or {})
            response = connection.getresponse()
            return response.status, response.getheader('ETag'), response.read()

    def test_optional_database_failure_keeps_web_rows_available(self):
        runtime = self.home / "runtime"
        patch = mock.patch.object(watch, "control_path", side_effect=lambda port: runtime / f"{port}.json")
        patch.start()
        self.addCleanup(patch.stop)
        (self.home / "logs_2.sqlite").write_bytes(b"not a sqlite database")
        self.append(event("r1"))
        self.start_server(self.summary())
        status, tag, raw = self.fetch()
        deadline = time.monotonic() + 4
        while json.loads(raw).get("loading") and time.monotonic() < deadline:
            status, tag, raw = self.fetch({"If-None-Match": tag, "Prefer": "wait=1"})
            self.assertEqual(status, 200)
        self.assertEqual(status, 200)
        payload = json.loads(raw)
        self.assertEqual(payload["rows"][0]["response_id"], "r1")
        self.assertIsNone(payload["diagnostics"]["blocked_sqlite_log_insert"])
        self.assertTrue(payload["diagnostics"]["sqlite_log_error"])

    def test_non_ascii_control_authorization_returns_forbidden(self):
        runtime = self.home / "runtime"
        patch = mock.patch.object(watch, "control_path", side_effect=lambda port: runtime / f"{port}.json")
        patch.start()
        self.addCleanup(patch.stop)
        self.start_server(self.summary())
        for authorization in ("Bearer é", "Bearer wrong-token"):
            with self.subTest(authorization=authorization), \
                 contextlib.closing(http.client.HTTPConnection("127.0.0.1", self.port, timeout=2)) as connection:
                connection.request("POST", "/api/control/stop", headers={"Authorization": authorization})
                response = connection.getresponse()
                self.assertEqual(response.status, 403)
                response.read()
        self.assertEqual(watch.control(self.port)["pid"], os.getpid())

    def test_blocked_browser_open_does_not_block_status_reuse_or_stop(self):
        runtime = self.home / "runtime"
        patch = mock.patch.object(watch, "control_path", side_effect=lambda port: runtime / f"{port}.json")
        patch.start()
        self.addCleanup(patch.stop)
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()

        def blocked_open(url):
            entered.set()
            release.wait(8)
            finished.set()

        with mock.patch.object(watch.webbrowser, "open", side_effect=blocked_open):
            thread, _ = self.start_server(self.summary(), open_browser=True)
            self.addCleanup(release.set)
            try:
                self.assertTrue(entered.wait(2))
                self.assertEqual(self.fetch(path="/")[0], 200)
                self.assertEqual(self.fetch()[0], 200)
                self.assertEqual(watch.control(self.port, timeout=1)["pid"], os.getpid())
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    reuse = pool.submit(watch.serve, self.summary(), self.port, 200, False)
                    try:
                        reuse.result(timeout=2)
                    except BaseException:
                        release.set()
                        raise
                self.assertEqual(watch.control(self.port, "stop", timeout=1)["pid"], os.getpid())
                thread.join(1)
                self.assertFalse(thread.is_alive())
                self.assertFalse(watch.control_path(self.port).exists())
                self.assertFalse(release.is_set())
            finally:
                release.set()
                self.assertTrue(finished.wait(2))

    def test_reuse_process_finishes_opening_browser_before_exit(self):
        runtime = self.home / "runtime"
        patch = mock.patch.object(watch, "control_path", side_effect=lambda port: runtime / f"{port}.json")
        patch.start()
        self.addCleanup(patch.stop)
        self.start_server(self.summary())
        marker = self.home / "browser-opened"
        bootstrap = """
import importlib.util
from pathlib import Path
import sys
import time
script, home, runtime, port, marker = sys.argv[1:]
spec = importlib.util.spec_from_file_location('viewer', script)
viewer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(viewer)
viewer.control_path = lambda port: Path(runtime) / f'{port}.json'
def delayed_open(url):
    time.sleep(0.2)
    Path(marker).write_text(url)
viewer.webbrowser.open = delayed_open
sys.argv = [script, '--home', home, '--web', '--port', port]
viewer.main()
"""
        result = subprocess.run([sys.executable, "-B", "-c", bootstrap, str(SCRIPT), str(self.home),
                                 str(runtime), str(self.port), str(marker)],
                                capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("未重复启动", result.stdout)
        self.assertTrue(marker.is_file())
        self.assertEqual(marker.read_text(), f"http://127.0.0.1:{self.port}")

    def test_initial_progress_is_visible_before_reading_linking_and_metadata_finish(self):
        self.context()
        self.link("r1")
        self.link("r2")
        self.append(event("r1") + event("r2"))
        summary = self.summary()
        rollout = self.home / "thread-a.jsonl"
        entered = {phase: threading.Event() for phase in ("logs", "links", "metadata")}
        release = {phase: threading.Event() for phase in entered}
        read_appended, ro_database, snapshot_type = watch.read_appended, watch.ro_database, watch.RowSnapshot
        snapshots = []

        def pause(phase):
            entered[phase].set()
            if not release[phase].wait(8):
                raise AssertionError(phase + " phase was not released")

        def read(path, *args, **kwargs):
            for index, line in enumerate(read_appended(path, *args, **kwargs)):
                yield line
                if index == 0 and path in (self.log, rollout):
                    pause("logs" if path == self.log else "links")

        def database(path):
            if path == self.history:
                pause("metadata")
            return ro_database(path)

        def snapshot(*args, **kwargs):
            result = snapshot_type(*args, **kwargs)
            snapshots.append(result)
            return result

        with mock.patch.object(watch, "read_appended", read), \
             mock.patch.object(watch, "ro_database", database), \
             mock.patch.object(watch, "RowSnapshot", snapshot), \
             mock.patch.object(watch, "PROGRESS_INTERVAL", 0), \
             mock.patch.object(watch, "control_path", side_effect=lambda port: self.home / f"runtime-{port}.json"):
            thread, server = self.start_server(summary)
            try:
                self.assertTrue(entered["logs"].wait(2))
                status, reading_tag, raw = self.fetch()
                reading = json.loads(raw)
                self.assertEqual(status, 200)
                self.assertTrue(reading["loading"])
                self.assertEqual((reading["rows"], reading["retained_count"]), ([], 1))
                self.assertIn("正在读取日志", reading["status"])
                self.assertIn("已处理 0/1 个文件", reading["status"])
                self.assertIn("字节", reading["status"])
                self.assertNotIn(str(self.log), summary.cursors)
                retained_snapshot = next(item for item in snapshots if item.etag == reading_tag)
                retained_body = retained_snapshot.encode()

                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    waiting = pool.submit(self.fetch, {"If-None-Match": reading_tag, "Prefer": "wait=1"})
                    with server.requests_changed:
                        self.assertTrue(server.requests_changed.wait_for(lambda: server.active_requests == 1, 1))
                    self.assertFalse(waiting.done())
                    release["logs"].set()
                    status, changed_tag, raw = waiting.result(timeout=2)
                self.assertEqual(status, 200)
                self.assertNotEqual(changed_tag, reading_tag)
                self.assertTrue(json.loads(raw)["loading"])

                self.assertTrue(entered["links"].wait(2))
                status, linking_tag, raw = self.fetch()
                linking = json.loads(raw)
                self.assertNotEqual(linking_tag, reading_tag)
                self.assertTrue(linking["loading"])
                self.assertEqual(linking["retained_count"], 2)
                self.assertIn("正在关联会话", linking["status"])
                self.assertIn("字节", linking["status"])
                self.assertEqual(summary.cursors[str(self.log)][1], self.log.stat().st_size)
                self.assertNotIn(str(rollout), summary.rollout_cursors)

                release["links"].set()
                self.assertTrue(entered["metadata"].wait(2))
                status, metadata_tag, raw = self.fetch()
                metadata = json.loads(raw)
                self.assertNotEqual(metadata_tag, linking_tag)
                self.assertEqual((metadata["status"], metadata["loading"], metadata["rows"]),
                                 ("正在读取会话元数据", True, []))
                release["metadata"].set()
                deadline = time.monotonic() + 3
                tag, payload = metadata_tag, metadata
                while payload["loading"] and time.monotonic() < deadline:
                    status, tag, raw = self.fetch({"If-None-Match": tag, "Prefer": "wait=1"})
                    self.assertEqual(status, 200)
                    payload = json.loads(raw)
                self.assertFalse(payload["loading"])
                self.assertEqual(payload["retained_count"], 2)
                self.assertEqual([row["response_id"] for row in payload["rows"]], ["r2", "r1"])
                self.assertEqual(retained_snapshot.data, reading)
                self.assertEqual(retained_snapshot.encode(), retained_body)
                self.assertIsNone(summary.progress)
            finally:
                for gate in release.values():
                    gate.set()
                if thread.is_alive():
                    watch.control(self.port, "stop")
                thread.join(3)

    def test_slow_collection_keeps_page_status_and_stop_responsive(self):
        summary = self.summary()
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        poll = summary.poll
        def slow_poll():
            entered.set()
            release.wait(8)
            try:
                return poll()
            finally:
                finished.set()
        summary.poll = slow_poll
        snapshot_patch = mock.patch.object(watch, "RowSnapshot", wraps=watch.RowSnapshot)
        snapshots = snapshot_patch.start()
        self.addCleanup(snapshot_patch.stop)
        thread, _ = self.start_server(summary)
        self.addCleanup(release.set)
        self.assertTrue(entered.wait(2))
        self.assertEqual(self.fetch(path='/')[0], 200)
        status, _, raw = self.fetch()
        self.assertEqual(status, 200)
        self.assertTrue(json.loads(raw)['loading'])
        self.assertEqual(watch.control(self.port)['pid'], os.getpid())
        before_stop = snapshots.call_count
        watch.control(self.port, 'stop')
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertFalse(release.is_set())
        release.set()
        self.assertTrue(finished.wait(2))
        self.assertEqual(snapshots.call_count, before_stop, "late progress was published after shutdown")

    def test_disconnected_long_poll_releases_its_request_thread(self):
        self.append(event('r1'))
        _, server = self.start_server(self.summary())
        status, tag, body = self.fetch()
        deadline = time.monotonic() + 4
        while json.loads(body).get('loading') and time.monotonic() < deadline:
            status, tag, body = self.fetch({'If-None-Match': tag, 'Prefer': 'wait=1'})
            self.assertEqual(status, 200)
        self.assertEqual(status, 200)
        with server.requests_changed:
            self.assertTrue(server.requests_changed.wait_for(lambda: server.active_requests == 0, 1))
        client = http.client.HTTPConnection('127.0.0.1', self.port, timeout=1)
        client.request('GET', '/api/rows', headers={'If-None-Match': tag, 'Prefer': 'wait=25'})
        with server.requests_changed:
            self.assertTrue(server.requests_changed.wait_for(lambda: server.active_requests == 1, 1))
        client.close()
        with server.requests_changed:
            self.assertTrue(server.requests_changed.wait_for(lambda: server.active_requests == 0, 2))

    def test_stop_drains_waiting_503_writes_and_releases_failed_writes(self):
        for fail_write in (False, True):
            with self.subTest(fail_write=fail_write):
                self.append(event("r1"))
                thread, _ = self.start_server(self.summary())
                status, tag, raw = self.fetch()
                deadline = time.monotonic() + 3
                while json.loads(raw).get("loading") and time.monotonic() < deadline:
                    status, tag, raw = self.fetch({"If-None-Match": tag, "Prefer": "wait=1"})
                self.assertEqual(status, 200)
                entered, writing, draining, release = (threading.Event() for _ in range(4))
                wait_condition = []
                original_wait = threading.Condition.wait
                original_error = watch.BaseHTTPRequestHandler.send_error

                def observed_wait(condition, timeout=None):
                    caller = sys._getframe(1)
                    if caller.f_code.co_name == "wait_for_rows" and caller.f_locals["self"].headers.get("X-Test-Wait"):
                        wait_condition[:] = [condition]
                        entered.set()
                    elif wait_condition and condition is wait_condition[0] and caller.f_code.co_name == "wait_for":
                        draining.set()
                    return original_wait(condition, timeout)

                def gated_error(handler, code, *args, **kwargs):
                    if code == 503:
                        writing.set()
                        if not release.wait(5):
                            raise TimeoutError("synthetic 503 gate timed out")
                        if fail_write:
                            raise ConnectionResetError("synthetic disconnected waiter")
                    return original_error(handler, code, *args, **kwargs)

                with mock.patch.object(threading.Condition, "wait", observed_wait), \
                     mock.patch.object(watch.BaseHTTPRequestHandler, "send_error", gated_error), \
                     concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    waiting = pool.submit(LongPollTests.fetch, self, tag, 25, wait_marker=True)
                    try:
                        self.assertTrue(entered.wait(2), "request did not enter the server's wait")
                        watch.control(self.port, "stop")
                        self.assertTrue(writing.wait(2))
                        self.assertTrue(draining.wait(2), "serve returned before draining its waiting request")
                        self.assertTrue(thread.is_alive(), "serve returned before the gated 503 write")
                        release.set()
                        if fail_write:
                            with self.assertRaises(http.client.RemoteDisconnected):
                                waiting.result(timeout=2)
                        else:
                            response = waiting.result(timeout=2)
                            self.assertEqual(response[0], 503)
                            self.assertIn(b"Viewer is stopping", response[2])
                        thread.join(2)
                        self.assertFalse(thread.is_alive())
                    finally:
                        release.set()
                        if thread.is_alive():
                            watch.control(self.port, "stop")
                        thread.join(2)

    def test_stop_does_not_wait_for_incomplete_request_headers(self):
        thread, server = self.start_server(self.summary())
        with contextlib.closing(watch.socket.create_connection(("127.0.0.1", self.port), timeout=2)) as empty, \
             contextlib.closing(watch.socket.create_connection(("127.0.0.1", self.port), timeout=2)) as partial:
            partial.sendall(b"GET /api/rows HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Slow: ")
            with server.requests_changed:
                self.assertTrue(server.requests_changed.wait_for(lambda: server.active_requests == 2, 2))
            watch.control(self.port, "stop")
            thread.join(1)
            self.assertFalse(thread.is_alive(), "incomplete headers delayed stopping")

    def test_normal_long_poll_update_leaves_shutdown_drain_before_encoding(self):
        self.append(event("r1"))
        thread, _ = self.start_server(self.summary())
        status, tag, raw = self.fetch()
        deadline = time.monotonic() + 3
        while json.loads(raw).get("loading") and time.monotonic() < deadline:
            status, tag, raw = self.fetch({"If-None-Match": tag, "Prefer": "wait=1"})
        self.assertEqual(status, 200)
        entered, encoding, release = (threading.Event() for _ in range(3))
        original_wait = threading.Condition.wait
        original_encode = watch.RowSnapshot.encode

        def observed_wait(condition, timeout=None):
            caller = sys._getframe(1)
            if caller.f_code.co_name == "wait_for_rows" and caller.f_locals["self"].headers.get("X-Test-Wait"):
                entered.set()
            return original_wait(condition, timeout)

        def gated_encode(snapshot, requested_tag=None, delta=False):
            if requested_tag == tag:
                encoding.set()
                if not release.wait(5):
                    raise TimeoutError("synthetic encode gate timed out")
            return original_encode(snapshot, requested_tag, delta)

        with mock.patch.object(threading.Condition, "wait", observed_wait), \
             mock.patch.object(watch.RowSnapshot, "encode", gated_encode), \
             concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            waiting = pool.submit(LongPollTests.fetch, self, tag, 25, wait_marker=True)
            try:
                self.assertTrue(entered.wait(2))
                self.append(event("r2"))
                self.assertTrue(encoding.wait(3))
                watch.control(self.port, "stop")
                thread.join(1)
                self.assertFalse(thread.is_alive(), "normal update remained in the shutdown drain")
                release.set()
                self.assertEqual(waiting.result(timeout=2)[0], 200)
            finally:
                release.set()
                if thread.is_alive():
                    watch.control(self.port, "stop")
                thread.join(2)

    def test_collector_failure_stops_the_server_and_reports_the_error(self):
        summary = self.summary()
        entered, release = threading.Event(), threading.Event()
        def failed_poll():
            entered.set()
            release.wait(5)
            raise ValueError('synthetic collector failure')
        summary.poll = failed_poll
        thread, _ = self.start_server(summary, expected_error='synthetic collector failure')
        self.addCleanup(release.set)
        self.assertTrue(entered.wait(2))
        self.assertEqual(watch.control(self.port)['pid'], os.getpid())
        release.set()
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertFalse(watch.control_path(self.port).exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
