"""Command output, configuration, diagnostics, and service lifecycle."""

import contextlib
import errno
import io
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from tests.support import LogFixture, SCRIPT, event, watch


class CommandLineTests(LogFixture):
    def test_once_reads_relative_config_directory_and_last_n(self):
        self.link("r1")
        self.link("r2")
        self.append(event("r1", model="first") + event("r2", model="last"))
        result = subprocess.run([sys.executable, str(SCRIPT), "--home", str(self.home), "--once", "-n", "1"],
                                capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(result.stdout.splitlines()), 2)
        self.assertIn("last", result.stdout)
        self.assertNotIn("first", result.stdout)
        self.assertEqual(result.stderr, "")

    def test_default_follows_new_responses_and_ctrl_c_exits(self):
        child = subprocess.Popen([sys.executable, str(SCRIPT), "--home", str(self.home)],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertIn("对话名称", child.stdout.readline())
            self.link("streamed")
            self.append(event("streamed"))
            time.sleep(1.3)
            self.assertIsNone(child.poll())
            child.send_signal(signal.SIGINT)
            stdout, stderr = child.communicate(timeout=5)
            self.assertEqual(child.returncode, 0, stderr)
            self.assertEqual(stdout.split(), ["手动名称", "1", "test-model", "high"])
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=5)

    def test_n_zero_skips_startup_responses_even_if_link_is_delayed(self):
        self.append(event("old"))
        child = subprocess.Popen([sys.executable, str(SCRIPT), "--home", str(self.home), "-n", "0"],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertIn("对话名称", child.stdout.readline())
            time.sleep(0.2)
            self.link("old")
            self.link("new")
            self.append(event("new", model="new-response"))
            time.sleep(1.3)
            child.send_signal(signal.SIGINT)
            stdout, stderr = child.communicate(timeout=5)
            self.assertEqual(child.returncode, 0, stderr)
            self.assertEqual(stdout.split(), ["手动名称", "1", "new-response", "high"])
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=5)


class ConfigurationTests(unittest.TestCase):
    def test_default_desktop_path_uses_expanded_and_resolved_home(self):
        with tempfile.TemporaryDirectory() as folder:
            user_home = Path(folder)
            actual_home = user_home / "profile"
            actual_home.mkdir()
            (user_home / ".codex").symlink_to(actual_home, target_is_directory=True)
            desktop = user_home / "desktop-logs"
            env = {"HOME": str(user_home), "CODEX_HOME": "~/.codex", "CODEX_SQLITE_HOME": ""}
            choices = ([], ["--home", "~/.codex"], ["--home", str(actual_home)],
                       ["--home", str(user_home / ".codex")])
            with mock.patch.dict(os.environ, env), mock.patch.object(watch, "DESKTOP_LOG_DIR", desktop):
                for flags in choices:
                    with self.subTest(flags=flags), mock.patch.object(watch, "Summary") as summary, \
                         mock.patch.object(watch, "serve"), \
                         mock.patch.object(sys, "argv", [str(SCRIPT), "--web", "--no-open", *flags]):
                        watch.main()
                        self.assertEqual(summary.call_args.args[0], actual_home.resolve())
                        self.assertEqual(summary.call_args.args[4], desktop)


class DiagnosticsTests(LogFixture):
    def test_optional_database_lock_is_unknown_and_recovers(self):
        path = self.home / "logs_2.sqlite"
        summary = self.summary()
        self.assertIs(watch.diagnostics(summary)["blocked_sqlite_log_insert"], False)
        with contextlib.closing(sqlite3.connect(path)) as db:
            db.execute("CREATE TABLE logs (message TEXT)")
            db.execute("CREATE TRIGGER codex_block_logs_insert BEFORE INSERT ON logs BEGIN SELECT RAISE(IGNORE); END")
            db.commit()
            self.assertIs(watch.diagnostics(summary)["blocked_sqlite_log_insert"], True)
            db.execute("BEGIN EXCLUSIVE")
            result = watch.diagnostics(summary)
            self.assertIsNone(result["blocked_sqlite_log_insert"])
            self.assertTrue(result["sqlite_log_error"])
            db.rollback()
        result = watch.diagnostics(summary)
        self.assertIs(result["blocked_sqlite_log_insert"], True)
        self.assertNotIn("sqlite_log_error", result)

    def test_corrupt_optional_database_does_not_hide_json_rows(self):
        (self.home / "logs_2.sqlite").write_bytes(b"not a sqlite database")
        self.append(event("r1"))
        result = subprocess.run([sys.executable, "-B", str(SCRIPT), "--home", str(self.home),
                                 "--no-desktop", "--once", "--json"],
                                capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["rows"][0]["response_id"], "r1")
        self.assertIsNone(payload["diagnostics"]["blocked_sqlite_log_insert"])
        self.assertTrue(payload["diagnostics"]["sqlite_log_error"])

    def test_json_command_preserves_isolated_surrogates(self):
        message = "broken\ud800 \udcff 中文"
        self.append(("data: " + json.dumps({"type": "response.failed", "response": {
            "id": "r1", "error": {"message": message}}}) + "\n").encode())
        result = subprocess.run([sys.executable, "-B", str(SCRIPT), "--home", str(self.home),
                                 "--no-desktop", "--once", "--json"],
                                capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout.decode("utf-8"))
        self.assertEqual(payload["rows"][0]["error_message"], message)
        self.assertIn("中文".encode("utf-8"), result.stdout)

    def test_optional_database_access_error_is_unknown(self):
        (self.home / "logs_2.sqlite").touch()
        with mock.patch.object(watch, "ro_database", side_effect=PermissionError("synthetic access denied")):
            result = watch.diagnostics(self.summary())
        self.assertIsNone(result["blocked_sqlite_log_insert"])
        self.assertEqual(result["sqlite_log_error"], "synthetic access denied")


class LifecycleTests(unittest.TestCase):
    def test_start_status_reuse_authenticated_stop_and_stale_state(self):
        import urllib.request
        import urllib.error
        import urllib.parse
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        home = Path(temp.name) / "nonexistent-fixture"
        env = dict(os.environ, CODEX_SQLITE_HOME=str(home))
        child = subprocess.Popen([sys.executable, str(SCRIPT), "--web", "--no-open", "--port", "0",
                                  "--home", str(home)],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
        try:
            url = child.stdout.readline().split()[0].removeprefix("实时表格：")
            port = urllib.parse.urlsplit(url).port
            path = watch.control_path(port)
            state = path.read_text()
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            def command(*args):
                return subprocess.run([sys.executable, str(SCRIPT), "--home", str(home),
                                       "--port", str(port), *args],
                                      capture_output=True, text=True, timeout=8, env=env)
            result = command("--status")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(str(child.pid), result.stdout)
            result = command("--web", "--no-open")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("未重复启动", result.stdout)
            req = urllib.request.Request(url + "/api/control/stop", data=b"", method="POST")
            with self.assertRaises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(req, timeout=3)
            self.assertEqual(error.exception.code, 403)
            self.assertIsNone(child.poll())
            result = command("--stop")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("已停止", result.stdout)
            child.communicate(timeout=5)
            self.assertEqual(child.returncode, 0)
            self.assertFalse(path.exists())
            self.assertIn("未运行", command("--stop").stdout)
            path.write_text(state)
            try:
                self.assertIn("未运行", command("--status").stdout)
            finally:
                path.unlink()
        finally:
            if child.poll() is None:
                child.send_signal(signal.SIGINT)
                child.communicate(timeout=5)


class StartupRaceTests(LogFixture):
    def test_concurrent_start_reuses_the_authenticated_winner(self):
        bound, release, retrying = threading.Event(), threading.Event(), threading.Event()
        observed, errors = [], []
        runtime = self.home / "runtime"
        real_control = watch.control

        class DelayedServer(watch.ThreadingHTTPServer):
            def __init__(server, *args, **kwargs):
                super().__init__(*args, **kwargs)
                observed.append(server)
                bound.set()
                if not release.wait(5):
                    server.server_close()
                    raise TimeoutError("synthetic startup gate timed out")

        def control(port, *args, **kwargs):
            if not (runtime / f"{port}.json").exists():
                retrying.set()
            return real_control(port, *args, **kwargs)

        def start(port):
            try:
                watch.serve(self.summary(), port, 20, False)
            except Exception as error:
                errors.append(error)

        output = io.StringIO()
        with mock.patch.object(watch, "control_path", side_effect=lambda port: runtime / f"{port}.json"), \
             mock.patch.object(watch, "ThreadingHTTPServer", DelayedServer), \
             mock.patch.object(watch, "control", side_effect=control), contextlib.redirect_stdout(output):
            winner = threading.Thread(target=start, args=(0,), daemon=True)
            winner.start()
            loser = None
            try:
                self.assertTrue(bound.wait(2))
                port = observed[0].server_port
                loser = threading.Thread(target=start, args=(port,), daemon=True)
                loser.start()
                self.assertTrue(retrying.wait(2))
                release.set()
                loser.join(3)
                self.assertFalse(loser.is_alive())
                self.assertEqual(errors, [])
                self.assertIn("未重复启动", output.getvalue())
                self.assertTrue(winner.is_alive())
                self.assertEqual(real_control(port)["script"], str(SCRIPT.resolve()))
            finally:
                release.set()
                if observed:
                    deadline = time.monotonic() + 3
                    while winner.is_alive() and time.monotonic() < deadline:
                        if real_control(observed[0].server_port, "stop"):
                            break
                        time.sleep(0.01)
                winner.join(3)
                if loser is not None:
                    loser.join(3)
            self.assertFalse(winner.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(list(runtime.glob("*.json")), [])

    def test_occupied_foreign_port_is_never_reused_or_stopped(self):
        requests = []
        runtime = self.home / "runtime"
        runtime.mkdir()

        class ForeignHandler(watch.BaseHTTPRequestHandler):
            def do_POST(handler):
                requests.append(handler.path)
                data = json.dumps({"pid": os.getpid(), "url": "http://127.0.0.1",
                                   "script": str(SCRIPT) + ".foreign"}).encode()
                handler.send_response(200)
                handler.send_header("Content-Length", str(len(data)))
                handler.end_headers()
                handler.wfile.write(data)

            def log_message(handler, *args):
                pass

        with watch.ThreadingHTTPServer(("127.0.0.1", 0), ForeignHandler) as foreign, \
             mock.patch.object(watch, "control_path", side_effect=lambda port: runtime / f"{port}.json"):
            thread = threading.Thread(target=foreign.serve_forever, daemon=True)
            thread.start()
            try:
                started = time.monotonic()
                with self.assertRaises(OSError) as occupied:
                    watch.serve(self.summary(), foreign.server_port, 20, False)
                self.assertEqual(occupied.exception.errno, errno.EADDRINUSE)
                self.assertLess(time.monotonic() - started, 2)
                self.assertEqual(requests, [])
                state = watch.control_path(foreign.server_port)
                state.write_text(json.dumps({"token": "synthetic-token", "pid": os.getpid()}))
                with self.assertRaisesRegex(OSError, "不是当前查看器"):
                    watch.serve(self.summary(), foreign.server_port, 20, False)
                self.assertTrue(requests)
                self.assertEqual(set(requests), {"/api/control/status"})
                requests.clear()
                with self.assertRaisesRegex(OSError, "不是当前查看器"):
                    watch.control(foreign.server_port, "stop")
                self.assertEqual(requests, ["/api/control/status"])
                self.assertTrue(thread.is_alive())
                self.assertTrue(state.exists())
            finally:
                foreign.shutdown()
                thread.join(2)

    def test_non_conflict_bind_error_is_not_retried(self):
        with mock.patch.object(watch.socketserver.TCPServer, "server_bind", side_effect=OSError(errno.EACCES, "synthetic denied")), \
             mock.patch.object(watch, "control") as control:
            with self.assertRaises(OSError) as denied:
                watch.serve(self.summary(), 12345, 20, False)
        self.assertEqual(denied.exception.errno, errno.EACCES)
        control.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
