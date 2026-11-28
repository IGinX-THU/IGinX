"""Deterministic Pemjax (>= 0.2.4) regressions: no database, model download or torch required."""
import ctypes
import importlib
import importlib.abc
import importlib.util
import math
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import time
import types

import timeout_handler as timeout

PROBES = []
IMPORT_STARTED = threading.Event()
IMPORT_RELEASE = threading.Event()
PACKAGE = "iginx_pemjax_slow_import"

# State of BlockingUDF, which PyUDF re-instantiates on every invocation.
UDF_BLOCK = threading.Event()
UDF_RELEASE = threading.Event()
UDF_LATE = []


class Probe:
    def __init__(self):
        self.release = threading.Event()
        self.hook_release = threading.Event()
        self.finished = threading.Event()
        self.count = 0
        self.late_write = False
        self.caught = False
        PROBES.append(self)

    def normal(self, data, args, kvargs):
        return [[1, 2, 3]]

    def spin(self, data=None, args=None, kvargs=None):
        try:
            until = time.monotonic() + 10
            while time.monotonic() < until:
                timeout.check_cancelled()
                self.count += 1
        finally:
            self.finished.set()

    def catch_exception(self, data, args, kvargs):
        try:
            self.spin()
        except Exception:
            self.caught = True
            return [[99]]

    def catch_baseexception(self, data=None, args=None, kvargs=None):
        try:
            self.spin()
        except BaseException:
            return "swallowed"

    def swallow_per_row(self, data=None, args=None, kvargs=None):
        # Cancellation checkpoints must be outside broad per-row handlers.
        try:
            until = time.monotonic() + 10
            while time.monotonic() < until:
                timeout.check_cancelled()
                try:
                    for _ in range(1000):
                        self.count += 1
                except:  # noqa: E722 - the pattern under test
                    pass
        finally:
            self.finished.set()

    def slow_return(self, data, args, kvargs):
        # A late return must still be reported as a timeout.
        return time.sleep(1.2) or [[7]]

    def fail_row(self, data, args, kvargs):
        raise ValueError("bad row in pemjax regression")

    def blocked(self):
        try:
            self.release.wait(10)
            timeout.check_cancelled()
            self.late_write = True
        finally:
            self.finished.set()

    def importing(self, data, args, kvargs):
        try:
            importlib.import_module(PACKAGE)
            timeout.check_cancelled()
            self.late_write = True
        finally:
            self.finished.set()

    def values(self):
        try:
            yield 1
            self.spin()
            yield 2
        finally:
            self.finished.set()


class PublicCheckpointProbe(Probe):
    def spin(self, data=None, args=None, kvargs=None):
        # The JNI wrapper imports timeout_handler under its legacy top-level name.
        from pemja.timeout_handler import check_cancelled
        try:
            until = time.monotonic() + 10
            while time.monotonic() < until:
                check_cancelled()
                self.count += 1
        finally:
            self.finished.set()


class BlockingUDF:
    """Invoked through PyUDF.invokePyUDF, which creates a new instance per call."""

    def transform(self, data, args, kvargs):
        if UDF_BLOCK.is_set():
            UDF_RELEASE.wait(10)  # A native wait the watchdog cannot interrupt.
            timeout.check_cancelled()
            UDF_LATE.append(id(self))  # Must never run once the deadline passed.
        return [[len(data)]]


class ProcessUDF:
    def __init__(self):
        self.calls = 0

    def transform(self, data, args, kvargs):
        self.calls += 1
        return [[len(data), self.calls]]


class SlowFinder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in (PACKAGE, PACKAGE + ".child"):
            return importlib.util.spec_from_loader(fullname, self, is_package=fullname == PACKAGE)
        return None

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        if module.__name__ == PACKAGE + ".child":
            module.parent = importlib.import_module(PACKAGE)
        else:
            module.child = importlib.import_module(PACKAGE + ".child")
            IMPORT_STARTED.set()
            IMPORT_RELEASE.wait(10)
            timeout.check_cancelled()
            module.ready = True


FINDER = SlowFinder()


def prepare_import():
    IMPORT_STARTED.clear()
    IMPORT_RELEASE.clear()
    sys.meta_path.insert(0, FINDER)


def verify_import(probe):
    assert IMPORT_STARTED.is_set(), "timeout did not occur inside the import"
    assert timeout.pending_tasks(), "live import was not tracked"
    IMPORT_RELEASE.set()
    assert timeout.wait_for_pending_tasks(5)
    module = importlib.import_module(PACKAGE)
    assert module.ready and module.child.parent is module
    assert module.child.parent.ready, "child retained a partially initialized parent"
    assert probe.finished.is_set() and not probe.late_write


def verify_stopped(probe):
    assert probe.finished.is_set(), "worker did not execute cleanup"
    assert not probe.caught, "ordinary except Exception swallowed cancellation"
    count = probe.count
    time.sleep(0.03)
    assert count == probe.count, "timed-out UDF still produced side effects"
    assert not timeout.pending_tasks()


def check_ident_and_rollback():
    assert timeout.get_thread_id(types.SimpleNamespace(ident=123, native_id=456, _thread_id=789)) == 123
    ready, release = threading.Event(), threading.Event()
    outcome = []

    def worker():
        try:
            ready.set()
            release.wait(3)
            outcome.append("ok")
        except BaseException as exc:
            outcome.append(type(exc).__name__)

    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert ready.wait(1)
        assert timeout.get_thread_id(thread) == thread.ident
    finally:
        release.set()
        thread.join(3)
    assert outcome == ["ok"], outcome
    # Never inject into the shared JNI interpreter just to test the unsafe
    # compatibility helper; the real C API is covered in an isolated process
    # by Pemjax's Python suite.
    original_api = timeout._set_async_exc
    calls = []

    def fake_api(ident, exception):
        calls.append((ident, exception))
        return 2 if len(calls) == 1 else 1

    try:
        assert original_api.argtypes == (ctypes.c_ulong, ctypes.py_object)
        timeout._set_async_exc = fake_api
        try:
            timeout._async_raise(123, timeout.TimeoutError)
            raise AssertionError("ambiguous cancellation was accepted")
        except RuntimeError:
            pass
        try:
            _ = calls[1][1].value
            raise AssertionError("rollback used Python None instead of C NULL")
        except ValueError:
            pass
    finally:
        timeout._set_async_exc = original_api
    return True


def check_pending_and_reuse():
    probe = Probe()
    task = timeout.ThreadWithTimeout(probe.blocked, cancel_grace=0.02)
    try:
        try:
            task.run_with_timeout(0.05)
            raise AssertionError("missing timeout")
        except timeout.TimeoutError as exc:
            assert not exc.terminated and exc.task is task
        assert task in timeout.pending_tasks()
        try:
            timeout.TimeoutSafeWrapper(probe, 1).normal([], [], {})
            raise AssertionError("reused a live timed-out object")
        except RuntimeError as exc:
            assert "still running" in str(exc)
    finally:
        probe.release.set()
        assert task.wait(3)
    assert probe.finished.is_set() and not probe.late_write
    assert timeout.TimeoutSafeWrapper(probe, 1).normal([], [], {}) == [[1, 2, 3]]
    return True


def check_blocking_interrupt():
    class BlockingHook(Probe):
        def interrupt(self):
            self.hook_release.wait(10)

    probe = BlockingHook()
    task = timeout.ThreadWithTimeout(probe.spin, cancel_grace=0.02)
    started = time.monotonic()
    try:
        try:
            task.run_with_timeout(0.05)
            raise AssertionError("missing timeout")
        except timeout.TimeoutError as exc:
            assert not exc.terminated
        assert time.monotonic() - started < 2, "interrupt blocked the watchdog"
        assert task in timeout.pending_tasks()
    finally:
        probe.hook_release.set()
        assert task.wait(3)
    return True


def check_swallowed_cancellation():
    task = timeout.ThreadWithTimeout(Probe().catch_baseexception)
    try:
        task.run_with_timeout(0.05)
        raise AssertionError("returned success after the deadline")
    except timeout.TimeoutError as exc:
        assert exc.terminated
    return True


def check_generator():
    probe = Probe()
    values = timeout.TimeoutSafeWrapper(probe, 0.05).values()
    assert next(values) == 1
    try:
        next(values)
        raise AssertionError("generator next() was not timed")
    except timeout.TimeoutError as exc:
        assert exc.terminated
    assert probe.finished.is_set()
    return True


def child_block(marker):
    pathlib.Path(marker).write_text("started", encoding="utf-8")
    threading.Event().wait(10)
    pathlib.Path(marker).write_text("late write", encoding="utf-8")


def child_success(value):
    return value + 1


def child_failure():
    raise ValueError("remote failure")


def child_values():
    yield 1


def child_spawn_descendant(started, late):
    # Inherited output handles must not keep timeout cleanup blocked.
    # The descendant writes "late" only if it survives the timeout.
    subprocess.Popen(
        [sys.executable, "-c",
         "import sys, time; time.sleep(6); open(sys.argv[1], 'w').write('late')", late],
        stdin=subprocess.DEVNULL, stdout=2, stderr=2)
    pathlib.Path(started).write_text("started", encoding="utf-8")
    time.sleep(30)


def _expect_timeout(call, terminated=True):
    try:
        call()
    except timeout.TimeoutError as exc:
        assert exc.terminated is terminated, "terminated=%s" % exc.terminated
        return exc
    raise AssertionError("missing timeout")


def _expect_raises(kind, call, text=""):
    try:
        call()
    except kind as exc:
        assert text in str(exc), str(exc)
        return exc
    raise AssertionError("expected " + kind.__name__)


def check_swallow_variants():
    def nested():
        until = time.monotonic() + 3
        while time.monotonic() < until:
            timeout.check_cancelled()
            try:
                try:
                    for _ in range(10 ** 5):
                        pass
                except:  # noqa: E722
                    [].append(1)
            except BaseException:
                pass

    def callee():
        for _ in range(10 ** 5):
            pass

    def swallowed_by_caller():
        until = time.monotonic() + 3
        while time.monotonic() < until:
            timeout.check_cancelled()
            try:
                callee()
            except BaseException:
                continue

    for work in (nested, swallowed_by_caller):
        task = timeout.ThreadWithTimeout(work)
        _expect_timeout(lambda: task.run_with_timeout(0.05))
        assert not task.thread.is_alive(), work.__name__ + " kept running"
    return True


def check_interrupt_releases_wait():
    class Hooked(Probe):
        def interrupt(self):
            self.release.set()

    probe = Hooked()
    task = timeout.ThreadWithTimeout(probe.blocked)
    _expect_timeout(lambda: task.run_with_timeout(0.05), terminated=True)
    assert probe.finished.is_set() and not probe.late_write
    return True


def check_generator_from_function():
    probe = Probe()
    values = timeout.timeout(0.05)(lambda: probe.values())()
    assert next(values) == 1
    _expect_timeout(lambda: next(values))
    assert probe.finished.is_set()
    return True


def check_exceptions_propagate():
    def exits():
        raise SystemExit(7)

    def fails():
        raise ValueError("udf failure")

    assert _expect_raises(SystemExit, timeout.timeout(1)(exits)).code == 7
    _expect_raises(ValueError, timeout.timeout(1)(fails), "udf failure")
    assert timeout.TimeoutSafeWrapper(Probe(), 1).normal([], [], {}) == [[1, 2, 3]]
    return True


def check_invalid_timeouts():
    for bad in (-1, math.nan, math.inf):
        _expect_raises(ValueError, lambda: timeout.timeout(bad))
        _expect_raises(ValueError, lambda: timeout.ThreadWithTimeout(int).run_with_timeout(bad))
    _expect_raises(ValueError, lambda: timeout.ThreadWithTimeout(int, cancel_grace=None))
    task = timeout.ThreadWithTimeout(int)
    assert task.run_with_timeout(1) == 0
    _expect_raises(RuntimeError, lambda: task.run_with_timeout(1), "only be run once")
    return True


def check_concurrent_calls():
    stuck, other = Probe(), Probe()
    outcome = []

    def slow_caller():
        try:
            timeout.TimeoutSafeWrapper(stuck, 0.2).blocked()
        except timeout.TimeoutError as exc:
            outcome.append(exc)

    caller = threading.Thread(target=slow_caller)
    caller.start()
    try:
        for _ in range(5):
            assert timeout.TimeoutSafeWrapper(other, 5).normal([], [], {}) == [[1, 2, 3]]
        caller.join(5)
        assert len(outcome) == 1 and not outcome[0].terminated, outcome
        assert timeout.TimeoutSafeWrapper(other, 5).normal([], [], {}) == [[1, 2, 3]]
    finally:
        stuck.release.set()
        caller.join(5)
    assert timeout.wait_for_pending_tasks(5) and not stuck.late_write
    return True


def check_process_kills_descendants(python_executable):
    with tempfile.TemporaryDirectory(prefix="iginx-pemjax-tree-") as directory:
        started = pathlib.Path(directory) / "started"
        late = pathlib.Path(directory) / "late"
        begin = time.monotonic()
        task = timeout.ProcessWithTimeout(
            child_spawn_descendant, (str(started), str(late)),
            python_executable=python_executable)
        _expect_timeout(lambda: task.run_with_timeout(3))
        elapsed = time.monotonic() - begin
        assert elapsed < 5, "descendant holding the pipes delayed the timeout: %.1fs" % elapsed
        assert started.exists(), "descendant was never started"
        time.sleep(max(0, 7.5 - (time.monotonic() - begin)))
        assert not late.exists(), "descendant survived the timeout"
    return True


def check_process_modes(python_executable):
    udf = ProcessUDF()
    safe = timeout.TimeoutSafeWrapper(udf, 20, mode="process", python_executable=python_executable)
    assert safe.transform([[1], [2]], [], {}) == [[2, 1]]
    assert udf.calls == 0, "process mode must not mutate the parent instance"
    decorated = timeout.timeout(20, mode="process", python_executable=python_executable)(child_success)
    assert decorated(1) == 2
    _expect_raises(ValueError, lambda: timeout.ProcessWithTimeout(
        child_failure, python_executable=python_executable).run_with_timeout(20), "remote failure")
    _expect_raises(TypeError, lambda: timeout.ProcessWithTimeout(
        child_success, (threading.Lock(),), python_executable=python_executable).run_with_timeout(20))
    _expect_raises(TypeError, lambda: timeout.ProcessWithTimeout(
        lambda: 1, python_executable=python_executable).run_with_timeout(20))
    _expect_raises(TypeError, lambda: timeout.ProcessWithTimeout(
        child_values, python_executable=python_executable).run_with_timeout(20))
    return True


def check_default_executable():
    """Under JNI sys.executable may be the JVM: the default must fail fast, never hang."""
    executable = sys.executable or ""
    begin = time.monotonic()
    try:
        timeout.ProcessWithTimeout(child_success, (1,)).run_with_timeout(30)
        outcome = "works"
    except (RuntimeError, OSError) as exc:
        outcome = "fails fast (%s)" % type(exc).__name__
    assert time.monotonic() - begin < 30
    return "sys.executable=%r: default python_executable %s" % (executable, outcome)


def prepare_udf_block():
    UDF_LATE.clear()
    UDF_RELEASE.clear()
    UDF_BLOCK.set()


def verify_udf_pending():
    pending = timeout.pending_tasks()
    assert len(pending) == 1, pending
    UDF_BLOCK.clear()  # The next query must not be blocked by the stuck worker.
    return True


def verify_udf_released():
    UDF_RELEASE.set()
    assert timeout.wait_for_pending_tasks(5), "stuck UDF worker never finished"
    assert not UDF_LATE, "timed-out UDF produced a side effect after the deadline"
    return True


def check_process(python_executable):
    with tempfile.TemporaryDirectory(prefix="iginx-pemjax-") as directory:
        marker = str(pathlib.Path(directory) / "marker")
        task = timeout.ProcessWithTimeout(child_block, (marker,), python_executable=python_executable)
        try:
            task.run_with_timeout(2)
            raise AssertionError("blocked process returned success")
        except timeout.TimeoutError as exc:
            assert exc.terminated
        assert task.process.poll() is not None, "worker process not reaped"
        assert pathlib.Path(marker).read_text(encoding="utf-8") == "started"
    assert timeout.ProcessWithTimeout(child_success, (41,), python_executable=python_executable).run_with_timeout(5) == 42
    return True


def cleanup():
    IMPORT_RELEASE.set()
    UDF_RELEASE.set()
    UDF_BLOCK.clear()
    for probe in PROBES:
        probe.release.set()
        probe.hook_release.set()
    assert timeout.wait_for_pending_tasks(5), "test leaked a worker or interrupt hook"
    PROBES.clear()
    if FINDER in sys.meta_path:
        sys.meta_path.remove(FINDER)
    sys.modules.pop(PACKAGE + ".child", None)
    sys.modules.pop(PACKAGE, None)




def check_cleanup_is_not_interrupted():
    for fail in (False, True):
        release, entered = threading.Event(), threading.Event()
        cleaned = []

        def work():
            try:
                if fail:
                    raise ValueError("original failure")
            finally:
                entered.set()
                release.wait(5)
                cleaned.append("closed")

        task = timeout.ThreadWithTimeout(work, cancel_grace=0.01)
        try:
            _expect_timeout(lambda: task.run_with_timeout(0.1), terminated=False)
            assert entered.is_set()
        finally:
            release.set()
            assert task.wait(3)
        assert cleaned == ["closed"], "timeout interrupted resource cleanup"
        if fail:
            assert isinstance(task.exception, ValueError)
    return True


def check_uncooperative_work_is_honestly_pending():
    release, entered = threading.Event(), threading.Event()
    finished = []

    def work():
        entered.set()
        release.wait(5)
        finished.append(True)

    task = timeout.ThreadWithTimeout(work, cancel_grace=0.01)
    try:
        _expect_timeout(lambda: task.run_with_timeout(0.1), terminated=False)
        assert entered.is_set() and task.thread.is_alive()
        assert task in timeout.pending_tasks()
    finally:
        release.set()
        assert task.wait(3)
    assert finished == [True], "uncooperative work cannot be forcibly stopped safely"
    return True
