import asyncio
import contextlib
import dataclasses
import time
import traceback
import typing as t

import lightbulb

import saru

from . import util

T = t.TypeVar("T")
TestCallback = t.Callable[[T], t.Coroutine[t.Any, t.Any, None]]


@dataclasses.dataclass
class TestInfo:
    """
    Test information, like name and description.
    """
    name: str
    description: str


@dataclasses.dataclass
class TestResult:
    """
    Info about the result of a test.
    """
    testinfo: TestInfo
    time: float
    exc: t.Optional[Exception] = None

    def passed(self) -> bool:
        return self.exc is None

    def __str__(self) -> str:
        outheader = f"[{'PASS' if self.passed() else 'FAIL'}] {self.testinfo.name} ({self.time:.2f}s)"
        # Unfortunately mypy isn't smart enough to tell that passed() is the same as "self.exc is None".
        # So, use the latter directly here.
        tb = [] if self.exc is None else traceback.format_exception(
            type(self.exc),
            self.exc,
            self.exc.__traceback__
        )

        # Annoying quirk here, __traceback__ is a sequence of strings, but each string has multiple
        # lines in it. We want to add spaces for indentation here, so need to split out each line.
        tb_lines = "".join(tb).splitlines()

        return "\n".join([
            outheader,
            *[" "*4 + line for line in tb_lines]
        ])


class Test(t.Generic[T]):
    def __init__(
        self,
        callback: TestCallback,
        info: TestInfo
    ) -> None:
        self.callback = callback
        self.testinfo = info

    async def run(self, ctx: T) -> TestResult:
        starttime = time.time()
        exc: t.Optional[Exception] = None

        try:
            await self(ctx)
        except Exception as e:
            exc = e
        finally:
            time_elapsed = time.time() - starttime

        return TestResult(
            testinfo=self.testinfo,
            time=time_elapsed,
            exc=exc
        )

    # SelfTests may be called, which will directly call the callback.
    # This is to allow the self test to remain callable even after the use of the
    # selftest.test decorator
    async def __call__(self, ctx: T) -> None:
        await self.callback(ctx)


class TestSuite(t.Generic[T]):
    def __init__(self) -> None:
        self.tests: t.MutableSequence[Test] = []

    def test(self, test: Test) -> Test:
        self.tests.append(test)
        return test

    async def run(self, ctx: T) -> t.Sequence[TestResult]:
        return [await test.run(ctx) for test in self.tests]


def as_selftest(
    name: str,
    description: str
) -> t.Callable[[TestCallback], Test[T]]:
    def deco(callback: TestCallback) -> Test[T]:
        testinfo = TestInfo(
            name=name,
            description=description
        )

        return Test(callback, testinfo)

    return deco


@contextlib.contextmanager
def raises(exc_type: t.Type[Exception]) -> t.Generator[None, t.Any, None]:
    try:
        yield None
    except Exception as e:
        # Reraise exceptions we don't want to catch
        if issubclass(type(e), exc_type):
            return
        else:
            raise

    # If no exception, this is unexpected, so assert
    assert False, "Exception was not raised!"


def format_test_time(seconds: float) -> str:
    """
    Formats the time it took to run a test.
    """
    minutes = int(seconds // 60)
    seconds = seconds - 60*minutes

    formatted = f"{seconds:.3f}s"
    if minutes > 0:
        formatted = f"{minutes}m" + formatted

    return formatted


def format_test_result(results: t.Sequence[TestResult]) -> str:
    """
    Formats the result of a test suite into text suitable for a discord message.
    """
    passed = []
    failed = []

    total_time = 0.0

    for result in results:
        passed.append(str(result)) if result.passed() else failed.append(str(result))
        total_time += result.time

    return "\n".join([
        *failed,
        *passed,
        "",
        f"Total time: {format_test_time(total_time)}"
    ])


# LIGHTBULB SPECIFIC
CommandCallbackT = t.Callable[[lightbulb.Context], t.Coroutine[t.Any, t.Any, None]]


def as_command_test(
    command: str,
    expected_exception_type: t.Optional[t.Type[Exception]] = None
) -> t.Callable[[CommandCallbackT], Test[lightbulb.PrefixContext]]:
    """
    Second order decorator that creates a saru.Test instance that simply runs a prefix command.
    Optionally, an exception type may be specified, in which case the test will succeed if and only if
    the exception is raised while running the command.
    """
    def deco(extra_callback: CommandCallbackT) -> Test[lightbulb.PrefixContext]:
        @as_selftest(
            ("" if expected_exception_type is None else "-") + command,
            f"Test invocation of the command \"{command}\""
        )
        async def test(ctx: lightbulb.PrefixContext) -> None:
            if expected_exception_type is not None:
                with raises(expected_exception_type):
                    await util.invoke_prefix_command(ctx, command)
            else:
                await util.invoke_prefix_command(ctx, command)

            await extra_callback(ctx)

        return t.cast(Test[lightbulb.PrefixContext], test)

    return deco


def add_command_tests(
    suite: TestSuite[lightbulb.Context],
    command_tests: t.Sequence[t.Union[str, t.Tuple[str, t.Type[Exception]]]],
    delay: t.Optional[float] = None
) -> None:
    """
    Convenience function. Creates a set of command tests with `as_command_test` and attaches
    them to a suite.
    """
    for test in command_tests:
        if isinstance(test, str):
            command, exc = test, None
        else:
            command, exc = test

        # Extra callback for the test. Delay if it's asked for, some tests need it.
        async def _(*_: t.Any) -> None:
            if delay:
                await asyncio.sleep(delay)

        suite.test(as_command_test(command, exc)(_))


async def send_test_result_response(ctx: lightbulb.Context, results: t.Sequence[TestResult]) -> None:
    await util.respond_code_or_txt(ctx, "\n".join([
        "Tests complete. Results:",
        "",
        format_test_result(results)
    ]))


async def basic_selftest_command(ctx: lightbulb.Context, *suites: TestSuite) -> None:
    """
    Implementation for a very basic selftest command. Runs each suite in sequence,
    collects their results, and presents the test results to the user.

    See `saru.extension.__init__.selftest` for a use of this function.

    Example usage:
    ```py
    @plugin.command()
    @lightbulb.add_checks(lightbulb.owner_only)
    @lightbulb.implements(lightbulb.PrefixCommand)
    async def selftest_cmd(ctx: lightbulb.Context) -> None:
        await saru.basic_selftest_command(ctx, your_suite, ...)
    ```
    """
    results: t.MutableSequence[TestResult] = []
    for suite in suites:
        results += await suite.run(ctx)

    await saru.send_test_result_response(ctx, results)
