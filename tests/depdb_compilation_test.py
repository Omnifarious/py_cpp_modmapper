import enum
import logging
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable

import pytest
import asyncio
import os
import re
from pathlib import Path
from py_cpp_modmapper.dependency_db import (
    DependencyDB, DBModuleKey, CompilationStatus, CompilationResults
)
from py_cpp_modmapper.depdb_types import DBModuleValue

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class FakeModule:
    name: str
    source: Path
    dest: Path
    dep_mods: list[str]
    dep_headers: list[Path]


@dataclass(frozen=True, slots=True)
class FakeSourceTree:
    root: Path
    modules: list[FakeModule]
    index: dict[str, FakeModule]


@pytest.fixture(scope="function")
def depdb(tmp_path):
    db_path = tmp_path / "test_compilation.db"
    yield DependencyDB(db_path)


@pytest.fixture(scope="function")
def fake_src_tree(tmp_path):
    relocated_data = [
        replace(
            fake,
            source=tmp_path / fake.source,
            dest=tmp_path / fake.dest,
            dep_headers = [tmp_path / header for header in fake.dep_headers]
        )
        for fake in module_tree_data
    ]
    index = {fake.name: fake for fake in relocated_data}
    for fake in relocated_data:
        create_fake_file(fake.source)
        for header in fake.dep_headers:
            create_fake_file(header)
    yield FakeSourceTree(tmp_path, relocated_data, index)


def create_fake_file(path: Path, content: str = "fake content"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


module_tree_data = [
    FakeModule(
        "top", Path("src/top.cppm"), Path("build/top.bmi"),
        [],
        [Path("include/foo.h",)]
    ),
    FakeModule(
        "lonely_top", Path("src/lonely_top.cppm"), Path("build/lonely_top.bmi"),
        [],
        [Path("include/shared.h"), Path("include/common.h")]
    ),
    FakeModule(
        "mid_a", Path("src/mid_a.cppm"), Path("build/mid_a.bmi"),
        ["top"],
        []
    ),
    FakeModule(
        "mid_b", Path("src/mid_b.cppm"), Path("build/mid_b.bmi"),
        ["top"],
        [Path("include/common.h")]
    ),
    FakeModule(
        "bottom", Path("src/bottom.cppm"), Path("build/bottom.bmi"),
        ["mid_a", "mid_b"],
        [Path("include/uncommon.h")]
    ),
    FakeModule(
        "alt_bottom", Path("src/alt_bottom.cppm"), Path("build/alt_bottom.bmi"),
        ["mid_a"],
        [Path("include/common.h"), Path("include/shared.h")]
    ),
]


# bottom
# ├── include/uncommon.h
# ├── mid_a
# │   └── top
# │       └── include/foo.h
# └── mid_b
#     ├── include/common.h
#     └── top
#         └── include/foo.h
#
# alt_bottom
# ├── include/common.h
# ├── include/shared.h
# └── mid_a
#     └── top
#         └── include/foo.h
#
# lonely_top
# ├── include/shared.h
# └── include/common.h


def fake_hook_noop(DBModuleKey) -> None:
    pass


@dataclass(frozen=True, slots=True)
class FakeHookset:
    compile_start: Callable[[DBModuleKey], None] = fake_hook_noop
    compiler_called: Callable[[DBModuleKey], None] = fake_hook_noop
    compile_done: Callable[[DBModuleKey], None] = fake_hook_noop
    result_recorded: Callable[[DBModuleKey], None] = fake_hook_noop


async def compile_fake_module(
        depdb: DependencyDB, key: DBModuleKey,
        fake_index: dict[str, FakeModule],
        time_to_compile: float = 0.01,
        hooks: FakeHookset | None = None
) -> bool:
    class InternalResult(Enum):
        SUCCESS = 0
        FAILED = 1
        BLOCKED = 2

    fake = fake_index[key.modname]
    last_result = InternalResult.BLOCKED
    started = datetime.now()
    if hooks is None:
        hooks = FakeHookset()

    async def do_compilation() -> InternalResult:
        async with depdb.start_compilation(key, fake.source) as (status, pid):
            hooks.compile_start(key)
            if status in (CompilationStatus.COMPILE_BLOCKED, CompilationStatus.COMPILE_CYCLE):
                return InternalResult.BLOCKED
            if not await depdb.is_out_of_date(key):
                logger.info(f"Skipping {key!r} because it's up-to-date")
                hooks.compile_done(key)
                await depdb.no_compilation_needed(key)
                return InternalResult.SUCCESS
            compilation_start_ns = time.time_ns()
            tasks = [
                compile_fake_module(
                    depdb, DBModuleKey(mod, key.option_hash), fake_index,
                    time_to_compile, hooks
                )
                for mod in fake.dep_mods
            ]
            task_results: list[bool | Exception] = await asyncio.gather(*tasks)
            def check_child_compiles():
                for i, tr in enumerate(task_results):
                    if isinstance(tr, Exception):
                        logger.warning(
                            f"Compilation failed for {fake.dep_mods[i]!r}: {tr}"
                        )
                        return InternalResult.FAILED
                    elif not tr:
                        return InternalResult.FAILED
                return InternalResult.SUCCESS

            internal_result = check_child_compiles()
            compile_result = InternalResult.FAILED
            if internal_result == InternalResult.SUCCESS:
                try:
                    if fake.source.exists():
                        await asyncio.sleep(time_to_compile)
                        hooks.compiler_called(key)
                        create_fake_file(fake.dest)
                        compile_result = InternalResult.SUCCESS
                    else:
                        compile_result = InternalResult.FAILED
                except OSError:
                    compile_result = InternalResult.FAILED
            hooks.compile_done(key)
            if compile_result == InternalResult.SUCCESS:
                results = CompilationResults(
                    success=True,
                    module_path=str(fake.source),
                    bmi_path=str(fake.dest),
                    dep_modules=fake.dep_mods,
                    dep_headers=[str(p) for p in fake.dep_headers],
                    start_time_ns=compilation_start_ns
                )
                still_good = await depdb.compilation_success(key, results)
                if not still_good:
                    internal_result = InternalResult.FAILED
            if internal_result == InternalResult.FAILED:
                await depdb.fail_compilation(key)
            assert internal_result in (
                InternalResult.SUCCESS, InternalResult.FAILED
            )
            return internal_result

    # Seed with enough time in the past to force a log entry on first block.
    last_blocked_report = started - timedelta(seconds=16)
    while last_result == InternalResult.BLOCKED:
        last_result = await do_compilation()
        if last_result == InternalResult.BLOCKED:
            now = datetime.now()
            total = now - started
            # Try to make sure at least 15 between log entries.
            if now - last_blocked_report > timedelta(seconds=15):
                logger.info(f"Compilation blocked for {key!r}, waiting... {total}")
                last_blocked_report = now
            if total < timedelta(minutes=5):
                await asyncio.sleep(0.2)
            else:
                raise RuntimeError(f"Compilation took too long for {key!r}, "
                                   "maybe a deadlock?")
    hooks.result_recorded(key)
    return last_result == InternalResult.SUCCESS


class DBEntryState(enum.Enum):
    COMPLETELY_NEW = 0
    COMPILING_NEW = 1
    COMPILING_EXISTING = 2
    FAILED = 3
    SUCCESS = 4


async def check_db_congruence(
        depdb: DependencyDB,
        key: DBModuleKey,
        fake_data: FakeModule,
        compilation_state: DBEntryState
) -> None:
    """Check that a database entry is consistent with the fake modules."""
    assert key.modname == fake_data.name
    value = await depdb._get(key)
    if compilation_state == DBEntryState.COMPLETELY_NEW:
        assert value is None
        return
    assert value is not None
    assert isinstance(value, DBModuleValue)
    assert value.module_path == str(fake_data.source)
    if compilation_state == DBEntryState.COMPILING_NEW:
        assert value.src_stat_data is None
        assert value.dest_stat_data is None
        assert value.bmi_path is None
        assert value.dep_modules == []
        assert value.dep_headers == []
    if compilation_state in (DBEntryState.COMPILING_NEW, DBEntryState.COMPILING_EXISTING):
        assert value.last_compile_success is False
        assert value.compile_pid == os.getpid()
        return
    if compilation_state == DBEntryState.FAILED:
        assert value.last_compile_success is False
        assert value.compile_pid is None
        return
    if compilation_state == DBEntryState.SUCCESS:
        assert value.last_compile_success is True
        assert value.compile_pid is None
        assert value.bmi_path == str(fake_data.dest)
        assert value.dep_modules == fake_data.dep_mods
        assert value.dep_headers == [str(x) for x in fake_data.dep_headers]
        assert value.src_stat_data is not None
        assert value.dest_stat_data is not None
        return
    assert False, f"Unknown compilation state: {compilation_state}"


@pytest.mark.asyncio
async def test_no_recurse_manual(
        fake_src_tree: FakeSourceTree,
        min_file_interval: float
):
    """Simple tests where compilation is manually simulated for modules that
    have no dependencies and hence have no need for recursion."""
    depdb = DependencyDB(fake_src_tree.root / "test_compilation.db")
    fakemods = fake_src_tree.modules
    # Parents are modules that have no dependencies on other modules.
    parents = [fake for fake in fakemods if len(fake.dep_mods) <= 0]

    # Fresh compile
    for parent in parents:
        pkey = DBModuleKey(parent.name, "prototype")
        src = parent.source
        await check_db_congruence(
            depdb, pkey, parent, DBEntryState.COMPLETELY_NEW
        )
        async with depdb.start_compilation(pkey, src) as (status, pid):
            assert status == CompilationStatus.COMPILE_START
            assert pid == os.getpid()
            assert depdb.outstanding_compilations.get(pkey) is False
            await check_db_congruence(
                depdb, pkey, parent, DBEntryState.COMPILING_NEW
            )
            assert await depdb.is_out_of_date(pkey)
            start_time = time.time_ns()
            await asyncio.sleep(min_file_interval)
            create_fake_file(parent.dest)
            results = CompilationResults(
                success=True,
                module_path=str(src),
                bmi_path=str(parent.dest),
                dep_modules=[],
                dep_headers=[
                    str(header) for header in parent.dep_headers
                ],
                start_time_ns=start_time
            )
            assert depdb.outstanding_compilations.get(pkey) is False
            assert await depdb.compilation_success(pkey, results)
            assert depdb.outstanding_compilations.get(pkey) is True
        assert depdb.outstanding_compilations.get(pkey) is None
        await check_db_congruence(
            depdb, pkey, parent, DBEntryState.SUCCESS
        )

    # Remove the destination file and try again.
    for parent in parents:
        pkey = DBModuleKey(parent.name, "prototype")
        src = parent.source
        parent.dest.unlink()
        assert depdb.outstanding_compilations.get(pkey) is None
        async with depdb.start_compilation(pkey, src) as (status, pid):
            assert status == CompilationStatus.COMPILE_START
            assert pid == os.getpid()
            assert depdb.outstanding_compilations.get(pkey) is False
            await check_db_congruence(
                depdb, pkey, parent, DBEntryState.COMPILING_EXISTING
            )
            assert await depdb.is_out_of_date(pkey)
            start_time = time.time_ns()
            await asyncio.sleep(min_file_interval)
            create_fake_file(parent.dest)
            results = CompilationResults(
                success=True,
                module_path=str(src),
                bmi_path=str(parent.dest),
                dep_modules=[],
                dep_headers=[
                    str(header) for header in parent.dep_headers
                ],
                start_time_ns=start_time
            )
            assert depdb.outstanding_compilations.get(pkey) is False
            assert await depdb.compilation_success(pkey, results)
            assert depdb.outstanding_compilations.get(pkey) is True
        assert depdb.outstanding_compilations.get(pkey) is None
        await check_db_congruence(
            depdb, pkey, parent, DBEntryState.SUCCESS
        )

    # Try again with no removal to see if up-to-date checking says no compile
    for parent in parents:
        pkey = DBModuleKey(parent.name, "prototype")
        src = parent.source
        assert depdb.outstanding_compilations.get(pkey) is None
        async with depdb.start_compilation(pkey, src) as (status, pid):
            assert status == CompilationStatus.COMPILE_START
            assert pid == os.getpid()
            assert depdb.outstanding_compilations.get(pkey) is False
            await check_db_congruence(
                depdb, pkey, parent, DBEntryState.COMPILING_EXISTING
            )
            assert await depdb.is_out_of_date(pkey) is False
            assert depdb.outstanding_compilations.get(pkey) is False
            await depdb.no_compilation_needed(pkey)
            assert depdb.outstanding_compilations.get(pkey) is True
        assert depdb.outstanding_compilations.get(pkey) is None
        await check_db_congruence(
            depdb, pkey, parent, DBEntryState.SUCCESS
        )

    # Update source files to make sure that up-to-date checking says a compile
    # is needed
    for parent in parents:
        pkey = DBModuleKey(parent.name, "prototype")
        src = parent.source
        # Update the source file.
        create_fake_file(src)
        assert depdb.outstanding_compilations.get(pkey) is None
        async with depdb.start_compilation(pkey, src) as (status, pid):
            assert status == CompilationStatus.COMPILE_START
            assert pid == os.getpid()
            assert depdb.outstanding_compilations.get(pkey) is False
            await check_db_congruence(
                depdb, pkey, parent, DBEntryState.COMPILING_EXISTING
            )
            assert await depdb.is_out_of_date(pkey) is True
            assert depdb.outstanding_compilations.get(pkey) is False
            start_time = time.time_ns()
            await asyncio.sleep(min_file_interval)
            create_fake_file(parent.dest)
            results = CompilationResults(
                success=True,
                module_path=str(src),
                bmi_path=str(parent.dest),
                dep_modules=[],
                dep_headers=[
                    str(header) for header in parent.dep_headers
                ],
                start_time_ns=start_time
            )
            assert depdb.outstanding_compilations.get(pkey) is False
            assert await depdb.compilation_success(pkey, results)
            assert depdb.outstanding_compilations.get(pkey) is True
        await check_db_congruence(
            depdb, pkey, parent, DBEntryState.SUCCESS
        )

    # Filter for only those parent modules that also include header files.
    with_headers = [fake for fake in parents if len(fake.dep_headers) > 0]

    # For modules with includes, does modifying the include make the module
    # out-of-date?
    for parent in with_headers:
        pkey = DBModuleKey(parent.name, "prototype")
        src = parent.source
        hdr0 = parent.dep_headers[0]
        create_fake_file(hdr0)
        assert depdb.outstanding_compilations.get(pkey) is None
        async with depdb.start_compilation(pkey, src) as (status, pid):
            assert status == CompilationStatus.COMPILE_START
            assert pid == os.getpid()
            assert depdb.outstanding_compilations.get(pkey) is False
            await check_db_congruence(
                depdb, pkey, parent, DBEntryState.COMPILING_EXISTING
            )
            assert await depdb.is_out_of_date(pkey)
            start_time = time.time_ns()
            await asyncio.sleep(min_file_interval)
            create_fake_file(parent.dest)
            results = CompilationResults(
                success=True,
                module_path=str(src),
                bmi_path=str(parent.dest),
                dep_modules=[],
                dep_headers=[
                    str(header) for header in parent.dep_headers
                ],
                start_time_ns=start_time
            )
            assert depdb.outstanding_compilations.get(pkey) is False
            assert await depdb.compilation_success(pkey, results)
            assert depdb.outstanding_compilations.get(pkey) is True
        assert depdb.outstanding_compilations.get(pkey) is None
        await check_db_congruence(
            depdb, pkey, parent, DBEntryState.SUCCESS
        )

    # If a header is modified 'during compilation', does this cause the
    # dependency database update at the end to fail?
    for parent in with_headers:
        pkey = DBModuleKey(parent.name, "prototype")
        src = parent.source
        hdr0 = parent.dep_headers[0]
        create_fake_file(hdr0)
        assert depdb.outstanding_compilations.get(pkey) is None
        async with depdb.start_compilation(pkey, src) as (status, pid):
            assert status == CompilationStatus.COMPILE_START
            assert pid == os.getpid()
            assert depdb.outstanding_compilations.get(pkey) is False
            await check_db_congruence(
                depdb, pkey, parent, DBEntryState.COMPILING_EXISTING
            )
            assert await depdb.is_out_of_date(pkey)
            start_time = time.time_ns()
            await asyncio.sleep(min_file_interval)
            create_fake_file(parent.dest)
            create_fake_file(hdr0)
            results = CompilationResults(
                success=True,
                module_path=str(src),
                bmi_path=str(parent.dest),
                dep_modules=[],
                dep_headers=[
                    str(header) for header in parent.dep_headers
                ],
                start_time_ns=start_time
            )
            assert depdb.outstanding_compilations.get(pkey) is False
            assert not await depdb.compilation_success(pkey, results)
            assert depdb.outstanding_compilations.get(pkey) is False
            await depdb.fail_compilation(pkey)
            assert depdb.outstanding_compilations.get(pkey) is True
        assert depdb.outstanding_compilations.get(pkey) is None
        await check_db_congruence(
            depdb, pkey, parent, DBEntryState.FAILED
        )

    # If we remove a depended upon header without removing it from the module's
    # dependencies, will this both result in the module being considered
    # out-of-date and the dependency database update failing?
    single = with_headers[0]
    single.dep_headers[0].unlink()
    pkey = DBModuleKey(single.name, "prototype")
    src = single.source
    assert depdb.outstanding_compilations.get(pkey) is None
    async with depdb.start_compilation(pkey, src) as (status, pid):
        assert status == CompilationStatus.COMPILE_START
        assert pid == os.getpid()
        assert depdb.outstanding_compilations.get(pkey) is False
        assert await depdb.is_out_of_date(pkey)
        start_time = time.time_ns()
        await asyncio.sleep(min_file_interval)
        create_fake_file(single.dest)
        assert depdb.outstanding_compilations.get(pkey) is False
        results = CompilationResults(
            success=True,
            module_path=str(src),
            bmi_path=str(single.dest),
            dep_modules=[],
            dep_headers=[
                str(header) for header in single.dep_headers
            ],
            start_time_ns=start_time
        )
        assert depdb.outstanding_compilations.get(pkey) is False
        assert not await depdb.compilation_success(pkey, results)
        assert depdb.outstanding_compilations.get(pkey) is False
        await depdb.fail_compilation(pkey)
        assert depdb.outstanding_compilations.get(pkey) is True
    assert depdb.outstanding_compilations.get(pkey) is None
    await check_db_congruence(
        depdb, pkey, single, DBEntryState.FAILED
    )
    create_fake_file(single.dep_headers[0], "restored")

    # Test removing a header and removing it from the module's dependencies.
    # It should still register as out-of-date.
    # First, pick a module with multiple headers.
    with_headers.sort(key=lambda fake: len(fake.dep_headers), reverse=True)
    single = with_headers[0]
    single.dep_headers[0].unlink()
    assert len(single.dep_headers) > 1
    pkey = DBModuleKey(single.name, "prototype")
    src = single.source
    assert depdb.outstanding_compilations.get(pkey) is None
    async with depdb.start_compilation(pkey, src) as (status, pid):
        assert status == CompilationStatus.COMPILE_START
        assert pid == os.getpid()
        assert depdb.outstanding_compilations.get(pkey) is False
        await check_db_congruence(
            depdb, pkey, single, DBEntryState.COMPILING_EXISTING
        )
        assert await depdb.is_out_of_date(pkey)
        start_time = time.time_ns()
        await asyncio.sleep(min_file_interval)
        create_fake_file(single.dest)
        # Oh, compilation revealed the header is no longer needed.
        del single.dep_headers[0]
        assert depdb.outstanding_compilations.get(pkey) is False
        results = CompilationResults(
            success=True,
            module_path=str(src),
            bmi_path=str(single.dest),
            dep_modules=[],
            dep_headers=[
                str(header) for header in single.dep_headers
            ],
            start_time_ns=start_time
        )
        assert depdb.outstanding_compilations.get(pkey) is False
        assert await depdb.compilation_success(pkey, results)
        assert depdb.outstanding_compilations.get(pkey) is True
    assert depdb.outstanding_compilations.get(pkey) is None
    await check_db_congruence(
        depdb, pkey, single, DBEntryState.SUCCESS
    )

    # Test adding a header (which requries modifying the source).
    single.dep_headers.append(Path(fake_src_tree.root) / "include/newhdr.h")
    create_fake_file(single.dep_headers[-1])
    pkey = DBModuleKey(single.name, "prototype")
    src = single.source
    create_fake_file(src) # How did the new header get included otherwise?
    assert depdb.outstanding_compilations.get(pkey) is None
    async with depdb.start_compilation(pkey, src) as (status, pid):
        assert status == CompilationStatus.COMPILE_START
        assert pid == os.getpid()
        assert depdb.outstanding_compilations.get(pkey) is False
        await check_db_congruence(
            depdb, pkey, single, DBEntryState.COMPILING_EXISTING
        )
        assert await depdb.is_out_of_date(pkey)
        start_time = time.time_ns()
        await asyncio.sleep(min_file_interval)
        create_fake_file(single.dest)
        assert depdb.outstanding_compilations.get(pkey) is False
        results = CompilationResults(
            success=True,
            module_path=str(src),
            bmi_path=str(single.dest),
            dep_modules=[],
            dep_headers=[
                str(header) for header in single.dep_headers
            ],
            start_time_ns=start_time
        )
        assert depdb.outstanding_compilations.get(pkey) is False
        assert await depdb.compilation_success(pkey, results)
        assert depdb.outstanding_compilations.get(pkey) is True
    assert depdb.outstanding_compilations.get(pkey) is None
    await check_db_congruence(
        depdb, pkey, single, DBEntryState.SUCCESS
    )
    assert len(depdb.outstanding_compilations) == 0


@pytest.mark.asyncio
async def test_no_recurse(
        fake_src_tree: FakeSourceTree,
        min_file_interval: float
):
    """
    Simple tests where compilation is done using the compile_fake_module
    function, but only for modules that have no dependencies and hence
    have no need for recursion.

    Basically, this is a test for the compile_fake_module function.
    """
    depdb = DependencyDB(fake_src_tree.root / "test_compilation.db")
    fakemods = fake_src_tree.modules
    fake_index = fake_src_tree.index
    parents = [fake for fake in fakemods if len(fake.dep_mods) <= 0]

    compile_happened = False
    def compiler_called(_: DBModuleKey):
        nonlocal compile_happened
        compile_happened = True
    compiler_called_hook = FakeHookset(compiler_called=compiler_called)

    for parent in parents:
        pkey = DBModuleKey(parent.name, "prototype")
        assert not parent.dest.exists()
        await check_db_congruence(
            depdb, pkey, parent, DBEntryState.COMPLETELY_NEW
        )
        compile_happened = False
        assert await compile_fake_module(
            depdb, pkey, fake_index, min_file_interval, compiler_called_hook
        )
        assert compile_happened
        assert parent.dest.exists()
        await check_db_congruence(
            depdb, pkey, parent, DBEntryState.SUCCESS
        )

    # Remove the destination file and try again.
    for parent in parents:
        pkey = DBModuleKey(parent.name, "prototype")
        parent.dest.unlink()
        compile_happened = False
        assert await compile_fake_module(
            depdb, pkey, fake_index, min_file_interval, compiler_called_hook
        )
        assert compile_happened
        assert parent.dest.exists()
        await check_db_congruence(
            depdb, pkey, parent, DBEntryState.SUCCESS
        )

    # Try again with no removal to see if up-to-date checking says no compile
    for parent in parents:
        pkey = DBModuleKey(parent.name, "prototype")
        compile_happened = False
        assert await compile_fake_module(
            depdb, pkey, fake_index, min_file_interval, compiler_called_hook
        )
        assert not compile_happened
        await check_db_congruence(
            depdb, pkey, parent, DBEntryState.SUCCESS
        )

    # Update source files to make sure that up-to-date checking says a compile
    # is needed
    for parent in parents:
        pkey = DBModuleKey(parent.name, "prototype")
        create_fake_file(parent.source)
        compile_happened = False
        assert await compile_fake_module(
            depdb, pkey, fake_index, min_file_interval, compiler_called_hook
        )
        assert compile_happened
        await check_db_congruence(
            depdb, pkey, parent, DBEntryState.SUCCESS
        )

    # Filter for only those parent modules that also include header files.
    with_headers = [fake for fake in parents if len(fake.dep_headers) > 0]

    # For modules with includes, does modifying the include make the module
    # out-of-date?
    for parent in with_headers:
        pkey = DBModuleKey(parent.name, "prototype")
        create_fake_file(parent.dep_headers[0])
        compile_happened = False
        assert await compile_fake_module(
            depdb, pkey, fake_index, min_file_interval, compiler_called_hook
        )
        assert compile_happened
        await check_db_congruence(
            depdb, pkey, parent, DBEntryState.SUCCESS
        )

    # If a header is modified 'during compilation', does this cause the
    # dependency database update at the end to fail?
    for parent in with_headers:
        pkey = DBModuleKey(parent.name, "prototype")
        hdr0 = parent.dep_headers[0]
        create_fake_file(hdr0)

        def modify_header(_: DBModuleKey):
            nonlocal compile_happened
            compile_happened = True
            create_fake_file(hdr0)

        hooks = FakeHookset(compiler_called=modify_header)
        compile_happened = False
        assert not await compile_fake_module(
            depdb, pkey, fake_index, min_file_interval, hooks
        )
        assert compile_happened
        await check_db_congruence(
            depdb, pkey, parent, DBEntryState.FAILED
        )
        # Now compile it for real so it is no longer failed.
        assert await compile_fake_module(
            depdb, pkey, fake_index, min_file_interval
        )
        await check_db_congruence(
            depdb, pkey, parent, DBEntryState.SUCCESS
        )

    # If we remove a depended upon header without removing it from the module's
    # dependencies, will this both result in the module being considered
    # out-of-date and the dependency database update failing?
    single = with_headers[0]
    single.dep_headers[0].unlink()
    pkey = DBModuleKey(single.name, "prototype")
    assert not await compile_fake_module(
        depdb, pkey, fake_index, min_file_interval
    )
    await check_db_congruence(
        depdb, pkey, single, DBEntryState.FAILED
    )
    create_fake_file(single.dep_headers[0], "restored")

    # Test removing a header and removing it from the module's dependencies.
    # It should still register as out-of-date.
    # First, pick a module with multiple headers.
    with_headers.sort(key=lambda fake: len(fake.dep_headers), reverse=True)
    single = with_headers[0]
    single.dep_headers[0].unlink()
    assert len(single.dep_headers) > 1
    pkey = DBModuleKey(single.name, "prototype")

    # We need to simulate the compilation revealing the header is no longer
    # needed. compile_fake_module uses fake.dep_headers to report results.
    # So we can just remove it from single.dep_headers before it finishes.

    def remove_header(_: DBModuleKey):
        if len(single.dep_headers) > 1:
            del single.dep_headers[0]

    hooks = FakeHookset(compile_done=remove_header)
    assert await compile_fake_module(
        depdb, pkey, fake_index, min_file_interval, hooks
    )
    await check_db_congruence(
        depdb, pkey, single, DBEntryState.SUCCESS
    )

    # Test adding a header (which requries modifying the source).
    single.dep_headers.append(Path(fake_src_tree.root) / "include/newhdr.h")
    create_fake_file(single.dep_headers[-1])
    pkey = DBModuleKey(single.name, "prototype")
    create_fake_file(single.source)
    assert await compile_fake_module(
        depdb, pkey, fake_index, min_file_interval
    )
    # Make sure the added header is correctly registered
    await check_db_congruence(
        depdb, pkey, single, DBEntryState.SUCCESS
    )
    assert len(depdb.outstanding_compilations) == 0


def normalize_dump(dump_str: str, root_path: Path) -> str:
    # Replace absolute paths with relative ones
    root_str = str(root_path)
    if not root_str.endswith("/"):
        root_str += "/"
    normalized = dump_str.replace(root_str, "")

    # Replace mtime and size with placeholders to avoid non-deterministic output
    # RelevantStatData(ctime_offset=0, mtime=1774223925427848946, size=12)
    normalized = re.sub(r"mtime=\d+", "mtime=MTIME", normalized)
    normalized = re.sub(r"size=\d+", "size=SIZE", normalized)

    return normalized


@pytest.mark.asyncio
async def test_no_branch_recursion(
        fake_src_tree: FakeSourceTree,
        min_file_interval: float
):
    # These tests test compilations of modules that have a dependency on
    # another module, but never two dependencies on another module. So, no
    # forks in the dependency graph.

    # Test initial compilation of a mid-tier module and its one dependency.
    key = DBModuleKey("mid_a", "prototype")
    depdb = DependencyDB(fake_src_tree.root / "test_compilation.db")
    fake_index = fake_src_tree.index

    compiled_modules: set[DBModuleKey] = set()
    def compiler_called(compiled_key: DBModuleKey):
        assert depdb.outstanding_compilations.get(compiled_key) is False
        assert compiled_key not in compiled_modules
        compiled_modules.add(compiled_key)

    assert depdb.outstanding_compilations.get(key) is None
    assert await compile_fake_module(
        depdb, key, fake_index, min_file_interval,
        FakeHookset(compiler_called=compiler_called)
    )
    assert depdb.outstanding_compilations.get(key) is None
    assert len(compiled_modules) == 2
    assert DBModuleKey("top", "prototype") in compiled_modules
    assert key in compiled_modules

    expected = """DBHeaderKey(header_path='include/foo.h'): DBHeaderValue(stat_data=RelevantStatData(ctime_offset=0, mtime=MTIME, size=SIZE))
DBModuleKey(modname='top', option_hash='prototype'): DBModuleValue(compile_pid=None, last_compile_success=True, src_stat_data=RelevantStatData(ctime_offset=0, mtime=MTIME, size=SIZE), dest_stat_data=RelevantStatData(ctime_offset=0, mtime=MTIME, size=SIZE), module_path='src/top.cppm', bmi_path='build/top.bmi', dep_modules=[], dep_headers=['include/foo.h'])
DBModuleKey(modname='mid_a', option_hash='prototype'): DBModuleValue(compile_pid=None, last_compile_success=True, src_stat_data=RelevantStatData(ctime_offset=0, mtime=MTIME, size=SIZE), dest_stat_data=RelevantStatData(ctime_offset=0, mtime=MTIME, size=SIZE), module_path='src/mid_a.cppm', bmi_path='build/mid_a.bmi', dep_modules=['top'], dep_headers=[])"""

    assert normalize_dump(depdb.dump(), fake_src_tree.root) == expected

    # Test compiling a module that depends on the module just compiled, and
    # make sure that doesn't result in the previous modules being recompiled.
    compiled_modules.clear()
    key = DBModuleKey("alt_bottom", "prototype")
    assert depdb.outstanding_compilations.get(key) is None
    assert await compile_fake_module(
        depdb, key, fake_index, min_file_interval,
        FakeHookset(compiler_called=compiler_called)
    )
    assert depdb.outstanding_compilations.get(key) is None
    assert len(compiled_modules) == 1
    assert key in compiled_modules
    assert DBModuleKey("top", "prototype") not in compiled_modules
    assert DBModuleKey("mid_a", "prototype") not in compiled_modules

    expected += """
DBHeaderKey(header_path='include/common.h'): DBHeaderValue(stat_data=RelevantStatData(ctime_offset=0, mtime=MTIME, size=SIZE))
DBHeaderKey(header_path='include/shared.h'): DBHeaderValue(stat_data=RelevantStatData(ctime_offset=0, mtime=MTIME, size=SIZE))
DBModuleKey(modname='alt_bottom', option_hash='prototype'): DBModuleValue(compile_pid=None, last_compile_success=True, src_stat_data=RelevantStatData(ctime_offset=0, mtime=MTIME, size=SIZE), dest_stat_data=RelevantStatData(ctime_offset=0, mtime=MTIME, size=SIZE), module_path='src/alt_bottom.cppm', bmi_path='build/alt_bottom.bmi', dep_modules=['mid_a'], dep_headers=['include/common.h', 'include/shared.h'])"""

    assert normalize_dump(depdb.dump(), fake_src_tree.root) == expected

    # Test that modifying a dependency triggers recompilation of only the
    # dependent modules.
    create_fake_file(fake_index["mid_a"].source)
    compiled_modules.clear()
    # Note that key is still alt_bottom.
    assert depdb.outstanding_compilations.get(key) is None
    assert await compile_fake_module(
        depdb, key, fake_index, min_file_interval,
        FakeHookset(compiler_called=compiler_called)
    )
    assert depdb.outstanding_compilations.get(key) is None
    assert len(compiled_modules) == 2
    assert key in compiled_modules
    assert DBModuleKey("top", "prototype") not in compiled_modules
    assert DBModuleKey("mid_a", "prototype") in compiled_modules

    assert normalize_dump(depdb.dump(), fake_src_tree.root) == expected

    # Test that modifying a deep header dependency triggers a
    # full branch recompilation.
    create_fake_file(fake_index["top"].dep_headers[0])
    compiled_modules.clear()
    # Note again that key is still alt_bottom.
    assert depdb.outstanding_compilations.get(key) is None
    assert await compile_fake_module(
        depdb, key, fake_index, min_file_interval,
        FakeHookset(compiler_called=compiler_called)
    )
    assert depdb.outstanding_compilations.get(key) is None
    assert len(compiled_modules) == 3
    assert key in compiled_modules
    assert DBModuleKey("top", "prototype") in compiled_modules
    assert DBModuleKey("mid_a", "prototype") in compiled_modules

    assert normalize_dump(depdb.dump(), fake_src_tree.root) == expected

@pytest.mark.asyncio
async def test_full_recursion(
        fake_src_tree: FakeSourceTree, min_file_interval: float
):
    depdb = DependencyDB(fake_src_tree.root / "test_compilation.db")
    fake_index = fake_src_tree.index

    key = DBModuleKey("bottom", "prototype")
    compiled_modules: set[DBModuleKey] = set()
    def compiler_called(compiled_key: DBModuleKey):
        assert depdb.outstanding_compilations.get(compiled_key) is False
        assert compiled_key not in compiled_modules
        compiled_modules.add(compiled_key)
        logger.debug(f"Outstanding compilations: {depdb.outstanding_compilations}")
    hooks = FakeHookset(compiler_called=compiler_called)
    assert len(depdb.outstanding_compilations) == 0
    assert await compile_fake_module(
        depdb, key, fake_index, min_file_interval, hooks
    )
    assert len(depdb.outstanding_compilations) == 0
    assert len(compiled_modules) == 4
    assert DBModuleKey("top", "prototype") in compiled_modules
    assert key in compiled_modules
    assert DBModuleKey("mid_a", "prototype") in compiled_modules
    assert DBModuleKey("mid_b", "prototype") in compiled_modules

    # And recompiling with no changes should not result in any compilations.
    compiled_modules.clear()
    assert len(depdb.outstanding_compilations) == 0
    assert await compile_fake_module(
        depdb, key, fake_index, min_file_interval, hooks
    )
    assert len(depdb.outstanding_compilations) == 0
    assert len(compiled_modules) == 0

    # alt_bottom depends on mid_a, which has already been compiled and
    # shouldn't be out-of-date.
    compiled_modules.clear()
    key = DBModuleKey("alt_bottom", "prototype")
    assert len(depdb.outstanding_compilations) == 0
    assert await compile_fake_module(
        depdb, key, fake_index, min_file_interval, hooks
    )
    assert len(depdb.outstanding_compilations) == 0
    assert len(compiled_modules) == 1
    assert key in compiled_modules

    # Now lets build lonely_top, which has no dependencies.
    compiled_modules.clear()
    key = DBModuleKey("lonely_top", "prototype")
    assert len(depdb.outstanding_compilations) == 0
    assert await compile_fake_module(
        depdb, key, fake_index, min_file_interval, hooks
    )
    assert len(depdb.outstanding_compilations) == 0
    assert len(compiled_modules) == 1
    assert key in compiled_modules

    # Now, lets touch include/common.h and try compiling alt_bottom, lonely_top,
    # and bottom to see if the right things get compiled for each.

    create_fake_file(fake_index["alt_bottom"].dep_headers[0])
    compiled_modules.clear()
    key = DBModuleKey("alt_bottom", "prototype")
    assert len(depdb.outstanding_compilations) == 0
    assert await compile_fake_module(
        depdb, key, fake_index, min_file_interval, hooks
    )
    assert len(depdb.outstanding_compilations) == 0
    assert len(compiled_modules) == 1
    assert key in compiled_modules

    compiled_modules.clear()
    key = DBModuleKey("lonely_top", "prototype")
    assert len(depdb.outstanding_compilations) == 0
    assert await compile_fake_module(
        depdb, key, fake_index, min_file_interval, hooks
    )
    assert len(depdb.outstanding_compilations) == 0
    assert len(compiled_modules) == 1
    assert key in compiled_modules

    compiled_modules.clear()
    key = DBModuleKey("bottom", "prototype")
    assert len(depdb.outstanding_compilations) == 0
    assert await compile_fake_module(
        depdb, key, fake_index, min_file_interval, hooks
    )
    assert len(depdb.outstanding_compilations) == 0
    assert len(compiled_modules) == 2
    assert key in compiled_modules
    assert DBModuleKey("mid_a", "prototype") in compiled_modules
    compiled_modules.clear()
