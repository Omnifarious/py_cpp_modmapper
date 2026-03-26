import logging
import os
from asyncio import get_event_loop
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TypeAlias, Any, AsyncGenerator
from weakref import WeakValueDictionary

import lmdb

from . import dependency_scan
from .depdb_types import (
    CompilationStatus, CompilationResults, DBModuleKey, DBHeaderKey, DBKey,
    DBModuleValue, DBValue,
    serialize_key, deserialize_key, serialize_value, deserialize_value
)


# Types needed by public interface, even if they don't originate in this module
__all__ = [
    "DependencyDB", "DBModuleKey", "CompilationStatus", "CompilationResults",
]


# lmdb.Environment(metasync=False, writemap=True, max_readers=8000)
# with env.begin(write=True) as txn:
#     txn.put(key, value)
# with env.begin() as txn:
#     value = txn.get(key)


logger = logging.getLogger("py_cpp_modmapper.dependency_db")

DBDictType: TypeAlias = WeakValueDictionary[Path, "DependencyDB"]
CompilationSet: TypeAlias = dict[DBModuleKey, bool]

class DependencyDB:
    db_by_path: DBDictType = WeakValueDictionary()

    def __new__(cls, *args, **kwargs):
        db_path = kwargs.get("db_path")
        if db_path is None:
            if len(args) < 1:
                raise Exception("DependencyDB requires a Database path argument")
            db_path = args[0]
        db_path = Path(db_path)
        db = cls.db_by_path.get(db_path)
        if not isinstance(db, DependencyDB):
            db = super().__new__(cls)
            db._initalized = False
            cls.db_by_path[db_path] = db
        return db

    def __init__(self, db_path: Path):
        if self._initalized:
            return
        self._initalized = True
        self.db_path = db_path
        self.db_env = lmdb.Environment(
            path=str(db_path),
            readonly=False, create=True,
            metasync=False, writemap=True, max_readers=8000
        )
        self.outstanding_compilations: CompilationSet = {}

    async def _put(self, key: DBKey, value: DBValue):
        await get_event_loop().run_in_executor(
            None, self._put_sync, key, value
        )

    async def _get(self, key: DBKey) -> DBValue | None:
        return await get_event_loop().run_in_executor(
            None, self._get_sync, key
        )

    @asynccontextmanager
    async def start_compilation(
            self, key: DBModuleKey, module_path: str | Path
    ) -> AsyncGenerator[tuple[CompilationStatus, int], Any]:
        # We've haven't yet started a new compilation
        # as part of this context manager.
        its_ours = False
        if not isinstance(module_path, str):
            module_path = str(module_path)
        if key in self.outstanding_compilations:
            yield CompilationStatus.COMPILE_CYCLE, os.getpid()
        else:
            try:
                result = await get_event_loop().run_in_executor(
                    None, self._start_compilation_sync, key, module_path
                )
                assert result is not None
                if result[0] == CompilationStatus.COMPILE_START:
                    # Our state and the database state should align here
                    assert key not in self.outstanding_compilations
                    # False meaning the status hasn't been updated yet
                    self.outstanding_compilations[key] = False
                    its_ours = True
                    # It otherwise isn't "ours" because it's already started
                    # in some other context (maybe even in another process)
                    # and we're just waiting for it to finish.
                yield result
            finally:
                # If it wasn't started as part if this context
                # (i.e. it isn't ours), no need to do anything.
                if its_ours:
                    assert key in self.outstanding_compilations
                    # If nobody has updated the compilation status, fail it
                    if not self.outstanding_compilations[key]:
                        await self.fail_compilation(key)
                    del self.outstanding_compilations[key]

    async def is_out_of_date(self, key: DBModuleKey) -> bool:
        assert key in self.outstanding_compilations
        assert self.outstanding_compilations[key] is False
        return await get_event_loop().run_in_executor(
            None, self._is_out_of_date_sync, key
        )

    async def fail_compilation(self, key: DBModuleKey):
        # This process must be currently compiling this module, and most not
        # have already updated the compilation status.
        assert key in self.outstanding_compilations
        assert self.outstanding_compilations[key] is False
        mod_data = await self._get(key)
        # The database must also think this module is currently being compiled
        # by this process.
        assert mod_data is not None and isinstance(mod_data, DBModuleValue)
        assert mod_data.compile_pid == os.getpid()
        mod_data.compile_pid = None
        mod_data.last_compile_success = False
        await self._put(key, mod_data)
        self.outstanding_compilations[key] = True

    async def no_compilation_needed(self, key: DBModuleKey):
        """
        This module was already compiled successfully and none of its
        dependencies have changed since.
        """
        # This process must be currently compiling this module, and most not
        # have already updated the compilation status.
        assert key in self.outstanding_compilations
        assert self.outstanding_compilations[key] is False
        mod_data = await self._get(key)
        # The database must also think this module is currently being compiled
        # by this process.
        assert mod_data is not None and isinstance(mod_data, DBModuleValue)
        assert mod_data.compile_pid == os.getpid()
        mod_data.compile_pid = None
        mod_data.last_compile_success = True
        await self._put(key, mod_data)
        self.outstanding_compilations[key] = True

    async def compilation_success(
            self, key: DBModuleKey, results: CompilationResults
    ) -> bool:
        """
        Set compilation to successful. Will return True if the compilation was
        truly successful, and False if a dependency was updated during
        compilation. If False, the compilation will need to be redone.

        :param key:
        :param results:
        :return: True if the compilation was truly successful, False otherwise.
        """
        # This process must be currently compiling this module, and most not
        # have already updated the compilation status.
        assert key in self.outstanding_compilations
        assert self.outstanding_compilations[key] is False
        no_dep_updates = await get_event_loop().run_in_executor(
            None, self._compilation_success_sync, key, results
        )
        if no_dep_updates:
            self.outstanding_compilations[key] = True
        return no_dep_updates

    def _put_sync(self, key: DBKey, value: DBValue):
        with self.db_env.begin(write=True) as txn:
            txn.put(serialize_key(key), serialize_value(value))

    def _get_sync(self, key: DBKey) -> DBValue | None:
        with self.db_env.begin(write=False) as txn:
            value = txn.get(serialize_key(key))
            if value is None:
                return None
            return deserialize_value(value)

    def _is_out_of_date_sync(self, key: DBModuleKey) -> bool:
        with self.db_env.begin(write=False) as txn:
            ood = dependency_scan.is_out_of_date(key, txn)
            if ood:
                logger.debug(f"Module {key!r} determined to be out of date")
            return ood

    def _compilation_success_sync(
            self, key: DBModuleKey, results: CompilationResults
    ) -> bool:
        with self.db_env.begin(write=True) as txn:
            keybytes = serialize_key(key)
            result = txn.get(keybytes)
            if result is None:
                raise Exception(f"Module {key!r} not found in database")
            value = deserialize_value(result)
            assert value is not None and isinstance(value, DBModuleValue)
            if value.compile_pid != os.getpid():
                raise Exception(
                    f"Module {key!r} is not being compiled by this process"
                )
            value.compile_pid = None
            value.last_compile_success = True
            value.module_path = results.module_path
            value.bmi_path = results.bmi_path
            value.dep_modules = results.dep_modules
            value.dep_headers = results.dep_headers
            try:
                dependency_scan.update_dependencies(
                    txn, key, value, results.start_time_ns
                )
            except dependency_scan.OutOfDateError as e:
                logger.warning(
                    "Out of date error after successful compilation: "
                    f"<{e.key!r}> [{e!r}]"
                )
                return False
            txn.put(keybytes, serialize_value(value))
            return True

    def _start_compilation_sync(
            self, key: DBModuleKey, module_path: str
    ) -> tuple[CompilationStatus, int]:
        pid = os.getpid()
        with self.db_env.begin(write=True) as txn:
            keybytes = serialize_key(key)
            result = txn.get(keybytes)
            if result is None:
                value = DBModuleValue(
                    pid, False, None, None, module_path, None, [], []
                )
            else:
                value = deserialize_value(result)
                assert isinstance(value, DBModuleValue)
                if value.compile_pid == pid:
                    return CompilationStatus.COMPILE_CYCLE, pid
                elif value.compile_pid is not None:
                    return CompilationStatus.COMPILE_BLOCKED, value.compile_pid
            value.compile_pid = pid
            value.last_compile_success = False
            value.module_path = module_path
            txn.put(keybytes, serialize_value(value))
            return CompilationStatus.COMPILE_START, pid

    def __repr__(self):
        return f"DependencyDB(db_path={self.db_path}, db_env={self.db_env!r})"

    def dump(self) -> str:
        mem_db: dict[DBKey, DBValue] = {}
        with self.db_env.begin() as txn:
            mem_db = {
                deserialize_key(key): deserialize_value(value)
                for key, value in txn.cursor()
            }

        def keyfunc(key: DBKey):
            if isinstance(key, DBModuleKey):
                return 0, key.option_hash, key.modname
            elif isinstance(key, DBHeaderKey):
                return 1, key.header_path
            else:
                assert False, f"Unknown key type: {key!r}"

        mem_db = {k: mem_db[k] for k in sorted(mem_db.keys(), key=keyfunc)}
        used: set[DBKey] = set()
        tsorted: list[DBKey] = []
        key_interns = {k: k for k in mem_db.keys()}
        def tsort_helper(key: DBKey, value: DBValue):
            if key in used:
                return
            used.add(key)
            if isinstance(key, DBHeaderKey):
                tsorted.append(key)
                return
            assert isinstance(key, DBModuleKey), f"Unknown key type: {key!r}"
            assert value is not None, f"Missing value for key {key!r}"
            assert isinstance(value, DBModuleValue), \
                f"Unexpected value type {type(value)!r} for key {key!r}"
            def handle_key(k: DBKey):
                k = key_interns.get(k)
                if k is not None:
                    tsort_helper(k, mem_db[k])
            for dep in value.dep_modules:
                handle_key(DBModuleKey(dep, key.option_hash))
            for dep in value.dep_headers:
                handle_key(DBHeaderKey(dep))
            tsorted.append(key)
        for key, value in mem_db.items():
            if key not in used:
                if isinstance(key, DBModuleKey):
                    tsort_helper(key, value)
                else:
                    # All headers occur after all modules because of the sorting
                    # so this just adds orphans to the end of the list
                    tsorted.append(key)

        del used
        del key_interns

        return "\n".join(f"{key!r}: {mem_db[key]!r}" for key in tsorted)
