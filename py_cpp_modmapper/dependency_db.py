import logging
import os
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias
from weakref import WeakValueDictionary

import lmdb


# lmdb.Environment(metasync=False, writemap=True, max_readers=8000)
# with env.begin(write=True) as txn:
#     txn.put(key, value)
# with env.begin() as txn:
#     value = txn.get(key)


logger = logging.getLogger("py_cpp_modmapper.dependency_db")


@dataclass(frozen=True, slots=True)
class DBModuleKey:
    modname: str
    option_hash: str

@dataclass(frozen=True, slots=True)
class DBHeaderKey:
    header_path: str

DBKey: TypeAlias = DBModuleKey | DBHeaderKey

@dataclass(slots=True)
class DBModuleValue:
    src_stat_data: os.stat_result
    dest_stat_data: os.stat_result
    module_path: str
    dep_modules: list[str]
    dep_headers: list[str]

@dataclass(slots=True)
class DBHeaderValue:
    stat_data: os.stat_result

DBValue: TypeAlias = DBModuleValue | DBHeaderValue

def serialize_key(key: DBKey) -> bytes:
    if isinstance(key, DBModuleKey):
        return f"m:{key.modname}\0{key.option_hash}".encode('utf-8')
    elif isinstance(key, DBHeaderKey):
        return f"h:{key.header_path}".encode('utf-8')
    else:
        raise Exception(f"Unknown key type: {key!r}")

def deserialize_key(key_bytes: bytes) -> DBKey:
    key_str = key_bytes.decode('utf-8')
    if key_str.startswith('m:'):
        modname, option_hash = key_str[2:].split('\0', 1)
        return DBModuleKey(modname, option_hash)
    elif key_str.startswith('h:'):
        header_path = key_str[2:]
        return DBHeaderKey(header_path)
    else:
        raise Exception(f"Unknown key type: {key_str[0:2]!r}")

def serialize_value(value: DBValue) -> bytes:
    if not isinstance(value, (DBModuleValue, DBHeaderValue)):
        raise Exception(f"Unknown value type: {value!r}")
    return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)

def deserialize_value(value_bytes: bytes) -> DBValue:
    value = pickle.loads(value_bytes)
    if not isinstance(value, (DBModuleValue, DBHeaderValue)):
        raise Exception(f"Unknown value type: {value!r}")
    return value

DBDictType: TypeAlias = WeakValueDictionary[Path, "DependencyDB"]

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

    def __repr__(self):
        return (f"DependencyDB(engine_count={DependencyDB.engine_count}, "
                f"db_path={DependencyDB.db_path})")

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

        used = None
        key_interns = None

        return "\n".join(f"{key!r}: {mem_db[key]!r}" for key in tsorted)
