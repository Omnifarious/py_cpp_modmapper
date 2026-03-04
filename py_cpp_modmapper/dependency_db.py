import os
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias

import lmdb


# lmdb.Environment(metasync=False, writemap=True, max_readers=8000)
# with env.begin(write=True) as txn:
#     txn.put(key, value)
# with env.begin() as txn:
#     value = txn.get(key)


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
    stat_data: os.stat_result
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


class DependencyDB:
    db_env: lmdb.Environment | None = None
    db_path: Path | None = None
    engine_count = 0

    def __init__(self, bmi_path: Path):
        db_path = bmi_path / "dependency_db.mdb"
        if DependencyDB.db_env is None:
            assert DependencyDB.db_path is None
            db_path.mkdir(parents=True, exist_ok=True)
            DependencyDB.db_path = db_path
            DependencyDB.db_env = lmdb.Environment(
                path=str(db_path),
                readonly=False, create=True,
                metasync=False, writemap=True, max_readers=8000
            )
        else:
            assert DependencyDB.db_path == db_path, \
            "Configuration BMI root changed between "\
            "instantiations of DependencyDB "\
            f"({DependencyDB.db_path!r} != {db_path!r})"
        DependencyDB.engine_count += 1

    def __del__(self):
        if DependencyDB.engine_count > 0:
            assert DependencyDB.db_env is not None
            DependencyDB.engine_count -= 1
            if DependencyDB.engine_count == 0:
                DependencyDB.db_env.close()
                DependencyDB.db_env = None
                DependencyDB.db_path = None


