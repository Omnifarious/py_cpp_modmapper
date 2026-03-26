import enum
import os
import pickle
from dataclasses import dataclass
from typing import TypeAlias


class CompilationStatus(enum.Enum):
    COMPILE_BLOCKED = 0
    COMPILE_CYCLE = 1
    COMPILE_START = 2


@dataclass(frozen=True, slots=True)
class RelevantStatData:
    # mtime - ctime : Recover ctime with mtime - ctime_offset
    # This makes this smaller to store with pickle.
    ctime_offset: int
    mtime: int
    size: int


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
    compile_pid: int | None  # A value here indicates compilation in progress
    last_compile_success: bool
    src_stat_data: RelevantStatData | None
    dest_stat_data: RelevantStatData | None
    module_path: str
    bmi_path: str | None
    dep_modules: list[str]
    dep_headers: list[str]


@dataclass(frozen=True, slots=True)
class CompilationResults:
    success: bool
    module_path: str
    bmi_path: str | None
    dep_modules: list[str]
    dep_headers: list[str]
    start_time_ns: int


@dataclass(slots=True)
class DBHeaderValue:
    stat_data: RelevantStatData


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
