import enum
import pickle
from dataclasses import dataclass
from sys import intern


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

    @property
    def ctime(self) -> int:
        return self.mtime - self.ctime_offset


@dataclass(frozen=True, slots=True)
class DBModuleKey:
    modname: str
    option_hash: str


@dataclass(frozen=False, slots=True)
class HeaderInfo:
    path: str
    stat_data: RelevantStatData | None


@dataclass(slots=True)
class DBModuleValue:
    compile_pid: int | None  # A value here indicates compilation in progress
    last_compile_success: bool
    src_stat_data: RelevantStatData | None
    dest_stat_data: RelevantStatData | None
    module_path: str
    bmi_path: str | None
    dep_modules: list[str]
    dep_headers: list[HeaderInfo]


@dataclass(frozen=True, slots=True)
class CompilationResults:
    success: bool
    module_path: str
    bmi_path: str | None
    dep_modules: list[str]
    dep_headers: list[str]
    start_time_ns: int


def serialize_key(key: DBModuleKey) -> bytes:
    return f"m:{key.modname}\0{key.option_hash}".encode('utf-8')


def deserialize_key(key_bytes: bytes) -> DBModuleKey:
    key_str = key_bytes.decode('utf-8')
    assert key_str.startswith('m:')
    modname, option_hash = key_str[2:].split('\0', 1)
    return DBModuleKey(modname, intern(option_hash))


def serialize_value(value: DBModuleValue) -> bytes:
    if not isinstance(value, DBModuleValue):
        raise TypeError(f"Unknown value type: {value!r}")
    return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)


def deserialize_value(value_bytes: bytes) -> DBModuleValue:
    value = pickle.loads(value_bytes)
    if not isinstance(value, DBModuleValue):
        raise TypeError(f"Unknown value type: {value!r}")
    return value
