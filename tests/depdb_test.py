import pytest

from py_cpp_modmapper.dependency_db import (
    DependencyDB, DBModuleKey, DBHeaderKey, DBModuleValue, DBHeaderValue,
    serialize_key, deserialize_key, serialize_value, deserialize_value
)


@pytest.fixture(scope="function")
def depdb(tmp_path):
    yield DependencyDB(tmp_path)

module_test_data = [
    ("bottom", "bar", None, None, "d", ["middle_a", "middle_b"], ["iostream.h"]),
    ("top", "bar", None, None, "a", [], ["bouncy.h"]),
    ("middle_a", "bar", None, None, "b", ["top"], []),
    ("middle_b", "bar", None, None, "c", ["top"], []),
    ("top", "baz", None, None, "a", ["ghost"], ["bouncy.h"]),
    ("next_a", "baz", None, None, "e", ["top"], []),
    ("baloon", "baz", None, None, "g", ["next_a"], ["iostream.h"]),
    ("combined", "baz", None, None, "h", ["next_a", "baloon"], ["foo.h"]),
    ("next_b", "baz", None, None, "f", ["next_a"], []),
    ("all", "baz", None, None, "i", ["next_a", "baloon", "middle_a"], ["bouncy.h"])
]
include_test_data = [
    ("bouncy.h", None),
    ("iostream.h", None),
    ("unused.h", None)
]
dump_output = """DBHeaderKey(header_path='bouncy.h'): DBHeaderValue(stat_data=None)
DBModuleKey(modname='top', option_hash='bar'): DBModuleValue(compile_pid=None, last_compile_success=True, src_stat_data=None, dest_stat_data=None, module_path='a', bmi_path='a', dep_modules=[], dep_headers=['bouncy.h'])
DBModuleKey(modname='middle_a', option_hash='bar'): DBModuleValue(compile_pid=None, last_compile_success=True, src_stat_data=None, dest_stat_data=None, module_path='b', bmi_path='b', dep_modules=['top'], dep_headers=[])
DBModuleKey(modname='middle_b', option_hash='bar'): DBModuleValue(compile_pid=None, last_compile_success=True, src_stat_data=None, dest_stat_data=None, module_path='c', bmi_path='c', dep_modules=['top'], dep_headers=[])
DBHeaderKey(header_path='iostream.h'): DBHeaderValue(stat_data=None)
DBModuleKey(modname='bottom', option_hash='bar'): DBModuleValue(compile_pid=None, last_compile_success=True, src_stat_data=None, dest_stat_data=None, module_path='d', bmi_path='d', dep_modules=['middle_a', 'middle_b'], dep_headers=['iostream.h'])
DBModuleKey(modname='top', option_hash='baz'): DBModuleValue(compile_pid=None, last_compile_success=True, src_stat_data=None, dest_stat_data=None, module_path='a', bmi_path='a', dep_modules=['ghost'], dep_headers=['bouncy.h'])
DBModuleKey(modname='next_a', option_hash='baz'): DBModuleValue(compile_pid=None, last_compile_success=True, src_stat_data=None, dest_stat_data=None, module_path='e', bmi_path='e', dep_modules=['top'], dep_headers=[])
DBModuleKey(modname='baloon', option_hash='baz'): DBModuleValue(compile_pid=None, last_compile_success=True, src_stat_data=None, dest_stat_data=None, module_path='g', bmi_path='g', dep_modules=['next_a'], dep_headers=['iostream.h'])
DBModuleKey(modname='all', option_hash='baz'): DBModuleValue(compile_pid=None, last_compile_success=True, src_stat_data=None, dest_stat_data=None, module_path='i', bmi_path='i', dep_modules=['next_a', 'baloon', 'middle_a'], dep_headers=['bouncy.h'])
DBModuleKey(modname='combined', option_hash='baz'): DBModuleValue(compile_pid=None, last_compile_success=True, src_stat_data=None, dest_stat_data=None, module_path='h', bmi_path='h', dep_modules=['next_a', 'baloon'], dep_headers=['foo.h'])
DBModuleKey(modname='next_b', option_hash='baz'): DBModuleValue(compile_pid=None, last_compile_success=True, src_stat_data=None, dest_stat_data=None, module_path='f', bmi_path='f', dep_modules=['next_a'], dep_headers=[])
DBHeaderKey(header_path='unused.h'): DBHeaderValue(stat_data=None)"""

@pytest.fixture(scope="function")
def debdb_sample_db(depdb: DependencyDB):
    with depdb.db_env.begin(write=True) as txn:
        for modname, option_hash, src_stat_data, dest_stat_data, path, dep_modules, dep_headers in module_test_data:
            txn.put(serialize_key(DBModuleKey(modname, option_hash)),
                    serialize_value(DBModuleValue(None, True, src_stat_data, dest_stat_data, path, path, dep_modules, dep_headers)))
        for header_path, src_stat_data in include_test_data:
            txn.put(serialize_key(DBHeaderKey(header_path)), serialize_value(DBHeaderValue(src_stat_data)))
    yield depdb

def test_one_db_per_path(tmp_path):
    db1 = DependencyDB(tmp_path / "foo.db")
    db2 = DependencyDB(tmp_path / "bar.db")
    db3 = DependencyDB(tmp_path / "foo.db")
    assert db1 is db3
    assert db2 is not db3
    with db2.db_env.begin(write=True) as txn:
        txn.put(b"foo", b"bar")
    with db3.db_env.begin(write=False) as txn:
        assert txn.get(b"foo") != b"bar"


def test_depdb_init(depdb: DependencyDB):
    assert depdb.db_path is not None
    assert depdb.db_env is not None
    assert depdb.dump() == ""

def test_depdb_sample_dump(debdb_sample_db: DependencyDB):
    assert debdb_sample_db.dump() == dump_output
