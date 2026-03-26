import logging
from os import getpid
from pathlib import Path

from lmdb import Transaction

from .depdb_types import (
    DBModuleValue, DBModuleKey, DBKey, serialize_key, deserialize_value,
    RelevantStatData, DBHeaderValue, DBHeaderKey, serialize_value
)

logger = logging.getLogger("py_cpp_modmapper.dependency_scan")


class OutOfDateError(Exception):
    def __init__(self, msg: str, key: DBKey):
        super().__init__(msg)
        self.key = key


def fetch_relevant_stat(module: DBModuleKey, path: Path) -> RelevantStatData:
    try:
        osstat = path.stat()
        return RelevantStatData(
            osstat.st_mtime_ns - osstat.st_ctime_ns, osstat.st_mtime_ns,
            osstat.st_size
        )
    except FileNotFoundError:
        raise
    except OSError as e:
        logger.warning(f"Error {e} for file: {path!r}")
        raise OutOfDateError(f"Permission denied for file: {path!r}", module)


def check_header(
        header_path: Path, txn: Transaction, child_key: DBModuleKey,
        update=False, barrier_time_ns: int | None = None
):
    if update:
        assert barrier_time_ns is not None, "barrier_time_ns needed for update"
    key = DBHeaderKey(str(header_path))
    keybytes = serialize_key(key)
    value: bytes | DBHeaderValue | None = txn.get(keybytes)
    if value is not None:
        value = deserialize_value(value)
        assert isinstance(value, DBHeaderValue)
    elif not update:
        raise OutOfDateError(f"Missing header: {header_path!r}", child_key)
    header_stat: RelevantStatData | None = None
    try:
        header_stat = fetch_relevant_stat(child_key, header_path)
    except (OutOfDateError, FileNotFoundError) as e:
        if update:
            raise OutOfDateError(f"Header issue {e}: {header_path!r}", child_key)
    assert not update or header_stat is not None
    assert update or value is not None
    if update and header_stat.mtime > barrier_time_ns:
        logger.warning(f"Header {header_path!r} updated during compilation")
        raise OutOfDateError(
            f"Header {header_path!r} is out of date", child_key
        )
    if value is None or value.stat_data != header_stat:
        if update:
            txn.put(keybytes, serialize_value(DBHeaderValue(header_stat)))
        else:
            raise OutOfDateError(
                f"Header {header_path!r} is out of date", child_key
            )

def update_dependencies(
        txn: Transaction, key: DBModuleKey, value: DBModuleValue,
        barrier_time_ns: int
):
    """
    Recursively descend through the freshly compiled module's dependency tree,
    ensuring that dependencies were not updated during compilation. Then fill
    in valid stat data for the aforementioned module.

    Args:
        :param txn: The LMDB write transaction for the update.
        :param key: The DBModuleKey of the module being compiled.
        :param value: The DBModuleValue containing dependency information.
        :param barrier_time_ns: Time of compilation start in ns since epoch UTC.
    """

    checked_modules: set[str] = set()

    def update_submodule(subkey: DBModuleKey, child_mtime: int):
        if subkey.modname in checked_modules:
            return
        checked_modules.add(subkey.modname)
        keybytes = serialize_key(subkey)
        submod: bytes | DBModuleValue = txn.get(keybytes)
        if submod is None:
            logger.warning(f"Missing dependency: {subkey!r} for {key!r}")
            raise OutOfDateError(f"Missing dependency: {subkey!r}", key)
        submod = deserialize_value(submod)
        assert isinstance(submod, DBModuleValue)
        if submod.compile_pid is not None or not submod.last_compile_success:
            assert submod.compile_pid != getpid()
            if submod.compile_pid is not None:
                logger.warning(
                    f"Dependency {subkey!r} is currently being compiled by "
                    f"process {submod.compile_pid}"
                )
            else:
                logger.warning(f"Last compile of dependency {subkey!r} failed")
            raise OutOfDateError(
                f"Dependency {subkey!r} is not up-to-date", key
            )
        if (
                submod.dest_stat_data.mtime > child_mtime or
                submod.src_stat_data.mtime > barrier_time_ns
        ):
            logger.warning(f"Dependency {subkey!r} updated during compilation")
            raise OutOfDateError(
                f"Dependency {subkey!r} is out of date", key
            )
        for child_dep_mod in submod.dep_modules:
            update_submodule(
                DBModuleKey(child_dep_mod, subkey.option_hash),
                submod.dest_stat_data.mtime
            )

    try:
        value.src_stat_data = fetch_relevant_stat(key, Path(value.module_path))
        output_stat = fetch_relevant_stat(key, Path(value.bmi_path))
    except FileNotFoundError:
        logger.warning(
            f"Missing source or output file for {key!r} after compilation"
        )
        raise OutOfDateError(f"Missing source or output file", key)
    value.dest_stat_data = output_stat
    for dep_mod in value.dep_modules:
        update_submodule(
            DBModuleKey(dep_mod, key.option_hash), output_stat.mtime
        )
    for header_path in value.dep_headers:
        check_header(
            Path(header_path), txn, key,
            update=True, barrier_time_ns=barrier_time_ns
        )

def is_out_of_date(key: DBModuleKey, txn: Transaction) -> bool:
    """Check if a module is out of date based on its LMDB entry and state of
    the filesystem.

    key: The key of the module to check.
    txn: The LMDB read transaction
    :return: True if the module is out of date, False otherwise.
    """

    keybytes = serialize_key(key)
    value: bytes | DBModuleValue = txn.get(keybytes)
    assert value is not None
    value = deserialize_value(value)
    assert isinstance(value, DBModuleValue)
    if value.compile_pid != getpid():
        raise RuntimeError(
            f"Module {key!r} is currently not being compiled by not me?!?!"
        )

    try:
        if value.src_stat_data != fetch_relevant_stat(key, Path(value.module_path)):
            logger.info(f"Module {key!r} is out of date because of changed source file.")
            return True
    except OutOfDateError:
        logger.info(
            f"Module {key!r} is out of date because source file has something "
            "wrong."
        )
        return True
    except FileNotFoundError:
        logger.warning(f"Missing source file for {key!r} before compilation")
        raise OutOfDateError(f"Missing source file", key)
    if value.bmi_path is None:
        logger.info(f"Module {key!r} is out of date because it has no output file.")
        return True
    try:
        dest_stat = fetch_relevant_stat(key, Path(value.bmi_path))
    except OutOfDateError:
        logger.info(f"Module {key!r} is out of date because output file has something wrong.")
        return True
    except FileNotFoundError:
        logger.info(f"Module {key!r} is out of date because output file is missing.")
        return True
    if dest_stat != value.dest_stat_data:
        logger.info(f"Module {key!r} is out of date because of changed output file.")
        return True

    checked_modules = {key.modname,}
    headers_to_check = set(value.dep_headers)

    def check_modules_depended_upon(subkey: DBModuleKey):
        if subkey.modname in checked_modules:
            return False
        checked_modules.add(subkey.modname)
        subvalue: bytes | DBModuleValue = txn.get(serialize_key(subkey))
        if subvalue is None:
            return True
        subvalue = deserialize_value(subvalue)
        assert isinstance(subvalue, DBModuleValue)
        if subvalue.compile_pid is not None:
            logger.info(
                f"Module {subkey!r} is out of date because it is currently "
                "being compiled."
            )
            return True
        if not subvalue.last_compile_success:
            logger.info(
                f"Module {subkey!r} is out of date because of failed compilation."
            )
            return True
        try:
            src_stat = fetch_relevant_stat(subkey, Path(subvalue.module_path))
            dest_stat = fetch_relevant_stat(subkey, Path(subvalue.bmi_path))
        except OutOfDateError:
            logger.info(
                f"Module {key!r} is out of date because of something wrong "
                "with source or output file."
            )
            return True
        except FileNotFoundError:
            logger.info(
                f"Module {key!r} is out of date because of missing source or "
                "output file."
            )
            return True
        if src_stat != subvalue.src_stat_data :
            logger.info(
                f"Module {subkey!r} is out of date because of changed "
                "source file."
            )
            return True
        elif dest_stat != subvalue.dest_stat_data:
            logger.info(
                f"Module {subkey!r} is out of date because of changed "
                "output file."
            )
            return True
        elif dest_stat.mtime > value.dest_stat_data.mtime:
            logger.info(
                f"Module {key!r} is out of date because output older "
                f"than {subkey!r} output file."
            )
            return True
        headers_to_check.update(subvalue.dep_headers)
        return any(
            check_modules_depended_upon(
                DBModuleKey(dep_mod, subkey.option_hash)
            ) for dep_mod in subvalue.dep_modules
        )

    out_of_date = any(
        check_modules_depended_upon(DBModuleKey(mod_name, key.option_hash))
        for mod_name in value.dep_modules
    )
    if out_of_date:
        return True

    try:
        for header_path in headers_to_check:
            check_header(Path(header_path), txn, key)
    except OutOfDateError:
        logger.info(f"Module {key!r} is out of date because of changed header.")
        return True
    return False
