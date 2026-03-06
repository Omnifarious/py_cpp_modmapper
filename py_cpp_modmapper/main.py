#!/usr/bin/env /usr/bin/python3
import asyncio
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
import logging
from enum import Enum
from pathlib import Path
import socket
import subprocess
import sys
from typing import TypeAlias
from collections.abc import Awaitable, AsyncIterator
import regex

from .parsing import (parse_gcc_arguments, GccOptions,
                      split_command, join_command)
from .dependency_db import DependencyDB

"""
 An attempt to implement the protocol described in
 https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2020/p1184r2.pdf
 except that description is out-of-date and this attempts to reach parity with
 the implementation in libcody in
 https://gcc.gnu.org/git/?p=gcc.git;a=tree;f=libcody;hb=master
 A somewhat out-of-date version of this library can be found on github at
 https://github.com/urnathan/libcody
"""


@dataclass(slots=True)
class Configuration:
    module_src_root: Path
    module_bmi_root: Path
    module_object_root: Path
    project_root: Path
    hash_flags: bool
    real_gcc_executable: Path
    gcc_options: GccOptions
    logger: logging.Logger

Compilation: TypeAlias = Awaitable[list[str]]


@dataclass(frozen=True, slots=True)
class GccSession:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    proc: asyncio.subprocess.Process


@asynccontextmanager
async def subordinate_gcc(
        c: Configuration, gcc_command: list[str], id: str
) -> AsyncIterator[GccSession]:
    gcc_sock: socket.socket | None
    my_sock: socket.socket | None
    gcc_sock, my_sock = socket.socketpair()

    me_comm = [f'-fmodule-mapper=<{gcc_sock.fileno()}>{gcc_sock.fileno()}?{id}']
    gcc: asyncio.subprocess.Process | None = None
    reader: asyncio.StreamReader | None
    writer: asyncio.StreamWriter | None = None
    try:
        c.logger.debug(f"Running subordinate GCC: {gcc_command!r} {me_comm!r}")
        gcc = await asyncio.create_subprocess_exec(
            c.real_gcc_executable,
            *gcc_command, *me_comm,
            stdin=subprocess.DEVNULL,
            pass_fds=(gcc_sock.fileno(),)
        )
        gcc_sock.close()
        gcc_sock = None
        my_sock.setblocking(False)
        reader, writer = await asyncio.open_connection(sock=my_sock)
        my_sock = None
        yield GccSession(reader, writer, gcc)
    finally:
        if gcc_sock is not None:
            gcc_sock.close()
        if my_sock is not None:
            my_sock.close()
        if writer is not None:
            writer.close()
            await writer.wait_closed()
        if gcc is not None:
            try:
                await asyncio.wait_for(gcc.wait(), timeout=0.1)
            except asyncio.TimeoutError:
                gcc.terminate()
                try:
                    await asyncio.wait_for(gcc.wait(), timeout=0.1)
                except asyncio.TimeoutError:
                    gcc.kill()
                    await gcc.wait()

class EngineStates(Enum):
    START = 0
    HELLO = 1
    IMPORT_NO_EXPORT = 3
    MODULE_EXPORT = 4
    IMPORT_EXPORT = 5
    FINISHED = 6

class ProtocolEngine:
    """Just trying to encapsulate the protocol logic here."""

    mod_name_part = r'(?:[\p{XID_Start}a-zA-Z_][\p{XID_Continue}a-zA-Z0-9_]*)'
    mod_name_re = regex.compile(
        f'^(?P<modname>{mod_name_part}(?:\\.{mod_name_part})*)'
        f'(?P<fragment>:{mod_name_part})?$'
    )

    def __init__(self, c: Configuration, level: str):
        self.config = c
        self.level = level
        self.dependencies = DependencyDB(c.module_bmi_root)
        self.state = EngineStates.START
        self.includes: set[str] = set()
        self.modules: set[str] = set()
        self.this_module: str | None = None
        self.config.logger.debug(f"{level}: [========== START ==========]")
        self._handlers = {
            'HELLO': self._handle_hello,
            'MODULE-REPO': self._handle_module_repo,
            'INCLUDE-TRANSLATE': self._handle_include_translate,
            'MODULE-EXPORT': self._handle_module_export,
            'MODULE-COMPILED': self._handle_module_compiled,
            'MODULE-IMPORT': self._handle_module_import,
        }

    def flag_dir(self, path: Path) -> Path:
        # This will use a hash self.config.gcc_options.flags someday
        if not self.config.hash_flags:
            return path
        else:
            return path / "prototype"

    def _handle_hello(self, words: list[str]) -> list[str]:
        if self.state != EngineStates.START:
            raise Exception('Protocol error: HELLO received after other commands')
        if words[1] != '1':
            raise Exception('Unsupported protocol version')
        if words[2] != 'GCC':
            raise Exception('Unsupported protocol flavor')
        self.state = EngineStates.HELLO
        return ['HELLO', '1', 'py_cpp_modmapper',]

    def _handle_module_repo(self, words: list[str]) -> list[str]:
        if len(words) != 1:
            raise Exception('MODULE-REPO command must have no arguments')
        if self.state != EngineStates.HELLO:
            raise Exception('Protocol error: Unexpected MODULE-REPO in {self.state.name} state')
        return ['MODULE-REPO', str(self.flag_dir(self.config.module_bmi_root))]

    def _handle_module_import(
            self, words: list[str]
    ) -> Compilation | list[str]:
        if len(words) != 2:
            raise Exception('MODULE-IMPORT command must have exactly one argument')
        if self.state == EngineStates.HELLO:
            self.state = EngineStates.IMPORT_NO_EXPORT
        elif self.state == EngineStates.MODULE_EXPORT:
            self.state = EngineStates.IMPORT_EXPORT
        elif self.state not in (EngineStates.IMPORT_EXPORT, EngineStates.IMPORT_NO_EXPORT):
            raise Exception(f'Unexpected MODULE-IMPORT in {self.state.name} state')
        self.modules.add(words[1])
        mod_path = self.module_name_to_path(
            words[1], 'cppm', flag_swizzle=False
        )
        mod_path = self.config.module_src_root / mod_path
        if not mod_path.exists():
            return ['ERROR', f'Module {words[1]} source not found {mod_path!r}']
        mod_bmi_path = self.module_name_to_path(words[1], 'gcm')
        mod_bmi_path = self.config.module_bmi_root / mod_bmi_path
        mod_obj_path = self.module_name_to_path(words[1], 'o')
        mod_obj_path = self.config.module_object_root / mod_obj_path
        mod_obj_path.parent.mkdir(parents=True, exist_ok=True)
        mod_bmi_path.parent.mkdir(parents=True, exist_ok=True)
        options = self.config.gcc_options.options.copy()
        options += ['-c', '-o', str(mod_obj_path), str(mod_path)]
        compilation = compile_module(self.config, options, f"/{words[1]}")
        async def wait_for_compilation() -> list[str]:
            retcode = await compilation
            if retcode == 0:
                return ['PATHNAME', str(mod_bmi_path)]
            else:
                return [
                    'ERROR',
                    f'Failed to compile module {mod_path!r}: '
                    f'return code {retcode}'
                ]

        return wait_for_compilation()

    def _handle_include_translate(self, words: list[str]) -> list[str]:
        if self.state != EngineStates.HELLO:
            raise Exception(f'Unexpected INCLUDE-TRANSLATE in {self.state.name} state')
        self.includes.add(words[1])
        return ['BOOL', 'TRUE']

    def _handle_module_export(self, words: list[str]) -> list[str]:
        if len(words) != 2:
            raise Exception('MODULE-EXPORT command must have exactly one argument')
        if self.state != EngineStates.HELLO:
            raise Exception(f'Unexpected MODULE-EXPORT in {self.state.name} state')
        self.state = EngineStates.MODULE_EXPORT
        assert self.this_module is None
        self.this_module = words[1]
        mod_name = words[1]
        mod_path = self.module_name_to_path(mod_name, 'gcm')
        mod_path = self.config.module_bmi_root / mod_path
        mod_path.parent.mkdir(parents=True, exist_ok=True)
        return ['PATHNAME', str(mod_path)]

    def _handle_module_compiled(self, words: list[str]) -> list[str]:
        if len(words) != 2:
            raise Exception('MODULE-COMPILED command must have exactly one argument')
        if self.state not in (EngineStates.MODULE_EXPORT, EngineStates.IMPORT_EXPORT):
            raise Exception(f'Unexpected MODULE-COMPILED in {self.state.name} state')
        assert self.this_module is not None
        if words[1] != self.this_module:
            raise Exception(
                f'MODULE-COMPILED command for module {words[1]} '
                f'without preceding MODULE-EXPORT for module {self.this_module}'
            )
        self.state = EngineStates.FINISHED
        self.this_module = None
        self.config.logger.debug(f"{self.level}: [Module compiled: {words[1]} "
                                 f"includes: {self.includes} | "
                                 f"modules: {self.modules}]")
        return ['OK']

    def module_name_to_path(
            self, module_name: str, suffix: str, flag_swizzle=True
    ) -> Path:
        modname_match = ProtocolEngine.mod_name_re.fullmatch(module_name)
        if not modname_match:
            raise Exception(f'Invalid module name: {module_name!r}')
        modname_dict = modname_match.groupdict()
        components = modname_dict['modname'].split('.')
        assert(len(components) > 0)
        assert(all(len(x) > 0 for x in components))
        fragment = modname_dict['fragment']
        if fragment is not None:
            fragment = fragment[1:]
        assert(fragment is None or len(fragment) > 0)
        if fragment is not None:
            components[-1] = f"{components[-1]}-{fragment}.{suffix}"
        else:
            components[-1] = f"{components[-1]}.{suffix}"
        mod_path = Path('.')
        if flag_swizzle:
            mod_path = self.flag_dir(mod_path)
        mod_path = mod_path.joinpath(*components)
        return mod_path

    def process_command_bundle(
            self, command_bundle: list[bytes]
    ) -> list[list[str] | Compilation]:
        self.config.logger.debug(
            f'{self.level}: [Got command bundle: {command_bundle!r}]'
        )
        output_bundle: list[list[str]] = []
        for command in command_bundle:
            words = split_command(command)
            self.config.logger.debug(f'{self.level}: [Parsed words: {words!r}]')
            verb = words[0]
            handler = self._handlers.get(verb)
            if handler:
                result = handler(words)
                output_bundle.append(result)
            else:
                raise Exception(f'Unrecognized command: {command.decode("utf-8")}')
        return output_bundle

async def compile_module(
        c: Configuration, gcc_args: list[str], level: str
) -> int:
    async with subordinate_gcc(c, gcc_args, level) as session:
        reader, writer = session.reader, session.writer
        protocol_engine = ProtocolEngine(c, level)
        line = await reader.readline()
        command_bundle: list[bytes] = []
        while len(line) > 0:
            trailing_semicolon = line.endswith(b';\n')
            if trailing_semicolon:
                line = line[:-2]
            elif line.endswith(b'\n'):
                line = line[:-1]
            command_bundle.append(line)
            if not trailing_semicolon:
                output_bundle = protocol_engine.process_command_bundle(command_bundle)
                sent_line = False
                output_buf = bytearray()
                for reply in output_bundle:
                    if not isinstance(reply, list):
                        reply = await reply
                    if not sent_line:
                        output_buf.extend(join_command(reply))
                        sent_line = True
                    else:
                        output_buf.extend(b' ;\n')
                        output_buf.extend(join_command(reply))
                if sent_line:
                    output_buf.append(ord('\n'))
                c.logger.debug(f'{level}: [Replied with: {output_buf!r}]')
                writer.write(output_buf)
                command_bundle = []
                await writer.drain()
            line = await reader.readline()
        if len(command_bundle) > 0:
            raise Exception(f'Incomplete command bundle: {command_bundle!r}')
        writer.close()
        await writer.wait_closed()
    assert session.proc.returncode is not None
    return session.proc.returncode

def setup_logging(logger: logging.Logger):
    handler = logging.FileHandler('cpp_mapper.log', mode='a')
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

async def main(argv: list[str]):
    """
    This is intended as something to run _instead_ of g++, and will wrap the
    compilation to ensure that any modules are properly compiled.
    """
    logger = logging.getLogger('cpp_mapper')
    if not logger.handlers:
        setup_logging(logger)
    medir = Path(os.getcwd())
    c = Configuration(
        module_src_root=medir / 'modules',
        module_bmi_root=medir / 'gcm.cache',
        module_object_root=medir / 'build' / 'modules',
        hash_flags=True,
        project_root=medir,
        real_gcc_executable=Path('/opt/gcc-latest/bin/g++'),
        gcc_options=parse_gcc_arguments(argv),
        logger=logger
    )
    if len(argv) < 2:
        print(f"Usage: {argv[0]} [gcc args...]", file=sys.stderr)
        sys.exit(1)
    sys.exit(await compile_module(c, argv[1:], '@TOP'))

def modmain():
    asyncio.run(main(sys.argv))

if __name__ == '__main__':
    modmain()
