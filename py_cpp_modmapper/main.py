#!/usr/bin/env /usr/bin/python3
import asyncio
import os
import pickle
from contextlib import asynccontextmanager
from dataclasses import dataclass
import logging
from pathlib import Path
import socket
import subprocess
import sys
from typing import Optional, TypeAlias
from collections.abc import Awaitable, AsyncIterator
import regex
import lmdb


"""
 An attempt to implement the protocol described in
 https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2020/p1184r2.pdf
 except that description is out-of-date and this attempts to reach parity with
 the implementation in libcody in
 https://gcc.gnu.org/git/?p=gcc.git;a=tree;f=libcody;hb=master
 A somewhat out-of-date version of this library can be found on github at
 https://github.com/urnathan/libcody
"""


# lmdb.Environment(metasync=False, writemap=True, max_readers=8000)
# with env.begin(write=True) as txn:
#     txn.put(key, value)
# with env.begin() as txn:
#     value = txn.get(key)


@dataclass(frozen=True, slots=True)
class GccOptions:
    inputs: list[str]
    output: Optional[str]
    options: list[str]
    mode: Optional[str] # -c, -S, -E, or -fmodule-only

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

Compilation: TypeAlias = Awaitable[list[str]]

_ARG_ARGUMENTS = frozenset({
    '-I', '-L', '-D', '-U', '-MF', '-MT', '-MQ', '-include', '-imacros',
    '-isystem', '-idirafter', '-iprefix', '-iwithprefix', '-iwithprefixbefore',
    '-isysroot', '-imultilib'
})


def parse_gcc_arguments(argv: list[str]) -> GccOptions:
    # Cache in local for minor speed increase.
    argtable = _ARG_ARGUMENTS
    inputs = []
    output: Optional[str] = None
    options = []
    mode = None

    i = 1 # Skip argv[0] which is the compiler name
    while i < len(argv):
        arg = argv[i]

        if arg == '-o':
            if i + 1 < len(argv):
                output = argv[i + 1]
                i += 2
                continue
            else:
                raise RuntimeError(
                    f"Missing argument to -o option ({argv[i:i+1]!r})"
                )
        elif arg.startswith('-o'):
            output = arg[2:]
            i += 1
            continue
        elif arg in ('-c', '-S', '-E', '-fmodule-only'):
            mode = arg
            i += 1
            continue
        elif arg.startswith('-fmodule-mapper='):
            # Remove existing module mapper
            i += 1
            continue
        elif arg == '-fmodule-mapper':
            if len(argv) < i + 2:
                raise RuntimeError(f"Missing argument to -fmodule-mapper option")
            # Remove existing module mapper (two-arg form)
            print(
                "Warning, removing existing module mapper "
                f"({argv[i:i+1]!r}) in favor of mine ", file=sys.stderr
            )
            i += 2
            continue
        elif arg.startswith('-'):
            options.append(arg)
            # Common options that take an argument
            if arg in argtable:
                 if i + 1 < len(argv) and not argv[i+1].startswith('-'):
                     options.append(argv[i+1])
                     i += 2
                     continue
            i += 1
            continue
        else:
            inputs.append(arg)
            i += 1

    return GccOptions(inputs=inputs, output=output, options=options, mode=mode)


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

HEX_RE = regex.compile(b'[0-9a-fA-F]{1,2}')

def split_command(line: bytes) -> list[str]:
    words: list[str] = []
    current_word = bytearray()
    i = 0
    in_word = False

    while i < len(line):
        c = line[i]

        if c in (0x20, 0x09): # WHITESPACE (well, space or tab).
            if in_word:
                words.append(current_word.decode('utf-8'))
                current_word = bytearray()
                in_word = False
            i += 1
            continue

        in_word = True
        if c == 0x27: # APOSTROPHE '
            i += 1
            while i < len(line):
                if line[i] == 0x27: # Closing '
                    i += 1
                    break
                elif line[i] == 0x5c: # BACKSLASH \
                    i += 1
                    if i >= len(line):
                        break
                    esc = line[i]
                    if esc == ord('n'):
                        current_word.append(0x0a)
                        i += 1
                    elif esc == ord('t'):
                        current_word.append(0x09)
                        i += 1
                    elif esc == ord('\''):
                        current_word.append(0x27)
                        i += 1
                    elif esc == ord('\\'):
                        current_word.append(0x5c)
                        i += 1
                    else:
                        # Try to decode hex
                        hex_match = HEX_RE.match(line[i:])
                        if hex_match:
                            hex_val = int(hex_match.group(), 16)
                            current_word.append(hex_val)
                            i += len(hex_match.group())
                        else:
                            # Just append the character if not a valid hex?
                            # The spec says "\ followed by one or two lower case hex characters decode to that octet"
                            current_word.append(esc)
                            i += 1
                else:
                    current_word.append(line[i])
                    i += 1
        else:
            # Unquoted sequence
            current_word.append(c)
            i += 1

    if in_word:
        words.append(current_word.decode('utf-8'))

    return words

SAFE_RE = regex.compile(b'[-+_/%.A-Za-z0-9]+')

def join_command(words: list[str]) -> bytes:
    encoded_words = []
    for word_str in words:
        word = word_str.encode('utf-8')
        if not word:
            encoded_words.append(b"''")
            continue

        needs_quoting = not SAFE_RE.fullmatch(word)

        if not needs_quoting:
            encoded_words.append(word)
        else:
            encoded = bytearray(b"'")
            for c in word:
                if c == b'\n'[0]:
                    encoded.extend(b"\\n")
                elif c == b'\t'[0]:
                    encoded.extend(b"\\t")
                elif c == b"'"[0]:
                    encoded.extend(b"\\'")
                elif c == b'\\'[0]:
                    encoded.extend(b"\\\\")
                # SPACE or DEL
                elif c < 0x20 or c >= 0x7f:
                    encoded.extend(f"\\{c:02x}".encode('ascii'))
                else:
                    encoded.append(c)
            encoded.append(b"'"[0])
            encoded_words.append(bytes(encoded))

    return b" ".join(encoded_words)


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


class ProtocolEngine:
    """Just trying to encapsulate the protocol logic here."""

    mod_name_part = r'(?:[\p{XID_Start}a-zA-Z_][\p{XID_Continue}a-zA-Z0-9_]*)'
    mod_name_re = regex.compile(
        f'^(?P<modname>{mod_name_part}(?:\\.{mod_name_part})*)'
        f'(?P<fragment>:{mod_name_part})?$'
    )
    db_env: lmdb.Environment | None = None
    db_path: Path | None = None
    engine_count = 0

    def __init__(self, c: Configuration, level: str):
        self.config = c
        self.level = level
        local_db_path = c.module_bmi_root / "module_db.mdb"
        if ProtocolEngine.db_env is None:
            assert ProtocolEngine.db_path is None
            ProtocolEngine.db_path = local_db_path
            local_db_path.parent.mkdir(parents=True, exist_ok=True)
            ProtocolEngine.db_env = lmdb.Environment(
                path=str(ProtocolEngine.db_path),
                readonly=False, create=True,
                metasync=False, writemap=True, max_readers=8000
            )
        else:
            assert ProtocolEngine.db_path == local_db_path, \
                "Configuration BMI root changed between "\
                "instantiations of ProtocolEngine "\
                f"({ProtocolEngine.db_path!r} != {local_db_path!r})"
        ProtocolEngine.engine_count += 1
        self.state = 0
        self.config.logger.debug(f"{level}: [========== START ==========]")
        self._handlers = {
            'HELLO': self._handle_hello,
            'MODULE-REPO': self._handle_module_repo,
            'INCLUDE-TRANSLATE': self._handle_include_translate,
            'MODULE-EXPORT': self._handle_module_export,
            'MODULE-COMPILED': self._handle_module_compiled,
            'MODULE-IMPORT': self._handle_module_import,
        }

    def __del__(self):
        if ProtocolEngine.engine_count > 0:
            ProtocolEngine.engine_count -= 1
        if ProtocolEngine.engine_count == 0:
            if ProtocolEngine.db_env is not None:
                ProtocolEngine.db_env.close()
                ProtocolEngine.db_env = None

    def flag_dir(self, path: Path) -> Path:
        # This will use a hash self.config.gcc_options.flags someday
        if not self.config.hash_flags:
            return path
        else:
            return path / "prototype"

    def _handle_hello(self, words: list[str]) -> list[str]:
        if self.state != 0:
            raise Exception('Protocol error: HELLO received after other commands')
        if words[1] != '1':
            raise Exception('Unsupported protocol version')
        if words[2] != 'GCC':
            raise Exception('Unsupported protocol flavor')
        self.state = 1
        return ['HELLO', '1', 'python_cpp_server']

    def _handle_module_repo(self, words: list[str]) -> list[str]:
        if len(words) != 1:
            raise Exception('MODULE-REPO command must have no arguments')
        return ['MODULE-REPO', str(self.flag_dir(self.config.module_bmi_root))]

    def _handle_module_import(
            self, words: list[str]
    ) -> Compilation | list[str]:
        if len(words) != 2:
            raise Exception('MODULE-IMPORT command must have exactly one argument')
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
        return ['BOOL', 'TRUE']

    def _handle_module_export(self, words: list[str]) -> list[str]:
        if len(words) != 2:
            raise Exception('MODULE-EXPORT command must have exactly one argument')
        mod_name = words[1]
        mod_path = self.module_name_to_path(mod_name, 'gcm')
        mod_path = self.config.module_bmi_root / mod_path
        mod_path.parent.mkdir(parents=True, exist_ok=True)
        return ['PATHNAME', str(mod_path)]

    def _handle_module_compiled(self, words: list[str]) -> list[str]:
        if len(words) != 2:
            raise Exception('MODULE-COMPILED command must have exactly one argument')
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


async def main(argv: list[str]):
    """
    This is intended as something to run _instead_ of g++, and will wrap the
    compilation to ensure that any modules are properly compiled.
    """
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(message)s',
        filename='cpp_server.log',
        filemode='a'
    )
    logger = logging.getLogger('cpp_server')
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
