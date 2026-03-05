import sys
from dataclasses import dataclass
from typing import Optional

import regex

_ARG_ARGUMENTS = frozenset({
    '-I', '-L', '-D', '-U', '-MF', '-MT', '-MQ', '-include', '-imacros',
    '-isystem', '-idirafter', '-iprefix', '-iwithprefix', '-iwithprefixbefore',
    '-isysroot', '-imultilib'
})


@dataclass(frozen=True, slots=True)
class GccOptions:
    inputs: list[str]
    output: Optional[str]
    options: list[str]
    mode: Optional[str] # -c, -S, -E, or -fmodule-only


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
