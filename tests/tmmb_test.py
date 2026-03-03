import subprocess
from pathlib import Path

import pytest

from py_cpp_modmapper.main import main as modmapper_main
from contextlib import chdir
import glob

pytestmark = pytest.mark.cpp_project_src('test_cpp_project')

async def test_cpp_project_compilation(cpp_project: Path):
    test_dir = cpp_project

    assert not (test_dir / 'build').exists()
    assert not (test_dir / 'foo').exists()
    assert not (test_dir / 'gcm.cache').exists()
    assert not (test_dir / 'cpp_server.log').exists()

    # Ensure output directory for direct build products exists
    (test_dir / 'build').mkdir(exist_ok=True)

    with chdir(test_dir):
        cmd1 = [
            'py_cpp_modmapper.main', '-fmodules', '-march=native',
            '-mtune=native', '-std=c++23', '-c', 'src/top-impl.cpp',
            '-o', 'build/top-impl.o'
        ]
        try:
            await modmapper_main(cmd1)
        except SystemExit as e:
            assert e.code == 0 or e.code is None, f"{cmd1!r} failed with exit code {e.code}"

        cmd2 = [
            'py_cpp_modmapper.main', '-fmodules', '-march=native',
            '-mtune=native', '-std=c++23', '-c', 'src/main.cpp',
            '-o', 'build/main.o'
        ]
        try:
            await modmapper_main(cmd2)
        except SystemExit as e:
            assert e.code == 0 or e.code is None, f"{cmd2!r} failed with exit code {e.code}"

        # Compile list of all generated object files
        objs = glob.glob('build/*.o')
        module_objs = []
        for p in Path('modules').rglob('*.o'):
            module_objs.append(str(p))

        cmd3 = [
                   'py_cpp_modmapper.main', '-fmodules', '-march=native',
                   '-mtune=native', '-o', 'foo'
               ] + objs + module_objs
        try:
            await modmapper_main(cmd3)
        except SystemExit as e:
            assert e.code == 0 or e.code is None, f"Command 3 failed with exit code {e.code}"

        # ./foo
        result = subprocess.run(['./foo'], capture_output=True, text=True)
        assert result.returncode == 0, (
            f"./foo failed\n"
            f"Stdout: {result.stdout}\n"
            f"Stderr: {result.stderr}")
