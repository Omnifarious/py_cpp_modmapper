import subprocess
from pathlib import Path
import shutil

import pytest

from py_cpp_modmapper.main import main as modmapper_main
from contextlib import chdir
import glob

@pytest.fixture(scope='session')
def project_root(pytestconfig) -> Path:
    return Path(pytestconfig.rootpath).resolve()

@pytest.fixture(scope='module')
def cpp_project(tmp_path_factory, project_root):
    src_template = project_root / 'test_cpp_project'
    dest = tmp_path_factory.mktemp('test_cpp_project')
    dest.rmdir()
    shutil.copytree(src_template, dest)
    yield dest

async def test_cpp_project_compilation(cpp_project: Path):
    test_dir = cpp_project

    assert not (test_dir / 'build').exists()
    assert not (test_dir / 'foo').exists()
    assert not (test_dir / 'gcm.cache').exists()
    assert not (test_dir / 'cpp_server.log').exists()

    # Ensure build directory exists
    (test_dir / 'build').mkdir(exist_ok=True)

    # env = os.environ.copy()
    # env['PYTHONPATH'] = str(project_root)

    with chdir(test_dir):
        # python3 -m py_cpp_modmapper.main -fmodules -march=native -mtune=native -std=c++23 -c src/top-impl.cpp -o build/top-impl.o
        cmd1 = ['py_cpp_modmapper.main', '-fmodules', '-march=native', '-mtune=native', '-std=c++23', '-c', 'src/top-impl.cpp', '-o', 'build/top-impl.o']
        try:
            await modmapper_main(cmd1)
        except SystemExit as e:
            assert e.code == 0 or e.code is None, f"Command 1 failed with exit code {e.code}"

        # python3 -m py_cpp_modmapper.main -fmodules -march=native -mtune=native -std=c++23 -c src/main.cpp -o build/main.o
        cmd2 = ['py_cpp_modmapper.main', '-fmodules', '-march=native', '-mtune=native', '-std=c++23', '-c', 'src/main.cpp', '-o', 'build/main.o']
        try:
            await modmapper_main(cmd2)
        except SystemExit as e:
            assert e.code == 0 or e.code is None, f"Command 2 failed with exit code {e.code}"

        # python3 -m py_cpp_modmapper.main -fmodules -march=native -mtune=native -o foo build/*.o $(find modules -name '*.o')
        # Handle globbing manually
        objs = glob.glob('build/*.o')
        module_objs = []
        for p in Path('modules').rglob('*.o'):
            module_objs.append(str(p))

        cmd3 = ['py_cpp_modmapper.main', '-fmodules', '-march=native', '-mtune=native', '-o', 'foo'] + objs + module_objs
        try:
            await modmapper_main(cmd3)
        except SystemExit as e:
            assert e.code == 0 or e.code is None, f"Command 3 failed with exit code {e.code}"

        # ./foo
        result = subprocess.run(['./foo'], capture_output=True, text=True)
        assert result.returncode == 0, f"./foo failed\nStdout: {result.stdout}\nStderr: {result.stderr}"
