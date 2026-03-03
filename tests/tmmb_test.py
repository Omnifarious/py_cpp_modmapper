import subprocess
import os
import pytest
from pathlib import Path
import shutil

def test_cpp_project_compilation():
    project_root = Path(__file__).parent.parent.resolve()
    test_dir = project_root / 'test_cpp_project'

    shutil.rmtree(test_dir / 'build', ignore_errors=True)
    shutil.rmtree(test_dir / 'foo', ignore_errors=True)
    shutil.rmtree(test_dir / 'gcm.cache', ignore_errors=True)
    # Ensure build directory exists
    (test_dir / 'build').mkdir(exist_ok=True)

    env = os.environ.copy()
    env['PYTHONPATH'] = str(project_root)

    # PYTHONPATH=$(hg root) python3 -m py_cpp_modmapper.main -fmodules -march=native -mtune=native -std=c++23 -c src/top-impl.cpp -o build/top-impl.o
    cmd1 = ['python3', '-m', 'py_cpp_modmapper.main', '-fmodules', '-march=native', '-mtune=native', '-std=c++23', '-c', 'src/top-impl.cpp', '-o', 'build/top-impl.o']
    result = subprocess.run(cmd1, cwd=test_dir, env=env, capture_output=True, text=True)
    assert result.returncode == 0, f"Command 1 failed: {' '.join(cmd1)}\nStdout: {result.stdout}\nStderr: {result.stderr}"

    # PYTHONPATH=$(hg root) python3 -m py_cpp_modmapper.main -fmodules -march=native -mtune=native -std=c++23 -c src/main.cpp -o build/main.o
    cmd2 = ['python3', '-m', 'py_cpp_modmapper.main', '-fmodules', '-march=native', '-mtune=native', '-std=c++23', '-c', 'src/main.cpp', '-o', 'build/main.o']
    result = subprocess.run(cmd2, cwd=test_dir, env=env, capture_output=True, text=True)
    assert result.returncode == 0, f"Command 2 failed: {' '.join(cmd2)}\nStdout: {result.stdout}\nStderr: {result.stderr}"

    # PYTHONPATH=$(hg root) python3 -m py_cpp_modmapper.main -fmodules -march=native -mtune=native -o foo build/*.o $(find modules -name '*.o')
    # Using shell=True for expansion of build/*.o and $(find ...)
    cmd3 = "python3 -m py_cpp_modmapper.main -fmodules -march=native -mtune=native -o foo build/*.o $(find modules -name '*.o')"
    result = subprocess.run(cmd3, cwd=test_dir, env=env, shell=True, capture_output=True, text=True)
    assert result.returncode == 0, f"Command 3 failed: {cmd3}\nStdout: {result.stdout}\nStderr: {result.stderr}"

    # ./foo
    result = subprocess.run(['./foo'], cwd=test_dir, capture_output=True, text=True)
    assert result.returncode == 0, f"./foo failed\nStdout: {result.stdout}\nStderr: {result.stderr}"
