import random
import time

import pytest
import shutil
from pathlib import Path


@pytest.fixture(scope='session')
def project_root(pytestconfig) -> Path:
    return Path(pytestconfig.rootpath).resolve()


@pytest.fixture(scope='module')
def cpp_project(tmp_path_factory, project_root, request):
    """
    Clones a C++ project directory into a temporary folder.
    The source directory is determined by the 'cpp_project_src' marker.
    Usage: @pytest.mark.cpp_project_src('directory_name')
    """
    marker = request.node.get_closest_marker("cpp_project_src")
    if marker is None or not marker.args:
        pytest.fail("Test module or function must be marked with "
                    "@pytest.mark.cpp_project_src('directory_name')")

    src_dir_name = marker.args[0]
    src_template = project_root / src_dir_name

    if not src_template.exists():
        pytest.fail(f"Source directory '{src_template}' does not exist.")

    # Create a unique temp directory for this module and source directory
    # Using the directory name in the temp path helps with debugging
    dest = tmp_path_factory.mktemp(src_dir_name)

    shutil.copytree(src_template, dest, dirs_exist_ok=True)
    yield dest


@pytest.fixture(scope='session')
def min_file_interval(tmp_path_factory):
    """
    Minimum interval the filesystem can reliably report file changes for.
    """
    tmp_path = tmp_path_factory.mktemp('min_file_interval')
    test_interval = 0.0025
    jitter_floor = 0.00025
    test_file = tmp_path / "fs_time_res_test.txt"
    while True:
        if test_interval > 1.0:
            raise Exception("Filesystem time resolution is too coarse!")
        # Try to make sure the whole phase space is covered within 20 iterations
        jitter = jitter_floor + test_interval * 0.05
        # This is designed to catch 'aliasing' issues and use the random
        # part to break phase locks. Want to make sure (without knowing
        # what the resolution of the ticking clock might be) that we aren't
        # straddling a clock tick for all 20 iterations and getting a false
        # positive.
        for counter in range(20):
            test_file.write_text(f'{test_interval}-{counter}')
            stat = test_file.stat()
            time.sleep(test_interval)
            test_file.write_text(f'{test_interval}-{counter}-2')
            new_stat = test_file.stat()
            if new_stat.st_mtime_ns <= stat.st_mtime_ns:
                test_interval *= 2
                break
            time.sleep(jitter * (random.random() + 0.125))
        else:
            # Only if the for loop runs to completion without breaking
            return test_interval
