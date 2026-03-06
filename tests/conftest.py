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
