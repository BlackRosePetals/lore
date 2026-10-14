import os
import shutil

import pytest
from error_types import LoreException

from lore import Lore


@pytest.mark.regression
def test_directory_to_file_sync(new_lore_repo):
    repo: Lore = new_lore_repo()

    # Create a file that will later become a directory
    # file_that_becomes_a_directory = "ExtremelyLongFileAndDirectoryNamesUnderThisVeryLongDirectoryToTestEllipsisAndClippingWithFirefoxChromeAndSafari"
    file_that_becomes_a_directory = "file_to_directory"
    file_contents = os.urandom(1024)
    repo.write_commit_push(None, {file_that_becomes_a_directory: file_contents})
    assert repo.open_file(file_that_becomes_a_directory, "rb").read() == file_contents

    # Remove the file
    os.remove(repo._fix_path(file_that_becomes_a_directory))
    repo.stage(".", scan=True)
    repo.commit(".")

    # Create a directory with the same name as that file and create a new file nested in it
    os.mkdir(repo._fix_path(file_that_becomes_a_directory))
    nested_file_name = "nested"
    repo.write_commit_push(
        None,
        {
            os.path.join(file_that_becomes_a_directory, nested_file_name): os.urandom(
                1024
            )
        },
    )

    # Sync back to main@1, which needs to turn the directory back into a file
    repo.sync("main@1")
    assert repo.open_file(file_that_becomes_a_directory, "rb").read() == file_contents


@pytest.mark.regression
def test_directory_to_file_reset(new_lore_repo):
    repo: Lore = new_lore_repo()

    # Create a file that will later become a directory
    # file_that_becomes_a_directory = "ExtremelyLongFileAndDirectoryNamesUnderThisVeryLongDirectoryToTestEllipsisAndClippingWithFirefoxChromeAndSafari"
    file_that_becomes_a_directory = "file_to_directory"
    file_contents = os.urandom(1024)
    repo.write_commit_push(None, {file_that_becomes_a_directory: file_contents})
    assert repo.open_file(file_that_becomes_a_directory, "rb").read() == file_contents

    # Remove the file
    os.remove(repo._fix_path(file_that_becomes_a_directory))
    repo.stage(".", scan=True)
    repo.commit(".")

    # Create a directory with the same name as that file and create a new file nested in it
    os.mkdir(repo._fix_path(file_that_becomes_a_directory))
    nested_file_name = "nested"
    repo.write_commit_push(
        None,
        {
            os.path.join(file_that_becomes_a_directory, nested_file_name): os.urandom(
                1024
            )
        },
    )

    # Reset back to main@1, which needs to turn the directory back into a file
    repo.reset(revision="main@1")

    assert repo.open_file(file_that_becomes_a_directory, "rb").read() == file_contents


@pytest.mark.regression
def test_directory_to_file_sync_reset(new_lore_repo):
    repo: Lore = new_lore_repo()

    # Create a file that will later become a directory
    file_that_becomes_a_directory = "file_to_directory"
    file_contents = os.urandom(1024)
    repo.write_commit_push(None, {file_that_becomes_a_directory: file_contents})
    assert repo.open_file(file_that_becomes_a_directory, "rb").read() == file_contents

    # Remove the file
    os.remove(repo._fix_path(file_that_becomes_a_directory))
    repo.stage(".", scan=True)
    repo.commit(".")

    # Create a directory with the same name as that file and create a new file nested in it
    os.mkdir(repo._fix_path(file_that_becomes_a_directory))
    nested_file_name = "nested"
    repo.write_commit_push(
        None,
        {
            os.path.join(file_that_becomes_a_directory, nested_file_name): os.urandom(
                1024
            )
        },
    )

    # Sync back to main@1 with reset, which needs to turn the directory back into a file
    repo.sync("main@1", reset=True)
    assert repo.open_file(file_that_becomes_a_directory, "rb").read() == file_contents


@pytest.mark.regression
def test_directory_to_file_sync_reset_leaves_a_nested_repository_alone(new_lore_repo):
    repo: Lore = new_lore_repo()

    # Create a file that will later become a directory
    file_that_becomes_a_directory = "file_to_directory"
    repo.write_commit_push(None, {file_that_becomes_a_directory: os.urandom(1024)})

    # Remove the file and commit a directory with a nested file in its place
    os.remove(repo._fix_path(file_that_becomes_a_directory))
    repo.stage(".", scan=True)
    repo.commit(".")
    os.mkdir(repo._fix_path(file_that_becomes_a_directory))
    repo.write_commit_push(
        None, {os.path.join(file_that_becomes_a_directory, "nested"): os.urandom(1024)}
    )

    # Replace that directory on disk with another repository's working copy
    shutil.rmtree(repo._fix_path(file_that_becomes_a_directory))
    repo.make_dirs(file_that_becomes_a_directory)
    repo.run(
        ["repository", "create", file_that_becomes_a_directory],
        path=repo._fix_path(file_that_becomes_a_directory),
        offline=True,
    )
    work = os.path.join(file_that_becomes_a_directory, "work")
    with repo.open_file(work, "wb") as handle:
        handle.write(b"another repository's work")

    # A reset sync back to main@1 cannot turn the nested repository back into a file; it must
    # fail without deleting that repository's files
    with pytest.raises(LoreException):
        repo.sync("main@1", reset=True)
    assert repo.open_file(work, "rb").read() == b"another repository's work"
