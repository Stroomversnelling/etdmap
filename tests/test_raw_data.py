import os
import shutil

import pytest  # type: ignore # noqa: F401


def test_raw_data_fixture(raw_data_fixture, request):
    # Access the --copy-data option
    copy_to_persistent = request.config.getoption("--copy-data")


    if copy_to_persistent:
        print(f"Copy to persistent data folder: {copy_to_persistent}")
        # Path to the persistent directory
        persistent_dir = os.path.join(os.getcwd(), "persistent_output")

        # Ensure the persistent directory exists
        os.makedirs(persistent_dir, exist_ok=True)

        # Create a subdirectory in the persistent directory with the same name as the temporary directory
        persistent_subdir = os.path.join(persistent_dir, raw_data_fixture.name)
        os.makedirs(persistent_subdir, exist_ok=True)

        # Empty the subdirectory
        for filename in os.listdir(persistent_subdir):
            file_path = os.path.join(persistent_subdir, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")

        shutil.copytree(raw_data_fixture, persistent_subdir, dirs_exist_ok=True)

        print(f"Copied data to: {persistent_subdir}")
