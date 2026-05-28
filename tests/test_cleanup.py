import os
import time

from windows_worker.cleanup import cleanup_old_files


def test_cleanup_old_files(tmp_path) -> None:
    old_file = tmp_path / "old.png"
    new_file = tmp_path / "new.png"
    old_file.write_bytes(b"old")
    new_file.write_bytes(b"new")
    old_mtime = time.time() - 10 * 24 * 3600
    os.utime(old_file, (old_mtime, old_mtime))

    removed = cleanup_old_files(tmp_path, older_than_days=7)

    assert removed == 1
    assert not old_file.exists()
    assert new_file.exists()
