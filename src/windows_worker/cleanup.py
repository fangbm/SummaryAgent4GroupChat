from __future__ import annotations

import time
from pathlib import Path


def cleanup_old_files(output_dir: str | Path, *, older_than_days: int) -> int:
    root = Path(output_dir)
    if not root.exists():
        return 0
    cutoff = time.time() - older_than_days * 24 * 3600
    removed = 0
    for path in root.glob("*"):
        if not path.is_file():
            continue
        if path.stat().st_mtime < cutoff:
            path.unlink()
            removed += 1
    return removed
