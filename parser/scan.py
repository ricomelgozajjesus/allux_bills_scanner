# parser/scan.py
from __future__ import annotations

import os
from pathlib import Path
from typing import List

BILLS_FOLDER_NAME = "7. Recibos CFE"


def list_pdfs(root: Path, bills_folder_name: str = BILLS_FOLDER_NAME) -> List[Path]:
    """
    Recursively list all PDFs under `root`, but ONLY those inside:
        root/<mall>/7. Recibos CFE/.../*.pdf

    Uses os.walk(..., followlinks=True) to traverse your symlinked mall folders.
    Optimization: once inside a mall folder, we prune to ONLY walk into '7. Recibos CFE'.
    """
    root = Path(root).expanduser()

    if not root.exists() or not root.is_dir():
        return []

    pdfs: List[Path] = []

    for dirpath, dirnames, filenames in os.walk(root, followlinks=True):
        dp = Path(dirpath)

        # If we're at: root/<mall>/  -> prune to only '7. Recibos CFE'
        # This avoids walking 0..6 folders and everything else.
        if dp.parent == root:
            dirnames[:] = [d for d in dirnames if d == bills_folder_name]

        # Collect PDFs only if we are under ".../7. Recibos CFE/..."
        if bills_folder_name in dp.parts:
            for fn in filenames:
                if fn.lower().endswith(".pdf"):
                    pdfs.append(dp / fn)

    pdfs.sort(key=lambda p: str(p).lower())
    return pdfs