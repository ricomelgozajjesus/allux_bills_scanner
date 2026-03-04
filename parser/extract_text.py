from __future__ import annotations
from pathlib import Path
import pdfplumber

def extract_text_pdfplumber(pdf_path: str | Path, max_pages: int = 2) -> str:
    """
    Extract text from the first `max_pages` pages of a PDF.
    Works great for digital PDFs (text embedded).
    """
    pdf_path = Path(pdf_path)
    chunks = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages[:max_pages]:
            chunks.append(page.extract_text() or "")

    return "\n".join(chunks).strip()