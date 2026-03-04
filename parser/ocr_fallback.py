# parser/ocr_fallback.py
from __future__ import annotations
from pathlib import Path

def ocr_text_if_needed(pdf_path: str | Path, max_pages: int = 2) -> str:
    """
    OCR fallback for scanned PDFs.
    Only used when pdfplumber returns empty text.
    """
    pdf_path = Path(pdf_path)

    try:
        from pdf2image import convert_from_path
        import pytesseract
    except Exception:
        return ""

    try:
        images = convert_from_path(
            str(pdf_path),
            first_page=1,
            last_page=max_pages,
            dpi=200
        )
    except Exception:
        return ""

    chunks = []
    for img in images:
        try:
            txt = pytesseract.image_to_string(img, lang="spa+eng")
            if txt:
                chunks.append(txt)
        except Exception:
            continue

    return "\n".join(chunks).strip()