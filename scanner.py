# scanner.py
from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import pandas as pd

from parser.scan import list_pdfs
from parser.extract_text import extract_text_pdfplumber
from parser.parse_fields import parse_bill_fields
from parser.transform import transform_historico_v2
from parser.ocr_fallback import ocr_text_if_needed

BILLS_FOLDER_NAME = "7. Recibos CFE"


# -----------------------------
# Helpers
# -----------------------------
def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def safe_write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def append_jsonl(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def load_done_set(progress_path: Path) -> Set[str]:
    """
    Read progress.jsonl and return set of file_path already processed OK.
    """
    done: Set[str] = set()
    if not progress_path.exists():
        return done

    with open(progress_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                fp = rec.get("file_path")
                status = rec.get("status")
                if fp and status == "ok":
                    done.add(fp)
            except Exception:
                pass
    return done


def extract_path_metadata(root_path: Path, pdf_path: Path) -> Dict[str, Any]:
    rel = pdf_path.relative_to(root_path)
    parts = rel.parts

    mall_folder = parts[0] if parts else None

    try:
        idx = parts.index(BILLS_FOLDER_NAME)
    except ValueError:
        return {
            "mall_folder": mall_folder,
            "recibos_subgroup": None,
            "recibos_year_folder": None,
            "recibos_year": None,
        }

    after = parts[idx + 1 : -1]  # exclude leaf filename
    recibos_subgroup = after[0] if len(after) >= 1 else None

    year_folder = None
    year_value = None
    year_re = re.compile(r"(19|20)\d{2}")

    for seg in after:
        m = year_re.search(seg)
        if not m:
            continue
        y = int(m.group(0))
        if 1990 <= y <= 2035:
            year_folder = seg
            year_value = y
            break

    return {
        "mall_folder": mall_folder,
        "recibos_subgroup": recibos_subgroup,
        "recibos_year_folder": year_folder,
        "recibos_year": year_value,
    }


# -----------------------------
# OCR decision logic
# -----------------------------
def should_force_ocr(text: str, min_chars: int = 80) -> Tuple[bool, str]:
    """
    Force OCR if:
    - too short
    - CID garbage (font artifacts)
    - missing strong anchors (CFE)
    - looks corrupted by weird symbols ratio
    Iberdrola exception: accept if key Iberdrola anchors exist.
    """
    if text is None:
        return True, "none_text"

    s = text.strip()
    if len(s) < min_chars:
        return True, f"too_short<{min_chars}"

    # CID-garbage detector
    cid_count = len(re.findall(r"\(cid:\d+\)", s))
    cid_loose = len(re.findall(r"cid:\d+", s))
    cid_total = max(cid_count, cid_loose)
    if cid_total >= 50:
        return True, f"cid_garbage({cid_total})"

    up = s.upper()

    # Iberdrola exception
    if "IBERDROLA" in up:
        if (("CATEGOR" in up and "TARIF" in up)
            or ("DIVISIÓN TARIFARIA" in up)
            or ("DIVISION TARIFARIA" in up)):
            return False, "ok_iberdrola"
        return True, "iberdrola_missing_anchors"

    # Strong anchors (CFE-like)
    strong_hits = 0
    if "TARIFA" in up: strong_hits += 1
    if "CUENTA" in up: strong_hits += 1
    if "MEDIDOR" in up: strong_hits += 1
    if "RPU" in up: strong_hits += 1
    if re.search(r"NO\.?\s+DE\s+SERVICIO", up): strong_hits += 1

    if strong_hits < 2:
        return True, f"missing_strong_anchors({strong_hits})"

    # Corruption heuristic
    allowed = set(" \n\r\t.,:;/-_()[]$%+°º'\"")
    letters = sum(ch.isalpha() for ch in s)
    weird = sum((not ch.isalnum()) and (ch not in allowed) for ch in s)
    if letters > 0:
        weird_ratio = weird / max(letters, 1)
        if weird_ratio > 0.35:
            return True, f"weird_ratio>{weird_ratio:.2f}"

    return False, "ok"


def extract_text_with_robust_fallback(fp: str, max_pages: int) -> Tuple[str, bool, str, int, int]:
    """
    Returns:
      text, used_ocr, reason, len_pdfplumber, len_final
    """
    text_pdf = extract_text_pdfplumber(fp, max_pages=max_pages) or ""
    force, reason = should_force_ocr(text_pdf)

    if force:
        text_ocr = ocr_text_if_needed(fp, max_pages=max_pages) or ""
        return text_ocr, True, reason, len(text_pdf.strip()), len(text_ocr.strip())

    return text_pdf, False, reason, len(text_pdf.strip()), len(text_pdf.strip())


def coerce_id_fields(fields: Dict[str, Any]) -> None:
    """Keep ID-like fields as strings (avoid scientific notation downstream)."""
    for k in ["no_servicio", "cuenta", "rmu", "rpu", "medidor"]:
        v = fields.get(k)
        if v is not None:
            fields[k] = str(v).strip()


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="Allux Bills Scanner (robust CLI)")
    ap.add_argument("--root", required=True, help="Root folder to scan (e.g., /Users/.../Allux_Fraternity)")
    ap.add_argument("--limit", type=int, default=0, help="Process only first N PDFs (0 = all)")
    ap.add_argument("--pages", type=int, default=2, help="Max pages to read per PDF")
    ap.add_argument("--sleep-ms", type=int, default=0, help="Throttle between PDFs (ms)")
    ap.add_argument("--resume", action="store_true", help="Resume mode: skip files already marked ok in output/progress.jsonl")
    ap.add_argument("--fresh", action="store_true", help="Fresh run: delete output CSV/JSONL before scanning")
    ap.add_argument("--write-every", type=int, default=0, help="Write CSVs every N loop iterations (0 = only at end)")
    args = ap.parse_args()

    root_path = Path(args.root).expanduser()
    if not root_path.exists() or not root_path.is_dir():
        raise SystemExit(f"Root path not found or not a dir: {root_path}")

    out_dir = Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)

    index_csv = out_dir / "bills_index.csv"
    parsed_csv = out_dir / "bills_parsed_v2.csv"
    hist_csv = out_dir / "bills_historico_v2.csv"
    progress_jsonl = out_dir / "progress.jsonl"

    if args.fresh:
        for p in [index_csv, parsed_csv, hist_csv, progress_jsonl]:
            if p.exists():
                p.unlink()

    done_set = load_done_set(progress_jsonl) if args.resume else set()

    print(f"[scan] root={root_path}")
    pdfs = list_pdfs(root_path)
    print(f"[scan] found_pdfs={len(pdfs)}")

    if args.limit and args.limit > 0:
        pdfs = pdfs[: args.limit]
        print(f"[scan] limit applied => {len(pdfs)} PDFs")

    # -------------------
    # Index
    # -------------------
    index_records: List[Dict[str, Any]] = []
    for p in pdfs:
        meta = extract_path_metadata(root_path, p)
        index_records.append({"file_path": str(p), "file_name": p.name, **meta})

    df_index = pd.DataFrame(index_records)
    sort_cols = [c for c in ["mall_folder", "recibos_subgroup", "recibos_year", "file_name"] if c in df_index.columns]
    if sort_cols:
        df_index = df_index.sort_values(sort_cols)

    safe_write_csv(df_index, index_csv)
    print(f"[index] wrote {len(df_index)} rows => {index_csv}")

    # -------------------
    # Parse
    # -------------------
    parsed_rows: List[Dict[str, Any]] = []
    historico_rows: List[Dict[str, Any]] = []

    total = len(df_index)
    processed = 0
    skipped = 0
    errors = 0

    t0 = time.time()

    for i, r in enumerate(df_index.itertuples(index=False), start=1):
        fp = r.file_path

        if args.resume and fp in done_set:
            skipped += 1
            if i % 200 == 0 or i == total:
                print(f"[resume] {i}/{total} skipped={skipped} processed={processed} errors={errors}")
            continue

        meta = {
            "mall_folder": getattr(r, "mall_folder", None),
            "recibos_subgroup": getattr(r, "recibos_subgroup", None),
            "recibos_year_folder": getattr(r, "recibos_year_folder", None),
            "recibos_year": getattr(r, "recibos_year", None),
        }

        print(f"[{i}/{total}] {fp}")

        # ---- Initialize defaults to prevent UnboundLocalError ----
        parse_status = "ok"
        parse_error = ""
        used_ocr = False
        text_source = "pdfplumber"
        reason = "init"
        len_pdfplumber = 0
        len_final = 0
        fields: Dict[str, Any] = {}
        hist: list = []

        try:
            # Hard check for missing file (common in large folder trees)
            if not Path(fp).exists():
                raise FileNotFoundError(fp)

            text, used_ocr, reason, len_pdfplumber, len_final = extract_text_with_robust_fallback(
                fp, max_pages=args.pages
            )
            text_source = "ocr" if used_ocr else "pdfplumber"

            if len_final == 0:
                raise ValueError("empty_text_after_ocr")

            fields = parse_bill_fields(text) or {}
            coerce_id_fields(fields)

            hist = fields.pop("historico_rows", [])
            fields["historico_count"] = len(hist) if isinstance(hist, list) else 0

        except Exception as e:
            parse_status = f"error:{type(e).__name__}"
            parse_error = str(e)
            # keep fields/hist as empty, keep diagnostics defaults where possible
            errors += 1
        else:
            processed += 1

        # Always write a stable row
        fields.update(
            {
                "file_path": fp,
                "file_name": r.file_name,
                **meta,
                "parse_status": parse_status,
                "parse_error": parse_error,
                "text_len": len_final,
                "pdfplumber_text_len": len_pdfplumber,
                "used_ocr": used_ocr,
                "text_source": text_source,
                "text_source_reason": reason,
                "extracted_pages": args.pages,
                "parsed_at": now_iso(),
                "historico_count": fields.get("historico_count", 0),
            }
        )

        parsed_rows.append(fields)

        # Historico rows only when OK and present
        if parse_status == "ok" and isinstance(hist, list) and hist:
            for hr in hist:
                historico_rows.append(
                    {
                        "file_path": fp,
                        "file_name": r.file_name,
                        **meta,
                        "no_servicio": fields.get("no_servicio"),
                        "cuenta": fields.get("cuenta"),
                        "tarifa": fields.get("tarifa"),
                        **hr,
                    }
                )

        # Progress log always
        append_jsonl(
            progress_jsonl,
            {
                "file_path": fp,
                "status": "ok" if parse_status == "ok" else "error",
                "used_ocr": used_ocr,
                "text_source": text_source,
                "reason": reason,
                "at": now_iso(),
                "error": parse_error or None,
            },
        )

        if args.sleep_ms and args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000.0)

        # Periodic writes
        if args.write_every and args.write_every > 0 and (i % int(args.write_every) == 0):
            df_parsed = pd.DataFrame(parsed_rows)
            safe_write_csv(df_parsed, parsed_csv)

            df_hist = pd.DataFrame(historico_rows)
            if not df_hist.empty:
                df_hist_v2 = transform_historico_v2(df_hist)
                safe_write_csv(df_hist_v2, hist_csv)

        if i % 50 == 0 or i == total:
            print(f"[progress] {i}/{total} processed={processed} skipped={skipped} errors={errors}")

    # -------------------
    # Write outputs
    # -------------------
    df_parsed = pd.DataFrame(parsed_rows)
    safe_write_csv(df_parsed, parsed_csv)
    print(f"[parsed] wrote {len(df_parsed)} rows => {parsed_csv}")

    df_hist = pd.DataFrame(historico_rows)
    if not df_hist.empty:
        df_hist_v2 = transform_historico_v2(df_hist)
        safe_write_csv(df_hist_v2, hist_csv)
        print(f"[historico] wrote {len(df_hist_v2)} rows => {hist_csv}")
    else:
        print("[historico] no rows extracted")

    dt = time.time() - t0
    print(f"[done] elapsed_s={dt:.1f} processed={processed} skipped={skipped} errors={errors}")


if __name__ == "__main__":
    main()