# parser/historico.py
from __future__ import annotations
import re
from typing import List, Dict, Any

MONTHS_ES = {
    "ENE": 1, "FEB": 2, "MAR": 3, "ABR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AGO": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DIC": 12
}

def _num_clean(s: str | None) -> str | None:
    """Convert '13,752.38' -> '13752.38' (string)."""
    if not s:
        return None
    return s.replace("$", "").replace(",", "").replace(" ", "").strip()

def _parse_es_date(s: str | None) -> str | None:
    """Parse '07 NOV 25' -> '2025-11-07' (ISO string)."""
    if not s:
        return None
    s = s.strip().upper()
    m = re.match(r"(\d{1,2})\s+([A-ZÁÉÍÓÚÑ]{3})\s+(\d{2,4})$", s)
    if not m:
        return None
    dd = int(m.group(1))
    mon = m.group(2)[:3]
    yy = int(m.group(3))
    if yy < 100:
        yy += 2000
    mm = MONTHS_ES.get(mon)
    if not mm:
        return None
    return f"{yy:04d}-{mm:02d}-{dd:02d}"

def parse_historico(text: str) -> List[Dict[str, Any]]:
    """
    Extract rows from the 'CONSUMO HISTÓRICO' table.

    Returns list of dicts:
      {
        periodo_inicio_raw, periodo_fin_raw,
        periodo_inicio, periodo_fin,
        kwh, importe, pagos_pendientes
      }
    """
    start = re.search(r"CONSUMO\s+HIST[ÓO]RICO", text, re.IGNORECASE)
    if not start:
        return []

    tail = text[start.end():]

    # Stop at common footer headers to avoid false matches
    stop = re.search(
        r"(Datos\s+Fiscales|Cadena\s+Original|Instancias\s+y\s+recursos|Este\s+documento|-2-)",
        tail,
        re.IGNORECASE
    )
    if stop:
        tail = tail[:stop.start()]

    # Example line:
    # del 20 JUN 25 al 20 AGO 25 2556 $12,368.00 $12,368.00
    pattern = re.compile(
        r"del\s+(\d{1,2}\s+[A-ZÁÉÍÓÚÑ]{3}\s+\d{2,4})\s+al\s+(\d{1,2}\s+[A-ZÁÉÍÓÚÑ]{3}\s+\d{2,4})\s+"
        r"([0-9][0-9,\.]*)\s+\$?\s*([0-9][0-9,\.]*)\s+\$?\s*([0-9][0-9,\.]*)",
        re.IGNORECASE
    )

    rows: List[Dict[str, Any]] = []
    for m in pattern.finditer(tail):
        ini_raw = m.group(1).strip()
        fin_raw = m.group(2).strip()

        rows.append({
            "periodo_inicio_raw": ini_raw,
            "periodo_fin_raw": fin_raw,
            "periodo_inicio": _parse_es_date(ini_raw),
            "periodo_fin": _parse_es_date(fin_raw),
            "kwh": _num_clean(m.group(3)),
            "importe": _num_clean(m.group(4)),
            "pagos_pendientes": _num_clean(m.group(5)),
        })

    return rows