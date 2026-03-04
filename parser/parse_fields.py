# parser/parse_fields.py
from __future__ import annotations

import re

MONTHS_ES = {
    "ENE": 1, "FEB": 2, "MAR": 3, "ABR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AGO": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DIC": 12
}


def _find_first(pattern: str, text: str, flags: int = 0) -> str | None:
    m = re.search(pattern, text or "", flags)
    return m.group(1).strip() if m else None


def _digits_only(s: str | None) -> str | None:
    """Keep only digits from a string (for service numbers, RMU, etc.)."""
    if not s:
        return None
    d = re.sub(r"\D+", "", s)
    return d if d else None


def _alnum_only(s: str | None) -> str | None:
    """Keep only A-Z0-9 (useful for CUENTA / MEDIDOR when OCR adds noise)."""
    if not s:
        return None
    cleaned = re.sub(r"[^A-Z0-9]", "", s.upper())
    return cleaned if cleaned else None


def normalize_tarifa(value: str | None) -> str | None:
    """
    Normalize tariff strings, especially OCR noise like:
      PDBTNO -> PDBT
      GDMTONO -> GDMTO
      GDBTNO -> GDBT
      GDMTHNO -> GDMTH
    """
    if not isinstance(value, str):
        return None

    t = value.strip().upper().replace(" ", "")

    if not t:
        return None

    # Known specific noise
    if t == "PDBTNO":
        return "PDBT"

    # Keep known base prefixes if OCR appends junk
    known_prefixes = ("PDBT", "GDBT", "GDMTO", "GDMTH")
    for k in known_prefixes:
        if t.startswith(k):
            return k

    return t


def _num_clean(s: str | None) -> str | None:
    """Normalize currency/number strings to a plain numeric string (no commas, no $)."""
    if not s:
        return None
    s2 = s.replace("$", "").replace(" ", "").replace(",", "")
    s2 = s2.strip()
    return s2 if s2 else None


def _parse_es_date(s: str | None) -> str | None:
    """
    Parse '07 NOV 25' -> '2025-11-07' (ISO).
    """
    if not s:
        return None
    s = s.strip().upper()
    m = re.match(r"(\d{1,2})\s+([A-ZÁÉÍÓÚÑ]{3})\s+(\d{2,4})", s)
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


def detect_document_type(text: str) -> tuple[str, str]:
    t = (text or "").upper()

    # Iberdrola
    if "IBERDROLA" in t:
        return ("IBERDROLA", "iberdrola_cfdi")

    # CFE SSB
    if "CFE SUMINISTRADOR DE SERVICIOS BASICOS" in t:
        return ("CFE", "cfe_bill")

    # RFC CFE clásico
    if re.search(r"RFC:\s*CFE\d{6}", t):
        return ("CFE", "cfe_bill")

    # RFC CFE SSB
    if "CSS160330CP7" in t:
        return ("CFE", "cfe_bill")

    # Encabezado clásico
    if "COMISION FEDERAL DE ELECTRICIDAD" in t:
        return ("CFE", "cfe_bill")

    return ("UNKNOWN", "unknown")

def parse_bill_fields(text: str) -> dict:
    out: dict[str, object] = {}

    # -------------------------
    # Identify doc type
    # -------------------------
    source_utility, document_type = detect_document_type(text)
    out["source_utility"] = source_utility
    out["document_type"] = document_type

    # -------------------------
    # Identity / IDs
    # -------------------------
    out["cliente_nombre"] = _find_first(
        r"^\s*([A-ZÁÉÍÓÚÑ0-9\.\-&,/ ]+?)\s+TOTAL\s+A\s+PAGAR\s*:",
        text,
        flags=re.IGNORECASE | re.MULTILINE,
    )

    # CFE: NO. DE SERVICIO / CUENTA / RMU
    raw_no_servicio = _find_first(r"NO\.\s*DE\s*SERVICIO\s*:\s*([0-9 \-]+)", text, re.IGNORECASE)
    out["no_servicio"] = _digits_only(raw_no_servicio)

    raw_cuenta = _find_first(r"CUENTA\s*:\s*([A-Z0-9 \-]+)", text, re.IGNORECASE)
    # Keep alphanumerics only (safer than only stripping spaces/hyphens)
    out["cuenta"] = _alnum_only(raw_cuenta)

    raw_rmu = _find_first(r"RMU\s*:\s*([0-9 \-]+)", text, re.IGNORECASE)
    out["rmu"] = _digits_only(raw_rmu)

    # RPU: allow spaces/hyphens then clean
    raw_rpu = _find_first(r"\bRPU\s*:\s*([A-Z]{0,3}[0-9 \-]{6,})\b", text, re.IGNORECASE)
    if raw_rpu:
        out["rpu"] = raw_rpu.replace(" ", "").replace("-", "").strip()
    else:
        out["rpu"] = None

    # -------------------------
    # Dates (CFE)
    # -------------------------
    limite_pago_raw = _find_first(
        r"L[ÍI]MITE\s+DE\s+PAGO\s*:\s*([0-9]{1,2}\s+[A-ZÁÉÍÓÚÑ]{3}\s+[0-9]{2,4})",
        text,
        re.IGNORECASE,
    )
    corte_raw = _find_first(
        r"CORTE\s+A\s+PARTIR\s*:\s*([0-9]{1,2}\s+[A-ZÁÉÍÓÚÑ]{3}\s+[0-9]{2,4})",
        text,
        re.IGNORECASE,
    )
    out["limite_pago"] = _parse_es_date(limite_pago_raw)
    out["corte_a_partir"] = _parse_es_date(corte_raw)

    m = re.search(
        r"PERIODO\s+FACTURADO\s*:\s*([0-9]{1,2}\s+[A-ZÁÉÍÓÚÑ]{3}\s+[0-9]{2,4})\s*-\s*([0-9]{1,2}\s+[A-ZÁÉÍÓÚÑ]{3}\s+[0-9]{2,4})",
        text or "",
        re.IGNORECASE,
    )
    if m:
        out["periodo_inicio"] = _parse_es_date(m.group(1))
        out["periodo_fin"] = _parse_es_date(m.group(2))
    else:
        out["periodo_inicio"] = None
        out["periodo_fin"] = None

    # -------------------------
    # Tariff / meter
    # -------------------------
    tarifa: str | None = None

    # CFE standard: "TARIFA: XXXX"
    raw_tarifa = _find_first(r"\bTARIFA\s*:\s*([A-Z0-9 ]+)", text, re.IGNORECASE)
    tarifa = normalize_tarifa(raw_tarifa)

    # Iberdrola/MEM style: "Categoría Tarifaria XXXX"
    if not tarifa:
        raw_tarifa2 = _find_first(
            r"\bCATEGOR[ÍI]A\s+TARIFARIA\s+([A-Z0-9]+)\b",
            text,
            re.IGNORECASE,
        )
        tarifa = normalize_tarifa(raw_tarifa2)

    out["tarifa"] = tarifa

    # Meter-related fields
    out["medidor"] = _alnum_only(_find_first(r"\bMEDIDOR\s*:\s*([A-Z0-9\- ]+)\b", text, re.IGNORECASE))
    out["multiplicador"] = _num_clean(_find_first(r"\bMULTIPLICADOR\s*:\s*([0-9\.,]+)\b", text, re.IGNORECASE))
    out["no_hilos"] = _find_first(r"\bNO\s+HILOS\s*:\s*([0-9]+)\b", text, re.IGNORECASE)

    # -------------------------
    # Energy block (CFE-like)
    # -------------------------
    m = re.search(
        r"Energ[ií]a\s*\(kWh\)\s*([0-9][0-9,\.]*)\s+([0-9][0-9,\.]*)\s+([0-9][0-9,\.]*)",
        text or "",
        re.IGNORECASE,
    )
    if m:
        out["lectura_actual_kwh"] = _num_clean(m.group(1))
        out["lectura_anterior_kwh"] = _num_clean(m.group(2))
        out["kwh_total"] = _num_clean(m.group(3))
    else:
        out["lectura_actual_kwh"] = None
        out["lectura_anterior_kwh"] = None
        out["kwh_total"] = None

    # Iberdrola often shows "... kWh" somewhere
    if not out.get("kwh_total"):
        kwh_any = _find_first(r"([0-9][0-9,\.]*)\s*kWh\b", text, re.IGNORECASE)
        if kwh_any:
            out["kwh_total"] = _num_clean(kwh_any)

    # -------------------------
    # Money totals
    # -------------------------
    out["importe_total"] = _num_clean(
        _find_first(
            r"TOTAL\s+A\s+PAGAR\s*:?\s*[\r\n]*\s*\$?\s*([0-9][0-9\.,]*)",
            text,
            re.IGNORECASE,
        )
    )

    out["cargo_fijo"] = _num_clean(_find_first(r"Cargo\s+Fijo.*?\s([0-9][0-9\.,]*)", text, re.IGNORECASE))
    out["subtotal_energia"] = _num_clean(_find_first(r"\bEnerg[ií]a\b\s+([0-9][0-9\.,]*)", text, re.IGNORECASE))
    out["subtotal"] = _num_clean(_find_first(r"\bSubtotal\b\s+([0-9][0-9\.,]*)", text, re.IGNORECASE))
    out["iva"] = _num_clean(_find_first(r"\bIVA\b\s+\d+%?\s+([0-9][0-9\.,]*)", text, re.IGNORECASE))
    out["fac_del_periodo"] = _num_clean(_find_first(r"Fac\.\s*del\s*Periodo\s+([0-9][0-9\.,]*)", text, re.IGNORECASE))
    out["total_linea"] = _num_clean(_find_first(r"^\s*Total\s+([0-9][0-9\.,]*)\s*$", text, re.IGNORECASE | re.MULTILINE))

    # -------------------------
    # Historic table (only CFE)
    # -------------------------
    hist: list[dict] = []
    if document_type == "cfe_bill":
        from parser.historico import parse_historico
        hist = parse_historico(text)

    out["tiene_consumo_historico"] = len(hist) > 0
    out["historico_count"] = len(hist)
    out["historico_rows"] = hist

    # -------------------------
    # Final cleanup: ensure ID-like fields are strings
    # -------------------------
    for k in ["no_servicio", "cuenta", "rmu", "rpu", "medidor"]:
        v = out.get(k)
        if v is not None:
            out[k] = str(v).strip()

    return out