# enrich_local.py

from pathlib import Path
import argparse
import re
import time
import math

import pandas as pd


# ============================================================
# Utilidades básicas
# ============================================================

def clean_number(value):
    """
    Convierte strings como:
        '1,234.56'
        '$ 12,345.00'
        '98.45%'
    a float.
    """
    if value is None:
        return None

    value = str(value)
    value = value.replace("$", "")
    value = value.replace(",", "")
    value = value.replace("%", "")
    value = value.strip()

    if value == "":
        return None

    try:
        return float(value)
    except ValueError:
        return None


def normalize_spaces(text):
    if text is None:
        return ""
    return re.sub(r"[ \t]+", " ", str(text)).strip()


def safe_str(value):
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value)


def get_pdf_path_from_row(row):
    """
    Busca la ruta del PDF usando varios nombres posibles de columna.

    En el CSV actual la columna correcta es:
        file_path
    """
    candidates = [
        "file_path",
        "pdf_path",
        "source_path",
        "source_file",
        "path",
        "pdf_file",
        "pdf",
        "filepath",
    ]

    for col in candidates:
        value = row.get(col)
        if value is not None and not pd.isna(value) and str(value).strip():
            value = str(value).strip()
            if Path(value).exists():
                return value

    return None


# ============================================================
# Extracción de texto desde PDF
# ============================================================

def extract_text_from_pdf(pdf_path, max_pages=None):
    """
    Extrae texto de un PDF usando pypdf.

    Requiere:
        pip install pypdf

    Nota:
        No hace OCR. Si el PDF es imagen escaneada, regresará poco texto.
    """
    from pypdf import PdfReader

    pdf_path = Path(pdf_path)

    reader = PdfReader(str(pdf_path))
    pages = reader.pages

    if max_pages is not None:
        pages = pages[:max_pages]

    chunks = []

    for page in pages:
        try:
            page_text = page.extract_text() or ""
            chunks.append(page_text)
        except Exception:
            chunks.append("")

    return "\n".join(chunks)


# ============================================================
# Extracción de dirección
# ============================================================

def looks_like_cfe_corporate_address(address):
    """
    Detecta direcciones institucionales/corporativas que no corresponden
    al domicilio del cliente.
    """
    if not address:
        return False

    txt = safe_str(address).upper()

    bad_patterns = [
        "PASEO DE LA REFORMA",
        "REFORMA 164",
        "COL. JUAREZ",
        "COL. JUÁREZ",
        "CUAUHTEMOC",
        "CUAUHTÉMOC",
        "06600",
        "CIUDAD DE MEXICO",
        "CIUDAD DE MÉXICO",
        "CÓDIGO POSTAL",
        "CODIGO POSTAL",
    ]

    return any(p in txt for p in bad_patterns)


def extract_business_address(text, cliente_nombre=None):
    """
    Extrae una dirección probable del cliente.

    Estrategia:
    - Preferir líneas con 'C.P.' porque en los recibos observados
      los clientes usan C.P.
    - Ignorar líneas con 'Código Postal', que suelen corresponder
      a dirección institucional de CFE.
    - Conservar direccion_raw, direccion_source y direccion_confidence
      para auditoría.
    """
    result = {
        "direccion_completa": None,
        "direccion_calle": None,
        "direccion_colonia": None,
        "direccion_municipio": None,
        "direccion_estado": None,
        "direccion_cp": None,
        "direccion_raw": None,
        "direccion_source": None,
        "direccion_confidence": None,
    }

    lines = [normalize_spaces(ln) for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]

    if not lines:
        result["direccion_source"] = "no_text_lines"
        result["direccion_confidence"] = "none"
        return result

    client_cp_pattern = r"\bC\.?\s*P\.?\s*[:\-]?\s*\d{5}\b"
    corporate_cp_pattern = r"C[oó]digo\s+Postal\s*[:\-]?\s*\d{5}"

    candidate_lines = []

    for i, line in enumerate(lines):
        # Evitar explícitamente la forma institucional/corporativa
        if re.search(corporate_cp_pattern, line, flags=re.IGNORECASE):
            continue

        # Preferir la forma usada por clientes
        if re.search(client_cp_pattern, line, flags=re.IGNORECASE):
            start = max(0, i - 4)
            end = min(len(lines), i + 2)
            candidate_lines = lines[start:end]
            result["direccion_source"] = "client_cp_pattern"
            result["direccion_confidence"] = "medium"
            break

    if not candidate_lines:
        result["direccion_source"] = "not_found"
        result["direccion_confidence"] = "none"
        return result

    cleaned = []

    for ln in candidate_lines:
        if cliente_nombre and safe_str(cliente_nombre).upper() in ln.upper():
            continue

        # Evitar líneas administrativas del recibo
        if re.search(
            r"NO\.?\s*DE\s*SERVICIO|N[ÚU]MERO\s*DE\s*SERVICIO|RMU|RPU|CUENTA|"
            r"TARIFA|MEDIDOR|TOTAL\s+A\s+PAGAR|L[IÍ]MITE\s+DE\s+PAGO|CORTE|"
            r"PERIODO|MULTIPLICADOR|HILOS|CARGA\s+CONECTADA|DEMANDA\s+CONTRATADA",
            ln,
            flags=re.IGNORECASE,
        ):
            continue

        # Evitar encabezados e institución
        if re.search(
            r"COMISI[ÓO]N\s+FEDERAL|CFE|SUMINISTRADOR|RECIBO|AVISO|"
            r"DATOS\s+DEL\s+SERVICIO|C[oó]digo\s+Postal",
            ln,
            flags=re.IGNORECASE,
        ):
            continue

        cleaned.append(ln)

    direccion = " ".join(cleaned).strip()
    result["direccion_raw"] = " | ".join(candidate_lines)

    if not direccion:
        result["direccion_source"] = "client_cp_pattern_empty_after_cleaning"
        result["direccion_confidence"] = "none"
        return result

    if looks_like_cfe_corporate_address(direccion):
        result["direccion_completa"] = None
        result["direccion_source"] = "rejected_cfe_corporate_address"
        result["direccion_confidence"] = "rejected"
        return result

    result["direccion_completa"] = direccion

    cp_match = re.search(
        r"\bC\.?\s*P\.?\s*[:\-]?\s*(\d{5})\b",
        direccion,
        flags=re.IGNORECASE,
    )

    if cp_match:
        result["direccion_cp"] = cp_match.group(1)
    else:
        cp_match = re.search(r"\b(\d{5})\b", direccion)
        if cp_match:
            result["direccion_cp"] = cp_match.group(1)

    return result


# ============================================================
# Extracción de tabla horaria
# ============================================================

def numbers_in_line(line):
    """
    Regresa todos los números detectados en una línea.
    """
    return re.findall(r"[-+]?\$?\s?\d[\d,]*\.?\d*", line)


def last_number_from_matching_lines(lines, patterns):
    """
    Busca líneas que coincidan con alguno de los patrones y regresa
    el último número de la primera línea útil.

    Esto evita capturar el (3) de notas como:
        Cargo Factor de Potencia(3) 6,319.29

    En ese caso regresa 6319.29, no 3.
    """
    for line in lines:
        line_clean = normalize_spaces(line)

        for pattern in patterns:
            if re.search(pattern, line_clean, flags=re.IGNORECASE):
                nums = numbers_in_line(line_clean)
                if nums:
                    return clean_number(nums[-1])

    return None


def extract_last_number_from_line_containing(text, keywords):
    """
    Busca una línea que contenga una palabra clave y regresa el último número.
    """
    lines = text.splitlines()

    for line in lines:
        line_clean = normalize_spaces(line)

        for kw in keywords:
            if re.search(kw, line_clean, flags=re.IGNORECASE):
                nums = numbers_in_line(line_clean)
                if nums:
                    return clean_number(nums[-1])

    return None


def extract_first_number_after_label(text, labels, max_chars=140):
    """
    Busca el primer número después de una etiqueta.
    """
    txt = re.sub(r"\s+", " ", text)

    for label in labels:
        pattern = rf"{label}(.{{0,{max_chars}}}?)([-+]?\$?\s?\d[\d,]*\.?\d*)"
        match = re.search(pattern, txt, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return clean_number(match.group(2))

    return None


def extract_number_after_pattern(text, pattern):
    """
    Extrae el primer número después de un patrón específico.
    """
    if not text:
        return None

    match = re.search(
        pattern + r"\s*([-+]?\$?\s?\d[\d,]*\.?\d*)",
        text,
        flags=re.IGNORECASE,
    )

    if match:
        return clean_number(match.group(1))

    return None


def compute_power_factor_pct(kwh, kvarh):
    """
    Calcula factor de potencia a partir de energía activa y reactiva:
        FP = kWh / sqrt(kWh^2 + kVArh^2) * 100
    """
    if kwh is None or kvarh is None:
        return None

    try:
        kwh = float(kwh)
        kvarh = float(kvarh)

        if kwh <= 0:
            return None

        return round(100 * kwh / math.sqrt(kwh**2 + kvarh**2), 2)
    except Exception:
        return None


def extract_gdm_fields_from_hourly_block(block):
    """
    Extrae campos GDMTH directamente desde el bloque horario bruto.

    Espera bloques tipo:
        base 56,217 kWh intermedia 223,915 kWh punta 23,537
        kW base 1,209 kW intermedia 1,199 kW punta 717
        KWMax 1,209 kVArh 99,802 Factor de potencia 94.98
    """
    result = {
        "kwh_base": None,
        "kwh_intermedia": None,
        "kwh_punta": None,
        "kw_base": None,
        "kw_intermedia": None,
        "kw_punta": None,
        "kwmax": None,
        "kvarh": None,
        "factor_potencia_pct": None,
        "kwh_total_horario": None,
        "kwh_horario_check": None,
    }

    if not block:
        result["kwh_horario_check"] = "no_block"
        return result

    txt = re.sub(r"\s+", " ", block)

    # Energía horaria
    m_energy = re.search(
        r"\bbase\s+([-+]?\d[\d,]*\.?\d*)\s*kWh\s+"
        r"intermedia\s+([-+]?\d[\d,]*\.?\d*)\s*kWh\s+"
        r"punta\s+([-+]?\d[\d,]*\.?\d*)",
        txt,
        flags=re.IGNORECASE,
    )

    if m_energy:
        result["kwh_base"] = clean_number(m_energy.group(1))
        result["kwh_intermedia"] = clean_number(m_energy.group(2))
        result["kwh_punta"] = clean_number(m_energy.group(3))
    else:
        result["kwh_base"] = extract_number_after_pattern(txt, r"\bbase\b")
        result["kwh_intermedia"] = extract_number_after_pattern(txt, r"\bintermedia\b")
        result["kwh_punta"] = extract_number_after_pattern(txt, r"\bpunta\b")

    # Demandas horarias
    m_kw = re.search(
        r"\bkW\s+base\s+([-+]?\d[\d,]*\.?\d*)\s+"
        r"kW\s+intermedia\s+([-+]?\d[\d,]*\.?\d*)\s+"
        r"kW\s+punta\s+([-+]?\d[\d,]*\.?\d*)",
        txt,
        flags=re.IGNORECASE,
    )

    if m_kw:
        result["kw_base"] = clean_number(m_kw.group(1))
        result["kw_intermedia"] = clean_number(m_kw.group(2))
        result["kw_punta"] = clean_number(m_kw.group(3))
    else:
        result["kw_base"] = extract_number_after_pattern(txt, r"\bkW\s+base\b")
        result["kw_intermedia"] = extract_number_after_pattern(txt, r"\bkW\s+intermedia\b")
        result["kw_punta"] = extract_number_after_pattern(txt, r"\bkW\s+punta\b")

    result["kwmax"] = extract_number_after_pattern(
        txt,
        r"\bKWMax\b|\bkW\s*Max\b|\bDemanda\s+M[aá]xima\b",
    )

    result["kvarh"] = extract_number_after_pattern(
        txt,
        r"\bkVArh\b|\bkVARh\b",
    )

    # En GDMTH normalmente sí aparece explícito como porcentaje después del bloque.
    result["factor_potencia_pct"] = extract_number_after_pattern(
        txt,
        r"Factor\s+de\s+potencia",
    )

    # Si no viene explícito, calcular desde kWh total y kVArh.
    kwh_values = [
        result["kwh_base"],
        result["kwh_intermedia"],
        result["kwh_punta"],
    ]

    if all(v is not None for v in kwh_values):
        result["kwh_total_horario"] = sum(kwh_values)
        result["kwh_horario_check"] = "ok"
    elif any(v is not None for v in kwh_values):
        result["kwh_horario_check"] = "partial"
    else:
        result["kwh_horario_check"] = "not_found"

    if result["factor_potencia_pct"] is None:
        result["factor_potencia_pct"] = compute_power_factor_pct(
            result["kwh_total_horario"],
            result["kvarh"],
        )

    return result


def extract_block_around_hourly_terms(text):
    """
    Guarda un bloque bruto alrededor de términos horarios para auditoría.
    """
    txt = re.sub(r"\s+", " ", text)

    patterns = [
        r"(base.{0,600}?kWh.{0,600}?intermedia.{0,600}?kWh.{0,600}?punta.{0,1200}?(Factor\s+de\s+Potencia|Total|Importe|Subtotal))",
        r"(Base.{0,1200}?Intermedia.{0,1200}?Punta.{0,1200}?(Factor\s+de\s+Potencia|Total|Importe|Subtotal))",
        r"(Energ[ií]a\s+Base.{0,1800}?(Factor\s+de\s+Potencia|Total|Importe|Subtotal))",
        r"(Demanda\s+Base.{0,1800}?(Factor\s+de\s+Potencia|Total|Importe|Subtotal))",
        r"(GDMTH.{0,2200}?(Factor\s+de\s+Potencia|Total|Importe|Subtotal))",
        r"(GDMTO.{0,2200}?(Factor\s+de\s+Potencia|Total|Importe|Subtotal))",
    ]

    for pattern in patterns:
        match = re.search(pattern, txt, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).strip()

    return None


def extract_gdm_fields(text):
    """
    Extrae campos para tarifa GDMTH.
    """
    result = {
        "kwh_base": None,
        "kwh_intermedia": None,
        "kwh_punta": None,
        "kw_base": None,
        "kw_intermedia": None,
        "kw_punta": None,
        "kwmax": None,
        "kvarh": None,
        "factor_potencia_pct": None,
        "bonificacion_factor_potencia": None,
        "penalizacion_factor_potencia": None,
        "tarifa_horaria_detectada": False,
        "bloque_tarifa_horaria_raw": None,
        "kwh_total_horario": None,
        "kwh_horario_check": None,
    }

    txt = text or ""
    lines = [normalize_spaces(ln) for ln in txt.splitlines()]
    lines = [ln for ln in lines if ln]

    if re.search(r"\bGDMTH\b|Base|Intermedia|Punta", txt, flags=re.IGNORECASE):
        result["tarifa_horaria_detectada"] = True

    # Extracción inicial por líneas/etiquetas.
    result["kwh_base"] = extract_last_number_from_line_containing(
        txt,
        [
            r"Energ[ií]a\s+Base",
            r"\bBase\b",
        ],
    )

    result["kwh_intermedia"] = extract_last_number_from_line_containing(
        txt,
        [
            r"Energ[ií]a\s+Intermedia",
            r"\bIntermedia\b",
        ],
    )

    result["kwh_punta"] = extract_last_number_from_line_containing(
        txt,
        [
            r"Energ[ií]a\s+Punta",
            r"\bPunta\b",
        ],
    )

    if result["kwh_base"] is None:
        result["kwh_base"] = extract_first_number_after_label(
            txt,
            [
                r"Energ[ií]a\s+Base",
                r"kWh\s+Base",
                r"Base\s+kWh",
            ],
        )

    if result["kwh_intermedia"] is None:
        result["kwh_intermedia"] = extract_first_number_after_label(
            txt,
            [
                r"Energ[ií]a\s+Intermedia",
                r"kWh\s+Intermedia",
                r"Intermedia\s+kWh",
            ],
        )

    if result["kwh_punta"] is None:
        result["kwh_punta"] = extract_first_number_after_label(
            txt,
            [
                r"Energ[ií]a\s+Punta",
                r"kWh\s+Punta",
                r"Punta\s+kWh",
            ],
        )

    result["kw_base"] = extract_first_number_after_label(
        txt,
        [
            r"Demanda\s+Base",
            r"kW\s+Base",
            r"Base\s+kW",
        ],
    )

    result["kw_intermedia"] = extract_first_number_after_label(
        txt,
        [
            r"Demanda\s+Intermedia",
            r"kW\s+Intermedia",
            r"Intermedia\s+kW",
        ],
    )

    result["kw_punta"] = extract_first_number_after_label(
        txt,
        [
            r"Demanda\s+Punta",
            r"kW\s+Punta",
            r"Punta\s+kW",
        ],
    )

    result["kwmax"] = extract_first_number_after_label(
        txt,
        [
            r"Demanda\s+M[aá]xima",
            r"Demanda\s+Maxima",
            r"KW\s*Max",
            r"kWmax",
            r"Max[ií]metro",
            r"Demanda\s+Facturable",
        ],
    )

    result["kvarh"] = extract_first_number_after_label(
        txt,
        [
            r"Energ[ií]a\s+Reactiva",
            r"kVArh",
            r"kVARh",
        ],
    )

    result["factor_potencia_pct"] = extract_first_number_after_label(
        txt,
        [
            r"Factor\s+de\s+Potencia",
            r"F\.?P\.?",
            r"\bFP\b",
        ],
    )

    # Para bonificación/cargo usamos último número de la línea
    # para evitar capturar la nota "(3)".
    result["bonificacion_factor_potencia"] = last_number_from_matching_lines(
        lines,
        [
            r"Bonificaci[oó]n\s+Factor\s+de\s+Potencia",
            r"Bonificaci[oó]n\s+por\s+Factor\s+de\s+Potencia",
            r"Bonificaci[oó]n\s+FP",
            r"Bonificaci[oó]n",
        ],
    )

    result["penalizacion_factor_potencia"] = last_number_from_matching_lines(
        lines,
        [
            r"Penalizaci[oó]n\s+Factor\s+de\s+Potencia",
            r"Cargo\s+Factor\s+de\s+Potencia",
            r"Penalizaci[oó]n\s+FP",
            r"Penalizaci[oó]n",
        ],
    )

    # Fuente prioritaria: bloque horario bruto.
    result["bloque_tarifa_horaria_raw"] = extract_block_around_hourly_terms(txt)

    block_fields = extract_gdm_fields_from_hourly_block(
        result["bloque_tarifa_horaria_raw"]
    )

    for key, value in block_fields.items():
        if value is not None:
            result[key] = value

    return result


def extract_gdmto_fields(text):
    """
    Extrae campos para tarifa GDMTO.

    GDMTO no usa base/intermedia/punta como GDMTH.
    Normalmente aparece una tabla con filas:

        kWh    medidor    lectura actual    lectura anterior    diferencia    total
        kW     medidor    lectura actual    lectura anterior    diferencia    total
        kVArh  medidor    lectura actual    lectura anterior    diferencia    total

    Esta función toma el último número de cada fila como total.
    """
    result = {
        "kwh_base": None,
        "kwh_intermedia": None,
        "kwh_punta": None,
        "kwh_total_horario": None,
        "kwh_horario_check": None,

        "kw_base": None,
        "kw_intermedia": None,
        "kw_punta": None,
        "kwmax": None,

        "kvarh": None,
        "factor_potencia_pct": None,

        "bonificacion_factor_potencia": None,
        "penalizacion_factor_potencia": None,

        "tarifa_horaria_detectada": False,
        "bloque_tarifa_horaria_raw": None,
    }

    txt = text or ""

    if re.search(r"\bGDMTO\b", txt, flags=re.IGNORECASE):
        result["tarifa_horaria_detectada"] = True

    lines = [normalize_spaces(ln) for ln in txt.splitlines()]
    lines = [ln for ln in lines if ln]

    # ------------------------------------------------------------
    # Buscar filas principales de la tabla:
    # kWh, kW, kVArh
    # ------------------------------------------------------------
    kwh_line = None
    kw_line = None
    kvarh_line = None

    for line in lines:
        # Evitar encabezados
        if re.search(
            r"Concepto|Lectura actual|Lectura anterior|Diferencia|Totales",
            line,
            flags=re.IGNORECASE,
        ):
            continue

        if re.match(r"^kWh\b", line, flags=re.IGNORECASE):
            kwh_line = line

        elif re.match(r"^kW\b", line, flags=re.IGNORECASE):
            # Importante: que no capture kWh ni kVArh
            if not re.match(r"^kWh\b", line, flags=re.IGNORECASE):
                kw_line = line

        elif re.match(r"^kVArh\b", line, flags=re.IGNORECASE):
            kvarh_line = line

    if kwh_line:
        nums = numbers_in_line(kwh_line)
        if nums:
            result["kwh_total_horario"] = clean_number(nums[-1])
            result["kwh_horario_check"] = "ok_gdmto"

    if kw_line:
        nums = numbers_in_line(kw_line)
        if nums:
            result["kwmax"] = clean_number(nums[-1])

    if kvarh_line:
        nums = numbers_in_line(kvarh_line)
        if nums:
            result["kvarh"] = clean_number(nums[-1])

    # ------------------------------------------------------------
    # Si no encontró por líneas, intentar patrón compacto.
    # A veces pdf extraction junta fragmentos en una sola línea.
    # ------------------------------------------------------------
    flat = re.sub(r"\s+", " ", txt)

    if result["kwh_total_horario"] is None:
        m = re.search(
            r"\bkWh\b\s+\S+\s+"
            r"([-+]?\d[\d,]*\.?\d*)\s+"
            r"([-+]?\d[\d,]*\.?\d*)\s+"
            r"([-+]?\d[\d,]*\.?\d*)\s+"
            r"([-+]?\d[\d,]*\.?\d*)",
            flat,
            flags=re.IGNORECASE,
        )
        if m:
            result["kwh_total_horario"] = clean_number(m.group(4))
            result["kwh_horario_check"] = "ok_gdmto_flat"

    if result["kwmax"] is None:
        m = re.search(
            r"\bkW\b\s+\S+\s+"
            r"([-+]?\d[\d,]*\.?\d*)\s+"
            r"([-+]?\d[\d,]*\.?\d*)\s+"
            r"([-+]?\d[\d,]*\.?\d*)\s+"
            r"([-+]?\d[\d,]*\.?\d*)",
            flat,
            flags=re.IGNORECASE,
        )
        if m:
            result["kwmax"] = clean_number(m.group(4))

    if result["kvarh"] is None:
        m = re.search(
            r"\bkVArh\b\s+\S+\s+"
            r"([-+]?\d[\d,]*\.?\d*)\s+"
            r"([-+]?\d[\d,]*\.?\d*)\s+"
            r"([-+]?\d[\d,]*\.?\d*)\s+"
            r"([-+]?\d[\d,]*\.?\d*)",
            flat,
            flags=re.IGNORECASE,
        )
        if m:
            result["kvarh"] = clean_number(m.group(4))

    # ------------------------------------------------------------
    # Factor de potencia
    #
    # Para GDMTO lo calculamos desde kWh y kVArh.
    # Esto evita confundir el monto de bonificación/cargo con FP.
    # ------------------------------------------------------------
    result["factor_potencia_pct"] = compute_power_factor_pct(
        result["kwh_total_horario"],
        result["kvarh"],
    )

    # ------------------------------------------------------------
    # Bonificación / penalización por factor de potencia
    #
    # Tomamos el último número de la línea, no el primero.
    # Así evitamos capturar "(3)".
    # ------------------------------------------------------------
    result["bonificacion_factor_potencia"] = last_number_from_matching_lines(
        lines,
        [
            r"Bonificaci[oó]n\s+Factor\s+de\s+Potencia",
            r"Bonificaci[oó]n\s+por\s+Factor\s+de\s+Potencia",
            r"Bonificaci[oó]n\s+FP",
            r"Bonificaci[oó]n",
        ],
    )

    result["penalizacion_factor_potencia"] = last_number_from_matching_lines(
        lines,
        [
            r"Penalizaci[oó]n\s+Factor\s+de\s+Potencia",
            r"Cargo\s+Factor\s+de\s+Potencia",
            r"Penalizaci[oó]n\s+FP",
            r"Penalizaci[oó]n",
        ],
    )

    # ------------------------------------------------------------
    # Bloque bruto para auditoría
    # ------------------------------------------------------------
    block_lines = []

    for line in lines:
        if re.match(r"^kWh\b", line, flags=re.IGNORECASE):
            block_lines.append(line)
        elif re.match(r"^kW\b", line, flags=re.IGNORECASE) and not re.match(
            r"^kWh\b",
            line,
            flags=re.IGNORECASE,
        ):
            block_lines.append(line)
        elif re.match(r"^kVArh\b", line, flags=re.IGNORECASE):
            block_lines.append(line)
        elif re.search(r"Factor\s+de\s+potencia", line, flags=re.IGNORECASE):
            block_lines.append(line)
        elif re.search(r"Bonificaci[oó]n\s+Factor\s+de\s+Potencia", line, flags=re.IGNORECASE):
            block_lines.append(line)
        elif re.search(r"Cargo\s+Factor\s+de\s+Potencia", line, flags=re.IGNORECASE):
            block_lines.append(line)

    if block_lines:
        result["bloque_tarifa_horaria_raw"] = " | ".join(block_lines)

    if result["kwh_horario_check"] is None:
        if result["kwh_total_horario"] is not None:
            result["kwh_horario_check"] = "ok_gdmto"
        else:
            result["kwh_horario_check"] = "not_found_gdmto"

    return result


def extract_gdbt_fields(text):
    """
    Extrae campos relevantes para tarifa GDBT.

    GDBT no usa el bloque horario Base/Intermedia/Punta de GDMTH.
    Esta función se enfoca en recuperar:
      - kWh total
      - kW / demanda máxima
      - kVArh
      - factor de potencia
      - carga conectada y demanda contratada
      - importes principales
      - bonificación / penalización por factor de potencia
      - bloque crudo para auditoría

    La estrategia es robusta ante texto PDF colapsado:
      - primero intenta por líneas
      - después por patrones sobre texto normalizado
    """
    result = {
        "kwh_total": None,
        "kwmax": None,
        "kvarh": None,
        "factor_potencia_pct": None,

        "carga_conectada_kw": None,
        "demanda_contratada_kw": None,

        "bonificacion_factor_potencia": None,
        "penalizacion_factor_potencia": None,

        "subtotal_energia": None,
        "subtotal": None,
        "iva": None,
        "total_linea": None,

        "tarifa_horaria_detectada": False,
        "bloque_tarifa_gdbt_raw": None,
    }

    raw = text or ""
    txt = normalize_spaces(raw)

    if re.search(r"\bGDBT\b", txt, flags=re.IGNORECASE):
        result["tarifa_horaria_detectada"] = True

    lines = [normalize_spaces(ln) for ln in raw.splitlines()]
    lines = [ln for ln in lines if ln]

    # ------------------------------------------------------------
    # Bloque crudo para auditoría
    # ------------------------------------------------------------
    block_lines = []

    for line in lines:
        if re.match(r"^kWh\b", line, flags=re.IGNORECASE):
            block_lines.append(line)
        elif re.match(r"^kW\b", line, flags=re.IGNORECASE) and not re.match(
            r"^kWh\b", line, flags=re.IGNORECASE
        ):
            block_lines.append(line)
        elif re.match(r"^kVArh\b", line, flags=re.IGNORECASE):
            block_lines.append(line)
        elif re.search(r"Factor\s+de\s+potencia", line, flags=re.IGNORECASE):
            block_lines.append(line)
        elif re.search(r"Carga\s+conectada", line, flags=re.IGNORECASE):
            block_lines.append(line)
        elif re.search(r"Demanda\s+contratada", line, flags=re.IGNORECASE):
            block_lines.append(line)
        elif re.search(r"Bonificaci[oó]n\s+Factor\s+de\s+Potencia", line, flags=re.IGNORECASE):
            block_lines.append(line)
        elif re.search(r"Cargo\s+Factor\s+de\s+Potencia", line, flags=re.IGNORECASE):
            block_lines.append(line)
        elif re.search(r"Total\s+a\s+pagar|Subtotal|IVA", line, flags=re.IGNORECASE):
            block_lines.append(line)

    if block_lines:
        result["bloque_tarifa_gdbt_raw"] = " | ".join(block_lines)
    else:
        mblock = re.search(
            r"(GDBT.{0,2500}?(Total\s+a\s+pagar|Importe|Subtotal|IVA|Facturaci[oó]n))",
            txt,
            flags=re.IGNORECASE,
        )
        if not mblock:
            mblock = re.search(
                r"((kWh|kW|kVArh|Factor\s+de\s+Potencia).{0,2500}?"
                r"(Total\s+a\s+pagar|Importe|Subtotal|IVA))",
                txt,
                flags=re.IGNORECASE,
            )
        if mblock:
            result["bloque_tarifa_gdbt_raw"] = mblock.group(1)

    # ------------------------------------------------------------
    # Carga conectada y demanda contratada
    # ------------------------------------------------------------
    contract_patterns = {
        "carga_conectada_kw": [
            r"Carga\s+conectada\s*:?\s*([0-9,]+(?:\.[0-9]+)?)\s*kW",
            r"Carga\s+conectada\s*\(kW\)\s*:?\s*([0-9,]+(?:\.[0-9]+)?)",
            r"Carga\s+Conectada\s+([0-9,]+(?:\.[0-9]+)?)",
        ],
        "demanda_contratada_kw": [
            r"Demanda\s+contratada\s*:?\s*([0-9,]+(?:\.[0-9]+)?)\s*kW",
            r"Demanda\s+contratada\s*\(kW\)\s*:?\s*([0-9,]+(?:\.[0-9]+)?)",
            r"Demanda\s+Contratada\s+([0-9,]+(?:\.[0-9]+)?)",
        ],
    }

    for field, patterns in contract_patterns.items():
        for pat in patterns:
            m = re.search(pat, txt, flags=re.IGNORECASE)
            if m:
                result[field] = clean_number(m.group(1))
                break

    # ------------------------------------------------------------
    # Filas kWh / kW / kVArh
    # ------------------------------------------------------------
    kwh_line = None
    kw_line = None
    kvarh_line = None

    for line in lines:
        if kwh_line is None and re.match(r"^kWh\b", line, flags=re.IGNORECASE):
            kwh_line = line
        elif kw_line is None and re.match(r"^kW\b", line, flags=re.IGNORECASE) and not re.match(
            r"^kWh\b", line, flags=re.IGNORECASE
        ):
            kw_line = line
        elif kvarh_line is None and re.match(r"^kVArh\b", line, flags=re.IGNORECASE):
            kvarh_line = line

    if kwh_line:
        nums = re.findall(r"-?\$?\s*[0-9][0-9,]*(?:\.[0-9]+)?", kwh_line)
        nums = [clean_number(n) for n in nums]
        nums = [n for n in nums if n is not None]
        if nums:
            # En los recibos GDBT observados, la fila puede traer:
            # lectura actual, lectura anterior, diferencia, total.
            # El último número suele ser el total útil; si solo hay uno, usar ese.
            result["kwh_total"] = nums[-1]

    if kw_line:
        nums = re.findall(r"-?\$?\s*[0-9][0-9,]*(?:\.[0-9]+)?", kw_line)
        nums = [clean_number(n) for n in nums]
        nums = [n for n in nums if n is not None]
        if nums:
            result["kwmax"] = nums[-1]

    if kvarh_line:
        nums = re.findall(r"-?\$?\s*[0-9][0-9,]*(?:\.[0-9]+)?", kvarh_line)
        nums = [clean_number(n) for n in nums]
        nums = [n for n in nums if n is not None]
        if nums:
            result["kvarh"] = nums[-1]

    # Fallbacks si no se encontraron por línea.
    if result["kwh_total"] is None:
        patterns = [
            r"\bkWh\b\s+[A-Z0-9]+\s+(?:[0-9,]+(?:\.[0-9]+)?\s+){0,4}([0-9,]+(?:\.[0-9]+)?)",
            r"\bkWh\b\s+([0-9,]+(?:\.[0-9]+)?)",
            r"Consumo\s+(?:de\s+)?energ[ií]a\s*:?\s*([0-9,]+(?:\.[0-9]+)?)",
            r"Energ[ií]a\s+\(kWh\)\s*:?\s*([0-9,]+(?:\.[0-9]+)?)",
            r"Total\s+kWh\s*:?\s*([0-9,]+(?:\.[0-9]+)?)",
        ]
        for pat in patterns:
            m = re.search(pat, txt, flags=re.IGNORECASE)
            if m:
                result["kwh_total"] = clean_number(m.group(1))
                break

    if result["kwmax"] is None:
        patterns = [
            r"\bkW\b\s+[A-Z0-9]+\s+(?:[0-9,]+(?:\.[0-9]+)?\s+){0,4}([0-9,]+(?:\.[0-9]+)?)",
            r"Demanda\s+m[aá]xima\s*:?\s*([0-9,]+(?:\.[0-9]+)?)",
            r"Demanda\s+\(kW\)\s*:?\s*([0-9,]+(?:\.[0-9]+)?)",
        ]
        for pat in patterns:
            m = re.search(pat, txt, flags=re.IGNORECASE)
            if m:
                result["kwmax"] = clean_number(m.group(1))
                break

    if result["kvarh"] is None:
        patterns = [
            r"\bkVArh\b\s+[A-Z0-9]+\s+(?:[0-9,]+(?:\.[0-9]+)?\s+){0,4}([0-9,]+(?:\.[0-9]+)?)",
            r"Energ[ií]a\s+reactiva\s*:?\s*([0-9,]+(?:\.[0-9]+)?)",
        ]
        for pat in patterns:
            m = re.search(pat, txt, flags=re.IGNORECASE)
            if m:
                result["kvarh"] = clean_number(m.group(1))
                break

    # ------------------------------------------------------------
    # Factor de potencia y FP
    # ------------------------------------------------------------
    fp = last_number_from_matching_lines(
        lines,
        [
            r"Factor\s+de\s+potencia",
            r"Factor\s+de\s+Potencia",
            r"\bFP\b",
            r"F\.?\s*P\.?",
        ],
    )
    if fp is not None:
        result["factor_potencia_pct"] = fp
    else:
        patterns = [
            r"Factor\s+de\s+potencia\s*:?\s*([0-9]+(?:\.[0-9]+)?)\s*%?",
            r"F\.?\s*P\.?\s*:?\s*([0-9]+(?:\.[0-9]+)?)\s*%?",
        ]
        for pat in patterns:
            m = re.search(pat, txt, flags=re.IGNORECASE)
            if m:
                result["factor_potencia_pct"] = clean_number(m.group(1))
                break

    result["bonificacion_factor_potencia"] = last_number_from_matching_lines(
        lines,
        [
            r"Bonificaci[oó]n\s+Factor\s+de\s+Potencia",
            r"Bonificaci[oó]n\s+FP",
            r"Bonificaci[oó]n",
        ],
    )
    if result["bonificacion_factor_potencia"] is not None:
        result["bonificacion_factor_potencia"] = -abs(result["bonificacion_factor_potencia"])

    result["penalizacion_factor_potencia"] = last_number_from_matching_lines(
        lines,
        [
            r"Penalizaci[oó]n\s+Factor\s+de\s+Potencia",
            r"Cargo\s+Factor\s+de\s+Potencia",
            r"Penalizaci[oó]n\s+FP",
            r"Penalizaci[oó]n",
        ],
    )

    # ------------------------------------------------------------
    # Importes principales
    # ------------------------------------------------------------
    result["iva"] = last_number_from_matching_lines(lines, [r"\bIVA\b"])

    result["subtotal"] = last_number_from_matching_lines(
        lines,
        [
            r"\bSubtotal\b",
        ],
    )

    result["subtotal_energia"] = last_number_from_matching_lines(
        lines,
        [
            r"Subtotal\s+energ[ií]a",
            r"Energ[ií]a\s+\$",
        ],
    )

    result["total_linea"] = last_number_from_matching_lines(
        lines,
        [
            r"Total\s+a\s+pagar",
            r"Total\s+del\s+periodo",
            r"Total\s+\$",
        ],
    )

    # Fallback sobre texto normalizado para total.
    if result["total_linea"] is None:
        for pat in [
            r"Total\s+a\s+pagar\s*:?\s*\$?\s*([0-9,]+(?:\.[0-9]+)?)",
            r"Total\s+del\s+periodo\s*:?\s*\$?\s*([0-9,]+(?:\.[0-9]+)?)",
        ]:
            m = re.search(pat, txt, flags=re.IGNORECASE)
            if m:
                result["total_linea"] = clean_number(m.group(1))
                break

    return result


# ============================================================
# Enriquecimiento principal
# ============================================================

ENRICHMENT_COLUMNS = [
    # Dirección
    "direccion_completa",
    "direccion_calle",
    "direccion_colonia",
    "direccion_municipio",
    "direccion_estado",
    "direccion_cp",
    "direccion_raw",
    "direccion_source",
    "direccion_confidence",

    # Tarifa horaria
    "kwh_base",
    "kwh_intermedia",
    "kwh_punta",
    "kwh_total_horario",
    "kwh_horario_check",
    "kw_base",
    "kw_intermedia",
    "kw_punta",
    "kwmax",
    "kvarh",
    "factor_potencia_pct",
    "bonificacion_factor_potencia",
    "penalizacion_factor_potencia",

    # Auditoría de extracción
    "tarifa_horaria_detectada",
    "bloque_tarifa_horaria_raw",
    "bloque_tarifa_gdbt_raw",

    # Control de enriquecimiento
    "enriched",
    "enrichment_source",
    "enrichment_tariff_mode",
    "enrichment_error",
    "enrichment_seconds",
]


def ensure_columns(df, columns):
    for col in columns:
        if col not in df.columns:
            df[col] = None
    return df


def should_process_row(row, tariffs=None, only_missing=True):
    """
    Decide si el renglón debe procesarse.

    Esta función NO modifica df. Solo regresa True/False.
    """
    tarifa = safe_str(row.get("tarifa")).upper()

    if tariffs:
        if not any(t.upper() in tarifa for t in tariffs):
            return False

    if only_missing:
        has_address = pd.notna(row.get("direccion_completa"))
        has_tariff_enrichment = (
            pd.notna(row.get("kwh_base"))
            or pd.notna(row.get("kwh_intermedia"))
            or pd.notna(row.get("kwh_punta"))
            or pd.notna(row.get("kwmax"))
            or pd.notna(row.get("kwh_total_horario"))
            or pd.notna(row.get("kwh_total"))
            or pd.notna(row.get("kvarh"))
            or pd.notna(row.get("factor_potencia_pct"))
        )

        if has_address and has_tariff_enrichment:
            return False

    return True


def enrich_dataframe(
    df,
    tariffs=None,
    limit=None,
    max_pages=None,
    only_missing=True,
    write_every=None,
    output_csv=None,
):
    df = ensure_columns(df, ENRICHMENT_COLUMNS)

    processed = 0
    attempted = 0
    selected = 0
    errors = 0
    skipped = 0

    for idx, row in df.iterrows():
        if not should_process_row(row, tariffs=tariffs, only_missing=only_missing):
            continue

        selected += 1
        attempted += 1

        if limit is not None and attempted > limit:
            break

        start = time.time()

        try:
            document_type = safe_str(row.get("document_type")).lower()

            # Por ahora enriquecemos localmente solo recibos CFE.
            # Los formatos Iberdrola/CFDI conviene tratarlos aparte o mandarlos a LlamaExtract.
            if document_type and document_type != "cfe_bill":
                df.at[idx, "enriched"] = False
                df.at[idx, "enrichment_source"] = "local"
                df.at[idx, "enrichment_error"] = f"Skipped document_type={document_type}"
                df.at[idx, "enrichment_seconds"] = round(time.time() - start, 3)
                skipped += 1
                print(f"[SKIP] idx={idx} | document_type={document_type}")
                continue

            pdf_path = get_pdf_path_from_row(row)

            if not pdf_path or not Path(str(pdf_path)).exists():
                df.at[idx, "enriched"] = False
                df.at[idx, "enrichment_source"] = "local"
                df.at[idx, "enrichment_error"] = "PDF path missing or not found"
                df.at[idx, "enrichment_seconds"] = round(time.time() - start, 3)
                errors += 1
                print(f"[ERROR] idx={idx} | PDF path missing or not found")
                continue

            text = extract_text_from_pdf(pdf_path, max_pages=max_pages)

            if not text.strip():
                df.at[idx, "enriched"] = False
                df.at[idx, "enrichment_source"] = "local"
                df.at[idx, "enrichment_error"] = "No text extracted from PDF"
                df.at[idx, "enrichment_seconds"] = round(time.time() - start, 3)
                errors += 1
                print(f"[ERROR] idx={idx} | No text extracted | {Path(str(pdf_path)).name}")
                continue

            tarifa = safe_str(row.get("tarifa")).upper()

            address_fields = extract_business_address(
                text,
                cliente_nombre=row.get("cliente_nombre"),
            )

            for key, value in address_fields.items():
                df.at[idx, key] = value

            if "GDMTH" in tarifa:
                tariff_fields = extract_gdm_fields(text)

                for key, value in tariff_fields.items():
                    df.at[idx, key] = value

                df.at[idx, "enrichment_tariff_mode"] = "GDMTH"

            elif "GDMTO" in tarifa:
                tariff_fields = extract_gdmto_fields(text)

                for key, value in tariff_fields.items():
                    df.at[idx, key] = value

                df.at[idx, "enrichment_tariff_mode"] = "GDMTO"

            elif "GDBT" in tarifa:
                tariff_fields = extract_gdbt_fields(text)

                for key, value in tariff_fields.items():
                    df.at[idx, key] = value

                df.at[idx, "enrichment_tariff_mode"] = "GDBT"

            else:
                df.at[idx, "enrichment_tariff_mode"] = tarifa

            df.at[idx, "enriched"] = True
            df.at[idx, "enrichment_source"] = "local"
            df.at[idx, "enrichment_error"] = None
            df.at[idx, "enrichment_seconds"] = round(time.time() - start, 3)

            processed += 1

            print(
                f"[OK] {processed} | idx={idx} | tarifa={tarifa} | "
                f"{Path(str(pdf_path)).name}"
            )

            if write_every and output_csv and processed % write_every == 0:
                df.to_csv(output_csv, index=False)
                print(f"[SAVE] Parcial guardado en {output_csv}")

        except Exception as e:
            df.at[idx, "enriched"] = False
            df.at[idx, "enrichment_source"] = "local"
            df.at[idx, "enrichment_error"] = str(e)
            df.at[idx, "enrichment_seconds"] = round(time.time() - start, 3)
            errors += 1

            print(f"[ERROR] idx={idx} | {e}")

    print()
    print("Resumen enriquecimiento local")
    print("-----------------------------")
    print(f"Seleccionados: {selected}")
    print(f"Intentados:    {attempted if limit is None else min(attempted, limit)}")
    print(f"Procesados:    {processed}")
    print(f"Saltados:      {skipped}")
    print(f"Errores:       {errors}")

    return df


# ============================================================
# CLI
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Segunda pasada local para enriquecer recibos CFE por tarifa."
    )

    parser.add_argument(
        "--input",
        required=True,
        help="CSV base, por ejemplo output/bills_parsed_v2.csv",
    )

    parser.add_argument(
        "--output",
        required=True,
        help="CSV enriquecido, por ejemplo output/bills_parsed_v3_enriched.csv",
    )

    parser.add_argument(
        "--tariffs",
        default="GDMTH,GDMTO,GDBT",
        help="Tarifas a enriquecer separadas por coma. Default: GDMTH,GDMTO,GDBT",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Límite de recibos a intentar procesar para prueba.",
    )

    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Máximo de páginas PDF a leer. Para pruebas puede usarse 1 o 2.",
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Reprocesar aunque ya existan campos enriquecidos.",
    )

    parser.add_argument(
        "--write-every",
        type=int,
        default=25,
        help="Guardar avance cada N recibos procesados correctamente. Default: 25.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    input_csv = Path(args.input)
    output_csv = Path(args.output)

    if not input_csv.exists():
        raise FileNotFoundError(f"No existe el archivo de entrada: {input_csv}")

    output_csv.parent.mkdir(parents=True, exist_ok=True)

    tariffs = [
        t.strip()
        for t in args.tariffs.split(",")
        if t.strip()
    ]

    print("Enriquecimiento local de recibos CFE")
    print("------------------------------------")
    print(f"Input:        {input_csv}")
    print(f"Output:       {output_csv}")
    print(f"Tarifas:      {tariffs}")
    print(f"Limit:        {args.limit}")
    print(f"Max pages:    {args.max_pages}")
    print(f"Only missing: {not args.all}")
    print()

    df = pd.read_csv(input_csv)

    df_enriched = enrich_dataframe(
        df,
        tariffs=tariffs,
        limit=args.limit,
        max_pages=args.max_pages,
        only_missing=not args.all,
        write_every=args.write_every,
        output_csv=output_csv,
    )

    df_enriched.to_csv(output_csv, index=False)

    print()
    print("[DONE] Archivo enriquecido guardado en:")
    print(output_csv)


if __name__ == "__main__":
    main()