#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
repair_tariff.py

Reparaciones controladas por tarifa y topic.

Primer caso implementado:
  --topic power_factor --tariff GDBT

Corrige factor_potencia_pct en GDBT cuando el extractor puso ahí
el importe de bonificación/penalización en vez del valor porcentual.

Para GDBT, el factor de potencia se recalcula desde:
  kwh_total
  kvarh

Fórmula:
  fp_pct = 100 * kWh / sqrt(kWh^2 + kVArh^2)

Uso:
    python tools/repair_tariff.py \
      --topic power_factor \
      --tariff GDBT \
      --input output/tarifa_GDBT.csv \
      --output output/tarifa_GDBT_fixed_fp.csv \
      --audit output/tarifa_GDBT_fixed_fp_audit.csv
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path
import sys

import pandas as pd


TOPIC_ALLOWED_TARIFFS = {
    "power_factor": {"GDBT"},
}


def normalize_tariff(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def to_number_value(value: object):
    if pd.isna(value):
        return pd.NA

    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "null", "<na>"}:
        return pd.NA

    s = (
        s.replace("$", "")
        .replace(",", "")
        .replace("%", "")
        .replace(" ", "")
    )

    try:
        return float(s)
    except Exception:
        return pd.NA


def to_number(series: pd.Series) -> pd.Series:
    return series.map(to_number_value)


def ensure_allowed(topic: str, tariff: str) -> None:
    topic = topic.strip().lower()
    tariff = tariff.strip().upper()

    if topic not in TOPIC_ALLOWED_TARIFFS:
        valid = ", ".join(sorted(TOPIC_ALLOWED_TARIFFS))
        raise ValueError(f"Topic desconocido: {topic}. Topics válidos: {valid}")

    allowed = TOPIC_ALLOWED_TARIFFS[topic]
    if tariff not in allowed:
        allowed_txt = ", ".join(sorted(allowed))
        raise ValueError(
            f"El repair '{topic}' no aplica para tarifa {tariff}. "
            f"Tarifas válidas: {allowed_txt}."
        )


def require_columns(df: pd.DataFrame, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(
            "Faltan columnas obligatorias: "
            + ", ".join(missing)
            + "\nColumnas disponibles:\n"
            + ", ".join(df.columns)
        )


def calc_fp_pct(kwh, kvarh):
    if pd.isna(kwh) or pd.isna(kvarh):
        return pd.NA

    kwh = float(kwh)
    kvarh = float(kvarh)

    if kwh <= 0:
        return pd.NA

    denom = math.sqrt(kwh * kwh + kvarh * kvarh)
    if denom <= 0:
        return pd.NA

    return round(100.0 * kwh / denom, 2)


def repair_gdbt_power_factor(
    df: pd.DataFrame,
    output: Path,
    audit: Path | None = None,
    tariff_column: str = "tarifa",
    filter_tariff: bool = False,
) -> None:
    required = ["kwh_total", "kvarh", "factor_potencia_pct"]
    require_columns(df, required)

    out = df.copy()

    if filter_tariff:
        if tariff_column not in out.columns:
            raise KeyError(f"No existe columna de tarifa: {tariff_column}")
        mask = out[tariff_column].map(normalize_tariff).eq("GDBT")
    else:
        mask = pd.Series(True, index=out.index)

    out["factor_potencia_pct_original"] = out["factor_potencia_pct"]

    kwh_num = to_number(out["kwh_total"])
    kvarh_num = to_number(out["kvarh"])
    fp_old_num = to_number(out["factor_potencia_pct"])

    fp_calc = [
        calc_fp_pct(kwh, kvarh)
        for kwh, kvarh in zip(kwh_num, kvarh_num)
    ]
    fp_calc = pd.Series(fp_calc, index=out.index, dtype="Float64")

    # Se corrige cuando pertenece a la tarifa y existe cálculo posible.
    can_fix = mask & fp_calc.notna()

    out.loc[can_fix, "factor_potencia_pct"] = fp_calc.loc[can_fix]
    out.loc[can_fix, "factor_potencia_pct_source"] = "calculated_from_kwh_kvarh"
    out.loc[~can_fix, "factor_potencia_pct_source"] = "not_calculated"

    # Auditoría
    audit_df = pd.DataFrame({
        "row_index": out.index,
        "file_name": out["file_name"] if "file_name" in out.columns else pd.NA,
        "mall_folder": out["mall_folder"] if "mall_folder" in out.columns else pd.NA,
        "cliente_nombre": out["cliente_nombre"] if "cliente_nombre" in out.columns else pd.NA,
        "no_servicio": out["no_servicio"] if "no_servicio" in out.columns else pd.NA,
        "tarifa": out[tariff_column] if tariff_column in out.columns else pd.NA,
        "kwh_total": out["kwh_total"],
        "kvarh": out["kvarh"],
        "factor_potencia_pct_original": out["factor_potencia_pct_original"],
        "factor_potencia_pct_original_num": fp_old_num,
        "factor_potencia_pct_calculado": fp_calc,
        "factor_potencia_pct_final": out["factor_potencia_pct"],
        "fixed": can_fix,
    })

    audit_df["old_out_of_range"] = (
        audit_df["factor_potencia_pct_original_num"].notna()
        & (
            (audit_df["factor_potencia_pct_original_num"] < 0)
            | (audit_df["factor_potencia_pct_original_num"] > 100)
        )
    )

    audit_df["delta_old_vs_calc"] = (
        audit_df["factor_potencia_pct_original_num"]
        - audit_df["factor_potencia_pct_calculado"]
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output, index=False, encoding="utf-8-sig")

    if audit is not None:
        audit.parent.mkdir(parents=True, exist_ok=True)
        audit_df.to_csv(audit, index=False, encoding="utf-8-sig")

    total = len(out)
    fixed = int(can_fix.sum())
    old_bad = int(audit_df["old_out_of_range"].sum())
    still_missing = int(out["factor_potencia_pct"].isna().sum())

    print("Repair por tarifa")
    print("-----------------")
    print("Topic:       power_factor")
    print("Tarifa:      GDBT")
    print(f"Filas:       {total:,}")
    print(f"Corregidas:  {fixed:,}")
    print(f"Old fuera rango 0-100: {old_bad:,}")
    print(f"FP final faltante:     {still_missing:,}")
    print(f"Output:      {output}")
    if audit is not None:
        print(f"Audit:       {audit}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Repara campos por tarifa y topic sin invadir ámbitos."
    )

    parser.add_argument("--topic", required=True, help="Topic de reparación. Por ahora: power_factor.")
    parser.add_argument("--tariff", required=True, help="Tarifa. Por ahora para power_factor: GDBT.")
    parser.add_argument("--input", required=True, help="CSV de entrada.")
    parser.add_argument("--output", required=True, help="CSV corregido de salida.")
    parser.add_argument("--audit", default=None, help="CSV de auditoría opcional.")
    parser.add_argument("--tariff-column", default="tarifa", help="Columna de tarifa. Default: tarifa.")
    parser.add_argument(
        "--filter-tariff",
        action="store_true",
        help="Filtra por --tariff si el input es un CSV general.",
    )

    args = parser.parse_args(argv)

    topic = args.topic.strip().lower()
    tariff = args.tariff.strip().upper()

    try:
        ensure_allowed(topic, tariff)

        input_path = Path(args.input)
        if not input_path.exists():
            raise FileNotFoundError(f"No existe el input: {input_path}")

        df = pd.read_csv(input_path, low_memory=False)

        if topic == "power_factor" and tariff == "GDBT":
            repair_gdbt_power_factor(
                df=df,
                output=Path(args.output),
                audit=Path(args.audit) if args.audit else None,
                tariff_column=args.tariff_column,
                filter_tariff=args.filter_tariff,
            )
        else:
            raise ValueError(f"Repair no implementado para topic={topic}, tariff={tariff}")

        return 0

    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
