#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extract_tariff_rows.py

Extrae de un CSV general los renglones correspondientes a una tarifa dada,
conservando TODAS las columnas/campos del archivo original.

Ejemplo:
    python extract_tariff_rows.py \
      --input output/bills_parsed_v4_enriched_CONSOLIDATED_ALL_TARIFFS.csv \
      --tariff GDMTH \
      --output output/recibos_tarifa_GDMTH.csv

También puedes usarlo para GDMTO, GDBT, PDBT, etc.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd


def normalize_tariff_value(value: object) -> str:
    """Normaliza el valor de tarifa para comparación robusta."""
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def extract_tariff_rows(
    input_csv: Path,
    tariff: str,
    output_csv: Path,
    tariff_column: str = "tarifa",
    exact: bool = True,
    encoding: str = "utf-8-sig",
) -> pd.DataFrame:
    """
    Lee input_csv y escribe output_csv con todos los renglones de la tarifa solicitada.

    Parameters
    ----------
    input_csv:
        Archivo CSV general.
    tariff:
        Tarifa a extraer, por ejemplo GDMTH, GDMTO, GDBT, PDBT.
    output_csv:
        Archivo CSV de salida.
    tariff_column:
        Nombre de la columna donde está la tarifa.
    exact:
        Si True, exige igualdad exacta normalizada.
        Si False, usa contains; útil si hay variantes como "GDMTH ..." .
    encoding:
        Encoding de salida. utf-8-sig abre bien en Excel.
    """
    if not input_csv.exists():
        raise FileNotFoundError(f"No existe el archivo de entrada: {input_csv}")

    df = pd.read_csv(input_csv, low_memory=False)

    if tariff_column not in df.columns:
        available = ", ".join(df.columns)
        raise KeyError(
            f"No existe la columna '{tariff_column}' en el CSV.\n"
            f"Columnas disponibles:\n{available}"
        )

    requested = tariff.strip().upper()
    tariff_series = df[tariff_column].map(normalize_tariff_value)

    if exact:
        mask = tariff_series.eq(requested)
    else:
        mask = tariff_series.str.contains(requested, na=False, regex=False)

    out = df.loc[mask].copy()

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_csv, index=False, encoding=encoding)

    return out


def build_default_output_path(input_csv: Path, tariff: str) -> Path:
    """Si el usuario no da --output, genera un nombre automático junto al input."""
    safe_tariff = tariff.strip().upper().replace(" ", "_")
    return input_csv.with_name(f"{input_csv.stem}_tarifa_{safe_tariff}.csv")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Extrae de un CSV general los renglones de una tarifa dada, "
            "conservando todas las columnas del archivo original."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help="CSV general de entrada, por ejemplo output/bills_parsed_v4_enriched_CONSOLIDATED_ALL_TARIFFS.csv",
    )

    parser.add_argument(
        "--tariff",
        required=True,
        help="Tarifa a extraer, por ejemplo GDMTH, GDMTO, GDBT o PDBT.",
    )

    parser.add_argument(
        "--output",
        default=None,
        help="CSV de salida. Si se omite, se genera automáticamente junto al archivo de entrada.",
    )

    parser.add_argument(
        "--tariff-column",
        default="tarifa",
        help="Nombre de la columna de tarifa. Default: tarifa.",
    )

    parser.add_argument(
        "--contains",
        action="store_true",
        help="Usar coincidencia parcial en vez de igualdad exacta.",
    )

    parser.add_argument(
        "--encoding",
        default="utf-8-sig",
        help="Encoding del CSV de salida. Default: utf-8-sig.",
    )

    args = parser.parse_args(argv)

    input_csv = Path(args.input)
    output_csv = Path(args.output) if args.output else build_default_output_path(input_csv, args.tariff)

    try:
        out = extract_tariff_rows(
            input_csv=input_csv,
            tariff=args.tariff,
            output_csv=output_csv,
            tariff_column=args.tariff_column,
            exact=not args.contains,
            encoding=args.encoding,
        )

        print("Extracción por tarifa")
        print("---------------------")
        print(f"Input:          {input_csv}")
        print(f"Tarifa:         {args.tariff.strip().upper()}")
        print(f"Output:         {output_csv}")
        print(f"Renglones:      {len(out):,}")
        print(f"Columnas:       {len(out.columns):,}")

        if len(out) == 0:
            print()
            print("[WARN] No se encontraron renglones para esa tarifa.")
            print("       Revisa valores disponibles con un value_counts sobre la columna tarifa.")

        return 0

    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
