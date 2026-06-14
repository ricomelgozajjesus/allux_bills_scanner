#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
merge_csv.py

Une varios archivos CSV en el orden indicado, conservando todas las columnas.

Reglas:
  1. El orden final respeta el orden de --inputs.
  2. No elimina columnas.
  3. Si un archivo no tiene una columna, esa columna queda vacía para sus filas.
  4. Agrega trazabilidad:
       merge_source_file
       merge_source_label
       merge_order
  5. Opcionalmente genera un resumen CSV.
  6. Opcionalmente reporta duplicados por una columna llave.

Ejemplo simple:
    python tools/merge_csv.py \
      --inputs output/tarifa_GDMTH.csv output/tarifa_GDMTO.csv \
      --output output/consolidado_GDMTH_GDMTO.csv \
      --source-labels GDMTH GDMTO \
      --summary output/consolidado_GDMTH_GDMTO_summary.csv

Ejemplo completo:
    python tools/merge_csv.py \
      --inputs \
        output/tarifa_GDMTH.csv \
        output/tarifa_GDMTO.csv \
        output/tarifa_GDBT_fixed_fp.csv \
        output/tarifa_PDBT.csv \
      --output output/bills_parsed_v5_validated_by_tariff.csv \
      --source-labels GDMTH GDMTO GDBT_FIXED_FP PDBT \
      --summary output/bills_parsed_v5_validated_by_tariff_summary.csv \
      --duplicates-key pdf_path \
      --duplicates-output output/bills_parsed_v5_duplicates_by_pdf_path.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd


TRACE_COLUMNS = [
    "merge_source_file",
    "merge_source_label",
    "merge_order",
]


def read_csv_safely(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo: {path}")

    return pd.read_csv(path, low_memory=False)


def build_labels(inputs: list[Path], labels: list[str] | None) -> list[str]:
    if labels is None or len(labels) == 0:
        return [p.stem for p in inputs]

    if len(labels) != len(inputs):
        raise ValueError(
            f"--source-labels debe tener la misma cantidad de elementos que --inputs. "
            f"inputs={len(inputs)}, labels={len(labels)}"
        )

    return labels


def ordered_union_columns(frames: list[pd.DataFrame]) -> list[str]:
    """
    Unión de columnas preservando el orden de aparición.
    Las columnas de trazabilidad van al final.
    """
    cols: list[str] = []
    seen = set()

    for df in frames:
        for col in df.columns:
            if col not in seen and col not in TRACE_COLUMNS:
                cols.append(col)
                seen.add(col)

    return cols + TRACE_COLUMNS


def merge_csvs(
    inputs: list[Path],
    output: Path,
    source_labels: list[str] | None = None,
    encoding: str = "utf-8-sig",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Une varios CSVs y devuelve:
      - merged_df
      - summary_df
    """
    labels = build_labels(inputs, source_labels)

    frames = []
    summary_rows = []

    for i, (path, label) in enumerate(zip(inputs, labels), start=1):
        df = read_csv_safely(path)
        original_rows = len(df)
        original_cols = len(df.columns)

        # Si algún CSV ya traía trazabilidad previa, no la conservamos como verdad actual.
        for trace_col in TRACE_COLUMNS:
            if trace_col in df.columns:
                df = df.drop(columns=[trace_col])

        df["merge_source_file"] = str(path)
        df["merge_source_label"] = label
        df["merge_order"] = i

        frames.append(df)

        summary_rows.append(
            {
                "merge_order": i,
                "source_label": label,
                "source_file": str(path),
                "rows": original_rows,
                "columns_original": original_cols,
            }
        )

    all_columns = ordered_union_columns(frames)

    aligned = []
    for df in frames:
        aligned.append(df.reindex(columns=all_columns))

    merged = pd.concat(aligned, ignore_index=True)

    output.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output, index=False, encoding=encoding)

    summary = pd.DataFrame(summary_rows)
    summary["rows_pct"] = (100 * summary["rows"] / len(merged)).round(2) if len(merged) else 0
    summary.loc[len(summary)] = {
        "merge_order": "TOTAL",
        "source_label": "TOTAL",
        "source_file": str(output),
        "rows": len(merged),
        "columns_original": len(merged.columns),
        "rows_pct": 100.0 if len(merged) else 0,
    }

    return merged, summary


def write_duplicate_report(
    merged: pd.DataFrame,
    key: str,
    output: Path,
    encoding: str = "utf-8-sig",
) -> pd.DataFrame:
    if key not in merged.columns:
        raise KeyError(f"No existe la columna llave para duplicados: {key}")

    key_series = merged[key].astype(str).str.strip()
    valid_key = merged[key].notna() & key_series.ne("") & ~key_series.str.lower().isin(["nan", "none", "null", "<na>"])

    dup_mask = valid_key & key_series.duplicated(keep=False)
    dup_df = merged.loc[dup_mask].copy()

    if not dup_df.empty:
        dup_df["_duplicate_key"] = key_series.loc[dup_mask]
        first_cols = ["_duplicate_key", key, "merge_source_label", "merge_order", "merge_source_file"]
        first_cols = [c for c in first_cols if c in dup_df.columns]
        rest = [c for c in dup_df.columns if c not in first_cols]
        dup_df = dup_df[first_cols + rest].sort_values(["_duplicate_key", "merge_order"])

    output.parent.mkdir(parents=True, exist_ok=True)
    dup_df.to_csv(output, index=False, encoding=encoding)

    return dup_df


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Une varios CSVs, conservando todas las columnas y agregando trazabilidad."
    )

    parser.add_argument(
        "--inputs",
        nargs="+",
        required=True,
        help="Lista de CSVs de entrada, en el orden en que deben aparecer en el resultado.",
    )

    parser.add_argument(
        "--output",
        required=True,
        help="CSV unido de salida.",
    )

    parser.add_argument(
        "--source-labels",
        nargs="*",
        default=None,
        help="Etiquetas para cada input. Debe tener la misma longitud que --inputs.",
    )

    parser.add_argument(
        "--summary",
        default=None,
        help="CSV opcional con resumen por archivo.",
    )

    parser.add_argument(
        "--duplicates-key",
        default=None,
        help="Columna opcional para detectar duplicados, por ejemplo pdf_path, file_path o no_servicio.",
    )

    parser.add_argument(
        "--duplicates-output",
        default=None,
        help="CSV opcional para escribir duplicados detectados.",
    )

    parser.add_argument(
        "--encoding",
        default="utf-8-sig",
        help="Encoding de salida. Default: utf-8-sig.",
    )

    args = parser.parse_args(argv)

    try:
        inputs = [Path(p) for p in args.inputs]
        output = Path(args.output)

        merged, summary = merge_csvs(
            inputs=inputs,
            output=output,
            source_labels=args.source_labels,
            encoding=args.encoding,
        )

        if args.summary:
            summary_path = Path(args.summary)
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            summary.to_csv(summary_path, index=False, encoding=args.encoding)
        else:
            summary_path = None

        duplicates_count = None
        duplicates_path = None

        if args.duplicates_key:
            duplicates_path = Path(args.duplicates_output) if args.duplicates_output else output.with_name(output.stem + f"_duplicates_by_{args.duplicates_key}.csv")
            dup_df = write_duplicate_report(
                merged=merged,
                key=args.duplicates_key,
                output=duplicates_path,
                encoding=args.encoding,
            )
            duplicates_count = len(dup_df)

        print("Merge CSV")
        print("---------")
        print(f"Inputs:     {len(inputs)}")
        for i, path in enumerate(inputs, start=1):
            label = args.source_labels[i - 1] if args.source_labels else path.stem
            print(f"  {i}. {label}: {path}")

        print(f"Output:     {output}")
        print(f"Filas:      {len(merged):,}")
        print(f"Columnas:   {len(merged.columns):,}")

        if summary_path:
            print(f"Summary:    {summary_path}")

        if duplicates_path:
            print(f"Duplicados: {duplicates_count:,} filas -> {duplicates_path}")

        print()
        print("Resumen por fuente:")
        print(summary.to_string(index=False))

        return 0

    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
