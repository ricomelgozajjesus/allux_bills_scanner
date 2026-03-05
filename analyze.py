#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import json
import pandas as pd
import matplotlib.pyplot as plt


def pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first candidate column that exists in df."""
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None


def safe_numeric(df: pd.DataFrame, col: str) -> None:
    df[col] = pd.to_numeric(df[col], errors="coerce")


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyze parsed CFE bills CSV and produce summaries.")
    ap.add_argument("--input", "-i", default="output/bills_parsed_v2.csv", help="Path to bills_parsed_v2.csv")
    ap.add_argument("--outdir", "-o", default="output/analysis", help="Directory for analysis outputs")
    ap.add_argument("--hours", type=float, default=720.0, help="Hours in billing period for load factor proxy")
    args = ap.parse_args()

    in_path = Path(args.input)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    figdir = outdir / "figures"
    figdir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(in_path)
    df.columns = [c.strip() for c in df.columns]

    # Try to locate common columns across versions
    col_tarifa = pick_column(df, ["tarifa", "Tarifa", "TARIFA"])
    col_meter = pick_column(df, ["medidor", "meter", "Meter", "no_medidor", "numero_medidor"])
    col_kwh = pick_column(df, ["kwh", "kWh", "energia_kwh", "energy_kwh"])
    col_kw = pick_column(df, ["kw_peak", "kW_peak", "kw_max", "demanda_kw", "demand_kw"])

    # Basic sanity
    summary: dict = {
        "rows": int(len(df)),
        "columns": list(df.columns),
    }

    if col_tarifa:
        summary["tarifa_counts"] = df[col_tarifa].astype(str).value_counts(dropna=False).head(20).to_dict()

    if col_meter:
        summary["unique_meters"] = int(df[col_meter].nunique(dropna=True))

    # Numeric summaries
    if col_kwh:
        safe_numeric(df, col_kwh)
        summary["kwh_desc"] = df[col_kwh].describe().to_dict()

    if col_kw:
        safe_numeric(df, col_kw)
        summary["kw_desc"] = df[col_kw].describe().to_dict()

    # Load factor proxy (if kWh and kW exist)
    if col_kwh and col_kw:
        df["load_factor"] = df[col_kwh] / (df[col_kw] * float(args.hours))
        summary["load_factor_desc"] = df["load_factor"].replace([float("inf"), -float("inf")], pd.NA).dropna().describe().to_dict()

    # Tenant fingerprints (group by meter)
    if col_meter and col_kwh:
        group_cols = {col_kwh: "mean"}
        if col_kw:
            group_cols[col_kw] = "mean"

        tenant = df.groupby(col_meter, dropna=True).agg(group_cols)
        tenant = tenant.rename(columns={col_kwh: "avg_kwh", col_kw: "avg_kw"} if col_kw else {col_kwh: "avg_kwh"})
        tenant["bills"] = df.groupby(col_meter, dropna=True).size()

        if col_kw:
            tenant["load_factor"] = tenant["avg_kwh"] / (tenant["avg_kw"] * float(args.hours))

        tenant = tenant.reset_index().rename(columns={col_meter: "medidor"})
        tenant.to_csv(outdir / "tenant_fingerprints.csv", index=False)
        summary["tenant_fingerprints_csv"] = str(outdir / "tenant_fingerprints.csv")

    # Simple plots
    if col_kwh:
        plt.figure()
        df[col_kwh].dropna().plot(kind="hist", bins=30)
        plt.xlabel("Monthly kWh")
        plt.ylabel("Count")
        plt.title("Distribution of kWh (bills)")
        plt.tight_layout()
        plt.savefig(figdir / "hist_kwh.png", dpi=160)
        plt.close()

    if col_kw:
        plt.figure()
        df[col_kw].dropna().plot(kind="hist", bins=30)
        plt.xlabel("Peak kW")
        plt.ylabel("Count")
        plt.title("Distribution of peak kW (bills)")
        plt.tight_layout()
        plt.savefig(figdir / "hist_kw.png", dpi=160)
        plt.close()

    # Save summary JSON
    with open(outdir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print(f"✅ Loaded: {in_path}")
    print(f"✅ Wrote:  {outdir / 'summary.json'}")
    if (outdir / "tenant_fingerprints.csv").exists():
        print(f"✅ Wrote:  {outdir / 'tenant_fingerprints.csv'}")
    print(f"✅ Figures in: {figdir}")


if __name__ == "__main__":
    main()