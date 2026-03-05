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



import numpy as np
import pandas as pd

def compute_mall_fingerprints(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mall-level rollup for benchmarking.

    Required columns:
      - mall_folder
      - recibos_subgroup
      - medidor
      - kwh_total
      - importe_total
      - tarifa
    """
    required = {"mall_folder", "recibos_subgroup", "medidor", "kwh_total", "importe_total", "tarifa"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"compute_mall_fingerprints missing columns: {sorted(missing)}")

    x = df.copy()
    x["kwh_total"] = pd.to_numeric(x["kwh_total"], errors="coerce")
    x["importe_total"] = pd.to_numeric(x["importe_total"], errors="coerce")

    # Cost per kWh at bill level (avoid divide by 0)
    x["mxn_per_kwh"] = np.where(
        (x["kwh_total"].notna()) & (x["kwh_total"] > 0) & (x["importe_total"].notna()),
        x["importe_total"] / x["kwh_total"],
        np.nan,
    )

    g = x.groupby(["mall_folder", "recibos_subgroup"], dropna=False)

    out = g.agg(
        bills=("kwh_total", "count"),
        meters=("medidor", pd.Series.nunique),
        kwh_sum=("kwh_total", "sum"),
        kwh_mean_bill=("kwh_total", "mean"),
        importe_sum=("importe_total", "sum"),
        mxn_per_kwh_mean=("mxn_per_kwh", "mean"),
    ).reset_index()

    # A very handy metric: average kWh per meter (over the whole dataset window)
    out["kwh_per_meter"] = out["kwh_sum"] / out["meters"]

    # Tariff mix (shares) for quick mall characterization
    # We compute shares by bill count (simple + robust).
    tariff_counts = (
        x.assign(tarifa=x["tarifa"].astype(str))
         .groupby(["mall_folder", "recibos_subgroup", "tarifa"], dropna=False)
         .size()
         .rename("tarifa_bills")
         .reset_index()
    )

    # Pivot tariffs into columns like share_PDBT, share_GDMTO, etc.
    totals = tariff_counts.groupby(["mall_folder", "recibos_subgroup"])["tarifa_bills"].sum().rename("tarifa_bills_total")
    tariff_counts = tariff_counts.merge(totals.reset_index(), on=["mall_folder", "recibos_subgroup"], how="left")
    tariff_counts["tarifa_share"] = tariff_counts["tarifa_bills"] / tariff_counts["tarifa_bills_total"]

    tariff_pivot = (
        tariff_counts.pivot_table(
            index=["mall_folder", "recibos_subgroup"],
            columns="tarifa",
            values="tarifa_share",
            fill_value=0.0,
            aggfunc="first",
        )
        .reset_index()
    )

    # Rename columns to share_<tarifa>
    for c in list(tariff_pivot.columns):
        if c not in ("mall_folder", "recibos_subgroup"):
            tariff_pivot = tariff_pivot.rename(columns={c: f"share_{c}"})

    out = out.merge(tariff_pivot, on=["mall_folder", "recibos_subgroup"], how="left")

    return out.sort_values(["mall_folder", "recibos_subgroup"]).reset_index(drop=True)

def compute_tenant_fingerprints(
    df: pd.DataFrame,
    hours_default: float = 720.0,
) -> pd.DataFrame:
    """
    Produces a per-tenant (per 'medidor') fingerprint table.

    Expected columns in df (based on your dataset):
      - medidor
      - mall_folder
      - recibos_subgroup
      - tarifa
      - kwh_total
      - importe_total
      - periodo_inicio (optional)
      - periodo_fin (optional)

    Output columns:
      - mall_folder, recibos_subgroup, medidor
      - tarifa_mode
      - bills_count
      - kwh_sum, kwh_mean, kwh_median
      - importe_sum, importe_mean, mxn_per_kwh_mean
      - first_periodo_inicio, last_periodo_fin
      - months_span (rough)
    """

    required = {"medidor", "kwh_total", "importe_total", "tarifa", "mall_folder", "recibos_subgroup"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"compute_tenant_fingerprints missing columns: {sorted(missing)}")

    x = df.copy()

    # Normalize types
    x["kwh_total"] = pd.to_numeric(x["kwh_total"], errors="coerce")
    x["importe_total"] = pd.to_numeric(x["importe_total"], errors="coerce")

    # Dates (optional but very useful)
    for c in ["periodo_inicio", "periodo_fin"]:
        if c in x.columns:
            x[c] = pd.to_datetime(x[c], errors="coerce")

    # Avoid divide-by-zero
    x["mxn_per_kwh"] = np.where(
        (x["kwh_total"].notna()) & (x["kwh_total"] > 0) & (x["importe_total"].notna()),
        x["importe_total"] / x["kwh_total"],
        np.nan,
    )

    group_keys = ["mall_folder", "recibos_subgroup", "medidor"]

    def mode_or_nan(s: pd.Series):
        s = s.dropna().astype(str)
        if s.empty:
            return np.nan
        return s.mode().iloc[0]

    agg = {
        "tarifa": mode_or_nan,
        "kwh_total": ["count", "sum", "mean", "median"],
        "importe_total": ["sum", "mean"],
        "mxn_per_kwh": ["mean", "median"],
    }

    out = x.groupby(group_keys, dropna=False).agg(agg)

    # Flatten columns
    out.columns = [
        "tarifa_mode",
        "bills_count",
        "kwh_sum",
        "kwh_mean",
        "kwh_median",
        "importe_sum",
        "importe_mean",
        "mxn_per_kwh_mean",
        "mxn_per_kwh_median",
    ]
    out = out.reset_index()

    # Optional period coverage metrics
    if "periodo_inicio" in x.columns:
        first_start = x.groupby(group_keys)["periodo_inicio"].min().rename("first_periodo_inicio")
        out = out.merge(first_start.reset_index(), on=group_keys, how="left")

    if "periodo_fin" in x.columns:
        last_end = x.groupby(group_keys)["periodo_fin"].max().rename("last_periodo_fin")
        out = out.merge(last_end.reset_index(), on=group_keys, how="left")

    # Rough span in months (useful to judge completeness)
    if "first_periodo_inicio" in out.columns and "last_periodo_fin" in out.columns:
        span_days = (out["last_periodo_fin"] - out["first_periodo_inicio"]).dt.days
        out["months_span"] = (span_days / 30.0).round(1)

    # Sort for readability
    out = out.sort_values(["mall_folder", "recibos_subgroup", "medidor"]).reset_index(drop=True)

    return out

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
# --- Tenant fingerprints ---
    tenant_fp = compute_tenant_fingerprints(df)
    tenant_fp_path = outdir / "tenant_fingerprints.csv"
    tenant_fp.to_csv(tenant_fp_path, index=False)

    print(f"✅ Wrote:  {tenant_fp_path}")
    mall_fp = compute_mall_fingerprints(df)
    mall_fp_path = outdir / "mall_fingerprints.csv"
    mall_fp.to_csv(mall_fp_path, index=False)
    print(f"✅ Wrote:  {mall_fp_path}")

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