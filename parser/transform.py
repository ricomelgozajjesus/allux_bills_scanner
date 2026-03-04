# parser/transform.py
import pandas as pd


def _to_num_series(s: pd.Series) -> pd.Series:
    """
    Robust numeric cleaning:
    - strips spaces
    - removes $ and commas
    - coerces to float
    """
    if s is None:
        return s
    s = s.astype(str).str.strip()
    s = s.str.replace("$", "", regex=False).str.replace(",", "", regex=False)
    s = s.replace({"": None, "None": None, "nan": None})
    return pd.to_numeric(s, errors="coerce")


def transform_historico_v2(historico_df: pd.DataFrame) -> pd.DataFrame:
    df = historico_df.copy()

    # -------------------------
    # 0) Normalize column names (avoid hidden spaces)
    # -------------------------
    df.columns = [c.strip() for c in df.columns]

    # -------------------------
    # 1) Parse dates (safe)
    # -------------------------
    for c in ["periodo_inicio", "periodo_fin"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # -------------------------
    # 2) Force numeric columns (robust)
    # -------------------------
    for c in ["kwh", "importe", "pagos_pendientes"]:
        if c in df.columns:
            df[c] = _to_num_series(df[c])

    # -------------------------
    # 3) Compute billing days (inclusive)
    # -------------------------
    if "periodo_inicio" in df.columns and "periodo_fin" in df.columns:
        days = (df["periodo_fin"] - df["periodo_inicio"]).dt.days
        days = days + 1  # inclusive counting
        df["billing_days"] = pd.to_numeric(days, errors="coerce")
        df.loc[df["billing_days"].notna() & (df["billing_days"] <= 0), "billing_days"] = pd.NA
    else:
        df["billing_days"] = pd.NA

    # -------------------------
    # 4) Stable period key (great for dedup & joins)
    # -------------------------
    if "periodo_inicio" in df.columns and "periodo_fin" in df.columns:
        df["period_key"] = (
            df["periodo_inicio"].dt.date.astype(str)
            + "__"
            + df["periodo_fin"].dt.date.astype(str)
        )
    else:
        df["period_key"] = pd.NA

    # -------------------------
    # 5) Enforce/diagnose your assumption:
    #    1 PDF should contain 1 contract (no_servicio) + 1 cuenta
    # -------------------------
    # This does NOT drop anything; it only flags problems.
    if "file_path" in df.columns:
        if "no_servicio" in df.columns:
            df["pdf_no_servicio_nunique"] = (
                df.groupby("file_path")["no_servicio"].transform(lambda x: x.dropna().nunique())
            )
        else:
            df["pdf_no_servicio_nunique"] = pd.NA

        if "cuenta" in df.columns:
            df["pdf_cuenta_nunique"] = (
                df.groupby("file_path")["cuenta"].transform(lambda x: x.dropna().nunique())
            )
        else:
            df["pdf_cuenta_nunique"] = pd.NA

        df["flag_pdf_mixed_no_servicio"] = df["pdf_no_servicio_nunique"].fillna(1) > 1
        df["flag_pdf_mixed_cuenta"] = df["pdf_cuenta_nunique"].fillna(1) > 1
    else:
        df["pdf_no_servicio_nunique"] = pd.NA
        df["pdf_cuenta_nunique"] = pd.NA
        df["flag_pdf_mixed_no_servicio"] = False
        df["flag_pdf_mixed_cuenta"] = False

    # -------------------------
    # 6) Intelligence-ready features (safe divide)
    # -------------------------
    df["kwh_por_dia"] = pd.NA
    df["importe_por_dia"] = pd.NA
    df["costo_por_kwh"] = pd.NA

    mask_days = df["billing_days"].notna() & (df["billing_days"] > 0)
    mask_kwh = df["kwh"].notna() & (df["kwh"] > 0)

    df.loc[mask_days, "kwh_por_dia"] = df.loc[mask_days, "kwh"] / df.loc[mask_days, "billing_days"]
    df.loc[mask_days, "importe_por_dia"] = df.loc[mask_days, "importe"] / df.loc[mask_days, "billing_days"]
    df.loc[mask_kwh, "costo_por_kwh"] = df.loc[mask_kwh, "importe"] / df.loc[mask_kwh, "kwh"]

    # -------------------------
    # 7) Basic QA flags
    # -------------------------
    df["flag_missing_kwh"] = df["kwh"].isna()
    df["flag_missing_dates"] = df.get("periodo_inicio").isna() | df.get("periodo_fin").isna()
    df["flag_bad_days"] = df["billing_days"].isna()

    return df