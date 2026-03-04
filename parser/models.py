from __future__ import annotations

from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field, ConfigDict, computed_field


# ----------------------------
# Enums (optional but useful)
# ----------------------------
class Tarifa(str, Enum):
    GDBT = "GDBT"
    GDMTO = "GDMTO"
    GDMTH = "GDMTH"
    PDBT = "PDBT"
    UNKNOWN = "UNKNOWN"


class ParseStatus(str, Enum):
    OK = "ok"
    NO_FIELDS = "no_fields"
    SCANNED_OR_EMPTY = "scanned_or_empty"
    ERROR = "error"


# ----------------------------
# Small submodels
# ----------------------------
class Provenance(BaseModel):
    model_config = ConfigDict(extra="ignore")

    file_path: str
    file_name: Optional[str] = None

    # from folder taxonomy
    cliente_tipo: Optional[str] = None
    voltaje_label: Optional[str] = None

    # extraction metadata
    extracted_pages: Optional[int] = None
    text_len: Optional[int] = None
    parser_version: str = "BillSchemaV1"
    parse_status: ParseStatus = ParseStatus.OK
    parse_error: Optional[str] = None


class Identifiers(BaseModel):
    model_config = ConfigDict(extra="ignore")

    cliente_nombre: Optional[str] = None
    rpu: Optional[str] = None
    no_servicio: Optional[str] = None
    no_medidor: Optional[str] = None
    tarifa: Tarifa = Tarifa.UNKNOWN
    direccion_servicio: Optional[str] = None  # may be sensitive—store only if you want


class Period(BaseModel):
    model_config = ConfigDict(extra="ignore")

    fecha_emision: Optional[date] = None
    periodo_inicio: Optional[date] = None
    periodo_fin: Optional[date] = None
    dias_facturados: Optional[int] = None


class MeterReadings(BaseModel):
    model_config = ConfigDict(extra="ignore")

    lectura_anterior: Optional[Decimal] = None
    lectura_actual: Optional[Decimal] = None
    multiplicador: Optional[Decimal] = None


class EnergyDemand(BaseModel):
    model_config = ConfigDict(extra="ignore")

    kwh_total: Optional[Decimal] = None

    # time-of-use (if applicable)
    kwh_base: Optional[Decimal] = None
    kwh_intermedia: Optional[Decimal] = None
    kwh_punta: Optional[Decimal] = None

    kw_max_demand: Optional[Decimal] = None

    # power quality
    factor_potencia: Optional[Decimal] = None  # e.g. 0.92
    kvarh: Optional[Decimal] = None


class Charges(BaseModel):
    model_config = ConfigDict(extra="ignore")

    # energy + demand breakdown (if available)
    subtotal_energia: Optional[Decimal] = None
    cargo_demanda: Optional[Decimal] = None
    cargo_fijo: Optional[Decimal] = None

    # taxes and other fees
    iva: Optional[Decimal] = None
    otros_cargos: Optional[Decimal] = None
    bonificaciones: Optional[Decimal] = None

    # totals
    importe_total: Optional[Decimal] = None


class Payments(BaseModel):
    model_config = ConfigDict(extra="ignore")

    limite_pago: Optional[date] = None
    saldo_anterior: Optional[Decimal] = None
    pagos: Optional[Decimal] = None
    saldo_actual: Optional[Decimal] = None


# ----------------------------
# Main schema
# ----------------------------
class BillSchemaV1(BaseModel):
    """
    Canonical bill record for analytics.
    Keep raw fields optional; you can still compute derived metrics when available.
    """
    model_config = ConfigDict(extra="ignore")

    provenance: Provenance
    ids: Identifiers = Field(default_factory=Identifiers)
    period: Period = Field(default_factory=Period)
    meter: MeterReadings = Field(default_factory=MeterReadings)
    energy: EnergyDemand = Field(default_factory=EnergyDemand)
    charges: Charges = Field(default_factory=Charges)
    payments: Payments = Field(default_factory=Payments)

    # optional: keep evidence snippets for debugging/explainability
    evidence: Dict[str, str] = Field(default_factory=dict)

    # ----------------------------
    # Derived metrics (computed)
    # ----------------------------
    @computed_field
    @property
    def kwh_per_day(self) -> Optional[Decimal]:
        if self.energy.kwh_total is None or self.period.dias_facturados in (None, 0):
            return None
        return (self.energy.kwh_total / Decimal(self.period.dias_facturados)).quantize(Decimal("0.0001"))

    @computed_field
    @property
    def cost_per_kwh(self) -> Optional[Decimal]:
        if self.charges.importe_total is None or self.energy.kwh_total in (None, 0):
            return None
        return (self.charges.importe_total / self.energy.kwh_total).quantize(Decimal("0.0001"))

    @computed_field
    @property
    def load_factor(self) -> Optional[Decimal]:
        """
        LF = kWh_total / (kW_max * 24 * days)
        """
        if (
            self.energy.kwh_total is None
            or self.energy.kw_max_demand in (None, 0)
            or self.period.dias_facturados in (None, 0)
        ):
            return None
        denom = self.energy.kw_max_demand * Decimal(24) * Decimal(self.period.dias_facturados)
        return (self.energy.kwh_total / denom).quantize(Decimal("0.0001"))

    @computed_field
    @property
    def demand_ratio(self) -> Optional[Decimal]:
        """
        share of total bill explained by demand charges (when present)
        """
        if self.charges.cargo_demanda is None or self.charges.importe_total in (None, 0):
            return None
        return (self.charges.cargo_demanda / self.charges.importe_total).quantize(Decimal("0.0001"))

    # ----------------------------
    # Convenience: build from your parser dict
    # ----------------------------
    @classmethod
    def from_parser(
        cls,
        *,
        file_path: str,
        file_name: Optional[str] = None,
        cliente_tipo: Optional[str] = None,
        voltaje_label: Optional[str] = None,
        extracted_pages: Optional[int] = None,
        text_len: Optional[int] = None,
        parse_status: ParseStatus = ParseStatus.OK,
        parse_error: Optional[str] = None,
        fields: Dict[str, Any] | None = None,
        evidence: Dict[str, str] | None = None,
    ) -> "BillSchemaV1":
        """
        `fields` is a flat dict from regex extraction; we map into nested structure here.
        """
        fields = fields or {}
        evidence = evidence or {}

        prov = Provenance(
            file_path=file_path,
            file_name=file_name,
            cliente_tipo=cliente_tipo,
            voltaje_label=voltaje_label,
            extracted_pages=extracted_pages,
            text_len=text_len,
            parse_status=parse_status,
            parse_error=parse_error,
        )

        ids = Identifiers(
            cliente_nombre=fields.get("cliente_nombre"),
            rpu=fields.get("rpu"),
            no_servicio=fields.get("no_servicio"),
            no_medidor=fields.get("no_medidor") or fields.get("medidor"),
            tarifa=Tarifa(fields.get("tarifa")) if fields.get("tarifa") in Tarifa._value2member_map_ else Tarifa.UNKNOWN,
            direccion_servicio=fields.get("direccion_servicio"),
        )

        period = Period(
            fecha_emision=fields.get("fecha_emision"),
            periodo_inicio=fields.get("periodo_inicio"),
            periodo_fin=fields.get("periodo_fin"),
            dias_facturados=fields.get("dias_facturados"),
        )

        meter = MeterReadings(
            lectura_anterior=fields.get("lectura_anterior"),
            lectura_actual=fields.get("lectura_actual"),
            multiplicador=fields.get("multiplicador"),
        )

        energy = EnergyDemand(
            kwh_total=fields.get("kwh_total") or fields.get("kwh"),
            kwh_base=fields.get("kwh_base"),
            kwh_intermedia=fields.get("kwh_intermedia"),
            kwh_punta=fields.get("kwh_punta"),
            kw_max_demand=fields.get("kw_max_demand"),
            factor_potencia=fields.get("factor_potencia"),
            kvarh=fields.get("kvarh"),
        )

        charges = Charges(
            subtotal_energia=fields.get("subtotal_energia"),
            cargo_demanda=fields.get("cargo_demanda"),
            cargo_fijo=fields.get("cargo_fijo"),
            iva=fields.get("iva"),
            otros_cargos=fields.get("otros_cargos"),
            bonificaciones=fields.get("bonificaciones"),
            importe_total=fields.get("importe_total"),
        )

        payments = Payments(
            limite_pago=fields.get("limite_pago"),
            saldo_anterior=fields.get("saldo_anterior"),
            pagos=fields.get("pagos"),
            saldo_actual=fields.get("saldo_actual"),
        )

        return cls(
            provenance=prov,
            ids=ids,
            period=period,
            meter=meter,
            energy=energy,
            charges=charges,
            payments=payments,
            evidence=evidence,
        )