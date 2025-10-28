"""Procesamiento de archivos de Excel con información financiera de bancos."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

try:
    import yaml
except ImportError as exc:  # pragma: no cover - la librería es requerida en producción
    raise ImportError(
        "PyYAML es un requerimiento para cargar archivos de configuración."
    ) from exc

LOGGER = logging.getLogger(__name__)


def _normalize_column_name(name: str) -> str:
    """Normaliza un nombre de columna para facilitar comparaciones."""

    return "".join(name.strip().lower().split())


@dataclass
class ColumnAliasConfig:
    """Manejo de alias para columnas con nomenclaturas heterogéneas."""

    aliases: Mapping[str, Sequence[str]]
    required: Sequence[str] = field(default_factory=lambda: ("bank", "metric", "period", "value"))

    def __post_init__(self) -> None:
        self._normalized_aliases: Dict[str, Tuple[str, ...]] = {}
        for target, options in self.aliases.items():
            normalized_options = tuple({
                _normalize_column_name(option): option for option in options
            }.keys())
            self._normalized_aliases[target] = normalized_options

    def resolve(self, columns: Sequence[str]) -> Tuple[Dict[str, str], List[str]]:
        """Devuelve un mapeo de columnas estandarizadas y las faltantes."""

        normalized_source = {
            _normalize_column_name(original): original for original in columns
        }

        resolved: Dict[str, str] = {}
        missing: List[str] = []

        for target, aliases in self._normalized_aliases.items():
            found: Optional[str] = None
            for alias in (*aliases, _normalize_column_name(target)):
                if alias in normalized_source:
                    found = normalized_source[alias]
                    break
            if found is not None:
                resolved[target] = found
            elif target in self.required:
                missing.append(target)

        return resolved, missing


@dataclass
class ProcessorConfig:
    """Configuración del proceso de estandarización."""

    column_aliases: ColumnAliasConfig
    parse_dates: Sequence[str] = field(default_factory=tuple)
    numeric_columns: Sequence[str] = field(default_factory=lambda: ("value",))
    dropna_subset: Sequence[str] = field(default_factory=lambda: ("metric", "value"))
    sheet_name: Optional[str] = None
    header_row: int = 0
    trim_strings: bool = True
    additional_metadata: Mapping[str, str] = field(default_factory=dict)
    column_order: Sequence[str] = field(
        default_factory=lambda: (
            "bank",
            "metric",
            "period",
            "value",
            "currency",
            "units",
            "source_file",
            "sheet_name",
        )
    )
    summary_sheet_name: str = "resumen"
    summary_bank_sheet_prefix: str = "resumen_"
    summary_include_totals: bool = True
    summary_total_label: str = "Total activos"

    @classmethod
    def default(cls) -> "ProcessorConfig":
        """Genera una configuración predeterminada adecuada para la mayoría de los casos."""

        aliases = ColumnAliasConfig(
            aliases={
                "bank": ("Banco", "Entidad", "Institución", "Bank"),
                "metric": ("Cuenta", "Concepto", "Rubro", "Metric"),
                "period": (
                    "Fecha",
                    "Periodo",
                    "Periodo_Contable",
                    "Quarter",
                    "Year",
                    "Mes",
                ),
                "value": (
                    "Importe",
                    "Valor",
                    "Monto",
                    "Saldo",
                    "Balance",
                    "Value",
                ),
                "currency": ("Moneda", "Divisa", "Currency"),
                "units": ("Unidades", "Unidad", "Units"),
            }
        )
        return cls(column_aliases=aliases)

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> "ProcessorConfig":
        """Construye la configuración a partir de un diccionario (por ejemplo YAML)."""

        aliases_data = data.get("column_aliases")
        if not isinstance(aliases_data, Mapping):
            raise ValueError("La sección 'column_aliases' es obligatoria en la configuración.")

        required = data.get("required_columns")
        if required is None:
            required_seq: Sequence[str] = ("bank", "metric", "period", "value")
        else:
            if not isinstance(required, Sequence):
                raise ValueError("'required_columns' debe ser una secuencia de nombres.")
            required_seq = tuple(str(item) for item in required)

        column_aliases = ColumnAliasConfig(
            aliases={key: tuple(map(str, value)) for key, value in aliases_data.items()},
            required=required_seq,
        )

        parse_dates = tuple(map(str, data.get("parse_dates", ())))
        numeric_columns = tuple(map(str, data.get("numeric_columns", ("value",))))
        dropna_subset = tuple(map(str, data.get("dropna_subset", ("metric", "value"))))
        sheet_name = data.get("sheet_name")
        header_row = int(data.get("header_row", 0))
        trim_strings = bool(data.get("trim_strings", True))
        additional_metadata = {
            str(key): str(value) for key, value in data.get("additional_metadata", {}).items()
        }
        column_order = tuple(map(str, data.get("column_order", []))) or (
            "bank",
            "metric",
            "period",
            "value",
            "currency",
            "units",
            "source_file",
            "sheet_name",
        )

        summary_sheet_name = str(data.get("summary_sheet_name", "resumen"))
        summary_bank_sheet_prefix = str(
            data.get("summary_bank_sheet_prefix", "resumen_")
        )
        summary_include_totals = bool(data.get("summary_include_totals", True))
        summary_total_label = str(data.get("summary_total_label", "Total activos"))

        return cls(
            column_aliases=column_aliases,
            parse_dates=parse_dates,
            numeric_columns=numeric_columns,
            dropna_subset=dropna_subset,
            sheet_name=sheet_name if isinstance(sheet_name, str) else None,
            header_row=header_row,
            trim_strings=trim_strings,
            additional_metadata=additional_metadata,
            column_order=column_order,
            summary_sheet_name=summary_sheet_name,
            summary_bank_sheet_prefix=summary_bank_sheet_prefix,
            summary_include_totals=summary_include_totals,
            summary_total_label=summary_total_label,
        )


def load_config(path: Optional[Path]) -> ProcessorConfig:
    """Carga la configuración desde YAML o devuelve la configuración por defecto."""

    if path is None:
        return ProcessorConfig.default()

    with Path(path).open("r", encoding="utf-8") as handler:
        data = yaml.safe_load(handler)

    if not isinstance(data, Mapping):
        raise ValueError("El archivo de configuración debe contener un mapeo de claves.")

    return ProcessorConfig.from_dict(data)


class BankExcelProcessor:
    """Motor principal de estandarización y consolidación de Excel de bancos."""

    def __init__(self, config: Optional[ProcessorConfig] = None) -> None:
        self.config = config or ProcessorConfig.default()

    def process(self, input_path: Path | str, output_path: Path | str) -> pd.DataFrame:
        """Procesa el origen y genera un Excel consolidado."""

        input_path = Path(input_path)
        output_path = Path(output_path)

        files = self._discover_files(input_path)
        processed_frames: List[pd.DataFrame] = []

        for excel_file in files:
            for sheet_name, frame in self._load_excel(excel_file):
                processed = self._transform_frame(frame, excel_file, sheet_name)
                if processed is None or processed.empty:
                    continue
                processed_frames.append(processed)

        if not processed_frames:
            raise ValueError(
                "No se encontraron datos válidos. Verifique los alias de columnas y los archivos de origen."
            )

        combined = pd.concat(processed_frames, ignore_index=True)
        combined = self._reorder_columns(combined)

        self._export_output(combined, output_path)
        return combined

    def _discover_files(self, input_path: Path) -> List[Path]:
        if input_path.is_file():
            return [input_path]
        if input_path.is_dir():
            files = sorted(
                path
                for path in input_path.iterdir()
                if path.suffix.lower() in {".xls", ".xlsx", ".xlsm", ".xlsb"}
            )
            if not files:
                raise FileNotFoundError(
                    f"No se encontraron archivos de Excel en el directorio {input_path!s}."
                )
            return files
        raise FileNotFoundError(f"La ruta {input_path!s} no existe.")

    def _load_excel(self, path: Path) -> Iterator[Tuple[str, pd.DataFrame]]:
        sheet_name = self.config.sheet_name if self.config.sheet_name else None
        try:
            data = pd.read_excel(path, sheet_name=sheet_name, header=self.config.header_row)
        except Exception as exc:  # pragma: no cover - pandas ya gestiona el detalle
            raise RuntimeError(f"Error al leer el archivo {path!s}: {exc}") from exc

        if isinstance(data, Mapping):
            for sheet, frame in data.items():
                yield str(sheet), frame
        else:
            yield self.config.sheet_name or "Sheet1", data

    def _transform_frame(self, frame: pd.DataFrame, source: Path, sheet_name: str) -> Optional[pd.DataFrame]:
        original_columns = list(frame.columns)
        frame = frame.copy()

        if self.config.trim_strings:
            frame.columns = [str(col).strip() for col in frame.columns]
            for column in frame.select_dtypes(include="object"):
                frame[column] = frame[column].astype(str).str.strip()

        resolved, missing = self.config.column_aliases.resolve(frame.columns)
        if missing:
            LOGGER.warning(
                "Se omite la hoja '%s' de %s porque faltan columnas requeridas: %s",
                sheet_name,
                source.name,
                ", ".join(missing),
            )
            return None

        frame = frame.rename(columns=resolved)

        for column in self.config.parse_dates:
            if column in frame.columns:
                frame[column] = pd.to_datetime(frame[column], errors="coerce")

        for column in self.config.numeric_columns:
            if column in frame.columns:
                cleaned = (
                    frame[column]
                    .astype(str)
                    .str.replace("%", "", regex=False)
                    .str.replace(",", "", regex=False)
                    .replace({"": pd.NA, "nan": pd.NA})
                )
                frame[column] = pd.to_numeric(cleaned, errors="coerce")

        drop_subset = [col for col in self.config.dropna_subset if col in frame.columns]
        if drop_subset:
            frame = frame.dropna(subset=drop_subset, how="any")

        frame = frame.assign(
            source_file=source.name,
            sheet_name=sheet_name,
            **self.config.additional_metadata,
        )

        for column in self.config.column_order:
            if column not in frame.columns:
                frame[column] = pd.NA

        # Retener únicamente columnas relevantes respetando el orden deseado
        columns_to_keep: List[str] = list(self.config.column_order)
        for column in frame.columns:
            if column not in columns_to_keep:
                columns_to_keep.append(column)

        frame = frame.loc[:, [col for col in columns_to_keep if col in frame.columns]]

        LOGGER.info(
            "Procesado %s | Hoja: %s | Columnas originales: %s | Columnas finales: %s",
            source.name,
            sheet_name,
            original_columns,
            frame.columns.tolist(),
        )

        return frame

    def _reorder_columns(self, frame: pd.DataFrame) -> pd.DataFrame:
        ordered_cols = [col for col in self.config.column_order if col in frame.columns]
        remaining_cols = [col for col in frame.columns if col not in ordered_cols]
        return frame.loc[:, ordered_cols + remaining_cols]

    def _export_output(self, frame: pd.DataFrame, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            used_sheet_names: set[str] = set()

            dataset_sheet = self._ensure_unique_sheet_name("dataset", used_sheet_names)
            frame.to_excel(writer, sheet_name=dataset_sheet, index=False)

            summary, per_bank_summaries = self._build_summary_tables(frame)
            if not summary.empty:
                summary_sheet = self._ensure_unique_sheet_name(
                    self.config.summary_sheet_name, used_sheet_names
                )
                summary.to_excel(writer, sheet_name=summary_sheet)

                for bank, summary_frame in per_bank_summaries.items():
                    sheet_label = f"{self.config.summary_bank_sheet_prefix}{bank}"
                    sheet_name = self._ensure_unique_sheet_name(sheet_label, used_sheet_names)
                    summary_frame.to_excel(writer, sheet_name=sheet_name)

            sources = (
                frame[["source_file", "sheet_name"]]
                .drop_duplicates()
                .sort_values(by=["source_file", "sheet_name"])
            )
            sources_sheet = self._ensure_unique_sheet_name("fuentes", used_sheet_names)
            sources.to_excel(writer, sheet_name=sources_sheet, index=False)

    def _build_summary_tables(
        self, frame: pd.DataFrame
    ) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
        if not {"bank", "metric", "period", "value"}.issubset(frame.columns):
            return pd.DataFrame(), {}

        working = frame.dropna(subset=["value"]).assign(
            period=lambda df: df["period"].astype(str)
        )

        if working.empty:
            return pd.DataFrame(), {}

        summary = working.pivot_table(
            index=["bank", "metric"],
            columns="period",
            values="value",
            aggfunc="sum",
            fill_value=0,
        )

        summary = summary.sort_index()
        summary.columns = summary.columns.map(str)
        summary = summary.loc[:, self._sorted_periods(summary.columns)]
        summary.columns.name = "period"

        per_bank: Dict[str, pd.DataFrame] = {}
        if isinstance(summary.index, pd.MultiIndex):
            for bank in summary.index.get_level_values(0).unique():
                bank_summary = summary.xs(bank, level=0)
                if isinstance(bank_summary, pd.Series):
                    bank_summary = bank_summary.to_frame().T
                bank_summary = bank_summary.copy()
                bank_summary.index.name = "metric"
                bank_summary.columns = bank_summary.columns.map(str)
                bank_summary = bank_summary.loc[:, self._sorted_periods(bank_summary.columns)]
                if self.config.summary_include_totals:
                    totals = bank_summary.sum(axis=0)
                    totals.name = self.config.summary_total_label
                    bank_summary = pd.concat([bank_summary, totals.to_frame().T])
                per_bank[str(bank)] = bank_summary
        else:
            bank_summary = summary.copy()
            bank_summary.index.name = "metric"
            if self.config.summary_include_totals:
                totals = bank_summary.sum(axis=0)
                totals.name = self.config.summary_total_label
                bank_summary = pd.concat([bank_summary, totals.to_frame().T])
            per_bank["general"] = bank_summary

        return summary, per_bank

    def _sorted_periods(self, periods: Sequence[object]) -> List[str]:
        def sort_key(value: object) -> Tuple[int, str]:
            text = str(value)
            match = re.fullmatch(r"(\d{4})(?:-(\d{2})-(\d{2}))?", text)
            if match:
                year = int(match.group(1))
                month = int(match.group(2)) if match.group(2) else 0
                day = int(match.group(3)) if match.group(3) else 0
                return (0, f"{year:04d}-{month:02d}-{day:02d}")
            return (1, text)

        unique_values: List[str] = []
        seen = set()
        for period in periods:
            text = str(period)
            if text not in seen:
                unique_values.append(text)
                seen.add(text)

        ordered = sorted(unique_values, key=sort_key)
        return ordered

    def _ensure_unique_sheet_name(
        self, raw_name: str, used_names: set[str]
    ) -> str:
        cleaned = re.sub(r"[\\/?*\[\]:]", "", str(raw_name)).strip() or "Sheet"
        cleaned = cleaned[:31]
        candidate = cleaned
        counter = 1
        while candidate in used_names:
            suffix = f"_{counter}"
            candidate = f"{cleaned[: 31 - len(suffix)]}{suffix}"
            counter += 1
        used_names.add(candidate)
        return candidate
