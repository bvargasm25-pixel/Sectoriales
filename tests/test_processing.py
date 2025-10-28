from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("openpyxl")

from sectoriales.processing import BankExcelProcessor, ProcessorConfig


def _create_sample_excel(path: Path, data: pd.DataFrame, sheet_name: str = "Hoja1") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        data.to_excel(writer, sheet_name=sheet_name, index=False)


def test_process_directory(tmp_path: Path) -> None:
    base = tmp_path / "insumos"
    base.mkdir()

    data_a = pd.DataFrame(
        {
            "Banco": [
                "Banco Uno",
                "Banco Uno",
                "Banco Uno",
                "Banco Uno",
            ],
            "Cuenta": ["Cartera", "Cartera", "Depósitos", "Depósitos"],
            "Fecha": [
                "2022-12-31",
                "2023-12-31",
                "2022-12-31",
                "2023-12-31",
            ],
            "Importe": [800, 1000, 1500, 2000],
            "Moneda": ["USD", "USD", "USD", "USD"],
        }
    )
    data_b = pd.DataFrame(
        {
            "Entidad": ["Banco Dos", "Banco Dos"],
            "Concepto": ["Créditos", "Créditos"],
            "Periodo": ["2022-12-31", "2023-12-31"],
            "Monto": [1200, 1500],
            "Divisa": ["EUR", "EUR"],
        }
    )

    _create_sample_excel(base / "banco_uno.xlsx", data_a)
    _create_sample_excel(base / "banco_dos.xlsx", data_b, sheet_name="Balance")

    output = tmp_path / "resultado.xlsx"

    config = ProcessorConfig.default()
    config.parse_dates = ("period",)

    processor = BankExcelProcessor(config)
    combined = processor.process(base, output)

    assert {"bank", "metric", "period", "value"}.issubset(combined.columns)
    assert len(combined) == 6
    assert output.exists()

    dataset = pd.read_excel(output, sheet_name="dataset")
    assert len(dataset) == 6

    workbook = pd.ExcelFile(output, engine="openpyxl")
    assert any(name.startswith("resumen") for name in workbook.sheet_names)

    bank_sheets = [name for name in workbook.sheet_names if name.startswith("resumen_")]
    assert bank_sheets, "Se esperaba al menos una hoja de resumen por banco"

    banco_uno_sheet = next(
        (name for name in bank_sheets if "uno" in name.lower()), bank_sheets[0]
    )
    resumen_banco_uno = pd.read_excel(output, sheet_name=banco_uno_sheet)
    assert resumen_banco_uno.iloc[-1, 0] == "Total activos"
    total_row = resumen_banco_uno.iloc[-1, 1:]
    expected_total = resumen_banco_uno.iloc[:-1, 1:].sum(axis=0)
    pd.testing.assert_series_equal(total_row.reset_index(drop=True), expected_total.reset_index(drop=True))
