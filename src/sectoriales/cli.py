"""Interface de línea de comandos para consolidar información bancaria."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

from .processing import BankExcelProcessor, load_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Consolida múltiples archivos de Excel con información financiera de bancos "
            "en un único Excel organizado."
        )
    )
    parser.add_argument(
        "input",
        help="Ruta a un archivo de Excel o directorio con múltiples archivos.",
    )
    parser.add_argument(
        "output",
        help="Ruta del archivo de Excel que se generará como resultado.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Archivo YAML con la configuración de alias y opciones de procesamiento.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Nivel de logeo (por ejemplo INFO, DEBUG).",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))

    config = load_config(args.config)
    processor = BankExcelProcessor(config)
    processor.process(args.input, args.output)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
