# Sectoriales - Bancos

Herramienta en Python para consolidar la información financiera de distintos bancos a partir de archivos de Excel. El objetivo es unificar los datos provenientes de distintas fuentes en un único archivo organizado que facilite el análisis sectorial.

## Características

- Lectura de múltiples archivos de Excel (soporta `.xls`, `.xlsx`, `.xlsm`, `.xlsb`).
- Detección automática de columnas relevantes a través de alias configurables.
- Limpieza básica de datos: eliminación de filas vacías, conversión de fechas y métricas numéricas.
- Exportación de un Excel consolidado con un dataset estandarizado, hojas de
  resumen por banco y trazabilidad de fuentes:
  - `dataset`: datos estandarizados y listos para el análisis.
  - `resumen`: tabla dinámica global con los valores agregados por banco, métrica y periodo.
  - `resumen_<banco>`: tablas resumen con columnas por periodo y totales como el "Total activos" mostrado en el ejemplo.
  - `fuentes`: detalle de los archivos y hojas procesadas.
- Configuración flexible mediante un archivo YAML opcional.
- Interfaz de línea de comandos (`sectoriales-process`).

## Instalación

Se recomienda utilizar un entorno virtual de Python 3.10 o superior.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

## Uso desde la línea de comandos

```bash
sectoriales-process <ruta_de_entrada> <ruta_de_salida> [--config config/ejemplo_configuracion.yaml]
```

- `<ruta_de_entrada>`: archivo de Excel individual o directorio con múltiples archivos.
- `<ruta_de_salida>`: archivo de Excel a generar (por ejemplo `salida/sectorial.xlsx`).
- `--config`: ruta a un archivo YAML con configuraciones personalizadas (alias de columnas, columnas obligatorias, etc.).

### Ejemplo

```bash
sectoriales-process datos_bancos/ salida/sectorial.xlsx --config config/ejemplo_configuracion.yaml
```

## Uso programático

```python
from sectoriales.processing import BankExcelProcessor, load_config

config = load_config("config/ejemplo_configuracion.yaml")
processor = BankExcelProcessor(config)
resultado = processor.process("datos_bancos", "salida/sectorial.xlsx")
```

`resultado` es un `pandas.DataFrame` con la información estandarizada.

## Configuración personalizada

El archivo [`config/ejemplo_configuracion.yaml`](config/ejemplo_configuracion.yaml) muestra cómo definir alias y opciones específicas para adaptar el proceso a distintas fuentes de información.

## Pruebas

```bash
pytest
```
