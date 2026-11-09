#!/usr/bin/env python3
"""Convert a repository SQLite snapshot (.db or .db.zst) to JSON, YAML, and XLSX."""

from __future__ import annotations

import argparse
import base64
import json
import math
import os
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Any


EXCEL_MAX_ROWS = 1_048_576
EXCEL_MAX_CELL_CHARS = 32_767


class ConversionError(RuntimeError):
    """The source database could not be converted safely."""


def json_value(value: Any, column: str | None = None) -> Any:
    if isinstance(value, bytes):
        return {"$binary_base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if column == "document" and isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def output_stem(source: Path) -> str:
    name = source.name
    if name.lower().endswith(".db.zst"):
        return name[:-7]
    if name.lower().endswith(".db"):
        return name[:-3]
    raise ConversionError("El archivo de entrada debe terminar en .db o .db.zst")


def open_database(source: Path, temp_dir: Path) -> tuple[Path, Path | None]:
    if source.suffix.lower() == ".db":
        return source, None
    if not source.name.lower().endswith(".db.zst"):
        raise ConversionError("El archivo de entrada debe terminar en .db o .db.zst")
    try:
        import zstandard
    except ImportError as exc:
        raise ConversionError("Para leer .db.zst instala zstandard: python -m pip install zstandard") from exc

    handle = tempfile.NamedTemporaryFile(prefix="codex-db-", suffix=".db", dir=temp_dir, delete=False)
    decompressed = Path(handle.name)
    try:
        with source.open("rb") as compressed, handle:
            zstandard.ZstdDecompressor().copy_stream(compressed, handle)
        return decompressed, decompressed
    except Exception:
        handle.close()
        decompressed.unlink(missing_ok=True)
        raise ConversionError(f"No se pudo descomprimir {source}") from None


def table_names(connection: sqlite3.Connection) -> list[str]:
    return [
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]


def excel_sheet_name(table: str, used: set[str], part: int) -> str:
    base = "".join("_" if char in "[]:*?/\\" else char for char in table).strip("'") or "table"
    suffix = f"_{part}" if part > 1 else ""
    base = base[: 31 - len(suffix)]
    candidate = f"{base}{suffix}"
    counter = 2
    while candidate.casefold() in used:
        suffix = f"_{part}_{counter}"
        candidate = f"{base[:31 - len(suffix)]}{suffix}"
        counter += 1
    used.add(candidate.casefold())
    return candidate


def excel_value(value: Any, table: str, row_number: int, column: str) -> Any:
    if isinstance(value, (dict, list)):
        value = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    elif isinstance(value, bytes):
        value = base64.b64encode(value).decode("ascii")
    elif isinstance(value, float) and not math.isfinite(value):
        value = str(value)
    if isinstance(value, str) and len(value) > EXCEL_MAX_CELL_CHARS:
        raise ConversionError(
            f"Excel limita cada celda a {EXCEL_MAX_CELL_CHARS:,} caracteres; "
            f"{table}, fila {row_number}, columna {column!r} supera ese límite"
        )
    return value


def convert(database: Path, destination: Path, stem: str) -> tuple[Path, Path, Path]:
    try:
        from openpyxl import Workbook
    except ImportError as exc:
        raise ConversionError("Para generar XLSX instala openpyxl: python -m pip install openpyxl") from exc

    destination.mkdir(parents=True, exist_ok=True)
    json_path = destination / f"{stem}.json"
    yaml_path = destination / f"{stem}.yml"
    xlsx_path = destination / f"{stem}.xlsx"
    json_temp = destination / f".{stem}.json.tmp"
    yaml_temp = destination / f".{stem}.yml.tmp"
    xlsx_temp = destination / f".{stem}.tmp.xlsx"
    workbook = Workbook(write_only=True)
    used_sheet_names: set[str] = set()

    try:
        with sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True) as connection, \
                json_temp.open("w", encoding="utf-8", newline="\n") as json_out, \
                yaml_temp.open("w", encoding="utf-8", newline="\n") as yaml_out:
            names = table_names(connection)
            json_out.write('{"tables":{')
            yaml_out.write("# YAML 1.2; each row uses JSON-compatible flow syntax.\ntables:\n")
            first_table = True

            for table in names:
                columns = [row[1] for row in connection.execute(f"PRAGMA table_info({quote_identifier(table)})")]
                if not columns:
                    continue
                if not first_table:
                    json_out.write(",")
                first_table = False
                json_out.write(json.dumps(table, ensure_ascii=False) + ":[")
                yaml_out.write(f"  {json.dumps(table, ensure_ascii=False)}:\n")

                sheet_part = 1
                sheet = workbook.create_sheet(excel_sheet_name(table, used_sheet_names, sheet_part))
                sheet.append(columns)
                rows_in_sheet = 1
                row_number = 0
                cursor = connection.execute(f"SELECT * FROM {quote_identifier(table)}")
                for row in cursor:
                    row_number += 1
                    record = {
                        column: json_value(value, column)
                        for column, value in zip(columns, row, strict=True)
                    }
                    encoded = json.dumps(record, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
                    if row_number > 1:
                        json_out.write(",")
                    json_out.write(encoded)
                    yaml_out.write("    - " + encoded + "\n")

                    if rows_in_sheet >= EXCEL_MAX_ROWS:
                        sheet_part += 1
                        sheet = workbook.create_sheet(excel_sheet_name(table, used_sheet_names, sheet_part))
                        sheet.append(columns)
                        rows_in_sheet = 1
                    excel_row = [
                        excel_value(json_value(value, column), table, row_number, column)
                        for column, value in zip(columns, row, strict=True)
                    ]
                    sheet.append(excel_row)
                    rows_in_sheet += 1

                if row_number == 0:
                    yaml_out.write("    []\n")
                json_out.write("]")
            json_out.write("}}\n")

        if not names:
            workbook.create_sheet("empty")
        workbook.save(xlsx_temp)
        os.replace(json_temp, json_path)
        os.replace(yaml_temp, yaml_path)
        os.replace(xlsx_temp, xlsx_path)
        return json_path, yaml_path, xlsx_path
    except Exception:
        workbook.close()
        for temporary in (json_temp, yaml_temp, xlsx_temp):
            temporary.unlink(missing_ok=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convierte una base SQLite .db o .db.zst a JSON, YAML y Excel (.xlsx)"
    )
    parser.add_argument("database", type=Path, help="Archivo .db o .db.zst que se convertirá")
    parser.add_argument("--output-dir", type=Path, help="Carpeta de salida (por defecto, junto al archivo de entrada)")
    args = parser.parse_args()

    source = args.database.expanduser().resolve()
    if not source.is_file():
        print(f"No existe el archivo: {source}", file=sys.stderr)
        return 2
    destination = (args.output_dir.expanduser().resolve() if args.output_dir else source.parent)
    stem = output_stem(source)
    decompressed = None
    try:
        destination.mkdir(parents=True, exist_ok=True)
        database, decompressed = open_database(source, destination)
        outputs = convert(database, destination, stem)
        for output in outputs:
            print(f"Creado: {output}")
        return 0
    except (ConversionError, OSError, sqlite3.Error, ValueError) as exc:
        print(f"Conversión fallida: {exc}", file=sys.stderr)
        return 1
    finally:
        if decompressed is not None:
            decompressed.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
