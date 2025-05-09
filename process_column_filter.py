#!/usr/bin/env python3
"""
process_column_filter.py

Script para filtrar columnas de un archivo CSV.

Ejemplo de uso:
    python process_column_filter.py \
        --input  QR_TRANSACTION.csv \
        --output datos_filtrados.csv \
        --columns document_number trx_id created_date status qr_type amount

    # O con columnas en una sola cadena separadas por coma
    python process_column_filter.py -i QR_TRANSACTION.csv -o datos_filtrados.csv -c "document_number,trx_id,created_date,status,qr_type,amount"

Parámetros opcionales:
    --ignore-missing  → si se pasa, no fallará cuando falten columnas,
                        simplemente las omite y avisa por stderr.
"""

import argparse
import csv
import os
import sys
from typing import List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filtra columnas específicas de un archivo CSV."
    )
    parser.add_argument(
        "-i", "--input", required=True, help="Ruta del archivo CSV de entrada"
    )
    parser.add_argument(
        "-o", "--output", required=True, help="Ruta del archivo CSV de salida"
    )
    parser.add_argument(
        "-c",
        "--columns",
        nargs="+",
        required=True,
        help=(
            "Columnas a conservar. "
            "Puede pasarse una lista separada por espacios o una única "
            "cadena con columnas separadas por coma."
        ),
    )
    parser.add_argument(
        "--ignore-missing",
        action="store_true",
        help="Si se indica, las columnas que no existan se ignoran en vez de abortar.",
    )
    return parser.parse_args()


def normalize_columns(column_args: List[str]) -> List[str]:
    """
    Convierte ['a,b,c', 'd']  → ['a', 'b', 'c', 'd']
    """
    normalized: List[str] = []
    for arg in column_args:
        normalized.extend([col.strip() for col in arg.split(",") if col.strip()])
    # Eliminar duplicados preservando orden
    seen = set()
    unique_columns = []
    for col in normalized:
        if col not in seen:
            unique_columns.append(col)
            seen.add(col)
    return unique_columns


def filter_csv(
    in_path: str,
    out_path: str,
    columns_to_keep: List[str],
    ignore_missing: bool = False,
) -> None:
    if not os.path.isfile(in_path):
        raise FileNotFoundError(f"Archivo de entrada no encontrado: {in_path}")

    with open(in_path, newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        existing_cols = reader.fieldnames or []

        # Validación de columnas
        missing = [col for col in columns_to_keep if col not in existing_cols]
        if missing and not ignore_missing:
            raise ValueError(
                f"Las siguientes columnas no existen en el CSV de entrada:\n{missing}"
            )
        elif missing and ignore_missing:
            sys.stderr.write(
                f"[Advertencia] Se ignorarán columnas inexistentes: {missing}\n"
            )
            columns_to_keep = [c for c in columns_to_keep if c in existing_cols]

        if not columns_to_keep:
            raise ValueError("No quedan columnas válidas para exportar.")

        # Crear directorio de salida si no existe
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)

        with open(out_path, "w", newline="", encoding="utf-8") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=columns_to_keep)
            writer.writeheader()

            for row in reader:
                filtered_row = {col: row.get(col, "") for col in columns_to_keep}
                writer.writerow(filtered_row)


def main() -> None:
    args = parse_args()
    columns = normalize_columns(args.columns)

    try:
        filter_csv(
            in_path=args.input,
            out_path=args.output,
            columns_to_keep=columns,
            ignore_missing=args.ignore_missing,
        )
        print(f"Archivo generado correctamente en: {args.output}")
    except Exception as exc:
        sys.stderr.write(f"Error: {exc}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()