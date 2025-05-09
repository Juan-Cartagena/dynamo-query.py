#!/usr/bin/env python3
"""
Exporta una tabla DynamoDB a CSV con posibilidad de filtrar por fechas
y ordenar por un atributo.

Requisitos:
    pip install boto3

Ejemplos
  python dynamo_query.py --table QR_TRANSACTION --sort-by created_date
  python download_dynamodb_to_csv.py --table MiTabla --start-date 2023-01-01 \
         --end-date 2023-01-31 --sort-by created_date --desc
"""

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set

import boto3
from boto3.dynamodb.conditions import Attr


# ------------------------- Argumentos CLI ------------------------- #
def parse_arguments() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Exporta DynamoDB a CSV.")
    p.add_argument("--table", required=True, help="Nombre de la tabla DynamoDB.")
    p.add_argument("--start-date", help="Fecha inicio (YYYY-MM-DD o ISO 8601).")
    p.add_argument("--end-date", help="Fecha fin    (YYYY-MM-DD o ISO 8601).")
    p.add_argument("--date-attr", default="created_at",
                   help="Campo fecha usado en el filtro (def: created_at).")
    p.add_argument("--stdout", action="store_true",
                   help="Imprime el CSV en stdout en lugar de archivo.")
    p.add_argument("--profile", help="Perfil AWS.")
    p.add_argument("--region", help="Región AWS.")
    p.add_argument("--delimiter", default=",", help="Delimitador CSV (def: ,).")

    # ------------- NUEVO: ordenación ------------- #
    p.add_argument("--sort-by", default="created_date",
                   help="Nombre del atributo por el que ordenar (def: created_date).")
    p.add_argument("--desc", action="store_true",
                   help="Orden descendente si se indica.")
    return p.parse_args()


# ------------------------- Utilidades de fechas y tipos ------------------------- #
def iso_to_timestamp_ms(date_str: str) -> int:
    dt = datetime.fromisoformat(date_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def value_as_sort_key(val: Any) -> Any:
    """
    Convierte un valor a una clave comparable para la ordenación.
    Maneja tipos numéricos y strings ISO de fecha.
    """
    # Decimal → int / float
    if isinstance(val, Decimal):
        val = int(val) if val % 1 == 0 else float(val)

    # Ya es un número
    if isinstance(val, (int, float)):
        return val

    # Cadena → intentar ISO 8601
    if isinstance(val, str):
        try:
            dt = datetime.fromisoformat(val)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except ValueError:
            pass  # no era fecha

        # Probar si es número en string
        try:
            return float(val)
        except ValueError:
            pass

    # Fallback: ordenar por la representación de texto
    return str(val)


def to_scalar(val: Any) -> Any:
    """Prepara el valor para volcarlo en CSV."""
    if isinstance(val, Decimal):
        return int(val) if val % 1 == 0 else float(val)
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False)
    return val


def get_dynamodb_resource(profile: Optional[str], region: Optional[str]):
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    return session.resource("dynamodb", region_name=region) if region else session.resource("dynamodb")


# ------------------------- DynamoDB Scan ------------------------- #
def scan_table(
    table_name: str,
    date_attr: str,
    start_ts: Optional[int],
    end_ts: Optional[int],
    dynamodb
) -> List[Dict[str, Any]]:
    table = dynamodb.Table(table_name)
    scan_kwargs: Dict[str, Any] = {"ConsistentRead": False}

    if start_ts and end_ts:
        scan_kwargs["FilterExpression"] = Attr(date_attr).between(start_ts, end_ts)
    elif start_ts:
        scan_kwargs["FilterExpression"] = Attr(date_attr).gte(start_ts)
    elif end_ts:
        scan_kwargs["FilterExpression"] = Attr(date_attr).lte(end_ts)

    items: List[Dict[str, Any]] = []
    last_key = None
    while True:
        if last_key:
            scan_kwargs["ExclusiveStartKey"] = last_key
        resp = table.scan(**scan_kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
    return items


# ------------------------- CSV helpers ------------------------- #
def collect_headers(items: List[Dict[str, Any]]) -> List[str]:
    cols: Set[str] = set()
    for it in items:
        cols.update(it.keys())
    return sorted(cols)


def write_csv(items: List[Dict[str, Any]], headers: List[str],
              delimiter: str, fp):
    writer = csv.DictWriter(fp, fieldnames=headers,
                            delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    for it in items:
        row = {h: to_scalar(it.get(h, "")) for h in headers}
        writer.writerow(row)


# ------------------------- main ------------------------- #
def main() -> None:
    args = parse_arguments()

    # Parseo de rango de fechas para el filtro
    start_ts = iso_to_timestamp_ms(args.start_date) if args.start_date else None
    end_ts   = iso_to_timestamp_ms(args.end_date)   if args.end_date   else None

    dynamodb = get_dynamodb_resource(args.profile, args.region)

    try:
        items = scan_table(args.table, args.date_attr, start_ts, end_ts, dynamodb)
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        print(f"❌  La tabla «{args.table}» no existe.", file=sys.stderr)
        sys.exit(1)

    # ---------------- Ordenación ---------------- #
    sort_attr = args.sort_by
    items.sort(
        key=lambda it: value_as_sort_key(it.get(sort_attr)),
        reverse=args.desc
    )

    # ---------------- Escritura CSV ------------- #
    headers = collect_headers(items)
    if args.stdout:
        write_csv(items, headers, args.delimiter, sys.stdout)
    else:
        outfile = f"{args.table}.csv"
        with open(outfile, "w", newline='', encoding="utf-8") as f:
            write_csv(items, headers, args.delimiter, f)
        print(f"✅  {len(items)} ítems exportados y ordenados por «{sort_attr}» en {outfile}")


if __name__ == "__main__":
    main()