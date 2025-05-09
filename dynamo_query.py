#!/usr/bin/env python3
"""
Exporta una tabla DynamoDB a CSV con filtro de fechas y orden configurable.
Requisitos:
    pip install boto3

Ejemplos
  python download_dynamodb_to_csv.py --table MiTabla --order asc
  python dynamo_query.py --table QR_TRANSACTION --order desc --sort-by created_date
  python dynamo_query.py --table QR_TRANSACTION --order desc --sort-by created_date
  python dynamo_query.py --table QR_CUSTOMER --order desc --sort-by created_date
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
                   help="Atributo fecha usado en el filtro (def: created_at).")
    p.add_argument("--stdout", action="store_true",
                   help="Imprime CSV en stdout en vez de archivo.")
    p.add_argument("--profile", help="Perfil AWS.")
    p.add_argument("--region", help="Región AWS.")
    p.add_argument("--delimiter", default=",", help="Delimitador CSV (def: ,).")

    # Ordenación
    p.add_argument("--sort-by", default="created_date",
                   help="Atributo por el que ordenar (def: created_date).")
    p.add_argument("--order", choices=["asc", "desc"], default="asc",
                   help="Orden asc o desc (def: asc).")
    return p.parse_args()


# ------------------------- Utilidades ------------------------- #
def iso_to_timestamp_ms(date_str: str) -> int:
    dt = datetime.fromisoformat(date_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def value_as_sort_key(val: Any) -> Any:
    """
    Devuelve SIEMPRE una tupla (tipo, valor) para que la comparación
    sea consistente y no se mezclen tipos incompatibles.
        tipo 0: número/epoch
        tipo 1: string
        tipo 2: None
    """
    # None al final (tipo 2)
    if val is None:
        return (2, None)

    # Decimal ➜ numérico
    if isinstance(val, Decimal):
        val = int(val) if val % 1 == 0 else float(val)

    # Numérico (int/float)  ➜ tipo 0
    if isinstance(val, (int, float)):
        return (0, val)

    # String: fecha ISO o número
    if isinstance(val, str):
        # fecha ISO
        try:
            dt = datetime.fromisoformat(val)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return (0, dt.timestamp())        # tipo 0  ➜ numérico
        except ValueError:
            pass
        # número
        try:
            return (0, float(val))            # tipo 0  ➜ numérico
        except ValueError:
            pass
        # cualquier otro string ➜ tipo 1
        return (1, val)

    # Último recurso: serializar a string (tipo 1)
    return (1, str(val))


def to_scalar(val: Any) -> Any:
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

    start_ts = iso_to_timestamp_ms(args.start_date) if args.start_date else None
    end_ts   = iso_to_timestamp_ms(args.end_date)   if args.end_date   else None

    dynamodb = get_dynamodb_resource(args.profile, args.region)

    try:
        items = scan_table(args.table, args.date_attr, start_ts, end_ts, dynamodb)
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        print(f"❌  La tabla «{args.table}» no existe.", file=sys.stderr)
        sys.exit(1)

    # Ordenar
    reverse = args.order == "desc"
    #items.sort(key=lambda it: value_as_sort_key(it.get(args.sort_by)), reverse=reverse)
    items.sort(key=lambda it: value_as_sort_key(it.get(args.sort_by)), reverse=reverse)

    # CSV
    headers = collect_headers(items)
    if args.stdout:
        write_csv(items, headers, args.delimiter, sys.stdout)
    else:
        outfile = f"{args.table}.csv"
        with open(outfile, "w", newline='', encoding="utf-8") as f:
            write_csv(items, headers, args.delimiter, f)
        print(f"✅  {len(items)} ítems exportados en {outfile} (orden {args.order}).")


if __name__ == "__main__":
    main()