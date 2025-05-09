#!/usr/bin/env python3
"""
Descarga registros de DynamoDB y los guarda en CSV.

Requisitos:
    pip install boto3

Uso:
  python dynamo_query.py --table QR_TRANSACTION
  python dynamo_query.py --table MiTabla --start-date 2023-01-01 --end-date 2023-01-31
  python dynamo_query.py --table MiTabla --stdout
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
                   help="Campo fecha usado para filtrar (def: created_at).")
    p.add_argument("--stdout", action="store_true",
                   help="Imprime CSV en la salida estándar en vez de archivo.")
    p.add_argument("--profile", help="Perfil AWS (opcional).")
    p.add_argument("--region", help="Región AWS (opcional).")
    p.add_argument("--delimiter", default=",",
                   help="Delimitador CSV (def: ,).")
    return p.parse_args()


# ------------------------- Utilidades ------------------------- #
def iso_to_timestamp_ms(date_str: str) -> int:
    try:
        dt = datetime.fromisoformat(date_str)
    except ValueError as exc:
        raise ValueError(f"Fecha inválida: {date_str}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def to_scalar(value: Any) -> Any:
    """Convierte Decimals y estructuras a formatos serializables en CSV."""
    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else float(value)
    # Para listas / mapas anidados devolvemos JSON compactado
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def get_dynamodb_resource(profile: Optional[str], region: Optional[str]):
    session_kwargs = {"profile_name": profile} if profile else {}
    session = boto3.Session(**session_kwargs)
    return session.resource("dynamodb", region_name=region) if region else session.resource("dynamodb")


# ------------------------- DynamoDB Scan ------------------------- #
def scan_table(
    table_name: str,
    date_attr: str,
    start_ts: Optional[int],
    end_ts: Optional[int],
    dynamodb_resource
) -> List[Dict[str, Any]]:
    table = dynamodb_resource.Table(table_name)
    scan_kwargs: Dict[str, Any] = {"ConsistentRead": False}

    if start_ts is not None and end_ts is not None:
        scan_kwargs["FilterExpression"] = Attr(date_attr).between(start_ts, end_ts)
    elif start_ts is not None:
        scan_kwargs["FilterExpression"] = Attr(date_attr).gte(start_ts)
    elif end_ts is not None:
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
    """Devuelve la unión de todas las claves en orden alfabético."""
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
        data = scan_table(args.table, args.date_attr, start_ts, end_ts, dynamodb)
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        print(f"❌  La tabla «{args.table}» no existe.", file=sys.stderr)
        sys.exit(1)

    headers = collect_headers(data)

    if args.stdout:
        # Escritura en stdout
        write_csv(data, headers, args.delimiter, sys.stdout)
    else:
        outfile = f"{args.table}.csv"
        with open(outfile, "w", newline='', encoding="utf-8") as f:
            write_csv(data, headers, args.delimiter, f)
        print(f"✅  {len(data)} ítems exportados a {outfile}")


if __name__ == "__main__":
    main()