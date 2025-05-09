#!/usr/bin/env python3
"""
Descarga registros de DynamoDB dentro de un rango de fechas (o todos).

Requisitos:
    pip install boto3

Uso:
    python dynamo_query.py --table QR_TRANSACTION               # tabla completa
    python dynamo_query.py --table MiTabla --start-date 2023-01-01 --end-date 2023-01-31
    python dynamo_query.py --table MiTabla --stdout      # imprime por pantalla
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Attr


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Descarga registros de DynamoDB.")
    parser.add_argument("--table", required=True, help="Nombre de la tabla DynamoDB.")
    parser.add_argument("--start-date", help="Fecha inicio (YYYY-MM-DD o ISO 8601).")
    parser.add_argument("--end-date", help="Fecha fin    (YYYY-MM-DD o ISO 8601).")
    parser.add_argument(
        "--date-attr",
        default="created_at",
        help="Nombre del atributo fecha en DynamoDB (default: created_at).",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Si se indica, imprime el JSON en stdout en lugar de guardarlo en archivo.",
    )
    parser.add_argument(
        "--profile",
        help="Perfil de AWS a usar (opcional). Si no se indica se toma el default.",
    )
    parser.add_argument(
        "--region",
        help="Región AWS (opcional, anula la configuración por defecto).",
    )
    return parser.parse_args()


def iso_to_timestamp_ms(date_str: str) -> int:
    """
    Convierte una fecha en formato ISO 8601 o YYYY-MM-DD a epoch en milisegundos (int).
    DynamoDB suele guardar timestamps como números (ms/seg) o strings ISO.
    Ajusta este método según tu diseño de tabla.
    """
    try:
        dt = datetime.fromisoformat(date_str)
    except ValueError as exc:
        raise ValueError(f"Fecha inválida: {date_str}") from exc

    # Si la fecha no trae zona, asumimos UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    ts_ms = int(dt.timestamp() * 1000)
    return ts_ms


def decimal_to_builtin(obj: Any) -> Any:
    """
    Converte los objetos Decimal que devuelve boto3 a float/int para poder serializar a JSON.
    """
    if isinstance(obj, list):
        return [decimal_to_builtin(i) for i in obj]
    if isinstance(obj, dict):
        return {k: decimal_to_builtin(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        # Si el número es entero (10, 45, etc) conservamos int
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    return obj


def build_dynamodb_resource(profile: Optional[str], region: Optional[str]):
    session_kwargs = {}
    if profile:
        session_kwargs["profile_name"] = profile
    session = boto3.Session(**session_kwargs)
    return session.resource("dynamodb", region_name=region) if region else session.resource("dynamodb")


def scan_table(
    table_name: str,
    date_attr: str,
    start_ts_ms: Optional[int],
    end_ts_ms: Optional[int],
    dynamodb_resource,
) -> List[Dict[str, Any]]:
    """
    Hace un Scan con o sin FilterExpression usando paginación.
    start_ts_ms y end_ts_ms son timestamps en milisegundos o None.
    """
    table = dynamodb_resource.Table(table_name)

    scan_kwargs = {"ConsistentRead": False}  # cambia a True si lo necesitas y tu tabla lo permite

    if start_ts_ms is not None and end_ts_ms is not None:
        scan_kwargs["FilterExpression"] = Attr(date_attr).between(start_ts_ms, end_ts_ms)
    elif start_ts_ms is not None:
        scan_kwargs["FilterExpression"] = Attr(date_attr).gte(start_ts_ms)
    elif end_ts_ms is not None:
        scan_kwargs["FilterExpression"] = Attr(date_attr).lte(end_ts_ms)

    items: List[Dict[str, Any]] = []
    done = False
    last_evaluated_key = None

    while not done:
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        response = table.scan(**scan_kwargs)
        items.extend(response.get("Items", []))
        last_evaluated_key = response.get("LastEvaluatedKey")
        done = last_evaluated_key is None

    return items


def main():
    args = parse_arguments()

    # Parseo de fechas
    start_ts = iso_to_timestamp_ms(args.start_date) if args.start_date else None
    end_ts = iso_to_timestamp_ms(args.end_date) if args.end_date else None

    # Conexión a DynamoDB
    dynamodb = build_dynamodb_resource(args.profile, args.region)

    try:
        data = scan_table(
            table_name=args.table,
            date_attr=args.date_attr,
            start_ts_ms=start_ts,
            end_ts_ms=end_ts,
            dynamodb_resource=dynamodb,
        )
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        print(f"❌  La tabla «{args.table}» no existe en la cuenta/región especificada.", file=sys.stderr)
        sys.exit(1)

    # Conversión Decimal → float/int para que el JSON sea serializable
    data_clean = decimal_to_builtin(data)

    if args.stdout:
        print(json.dumps(data_clean, ensure_ascii=False, indent=2))
    else:
        outfile = f"{args.table}.json"
        with open(outfile, "w", encoding="utf-8") as f:
            json.dump(data_clean, f, ensure_ascii=False, indent=2)
        print(f"✅  {len(data_clean)} ítems escritos en {outfile}")


if __name__ == "__main__":
    main()