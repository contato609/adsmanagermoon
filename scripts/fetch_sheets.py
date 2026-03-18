"""
Lê dados de campanhas do Google Sheets e gera data.json.
Requer variável de ambiente: GOOGLE_SERVICE_ACCOUNT_KEY (JSON da service account)
Opcional: SHEET_ID, SHEET_RANGE
"""

import json
import os
import sys
from datetime import datetime, timezone

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ── Config ────────────────────────────────────────────────────────────────────
SHEET_ID    = os.environ.get('SHEET_ID', '1EZfXPVkZFECR4apLqiS1HFYUgRYytEkR9rdEIDM1h-o')
SHEET_RANGE = os.environ.get('SHEET_RANGE', 'Sheet1')
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), '..', 'data.json')

NUMERIC_HINTS = {
    'gasto', 'spend', 'custo', 'cost',
    'impressoes', 'impressões', 'impressions',
    'cliques', 'clicks',
    'conversoes', 'conversões', 'conversions', 'resultados',
    'ctr', 'cpc', 'cpm', 'cpa', 'roas', 'frequencia', 'frequência',
    'alcance', 'reach', 'budget', 'orcamento', 'orçamento',
}

# ── Auth ──────────────────────────────────────────────────────────────────────
def build_service():
    key_json = os.environ.get('GOOGLE_SERVICE_ACCOUNT_KEY')
    if not key_json:
        print('ERROR: GOOGLE_SERVICE_ACCOUNT_KEY not set.', file=sys.stderr)
        sys.exit(1)

    key_data = json.loads(key_json)
    creds    = service_account.Credentials.from_service_account_info(
        key_data,
        scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'],
    )
    return build('sheets', 'v4', credentials=creds, cache_discovery=False)

# ── Fetch ─────────────────────────────────────────────────────────────────────
def fetch_values(service):
    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=SHEET_ID, range=SHEET_RANGE)
            .execute()
        )
        return result.get('values', [])
    except HttpError as e:
        print(f'ERROR fetching sheet: {e}', file=sys.stderr)
        sys.exit(1)

# ── Parse ─────────────────────────────────────────────────────────────────────
def coerce(header, value):
    """Try to parse numeric columns as float."""
    if header.strip().lower() in NUMERIC_HINTS:
        try:
            # Handle Brazilian number format (1.234,56 → 1234.56)
            cleaned = value.replace('.', '').replace(',', '.')
            return float(cleaned)
        except (ValueError, AttributeError):
            pass
    return value

def rows_to_records(values):
    if not values:
        return [], []

    headers = [h.strip() for h in values[0]]
    records = []
    for row in values[1:]:
        record = {
            headers[i]: coerce(headers[i], row[i]) if i < len(row) else ''
            for i in range(len(headers))
        }
        records.append(record)
    return headers, records

# ── Write ─────────────────────────────────────────────────────────────────────
def write_output(headers, records):
    output = {
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'sheet_id':   SHEET_ID,
        'range':      SHEET_RANGE,
        'headers':    headers,
        'rows':       records,
    }
    path = os.path.abspath(OUTPUT_FILE)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f'OK: {len(records)} rows written to {path}')

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    service          = build_service()
    values           = fetch_values(service)
    headers, records = rows_to_records(values)
    write_output(headers, records)
