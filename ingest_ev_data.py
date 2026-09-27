import json
import requests
import pandas as pd
import duckdb
import os
from dotenv import load_dotenv

# Search for the .env file and load the variables into the session
load_dotenv()

# The rest of your script stays exactly the same
api_key = os.environ.get('LTA_API_KEY')

if not api_key:
    raise ValueError("LTA_API_KEY environment variable not set. Check your .env file.")

print("Key loaded successfully!")
def safe_float(val):
    try:
        return float(val) if pd.notna(val) and str(val).strip() != "" else None
    except (ValueError, TypeError):
        return None

# 1. Fetch S3 Download Link
api_key = os.environ.get('LTA_API_KEY')
if not api_key:
    raise ValueError("LTA_API_KEY environment variable not set.")

lta_api_url = "https://datamall2.mytransport.sg/ltaodataservice/EVCBatch"
headers = {'AccountKey': api_key, 'accept': 'application/json'}

response = requests.get(lta_api_url, headers=headers)
response.raise_for_status()
data = response.json()

s3_url = data['Link'] if 'Link' in data else data['value'][0]['Link']

# 2. Download and Parse JSON Payload
s3_response = requests.get(s3_url)
s3_response.raise_for_status()
ev_payload = s3_response.json()
records = ev_payload.get('value', ev_payload)

df_raw = pd.DataFrame(records)

# 3. Flatten Nested Structure
rows = []
for index, row in df_raw.iterrows():
    updated_at = row['LastUpdatedTime']
    loc = row['evLocationsData']

    if isinstance(loc, str):
        try:
            loc = json.loads(loc)
        except json.JSONDecodeError:
            continue

    address = loc.get('address')
    postal = loc.get('postalCode')
    lat = loc.get('latitude')
    lon = loc.get('longtitude')

    for cp in loc.get('chargingPoints', []):
        operator = cp.get('operator')
        position = cp.get('position')

        for pt in cp.get('plugTypes', []):
            plug_type = pt.get('plugType')
            current_type = pt.get('current')
            power_rating = pt.get('powerRating')
            price = pt.get('price')

            for ev in pt.get('evIds', []):
                charger_id = ev.get('evCpId')
                status = ev.get('status')

                rows.append({
                    'snapshot_time': updated_at,
                    'address': address,
                    'postal_code': postal,
                    'latitude': safe_float(lat),
                    'longitude': safe_float(lon),
                    'operator': operator,
                    'position': position,
                    'plug_type': plug_type,
                    'current_type': current_type,
                    'power_rating_kw': safe_float(power_rating),
                    'price_per_kwh': safe_float(price),
                    'charger_id': charger_id,
                    'current_status': status
                })

df_flat = pd.DataFrame(rows)

# 4. Append to DuckDB
con = duckdb.connect('ev_pipeline.duckdb')
con.execute("""
CREATE TABLE IF NOT EXISTS raw_charger_snapshots AS 
SELECT * FROM df_flat WHERE 1=0;
""")
con.execute("INSERT INTO raw_charger_snapshots SELECT * FROM df_flat;")

total_count = con.execute("SELECT COUNT(*) FROM raw_charger_snapshots;").fetchone()[0]
print(f"Successfully ingested snapshot. Total records in DuckDB: {total_count}")
con.close()