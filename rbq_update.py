import io
import zipfile
import urllib.request
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import json
import os

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
URL_RBQ = (
    "https://www.donneesquebec.ca/recherche/dataset/"
    "755b45d6-7aee-46df-a216-748a0191c79f/resource/"
    "32f6ec46-85fd-45e9-945b-965d9235840a/download/"
    "rdl01_extractiondonneesouvertes.zip"
)
SHEET_ID = "1S705pc9MDjlDjhnvP4OhBu-J48DQg9P9jJk2TujtmDo"
ONGLET   = "Licences RBQ"

# ─────────────────────────────────────────────
# 1. TÉLÉCHARGEMENT RBQ
# ─────────────────────────────────────────────
print("📥 Téléchargement des données RBQ...")
with urllib.request.urlopen(URL_RBQ) as r:
    zip_data = r.read()

with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
    with z.open(z.namelist()[0]) as f:
        df = pd.read_csv(f, encoding="utf-8-sig", low_memory=False)

print(f"✅ {len(df):,} licences téléchargées")

# ─────────────────────────────────────────────
# 2. CONNEXION GOOGLE SHEETS
# ─────────────────────────────────────────────
print("🔗 Connexion à Google Sheets...")
creds_json = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = Credentials.from_service_account_info(
    creds_json,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
client = gspread.authorize(creds)
sheet  = client.open_by_key(SHEET_ID)

# ─────────────────────────────────────────────
# 3. MISE À JOUR GOOGLE SHEETS
# ─────────────────────────────────────────────
print("📤 Mise à jour Google Sheets...")
try:
    ws = sheet.worksheet(ONGLET)
except:
    ws = sheet.add_worksheet(ONGLET, rows=1, cols=1)

ws.clear()

# Écrire les entêtes + données
data = [df.columns.tolist()] + df.fillna("").values.tolist()

# Écrire par batch de 5000 lignes (limite Google)
batch = 5000
for i in range(0, len(data), batch):
    ws.append_rows(data[i:i+batch], value_input_option="RAW")
    print(f"  → {min(i+batch, len(data)):,} / {len(data):,} lignes écrites")

print(f"🎉 Terminé! {len(df):,} licences dans Google Sheets")
