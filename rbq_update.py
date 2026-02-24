import io
import zipfile
import urllib.request
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import json
import os

URL_RBQ = (
    "https://www.donneesquebec.ca/recherche/dataset/"
    "755b45d6-7aee-46df-a216-748a0191c79f/resource/"
    "32f6ec46-85fd-45e9-945b-965d9235840a/download/"
    "rdl01_extractiondonneesouvertes.zip"
)
SHEET_ID = "1S705pc9MDjlDjhnvP4OhBu-J48DQg9P9jJk2TujtmDo"
ONGLET   = "Licences RBQ"

# Sous-catégories à inclure
CODES_FILTRES = [
    "15.1", "15.1.1", "15.2", "15.2.1",
    "15.3", "15.3.1", "15.7", "15.8",
    "15.9", "15.10", "16"
]

print("📥 Téléchargement des données RBQ...")
with urllib.request.urlopen(URL_RBQ) as r:
    zip_data = r.read()

with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
    with z.open(z.namelist()[0]) as f:
        df = pd.read_csv(f, encoding="utf-8-sig", low_memory=False)

print(f"✅ {len(df):,} licences téléchargées")

# Filtrer par sous-catégories
print("🔍 Filtrage des sous-catégories...")
pattern = "|".join([c.replace(".", "\\.") for c in CODES_FILTRES])
masque = df["Sous-catégories"].astype(str).str.contains(pattern, na=False, regex=True)
df_filtre = df[masque].copy()
print(f"✅ {len(df_filtre):,} entrepreneurs trouvés")

# Connexion Google Sheets
print("🔗 Connexion à Google Sheets...")
creds_json = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = Credentials.from_service_account_info(
    creds_json,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
client = gspread.authorize(creds)
sheet  = client.open_by_key(SHEET_ID)

try:
    ws = sheet.worksheet(ONGLET)
except:
    ws = sheet.add_worksheet(ONGLET, rows=1, cols=1)

ws.clear()

# Écrire les données
print("📤 Envoi vers Google Sheets...")
data = [df_filtre.columns.tolist()] + df_filtre.fillna("").values.tolist()

batch = 5000
for i in range(0, len(data), batch):
    ws.append_rows(data[i:i+batch], value_input_option="RAW")
    print(f"  → {min(i+batch, len(data)):,} / {len(data):,} lignes écrites")

print(f"🎉 Terminé! {len(df_filtre):,} entrepreneurs dans Google Sheets")
