import io
import zipfile
import urllib.request
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import json
import os
import time
import requests
from bs4 import BeautifulSoup

URL_RBQ = (
    "https://www.donneesquebec.ca/recherche/dataset/"
    "755b45d6-7aee-46df-a216-748a0191c79f/resource/"
    "32f6ec46-85fd-45e9-945b-965d9235840a/download/"
    "rdl01_extractiondonneesouvertes.zip"
)
SHEET_ID = "1S705pc9MDjlDjhnvP4OhBu-J48DQg9P9jJk2TujtmDo"
ONGLET   = "Licences RBQ"

CODES_FILTRES = [
    "15.1", "15.1.1", "15.2", "15.2.1",
    "15.3", "15.3.1", "15.7", "15.8",
    "15.9", "15.10", "16"
]

def scraper_fiche(numero_licence):
    numero_clean = numero_licence.replace("-", "")
    url = f"https://www.pes.rbq.gouv.qc.ca/RegistreLicences/FicheDetenteur/{numero_clean}?mode=RegionTypeTravaux"
    
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return {"URL Fiche RBQ": url}
        
        soup = BeautifulSoup(resp.text, "html.parser")
        data = {"URL Fiche RBQ": url}

        # Chercher les champs dans les définitions
        dts = soup.find_all("dt")
        dds = soup.find_all("dd")
        for dt, dd in zip(dts, dds):
            label = dt.get_text(strip=True)
            valeur = dd.get_text(" ", strip=True)
            
            if "Courriel" in label:
                data["Courriel"] = valeur
            elif "Téléphone" in label:
                data["Téléphone"] = valeur
            elif "Autre" in label:
                data["Autres noms"] = valeur
            elif "paiement" in label.lower():
                data["Date paiement annuel"] = valeur
            elif "Montant" in label:
                data["Montant cautionnement"] = valeur
            elif "Réclamation" in label:
                data["Réclamations cautionnement"] = valeur

        # Chercher les répondants
        repondants = []
        tags = soup.find_all(["h3", "h4", "p", "div"])
        for tag in tags:
            texte = tag.get_text(strip=True)
            if "Répondant" in texte and len(texte) < 100:
                nom = texte.replace("Répondant", "").strip()
                if nom and nom not in repondants:
                    repondants.append(nom)

        for i, rep in enumerate(repondants[:3], 1):
            data[f"Répondant {i}"] = rep

        return data

    except Exception:
        return {"URL Fiche RBQ": url}

# ── Téléchargement ──────────────────────────────────────────
print("📥 Téléchargement des données RBQ...")
with urllib.request.urlopen(URL_RBQ) as r:
    zip_data = r.read()

with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
    with z.open(z.namelist()[0]) as f:
        df = pd.read_csv(f, encoding="utf-8-sig", low_memory=False)

print(f"✅ {len(df):,} licences téléchargées")

# ── Filtrage ─────────────────────────────────────────────────
print("🔍 Filtrage des sous-catégories...")
pattern = "|".join([c.replace(".", "\\.") for c in CODES_FILTRES])
masque = df["Sous-catégories"].astype(str).str.contains(pattern, na=False, regex=True)
df_filtre = df[masque].copy().reset_index(drop=True)
print(f"✅ {len(df_filtre):,} entrepreneurs trouvés")

# ── Scraping ─────────────────────────────────────────────────
print("🌐 Scraping des fiches RBQ...")
extras = []
total = len(df_filtre)

for idx, row in df_filtre.iterrows():
    numero = str(row["Numéro de licence"])
    info = scraper_fiche(numero)
    extras.append(info)
    
    if (idx + 1) % 500 == 0:
        print(f"  → {idx+1:,} / {total:,} fiches scrapées")
    
    time.sleep(0.5)

df_extra = pd.DataFrame(extras)
df_final = pd.concat([df_filtre, df_extra], axis=1)
print(f"✅ Scraping terminé !")

# ── Google Sheets ─────────────────────────────────────────────
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

print("📤 Envoi vers Google Sheets...")
data = [df_final.columns.tolist()] + df_final.fillna("").values.tolist()

batch = 5000
for i in range(0, len(data), batch):
    ws.append_rows(data[i:i+batch], value_input_option="RAW")
    print(f"  → {min(i+batch, len(data)):,} / {len(data):,} lignes écrites")
    time.sleep(2)

print(f"🎉 Terminé! {len(df_final):,} entrepreneurs dans Google Sheets")
