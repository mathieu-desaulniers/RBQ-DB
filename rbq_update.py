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
            return {"URL Fiche RBQ": url, "Réclamations cautionnement": "", "Répondant 1": "", "Répondant 2": "", "Répondant 3": ""}
        
        soup = BeautifulSoup(resp.text, "html.parser")
        data = {"URL Fiche RBQ": url}

        # Chercher seulement les réclamations
        dts = soup.find_all("dt")
        dds = soup.find_all("dd")
        for dt, dd in zip(dts, dds):
            label = dt.get_text(strip=True)
            valeur = dd.get_text(" ", strip=True)
            if "Réclamation" in label:
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

        # S'assurer que toutes les colonnes existent
        for col in ["Réclamations cautionnement", "Répondant 1", "Répondant 2", "Répondant 3"]:
            if col not in data:
                data[col] = ""

        return data

    except Exception:
        return {"URL Fiche RBQ": url, "Réclamations cautionnement": "", "Répondant 1": "", "Répondant 2": "", "Répondant 3": ""}

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

# ── Connexion Google Sheets ───────────────────────────────────
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

# ── Détecter les nouveaux ─────────────────────────────────────
print("🔍 Détection des nouveaux entrepreneurs...")
donnees_existantes = ws.get_all_values()

if len(donnees_existantes) > 1:
    licences_existantes = set(
        row[0] for row in donnees_existantes[1:] if row
    )
else:
    licences_existantes = set()

print(f"  → {len(licences_existantes):,} entrepreneurs déjà dans Google Sheets")

# Trouver les nouveaux
nouveaux = df_filtre[
    ~df_filtre["Numéro de licence"].astype(str).isin(licences_existantes)
].copy().reset_index(drop=True)

print(f"  → {len(nouveaux):,} nouveaux entrepreneurs trouvés")

if len(nouveaux) == 0:
    print("✅ Aucun nouveau entrepreneur, rien à faire !")
else:
    # ── Scraper seulement les nouveaux ───────────────────────
    print(f"🌐 Scraping de {len(nouveaux):,} nouveaux entrepreneurs...")
    extras = []
    for idx, row in nouveaux.iterrows():
        numero = str(row["Numéro de licence"])
        info = scraper_fiche(numero)
        extras.append(info)
        
        if (idx + 1) % 50 == 0:
            print(f"  → {idx+1:,} / {len(nouveaux):,} fiches scrapées")
        
        time.sleep(0.5)

    df_extra = pd.DataFrame(extras)
    df_nouveaux = pd.concat([nouveaux, df_extra], axis=1)

    # ── Ajouter dans Google Sheets ────────────────────────────
    print("📤 Ajout des nouveaux dans Google Sheets...")

    if not licences_existantes:
        # Première fois — écrire les entêtes et les données
        data = [df_nouveaux.columns.tolist()] + df_nouveaux.fillna("").values.tolist()
        ws.clear()
        batch = 5000
        for i in range(0, len(data), batch):
            ws.append_rows(data[i:i+batch], value_input_option="RAW")
            print(f"  → {min(i+batch, len(data)):,} / {len(data):,} lignes écrites")
            time.sleep(2)
    else:
        # Ajouter seulement les nouvelles lignes
        nouvelles_lignes = df_nouveaux.fillna("").values.tolist()
        batch = 5000
        for i in range(0, len(nouvelles_lignes), batch):
            ws.append_rows(nouvelles_lignes[i:i+batch], value_input_option="RAW")
            print(f"  → {min(i+batch, len(nouvelles_lignes)):,} / {len(nouvelles_lignes):,} lignes écrites")
            time.sleep(2)

    print(f"🎉 Terminé! {len(nouveaux):,} nouveaux entrepreneurs ajoutés!")
