import io
import zipfile
import urllib.request
import pandas as pd
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

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

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
            return {"url_fiche_rbq": url, "reclamations_cautionnement": "", "repondant_1": "", "repondant_2": "", "repondant_3": ""}
        
        soup = BeautifulSoup(resp.text, "html.parser")
        data = {"url_fiche_rbq": url}

        dts = soup.find_all("dt")
        dds = soup.find_all("dd")
        for dt, dd in zip(dts, dds):
            label = dt.get_text(strip=True)
            valeur = dd.get_text(" ", strip=True)
            if "Réclamation" in label:
                data["reclamations_cautionnement"] = valeur

        repondants = []
        tags = soup.find_all(["h3", "h4", "p", "div"])
        for tag in tags:
            texte = tag.get_text(strip=True)
            if "Répondant" in texte and len(texte) < 100:
                nom = texte.replace("Répondant", "").strip()
                if nom and nom not in repondants:
                    repondants.append(nom)

        for i, rep in enumerate(repondants[:3], 1):
            data[f"repondant_{i}"] = rep

        for col in ["reclamations_cautionnement", "repondant_1", "repondant_2", "repondant_3"]:
            if col not in data:
                data[col] = ""

        return data

    except Exception:
        return {"url_fiche_rbq": url, "reclamations_cautionnement": "", "repondant_1": "", "repondant_2": "", "repondant_3": ""}

def envoyer_supabase(rows, tentative=1):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal"
    }
    url = f"{SUPABASE_URL}/rest/v1/licences_rbq?on_conflict=numero_licence"
    
    try:
        resp = requests.post(url, headers=headers, json=rows, timeout=30)
        if resp.status_code not in [200, 201, 204]:
            print(f"  ⚠️ Erreur Supabase: {resp.status_code} - {resp.text[:200]}")
            if tentative < 3:
                print(f"  🔄 Nouvelle tentative {tentative+1}/3...")
                time.sleep(5)
                return envoyer_supabase(rows, tentative + 1)
        return resp.status_code
    except Exception as e:
        print(f"  ❌ Exception: {e}")
        if tentative < 3:
            time.sleep(5)
            return envoyer_supabase(rows, tentative + 1)

# ── Téléchargement ──────────────────────────────────────────
print("📥 Téléchargement des données RBQ...")
with urllib.request.urlopen(URL_RBQ) as r:
    zip_data = r.read()

with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
    with z.open(z.namelist()[0]) as f:
        df = pd.read_csv(f, encoding="utf-8-sig", low_memory=False)

print(f"✅ {len(df):,} licences téléchargées")
print(f"  → {len(df.columns)} colonnes détectées: {list(df.columns)}")

# ── Filtrage ─────────────────────────────────────────────────
print("🔍 Filtrage des sous-catégories...")
pattern = "|".join([c.replace(".", "\\.") for c in CODES_FILTRES])
masque = df["Sous-catégories"].astype(str).str.contains(pattern, na=False, regex=True)
df_filtre = df[masque].copy().reset_index(drop=True)
print(f"✅ {len(df_filtre):,} entrepreneurs trouvés")

# ── Renommer les colonnes pour Supabase ──────────────────────
noms_colonnes = [
    "numero_licence", "statut_licence", "type_licence", "date_delivrance",
    "restriction", "date_debut_restriction", "date_fin_restriction",
    "association_cautionnement", "montant_caution", "date_paiement_annuel",
    "mandataire", "courriel", "adresse", "neq", "nom_intervenant",
    "numero_telephone", "municipalite", "statut_juridique", "code_region",
    "region_administrative", "nombre_sous_categories", "categorie",
    "sous_categories", "autre_nom"
]

if len(df_filtre.columns) != len(noms_colonnes):
    print(f"⚠️ Nombre de colonnes inattendu: {len(df_filtre.columns)} au lieu de {len(noms_colonnes)}")
    print(f"  → Colonnes originales: {list(df_filtre.columns)}")
else:
    df_filtre.columns = noms_colonnes
    print(f"✅ Colonnes renommées avec succès")

# ── Scraping + envoi vers Supabase ───────────────────────────
print("🌐 Scraping et envoi vers Supabase...")
batch_rows = []
total = len(df_filtre)

for idx, row in df_filtre.iterrows():
    numero = str(row["numero_licence"])
    info = scraper_fiche(numero)
    
    ligne = row.to_dict()
    ligne.update(info)
    ligne = {k: ("" if pd.isna(v) else str(v)) for k, v in ligne.items()}
    batch_rows.append(ligne)
    
    if len(batch_rows) == 500:
        envoyer_supabase(batch_rows)
        print(f"  → {idx+1:,} / {total:,} fiches traitées")
        batch_rows = []
        time.sleep(0.5)

# Envoyer le reste
if batch_rows:
    envoyer_supabase(batch_rows)
    print(f"  → {total:,} / {total:,} fiches traitées")

print(f"🎉 Terminé! {total:,} entrepreneurs dans Supabase!")
