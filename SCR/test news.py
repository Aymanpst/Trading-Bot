"""
GDELT — Test de récupération de news S&P 500
Filtrage double : mots-clés pertinents + médias US uniquement
"""

import requests
import pandas as pd
import zipfile
import io
from datetime import datetime, timedelta
from typing import Optional

# ── Config ───────────────────────────────────
TEST_DATE = datetime.now() - timedelta(days=3)
MAX_ROWS  = 20

# Mots-clés ciblés S&P 500 et catalyseurs macro US
KEYWORDS = [
    # Indice direct
    "S&P 500", "S&P500", "Standard Poor's",
    # Politique monétaire — driver #1
    "Federal Reserve", "Fed rate", "Jerome Powell",
    "FOMC", "interest rate", "quantitative tightening", "quantitative easing",
    # Macro US
    "inflation", "CPI", "unemployment", "nonfarm payroll",
    "GDP", "recession", "debt ceiling",
    # Marchés US
    "Wall Street", "Dow Jones", "Nasdaq",
    "stock market crash", "market rally",
    # Risque systémique
    "banking crisis", "credit default", "sovereign debt",
    "VIX", "liquidity crisis",
    # Géopolitique à impact US
    "trade war", "tariff", "sanctions", "oil price",
]

# Médias US de référence — finance, économie, presse généraliste sérieuse
US_MEDIA = [
    # Presse financière spécialisée
    "wsj.com",            # Wall Street Journal — référence #1
    "bloomberg.com",      # Bloomberg — data + analyse
    "ft.com",             # Financial Times — perspective US/monde
    "marketwatch.com",    # MarketWatch — temps réel marchés
    "cnbc.com",           # CNBC — TV financière US
    "reuters.com",        # Reuters — agence de presse, neutre
    "apnews.com",         # Associated Press — agence US
    # Presse généraliste US sérieuse
    "nytimes.com",        # New York Times — économie
    "washingtonpost.com", # Washington Post — politique éco
    "forbes.com",         # Forbes — business / marchés
    "fortune.com",        # Fortune — corporate / macro
    # Finance en ligne
    "yahoo.com",          # Yahoo Finance — très couvert par GDELT
    "seekingalpha.com",   # Seeking Alpha — analyses actions
    "thestreet.com",      # The Street — marchés US
    "investopedia.com",   # Investopedia — pédagogie + actu
    "barrons.com",        # Barron's — hebdo financier US
    "morningstar.com",    # Morningstar — analyse fondamentale
]

GKG_COLUMNS = [
    "DATE", "NUMARTS", "COUNTS", "THEMES", "LOCATIONS",
    "PERSONS", "ORGANIZATIONS", "TONE", "CAMEOEVENTIDS",
    "SOURCES", "SOURCEURLS"
]


def test_gdelt_connection(date: datetime) -> bool:
    url = f"http://data.gdeltproject.org/gkg/{date.strftime('%Y%m%d')}.gkg.csv.zip"
    print(f"[1] Connexion à GDELT pour le {date.strftime('%Y-%m-%d')}...")
    print(f"    URL : {url}")
    try:
        r = requests.head(url, timeout=10)
        if r.status_code == 200:
            print("    ✅ Serveur accessible\n")
            return True
        else:
            print(f"    ❌ HTTP {r.status_code}\n")
            return False
    except Exception as e:
        print(f"    ❌ Erreur réseau : {e}\n")
        return False


def download_sample(date: datetime) -> Optional[pd.DataFrame]:
    url = f"http://data.gdeltproject.org/gkg/{date.strftime('%Y%m%d')}.gkg.csv.zip"
    print(f"[2] Téléchargement du fichier GKG (10-30s)...")
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            with z.open(z.namelist()[0]) as f:
                df = pd.read_csv(f, sep="\t", header=None, names=GKG_COLUMNS,
                                 dtype=str, on_bad_lines="skip")
        print(f"    ✅ {len(df):,} lignes chargées\n")
        return df
    except Exception as e:
        print(f"    ❌ Erreur : {e}\n")
        return None


def filter_by_keywords(df: pd.DataFrame) -> pd.DataFrame:
    """Filtre les lignes contenant au moins un mot-clé."""
    pattern = "|".join(KEYWORDS)
    mask = (
        df["THEMES"].str.contains(pattern, case=False, na=False) |
        df["SOURCEURLS"].str.contains(pattern, case=False, na=False) |
        df["ORGANIZATIONS"].str.contains(pattern, case=False, na=False)
    )
    return df[mask].copy()


def filter_by_us_media(df: pd.DataFrame) -> pd.DataFrame:
    """Ne garde que les articles provenant de médias US de référence."""
    pattern = "|".join(US_MEDIA)
    mask = df["SOURCEURLS"].str.contains(pattern, case=False, na=False)
    return df[mask].copy()


def display_results(df_before: pd.DataFrame, df_after: pd.DataFrame):
    """Affiche les stats de filtrage et un aperçu des résultats."""

    print(f"[3] Résultats du double filtrage :")
    print(f"    Après mots-clés           : {len(df_before):>5} articles")
    print(f"    Après filtre médias US     : {len(df_after):>5} articles")
    print(f"    Taux de rétention          : {len(df_after)/len(df_before)*100:.1f}%\n")

    if df_after.empty:
        print("    ⚠️  Aucun résultat — vérifie les médias ou la date.")
        return

    # Extraction sentiment et URL
    df_after = df_after.copy()
    df_after["sentiment"] = pd.to_numeric(
        df_after["TONE"].str.split(",").str[0], errors="coerce"
    )
    df_after["url"] = df_after["SOURCEURLS"].str.split("<UDIV>").str[0]

    # Stats sentiment
    print(f"    Sentiment moyen   : {df_after['sentiment'].mean():>+.3f}")
    print(f"    Sentiment min     : {df_after['sentiment'].min():>+.3f}")
    print(f"    Sentiment max     : {df_after['sentiment'].max():>+.3f}\n")

    # Aperçu des articles
    print(f"{'─'*75}")
    print(f"{'DATE':<10}  {'SENTIMENT':>10}  URL")
    print(f"{'─'*75}")
    for _, row in df_after.head(MAX_ROWS).iterrows():
        date_str = str(row["DATE"])[:8]
        url      = str(row["url"])[:58]
        try:
            print(f"{date_str:<10}  {float(row['sentiment']):>+10.3f}  {url}")
        except (ValueError, TypeError):
            print(f"{date_str:<10}  {'N/A':>10}  {url}")
    print(f"{'─'*75}")
    print(f"\n✅ TEST RÉUSSI — Double filtrage opérationnel.")
    print(f"   Prochaine étape : intégrer dans src/news_collector.py\n")


# ── Lancement ────────────────────────────────
if __name__ == "__main__":
    print(f"\n{'='*60}")
    print(f"  GDELT — Test S&P 500 — Mots-clés + Médias US")
    print(f"{'='*60}\n")

    ok = test_gdelt_connection(TEST_DATE)
    if not ok:
        TEST_DATE = datetime.now() - timedelta(days=8)
        print(f"  ↩  Retry avec {TEST_DATE.strftime('%Y-%m-%d')}...")
        ok = test_gdelt_connection(TEST_DATE)

    if ok:
        df_raw = download_sample(TEST_DATE)
        if df_raw is not None:
            df_keywords = filter_by_keywords(df_raw)
            df_final    = filter_by_us_media(df_keywords)
            display_results(df_keywords, df_final)