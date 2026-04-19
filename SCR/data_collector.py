import MetaTrader5 as mt5
import pandas as pd
import os
from connexion_mt5 import connexion, deconnexion

NB_BOUGIES = 99_999
TIMEFRAME  = mt5.TIMEFRAME_M5
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data")

INSTRUMENTS = {
    "[SP500]": "sp500_m5.csv",
    "GOLD":    "or_m5.csv",
    "BRENT":   "brent_m5.csv",
    "EURUSD":  "eurusd_m5.csv",
}


def collecter(symbole, fichier):
    ticks = mt5.copy_rates_from_pos(symbole, TIMEFRAME, 0, NB_BOUGIES)

    if ticks is None or len(ticks) == 0:
        print(f"  ❌ {symbole} — Erreur : {mt5.last_error()}")
        return None

    df = pd.DataFrame(ticks)
    df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
    df.set_index('time', inplace=True)
    df.drop(columns=['spread', 'real_volume'], inplace=True)

    path = os.path.join(OUTPUT_DIR, fichier)
    df.to_csv(path)

    print(f"  ✅ {symbole} — {len(df):,} bougies | {df.index[0].date()} → {df.index[-1].date()}")
    return df


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if connexion():
        print(f"📥 Récupération M5 — {NB_BOUGIES:,} bougies max par instrument")
        print("-" * 60)
        for symbole, fichier in INSTRUMENTS.items():
            collecter(symbole, fichier)
        print("-" * 60)
        print(f"💾 Fichiers sauvegardés dans : {OUTPUT_DIR}")
        deconnexion()