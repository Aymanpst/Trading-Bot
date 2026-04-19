import MetaTrader5 as mt5
import pandas as pd
import os
from connexion_mt5 import connexion, deconnexion

SYMBOLE    = "[SP500]"
TIMEFRAME  = mt5.TIMEFRAME_M5
NB_BOUGIES = 99_999
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "sp500_m5.csv")


def collecter():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    ticks = mt5.copy_rates_from_pos(SYMBOLE, TIMEFRAME, 0, NB_BOUGIES)

    if ticks is None or len(ticks) == 0:
        print(f"❌ Erreur : {mt5.last_error()}")
        return None

    df = pd.DataFrame(ticks)
    df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
    df.set_index('time', inplace=True)
    df.drop(columns=['spread', 'real_volume'], inplace=True)

    df.to_csv(OUTPUT_FILE)

    print(f"✅ {len(df):,} bougies sauvegardées → {OUTPUT_FILE}")
    print(f"   Du {df.index[0].date()} au {df.index[-1].date()}")
    print(df.tail(3))

    return df


if __name__ == "__main__":
    if connexion():
        collecter()
        deconnexion()