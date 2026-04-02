import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime
from connexion_mt5 import connexion, deconnexion


def recuperer_historique(symbole="EURUSD", timeframe=mt5.TIMEFRAME_H1, nb_bougies=1000):
    ticks = mt5.copy_rates_from_pos(symbole, timeframe, 0, nb_bougies)

    if ticks is None:
        print(f"Erreur récupération données : {mt5.last_error()}")
        return None

    df = pd.DataFrame(ticks)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df.set_index('time', inplace=True)

    print(f"{symbole} — {len(df)} bougies récupérées ✅")
    print(df.tail(3))

    return df


if __name__ == "__main__":
    if connexion():
        df = recuperer_historique("EURUSD", mt5.TIMEFRAME_H1, 1000)
        deconnexion()