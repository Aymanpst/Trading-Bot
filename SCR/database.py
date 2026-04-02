import sqlite3
import pandas as pd
import os

DB_PATH = "data/trading_bot.db"


def creer_base():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS prix (
            time        TEXT PRIMARY KEY,
            symbole     TEXT,
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL,
            tick_volume INTEGER,
            spread      INTEGER
        )
    ''')

    conn.commit()
    conn.close()
    print("Base de données créée ✅")


def sauvegarder_prix(df, symbole="EURUSD"):
    conn = sqlite3.connect(DB_PATH)

    df_save = df.copy()
    df_save['symbole'] = symbole
    df_save['time'] = df_save.index.astype(str)

    df_save[['time', 'symbole', 'open', 'high', 'low', 'close', 'tick_volume', 'spread']] \
        .to_sql('prix', conn, if_exists='replace', index=False)

    conn.close()
    print(f"{len(df_save)} bougies sauvegardées en base ✅")


def charger_prix(symbole="EURUSD"):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"SELECT * FROM prix WHERE symbole='{symbole}'", conn)
    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace=True)
    conn.close()
    print(f"{len(df)} bougies chargées depuis la base ✅")
    return df


if __name__ == "__main__":
    from connexion_mt5 import connexion, deconnexion
    from data_collector import recuperer_historique
    import MetaTrader5 as mt5

    if connexion():
        creer_base()
        df = recuperer_historique("EURUSD", mt5.TIMEFRAME_H1, 1000)
        sauvegarder_prix(df, "EURUSD")
        df_charge = charger_prix("EURUSD")
        print(df_charge.tail(3))
        deconnexion()