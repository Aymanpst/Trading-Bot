import MetaTrader5 as mt5
from dotenv import load_dotenv
import os

load_dotenv()

def connexion():
    if not mt5.initialize():
        print(f"Erreur de connexion : {mt5.last_error()}")
        return False

    print(f"Connecté à MetaTrader5 ")
    print(f"Version : {mt5.version()}")
    print(f"Broker : {mt5.account_info().company}")
    print(f"Balance : {mt5.account_info().balance} €")
    return True

def deconnexion():
    mt5.shutdown()
    print("Déconnecté de MT5")

if __name__ == "__main__":
    if connexion():
        deconnexion()

##