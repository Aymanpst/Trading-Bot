import requests
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_KEY")


def recuperer_news(symbole="INDEX:SPX", limite=100):
    url = "https://www.alphavantage.co/query"

    params = {
        "function": "NEWS_SENTIMENT",
        "tickers": symbole,
        "limit": limite,
        "apikey": API_KEY
    }

    response = requests.get(url, params=params)
    data = response.json()

    if "feed" not in data:
        print(f"Erreur : {data}")
        return None

    articles = []
    for item in data["feed"]:
        articles.append({
            "titre": item["title"],
            "source": item["source"],
            "date": item["time_published"],
            "sentiment": item["overall_sentiment_label"],
            "score": item["overall_sentiment_score"],
            "resume": item["summary"]
        })

    df = pd.DataFrame(articles)
    print(f"{len(df)} articles récupérés ✅")
    print(df[["titre", "sentiment", "score"]].head(3))

    return df


