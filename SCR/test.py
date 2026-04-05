# TEST - Reddit Fetch Light (SP500 filter)
# Vérifie que la récupération des posts fonctionne

import requests

HEADERS  = {"User-Agent": "trading_sentiment_thesis/1.0"}
KEYWORD  = "sp500"
LIMIT    = 100  # augmente jusqu'à 100 max par appel

url = f"https://www.reddit.com/r/wallstreetbets/new.json?limit={LIMIT}"

print(f"Fetching r/wallstreetbets — filtre: '{KEYWORD}'...")

response = requests.get(url, headers=HEADERS, timeout=10)
print(f"Status code: {response.status_code}")

if response.status_code == 200:
    all_posts    = response.json()["data"]["children"]
    filtered     = []

    for post in all_posts:
        data = post["data"]
        text = f"{data['title']} {data.get('selftext', '')}".lower()
        if KEYWORD.lower() in text:
            filtered.append(data)

    print(f"{len(all_posts)} posts récupérés → {len(filtered)} contiennent '{KEYWORD}'\n")

    if filtered:
        for data in filtered:
            print(f"  Titre        : {data['title'][:70]}...")
            print(f"  Score        : {data['score']}")
            print(f"  Commentaires : {data['num_comments']}")
            print(f"  URL          : https://reddit.com{data['permalink']}")
            print("-" * 50)
    else:
        print(f"Aucun post contenant '{KEYWORD}' dans les {LIMIT} derniers posts.")
        print("💡 Essaie avec un autre subreddit comme r/stocks ou r/investing.")

else:
    print(f"ERREUR : {response.status_code}")