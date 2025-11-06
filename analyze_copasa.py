import pandas as pd
from pathlib import Path
import re

DATA_PATH = Path("data/copasa_tweets.csv")

# Topic keyword groups
TOPICS = {
    "privatization": [
        "privatiza", "privatização", "desestatiza", "vender", "liquidação",
        "referendo", "pec", "cal(a)? a boca", "cala a boca", "golpe"
    ],
    "protest": [
        "protest", "manifest", "vândal", "ato", "resistên", "galerias", "esseTremÉNosso".lower()
    ],
    "investment_stock": [
        "#csmg3", "ação", "bolsa", "invest", "dividend", "preço", "recorde de investimento"
    ],
    "service_issue": [
        "falta água", "sem água", "vazamento", "água todo dia", "obra", "agência virtual",
        "copasa digital", "precari"
    ],
    "politics_government": [
        "zema", "governo", "governador", "lula", "assembleia", "almg", "deputad"
    ]
}

def normalize(t):
    return t.lower()

def classify(text):
    lt = normalize(text)
    matched = set()
    for topic, kws in TOPICS.items():
        for kw in kws:
            if re.search(r"\b" + kw, lt):
                matched.add(topic)
                break
    if not matched:
        return ["other"]
    return list(matched)

def main():
    if not DATA_PATH.exists():
        print("Data file not found.")
        return
    df = pd.read_csv(DATA_PATH)
    if "text" not in df.columns:
        print("No text column.")
        return

    df["topics"] = df["text"].apply(classify)
    # Explode for counting
    exploded = df.explode("topics")
    counts = exploded["topics"].value_counts().sort_values(ascending=False)

    total = len(df)
    print(f"Total tweets: {total}")
    print("\nCounts by topic:")
    for topic, cnt in counts.items():
        pct = cnt / total * 100
        print(f"- {topic}: {cnt} ({pct:.1f}%)")

    # Tweets specifically about privatization (privatization OR protest OR politics_government)
    subject_set = {"privatization", "protest", "politics_government"}
    mask_subject = exploded["topics"].isin(subject_set)
    subject_unique = exploded.loc[mask_subject, "tweet_id"].nunique()
    print(f"\nTweets about the subject (privatization/protest/politics): {subject_unique} ({subject_unique/total*100:.1f}%)")

    # Optional: save annotated file
    out = Path("data/copasa_tweets_annotated.csv")
    df.to_csv(out, index=False)
    print(f"Annotated data saved to {out}")

if __name__ == "__main__":
    main()