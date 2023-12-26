
import json
import os

import pandas as pd

import requests
from dagster import asset, AssetExecutionContext


@asset
def topstory_ids() -> None:
    """
    Retrieve a list of IDs from the current top stories from Hacker News and store them in json format in file
    """
    NEWS_STORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_new_story_ids = requests.get(NEWS_STORIES_URL).json()[:100]

    os.makedirs("data", exist_ok=True)
    with open("data/topstory_ids.json", "w") as f:
        json.dump(top_new_story_ids, f)


@asset(deps=[topstory_ids])
def topstories(context: AssetExecutionContext) -> None:
    """
    Uses the topstory_ids json and retrieves information for each id and stores it in a pandas dataframe, then stores it into a csv
    """
    with open("data/topstory_ids.json", "r") as f:
        topstory_ids = json.load(f)

    results = []
    for item_id in topstory_ids:
        item = requests.get(
            f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
        ).json()
        results.append(item)
    
        if len(results) % 20 == 0:
            context.log.info(f"Got {len(results)} items so far.")

    df = pd.DataFrame(results)
    df.to_csv("data/topstories.csv")


@asset(deps=[topstories])
def most_frequent_words() -> None:
    """
    Retrieve a list of the top-25 most frequent words in the titles of news articles and store them in json
    """
    STOPWORDS = ["a", "the", "an", "of", "to", "in", "for", "and", "with", "on", "is"]
    topstories = pd.read_csv("data/topstories.csv")

    word_counts = {}
    for raw_title in topstories["title"]:
        title = raw_title.lower()
        for word in title.split():
            cleaned_word = word.strip(".,-!?:;()[]'\"-")
            if cleaned_word not in STOPWORDS and len(cleaned_word) > 0:
                word_counts[cleaned_word] = word_counts.get(cleaned_word, 0) + 1

    top_words = {
        pair[0]: pair[1]
        for pair in sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:25]
    }

    with open("data/most_frequent_words.json", "w") as f:
        json.dump(top_words, f)
