
import json
import os
import base64

from io import BytesIO

import pandas as pd
import matplotlib.pyplot as plt

import requests
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue


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
def topstories(context: AssetExecutionContext) -> MaterializeResult:
    """
    Uses the topstory_ids json and retrieves information for each id and stores it in a pandas dataframe, then stores it into a csv. Returns a MaterializeResult object to capture metadata pretaining to the run
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

    # return metadata
    return MaterializeResult(
        metadata={
            "num_records": df.shape[0],
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )

@asset(deps=[topstories])
def most_frequent_words() -> MaterializeResult:
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

    # bar chart of top 25 words
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(list(top_words.keys()), list(top_words.values()))
    ax.set_xticks(range(len(top_words)), list(top_words.keys()), rotation=45, ha='right')
    ax.set_title("Top 25 Words in Hacker News Titles")
    fig.tight_layout()

    # convert image into savable format
    buffer = BytesIO()
    fig.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())

    # convert image to markdown
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    with open("data/most_frequent_words.json", "w") as f:
        json.dump(top_words, f)

    return MaterializeResult(
        metadata={"plot": MetadataValue.md(md_content)}
    )
