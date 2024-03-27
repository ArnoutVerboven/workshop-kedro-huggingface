"""
This is a boilerplate pipeline 'summarizer'
generated using Kedro 0.19.3
"""
import pandas as pd

import os
import datetime as dt
import json
import textwrap

import polars as pl
from praw import Reddit
from pydantic import BaseModel, TypeAdapter

from transformers import pipeline


def fetch_submissions(subreddit_name, submission_limit) -> pl.DataFrame:
    """Fetch."""
    reddit_client_id = os.environ["REDDIT_CLIENT_ID"]
    reddit_client_secret = os.environ["REDDIT_SECRET"]
    reddit_username = os.environ["REDDIT_USERNAME"]  # Just for assembling the user agent

    # Read-only Reddit connection https://praw.readthedocs.io/en/stable/getting_started/quick_start.html#read-only-reddit-instances
    reddit = Reddit(
        client_id=reddit_client_id,
        client_secret=reddit_client_secret,
        user_agent=f"social summarizer by u/{reddit_username}",
    )

    subreddit = reddit.subreddit(subreddit_name.removeprefix("r/"))

    # Possible parameters https://old.reddit.com/dev/api#GET_new
    submissions = []
    for submission in subreddit.new(limit=submission_limit):
        submissions.append({
            "title": submission.title,
            "author_name": submission.author.name,
            "creation_datetime": dt.datetime.utcfromtimestamp(submission.created_utc).isoformat(),
            "subreddit_name": submission.subreddit_name_prefixed,
            "num_comments": submission.num_comments,
            "sfw": not submission.over_18,
            "score": submission.score,
            "upvote_ratio": submission.upvote_ratio,
            "is_self": submission.is_self,
            "permalink": submission.permalink,
            "selftext": submission.selftext,
            "flair_text": submission.link_flair_text,
        })

    class RedditSubmission(BaseModel):
        title: str
        author_name: str
        creation_datetime: dt.datetime
        subreddit_name: str
        num_comments: int
        sfw: bool
        score: int
        upvote_ratio: float
        is_self: bool
        permalink: str
        selftext: str | None
        flair_text: str | None

    adapter = TypeAdapter(list[RedditSubmission])

    objects = adapter.validate_python(submissions)

    df = pl.from_dicts(objects)

    return df


def summarize_submissions(submissions_raw: pl.DataFrame, models: dict) -> pl.DataFrame:
    """Summarize."""
    summarizer = pipeline("summarization", model=models["summarizer"])
    mask_filler = pipeline("fill-mask", model=models["mask_filler"])

    df = submissions_raw.with_columns(
        pl.col("selftext").map_elements(lambda text: get_summary(text, summarizer_model=summarizer)).alias("summary"),
    ).with_columns(
        pl.col("summary").map_elements(lambda text: str(get_hashtags(text, mask_filler_model=mask_filler))).alias("hashtags")
    ).with_columns(
        (pl.col("summary").str.len_chars() / pl.col("selftext").str.len_chars()).alias("summarization_pct")
    ).select(
        pl.col("author_name", "creation_datetime", "permalink", "title", "selftext", "summary", "hashtags", "summarization_pct")
    )

    return df



def get_hashtags(text, mask_filler_model) -> list[str]:
    results = mask_filler_model(text.strip() + " #<mask>")

    hashtags = set()
    for ht_dict in results:
        hashtags.add(ht_dict["token_str"])

    if max(len(ht) for ht in hashtags) == 1:
        # Discard list of ugly hashtags
        return []

    return list(hashtags)


def get_summary(text, summarizer_model) -> str:
    summary = summarizer_model(text)[0]["summary_text"].strip()
    return summary