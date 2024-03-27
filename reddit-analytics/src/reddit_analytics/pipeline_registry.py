"""Project pipelines."""
from typing import Dict

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline

import reddit_analytics.pipelines.summarizer as s


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    summarizer_pipeline = s.create_pipeline()

    return {
        "__default__": summarizer_pipeline,
        "summarizer": summarizer_pipeline,
    }
