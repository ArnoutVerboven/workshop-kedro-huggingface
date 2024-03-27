"""
This is a boilerplate pipeline 'summarizer'
generated using Kedro 0.19.3
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import summarize_submissions, fetch_submissions, llm_inference  # To add


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=fetch_submissions,
                inputs=["params:subreddit_name", "params:submission_limit"],
                outputs="submissions_raw",
                name="fetch_submissions",
            ),
            node(
                func=summarize_submissions,
                inputs=["submissions_raw", "params:models"],
                outputs="submissions_summaries",
                name="summarize_submissions",
            ),
            # To add
            node(
                func=llm_inference,
                inputs=["submissions_raw", "params:llm_inference"],
                outputs="submissions_llm_summaries",
                name="llm_inference",
            )
        ]
    )
