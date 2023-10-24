import uuid
from datetime import timedelta
from typing import Any, Dict, List, Tuple

# from airflow.providers.slack.operators.slack import SlackAPIPostOperator


def send_slack_message_api(message: str, context: Dict[str, Any], channel: str):
    """sends a slack message to a channel using the slack API (not webhook)"""
    api_token = aws_secret("SLACK_AIRFLOW_TOKEN")
    slack_msg = SlackAPIPostOperator(
        task_id="slack_message_{}".format(uuid.uuid4()),
        dag=context.get("dag"),
        token=api_token,
        username="airflow",
        text=message,
        channel=channel,
    )
    slack_msg.execute(context=context)


def slack_dbt_error(data, context):
    """
    Notifies the appropriate slack channel
    Keyword arguments:
    data: A pandas data frame with the results of DBTLogParser
    context: Airflow context.
    """
    msg = """<!here> A DBT model or test owned by this team has experienced a warning or failure. <{url}|*Log*>
        ```\n{table} ```
        """
    for slack_channel in set(data["slack_id"]):
        filtered_df = data.loc[
            data["slack_id"] == slack_channel,
            ["model_owner", "path", "status", "depends_on", "message"],
        ]
        table = filtered_df.to_markdown(tablefmt="grid", index=False)
        final_msg = msg.format(table=table, url=context.get("task_instance").log_url)
        send_slack_message_api(
            message=final_msg,
            context=context,
            channel=slack_channel,
        )
