import json
import logging

import pandas as pd
from error_parsing_utils.dbt_manifest_graph import Manifest
from textwrap import fill


"""
This script is used to parse dbt log files and generate a pandas dataframe containing information about tests/models
that have failed, produced warnings, or had errors. It reads in two files, manifest.json and run_results.json, and
processes them using classes defined in the script. The ManifestProcessor class processes the manifest.json file and
extracts relevant information such as the unique id, node, model owner, slack id, path, and depends on values.
The RunResultsProcessor class processes the run_results.json file and creates a dataframe of the run results with
information such as the unique id, status, timing, result, and message. The JoinDf class then merges these two
dataframes on the unique id column. The resulting dataframe contains information about the failed/warning/error records,
their corresponding model owner, and Slack ID for notifications.
This script is called by instantiating an object of the DbtLogParser class with the file path arguments and then
calling the dbt_log_parser() function, which outputs a dataframe where each row equals a Slack alert to send.
"""

# File Paths for manifest.json and run_results.json
MANIFEST_FILE_PATH = "../../../../dbt/target/manifest.json"
RUN_RESULTS_FILE_PATH = "../../../../dbt/target/run_results.json"

# Default Slack Channel ID
DEFAULT_SLACK_ID = "C0514AZSN9Z"

# Class to process data from imported Manifest class
class ManifestProcessor:
    def __init__(self, data: dict):
        self.data = data

    def process_manifest(self):
        # Create a DataFrame from the data
        df = pd.DataFrame(self.data)

        # Extract the 'Data owner' and 'Slack ID' values from the split 'description' column
        data_owner_split = df["description"].str.extract(r"(Data Owner:\s*[A-Z0-9]+)", expand=True)
        slack_id_split = df["description"].str.extract(r"(Slack Channel ID:\s*[A-Z0-9]+)", expand=True)

        # Assign the extracted values to new columns in the DataFrame
        df["model_owner"] = data_owner_split
        df["slack_id"] = slack_id_split
        df["depends_on"] = df["depends_on"].apply(
            lambda x: x[0] if len(x) > 0 else None
        )

        # Select only the desired columns from the DataFrame
        df2 = df[["unique_id", "node", "model_owner", "slack_id", "path", "depends_on"]]

        # Returns the parsed Manifest file, with the 5 cols defined above
        return df2

    def lookup_related_node_for_tests(self, df2):
        for index, row in df2.iterrows():
            if row["unique_id"].startswith("test"):
                depends_on_row = df2.loc[df2["unique_id"] == row["depends_on"]]
                try:
                    slack_id = depends_on_row.iloc[0]["slack_id"]
                    df2.at[index, "slack_id_2"] = str(slack_id)
                except IndexError:
                    pass
        return df2
    
    # Set a default_value for the Slack Channel ID here, in case the script doesn't find an ID 
    def merge_slack_ids(self, df2, default_value: str = DEFAULT_SLACK_ID):
        # merge slack_id and slack_id_2 columns
        df2["slack_id"] = df2["slack_id"].combine_first(df2["slack_id_2"])

        # fill missing values with default_value
        df2["slack_id"].fillna(default_value, inplace=True)

        # fill non-null values that don't start with 'C' with default_value
        mask = df2["slack_id"].notnull() & (~df2["slack_id"].str.startswith("C"))
        df2.loc[mask, "slack_id"] = default_value

        # This df returns the node model_name, node text, model_owner as specified by the data owner param in the description,
        # the slack id as defined in the description, the model path in your project, and the dependecy nodes
        df2 = df2[
            ["unique_id", "node", "model_owner", "slack_id", "path", "depends_on"]
        ]

        return df2


# Class to parse the run_results file from dbt
class RunResultsProcessor:
    def __init__(self, file_path, notification_status=["fail", "warn", "error"]):
        self.file_path = file_path
        self.notification_status = notification_status

    def parse_data(self):
        with open(self.file_path) as f:
            data = json.load(f)

        # Empty list to write parsed data into
        rows = []

        # Iterate over the rows of the results and create a dictionary for each row
        for row in data["results"]:
            row_dict = {
                "node": row["unique_id"],
                "status": row["status"],
                "timing": row["timing"],
                "result": row["failures"],
                "message": row["message"],
            }

            # Add the row dictionary to the list
            rows.append(row_dict)

        # Create dataframe from rows list
        df = pd.DataFrame(rows)

        return df

    def process_data(self):
        # Call parse_data to get the parsed data as a DataFrame
        df = self.parse_data()

        # Split node and create 3 new fields based on split node (unique_id)
        df2 = df["node"].str.split(".", expand=True)
        df["process_type"] = df2[0]
        df["schema_name"] = df2[1]
        df["object_name"] = df2[2]
        df["clean_node_name"] = df.apply(
            lambda row: ".".join(
                [row["process_type"], row["schema_name"], row["object_name"]]
            ),
            axis=1,
        )

        # Filter to just failures / warn statuses in run log
        df_flagged = df[df.status.isin(self.notification_status)]

        return df_flagged


# Class to Join dataframes
# I need to rewrite this class to account for joining on depends on for test failures
class JoinDf:
    def __init__(self, df1, df2):
        self.df1 = df1
        self.df2 = df2

    def join_dataframes(self):
        df = pd.merge(self.df1, self.df2, left_on="node", right_on="unique_id")

        df = df.iloc[:, [8, 14, 1, 3, 4, 5, 11, 12, 13]]

        return df


# main function to execute above classes and functions
class DBTLogParser:
    def __init__(self, manifest_file_path, run_results_file_path, wrap_text=False):
        self.manifest_file_path = manifest_file_path
        self.run_results_file_path = run_results_file_path
        self.wrap_text = wrap_text

    def _wrap_text(self, row):
        width = 30
        if row.name == "message":
            width = 50
        return row.map(
            lambda x: fill(str(x), width=width), na_action="ignore"
        )  # str(x) to avoid issues wrapping dtypes != str

    def dbt_log_parser(self):
        logging.info("Parsing Manifest and run_results!")
        with open(self.manifest_file_path) as fh:
            data = json.load(fh)

        # manifest.json actions
        m = Manifest(**data)
        m2 = [
            {
                "node": node,
                "description": n.description,
                "path": n.path,
                "unique_id": n.unique_id,
                "depends_on": n.depends_on.nodes,
            }
            for node, n in m.nodes.items()
        ]
        processor = ManifestProcessor(m2)
        manifest_result = processor.process_manifest()
        lookup_result = processor.lookup_related_node_for_tests(manifest_result)
        manifest_result = processor.merge_slack_ids(lookup_result)

        # run_results.json actions
        rr_processor = RunResultsProcessor(self.run_results_file_path)
        rr_result = rr_processor.process_data()

        # join run result df and manifest df
        j = JoinDf(rr_result, manifest_result)
        final_df = j.join_dataframes()

        if self.wrap_text and not final_df.empty:
            final_df = final_df.apply(self._wrap_text)
        if final_df.empty:
            logging.info("No errors on the run_results!!")
        return final_df


if __name__ == "__main__":
    parser = DBTLogParser(MANIFEST_FILE_PATH, RUN_RESULTS_FILE_PATH, True)
    data = parser.dbt_log_parser()
    print(data.to_markdown())
