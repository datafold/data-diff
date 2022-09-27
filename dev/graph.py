# Use this to graph the benchmarking results (see benchmark.sh)
#
# To run this:
#   - pip install pandas
#   - pip install plotly
#

import pandas as pd
import plotly.graph_objects as go
from data_diff.utils import number_to_human
import glob

for benchmark_file in glob.glob("benchmark_*.jsonl"):
    rows = pd.read_json(benchmark_file, lines=True)
    rows["cloud"] = rows["test"].str.match(r".*(snowflake|redshift|presto|bigquery)")
    sha = benchmark_file.split("_")[1].split(".")[0]
    print(f"Generating graphs from {benchmark_file}..")

    for n_rows, group in rows.groupby(["rows"]):
        image_path = f"benchmark_{sha}_{number_to_human(n_rows)}.png"
        print(f"\t rows: {number_to_human(n_rows)}, image: {image_path}")

        r = group.drop_duplicates(subset=["name_human"])
        r = r.sort_values(by=["cloud", "source_type", "target_type", "name_human"])

        fig = go.Figure(
            data=[
                go.Bar(
                    name="count(*)",
                    x=r["name_human"],
                    y=r["count_max_sec"],
                    text=r["count_max_sec"],
                    textfont=dict(color="blue"),
                ),
                go.Bar(
                    name="data-diff (checksum)",
                    x=r["name_human"],
                    y=r["checksum_sec"],
                    text=r["checksum_sec"],
                    textfont=dict(color="red"),
                ),
                go.Bar(
                    name="Download and compare †",
                    x=r["name_human"],
                    y=r["download_sec"],
                    text=r["download_sec"],
                    textfont=dict(color="green"),
                ),
            ]
        )
        # Change the bar mode
        fig.update_layout(title=f"data-diff {number_to_human(n_rows)} rows, sha: {sha}")
        fig.update_traces(texttemplate="%{text:.1f}", textposition="outside")
        fig.update_layout(uniformtext_minsize=2, uniformtext_mode="hide")
        fig.update_yaxes(title="Time")
        fig.write_image(image_path, scale=2)
