import dlt
from dlt.common.runtime.slack import send_slack_message

from components import source_1_reader, source_2_reader, source_3_reader

pipeline = dlt.pipeline(
    pipeline_name="leads_csv_pipeline",
    dataset_name="raw",
    destination=dlt.destinations.duckdb("leads.duckdb"),
)

load_info = pipeline.run([source_1_reader, source_2_reader, source_3_reader])
print(load_info)
# we reuse the pipeline instance below and load to the same dataset as data
pipeline.run([load_info], table_name="_load_info")

## Check if we have a schema change
## TODO Always runs the 1st time
for package in load_info.load_packages:
    for table_name, table in package.schema_update.items():
        print(f"Table {table_name}: {table.get('description')}")
        for column_name, column in table["columns"].items():
            print(f"\tcolumn {column_name}: {column['data_type']}")

# send_slack_message(
#     pipeline.runtime_config.slack_incoming_hook,
#     "Schema change detected!",
# )
