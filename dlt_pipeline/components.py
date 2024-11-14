from typing import Any, Iterator
from dlt.sources.filesystem import FileItemDict, filesystem
from dlt.sources import TDataItems

import dlt


@dlt.transformer()
def read_csv_custom(
    items: Iterator[FileItemDict], chunksize: int = 10000, **pandas_kwargs: Any
) -> Iterator[TDataItems]:
    import pandas as pd

    # Apply defaults to pandas kwargs
    kwargs = {**{"header": "infer", "chunksize": chunksize}, **pandas_kwargs}

    for file_obj in items:
        with file_obj.open() as file:
            for df in pd.read_csv(file, **kwargs):
                df["_file_name"] = file_obj["file_name"]
                df["_relative_path"] = file_obj["relative_path"]
                df["_modification_date"] = file_obj["modification_date"]
                yield df.to_dict(orient="records")


source_1_files = filesystem(
    bucket_url="gs://source-bucket-dev", file_glob="technical_exercise/source_1/*.csv"
)
source_1_reader = (source_1_files | read_csv_custom()).with_name("source_1_raw")
source_2_files = filesystem(
    bucket_url="gs://source-bucket-dev", file_glob="technical_exercise/source_2/*.csv"
)
source_2_reader = (source_2_files | read_csv_custom()).with_name("source_2_raw")
source_3_files = filesystem(
    bucket_url="gs://source-bucket-dev", file_glob="technical_exercise/source_3/*.csv"
)
source_3_reader = (source_3_files | read_csv_custom()).with_name("source_3_raw")
