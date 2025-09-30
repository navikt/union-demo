import re
import pandas as pd
from flytekit.types.structured.structured_dataset import (
  StructuredDatasetEncoder, 
  StructuredDatasetDecoder, 
  StructuredDatasetTransformerEngine,
)
from flytekit.models.types import StructuredDatasetType
from flytekit.types.structured import StructuredDataset
from flytekit.models.literals import StructuredDatasetMetadata
from flytekit.core.context_manager import FlyteContext
from flytekit.models import literals
from typing import cast, TYPE_CHECKING
from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage_v1 import types
from flytekit import lazy_module


if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa
else:
    pd = lazy_module("pandas")
    pa = lazy_module("pyarrow")

BIGQUERY = "bq"


class BigQueryTableEncodingHandler(StructuredDatasetEncoder):
  def __init__(self):
      super().__init__(pd.DataFrame, BIGQUERY, supported_format="")

  def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
        structured_dataset_type: StructuredDatasetType,
    ) -> literals.StructuredDataset:
        _, project_id, dataset_id, table_id = re.split("\\.|://|:", structured_dataset.uri)
        client = bigquery.Client(project=project_id)
        client.load_table_from_dataframe(
            structured_dataset.dataframe,
            f"{project_id}.{dataset_id}.{table_id}"
        )

        return literals.StructuredDataset(
            uri=structured_dataset.uri,
        )


class BigQueryTableDecodingHandler(StructuredDatasetDecoder):
    def __init__(self):
      super().__init__(pd.DataFrame, BIGQUERY, supported_format="")

    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
        current_task_metadata: StructuredDatasetMetadata,
    ) -> pd.DataFrame:
      _, project_id, dataset_id, table_id = re.split("\\.|://|:", flyte_value.uri)
      table = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
      parent = "projects/{}".format(project_id)

      client = bigquery_storage.BigQueryReadClient()

      requested_session = types.ReadSession(table=table, data_format=types.DataFormat.ARROW)
      read_session = client.create_read_session(parent=parent, read_session=requested_session)

      frames = []
      for stream in read_session.streams:
          reader = client.read_rows(stream.name)
          for message in reader.rows().pages:
              frames.append(message.to_dataframe())

      if len(frames) > 0:
          df = pd.concat(frames)
      else:
          schema = pa.ipc.read_schema(pa.py_buffer(read_session.arrow_schema.serialized_schema))
          df = schema.empty_table().to_pandas()

      return df
