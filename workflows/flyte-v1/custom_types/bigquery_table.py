import re
import pandas as pd
from typing import Type, cast, TYPE_CHECKING

from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery_storage_v1 import types

from flytekit import lazy_module
from flytekit.models import literals
from flytekit.extend import TypeTransformer, TypeEngine
from flytekit.models.types import LiteralType, StructuredDatasetType
from flytekit.models.literals import StructuredDatasetMetadata
from flytekit.models.core.types import BlobType
from flytekit.core.context_manager import FlyteContext
from flytekit.models.literals import Literal, Scalar
from flytekit.types.structured import StructuredDataset

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa
else:
    pd = lazy_module("pandas")
    pa = lazy_module("pyarrow")

class CustomBigQueryTable:

  def __init__(self, df: pd.DataFrame, uri: str, project_id: str=None):
    self._project_id = project_id
    self._df = df
    self._uri = uri

  @property
  def uri(self) -> str:
    return self._uri

  @property
  def dataframe(self) -> pd.DataFrame:
    return self._df
  
  @property
  def project_id(self) -> str:
    return self._project_id


class CustomBigQueryTableTransformer(TypeTransformer[CustomBigQueryTable]):
  def __init__(self):
    super(CustomBigQueryTableTransformer, self).__init__(name="custom-bigquery-transform", t=CustomBigQueryTable)

  def get_literal_type(self, t: Type[CustomBigQueryTable]) -> LiteralType:
      return LiteralType(structured_dataset_type=StructuredDatasetType())

  def to_literal(
        self,
        ctx: FlyteContext,
        python_val: CustomBigQueryTable,
        python_type: Type[CustomBigQueryTable],
        expected: LiteralType,
    ) -> Literal:
        table_id = cast(str, python_val.uri).split("://", 1)[1].replace(":", ".")
        client = bigquery.Client(project=python_val.project_id)
        client.load_table_from_dataframe(python_val.dataframe, table_id)

        return Literal(scalar=Scalar(structured_dataset=literals.StructuredDataset(uri=python_val.uri)))

  def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[CustomBigQueryTable]) -> CustomBigQueryTable:
      _, project_id, dataset_id, table_id = re.split("\\.|://|:", lv.scalar.structured_dataset.uri)
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
      
      return CustomBigQueryTable(df=df, uri=lv.scalar.structured_dataset.uri, project_id=project_id)

TypeEngine.register(CustomBigQueryTableTransformer())
