import pandas as pd
import flask
import json
#from typing import List


class DataRetrieverInterface(object):

    def retrieve(self):
        raise NotImplementedError()

    def process(self):
        raise NotImplementedError()

    def save(self):
        raise NotImplementedError()


class ParquetRetriever(DataRetrieverInterface):

    def __init__(self, bucket_name: str, origin_path: str, destiny_path: str,
                 schema_name: str):
        self._bucket_name = bucket_name
        self._origin_path = origin_path
        self._destiny_path = destiny_path
        self._schema_name = schema_name

        self._data = None

    def retrieve(self):
        """
        It retrieves the data from the source url and stores it in the data attribute
        """
        origin_path = f"gcs://{self._bucket_name}/{self._origin_path}"
        self._data = pd.read_parquet(origin_path, engine='fastparquet')

    def process(self):
        """
        Casting schema
        """
        if self._data is None:
            raise Exception("There is no data to process")
        self._data.columns = self._data.columns.str.lower()
        schema = self._get_schema()

        #TODO: Daniel, no entiendo como pasar esto sin usar pandas...
        # int_columns = []
        # str_columns = []
        # float_columns = []

        # for column, data_type in schema.items():
        #     if data_type == 'int':
        #         int_columns.append(column)
        #     elif data_type == 'float':
        #         float_columns.append(column)
        #     elif data_type == 'str':
        #         str_columns.append(column)
        #     else:
        #         raise Exception("Unknown data type")

        # self._parse_int_columns(int_columns)
        # self._parse_float_columns(float_columns)
        # self._parse_str_columns(str_columns)

        self._data = self._data.astype(schema)

    def save(self):
        """
        It transform the dataframe to parquet format
        """
        if self._data is None:
            raise Exception("There is no data to write")
        destiny_path = f"gcs://{self._bucket_name}/{self._destiny_path}"
        self._data.to_parquet(destiny_path, engine='fastparquet')

    def _get_schema(self):
        with open(self._schema_name, 'r') as schema_file:
            schema = json.load(schema_file)
        return schema

    # def _parse_int_columns(self, int_columns: List[str]):
    #     pass

    # def _parse_str_columns(self, str_columns: List[str]):
    #     pass

    # def _parse_float_columns(self, float_columns: List[str]):
    #     pass


def main(request):

    request_json = request.get_json()

    bucket_name = request_json['bucket_name']
    origin_path = request_json['origin_path']
    destiny_path = request_json['destiny_path']
    schema_name = request_json['schema_name']

    try:
        parquet_retriever = ParquetRetriever(bucket_name, origin_path,
                                             destiny_path, schema_name)
        parquet_retriever.retrieve()
        parquet_retriever.process()
        parquet_retriever.save()
        response = flask.Response("DONE", status=200)
    except Exception as ex:
        response = flask.Response(str(ex), status=404)

    return response