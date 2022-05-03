import pandas as pd
import flask


class DataRetrieverInterface(object):

    def retrieve(self):
        raise NotImplementedError()

    def process(self):
        raise NotImplementedError()

    def save(self):
        raise NotImplementedError()


class ParquetRetriever(DataRetrieverInterface):

    def __init__(self, bucket_name: str, source_url: str, destiny_path: str):
        self._bucket_name = bucket_name
        self._source_url = source_url
        self._destiny_path = destiny_path
        self._data = None

    def retrieve(self):
        """
        It retrieves the data from the source url and stores it in the data attribute
        """
        self._data = pd.read_csv(self._source_url, sep=",")

    def process(self):
        """
        It transforms all columns to string
        """
        if self._data is None:
            raise Exception("There is no data to process")
        self._data = self._data.astype(str)

    def save(self):
        """
        It transform the dataframe to parquet format
        """
        if self._data is None:
            raise Exception("There is no data to write")
        destiny_path = f"gcs://{self._bucket_name}/{self._destiny_path}"
        self._data.to_parquet(destiny_path, engine='fastparquet')


def main(request):

    request_json = request.get_json()
    bucket_name = request_json['bucket_name']
    source_url = request_json['source_url']
    destiny_path = request_json['destiny_path']

    try:
        parquet_retriever = ParquetRetriever(bucket_name, source_url,
                                             destiny_path)
        parquet_retriever.retrieve()
        parquet_retriever.process()
        parquet_retriever.save()
        response = flask.Response("DONE", status=200)
    except Exception as ex:
        response = flask.Response(str(ex), status=404)

    return response