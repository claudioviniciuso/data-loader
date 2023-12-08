import multiprocessing
import os
import numpy as np
import pandas
from elasticsearch import Elasticsearch
import json


class ElasticHandler:
    def __init__(self, index_name):
        self.config = self._set_config(index_name)
        self.client = Elasticsearch(self.config['Url'], api_key=os.environ.get(self.config['ApiKey']))
        self.index_name = index_name
        self.cpu_count = multiprocessing.cpu_count() - 2
        self.chunck_size = 100

    @staticmethod
    def _set_config(index_name):
        with open('./appsettings.json', 'r') as f:
            config = json.load(f)
        try:
            current_index_config = config['ElasticsearchConfigs'][index_name]
        except (KeyError, ValueError):
            raise Exception("Índice inválido. Verificar appsettings.json")
        return current_index_config

    def send_to_elasticsearch(self, df_raw):

        try:
            fields_mapping = self.config['FieldsMapping']
            fields_to_keep = [mapping['FieldName'] for mapping in fields_mapping.values()]
        except (KeyError, ValueError):
            raise Exception("appsettings.json file is corrupted")

        df = df_raw[fields_to_keep]

        df = df.fillna(np.nan).replace([np.nan], [None])
        df = df.replace(np.NaN, pandas.NA).where(df.notnull(), None)

        if df is None:
            print('Dataframe is None')
            return

        if df.empty:
            print('Dataframe is Empty')
            return

        # Bulk index the records to Elasticsearch
        counter = 0
        bulk_pack = []
        bulk_unit = []
        for record in df.to_dict(orient='records'):
            data = {
                "index": {
                    "_index": self.index_name,
                }
            }
            counter += 1
            if counter > self.chunck_size:
                bulk_pack.append(bulk_unit)
                counter, bulk_unit = 0, []
            bulk_unit.append(json.dumps(data))
            bulk_unit.append(json.dumps(record))
        if len(bulk_unit) > 0:
            bulk_pack.append(bulk_unit)

        # Send bulk data to Elasticsearch
        for bulk in bulk_pack:
            bulk_data = "\n".join(bulk) + "\n"
            self.client.bulk(body=bulk_data)
