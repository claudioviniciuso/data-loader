from elasticsearch import Elasticsearch

import json


class ElasticHandler:
    def __init__(self, index_name):
        self.config = self._set_config(index_name)
        self.client = Elasticsearch(config, api_key=api_key)
        self.index_name = index_name

    @staticmethod
    def _set_config(index_name):
        with open('./appsettings.json', 'r') as f:
            config = json.load(f)
        try:
            current_index_config = config['ElasticsearchConfigs'][index_name]
        except (KeyError, ValueError):
            raise Exception("Índice inválido. Verificar appsettings.json")
        return current_index_config

    def send_to_elasticsearch(self, df, chunck_size=100):

        # If fields are pre-determined:
        fields_to_keep = df.columns.values
        if self.config:
            fields_mapping = self.config['ElasticsearchConfigs'][self.index_name]['FieldsMapping']
            fields_to_keep = [mapping['FieldName'] for mapping in fields_mapping.values()]

        df_filtered = df[fields_to_keep]

        # Convert DataFrame to JSON
        records = df_filtered.to_dict(orient='records')

        # Bulk index the records to Elasticsearch
        bulk_data = []
        for record in records:
            data = {
                "index": {
                    "_index": self.index_name,
                }
            }
            bulk_data.append(json.dumps(data))
            bulk_data.append(json.dumps(record))

        # Send bulk data to Elasticsearch
        bulk_data = "\n".join(bulk_data) + "\n"
        self.client.bulk(body=bulk_data)
