import datetime
import os
import json
import multiprocessing
import numpy
import pandas
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk


class ElasticSearchLoader:
    def __init__(self, extra: dict):
        self.index_name = extra['index_name']
        self.es = Elasticsearch(extra['Url'], api_key=extra['ApiKey'])
        self.cpu_count = extra.get('cpu_count', 1)
        self.chunk_size = extra.get('chunk_size', 500)

    def save(self, df: pandas.DataFrame):
        """
        Envia recordes de um Dataframe para Elasticsearch em lotes (bulk).

        :arg
            df (pandas.DataFrame).

        :returns
            list: Lista de dicionários representando um sumário do envio do lote.

        """

        # Remover campos nulos ou vazios
        df = df.fillna(numpy.nan).replace([numpy.nan], [None])
        df = df.replace(numpy.NaN, pandas.NA).where(df.notnull(), None)

        if df is None or df.empty:
            print('Dataframe nulo ou vazio')
            return

        # Composição dos pacotes para envio
        def prepare_documents(index, df_):
            for _, row in df_.iterrows():
                yield {
                    "_index": index,
                    "_source": row.to_dict()
                }

        successes = 0
        for success, info in parallel_bulk(self.es, prepare_documents(self.index_name, df), thread_count=self.cpu_count,
                                           chunk_size=self.chunk_size):
            if success:
                successes += 1
            else:
                print(f"Erro ao indexar documento: {info}")

        return successes


class PostgreSQLLoader:
    def __init__(self, extra: dict):
        self.cpu_count = multiprocessing.cpu_count() - 2
        self.chunck_size = 100

    def save(self, rawdf):
        return


class LoaderData:

    loaders = {
        'ElasticSearch': ElasticSearchLoader,
        'PostgreSQL': PostgreSQLLoader,
        # ... adicionar outros loaders
    }

    def __init__(self, chosen_loader: str, extra: dict):
        extra['loader_name'] = chosen_loader
        extra['cpu_count'] = multiprocessing.cpu_count() - 2
        extra['chunk_size'] = 500
        self.loader_instance = self._create_loader(chosen_loader, extra)

    def _create_loader(self, loader_type, extra):
        loader_class = self.loaders.get(loader_type)
        if loader_class:
            return loader_class(extra)
        else:
            raise ValueError(f"Tipo de loader não suportado: {loader_type}")

    def save(self, df):
        try:
            self.loader_instance.save(df)
        except Exception as e:
            raise Exception(f"Falhou ao salvar dataframe no loader. Detalhes: {e}")


# Exemplo de uso:
if __name__ == "__main__":
    import random
    from dateutil import parser
    # Mock do DataFrame
    rand = random.randint(10000, 20000)
    data = {
        "created_at": [parser.parse("2023-01-03"), parser.parse("2023-01-04")],
        "updated_at": [parser.parse("2023-01-03"), parser.parse("2023-01-04")],
        "title": [f"Título1 dia 9 de dezembro - {rand}", f"Título2 dia 9 de dezembro - {rand}"],
        "product_name": [f"Product {rand}", f"Product {rand}"],
        "quantity": random.randint(0, 100),
        "value": random.uniform(1000, 2000)
    }
    mock_df = pandas.DataFrame(data=data)

    extra_full = {
        'ElasticSearch': {
            'index_name': 'app_esther',
            'Url': 'https://ea4ccfddc4a64600a9e4b6f93d810ac8.us-central1.gcp.cloud.es.io:443',
            'ApiKey': '<PREENCHER>',
        },
        'PostgreSQLLoader': {
            'db': 'gooru_tests',
            'table_name': 'pusherpg',
            'host': 'localhost',
            'port': '5432'
        }
    }

    # Obtendo um loader específico (ElasticSearchLoader ou PostgreSQLLoader)
    ld = 'ElasticSearch'
    loader = LoaderData(ld, extra_full.get(ld))
    loader.save(mock_df)
