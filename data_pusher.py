import datetime
import os
import json
import multiprocessing
import numpy
import pandas
from sqlalchemy import create_engine
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
            int: Número de documentos enviados.

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


class PostgreSQLoader:
    def __init__(self, extra: dict):
        self.cpu_count = extra.get('cpu_count')
        self.chunk_size = extra.get('chunk_size')
        pg_user = extra.get('pg_user', 'postgres')
        pg_password = extra['pg_password']
        pg_host = extra.get('pg_host', 'localhost')
        pg_port = extra.get('pg_port', '5432')
        pg_dbname = extra['pg_dbname']
        self.engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_dbname}')

    def save(self, df: pandas.DataFrame, table_name=None):
        for i in range(0, len(df), self.chunk_size):
            chunk = df[i:i + self.chunk_size]
            with self.engine.connect() as conn:
                chunk.to_sql(table_name, conn, if_exists='append', index=False)

        # Verificar se existem colunas adicionais no DataFrame em relação à tabela no banco de dados
        with self.engine.connect() as conn:
            tabela_colunas = pandas.read_sql(f"SELECT * FROM {table_name} LIMIT 0", conn).columns

        colunas_novas = df.columns.difference(tabela_colunas)
        if not colunas_novas.empty:
            with self.engine.connect() as conn:
                for col in colunas_novas:
                    conn.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" TEXT')

        # Preencher campos ausentes com valores nulos na tabela do PostgreSQL (se necessário)
        # Suponha que 'coluna3' seja uma coluna ausente que precisa ser preenchida com valores nulos
        # Verifique se a coluna existe na tabela antes de preencher
        if 'coluna3' not in tabela_colunas:
            with self.engine.connect() as conn:
                conn.execute(f'ALTER TABLE {table_name} ADD COLUMN "coluna3" TEXT')

        # Selecione as colunas que não estão presentes no DataFrame e preencha com valores nulos
        colunas_faltantes = tabela_colunas.difference(df.columns)
        if not colunas_faltantes.empty:
            with self.engine.connect() as conn:
                for col in colunas_faltantes:
                    conn.execute(f'UPDATE {table} SET "{col}" = NULL')


class DataLoader:

    loaders = {
        'ElasticSearch': ElasticSearchLoader,
        'PostgreSQL': PostgreSQLoader,
        # ... adicionar outros loaders
    }

    def __init__(self, chosen_loader: str, extra: dict):
        extra['loader_name'] = chosen_loader
        extra['cpu_count'] = multiprocessing.cpu_count() - 2
        extra['chunk_size'] = extra.get('chunk_size', 500)
        self.loader_instance = self._create_loader(chosen_loader, extra)

    def _create_loader(self, loader_type, extra):
        loader_class = self.loaders.get(loader_type)
        if loader_class:
            return loader_class(extra)
        else:
            raise ValueError(f"Tipo de loader não suportado: {loader_type}")

    def save(self, df, **kwargs):
        try:
            self.loader_instance.save(df, **kwargs)
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
        "valueA": random.uniform(1000, 2000),
        "valueB": 1
    }
    mock_df = pandas.DataFrame(data=data)

    extra_full = {
        'ElasticSearch': {
            'index_name': 'app_esther',
            'Url': 'https://ea4ccfddc4a64600a9e4b6f93d810ac8.us-central1.gcp.cloud.es.io:443',
            'ApiKey': '<insira sua chave aqui>',
        },
        'PostgreSQL': {
            'pg_dbname': 'gooru_tests',
            'pg_password': '<insira sua senha aqui>',
            'chunk_size': 1
        }
    }

    # Obtendo um loader específico (PostgreSQLoader)
    ld = 'PostgreSQL'
    loader = DataLoader(ld, extra_full.get(ld))
    loader.save(mock_df, table_name='pusher_table')

    # Obtendo um loader específico (ElasticSearchLoader)
    ld = 'ElasticSearch'
    loader = DataLoader(ld, extra_full.get(ld))
    loader.save(mock_df)
