import datetime
import os
import json
import multiprocessing
import numpy
import pandas as pd
from sqlalchemy import text
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk
from abc import ABC, abstractmethod

class Loader(ABC):
    @abstractmethod
    def create_connection(self, connection:dict):
        pass
    
    def load_data(self, df: pd.DataFrame, dest_name:str):
        pass

class ElasticSearchLoader(Loader):
    def __init__(self, connection: dict, config: dict = None):
        if config is None:
            config = {}
        self.connection = self.create_connection(connection)
        self.config = config

    def create_connection(self, connection: dict):
        param_connections = ['url', 'api_key', 'extras'] 
        for param in param_connections:
            if param not in connection.keys():
                raise ValueError(f'Parametro {param} não encontrado na conexão com o o Elasticsearch')
        
        extra_connection_params = connection.get('extras', {})

        try:
            self.es = Elasticsearch(
                [connection['url']],
                api_key=(connection['api_key']),
                **extra_connection_params
            )
            if not self.es.ping():
                raise ConnectionError("Falha ao se conectar ao Elasticsearch")
            return True
        except Exception as e:
            raise Exception(f'Erro ao conectar ao banco de dados: {e}')
    
    def prepare_documents(self, df, index, id_ref):
        bulk = []
        id_exists = id_ref in df.columns

        for counter, doc in enumerate(df.to_dict('records'), start=1):
            _id = doc[id_ref] if id_exists else counter
            bulk.append({
                '_index': index,
                '_id': _id,
                '_source': doc
            })
        return bulk

    def load_data(self, df: pd.DataFrame, dest_name:str):
        if df.empty:
            raise Exception('Dataframe is empty')
        
        df = df.where(pd.notnull(df), None)
        bulk = self.prepare_documents(df, dest_name, 'id')

        try:
            for success, info in parallel_bulk(self.es, bulk, thread_count=self.config.get('thread_count', 4), chunk_size=self.config.get('chunk_size', 1000)):
                if not success:
                    print(f"Erro ao indexar documento: {info}")
        except Exception as e:
            raise Exception(f'Erro ao carregar dados no Elasticsearch: {e}')
     
        return "Success, data loaded"


class PostgreSQLoader(Loader):
    def __init__(self, connection: dict, config: dict = None):
        if config is None:
            config = {}
        self.connection_data = connection
        self.connection = self.create_connection(connection)
        self.config = config

    def create_connection(self, connection: dict):
        param_connections = ['host', 'port', 'database', 'user', 'password', 'schema'] 
        for param in param_connections:
            if param not in connection.keys():
                raise ValueError(f'Parametro {param} não encontrado na conexão com o o PostgreSQL')
        try:
            self.engine = create_engine(f"postgresql://{connection['user']}:{connection['password']}@{connection['host']}:{connection['port']}/{connection['database']}")
            return True
        except Exception as e:
            raise Exception(f'Erro ao conectar ao banco de dados: {e}')
    
    def load_data(self, df: pd.DataFrame, dest_name:str):
        if df.empty:
            raise Exception('Dataframe is empty')
        
        df = df.where(pd.notnull(df), None)

        try:
            chunk_size = self.config.get('chunk_size', 1000)
            for i in range(0, len(df), chunk_size):
                chunk = df[i:i + chunk_size]
                chunk.to_sql(dest_name, schema=self.connection_data['schema'], con=self.engine, if_exists='append', index=False)

        except Exception as e:
            raise Exception(f'Erro ao carregar dados no PostgreSQL: {e}')
     
        return "Success, data loaded"