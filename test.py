import json
import os
import pandas
import unittest
from unittest.mock import patch
from dotenv import load_dotenv
from elastic_client import ElasticHandler


class TestElasticHandler(unittest.TestCase):

    @patch('elastic_client.ElasticHandler')
    def test_send_to_elasticsearch(self, mock_es):

        load_dotenv()

        # Dados fictícios para simular um DataFrame
        data = {
            "created_at": ["2023-01-03", "2023-01-04"],
            "updated_at": ["2023-01-03", "2023-01-04"],
            "title": ["Title 3", "Title 4"],
            "product_name": ["Product C", "Product D"]
        }

        # Carregar as configurações do arquivo JSON
        with open('./appsettings.json', 'r') as f:
            config = json.load(f)

        app_esther_config = config['ElasticsearchConfigs']['app_esther']

        # Extrair informações para a configuração do índice 'app_esther'
        elasticsearch_url = app_esther_config['Url']
        index_name = 'app_esther'
        api_key = os.environ.get(app_esther_config['ApiKey'])

        elastic_handler = ElasticHandler(elasticsearch_url, api_key, index_name, config=config)

        # Mock do DataFrame (simulação de um DataFrame real)
        mock_df = pandas.DataFrame(data=data)

        # Chamar a função para enviar dados para o Elasticsearch
        elastic_handler.send_to_elasticsearch(mock_df)

        # Verificar se a função Elasticsearch.bulk foi chamada
        mock_es.return_value.bulk.assert_called_once()


if __name__ == '__main__':
    unittest.main()
