import json
import random
import pandas
import unittest
from dotenv import load_dotenv
from elastic_client import ElasticHandler


class TestElasticHandler(unittest.TestCase):

    def test_send_to_elasticsearch(self):

        load_dotenv()

        # Mock do DataFrame
        rand = random.randint(10000, 20000)
        data = {
            "created_at": ["2023-01-03", "2023-01-04"],
            "updated_at": ["2023-01-03", "2023-01-04"],
            "title": [f"Title {rand}", f"Title {rand}"],
            "product_name": [f"Product {rand}", f"Product {rand}"]
        }
        mock_df = pandas.DataFrame(data=data)

        # Carregar as configurações do arquivo JSON
        with open('./appsettings.json', 'r') as f:
            config = json.load(f)

        index = 'app_esther'

        elastic_handler = ElasticHandler(index)

        elastic_response = elastic_handler.send_to_elasticsearch(mock_df)

        errors = [x.get('errors') for x in elastic_response if x.get('errors') is True]
        items = [x.get('items') for x in elastic_response if x.get('items')]

        self.assertEqual(len(errors), 0, "Não há erros")
        self.assertGreater(len(items), 0, "Há mais que um elemento na lista.")


if __name__ == '__main__':
    unittest.main()
