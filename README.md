<<<<<<< HEAD
# data-loader
Repositório para criação de módulo responsável por carregar dados no Banco de Dados e ElasticSearch
=======
# Data pusher

## Visão geral
Conjunto de classes desenvolvido com o propósito de simplificar a transferência e o 
carregamento otimizado de dados tabulares - especificamente Pandas dataframes - 
para uma variedade de sistemas de armazenamento.

## Características
- Suporte para diferentes destinos de armazenamentos, tais como ElasticSearch e PostgreSQL.
- Extensibilidade: sua estrutura baseada no padrão de _desing_ _Factory_ permite a inclusão de novas classes de
carregamento, mantendo a facilidade e padronização de uso.
- Entrada para diferentes parâmetros de configurações, tais como, número de nodos ou chaves.

## Detalhes técnicos

### Classe DataLoader
Para fazer o _push_ de um dataframe para qualquer destino, deve-se
instanciar a classe DataLoader (Fábrica de loaders). Ela recebe somente o nome do Loader
("ElasticSearch", "PostgreSLQ", etc) e o dicionário de parâmetros.
A partir daí todos as decisões são tomadas pela classe fabricada.

Um exemplo dos parâmetros do dicionário `extra` é apresentado a seguir:


```
extra = {
    'ElasticSearch': {
        'index_name': 'app_name',
        'Url': 'https://abcdefgnijklmno.us-central1.gcp.cloud.es.io:443',
        'ApiKey': '<valor_da_api_key>',
    },
    'PostgreSQLLoader': {
        'db': 'gooru_tests',
        'table_name': 'pusherpg',
        'host': 'localhost',
        'port': '5432'
    }
}
```

### Classe fabricada 1: ElasticSearchLoader

Ao instanciar essa classe, o DataLoader passa os parâmetros. Ao invocar
a função `save()` deve-se passar o dataframe.

Duas observações importantes sobre essa implementação são:
- ao converter o dataframe para doc do elastic, os tipos são convertidos, conforme convenção do ElasticSearch.
- a transferência é feita em chunks definidos no parâmetro extra e adicionalmente são
paralelizados utilizando-se a função `parallel_bulk` da api do ElasticSearch.


###  Classe fabricada 2: PostgreSQLoader

A metodologia é similar a anterior, porém os parâmetros são diferentes.
É importante passar detalhes do banco, como host, user, password, etc.

O carregamento também é feito em _chunks_ que por default vale 500, mas 
pode ser alterado no dicionário `extra`. Uma funcionalidade bastante importante
é a elasticidade do banco relacional - se o dataframe de input vier com colunas
adicionais, as mesmas são criadas.

## Testes

Para testar o código, você pode utilizar o seguinte script:

```
import random
from dateutil import parser
# Mock do DataFrame
rand = random.randint(10000, 20000)
data = {
    "created_at": [parser.parse("2023-01-03"), parser.parse("2023-01-04")],
    "updated_at": [parser.parse("2023-01-03"), parser.parse("2023-01-04")],
    "title": [f"Título1 dia 9 de dezembro - {rand}", f"Título2 dia 9 de dezembro - {rand}"],
    "product_name": [f"Product {rand}", f"Product {rand}"]
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
        'pg_password': '<insira sua senha do postgres aqui>',
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
```

Teste adicionar colunas no dataframe e também alterar os tipos
para avaliar o comportamento do script.

>>>>>>> dev
