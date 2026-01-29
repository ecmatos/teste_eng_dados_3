# ETL Stage

## Configurando o ambiente

1. Atualize o arquivo docker-compose.env e preencha o valor das variáveis **AWS_ACCESS_KEY_ID** e **AWS_SECRET_ACCESS_KEY**.
2. Execute `docker-compose up -d` no terminal.
3. Execute `docker exec -it jupyter-notebook spark-submit /mnt/etl/script.py` para rodar job Spark por meio do container.

## Análise inicial da solução

A partir da análise exploratória do dataset, foi identificado que existem registros com o mesmo **cod_cliente**, mas com outros atributos diferentes, como **nm_cliente**.  Considerando o contexto, não foi considerada uma inconsistência de chave primária e assumiu-se que esses registros representam atualizações cadastrais. Com base nessa premissa e assumindo que todas as regras estejam alinhadas às áreas de negócio, será adotada a seguinte abordagem:

* **Camada bronze**: Armazena todos os registros.
* **Camada Silver**: Armazena clientes únicos, removendo duplicidade de dados baseado na coluna **cod_cliente** e mantendo apenas os registros mais recentes, determinado pela coluna **dt_atualizacao**.

## Decisões arquiteturais

### Definição de schemas

#### Bronze

``` python
BRONZE_SCHEMA = {
    "cod_cliente": "string",
    "nm_cliente": "string",
    "nm_pais_cliente": "string",
    "nm_cidade_cliente": "string",
    "nm_rua_cliente": "string",
    "num_casa_cliente": "string",
    "num_telefone_cliente": "string",
    "dt_nascimento_cliente": "string",
    "dt_atualizacao": "string",
    "tp_pessoa": "string",
    "vl_renda": "string"
}
```

#### Silver

``` python
SILVER_SCHEMA = {
    "cod_cliente": "string",
    "nm_cliente": "string",
    "nm_pais_cliente": "string",
    "nm_cidade_cliente": "string",
    "nm_rua_cliente": "string",
    "num_casa_cliente": "string",
    "num_telefone_cliente": "string",
    "dt_nascimento_cliente": "date",
    "dt_atualizacao": "timestamp",
    "tp_pessoa": "string",
    "vl_renda": "decimal(15,2)"
}
```

### Particionamento de tabela

Para o particionamento da tabela, a coluna **anomesdia** foi definida com o padrão **yyyy-mm-dd**, com o intuito de evitar inferência automática de tipos numéricos pelo spark e problemas de comparação durante consulta aos dados ou desenvolvimento posterior.

### Glue Data Catalog

Considerando a premissa de que ambas as tabelas já estão criadas, as tabelas foram criadas como **External Tables** através do Terraform, somente referenciando o local correto dos dados, para que engines de consultas possam enxergá-los. Sendo assim, o Spark ficou responsável somente pelo processamento e escrita dos dados.

**Nota:** Devido a limitações da imagem utilizada, para que as partições sejam aplicadas logicamente e os dados visíveis em engines de consulta, é necessário executar o comando abaixo no **AWS Athena**:

~~~~ sql
MSCK REPAIR TABLE table_name;
~~~~

### Performance

Durante o desenvolvimento, foram consideradas a utilização de controle de paralelismo e particionamento, pois apesar de ser um arquivo pequeno, configurações padrões do Spark forçam um shuffle para tratamento dos dados e degradam a performance do pipeline devido ao número elevado de partições a serem processadas.
