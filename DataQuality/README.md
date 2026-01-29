# Data Quality Stage

## Configurando o ambiente

1. Atualize o arquivo docker-compose.env e preencha o valor das variáveis **AWS_ACCESS_KEY_ID** e **AWS_SECRET_ACCESS_KEY**.
2. Execute `docker-compose up -d` no terminal.
3. Execute `docker exec -it jupyter-notebook spark-submit /mnt/data_quality/script_data_quality.py` para rodar job Spark por meio do container.

## Data Quality Results

Abaixo segue um exemplo de report output do Data Quality que pode ser salvo recursos como AWS S3 ou AWS DynamoDB para análise e geração de insights.

``` json
{
  "timestamp": "2026-01-29 19:08:16",
  "processed_records": 397,
  "data_quality_results": {
    "tp_pessoa": {
      "person_type": "PASS",
      "empty_values": "PASS"
    },
    "vl_renda": {
      "empty_values": "PASS",
      "income_values": "PASS"
    },
    "num_telefone_cliente": {
      "empty_values": "FAILED (35 records)",
      "phone_format": "PASS"
    },
    "dt_atualizacao": {
      "empty_values": "PASS"
    },
    "cod_cliente": {
      "empty_values": "PASS",
      "unique_values": "PASS"
    },
    "nm_cliente": {
      "empty_values": "PASS"
    },
    "dt_nascimento_cliente": {
      "empty_values": "PASS",
      "temporal_consistency": "PASS"
    }
  }
}
```
